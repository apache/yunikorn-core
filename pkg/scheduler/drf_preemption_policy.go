/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package scheduler

import (
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
)

// Preemption policy based-on DRF
type DRFPreemptionPolicy struct {
}

func (m *DRFPreemptionPolicy) DoPreemption(scheduler *Scheduler) {
	// First calculate ideal resource
	calculateIdealResources(scheduler)

	// Then go to under utilized queues and search for requests
	// removed pre-emption as its need a refactor
	// scheduler.schedule()
	// original call
	// scheduler.singleStepSchedule(16, &preemptionParameters{crossQueuePreemption: true, blacklistedRequest: make(map[string]bool)})
}

/*
 * It is possible that, preempt resources from one queue may not make it consumed by another queue.
 * An example is:
 *             root
 *         /    |   \
 *       a      b    c
 *            /  \
 *           b1   b2
 *
 * Let's assume a is underutilized and satisfied (no pending request).
 * b is over-utilized and reaches its maximum limit.
 * c is over-utilized.
 * b1 is demanding and underutilized, b2 is over-utilized.
 * For this case, preempting resources from c cannot help with b1, because b reaches its maximum capacity and cannot consume more resources.
 *
 * So our algorithm should properly answer: if we preempt resources N from queue X (preemptee),
 * can we make sure demanding queue Y (preemptor) or its parent reduces their shortages
 * return true if positive contribution made to headroom shortage.
 */

// TODO: An optimization is: calculate contributions first, and sort preemption victims by descend order of contribution to resource-to-preempt.
func headroomShortageUpdate(preemptor *preemptionQueueContext, preemptee *preemptionQueueContext, allocationResource *resources.Resource,
	queueHeadroomShortages map[string]*resources.Resource) bool {
	// When we don't have any resource shortage issue, no positive contribution we can make.
	if len(queueHeadroomShortages) == 0 {
		return false
	}

	// Try to deduct allocation resource from queueHeadroom shortage, and see if it makes positive contribution.
	cur := preemptee
	positiveContribution := false
	for cur != nil {
		if headroomShortage := queueHeadroomShortages[cur.queuePath]; headroomShortage != nil {
			newHeadroomShortage := resources.SubEliminateNegative(headroomShortage, allocationResource)
			if resources.StrictlyGreaterThan(headroomShortage, newHeadroomShortage) {
				// Good, makes positive contribution
				if resources.StrictlyGreaterThanZero(newHeadroomShortage) {
					queueHeadroomShortages[cur.queuePath] = newHeadroomShortage
				} else {
					delete(queueHeadroomShortages, cur.queuePath)
				}
				positiveContribution = true
			}
		}
		cur = cur.parent
	}

	return positiveContribution
}

func initHeadroomShortages(preemptorQueue *preemptionQueueContext, allocatedResource *resources.Resource) map[string]*resources.Resource {
	// Get all headroom shortages of preemptor's parent.
	headroomShortages := make(map[string]*resources.Resource)
	cur := preemptorQueue
	for cur != nil {
		// Headroom = max - may_allocated + preempting
		headroom := resources.Sub(cur.resources.max, cur.schedulingQueue.GetPreemptingResource())
		headroom.AddTo(cur.resources.markedPreemptedResource)
		headroomShortage := resources.SubEliminateNegative(allocatedResource, headroom)
		if resources.StrictlyGreaterThanZero(headroomShortage) {
			headroomShortages[cur.queuePath] = headroomShortage
		}
		cur = cur.parent
	}

	return headroomShortages
}

// Can we do surgical preemption on the node?
type singleNodePreemptResult struct {
	node                  *objects.Node
	toReleaseAllocations  map[string]*objects.Allocation
	totalReleasedResource *resources.Resource
}

// Do surgical preemption on node, if able to preempt, returns
func trySurgicalPreemptionOnNode(preemptionPartitionCtx *preemptionPartitionContext, preemptorQueue *preemptionQueueContext, node *objects.Node, candidate *objects.AllocationAsk,
	headroomShortages map[string]*resources.Resource) *singleNodePreemptResult {
	// If allocated resource can fit in the node, and no headroom shortage of preemptor queue, we can directly get it allocated. (lucky!)
	if node.CanAllocate(candidate.GetAllocatedResource()) {
		log.Logger().Debug("No preemption needed candidate fits on node",
			zap.String("nodeID", node.NodeID))
		return &singleNodePreemptResult{
			node:                  node,
			toReleaseAllocations:  make(map[string]*objects.Allocation),
			totalReleasedResource: resources.Zero,
		}
	}

	// the number of resources to preempt for this request is the requested - available
	// ignoring anything below 0 as we have more available than requested
	// don't count at what is already marked for preemption (that is still used)
	// the scheduling node's available resource takes into account what is being allocated
	resourceToPreempt := resources.SubEliminateNegative(candidate.GetAllocatedResource(), node.GetAvailableResource())

	toReleaseAllocations := make(map[string]*objects.Allocation)
	totalReleasedResource := resources.NewResource()

	// Otherwise, try to do preemption, list all allocations on the node.
	// Fixme: this operation has too many copies, should avoid for better perf
	for _, alloc := range node.GetAllAllocations() {
		queueName := alloc.GetQueue()
		// Try to do preemption.
		preemptQueue := preemptionPartitionCtx.leafQueues[queueName]
		if nil == preemptQueue {
			continue
		}

		// Skip when the queue has <= 0 preempt-able resource
		if resources.CompUsageRatio(preemptQueue.resources.preemptable, resources.Zero, preemptionPartitionCtx.partitionTotalResource) <= 0 {
			continue
		}

		postPreemption := resources.SubEliminateNegative(preemptQueue.resources.preemptable, candidate.GetAllocatedResource())

		// Make sure this preemption has positive impact of preemptable values. A corner case is:
		// A queue has preemptable resource = (memory=0, cpu=0, gpu=2), for this case, we should avoid preempt any allocation w/o gpu resource > 0
		if resources.CompUsageRatio(postPreemption, preemptQueue.resources.preemptable, preemptionPartitionCtx.partitionTotalResource) >= 0 {
			continue
		}

		// Add one more check, to make sure that preempted resource will be used by candidate queue.
		// When this check fails it means preempted container doesn't make a positive contribution towards preemptor queue and its parents' headroom shortages. (
		// How much headroom needed to allocate candidate).
		headroomShortageUpdate(preemptorQueue, preemptQueue, alloc.GetAllocatedResource(), headroomShortages)

		// let's preempt the container.
		toReleaseAllocations[alloc.GetUUID()] = alloc
		totalReleasedResource.AddTo(alloc.GetAllocatedResource())

		// Check if we preempted enough resources.
		if resources.StrictlyGreaterThanOrEquals(totalReleasedResource, resourceToPreempt) {
			log.Logger().Debug("Preemption requested on node",
				zap.String("nodeID", node.NodeID),
				zap.Any("resources released", totalReleasedResource))
			return &singleNodePreemptResult{
				node:                  node,
				toReleaseAllocations:  toReleaseAllocations,
				totalReleasedResource: totalReleasedResource,
			}
		}
	}

	return nil
}

//nolint:deadcode,unused
func crossQueuePreemptionAllocate(preemptionPartitionContext *preemptionPartitionContext, nodeIterator objects.NodeIterator, candidate *objects.AllocationAsk) *objects.Allocation {
	if preemptionPartitionContext == nil {
		return nil
	}

	preemptorQueue := preemptionPartitionContext.leafQueues[candidate.GetQueue()]
	if preemptorQueue == nil {
		return nil
	}

	// this is temporary; all this code will go away in YUNIKORN-1269
	headroomShortages := initHeadroomShortages(preemptorQueue, candidate.GetAllocatedResource())

	// TODO: do we want to make allocation-within-preemption sorted instead of randomly check nodes one-by-one?
	// First let's sort nodes by available resource
	var preemptResult *singleNodePreemptResult = nil

	for nodeIterator.HasNext() {
		node := nodeIterator.Next()
		if node == nil {
			log.Logger().Debug("Node iterator failed to return a node")
			return nil
		}
		if preemptResult = trySurgicalPreemptionOnNode(preemptionPartitionContext, preemptorQueue, node, candidate, headroomShortages); preemptResult != nil {
			break
		}
	}

	if preemptResult == nil {
		log.Logger().Debug("preemption result nil, no preemption possible",
			zap.Any("candidate", candidate))
		return nil
	}

	log.Logger().Debug("preemption result",
		zap.Any("candidate", candidate),
		zap.Any("node", preemptResult.node.NodeID),
		zap.Any("preemptResult", preemptResult.toReleaseAllocations))
	preemptionResults := make([]*singleNodePreemptResult, 0)
	preemptionResults = append(preemptionResults, preemptResult)
	nodeToAllocate := preemptResult.node

	// Now let's see if we need to reclaim some headroom shortages

	// TODO: Do this shotgun preemption in a separate patch, it should be very similar to CS's shotgun preemption. And we can prioritize to preempt from later launched
	//nolint:staticcheck
	if len(headroomShortages) != 0 {
		// Yeah .. we have some shortages.
		// allocations, allocation with lower priorities, etc.
	}

	// this is temporary; all this code will go away in YUNIKORN-1269
	preemptorQueue.schedulingQueue.IncPreemptingResource(candidate.GetAllocatedResource())

	// Finally, let's do preemption proposals
	return createPreemptionAndAllocationProposal(preemptionPartitionContext, nodeToAllocate, candidate, preemptionResults)
}

func createPreemptionAndAllocationProposal(preemptionPartitionContext *preemptionPartitionContext, nodeToAllocate *objects.Node, candidate *objects.AllocationAsk,
	preemptionResults []*singleNodePreemptResult) *objects.Allocation {
	// We will get this allocation by preempting resources.
	allocation := objects.NewAllocation(objects.None.String(), nodeToAllocate.NodeID, candidate)
	// TODO FIX
	// allocation.releases = make([]*commonevents.ReleaseAllocation, 0)

	// And add releases
	for _, pr := range preemptionResults {
		// TODO FIX
		// for uuid, alloc := range pr.toReleaseAllocations {
		// 	allocation.releases = append(allocation.releases, commonevents.NewReleaseAllocation(uuid, alloc.applicationID, nodeToAllocate.nodeInfo.Partition,
		// 		fmt.Sprintf("Preempt allocation=%s for ask=%s", alloc, candidate.allocationKey), si.AllocationReleaseResponse_PREEMPTED_BY_SCHEDULER))
		//
		// 	// Update metrics of preempt queue
		// 	preemptQueue := preemptionPartitionContext.leafQueues[alloc.queueName]
		// 	preemptQueue.resources.markedPreemptedResource.AddTo(alloc.allocatedResource)
		// 	preemptQueue.resources.preemptable = resources.SubEliminateNegative(preemptQueue.resources.preemptable, alloc.allocatedResource)
		// }
		pr.node.IncPreemptingResource(pr.totalReleasedResource)
	}

	// Update metrics
	// For node, update allocating and preempting resources
	// nodeToAllocate.incAllocatingResource(candidate.allocatedResource)

	return allocation
}
