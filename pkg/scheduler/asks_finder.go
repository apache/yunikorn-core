/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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

	"github.com/cloudera/yunikorn-core/pkg/common/resources"
	"github.com/cloudera/yunikorn-core/pkg/log"
)

// Find next set of allocation asks for scheduler to place
// This could be "mini batch", no need to return too many candidates
func (m *Scheduler) findAllocationAsks(partitionTotalResource *resources.Resource, partitionContext *PartitionSchedulingContext, n int,
	curStep uint64, preemptionParam *preemptionParameters) []*SchedulingAllocationAsk {
	mayAllocateList := make([]*SchedulingAllocationAsk, 0)

	// Do we have any pending resource?
	if !resources.StrictlyGreaterThanZero(partitionContext.Root.GetPendingResource()) {
		// If no pending resource, return empty array
		return mayAllocateList
	}

	// Reset may allocations
	m.resetMayAllocations(partitionContext)

	selectedAsksByAllocationKey := make(map[string]int32)

	// Repeatedly go to queue hierarchy, find next allocation ask, until we find N allocations
	found := true
	for found {
		// Find next allocation ask, see if it can be allocated, if yes, add to
		// may allocate list.
		next := m.findNextAllocationAskCandidate(partitionTotalResource, []*SchedulingQueue{partitionContext.Root}, partitionContext,
			nil, nil, curStep, selectedAsksByAllocationKey, preemptionParam)
		found = next != nil

		if found {
			mayAllocateList = append(mayAllocateList, next)
			if len(mayAllocateList) >= n {
				break
			}
		}
	}

	return mayAllocateList
}

// Return a sorted copy of the queues for this parent queue.
// Only queues with a pending resource request are considered. The queues are sorted using the
// sorting type for the parent queue.
// Stopped queues will be filtered out at a later stage.
func sortSubqueuesFromQueue(parentQueue *SchedulingQueue) []*SchedulingQueue {
	// Create a list of the queues with pending resources
	sortedQueues := make([]*SchedulingQueue, 0)
	for _, child := range parentQueue.GetCopyOfChildren() {
		// Only look at queue when pending-res > 0
		if resources.StrictlyGreaterThanZero(child.GetPendingResource()) {
			sortedQueues = append(sortedQueues, child)
		}
	}

	// Sort the queues
	SortQueue(sortedQueues, parentQueue.QueueSortType)

	return sortedQueues
}

// Return a sorted copy of the applications in the queue.
// Only applications with a pending resource request are considered. The applications are sorted using the
// sorting type for the leaf queue they are in.
func sortApplicationsFromQueue(leafQueue *SchedulingQueue) []*SchedulingApplication {
	leafQueue.lock.RLock()
	defer leafQueue.lock.RUnlock()

	// Create a copy of the applications with pending resources
	sortedApps := make([]*SchedulingApplication, 0)
	for _, v := range leafQueue.applications {
		// Only look at app when pending-res > 0
		if resources.StrictlyGreaterThanZero(v.Requests.GetPendingResource()) {
			sortedApps = append(sortedApps, v)
		}
	}

	// Sort the applications
	SortApplications(sortedApps, leafQueue.ApplicationSortType, leafQueue.CachedQueueInfo.GuaranteedResource)

	return sortedApps
}

// sort scheduling Requests from a job
func (m *Scheduler) findMayAllocationFromApplication(schedulingRequests *SchedulingRequests,
	headroom *resources.Resource, curStep uint64, selectedPendingAskByAllocationKey map[string]int32, preemptionParameters *preemptionParameters) *SchedulingAllocationAsk {
	schedulingRequests.lock.RLock()
	defer schedulingRequests.lock.RUnlock()

	var bestAsk *SchedulingAllocationAsk = nil

	for _, v := range schedulingRequests.requests {
		if preemptionParameters.crossQueuePreemption {
			// Skip black listed requests for this preemption cycle.
			if preemptionParameters.blacklistedRequest[v.AskProto.AllocationKey] {
				continue
			}
		} else {
			// For normal allocation.
			if m.waitTillNextTry[v.AskProto.AllocationKey]&curStep != 0 {
				// this request is "blacklisted"
				continue
			}
		}

		// Only sort request if its resource fits headroom
		if v.PendingRepeatAsk-selectedPendingAskByAllocationKey[v.AskProto.AllocationKey] > 0 && resources.FitIn(headroom, v.AllocatedResource) {
			if bestAsk == nil || v.NormalizedPriority > bestAsk.NormalizedPriority {
				bestAsk = v
			}
		}
	}

	if bestAsk != nil {
		selectedPendingAskByAllocationKey[bestAsk.AskProto.AllocationKey] += 1
	}

	return bestAsk
}

func getHeadroomOfQueue(parentHeadroom *resources.Resource, queueMaxLimit *resources.Resource, queue *SchedulingQueue,
	preemptionParameters *preemptionParameters) *resources.Resource {
	// When cross-queue preemption is enabled, don't calculate headroom of non-leaf queues.
	if preemptionParameters.crossQueuePreemption {
		if !queue.isLeafQueue() {
			return nil
		}
	}

	// new headroom for this queue
	if nil != parentHeadroom {
		return resources.ComponentWiseMin(resources.Sub(queueMaxLimit, queue.ProposingResource), parentHeadroom)
	}
	return resources.Sub(queueMaxLimit, queue.ProposingResource)
}

func getQueueMaxLimit(partitionTotalResource *resources.Resource, queue *SchedulingQueue, parentMaxLimit *resources.Resource) *resources.Resource {
	if queue.isRoot() {
		return partitionTotalResource
	}

	// Get max resource of parent queue
	maxResource := queue.CachedQueueInfo.MaxResource
	if maxResource == nil {
		maxResource = parentMaxLimit
	}
	maxResource = resources.ComponentWiseMin(maxResource, partitionTotalResource)
	return maxResource
}

// do this from queue hierarchy, sortedQueueCandidates is temporary var
// and won't be shared in other goroutines
func (m *Scheduler) findNextAllocationAskCandidate(
	partitionTotalResource *resources.Resource,
	sortedQueueCandidates []*SchedulingQueue,
	partitionContext *PartitionSchedulingContext,
	parentHeadroom *resources.Resource,
	parentQueueMaxLimit *resources.Resource,
	curStep uint64,
	selectedPendingAskByAllocationKey map[string]int32,
	preemptionParameters *preemptionParameters) *SchedulingAllocationAsk {
	for _, queue := range sortedQueueCandidates {
		// skip stopped queues: running and draining queues are allowed
		if queue.isStopped() {
			log.Logger().Debug("skip non-running queue",
				zap.String("queueName", queue.Name))
			continue
		}
		// Is it need any resource?
		if !resources.StrictlyGreaterThanZero(queue.GetPendingResource()) {
			log.Logger().Debug("skip queue because it has no pending resource",
				zap.String("queueName", queue.Name))
			continue
		}

		// Get queue max resource
		queueMaxLimit := getQueueMaxLimit(partitionTotalResource, queue, parentQueueMaxLimit)

		// Get headroom
		newHeadroom := getHeadroomOfQueue(parentHeadroom, queueMaxLimit, queue, preemptionParameters)

		if queue.isLeafQueue() {
			// Handle for cross queue preemption
			if preemptionParameters.crossQueuePreemption {
				// We won't allocate resources if the queue is above its guaranteed resource.
				if comp := resources.CompUsageRatio(queue.ProposingResource, queue.CachedQueueInfo.GuaranteedResource, queue.CachedQueueInfo.GuaranteedResource); comp >= 0 {
					log.Logger().Debug("skip queue because it is already beyond guaranteed",
						zap.String("queueName", queue.Name))
					continue
				}
			}

			sortedApps := sortApplicationsFromQueue(queue)
			for _, app := range sortedApps {
				if ask := m.findMayAllocationFromApplication(app.Requests, newHeadroom, curStep,
					selectedPendingAskByAllocationKey, preemptionParameters); ask != nil {
					app.MayAllocatedResource = resources.Add(app.MayAllocatedResource, ask.AllocatedResource)
					queue.ProposingResource = resources.Add(queue.ProposingResource, ask.AllocatedResource)
					return ask
				}
			}
		} else {
			sortedChildren := sortSubqueuesFromQueue(queue)
			if ask := m.findNextAllocationAskCandidate(partitionTotalResource, sortedChildren, partitionContext, newHeadroom, queueMaxLimit,
				curStep, selectedPendingAskByAllocationKey, preemptionParameters); ask != nil {
				queue.ProposingResource = resources.Add(queue.ProposingResource, ask.AllocatedResource)
				return ask
			}
		}
	}

	return nil
}

func (m *Scheduler) resetMayAllocations(partitionContext *PartitionSchedulingContext) {
	// Recursively reset may-allocation
	// lock the partition
	partitionContext.lock.Lock()
	defer partitionContext.lock.Unlock()

	m.resetMayAllocationsForQueue(partitionContext.Root)
}

func (m *Scheduler) resetMayAllocationsForQueue(queue *SchedulingQueue) {
	queue.ProposingResource = queue.CachedQueueInfo.GetAllocatedResource()
	queue.SetAllocatingResource(queue.CachedQueueInfo.GetAllocatedResource())
	if queue.isLeafQueue() {
		for _, app := range queue.applications {
			app.MayAllocatedResource = app.ApplicationInfo.GetAllocatedResource()
		}
	} else {
		for _, child := range queue.childrenQueues {
			m.resetMayAllocationsForQueue(child)
		}
	}
}
