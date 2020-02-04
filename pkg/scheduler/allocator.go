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
	"context"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// Visible by tests
func (m *Scheduler) SingleStepScheduleAllocTest(nAlloc int) {
	m.singleStepSchedule(nAlloc)
}

func (m *Scheduler) singleStepSchedule(nAlloc int) {
	for partition, partitionContext := range m.clusterSchedulingContext.getPartitionMapClone() {
		totalPartitionResource := m.clusterInfo.GetTotalPartitionResource(partition)
		if totalPartitionResource == nil {
			continue
		}

		// Following steps:
		// - According to resource usage, find next N allocation requests, N could be
		//   mini-batch because we don't want the process takes too long. And this
		//   runs as single thread.
		// - According to mini-batched allocation request. Try to allocate. This step
		//   can be done as multiple thread.
		// - For asks cannot be assigned, we will do preemption. Again it is done using
		//   single-thread.
		candidates := m.findAllocationAsks(totalPartitionResource, partitionContext, nAlloc, m.step)

		// Try to allocate from candidates, returns allocation proposal as well as failed allocation
		// ask candidates. (For preemption).
		// TODO clean this up we ignore failures so why collect them?
		allocations, _ := m.tryBatchAllocation(partition, partitionContext, candidates)

		// Send allocations to cache, and pending ask.
		confirmedAllocations := make([]*schedulingAllocation, 0)
		if len(allocations) > 0 {
			for _, alloc := range allocations {
				if alloc == nil {
					continue
				}

				proposal := newSingleAllocationProposal(alloc)
				err := m.updateSchedulingRequestPendingAskByDelta(proposal.AllocationProposals[0], -1)
				if err == nil {
					m.eventHandlers.CacheEventHandler.HandleEvent(newSingleAllocationProposal(alloc))
					confirmedAllocations = append(confirmedAllocations, alloc)
				} else {
					log.Logger().Error("failed to send allocation proposal",
						zap.Error(err))
				}
			}
		}

		nAlloc -= len(confirmedAllocations)

		// Update missed opportunities
		m.handleFailedToAllocationAllocations(confirmedAllocations, candidates)
	}
}

func (m *Scheduler) regularAllocate(nodeIterator NodeIterator, candidate *schedulingAllocationAsk) *schedulingAllocation {
	for nodeIterator.HasNext() {
		node := nodeIterator.Next()
		// skip the node if we cannot fit or are unschedulable
		if !node.preAllocateCheck(candidate.AllocatedResource, false) {
			// skip schedule onto node
			log.Logger().Info("skipping node for allocation",
				zap.String("reason", "basic condition not satisfied"),
				zap.String("node", node.NodeID),
				zap.Any("request", candidate.AskProto))
			continue
		}
		// skip the node if conditions can not be satisfied
		if !node.preAllocateConditions(candidate.AskProto.AllocationKey) {
			continue
		}
		// everything OK really allocate
		if node.allocateResource(candidate.AllocatedResource, false) {
			// before deciding on an allocation, call the reconcile plugin to sync scheduler cache
			// between core and shim if necessary. This is useful when running multiple allocations
			// in parallel and need to handle inter container affinity and anti-affinity.
			if rp := plugins.GetReconcilePlugin(); rp != nil {
				if err := rp.ReSyncSchedulerCache(&si.ReSyncSchedulerCacheArgs{
					AssumedAllocations: []*si.AssumedAllocation{
						{
							AllocationKey: candidate.AskProto.AllocationKey,
							NodeID:        node.NodeID,
						},
					},
				}); err != nil {
					log.Logger().Error("failed to sync cache",
						zap.Error(err))
				}
			}

			// return allocation
			return newSchedulingAllocation(candidate, node.NodeID)
		}
	}
	return nil
}

func (m *Scheduler) allocate(nodes NodeIterator, candidate *schedulingAllocationAsk) *schedulingAllocation {
	return m.regularAllocate(nodes, candidate)
}

// Do mini batch allocation
func (m *Scheduler) tryBatchAllocation(partition string, partitionContext *partitionSchedulingContext,
	candidates []*schedulingAllocationAsk) ([]*schedulingAllocation, []*schedulingAllocationAsk) {
	// copy list of node since we going to go through node list a couple of times
	nodeList := partitionContext.getSchedulableNodes()
	if len(nodeList) == 0 {
		return make([]*schedulingAllocation, 0), candidates
	}

	ctx, cancel := context.WithCancel(context.Background())

	allocations := make([]*schedulingAllocation, len(candidates))
	failedAsks := make([]*schedulingAllocationAsk, len(candidates))

	var allocatedLength int32
	var failedAskLength int32

	doAllocation := func(i int) {
		allocatingStart := time.Now()

		candidate := candidates[i]
		// Check if the same allocation key got rejected already.
		nodeIterator := m.evaluateForSchedulingPolicy(nodeList, partitionContext)

		if allocation := m.allocate(nodeIterator, candidate); allocation != nil {
			length := atomic.AddInt32(&allocatedLength, 1)
			allocations[length-1] = allocation
		}

		// record the latency
		metrics.GetSchedulerMetrics().ObserveSchedulingLatency(allocatingStart)
	}

	common.ParallelizeUntil(ctx, 1, len(candidates), doAllocation)

	cancel()

	// Logging only at debug level and just the parts needed
	if log.IsDebugEnabled() {
		if allocatedLength > 0 {
			log.Logger().Debug("allocations added",
				zap.Int32("numOfAllocations", allocatedLength))
			for _, alloc := range allocations {
				if alloc != nil {
					log.Logger().Debug("allocation",
						zap.Any("allocation", alloc))
				}
			}
		}
		if failedAskLength > 0 {
			log.Logger().Debug("asks failed",
				zap.Int32("numOfFailedAsks", failedAskLength))
			for _, failedAsk := range failedAsks {
				if failedAsk != nil {
					log.Logger().Debug("failedAsks",
						zap.Any("ask", failedAsk))
				}
			}
		}
	}

	// limit the slices based on what was found
	return allocations[:allocatedLength], failedAsks[:failedAskLength]
}

// TODO: convert this as an interface.
func (m *Scheduler) evaluateForSchedulingPolicy(nodes []*schedulingNode, partitionContext *partitionSchedulingContext) NodeIterator {
	// Sort Nodes based on the policy configured.
	configuredPolicy := partitionContext.partition.GetNodeSortingPolicy()
	switch configuredPolicy {
	case common.BinPackingPolicy:
		sortNodes(nodes, MinAvailableResources)
		return NewDefaultNodeIterator(nodes)
	case common.FairnessPolicy:
		sortNodes(nodes, MaxAvailableResources)
		return NewDefaultNodeIterator(nodes)
	}

	return nil
}

func (m *Scheduler) handleFailedToAllocationAllocations(allocations []*schedulingAllocation, candidates []*schedulingAllocationAsk) {
	// Failed allocated asks
	failedToAllocationKeys := make(map[string]bool)
	allocatedKeys := make(map[string]bool)

	for _, c := range candidates {
		failedToAllocationKeys[c.AskProto.AllocationKey] = true
	}

	for _, alloc := range allocations {
		delete(failedToAllocationKeys, alloc.schedulingAsk.AskProto.AllocationKey)
		allocatedKeys[alloc.schedulingAsk.AskProto.AllocationKey] = true
	}

	for failedAllocationKey := range failedToAllocationKeys {
		curWaitValue := m.waitTillNextTry[failedAllocationKey]
		if curWaitValue == 0 {
			curWaitValue = 2
		} else if curWaitValue < (1 << 20) {
			// Increase missed value if it is less than 2^20 (TODO need do some experiments about this value)
			curWaitValue <<= 1
		}
		m.waitTillNextTry[failedAllocationKey] = curWaitValue
	}

	for allocatedKey := range allocatedKeys {
		delete(m.waitTillNextTry, allocatedKey)
	}
}
