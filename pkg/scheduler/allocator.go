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
    "context"
    "github.com/cloudera/yunikorn-core/pkg/common"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "github.com/cloudera/yunikorn-core/pkg/metrics"
    "github.com/cloudera/yunikorn-core/pkg/plugins"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "go.uber.org/zap"
    "math/rand"
    "sync/atomic"
    "time"
)

// Visible by tests
func (m *Scheduler) SingleStepScheduleAllocTest(nAlloc int) {
    m.singleStepSchedule(nAlloc, &preemptionParameters{
        crossQueuePreemption: false,
        blacklistedRequest: make(map[string]bool),
    })
}

func (m *Scheduler) singleStepSchedule(nAlloc int, preemptionParam *preemptionParameters) {
    if !preemptionParam.crossQueuePreemption {
        m.step++
    }

    for partition, partitionContext := range m.clusterSchedulingContext.getPartitionMapClone() {
        totalPartitionResource := m.clusterInfo.GetTotalPartitionResource(partition)
        if totalPartitionResource == nil {
            continue
        }

        // Following steps:
        // - According to resource usage, find next N allocation Requests, N could be
        //   mini-batch because we don't want the process takes too long. And this
        //   runs as single thread.
        // - According to mini-batched allocation request. Try to allocate. This step
        //   can be done as multiple thread.
        // - For asks cannot be assigned, we will do preemption. Again it is done using
        //   single-thread.
        candidates := m.findAllocationAsks(totalPartitionResource, partitionContext, nAlloc, m.step, preemptionParam /* it is allocation phase */)

        // Try to allocate from candidates, returns allocation proposal as well as failed allocation
        // ask candidates. (For preemption).
        allocations, _ := m.tryBatchAllocation(partition, partitionContext, candidates, preemptionParam /* it is allocation phase */)

        // Send allocations to cache, and pending ask.
        confirmedAllocations := make([]*SchedulingAllocation, 0)
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
        m.handleFailedToAllocationAllocations(confirmedAllocations, candidates, preemptionParam)
    }
}

func (m *Scheduler) regularAllocate(nodes []*SchedulingNode, candidate *SchedulingAllocationAsk) *SchedulingAllocation {
    nNodes := len(nodes)
    startIdx := rand.Intn(nNodes)
    for i := 0; i < len(nodes); i++ {
        idx := (i + startIdx) % nNodes
        node := nodes[idx]
        if !node.CheckAllocateConditions(candidate.AskProto.AllocationKey) {
            // skip the node if conditions can not be satisfied
            continue
        }
        if node.CheckAndAllocateResource(candidate.AllocatedResource, false /* preemptionPhase */) {
            // before deciding on an allocation, call the reconcile plugin to sync scheduler cache
            // between core and shim if necessary. This is useful when running multiple allocations
            // in parallel and need to handle inter container affinity and anti-affinity.
            if rp := plugins.GetReconcilePlugin(); rp != nil {
                if err := rp.ReSyncSchedulerCache(&si.ReSyncSchedulerCacheArgs{
                    AssumedAllocations:    []*si.AssumedAllocation{
                        {
                            AllocationKey: candidate.AskProto.AllocationKey,
                            NodeId:        node.NodeId,
                        },
                    },
                }); err != nil {
                    log.Logger().Error("failed to sync cache",
                        zap.Error(err))
                }
            }

            // return allocation
            return NewSchedulingAllocation(candidate, node.NodeId)
        }
    }
    return nil
}

func (m *Scheduler) allocate(nodes []*SchedulingNode, candidate *SchedulingAllocationAsk, preemptionParam *preemptionParameters) *SchedulingAllocation {
    if preemptionParam.crossQueuePreemption {
        return crossQueuePreemptionAllocate(m.preemptionContext.partitions[candidate.PartitionName], nodes, candidate, preemptionParam)
    } else {
        return m.regularAllocate(nodes, candidate)
    }
}

// Do mini batch allocation
func (m *Scheduler) tryBatchAllocation(partition string, partitionContext *PartitionSchedulingContext,
    candidates []*SchedulingAllocationAsk,
    preemptionParam *preemptionParameters) ([]*SchedulingAllocation, []*SchedulingAllocationAsk) {
    // copy list of node since we going to go through node list a couple of times
    nodeList := getNodeList(m, partition)
    if nodeList == nil {
        return make([]*SchedulingAllocation, 0), candidates
    }

    ctx, cancel := context.WithCancel(context.Background())

    allocations := make([]*SchedulingAllocation, len(candidates))
    failedAsks := make([]*SchedulingAllocationAsk, len(candidates))

    var allocatedLength int32
    var failedAskLength int32

    doAllocation := func(i int) {
        allocatingStart := time.Now()

        candidate := candidates[i]
        // Check if the same allocation key got rejected already.
        if preemptionParam.blacklistedRequest[candidate.AskProto.AllocationKey] {
            return
        }

        schedulingNodeList := evaluateForSchedulingPolicy(m, nodeList, partition, candidate, partitionContext)

        if allocation := m.allocate(schedulingNodeList, candidate, preemptionParam); allocation != nil {
            length := atomic.AddInt32(&allocatedLength, 1)
            allocations[length-1] = allocation
        } else {
            length := atomic.AddInt32(&failedAskLength, 1)
            preemptionParam.blacklistedRequest[candidate.AskProto.AllocationKey] = true
            failedAsks[length-1] = candidate
        }

        // record the latency
        metrics.GetSchedulerMetrics().ObserveSchedulingLatency(allocatingStart)
    }

    common.ParallelizeUntil(ctx, 1, len(candidates), doAllocation)

    cancel()

    // Logging
    if len(allocations) > 0 || len(failedAsks) > 0 {
        log.Logger().Debug("allocations cannot satisfy asks",
            zap.Int("numOfAllocations", len(allocations)),
            zap.Int("numOfFailedAsks", len(failedAsks)))
        if len(allocations) > 0 {
            if log.IsDebugEnabled() {
                for _, alloc := range allocations {
                    if alloc != nil {
                        log.Logger().Debug("allocation",
                            zap.Any("allocation", alloc))
                    }
                }
            }
        }
        if len(failedAsks) > 0 {
            if log.IsDebugEnabled() {
                for _, failedAsk := range failedAsks {
                    if failedAsk != nil {
                        log.Logger().Debug("failedAsks",
                            zap.Any("ask", failedAsk))
                    }
                }
            }
        }
    }

    return allocations, failedAsks
}

// TODO: convert this as an interface.
func evaluateForSchedulingPolicy(m *Scheduler, nodes []*SchedulingNode, partition string,
    candidate *SchedulingAllocationAsk, partitionContext *PartitionSchedulingContext) []*SchedulingNode {

    // If bin-packing is not enabled, simply return
    if !partitionContext.partition.NeedBinPackingSchedulingPolicy() {
        // Sort by MAX_AVAILABLE resources.
        // TODO, this should be configurable.
        SortNodes(nodes, MaxAvailableResources)
        return nodes
    }

    // Do an in-place sorting
    nodes = m.SortAllNodesWithAscendingResource(partition)

    for i := 0; i < len(nodes); i++ {
        node := nodes[i]
        if !node.CheckAllocateConditions(candidate.AskProto.AllocationKey) {
            // skip the node if conditions can not be satisfied
            continue
        }

        // once we have a node which can fit the allocation ask, send back for scheduling
        if node.CheckAndAllocateResource(candidate.AllocatedResource, false /* preemptionPhase */) {
            return append(make([]*SchedulingNode, 1), node)
        }
    }

    return nodes
}

func getNodeList(m *Scheduler, partition string) []*SchedulingNode {
    nodeList := m.clusterInfo.GetPartition(partition).CopyNodeInfos()
    if len(nodeList) <= 0 {
        // When we don't have node, do nothing
        return nil
    }

    schedulingNodeList := make([]*SchedulingNode, len(nodeList))
    for idx, v := range nodeList {
        schedulingNodeList[idx] = NewSchedulingNode(v)
    }

    return schedulingNodeList
}

func (m* Scheduler) handleFailedToAllocationAllocations(allocations []*SchedulingAllocation, candidates []*SchedulingAllocationAsk, preemptionParam *preemptionParameters) {
    // Failed allocated asks
    failedToAllocationKeys := make(map[string]bool, 0)
    allocatedKeys := make(map[string]bool, 0)

    for _, c := range candidates {
        failedToAllocationKeys[c.AskProto.AllocationKey] = true
    }

    for _, alloc := range allocations {
        delete(failedToAllocationKeys, alloc.SchedulingAsk.AskProto.AllocationKey)
        allocatedKeys[alloc.SchedulingAsk.AskProto.AllocationKey] = true
    }

    for failedAllocationKey := range failedToAllocationKeys {
        if preemptionParam.crossQueuePreemption {
            preemptionParam.blacklistedRequest[failedAllocationKey] = true
        } else {
            curWaitValue := m.waitTillNextTry[failedAllocationKey]
            if curWaitValue == 0 {
                curWaitValue = 2
            } else if curWaitValue < (1 << 20) {
                // Increase missed value if it is less than 2^20 (TODO need do some experiments about this value)
                curWaitValue <<= 1
            }
            m.waitTillNextTry[failedAllocationKey] = curWaitValue
        }
    }

    for allocatedKey := range allocatedKeys {
        if preemptionParam.crossQueuePreemption {
            delete(preemptionParam.blacklistedRequest, allocatedKey)
        } else {
            delete(m.waitTillNextTry, allocatedKey)
        }
    }
}
