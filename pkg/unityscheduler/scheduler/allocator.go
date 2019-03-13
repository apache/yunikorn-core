/*
Copyright 2019 The Unity Scheduler Authors

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
    "github.com/golang/glog"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/common"
    "math/rand"
    "sync/atomic"
)

func (m *Scheduler) allocate(nodes []*SchedulingNode, candidate *SchedulingAllocationAsk) *SchedulingAllocation {
    nNodes := len(nodes)
    startIdx := rand.Intn(nNodes)
    for i := 0; i < len(nodes); i++ {
        idx := (i + startIdx) % nNodes
        node := nodes[idx]
        if node.CheckAndAllocateResource(candidate.AllocatedResource) {
            // return allocation
            return NewSchedulingAllocation(candidate, node.NodeId)
        }
    }
    return nil
}

// Do mini batch allocation
func (m *Scheduler) tryBatchAllocation(partition string, candidates []*SchedulingAllocationAsk) ([]*SchedulingAllocation, []*SchedulingAllocationAsk) {
    // copy list of node since we going to go through node list a couple of times
    nodeList := m.clusterInfo.GetPartition(partition).CopyNodeInfos()
    if len(nodeList) <= 0 {
        // When we don't have node, do nothing
        return make([]*SchedulingAllocation, 0), candidates
    }

    schedulingNodeList := make([]*SchedulingNode, len(nodeList))
    for idx, v := range nodeList {
        schedulingNodeList[idx] = NewSchedulingNode(v)
    }

    ctx, cancel := context.WithCancel(context.Background())

    allocations := make([]*SchedulingAllocation, len(candidates))
    failedAsks := make([]*SchedulingAllocationAsk, len(candidates))

    var allocatedLength int32
    var failedAskLength int32

    doAllocation := func(i int) {
        candidate := candidates[i]

        if allocation := m.allocate(schedulingNodeList, candidate); allocation != nil {
            length := atomic.AddInt32(&allocatedLength, 1)
            allocations[length-1] = allocation
        } else {
            length := atomic.AddInt32(&failedAskLength, 1)
            failedAsks[length-1] = candidate
        }
    }

    common.ParallelizeUntil(ctx, 1, len(candidates), doAllocation)

    cancel()

    // Logging
    if len(allocations) > 0 || len(failedAsks) > 0 {
        glog.V(2).Infof("Allocated %d allocations, and cannot satisfy %d asks", len(allocations), len(failedAsks))
        if len(allocations) > 0 {
            glog.V(2).Infof("  Allocations:%v", allocations)
        }
        if len(failedAsks) > 0 {
            glog.V(2).Infof("  Rejected-allocation-asks:%v", failedAsks)
        }
    }

    return allocations, failedAsks
}

func (m *Scheduler) updateMissedOpportunity(allocations []*SchedulingAllocation, candidates []*SchedulingAllocationAsk) {
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
        missedOpValue := m.missedOpportunities[failedAllocationKey]
        if missedOpValue == 0 {
            missedOpValue = 2
        } else if missedOpValue < (1 << 20) {
            // Increase missed value if it is less than 2^20 (TODO need do some experiments about this value)
            missedOpValue <<= 1
        }
        m.missedOpportunities[failedAllocationKey] = missedOpValue
    }

    for allocatedKey := range allocatedKeys {
        delete(m.missedOpportunities, allocatedKey)
    }
}
