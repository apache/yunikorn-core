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
    "container/list"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
)

// Visible by tests
func (m *Scheduler) SingleStepScheduleAllocTest(nAlloc int) {
    for i := 0; i < nAlloc; i++ {
        m.singleStepSchedule()
    }
}

func (m *Scheduler) singleStepSchedule() {
    // Do regular allocation
    for partition, partitionContext := range m.clusterSchedulingContext.getPartitionMapClone() {
        totalPartitionResource := m.clusterInfo.GetTotalPartitionResource(partition)
        if totalPartitionResource == nil {
            continue
        }

        // Do reservation allocation First
        allocs := m.reservationAllocation(partitionContext)
        if nil == allocs {
            allocForRegularAllocation := m.tryAllocationForPartition(totalPartitionResource, partitionContext)
            if allocForRegularAllocation != nil {
                allocs = []*SchedulingAllocation{allocForRegularAllocation}
            }
        }
        m.handleSchedulingAllocation(allocs, partitionContext)
    }
}

// Apps could have excessive reservations, we need to clean them up first
func dropExcessiveReservationRequests(reservedApps *list.List) []*SchedulingAllocation {
    droppedExcessiveReservationRequest := make([]*SchedulingAllocation, 0)

    for e := reservedApps.Front(); e != nil; e = e.Next() {
        app := e.Value.(*SchedulingApplication)
        appUnreserved := app.dropExcessiveReservationRequest()
        if appUnreserved != nil && len(appUnreserved) > 0 {
            for _, alloc := range appUnreserved {
                droppedExcessiveReservationRequest = append(droppedExcessiveReservationRequest, alloc)
            }
        }
    }

    return droppedExcessiveReservationRequest
}

// Do reservation allocation, returns if anything allocated
func (m *Scheduler) reservationAllocation(partitionCtx *PartitionSchedulingContext) []*SchedulingAllocation {
    appList := partitionCtx.GetReservationAppListClone()

    if appList == nil {
        return nil
    }

    droppedExcessiveReservationRequest := dropExcessiveReservationRequests(appList)
    if droppedExcessiveReservationRequest != nil && len(droppedExcessiveReservationRequest) > 0 {
        return droppedExcessiveReservationRequest
    }

    for appList.Len() > 0 {
        // Get first app, but not remove it for now since the app could have multiple reservation requests
        app := appList.Front().Value.(*SchedulingApplication)

        allocation := app.TryAllocateFromReservationRequests()

        if nil == allocation {
            appList.Remove(appList.Front())
            continue
        }

        return allocation
    }

    return nil
}

func (m *Scheduler) handleSchedulingAllocation(allocations []*SchedulingAllocation, partitionCtx *PartitionSchedulingContext) {
    for _, alloc := range allocations {
        if alloc == nil {
            return
        } else if alloc.AllocationResult == Reservation {
            // Reservation is kept inside scheduler, so update internal scheduler context
            partitionCtx.HandleReservationProposal(alloc)
        } else if alloc.AllocationResult == Unreserve {
            partitionCtx.HandleUnreservedRequest(alloc)
        } else if alloc.AllocationResult == Allocation || alloc.AllocationResult == AllocationFromReservation {
            partitionCtx.HandleAllocationProposal(alloc)
            // Send allocation proposal to cache to commit
            m.eventHandlers.CacheEventHandler.HandleEvent(newSingleAllocationProposal(alloc))
        }
    }
}

// Try to allocate for a given partition
func (m *Scheduler) tryAllocationForPartition(partitionTotalResource *resources.Resource, partitionContext *PartitionSchedulingContext) *SchedulingAllocation {

    // Do we have any pending resource?
    if !resources.StrictlyGreaterThanZero(partitionContext.Root.GetPendingResource()) {
        // If no pending resource, return empty array
        return nil
    }

    return partitionContext.Root.tryAllocate(partitionTotalResource, partitionContext, nil, nil)
}

// do this from queue hierarchy, sortedQueueCandidates is temporary var
// and won't be shared in other goroutines
func (m *Scheduler) tryAllocateForQueuesRecursively(
    partitionTotalResource *resources.Resource,
    sortedQueueCandidates []*SchedulingQueue,
    partitionContext *PartitionSchedulingContext,
    parentHeadroom *resources.Resource,
    parentQueueMaxLimit *resources.Resource) *SchedulingAllocation {
    for _, queue := range sortedQueueCandidates {
        if alloc := queue.tryAllocate(partitionTotalResource, partitionContext, parentHeadroom, parentQueueMaxLimit); alloc != nil {
            return alloc
        }
    }

    return nil
}
