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
    "fmt"
    "github.com/cloudera/yunikorn-core/pkg/cache"
    "github.com/cloudera/yunikorn-core/pkg/common"
    "github.com/cloudera/yunikorn-core/pkg/common/math"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "github.com/cloudera/yunikorn-core/pkg/plugins"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "go.uber.org/zap"
    gomath "math"
    "sync"
)

type SchedulingApplication struct {
    ApplicationInfo *cache.ApplicationInfo
    Requests        *AppSchedulingRequests

    // allocating resources, which is not confirmed yet from cache
    allocating *resources.Resource

    // Private fields need protection
    queue *SchedulingQueue // queue the application is running in

    // Reserved request, allocKey -> nodeId-> reservation
    reservedRequests map[string]map[string]*ReservedSchedulingRequest
    allocKeyToNumReservedRequests map[string]int

    lock sync.RWMutex
}

func NewSchedulingApplication(appInfo *cache.ApplicationInfo) *SchedulingApplication {
    app := &SchedulingApplication{
        ApplicationInfo: appInfo,
        allocating: resources.NewResource(),
        reservedRequests: make(map[string]map[string]*ReservedSchedulingRequest),
        allocKeyToNumReservedRequests: make(map[string]int),
    }
    app.Requests = NewSchedulingRequests(app)
    return app
}

// sort scheduling Requests from a job
func (app *SchedulingApplication) tryAllocate(partitionContext *PartitionSchedulingContext,
    headroom *resources.Resource) *SchedulingAllocation {
    app.lock.RLock()
    defer app.lock.RUnlock()

    for _, request := range app.Requests.sortedRequestsByPriority {
        if request.PendingRepeatAsk > 0 && resources.FitIn(headroom, request.AllocatedResource) {
            if allocation := app.allocateForOneRequest(partitionContext, request); allocation != nil {
                return allocation
            }
        }
    }

    // Nothing allocated, skip this app
    return nil
}


// This will be called when
func (app *SchedulingApplication) AddBackAllocationAskRepeat(allocationKey string, nAlloc int32) (*resources.Resource, error) {
    app.lock.Lock()
    defer app.lock.Unlock()

    return app.Requests.updateAllocationAskRepeat(allocationKey, nAlloc)
}


// This method will be called under application's allocation method.
// Returns
// - SchedulingAllocation (if allocated/reserved anything).
// - Enum of why cannot allocate or reserve (if allocated/reserved nothing)
func (m *SchedulingApplication) allocateForOneRequest(partitionContext* PartitionSchedulingContext, candidate *SchedulingAllocationAsk) *SchedulingAllocation {
    nodeList := partitionContext.getSchedulableNodes(true)
    if len(nodeList) == 0 {
        return nil
    }

    nodeIterator := evaluateForSchedulingPolicy(nodeList, partitionContext)

    var bestNodeToReserve *SchedulingNode = nil
    bestScore := gomath.Inf(+1)

    for nodeIterator.HasNext() {
        node := nodeIterator.Next()
        if !node.CheckAllocateConditions(candidate, false) {
            // skip the node if conditions can not be satisfied
            continue
        }
        ok, score := node.CheckResourceForAllocation(candidate.AllocatedResource)
        if ok {
            // return allocation (this is not a reservation)
            return NewSchedulingAllocation(candidate, node, Allocation)
        } else {
            // Record the so-far best node to reserve
            if score < bestScore {
                bestNodeToReserve = node
            }
        }
    }

    // Try to reserve on the node, if the best node to reserve is available.
    if bestNodeToReserve != nil {
        ok, err := m.precheckForReservation(candidate, bestNodeToReserve)
        if !ok {
            log.Logger().Debug("failed to reserve allocation on node",
                zap.String("error", err.Error()),
                zap.String("nodeId", bestNodeToReserve.NodeId),
                zap.String("app", candidate.ApplicationId),
                zap.String("allocKey", candidate.AskProto.AllocationKey))
            return nil
        }
        return NewSchedulingAllocation(candidate, bestNodeToReserve, Reservation)
    }
    // TODO: Need to fix the reservation logic here.

    return nil
}

func (m* SchedulingApplication) UpdateForAllocation(allocation *SchedulingAllocation) (bool, error) {
    m.lock.Lock()
    defer m.lock.Unlock()

    allocKey := allocation.SchedulingAsk.AskProto.AllocationKey

    if allocation.SchedulingAsk.PendingRepeatAsk <= 0 {
        return false, fmt.Errorf("trying to allocate for application, but the pending repeat ask is already <= 0, allocKey=%s", allocKey)
    }

    // When allocate from reservation, unreserve the request
    if allocation.AllocationResult == AllocationFromReservation && !m.internalUnreserveAllocation(allocation) {
        return false, fmt.Errorf("trying to allocate for application from reservation, but failed to unreserve request, allocKey=%s, " +
            "node=%s. It is possible the reservation is changed before arrived here.",
            allocKey, allocation.Node.NodeId)
    }

    if canAlloc := allocation.Node.UpdateForAllocation(allocation.SchedulingAsk, false); !canAlloc {
        return false, fmt.Errorf("trying to allocate for application, but the resource on the node is not available, allocKey=%s, node=%s",
            allocKey, allocation.Node.NodeId)
    }

    // before deciding on an allocation, call the reconcile plugin to sync scheduler cache
    // between core and shim if necessary. This is useful when running multiple allocations
    // in parallel and need to handle inter container affinity and anti-affinity.
    if rp := plugins.GetReconcilePlugin(); rp != nil {
        if err := rp.ReSyncSchedulerCache(&si.ReSyncSchedulerCacheArgs{
            AssumedAllocations: []*si.AssumedAllocation{
                {
                    AllocationKey: allocKey,
                    NodeId:        allocation.Node.NodeId,
                },
            },
        }); err != nil {
            log.Logger().Error("failed to sync cache",
                zap.Error(err))
        }
    }

    if allocation.AllocationResult == Allocation {
        // Update allocating resource only for new allocation; allocation from reservation already has allocating updated.
        m.allocating = resources.Add(m.allocating, allocation.SchedulingAsk.AllocatedResource)
    }
    m.Requests.updateAllocationAskRepeat(allocKey, -1)

    return true, nil
}

// Try to allocate from reservation, return !nil if any request got successfully allocated.
func (m *SchedulingApplication) TryAllocateFromReservationRequests() []*SchedulingAllocation {
    sortedReservationRequest := m.getSortedReservationRequestCopy()

    m.lock.RLock()
    defer m.lock.RUnlock()

    for _, request := range sortedReservationRequest {
        alloc := m.tryAllocateFromReservationRequest(request)
        if alloc != nil {
            return []*SchedulingAllocation{alloc}
        }
    }
    return nil
}

func (m *SchedulingApplication) tryAllocateFromReservationRequest(request *SchedulingAllocationAsk) *SchedulingAllocation {
    allocKey := request.AskProto.AllocationKey

    // Check if the reservation request is still valid
    if req := m.Requests.requests[allocKey]; req == nil || req.PendingRepeatAsk <= 0 {
        log.Logger().Debug("failed to allocate reservation request since we don't need it anymore",
            zap.String("allocKey", allocKey))
        return nil
    }

    // Loop nodes and allocate
    reservationNodeMap := m.reservedRequests[allocKey]
    if nil == reservationNodeMap || len(reservationNodeMap) == 0 {
        delete(m.reservedRequests, allocKey)

        log.Logger().Debug("failed to allocate reservation request since no more reservations",
            zap.String("allocKey", allocKey))
        return nil
    }

    for nodeId, reservationRequest := range reservationNodeMap {
        log.Logger().Debug("trying to allocate reservation for node",
            zap.String("allocKey", allocKey),
            zap.String("nodeId", nodeId))

        // TODO, add node reservation logic
        node := reservationRequest.SchedulingNode

        if !node.CheckAllocateConditions(reservationRequest.SchedulingAsk, true) {
            allocation := NewSchedulingAllocationFromReservationRequest(reservationRequest)
            allocation.AllocationResult = Unreserve
            return allocation
        }

        if ok, _ := node.CheckResourceForAllocation(reservationRequest.SchedulingAsk.AllocatedResource); ok {
            allocation := NewSchedulingAllocationFromReservationRequest(reservationRequest)
            allocation.AllocationResult = AllocationFromReservation
            return allocation
        }
    }

    // TODO, add swap-reservation logic
    return nil
}

func (m* SchedulingApplication) dropExcessiveReservationRequest() []*SchedulingAllocation {
    m.lock.RLock()
    defer m.lock.RUnlock()

    var excessiveReservations []*SchedulingAllocation = nil

    for allocKey, innerMap := range m.reservedRequests {
        // if reserved more than ask
        // No lock needed to access Requests.request, since it is already under lock.
        numToDrop := m.allocKeyToNumReservedRequests[allocKey] - int(m.Requests.requests[allocKey].PendingRepeatAsk)
        if numToDrop > 0 {
            for _, reservationRequest := range innerMap {
                nRequestToDrop := math.IntMin(numToDrop, reservationRequest.GetAmount())
                for i := 0; i < nRequestToDrop; i++ {
                    if nil == excessiveReservations {
                        excessiveReservations = make([]*SchedulingAllocation, 0)
                    }
                    unreserveAllocation := NewSchedulingAllocationFromReservationRequest(reservationRequest)
                    unreserveAllocation.NumAllocation = 1
                    unreserveAllocation.AllocationResult = Unreserve
                    excessiveReservations = append(excessiveReservations, unreserveAllocation)
                }
                numToDrop -= nRequestToDrop
                if numToDrop == 0 {
                    break
                }
            }
        }
    }

    return excessiveReservations
}

// Get sorted alloc keys of reserved requests based on priority
func (m *SchedulingApplication) getSortedReservationRequestCopy() []*SchedulingAllocationAsk {
    pendingAsks := make([]*SchedulingAllocationAsk, 0)

    m.lock.RLock()
    defer m.lock.RUnlock()

    for allocKey, valMap := range m.reservedRequests {
        ask := m.Requests.requests[allocKey]
        if ask != nil && len(valMap) > 0 {
            pendingAsks = append(pendingAsks, ask)
        }
    }

    SortAskRequestsByPriority(pendingAsks)

    return pendingAsks
}

// TODO: convert this as an interface.
func evaluateForSchedulingPolicy(nodes []*SchedulingNode, partitionContext *PartitionSchedulingContext) NodeIterator {
    // Sort Nodes based on the policy configured.
    configuredPolicy:= partitionContext.partition.GetNodeSortingPolicy()
    switch configuredPolicy {
    case common.BinPackingPolicy:
        SortNodes(nodes, MinAvailableResources)
        return NewDefaultNodeIterator(nodes)
    case common.FairnessPolicy:
        SortNodes(nodes, MaxAvailableResources)
        return NewDefaultNodeIterator(nodes)
    }
    return nil
}

func (m *SchedulingApplication) DecAllocatingResource(allocResource *resources.Resource) {
    m.lock.RLock()
    defer m.lock.RUnlock()

    m.allocating = resources.Sub(m.allocating, allocResource)
}

// Returns allocated + allocating
func (m* SchedulingApplication) GetTotalMayAllocated() *resources.Resource {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return resources.Add(m.allocating, m.ApplicationInfo.GetAllocatedResource())
}

// Increase allocating and its parent (ONLY USED BY TEST)
func (m* SchedulingApplication) IncreaseAllocatingAndParentQueuesTestOnly(allocatingDelta *resources.Resource) {
    m.queue.DecAllocatingResourceFromTheQueueAndParents(resources.Multiply(allocatingDelta, -1))

    m.lock.Lock()
    defer m.lock.Unlock()
    m.allocating = resources.Add(m.allocating, allocatingDelta)
}

func (m* SchedulingApplication) GetAllocatingResourceTestOnly() *resources.Resource {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.allocating
}

func (m* SchedulingApplication) precheckForReservation(ask *SchedulingAllocationAsk, node *SchedulingNode) (bool, error) {
    // Make sure we don't reserve more container than total repeat
    if int32(m.allocKeyToNumReservedRequests[ask.AskProto.AllocationKey]) >= ask.PendingRepeatAsk {
        return false, fmt.Errorf("Cannot reserve more since #reserved already >= #ask")
    }

    reserveRequest := NewReservedSchedulingRequest(ask, m, node)

    // Handle reservation on node
    ok, err := node.CanReserveOnNode(reserveRequest)
    if !ok {
        return false, err
    }
    return true, nil
}

// Reserve scheduling request, this also reserve resources on
func (m *SchedulingApplication) UpdateForReservation(allocation *SchedulingAllocation) (bool, error) {
    m.lock.Lock()
    defer m.lock.Unlock()

    reserveRequest := NewReservedSchedulingRequest(allocation.SchedulingAsk, m, allocation.Node)
    if ok, err := allocation.Node.UpdateForReservation(reserveRequest); !ok {
        return ok, err
    }

    m.addAppReservation(reserveRequest)

    m.allocating = resources.Add(m.allocating, allocation.SchedulingAsk.AllocatedResource)

    return true, nil
}

// Reserve scheduling request, this also reserve resources on
func (m *SchedulingApplication) UpdateForReservationCancellation(allocation *SchedulingAllocation) (bool, error) {
    m.lock.Lock()
    defer m.lock.Unlock()

    if !m.internalUnreserveAllocation(allocation) {
        return false, fmt.Errorf("Failed to unreserve alloc=%s, node=%s from app=%s", allocation.SchedulingAsk.AskProto.AllocationKey, allocation.Node.NodeId, m.ApplicationInfo.ApplicationId)
    }

    m.allocating = resources.Sub(m.allocating, allocation.SchedulingAsk.AllocatedResource)

    return true, nil
}

func (m* SchedulingApplication) addAppReservation(reservationRequest *ReservedSchedulingRequest) {
    allocKey := reservationRequest.SchedulingAsk.AskProto.AllocationKey

    reservationMap, exists := m.reservedRequests[allocKey]

    // Reserve for the app
    if !exists {
        reservationMap = make(map[string]*ReservedSchedulingRequest)
        m.reservedRequests[allocKey] = reservationMap
    }

    var req *ReservedSchedulingRequest = nil
    req, exists = reservationMap[reservationRequest.SchedulingNode.NodeId]

    if !exists {
        req = reservationRequest
        m.reservedRequests[allocKey][reservationRequest.SchedulingNode.NodeId] = reservationRequest
    } else {
        req.IncAmount(reservationRequest.GetAmount())
    }

    // updated allocKeyToNumReservationRequests
    existingNum := m.allocKeyToNumReservedRequests[allocKey]
    m.allocKeyToNumReservedRequests[allocKey] = existingNum + reservationRequest.GetAmount()
}

func (m* SchedulingApplication) removeAppReservation(reservationRequest *ReservedSchedulingRequest) {
    allocKey := reservationRequest.SchedulingAsk.AskProto.AllocationKey

    reservationMap, exists := m.reservedRequests[allocKey]

    // Reserve for the app
    if !exists {
        return
    }

    var req *ReservedSchedulingRequest = nil
    req, exists = reservationMap[reservationRequest.SchedulingNode.NodeId]

    if !exists {
        return
    }
    if amount, _ := req.DecAmount(reservationRequest.GetAmount()); amount <= 0 {
        delete(reservationMap, reservationRequest.SchedulingNode.NodeId)
        if len(m.reservedRequests[allocKey]) == 0 {
            delete(m.reservedRequests, allocKey)
        }
    }

    // updated allocKeyToNumReservationRequests
    existingNum := m.allocKeyToNumReservedRequests[allocKey]
    m.allocKeyToNumReservedRequests[allocKey] = existingNum - reservationRequest.GetAmount()
}

func (m* SchedulingApplication) internalUnreserveAllocation(allocation *SchedulingAllocation) bool {
    reservationRequest := NewReservedSchedulingRequest(allocation.SchedulingAsk, m, allocation.Node)

    allocation.Node.UnreserveOnNode(reservationRequest)

    m.removeAppReservation(reservationRequest)

    return true
}

// Only used by tests
func (m* SchedulingApplication) GetReservations() map[string]map[string]*ReservedSchedulingRequest {
    return m.reservedRequests
}

func (m* SchedulingApplication) GetAllReservationRequests() []*ReservedSchedulingRequest {
    m.lock.RLock()
    defer m.lock.RUnlock()

    requests := make([]*ReservedSchedulingRequest, 0)
    for _, v := range m.reservedRequests {
        for _, v := range v {
            requests = append(requests, v)
        }
    }

    return requests
}