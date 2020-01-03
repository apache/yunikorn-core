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
    "github.com/cloudera/yunikorn-core/pkg/cache"
    "github.com/cloudera/yunikorn-core/pkg/common"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "github.com/cloudera/yunikorn-core/pkg/plugins"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "go.uber.org/zap"
    "math"
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

    lock sync.RWMutex
}

func NewSchedulingApplication(appInfo *cache.ApplicationInfo) *SchedulingApplication {
    app := &SchedulingApplication{
        ApplicationInfo: appInfo,
        allocating: resources.NewResource(),
        reservedRequests: make(map[string]map[string]*ReservedSchedulingRequest),
    }
    app.Requests = NewSchedulingRequests(app)
    return app
}

// sort scheduling Requests from a job
func (app *SchedulingApplication) tryAllocate(partitionContext *PartitionSchedulingContext,
    headroom *resources.Resource) *SchedulingAllocation {
    app.lock.Lock()
    defer app.lock.Unlock()

    for _, request := range app.Requests.sortedRequestsByPriority {
        if request.PendingRepeatAsk > 0 && resources.FitIn(headroom, request.AllocatedResource) {
            if allocation := app.allocateForOneRequest(partitionContext, request); allocation != nil {
                app.allocating = resources.Add(app.allocating, allocation.SchedulingAsk.AllocatedResource)

                if allocation.AllocationResult == Allocation {
                    app.Requests.updateAllocationAskRepeat(request.AskProto.AllocationKey, -1)
                }
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
    nodeList := partitionContext.getSchedulingNodes()
    if len(nodeList) == 0 {
        return nil
    }

    nodeIterator := evaluateForSchedulingPolicy(nodeList, partitionContext)

    var bestNodeToReserve *SchedulingNode = nil
    bestScore := math.Inf(+1)

    for nodeIterator.HasNext() {
        node := nodeIterator.Next()
        if !node.CheckAllocateConditions(candidate.AskProto.AllocationKey, false) {
            // skip the node if conditions can not be satisfied
            continue
        }
        ok, score := node.CheckAndAllocateResource(candidate.AllocatedResource, false)
        if ok {
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
        ok, err := m.reserveSchedulingAllocation(candidate, bestNodeToReserve)
        if !ok {
            log.Logger().Debug("failed to reserve allocation on node",
                zap.String("error", err.Error()))
            return nil
        }
        return NewSchedulingAllocation(candidate, bestNodeToReserve, Reservation)
    }
    // TODO: Need to fix the reservation logic here.

    return nil
}

// Try to allocate from reservation, return !nil if any request got successfully allocated.
func (m *SchedulingApplication) TryAllocateFromReservation() *SchedulingAllocation {
    sortedReservationRequest := m.getSortedReservationRequestCopy()

    for _, request := range sortedReservationRequest {
        alloc := m.tryAllocateFromReservationRequest(request)
        if alloc != nil {
            // When alloc != nil, it means either allocation from reservation, or unreserve, for both case, unreserve the node
            m.unreserveSchedulingAllocation(alloc)
            if alloc.AllocationResult == AllocationFromReservation {
                return alloc
            }
        }
    }
    return nil
}

func (m *SchedulingApplication) tryAllocateFromReservationRequest(request *SchedulingAllocationAsk) *SchedulingAllocation {
    m.lock.Lock()
    defer m.lock.Unlock()

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

        if !node.CheckAllocateConditions(allocKey, true) {
            allocation := NewSchedulingAllocationFromReservationRequest(reservationRequest)
            allocation.AllocationResult = Unreserve
            return allocation
        }

        if ok, _ := node.CheckAndAllocateResource(reservationRequest.SchedulingAsk.AllocatedResource, false); ok {
            allocation := NewSchedulingAllocationFromReservationRequest(reservationRequest)
            allocation.AllocationResult = AllocationFromReservation
        }
    }

    // TODO, add swap-reservation logic
    return nil
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

// Reserve scheduling request, this also reserve resources on
func (m *SchedulingApplication) reserveSchedulingAllocation(ask *SchedulingAllocationAsk, node *SchedulingNode) (bool, error) {
    reserveRequest := NewReservedSchedulingRequest(ask, m, node)

    m.lock.Lock()
    defer m.lock.Unlock()

    // Handle reservation on node
    ok, err := node.ReserveOnNode(reserveRequest)
    if !ok {
        return false, err
    }

    m.addAppReservation(reserveRequest)

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
    }
    req.IncAmount(reservationRequest.GetAmount())
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
    req.DecAmount(reservationRequest.GetAmount())
}

func (m* SchedulingApplication) unreserveSchedulingAllocation(allocation *SchedulingAllocation) bool {
    m.lock.Lock()
    defer m.lock.Unlock()

    reservationRequest := NewReservedSchedulingRequest(allocation.SchedulingAsk, m, allocation.Node)

    allocation.Node.UnreserveOnNode(reservationRequest)

    m.removeAppReservation(reservationRequest)

    return true
}