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

                // Decrease pending request by 1
                if !allocation.Reservation {
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

    for nodeIterator.HasNext() {
        node := nodeIterator.Next()
        if !node.CheckAllocateConditions(candidate.AskProto.AllocationKey) {
            // skip the node if conditions can not be satisfied
            continue
        }
        if node.CheckAndAllocateResource(candidate.AllocatedResource, false) {
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
            return NewSchedulingAllocation(candidate, node, m, false)
        } else {
            // Try to reserve on the node
            ok, err := m.reserveSchedulingAllocation(candidate, node)
            if !ok {
                log.Logger().Debug("failed to reserve allocation on node",
                    zap.String("error", err.Error()))
                return nil
            }

            return NewSchedulingAllocation(candidate, node, m, true)
        }
    }

    // TODO: Need to fix the reservation logic here.

    return nil
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
    allocKey := ask.AskProto.AllocationKey
    reserveRequest := NewReservedSchedulingRequest(ask, m, node)

    m.lock.Lock()
    defer m.lock.Unlock()

    // Handle reservation on node
    ok, err := node.ReserveOnNode(reserveRequest)
    if !ok {
        return false, err
    }

    // Reserve for the app
    // No need to check if the reservation the key on the node existed or not if the ReserveOnNode succeeded
    if _, ok := m.reservedRequests[allocKey]; !ok {
        m.reservedRequests[allocKey] = make(map[string]*ReservedSchedulingRequest)
    }

    m.reservedRequests[allocKey][reserveRequest.SchedulingNode.NodeId] = reserveRequest

    return true, nil
}