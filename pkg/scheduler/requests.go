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
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
)

// Responsibility of this class:
// - Hold pending scheduling Requests.
// - Pre-aggregate scheduling Requests by pre-defined keys, and calculate pending resources
type AppSchedulingRequests struct {
    // AllocationKey -> allocationInfo
    requests                 map[string]*SchedulingAllocationAsk
    sortedRequestsByPriority []*SchedulingAllocationAsk
    totalPendingResource     *resources.Resource
    app                      *SchedulingApplication
}

func NewSchedulingRequests(app *SchedulingApplication) *AppSchedulingRequests {
    return &AppSchedulingRequests{
        requests:                 make(map[string]*SchedulingAllocationAsk),
        sortedRequestsByPriority: make([]*SchedulingAllocationAsk, 0),
        totalPendingResource:     resources.NewResource(),
        app:                      app,
    }
}

func (m*AppSchedulingRequests) GetPendingResource() *resources.Resource {
    m.app.lock.RLock()
    defer m.app.lock.RUnlock()

    return m.totalPendingResource
}

// This will be always called under lock of other function, so no additional lock needed.
func (m*AppSchedulingRequests) resortRequestByPriority() {
    m.sortedRequestsByPriority = make([]*SchedulingAllocationAsk, len(m.requests))
    idx := 0
    for _, value := range m.requests {
        m.sortedRequestsByPriority[idx] = value
        idx++
    }

    SortAskRequestsByPriority(m.sortedRequestsByPriority)
}

// Add new or replace
// Return delta of pending resource and error if anything bad happens.
func (m *AppSchedulingRequests) AddAllocationAsk(ask *SchedulingAllocationAsk) (*resources.Resource, error) {
    m.app.lock.Lock()
    defer m.app.lock.Unlock()

    deltaPendingResource := resources.MultiplyBy(ask.AllocatedResource, float64(ask.PendingRepeatAsk))

    var oldAskResource *resources.Resource = nil
    if oldAsk := m.requests[ask.AskProto.AllocationKey]; oldAsk != nil {
        oldAskResource = resources.MultiplyBy(oldAsk.AllocatedResource, float64(oldAsk.PendingRepeatAsk))
    }

    deltaPendingResource.SubFrom(oldAskResource)
    m.requests[ask.AskProto.AllocationKey] = ask

    // Update total pending resource
    m.totalPendingResource = resources.Add(m.totalPendingResource, deltaPendingResource)

    m.resortRequestByPriority()

    return deltaPendingResource, nil
}

// Update AllocationAsk #repeat, when delta > 0, increase repeat by delta, when delta < 0, decrease repeat by -delta
// This method should be called under app's lock, thus, no error check need to be done (external caller need to handle that)
// Return delta of updated pending resource, nil if failed
func (m *AppSchedulingRequests) updateAllocationAskRepeat(allocationKey string, delta int32) (*resources.Resource, error) {
    if ask := m.requests[allocationKey]; ask != nil {
        if ask.PendingRepeatAsk + delta < 0 {
            return nil, fmt.Errorf("pending repeat ask for allocation_ID=%s will be negative after update, allocationKey=%s", ask.ApplicationId, allocationKey)
        }

        deltaPendingResource := resources.MultiplyBy(ask.AllocatedResource, float64(delta))
        m.totalPendingResource = resources.Add(m.totalPendingResource, deltaPendingResource)
        ask.AddPendingAskRepeat(delta)

        return deltaPendingResource, nil
    }

    return nil, fmt.Errorf("couldn't find pending request for allocation_ID=%s, allocationKey=%s", m.app.ApplicationInfo.ApplicationId, allocationKey)
}

// Remove allocation ask by key.
// returns (change of pending resource, ask), return (nil, nil) if key cannot be found
func (m *AppSchedulingRequests) RemoveAllocationAsk(allocationKey string) (*resources.Resource, *SchedulingAllocationAsk) {
    // when allocation key not specified, return cleanup all allocation ask
    if allocationKey == "" {
        return m.CleanupAllocationAsks(), nil
    }

    m.app.lock.Lock()
    defer m.app.lock.Unlock()

    if ask := m.requests[allocationKey]; ask != nil {
        deltaPendingResource := resources.MultiplyBy(ask.AllocatedResource, -float64(ask.PendingRepeatAsk))
        m.totalPendingResource = resources.Add(m.totalPendingResource, deltaPendingResource)
        delete(m.requests, allocationKey)

        m.resortRequestByPriority()

        return deltaPendingResource, ask
    }

    return nil, nil
}

// Remove all allocation asks
// returns (change of pending resource), when no asks, return nil
func (m *AppSchedulingRequests) CleanupAllocationAsks() *resources.Resource {
    m.app.lock.Lock()
    defer m.app.lock.Unlock()

    var deltaPendingResource *resources.Resource = nil
    for _, ask := range m.requests {
        if deltaPendingResource == nil {
            deltaPendingResource = resources.NewResource()
        }
        deltaPendingResource = resources.Sub(deltaPendingResource, resources.MultiplyBy(ask.AllocatedResource, float64(ask.PendingRepeatAsk)))
    }

    // Cleanup total pending resource
    m.totalPendingResource = resources.NewResource()
    m.requests = make(map[string]*SchedulingAllocationAsk)

    return deltaPendingResource
}

func (m *AppSchedulingRequests) GetSchedulingAllocationAsk(allocationKey string) *SchedulingAllocationAsk {
    m.app.lock.RLock()
    defer m.app.lock.RUnlock()

    return m.requests[allocationKey]
}
