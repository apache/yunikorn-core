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
    "errors"
    "fmt"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "sync"
)

// Responsibility of this class:
// - Hold pending scheduling Requests.
// - Pre-aggregate scheduling Requests by pre-defined keys, and calculate pending resources
type SchedulingRequests struct {
    // AllocationKey -> allocationInfo
    requests             map[string]*SchedulingAllocationAsk
    TotalPendingResource *resources.Resource

    lock sync.RWMutex
}

func NewSchedulingRequests() *SchedulingRequests {
    return &SchedulingRequests{
        requests:             make(map[string]*SchedulingAllocationAsk),
        TotalPendingResource: resources.NewResource(),
    }
}

// Add new or replace
// Return delta of pending resource and error if anything bad happens.
func (m *SchedulingRequests) AddAllocationAsk(ask *SchedulingAllocationAsk) (*resources.Resource, error) {
    m.lock.Lock()
    defer m.lock.Unlock()

    deltaPendingResource := resources.MultiplyBy(ask.AllocatedResource, float64(ask.PendingRepeatAsk))

    var oldAskResource *resources.Resource = nil
    if oldAsk := m.requests[ask.AskProto.AllocationKey]; oldAsk != nil {
        oldAskResource = resources.MultiplyBy(oldAsk.AllocatedResource, float64(oldAsk.PendingRepeatAsk))
    }

    if nil != oldAskResource {
        resources.SubFrom(deltaPendingResource, oldAskResource)
    }
    m.requests[ask.AskProto.AllocationKey] = ask

    // Update total pending resource
    m.TotalPendingResource = resources.Add(m.TotalPendingResource, deltaPendingResource)

    return deltaPendingResource, nil
}

// Update AllocationAsk #repeat, when delta > 0, increase repeat by delta, when delta < 0, decrease repeat by -delta
// Returns error when allocationKey doesn't exist, or illegal delta specified.
// Return change of pending resources, it will be used to update queues, jobs, etc.
func (m *SchedulingRequests) UpdateAllocationAskRepeat(allocationKey string, delta int32) (*resources.Resource, error) {
    m.lock.Lock()
    defer m.lock.Unlock()

    if ask := m.requests[allocationKey]; ask != nil {
        if ask.PendingRepeatAsk+delta < 0 {
            return nil, errors.New(fmt.Sprintf("Trying to decrease number of allocation for key=%s, under zero, please double check", allocationKey))
        }

        deltaPendingResource := resources.MultiplyBy(ask.AllocatedResource, float64(delta))
        m.TotalPendingResource = resources.Add(m.TotalPendingResource, deltaPendingResource)
        ask.AddPendingAskRepeat(delta)

        return deltaPendingResource, nil
    }
    return nil, errors.New(fmt.Sprintf("Failed to locate request with key=%s", allocationKey))
}

// Remove allocation ask by key.
// returns (change of pending resource, ask), return (nil, nil) if key cannot be found
func (m *SchedulingRequests) RemoveAllocationAsk(allocationKey string) (*resources.Resource, *SchedulingAllocationAsk) {
    // when allocation key not specified, return cleanup all allocation ask
    if allocationKey == "" {
        return m.CleanupAllocationAsks(), nil
    }

    m.lock.Lock()
    defer m.lock.Unlock()

    if ask := m.requests[allocationKey]; ask != nil {
        deltaPendingResource := resources.MultiplyBy(ask.AllocatedResource, -float64(ask.PendingRepeatAsk))
        m.TotalPendingResource = resources.Add(m.TotalPendingResource, deltaPendingResource)
        delete(m.requests, allocationKey)
        return deltaPendingResource, ask
    }

    return nil, nil
}

// Remove all allocation asks
// returns (change of pending resource), when no asks, return nil
func (m *SchedulingRequests) CleanupAllocationAsks() *resources.Resource {
    m.lock.Lock()
    defer m.lock.Unlock()

    var deltaPendingResource *resources.Resource = nil
    for _, ask := range m.requests {
        if deltaPendingResource == nil {
            deltaPendingResource = resources.NewResource()
        }
        deltaPendingResource = resources.Sub(deltaPendingResource, resources.MultiplyBy(ask.AllocatedResource, float64(ask.PendingRepeatAsk)))
    }

    // Cleanup total pending resource
    m.TotalPendingResource = resources.NewResource()
    m.requests = make(map[string]*SchedulingAllocationAsk)

    return deltaPendingResource
}

func (m *SchedulingRequests) GetSchedulingAllocationAsk(allocationKey string) *SchedulingAllocationAsk {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.requests[allocationKey]
}
