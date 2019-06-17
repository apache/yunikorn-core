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
    "github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "sync"
)

type SchedulingAllocationAsk struct {
    // Original ask
    AskProto *si.AllocationAsk

    // Extracted info
    AllocatedResource  *resources.Resource
    PendingRepeatAsk   int32
    ApplicationId      string
    PartitionName      string
    NormalizedPriority int32

    // Lock
    lock sync.RWMutex
}

func NewSchedulingAllocationAsk(ask *si.AllocationAsk) *SchedulingAllocationAsk {
    return &SchedulingAllocationAsk{
        AskProto:          ask,
        AllocatedResource: resources.NewResourceFromProto(ask.ResourceAsk),
        PendingRepeatAsk:  ask.MaxAllocations,
        ApplicationId:     ask.ApplicationId,
        PartitionName:     ask.PartitionName,
        // TODO, normalize priority from ask
    }
}

// Add delta to pending ask,
//    if original_pending + delta >= 0, return true. And update internal pending ask.
//    If original_pending + delta < 0, return false and keep original_pending unchanged.
func (m* SchedulingAllocationAsk) AddPendingAskRepeat(delta int32) bool {
    m.lock.Lock()
    defer m.lock.Unlock()

    if m.PendingRepeatAsk + delta >= 0 {
        m.PendingRepeatAsk += delta
        return true
    } else {
        return false
    }
}
