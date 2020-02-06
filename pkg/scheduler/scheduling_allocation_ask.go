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
	"sync"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type SchedulingAllocationAsk struct {
	// Original ask
	AskProto *si.AllocationAsk

	// Extracted info
	AllocatedResource  *resources.Resource
	PendingRepeatAsk   int32
	ApplicationID      string
	PartitionName      string
	NormalizedPriority int32
	QueueName          string

	// Lock
	lock sync.RWMutex
}

func NewSchedulingAllocationAsk(ask *si.AllocationAsk) *SchedulingAllocationAsk {
	return &SchedulingAllocationAsk{
		AskProto:          ask,
		AllocatedResource: resources.NewResourceFromProto(ask.ResourceAsk),
		PendingRepeatAsk:  ask.MaxAllocations,
		ApplicationID:     ask.ApplicationID,
		PartitionName:     ask.PartitionName,
		// TODO, normalize priority from ask
	}
}

func ConvertFromAllocation(allocation *si.Allocation, rmID string) *SchedulingAllocationAsk {
	partitionWithRMId := common.GetNormalizedPartitionName(allocation.PartitionName, rmID)
	return &SchedulingAllocationAsk{
		AskProto: &si.AllocationAsk{
			AllocationKey:  allocation.AllocationKey,
			ResourceAsk:    allocation.ResourcePerAlloc,
			Tags:           allocation.AllocationTags,
			Priority:       allocation.Priority,
			MaxAllocations: 1,
			ApplicationID:  allocation.ApplicationID,
			PartitionName:  partitionWithRMId,
		},
		QueueName:         allocation.QueueName,
		AllocatedResource: resources.NewResourceFromProto(allocation.ResourcePerAlloc),
		PendingRepeatAsk:  1,
		ApplicationID:     allocation.ApplicationID,
		PartitionName:     partitionWithRMId,
	}
}

// Add delta to pending ask,
//    if original_pending + delta >= 0, return true. And update internal pending ask.
//    If original_pending + delta < 0, return false and keep original_pending unchanged.
func (m *SchedulingAllocationAsk) AddPendingAskRepeat(delta int32) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.PendingRepeatAsk+delta >= 0 {
		m.PendingRepeatAsk += delta
		return true
	}
	return false
}
