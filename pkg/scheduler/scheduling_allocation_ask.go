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
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type schedulingAllocationAsk struct {
	// Original ask
	AskProto *si.AllocationAsk

	// Extracted info
	AllocatedResource *resources.Resource
	ApplicationID     string
	PartitionName     string
	QueueName         string

	// Private fields need protection
	createTime       time.Time // the time this ask was created (used in reservations)
	priority         int32
	pendingRepeatAsk int32
	upscaleRequested bool

	sync.RWMutex
}

func newSchedulingAllocationAsk(ask *si.AllocationAsk) *schedulingAllocationAsk {
	saa := &schedulingAllocationAsk{
		AskProto:          ask,
		AllocatedResource: resources.NewResourceFromProto(ask.ResourceAsk),
		pendingRepeatAsk:  ask.MaxAllocations,
		ApplicationID:     ask.ApplicationID,
		PartitionName:     ask.PartitionName,
		createTime:        time.Now(),
	}
	saa.priority = saa.normalizePriority(ask.Priority)
	return saa
}

func convertFromAllocation(allocation *si.Allocation, rmID string) *schedulingAllocationAsk {
	partitionWithRMId := common.GetNormalizedPartitionName(allocation.PartitionName, rmID)
	return &schedulingAllocationAsk{
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
		ApplicationID:     allocation.ApplicationID,
		PartitionName:     partitionWithRMId,
		pendingRepeatAsk:  1,
		createTime:        time.Now(),
	}
}

// Update pending ask repeat with the delta given.
// Update the pending ask repeat counter with the delta (pos or neg). The pending repeat is always 0 or higher.
// If the update would cause the repeat to go negative the update is discarded and false is returned.
// In all other cases the repeat is updated and true is returned.
func (saa *schedulingAllocationAsk) updatePendingAskRepeat(delta int32) bool {
	saa.Lock()
	defer saa.Unlock()

	if saa.pendingRepeatAsk+delta >= 0 {
		saa.pendingRepeatAsk += delta
		return true
	}
	return false
}

// Get the pending ask repeat
func (saa *schedulingAllocationAsk) getPendingAskRepeat() int32 {
	saa.RLock()
	defer saa.RUnlock()

	return saa.pendingRepeatAsk
}

// Set the fact that the upscale was requested
func (saa *schedulingAllocationAsk) setUpscaleRequested(val bool) {
	saa.Lock()
	defer saa.Unlock()
	saa.upscaleRequested = val
}

// Get the fact that the upscale was requested
// visible for testing
func (saa *schedulingAllocationAsk) GetUpscaleRequested() bool {
	saa.RLock()
	defer saa.RUnlock()

	return saa.upscaleRequested
}

// Return the time this ask was created
// Should be treated as read only not te be modified
func (saa *schedulingAllocationAsk) getCreateTime() time.Time {
	return saa.createTime
}

// Normalised priority
// Currently a direct conversion.
func (saa *schedulingAllocationAsk) normalizePriority(priority *si.Priority) int32 {
	// TODO, really normalize priority from ask
	return priority.GetPriorityValue()
}
