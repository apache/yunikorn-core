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

package objects

import (
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type AllocationAsk struct {
	// Extracted info
	AllocationKey     string
	AllocatedResource *resources.Resource
	ApplicationID     string
	PartitionName     string
	QueueName         string
	Tags              map[string]string

	// Private fields need protection
	createTime       time.Time // the time this ask was created (used in reservations)
	priority         int32
	PendingRepeatAsk int32
	maxAllocations   int32

	sync.RWMutex
}

func NewAllocationAsk(ask *si.AllocationAsk) *AllocationAsk {
	saa := &AllocationAsk{
		AllocationKey:     ask.AllocationKey,
		AllocatedResource: resources.NewResourceFromProto(ask.ResourceAsk),
		PendingRepeatAsk:  ask.MaxAllocations,
		maxAllocations:    ask.MaxAllocations,
		ApplicationID:     ask.ApplicationID,
		PartitionName:     ask.PartitionName,
		Tags:              ask.Tags,
		createTime:        time.Now(),
	}
	saa.priority = saa.normalizePriority(ask.Priority)
	return saa
}

// Update pending ask repeat with the delta given.
// Update the pending ask repeat counter with the delta (pos or neg). The pending repeat is always 0 or higher.
// If the update would cause the repeat to go negative the update is discarded and false is returned.
// In all other cases the repeat is updated and true is returned.
func (saa *AllocationAsk) UpdatePendingAskRepeat(delta int32) bool {
	saa.Lock()
	defer saa.Unlock()

	if saa.PendingRepeatAsk+delta >= 0 {
		saa.PendingRepeatAsk += delta
		return true
	}
	return false
}

// Get the pending ask repeat
func (saa *AllocationAsk) GetPendingAskRepeat() int32 {
	saa.RLock()
	defer saa.RUnlock()

	return saa.PendingRepeatAsk
}

// Return the time this ask was created
// Should be treated as read only not te be modified
func (saa *AllocationAsk) GetCreateTime() time.Time {
	return saa.createTime
}

// Normalised priority
// Currently a direct conversion.
func (saa *AllocationAsk) normalizePriority(priority *si.Priority) int32 {
	// TODO, really normalize priority from ask
	return priority.GetPriorityValue()
}
