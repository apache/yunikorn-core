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
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type AllocationAsk struct {
	// Extracted info
	AllocationKey     string
	AllocatedResource *resources.Resource
	ApplicationID     string
	PartitionName     string
	QueueName         string // CLEANUP: why do we need this? the app is linked to the queue
	Tags              map[string]string

	// Private fields need protection
	taskGroupName    string        // task group this allocation ask belongs to
	placeholder      bool          // is this a placeholder allocation ask
	execTimeout      time.Duration // execTimeout for the allocation ask
	pendingRepeatAsk int32
	createTime       time.Time // the time this ask was created (used in reservations)
	priority         int32
	maxAllocations   int32

	sync.RWMutex
}

func NewAllocationAsk(ask *si.AllocationAsk) *AllocationAsk {
	saa := &AllocationAsk{
		AllocationKey:     ask.AllocationKey,
		AllocatedResource: resources.NewResourceFromProto(ask.ResourceAsk),
		pendingRepeatAsk:  ask.MaxAllocations,
		maxAllocations:    ask.MaxAllocations,
		ApplicationID:     ask.ApplicationID,
		PartitionName:     ask.PartitionName,
		Tags:              ask.Tags,
		createTime:        time.Now(),
		execTimeout:       common.ConvertSITimeout(ask.ExecutionTimeoutMilliSeconds),
		placeholder:       ask.Placeholder,
		taskGroupName:     ask.TaskGroupName,
	}
	saa.priority = saa.normalizePriority(ask.Priority)
	// this is a safety check placeholder and task group name must be set as a combo
	// order is important as task group can be set without placeholder but not the other way around
	if saa.placeholder && saa.taskGroupName == "" {
		log.Logger().Debug("Ask cannot be a placeholder without a TaskGroupName",
			zap.String("SI ask", ask.String()))
		return nil
	}
	return saa
}

func (aa *AllocationAsk) String() string {
	if aa == nil {
		return "ask is nil"
	}
	return fmt.Sprintf("AllocationKey %s, ApplicationID %s, Resource %s, PendingRepeats %d", aa.AllocationKey, aa.ApplicationID, aa.AllocatedResource, aa.pendingRepeatAsk)
}

// Update pending ask repeat with the delta given.
// Update the pending ask repeat counter with the delta (pos or neg). The pending repeat is always 0 or higher.
// If the update would cause the repeat to go negative the update is discarded and false is returned.
// In all other cases the repeat is updated and true is returned.
func (aa *AllocationAsk) updatePendingAskRepeat(delta int32) bool {
	aa.Lock()
	defer aa.Unlock()

	if aa.pendingRepeatAsk+delta >= 0 {
		aa.pendingRepeatAsk += delta
		return true
	}
	return false
}

// Get the pending ask repeat
func (aa *AllocationAsk) GetPendingAskRepeat() int32 {
	aa.RLock()
	defer aa.RUnlock()
	return aa.pendingRepeatAsk
}

// Return the time this ask was created
// Should be treated as read only not te be modified
func (aa *AllocationAsk) GetCreateTime() time.Time {
	aa.RLock()
	defer aa.RUnlock()
	return aa.createTime
}

// Set the queue name after it is added to the application
func (aa *AllocationAsk) setQueue(queueName string) {
	aa.Lock()
	defer aa.Unlock()
	aa.QueueName = queueName
}

// Normalised priority set on create only
// Currently a direct conversion.
func (aa *AllocationAsk) normalizePriority(priority *si.Priority) int32 {
	// TODO, really normalize priority from ask
	return priority.GetPriorityValue()
}

// Set the priority after it is created to the application
func (aa *AllocationAsk) setPriority(prio int32) {
	aa.Lock()
	defer aa.Unlock()
	aa.priority = prio
}

func (aa *AllocationAsk) isPlaceholder() bool {
	aa.Lock()
	defer aa.Unlock()
	return aa.placeholder
}

func (aa *AllocationAsk) getTaskGroup() string {
	aa.Lock()
	defer aa.Unlock()
	return aa.taskGroupName
}

func (aa *AllocationAsk) getTimeout() time.Duration {
	aa.Lock()
	defer aa.Unlock()
	return aa.execTimeout
}
