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

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type AllocationAsk struct {
	// Read-only fields
	allocationKey     string
	applicationID     string
	partitionName     string
	taskGroupName     string        // task group this allocation ask belongs to
	placeholder       bool          // is this a placeholder allocation ask
	execTimeout       time.Duration // execTimeout for the allocation ask
	createTime        time.Time     // the time this ask was created (used in reservations)
	priority          int32
	maxAllocations    int32
	requiredNode      string
	allowPreemptSelf  bool
	allowPreemptOther bool
	originator        bool
	tags              map[string]string
	allocatedResource *resources.Resource

	// Mutable fields which need protection
	pendingAskRepeat    int32
	allocLog            map[string]*AllocationLogEntry
	preemptionTriggered bool
	preemptCheckTime    time.Time

	sync.RWMutex
}

type AllocationLogEntry struct {
	Message        string
	LastOccurrence time.Time
	Count          int32
}

func NewAllocationAsk(allocationKey string, applicationID string, allocatedResource *resources.Resource) *AllocationAsk {
	return &AllocationAsk{
		allocationKey:     allocationKey,
		applicationID:     applicationID,
		allocatedResource: allocatedResource,
		allocLog:          make(map[string]*AllocationLogEntry),
	}
}

func NewAllocationAskFromSI(ask *si.AllocationAsk) *AllocationAsk {
	saa := &AllocationAsk{
		allocationKey:     ask.AllocationKey,
		allocatedResource: resources.NewResourceFromProto(ask.ResourceAsk),
		pendingAskRepeat:  ask.MaxAllocations,
		maxAllocations:    ask.MaxAllocations,
		applicationID:     ask.ApplicationID,
		partitionName:     ask.PartitionName,

		tags:              CloneAllocationTags(ask.Tags),
		createTime:        time.Now(),
		priority:          ask.Priority,
		execTimeout:       common.ConvertSITimeout(ask.ExecutionTimeoutMilliSeconds),
		placeholder:       ask.Placeholder,
		taskGroupName:     ask.TaskGroupName,
		requiredNode:      common.GetRequiredNodeFromTag(ask.Tags),
		allowPreemptSelf:  common.IsAllowPreemptSelf(ask.PreemptionPolicy),
		allowPreemptOther: common.IsAllowPreemptOther(ask.PreemptionPolicy),
		originator:        ask.Originator,
		allocLog:          make(map[string]*AllocationLogEntry),
	}
	// this is a safety check placeholder and task group name must be set as a combo
	// order is important as task group can be set without placeholder but not the other way around
	if saa.placeholder && saa.taskGroupName == "" {
		log.Logger().Debug("ask cannot be a placeholder without a TaskGroupName",
			zap.Stringer("SI ask", ask))
		return nil
	}
	return saa
}

func (aa *AllocationAsk) String() string {
	if aa == nil {
		return "ask is nil"
	}
	return fmt.Sprintf("allocationKey %s, applicationID %s, Resource %s, PendingRepeats %d", aa.allocationKey, aa.applicationID, aa.allocatedResource, aa.GetPendingAskRepeat())
}

// GetAllocationKey returns the allocation key for this ask
func (aa *AllocationAsk) GetAllocationKey() string {
	return aa.allocationKey
}

// GetApplicationID returns the application ID for this ask
func (aa *AllocationAsk) GetApplicationID() string {
	return aa.applicationID
}

// GetPartitionName returns the partition name for this ask
func (aa *AllocationAsk) GetPartitionName() string {
	return aa.partitionName
}

// updatePendingAskRepeat updates the pending ask repeat with the delta given.
// Update the pending ask repeat counter with the delta (pos or neg). The pending repeat is always 0 or higher.
// If the update would cause the repeat to go negative the update is discarded and false is returned.
// In all other cases the repeat is updated and true is returned.
func (aa *AllocationAsk) updatePendingAskRepeat(delta int32) bool {
	aa.Lock()
	defer aa.Unlock()

	if aa.pendingAskRepeat+delta >= 0 {
		aa.pendingAskRepeat += delta
		return true
	}
	return false
}

// GetPendingAskRepeat gets the number of repeat asks remaining
func (aa *AllocationAsk) GetPendingAskRepeat() int32 {
	aa.RLock()
	defer aa.RUnlock()
	return aa.pendingAskRepeat
}

// GetCreateTime returns the time this ask was created
func (aa *AllocationAsk) GetCreateTime() time.Time {
	return aa.createTime
}

// GetPreemptCheckTime returns the time this ask was last evaluated for preemption
func (aa *AllocationAsk) GetPreemptCheckTime() time.Time {
	aa.RLock()
	defer aa.RUnlock()
	return aa.preemptCheckTime
}

// UpdatePreemptCheckTime is used to mark when this ask is evaluated for preemption
func (aa *AllocationAsk) UpdatePreemptCheckTime() {
	aa.Lock()
	defer aa.Unlock()
	aa.preemptCheckTime = time.Now()
}

// IsPlaceholder returns whether this ask represents a placeholder
func (aa *AllocationAsk) IsPlaceholder() bool {
	return aa.placeholder
}

// GetTaskGroup returns the task group name for this ask
func (aa *AllocationAsk) GetTaskGroup() string {
	return aa.taskGroupName
}

// GetTimeout returns the timeout for this ask
func (aa *AllocationAsk) GetTimeout() time.Duration {
	return aa.execTimeout
}

// GetRequiredNode gets the node (if any) required by this ask.
func (aa *AllocationAsk) GetRequiredNode() string {
	return aa.requiredNode
}

// SetRequiredNode sets the required node (used only by testing so lock is not taken)
func (aa *AllocationAsk) SetRequiredNode(node string) {
	aa.requiredNode = node
}

// IsAllowPreemptSelf returns whether preemption is allowed for this ask
func (aa *AllocationAsk) IsAllowPreemptSelf() bool {
	return aa.allowPreemptSelf
}

// IsAllowPreemptOther returns whether this ask is allowed to preempt others
func (aa *AllocationAsk) IsAllowPreemptOther() bool {
	return aa.allowPreemptOther
}

// IsOriginator returns whether this ask is the originator for the application
func (aa *AllocationAsk) IsOriginator() bool {
	return aa.originator
}

// GetPriority returns the priority of this ask
func (aa *AllocationAsk) GetPriority() int32 {
	return aa.priority
}

// GetAllocatedResource returns a reference to the allocated resources for this ask. This must be treated as read-only.
func (aa *AllocationAsk) GetAllocatedResource() *resources.Resource {
	return aa.allocatedResource
}

// GetTag returns the value of a named tag or an empty string if not present
func (aa *AllocationAsk) GetTag(tagName string) string {
	result, ok := aa.tags[tagName]
	if !ok {
		return ""
	}
	return result
}

// GetTagsClone returns the copy of the tags for this ask
func (aa *AllocationAsk) GetTagsClone() map[string]string {
	return CloneAllocationTags(aa.tags)
}

// LogAllocationFailure keeps track of preconditions not being met for an allocation
func (aa *AllocationAsk) LogAllocationFailure(message string, allocate bool) {
	// for now, don't log reservations
	if !allocate {
		return
	}

	aa.Lock()
	defer aa.Unlock()

	entry, ok := aa.allocLog[message]
	if !ok {
		entry = &AllocationLogEntry{
			Message: message,
		}
		aa.allocLog[message] = entry
	}
	entry.LastOccurrence = time.Now()
	entry.Count++
}

// GetAllocationLog returns a list of log entries corresponding to allocation preconditions not being met
func (aa *AllocationAsk) GetAllocationLog() []*AllocationLogEntry {
	aa.RLock()
	defer aa.RUnlock()

	res := make([]*AllocationLogEntry, len(aa.allocLog))
	i := 0
	for _, entry := range aa.allocLog {
		res[i] = &AllocationLogEntry{
			Message:        entry.Message,
			LastOccurrence: entry.LastOccurrence,
			Count:          entry.Count,
		}
		i++
	}
	return res
}

// MarkTriggeredPreemption marks the current ask because it triggered preemption during allocation
func (aa *AllocationAsk) MarkTriggeredPreemption() {
	aa.Lock()
	defer aa.Unlock()
	aa.preemptionTriggered = true
}

// HasTriggeredPreemption returns whether this ask has triggered preemption
func (aa *AllocationAsk) HasTriggeredPreemption() bool {
	aa.RLock()
	defer aa.RUnlock()
	return aa.preemptionTriggered
}
