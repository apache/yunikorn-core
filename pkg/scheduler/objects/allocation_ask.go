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
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/log"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type AllocationAsk struct {
	// Read-only fields
	allocationKey     string
	applicationID     string
	taskGroupName     string    // task group this allocation ask belongs to
	placeholder       bool      // is this a placeholder allocation ask
	createTime        time.Time // the time this ask was created (used in reservations)
	priority          int32
	requiredNode      string
	allowPreemptSelf  bool
	allowPreemptOther bool
	originator        bool
	tags              map[string]string
	allocatedResource *resources.Resource
	resKeyWithoutNode string // the reservation key without node

	// Mutable fields which need protection
	allocated           bool
	allocLog            map[string]*AllocationLogEntry
	preemptionTriggered bool
	preemptCheckTime    time.Time
	schedulingAttempted bool              // whether scheduler core has tried to schedule this ask
	scaleUpTriggered    bool              // whether this ask has triggered autoscaling or not
	resKeyPerNode       map[string]string // reservation key for a given node

	askEvents            *askEvents
	userQuotaCheckFailed bool
	headroomCheckFailed  bool

	locking.RWMutex
}

type AllocationLogEntry struct {
	Message        string
	LastOccurrence time.Time
	Count          int32
}

func NewAllocationAsk(allocationKey string, applicationID string, allocatedResource *resources.Resource) *AllocationAsk {
	aa := &AllocationAsk{
		allocationKey:     allocationKey,
		applicationID:     applicationID,
		allocatedResource: allocatedResource,
		allocLog:          make(map[string]*AllocationLogEntry),
		resKeyPerNode:     make(map[string]string),
		askEvents:         newAskEvents(events.GetEventSystem()),
	}
	aa.resKeyWithoutNode = reservationKeyWithoutNode(applicationID, allocationKey)
	return aa
}

func NewAllocationAskFromSI(ask *si.AllocationAsk) *AllocationAsk {

	var createTime time.Time
	siCreationTime, err := strconv.ParseInt(ask.Tags[siCommon.CreationTime], 10, 64)
	if err != nil {
		log.Log(log.SchedAllocation).Debug("CreationTime is not set on the AllocationAsk object or invalid",
			zap.String("creationTime", ask.Tags[siCommon.CreationTime]))
		createTime = time.Now()
	} else {
		createTime = time.Unix(siCreationTime, 0)
	}

	saa := &AllocationAsk{
		allocationKey:     ask.AllocationKey,
		allocatedResource: resources.NewResourceFromProto(ask.ResourceAsk),
		applicationID:     ask.ApplicationID,
		tags:              CloneAllocationTags(ask.Tags),
		createTime:        createTime,
		priority:          ask.Priority,
		placeholder:       ask.Placeholder,
		taskGroupName:     ask.TaskGroupName,
		requiredNode:      common.GetRequiredNodeFromTag(ask.Tags),
		allowPreemptSelf:  common.IsAllowPreemptSelf(ask.PreemptionPolicy),
		allowPreemptOther: common.IsAllowPreemptOther(ask.PreemptionPolicy),
		originator:        ask.Originator,
		allocLog:          make(map[string]*AllocationLogEntry),
		resKeyPerNode:     make(map[string]string),
		askEvents:         newAskEvents(events.GetEventSystem()),
	}
	// this is a safety check placeholder and task group name must be set as a combo
	// order is important as task group can be set without placeholder but not the other way around
	if saa.placeholder && saa.taskGroupName == "" {
		log.Log(log.SchedAllocation).Debug("ask cannot be a placeholder without a TaskGroupName",
			zap.Stringer("SI ask", ask))
		return nil
	}
	saa.resKeyWithoutNode = reservationKeyWithoutNode(ask.ApplicationID, ask.AllocationKey)
	return saa
}

func (aa *AllocationAsk) String() string {
	if aa == nil {
		return "ask is nil"
	}
	return fmt.Sprintf("allocationKey %s, applicationID %s, Resource %s, Allocated %t", aa.allocationKey, aa.applicationID, aa.allocatedResource, aa.IsAllocated())
}

// GetAllocationKey returns the allocation key for this ask
func (aa *AllocationAsk) GetAllocationKey() string {
	return aa.allocationKey
}

// GetApplicationID returns the application ID for this ask
func (aa *AllocationAsk) GetApplicationID() string {
	return aa.applicationID
}

// allocate marks the ask as allocated and returns true if successful. An ask may not be allocated multiple times.
func (aa *AllocationAsk) allocate() bool {
	aa.Lock()
	defer aa.Unlock()

	if aa.allocated {
		return false
	}
	aa.allocated = true
	return true
}

// deallocate marks the ask as pending and returns true if successful. An ask may not be deallocated multiple times.
func (aa *AllocationAsk) deallocate() bool {
	aa.Lock()
	defer aa.Unlock()

	if !aa.allocated {
		return false
	}
	aa.allocated = false
	return true
}

// IsAllocated determines if this ask has been allocated yet
func (aa *AllocationAsk) IsAllocated() bool {
	aa.RLock()
	defer aa.RUnlock()
	return aa.allocated
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

func (aa *AllocationAsk) SendPredicateFailedEvent(message string) {
	aa.askEvents.sendPredicateFailed(aa.allocationKey, aa.applicationID, message, aa.GetAllocatedResource())
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

func (aa *AllocationAsk) LessThan(other *AllocationAsk) bool {
	if aa.priority == other.priority {
		return aa.createTime.After(other.createTime) || aa.createTime.Equal(other.createTime)
	}

	return aa.priority < other.priority
}

func (aa *AllocationAsk) SetSchedulingAttempted(attempted bool) {
	aa.Lock()
	defer aa.Unlock()
	aa.schedulingAttempted = attempted
}

func (aa *AllocationAsk) IsSchedulingAttempted() bool {
	aa.RLock()
	defer aa.RUnlock()
	return aa.schedulingAttempted
}

func (aa *AllocationAsk) SetScaleUpTriggered(triggered bool) {
	aa.Lock()
	defer aa.Unlock()
	aa.scaleUpTriggered = triggered
}

func (aa *AllocationAsk) HasTriggeredScaleUp() bool {
	aa.RLock()
	defer aa.RUnlock()
	return aa.scaleUpTriggered
}

func (aa *AllocationAsk) setReservationKeyForNode(node, resKey string) {
	aa.Lock()
	defer aa.Unlock()
	aa.resKeyPerNode[node] = resKey
}

func (aa *AllocationAsk) getReservationKeyForNode(node string) string {
	aa.RLock()
	defer aa.RUnlock()
	return aa.resKeyPerNode[node]
}

func (aa *AllocationAsk) setHeadroomCheckFailed(headroom *resources.Resource, queue string) {
	aa.Lock()
	defer aa.Unlock()
	if !aa.headroomCheckFailed {
		aa.headroomCheckFailed = true
		aa.askEvents.sendRequestExceedsQueueHeadroom(aa.allocationKey, aa.applicationID, headroom, aa.allocatedResource, queue)
	}
}

func (aa *AllocationAsk) setHeadroomCheckPassed(queue string) {
	aa.Lock()
	defer aa.Unlock()
	if aa.headroomCheckFailed {
		aa.headroomCheckFailed = false
		aa.askEvents.sendRequestFitsInQueue(aa.allocationKey, aa.applicationID, queue, aa.allocatedResource)
	}
}

func (aa *AllocationAsk) setUserQuotaCheckFailed(available *resources.Resource) {
	aa.Lock()
	defer aa.Unlock()
	if !aa.userQuotaCheckFailed {
		aa.userQuotaCheckFailed = true
		aa.askEvents.sendRequestExceedsUserQuota(aa.allocationKey, aa.applicationID, available, aa.allocatedResource)
	}
}

func (aa *AllocationAsk) setUserQuotaCheckPassed() {
	aa.Lock()
	defer aa.Unlock()
	if aa.userQuotaCheckFailed {
		aa.userQuotaCheckFailed = false
		aa.askEvents.sendRequestFitsInUserQuota(aa.allocationKey, aa.applicationID, aa.allocatedResource)
	}
}
