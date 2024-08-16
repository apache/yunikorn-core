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
	schedEvt "github.com/apache/yunikorn-core/pkg/scheduler/objects/events"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type Allocation struct {
	// Read-only fields
	allocationKey     string
	applicationID     string
	taskGroupName     string    // task group this allocation belongs to
	placeholder       bool      // is this a placeholder allocation
	createTime        time.Time // the time this allocation was created (used in reservations)
	priority          int32
	requiredNode      string
	allowPreemptSelf  bool
	allowPreemptOther bool
	originator        bool
	tags              map[string]string
	resKeyWithoutNode string // the reservation key without node
	foreign           bool
	preemptable       bool

	// Mutable fields which need protection
	allocated            bool
	allocLog             map[string]*AllocationLogEntry
	preemptionTriggered  bool
	preemptCheckTime     time.Time
	schedulingAttempted  bool              // whether scheduler core has tried to schedule this allocation
	scaleUpTriggered     bool              // whether this aloocation has triggered autoscaling or not
	resKeyPerNode        map[string]string // reservation key for a given node
	allocatedResource    *resources.Resource
	askEvents            *schedEvt.AskEvents
	userQuotaCheckFailed bool
	headroomCheckFailed  bool

	// Fields used once an allocation is bound
	nodeID                string      // the node this allocation is bound to
	bindTime              time.Time   // the time this allocation was bound to a node
	placeholderUsed       bool        // whether a placeholder was used for this allocation
	placeholderCreateTime time.Time   // the time the placeholder was created, if any
	released              bool        // whether this allocation has been released (for placeholders)
	release               *Allocation // placeholder to be released for this allocation
	preempted             bool        // whether this allocation has been marked for preemption
	instType              string      // the instance type of the node at the time this allocation was bound

	locking.RWMutex
}

type AllocationLogEntry struct {
	Message        string
	LastOccurrence time.Time
	Count          int32
}

// NewAllocationFromSI Create a new Allocation which has already been placed on a node, populating it with info from
// the SI Allocation object. If the input object is invalid, nil is returned.
func NewAllocationFromSI(alloc *si.Allocation) *Allocation {
	if alloc == nil {
		return nil
	}
	// this is a safety check placeholder and task group name must be set as a combo
	// order is important as task group can be set without placeholder but not the other way around
	if alloc.Placeholder && alloc.TaskGroupName == "" {
		log.Log(log.SchedAllocation).Debug("Allocation cannot be a placeholder without a TaskGroupName",
			zap.Stringer("SI alloc", alloc))
		return nil
	}

	var createTime time.Time
	siCreationTime, err := strconv.ParseInt(alloc.AllocationTags[siCommon.CreationTime], 10, 64)
	if err != nil {
		log.Log(log.SchedAllocation).Debug("CreationTime is not set on the Allocation object or invalid",
			zap.String("creationTime", alloc.AllocationTags[siCommon.CreationTime]))
		createTime = time.Now()
	} else {
		createTime = time.Unix(siCreationTime, 0)
	}

	foreign := false
	preemptable := true
	if foreignType, ok := alloc.AllocationTags["foreign"]; ok {
		foreign = true
		switch foreignType {
		case "static":
			preemptable = false
		case "default":
			preemptable = true
		default:
		}
	}

	var allocated bool
	var nodeID string
	var bindTime time.Time
	if alloc.NodeID != "" {
		allocated = true
		nodeID = alloc.NodeID
		bindTime = time.Now()
	}

	return &Allocation{
		allocationKey:     alloc.AllocationKey,
		applicationID:     alloc.ApplicationID,
		allocatedResource: resources.NewResourceFromProto(alloc.ResourcePerAlloc),
		tags:              CloneAllocationTags(alloc.AllocationTags),
		createTime:        createTime,
		priority:          alloc.Priority,
		placeholder:       alloc.Placeholder,
		taskGroupName:     alloc.TaskGroupName,
		requiredNode:      common.GetRequiredNodeFromTag(alloc.AllocationTags),
		allowPreemptSelf:  alloc.PreemptionPolicy.GetAllowPreemptSelf(),
		allowPreemptOther: alloc.PreemptionPolicy.GetAllowPreemptOther(),
		originator:        alloc.Originator,
		allocLog:          make(map[string]*AllocationLogEntry),
		resKeyPerNode:     make(map[string]string),
		resKeyWithoutNode: reservationKeyWithoutNode(alloc.ApplicationID, alloc.AllocationKey),
		askEvents:         schedEvt.NewAskEvents(events.GetEventSystem()),
		allocated:         allocated,
		nodeID:            nodeID,
		bindTime:          bindTime,
		foreign:           foreign,
		preemptable:       preemptable,
	}
}

// NewAllocationFromSIAllocated creates an Allocation where the "allocated" flag is always true,
// regardless whehether the NodeID if empty or not
func NewAllocationFromSIAllocated(siAlloc *si.Allocation) *Allocation {
	alloc := NewAllocationFromSI(siAlloc)
	alloc.allocated = true
	return alloc
}

// NewSIFromAllocation converts the Allocation into a SI object. This is a limited set of values that gets copied into
// the SI. This is only used to communicate *back* to the RM. All other fields are considered incoming fields from
// the RM into the core. The limited set of fields link the Allocation to an Application and Node.
func (a *Allocation) NewSIFromAllocation() *si.Allocation {
	if a == nil {
		return nil
	}
	return &si.Allocation{
		NodeID:           a.GetNodeID(),
		ApplicationID:    a.GetApplicationID(),
		AllocationKey:    a.GetAllocationKey(),
		ResourcePerAlloc: a.GetAllocatedResource().ToProto(), // needed in tests for restore
		TaskGroupName:    a.GetTaskGroup(),
		Placeholder:      a.IsPlaceholder(),
		Originator:       a.IsOriginator(),
		PreemptionPolicy: &si.PreemptionPolicy{
			AllowPreemptSelf:  a.IsAllowPreemptSelf(),
			AllowPreemptOther: a.IsAllowPreemptOther(),
		},
	}
}

func (a *Allocation) String() string {
	if a == nil {
		return "nil allocation"
	}
	return fmt.Sprintf("allocationKey %s, applicationID %s, Resource %s, Allocated %t", a.allocationKey, a.applicationID, a.GetAllocatedResource(), a.IsAllocated())
}

// GetAllocationKey returns the allocation key for this allocation.
func (a *Allocation) GetAllocationKey() string {
	return a.allocationKey
}

// GetApplicationID returns the application ID for this allocation.
func (a *Allocation) GetApplicationID() string {
	return a.applicationID
}

// GetTaskGroup returns the task group name for this allocation.
func (a *Allocation) GetTaskGroup() string {
	return a.taskGroupName
}

// GetCreateTime returns the time this allocation was created.
func (a *Allocation) GetCreateTime() time.Time {
	return a.createTime
}

// GetBindTime returns the time this allocation was bound.
func (a *Allocation) GetBindTime() time.Time {
	a.RLock()
	defer a.RUnlock()
	return a.bindTime
}

// SetBindTime sets the time this allocation was bound.
func (a *Allocation) SetBindTime(bindTime time.Time) {
	a.Lock()
	defer a.Unlock()
	a.bindTime = bindTime
}

// IsPlaceholderUsed returns whether this allocation is replacing a placeholder.
func (a *Allocation) IsPlaceholderUsed() bool {
	a.RLock()
	defer a.RUnlock()
	return a.placeholderUsed
}

// SetPlaceholderUsed sets whether this allocation is replacing a placeholder.
func (a *Allocation) SetPlaceholderUsed(placeholderUsed bool) {
	a.Lock()
	defer a.Unlock()
	a.placeholderUsed = placeholderUsed
}

// GetPlaceholderCreateTime returns the placeholder's create time for this allocation, if applicable.
func (a *Allocation) GetPlaceholderCreateTime() time.Time {
	a.RLock()
	defer a.RUnlock()
	return a.placeholderCreateTime
}

// SetPlaceholderCreateTime updates the placeholder's creation time.
func (a *Allocation) SetPlaceholderCreateTime(placeholderCreateTime time.Time) {
	a.Lock()
	defer a.Unlock()
	a.placeholderCreateTime = placeholderCreateTime
}

// IsPlaceholder returns whether this allocation represents a placeholder.
func (a *Allocation) IsPlaceholder() bool {
	return a.placeholder
}

// IsOriginator returns whether this alloocation is the originator for the application.
func (a *Allocation) IsOriginator() bool {
	return a.originator
}

// GetNodeID gets the node this allocation is assigned to.
func (a *Allocation) GetNodeID() string {
	a.RLock()
	defer a.RUnlock()
	return a.nodeID
}

// SetNodeID sets the node this allocation is assigned to.
func (a *Allocation) SetNodeID(nodeID string) {
	a.Lock()
	defer a.Unlock()
	a.nodeID = nodeID
}

// SetInstanceType sets node instance type for this allocation.
func (a *Allocation) SetInstanceType(instType string) {
	a.Lock()
	defer a.Unlock()
	a.instType = instType
}

// GetInstanceType return the type of the instance used by this allocation.
func (a *Allocation) GetInstanceType() string {
	a.RLock()
	defer a.RUnlock()
	return a.instType
}

// GetPriority returns the priority of this allocation.
func (a *Allocation) GetPriority() int32 {
	return a.priority
}

// IsReleased returns the release status of the allocation.
func (a *Allocation) IsReleased() bool {
	a.RLock()
	defer a.RUnlock()
	return a.released
}

// SetReleased updates the release status of the allocation.
func (a *Allocation) SetReleased(released bool) {
	a.Lock()
	defer a.Unlock()
	a.released = released
}

// GetTagsClone returns the copy of the tags for this allocation.
func (a *Allocation) GetTagsClone() map[string]string {
	return CloneAllocationTags(a.tags)
}

// GetRelease returns the associated release for this allocation.
func (a *Allocation) GetRelease() *Allocation {
	a.RLock()
	defer a.RUnlock()
	return a.release
}

// SetRelease sets the release for this allocation.
func (a *Allocation) SetRelease(release *Allocation) {
	a.Lock()
	defer a.Unlock()
	a.release = release
}

// ClearRelease removes any release from this allocation.
func (a *Allocation) ClearRelease() {
	a.Lock()
	defer a.Unlock()
	a.release = nil
}

// HasRelease determines if this allocation has an associated release.
func (a *Allocation) HasRelease() bool {
	a.RLock()
	defer a.RUnlock()
	return a.release != nil
}

// GetAllocatedResource returns a reference to the allocated resources for this allocation. This must be treated as read-only.
func (a *Allocation) GetAllocatedResource() *resources.Resource {
	a.RLock()
	defer a.RUnlock()
	return a.allocatedResource
}

// SetAllocatedResource updates the allocated resources for this allocation.
func (a *Allocation) SetAllocatedResource(allocatedResource *resources.Resource) {
	a.Lock()
	defer a.Unlock()
	a.allocatedResource = allocatedResource
}

// MarkPreempted marks the allocation as preempted.
func (a *Allocation) MarkPreempted() {
	a.Lock()
	defer a.Unlock()
	a.preempted = true
}

// IsPreempted returns whether the allocation has been marked for preemption or not.
func (a *Allocation) IsPreempted() bool {
	a.RLock()
	defer a.RUnlock()
	return a.preempted
}

// CloneAllocationTags clones a tag map for safe copying.
func CloneAllocationTags(tags map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range tags {
		result[k] = v
	}
	return result
}

// allocate marks this request as allocated and returns true if successful. A request may not be allocated multiple times.
func (a *Allocation) allocate() bool {
	a.Lock()
	defer a.Unlock()

	if a.allocated {
		return false
	}
	a.allocated = true
	return true
}

// deallocate marks this request as pending and returns true if successful. A request may not be deallocated multiple times.
func (a *Allocation) deallocate() bool {
	a.Lock()
	defer a.Unlock()

	if !a.allocated {
		return false
	}
	a.allocated = false
	return true
}

// IsAllocated determines if this request has been allocated yet.
func (a *Allocation) IsAllocated() bool {
	a.RLock()
	defer a.RUnlock()
	return a.allocated
}

// GetPreemptCheckTime returns the time this allocation was last evaluated for preemption.
func (a *Allocation) GetPreemptCheckTime() time.Time {
	a.RLock()
	defer a.RUnlock()
	return a.preemptCheckTime
}

// UpdatePreemptCheckTime is used to mark when this allocation is evaluated for preemption.
func (a *Allocation) UpdatePreemptCheckTime() {
	a.Lock()
	defer a.Unlock()
	a.preemptCheckTime = time.Now()
}

// GetRequiredNode gets the node (if any) required by this allocation.
func (a *Allocation) GetRequiredNode() string {
	return a.requiredNode
}

// SetRequiredNode sets the required node (used only by testing so lock is not taken)
func (a *Allocation) SetRequiredNode(node string) {
	a.requiredNode = node
}

// IsAllowPreemptSelf returns whether preemption is allowed for this allocation.
func (a *Allocation) IsAllowPreemptSelf() bool {
	return a.allowPreemptSelf
}

// IsAllowPreemptOther returns whether this allocation is allowed to preempt others.
func (a *Allocation) IsAllowPreemptOther() bool {
	return a.allowPreemptOther
}

// GetTag returns the value of a named tag or an empty string if not present.
func (a *Allocation) GetTag(tagName string) string {
	result, ok := a.tags[tagName]
	if !ok {
		return ""
	}
	return result
}

// LogAllocationFailure keeps track of preconditions not being met for an allocation.
func (a *Allocation) LogAllocationFailure(message string, allocate bool) {
	// for now, don't log reservations
	if !allocate {
		return
	}

	a.Lock()
	defer a.Unlock()

	entry, ok := a.allocLog[message]
	if !ok {
		entry = &AllocationLogEntry{
			Message: message,
		}
		a.allocLog[message] = entry
	}
	entry.LastOccurrence = time.Now()
	entry.Count++
}

// SendPredicatesFailedEvent updates the event system with the reason for predicate failures.
// The map predicateErrors contains how many times certain predicates failed in the scheduling cycle for this ask.
func (a *Allocation) SendPredicatesFailedEvent(predicateErrors map[string]int) {
	a.askEvents.SendPredicatesFailed(a.allocationKey, a.applicationID, predicateErrors, a.GetAllocatedResource())
}

// GetAllocationLog returns a list of log entries corresponding to allocation preconditions not being met.
func (a *Allocation) GetAllocationLog() []*AllocationLogEntry {
	a.RLock()
	defer a.RUnlock()

	res := make([]*AllocationLogEntry, len(a.allocLog))
	i := 0
	for _, entry := range a.allocLog {
		res[i] = &AllocationLogEntry{
			Message:        entry.Message,
			LastOccurrence: entry.LastOccurrence,
			Count:          entry.Count,
		}
		i++
	}
	return res
}

// MarkTriggeredPreemption marks the current allocation because it triggered preemption during scheduling.
func (a *Allocation) MarkTriggeredPreemption() {
	a.Lock()
	defer a.Unlock()
	a.preemptionTriggered = true
}

// HasTriggeredPreemption returns whether this allocation has triggered preemption.
func (a *Allocation) HasTriggeredPreemption() bool {
	a.RLock()
	defer a.RUnlock()
	return a.preemptionTriggered
}

// LessThan compares two allocations by priority and then creation time.
func (a *Allocation) LessThan(other *Allocation) bool {
	if a.priority == other.priority {
		return a.createTime.After(other.createTime) || a.createTime.Equal(other.createTime)
	}

	return a.priority < other.priority
}

// SetSchedulingAttempted marks whether scheduling has been attempted at least once for this allocation.
func (a *Allocation) SetSchedulingAttempted(attempted bool) {
	a.Lock()
	defer a.Unlock()
	a.schedulingAttempted = attempted
}

// IsSchedulingAttempted determines whether scheduling has been attempted at least once for this allocation.
func (a *Allocation) IsSchedulingAttempted() bool {
	a.RLock()
	defer a.RUnlock()
	return a.schedulingAttempted
}

// SetScaleUpTriggered marks this allocation as having triggered the autoscaler.
func (a *Allocation) SetScaleUpTriggered(triggered bool) {
	a.Lock()
	defer a.Unlock()
	a.scaleUpTriggered = triggered
}

// HasTriggeredScaleUp determines if this allocation has triggered auto-scaling.
func (a *Allocation) HasTriggeredScaleUp() bool {
	a.RLock()
	defer a.RUnlock()
	return a.scaleUpTriggered
}

func (a *Allocation) setReservationKeyForNode(node, resKey string) {
	a.Lock()
	defer a.Unlock()
	a.resKeyPerNode[node] = resKey
}

func (a *Allocation) getReservationKeyForNode(node string) string {
	a.RLock()
	defer a.RUnlock()
	return a.resKeyPerNode[node]
}

func (a *Allocation) setHeadroomCheckFailed(headroom *resources.Resource, queue string) {
	a.Lock()
	defer a.Unlock()
	if !a.headroomCheckFailed {
		a.headroomCheckFailed = true
		a.askEvents.SendRequestExceedsQueueHeadroom(a.allocationKey, a.applicationID, headroom, a.allocatedResource, queue)
	}
}

func (a *Allocation) setHeadroomCheckPassed(queue string) {
	a.Lock()
	defer a.Unlock()
	if a.headroomCheckFailed {
		a.headroomCheckFailed = false
		a.askEvents.SendRequestFitsInQueue(a.allocationKey, a.applicationID, queue, a.allocatedResource)
	}
}

func (a *Allocation) setUserQuotaCheckFailed(available *resources.Resource) {
	a.Lock()
	defer a.Unlock()
	if !a.userQuotaCheckFailed {
		a.userQuotaCheckFailed = true
		a.askEvents.SendRequestExceedsUserQuota(a.allocationKey, a.applicationID, available, a.allocatedResource)
	}
}

func (a *Allocation) setUserQuotaCheckPassed() {
	a.Lock()
	defer a.Unlock()
	if a.userQuotaCheckFailed {
		a.userQuotaCheckFailed = false
		a.askEvents.SendRequestFitsInUserQuota(a.allocationKey, a.applicationID, a.allocatedResource)
	}
}

func (a *Allocation) IsForeign() bool {
	return a.foreign
}

func (a *Allocation) IsPreemptable() bool {
	return a.preemptable
}
