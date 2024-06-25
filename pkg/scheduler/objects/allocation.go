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

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/log"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type AllocationResultType int

const (
	None AllocationResultType = iota
	Allocated
	AllocatedReserved
	Reserved
	Unreserved
	Replaced
)

func (art AllocationResultType) String() string {
	return [...]string{"None", "Allocated", "AllocatedReserved", "Reserved", "Unreserved", "Replaced"}[art]
}

type AllocationResult struct {
	ResultType     AllocationResultType
	NodeID         string
	ReservedNodeID string
	Ask            *AllocationAsk
	Allocation     *Allocation
}

func (ar *AllocationResult) String() string {
	if ar == nil {
		return "nil allocation result"
	}
	allocationKey := ""
	if ar.Ask != nil {
		allocationKey = ar.Ask.GetAllocationKey()
	}
	return fmt.Sprintf("resultType=%s, nodeID=%s, reservedNodeID=%s, allocationKey=%s", ar.ResultType.String(), ar.NodeID, ar.ReservedNodeID, allocationKey)
}

type Allocation struct {
	// Read-only fields
	ask               *AllocationAsk
	allocationKey     string
	applicationID     string
	taskGroupName     string // task group this allocation belongs to
	placeholder       bool   // is this a placeholder allocation
	nodeID            string
	priority          int32
	tags              map[string]string
	allocatedResource *resources.Resource
	createTime        time.Time // the time this allocation was created

	// Mutable fields which need protection
	placeholderUsed       bool
	bindTime              time.Time // the time this allocation was bound to a node
	placeholderCreateTime time.Time
	released              bool
	release               *Allocation
	preempted             bool
	instType              string

	locking.RWMutex
}

func NewAllocation(nodeID string, ask *AllocationAsk) *Allocation {
	return &Allocation{
		ask:               ask,
		allocationKey:     ask.GetAllocationKey(),
		applicationID:     ask.GetApplicationID(),
		createTime:        ask.GetCreateTime(),
		bindTime:          time.Now(),
		nodeID:            nodeID,
		tags:              ask.GetTagsClone(),
		priority:          ask.GetPriority(),
		allocatedResource: ask.GetAllocatedResource().Clone(),
		taskGroupName:     ask.GetTaskGroup(),
		placeholder:       ask.IsPlaceholder(),
	}
}

// newAllocatedAllocationResult creates a new allocation result for a new allocation.
func newAllocatedAllocationResult(nodeID string, ask *AllocationAsk, alloc *Allocation) *AllocationResult {
	return newAllocationResultInternal(Allocated, nodeID, ask, alloc)
}

// newReservedAllocationResult creates a new allocation result for reserving a node.
func newReservedAllocationResult(nodeID string, ask *AllocationAsk) *AllocationResult {
	return newAllocationResultInternal(Reserved, nodeID, ask, nil)
}

// newUnreservedAllocationResult creates a new allocation result for unreserving a node.
func newUnreservedAllocationResult(nodeID string, ask *AllocationAsk) *AllocationResult {
	return newAllocationResultInternal(Unreserved, nodeID, ask, nil)
}

// newReplacedAllocationResult create a new allocation result for replaced allocations.
func newReplacedAllocationResult(nodeID string, ask *AllocationAsk, alloc *Allocation) *AllocationResult {
	return newAllocationResultInternal(Replaced, nodeID, ask, alloc)
}

// newAllocationResultInternal creates a new allocation result. It should not be called directly.
func newAllocationResultInternal(resultType AllocationResultType, nodeID string, ask *AllocationAsk, alloc *Allocation) *AllocationResult {
	return &AllocationResult{
		ResultType: resultType,
		NodeID:     nodeID,
		Ask:        ask,
		Allocation: alloc,
	}
}

// Create a new Allocation from a node recovered allocation.
// Also creates an AllocationAsk to maintain backward compatible behaviour
// This returns a nil Allocation on nil input or errors
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

	ask := &AllocationAsk{
		allocationKey:     alloc.AllocationKey,
		applicationID:     alloc.ApplicationID,
		allocatedResource: resources.NewResourceFromProto(alloc.ResourcePerAlloc),
		tags:              CloneAllocationTags(alloc.AllocationTags),
		priority:          alloc.Priority,
		allocated:         true,
		taskGroupName:     alloc.TaskGroupName,
		placeholder:       alloc.Placeholder,
		createTime:        createTime,
		allocLog:          make(map[string]*AllocationLogEntry),
		originator:        alloc.Originator,
		allowPreemptSelf:  alloc.PreemptionPolicy.GetAllowPreemptSelf(),
		allowPreemptOther: alloc.PreemptionPolicy.GetAllowPreemptOther(),
	}
	newAlloc := NewAllocation(alloc.NodeID, ask)
	newAlloc.allocationKey = alloc.AllocationKey
	return newAlloc
}

// Convert the Allocation into a SI object. This is a limited set of values that gets copied into the SI.
// We only use this to communicate *back* to the RM. All other fields are considered incoming fields from
// the RM into the core.
// The limited set of fields link the Allocation to an Application, Node and AllocationAsk.
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
		Originator:       a.GetAsk().IsOriginator(),
		PreemptionPolicy: &si.PreemptionPolicy{
			AllowPreemptSelf:  a.GetAsk().IsAllowPreemptSelf(),
			AllowPreemptOther: a.GetAsk().IsAllowPreemptOther(),
		},
	}
}

func (a *Allocation) String() string {
	if a == nil {
		return "nil allocation"
	}
	a.RLock()
	defer a.RUnlock()
	return fmt.Sprintf("applicationID=%s, allocationKey=%s, Node=%s", a.applicationID, a.allocationKey, a.nodeID)
}

// GetAsk returns the ask associated with this allocation
func (a *Allocation) GetAsk() *AllocationAsk {
	return a.ask
}

// GetAllocationKey returns the allocation key of this allocation
func (a *Allocation) GetAllocationKey() string {
	return a.allocationKey
}

// GetApplicationID returns the application ID for this allocation
func (a *Allocation) GetApplicationID() string {
	return a.applicationID
}

// GetTaskGroup returns the task group name for this allocation
func (a *Allocation) GetTaskGroup() string {
	return a.taskGroupName
}

// GetCreateTime returns the time this allocation was created
func (a *Allocation) GetCreateTime() time.Time {
	return a.createTime
}

// GetBindTime returns the time this allocation was created
func (a *Allocation) GetBindTime() time.Time {
	a.RLock()
	defer a.RUnlock()
	return a.bindTime
}

func (a *Allocation) SetBindTime(bindTime time.Time) {
	a.Lock()
	defer a.Unlock()
	a.bindTime = bindTime
}

// IsPlaceholderUsed returns whether this alloc is replacing a placeholder
func (a *Allocation) IsPlaceholderUsed() bool {
	a.RLock()
	defer a.RUnlock()
	return a.placeholderUsed
}

// SetPlaceholderUsed sets whether this alloc is replacing a placeholder
func (a *Allocation) SetPlaceholderUsed(placeholderUsed bool) {
	a.Lock()
	defer a.Unlock()
	a.placeholderUsed = placeholderUsed
}

// GetPlaceholderCreateTime returns the placeholder's create time for this alloc, if applicable
func (a *Allocation) GetPlaceholderCreateTime() time.Time {
	a.RLock()
	defer a.RUnlock()
	return a.placeholderCreateTime
}

// SetPlaceholderCreateTime updates the placeholder's creation time
func (a *Allocation) SetPlaceholderCreateTime(placeholderCreateTime time.Time) {
	a.Lock()
	defer a.Unlock()
	a.placeholderCreateTime = placeholderCreateTime
}

// IsPlaceholder returns whether the allocation is a placeholder
func (a *Allocation) IsPlaceholder() bool {
	return a.placeholder
}

// GetNodeID gets the node this allocation is assigned to
func (a *Allocation) GetNodeID() string {
	return a.nodeID
}

// SetInstanceType sets node instance type for this allocation
func (a *Allocation) SetInstanceType(instType string) {
	a.Lock()
	defer a.Unlock()
	a.instType = instType
}

// GetInstanceType return the type of the instance used by this allocation
func (a *Allocation) GetInstanceType() string {
	a.RLock()
	defer a.RUnlock()
	return a.instType
}

// GetPriority returns the priority of this allocation
func (a *Allocation) GetPriority() int32 {
	return a.priority
}

// IsReleased returns the release status of the allocation
func (a *Allocation) IsReleased() bool {
	a.RLock()
	defer a.RUnlock()
	return a.released
}

// SetReleased updates the release status of the allocation
func (a *Allocation) SetReleased(released bool) {
	a.Lock()
	defer a.Unlock()
	a.released = released
}

// GetTagsClone returns the copy of the tags for this allocation
func (a *Allocation) GetTagsClone() map[string]string {
	return CloneAllocationTags(a.tags)
}

// GetRelease returns the associated release for this allocation
func (a *Allocation) GetRelease() *Allocation {
	a.RLock()
	defer a.RUnlock()
	return a.release
}

// SetRelease sets the release for this allocation
func (a *Allocation) SetRelease(release *Allocation) {
	a.Lock()
	defer a.Unlock()
	a.release = release
}

// ClearRelease removes all releases from this allocation
func (a *Allocation) ClearRelease() {
	a.Lock()
	defer a.Unlock()
	a.release = nil
}

// HasRelease determines if this allocation has an associated release
func (a *Allocation) HasRelease() bool {
	a.RLock()
	defer a.RUnlock()
	return a.release != nil
}

// GetAllocatedResource returns a reference to the allocated resources for this allocation. This must be treated as read-only.
func (a *Allocation) GetAllocatedResource() *resources.Resource {
	return a.allocatedResource
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

// CloneAllocationTags clones a tag map for safe copying
func CloneAllocationTags(tags map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range tags {
		result[k] = v
	}
	return result
}
