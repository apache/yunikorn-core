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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type AllocationResult int

const (
	None AllocationResult = iota
	Allocated
	AllocatedReserved
	Reserved
	Unreserved
	Replaced
)

func (ar AllocationResult) String() string {
	return [...]string{"None", "Allocated", "AllocatedReserved", "Reserved", "Unreserved", "Replaced"}[ar]
}

type Allocation struct {
	// Read-only fields
	ask               *AllocationAsk
	allocationKey     string
	applicationID     string
	partitionName     string
	taskGroupName     string // task group this allocation belongs to
	placeholder       bool   // is this a placeholder allocation
	nodeID            string
	uuid              string
	priority          int32
	tags              map[string]string
	allocatedResource *resources.Resource

	// Mutable fields which need protection
	placeholderUsed       bool
	createTime            time.Time // the time this allocation was created
	placeholderCreateTime time.Time
	released              bool
	reservedNodeID        string
	result                AllocationResult
	releases              []*Allocation
	preempted             bool

	sync.RWMutex
}

func NewAllocation(uuid, nodeID string, ask *AllocationAsk) *Allocation {
	var createTime time.Time
	if ask.GetTag(siCommon.CreationTime) == "" {
		createTime = time.Now()
	} else {
		createTime = ask.GetCreateTime()
	}
	return &Allocation{
		ask:               ask,
		allocationKey:     ask.GetAllocationKey(),
		applicationID:     ask.GetApplicationID(),
		createTime:        createTime,
		nodeID:            nodeID,
		partitionName:     common.GetPartitionNameWithoutClusterID(ask.GetPartitionName()),
		uuid:              uuid,
		tags:              ask.GetTagsClone(),
		priority:          ask.GetPriority(),
		allocatedResource: ask.GetAllocatedResource().Clone(),
		taskGroupName:     ask.GetTaskGroup(),
		placeholder:       ask.IsPlaceholder(),
		result:            Allocated,
	}
}

func newReservedAllocation(result AllocationResult, nodeID string, ask *AllocationAsk) *Allocation {
	alloc := NewAllocation("", nodeID, ask)
	alloc.SetResult(result)
	return alloc
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
		log.Logger().Debug("Allocation cannot be a placeholder without a TaskGroupName",
			zap.Stringer("SI alloc", alloc))
		return nil
	}

	creationTime, err := strconv.ParseInt(alloc.AllocationTags[siCommon.CreationTime], 10, 64)
	if err != nil {
		log.Logger().Warn("CreationTime is not set on the Allocation object or invalid",
			zap.String("creationTime", alloc.AllocationTags[siCommon.CreationTime]))
		creationTime = -1
	}

	ask := &AllocationAsk{
		allocationKey:     alloc.AllocationKey,
		applicationID:     alloc.ApplicationID,
		partitionName:     alloc.PartitionName,
		allocatedResource: resources.NewResourceFromProto(alloc.ResourcePerAlloc),
		tags:              CloneAllocationTags(alloc.AllocationTags),
		priority:          alloc.Priority,
		pendingAskRepeat:  0,
		maxAllocations:    1,
		taskGroupName:     alloc.TaskGroupName,
		placeholder:       alloc.Placeholder,
		createTime:        time.Unix(creationTime, 0),
		allocLog:          make(map[string]*AllocationLogEntry),
	}
	return NewAllocation(alloc.UUID, alloc.NodeID, ask)
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
		UUID:             a.GetUUID(),
		ResourcePerAlloc: a.GetAllocatedResource().ToProto(), // needed in tests for restore
		TaskGroupName:    a.GetTaskGroup(),
		Placeholder:      a.IsPlaceholder(),
	}
}

func (a *Allocation) String() string {
	if a == nil {
		return "nil allocation"
	}
	a.RLock()
	defer a.RUnlock()
	uuid := a.uuid
	if a.result == Reserved || a.result == Unreserved {
		uuid = "N/A"
	}
	return fmt.Sprintf("applicationID=%s, uuid=%s, allocationKey=%s, Node=%s, result=%s", a.applicationID, uuid, a.allocationKey, a.nodeID, a.result.String())
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

// GetPartitionName returns the partition name for this allocation
func (a *Allocation) GetPartitionName() string {
	return a.partitionName
}

// GetTaskGroup returns the task group name for this allocation
func (a *Allocation) GetTaskGroup() string {
	return a.taskGroupName
}

// GetCreateTime returns the time this allocation was created
func (a *Allocation) GetCreateTime() time.Time {
	a.RLock()
	defer a.RUnlock()
	return a.createTime
}

func (a *Allocation) SetCreateTime(createTime time.Time) {
	a.Lock()
	defer a.Unlock()
	a.createTime = createTime
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
func (a *Allocation) SetPlaceholderCreateTime(placeholdereCreateTime time.Time) {
	a.Lock()
	defer a.Unlock()
	a.placeholderCreateTime = placeholdereCreateTime
}

// IsPlaceholder returns whether the allocation is a placeholder
func (a *Allocation) IsPlaceholder() bool {
	return a.placeholder
}

// GetNodeID gets the node this allocation is assigned to
func (a *Allocation) GetNodeID() string {
	return a.nodeID
}

// GetUUID returns the uuid for this allocation
func (a *Allocation) GetUUID() string {
	return a.uuid
}

// GetPriority returns the priority of this allocation
func (a *Allocation) GetPriority() int32 {
	return a.priority
}

// GetReservedNodeID gets the node this allocation is reserved for
func (a *Allocation) GetReservedNodeID() string {
	a.RLock()
	defer a.RUnlock()
	return a.reservedNodeID
}

// SetReservedNodeID sets the node this allocation is reserved for
func (a *Allocation) SetReservedNodeID(reservedNodeID string) {
	a.Lock()
	defer a.Unlock()
	a.reservedNodeID = reservedNodeID
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

// GetResult gets the result of this allocation
func (a *Allocation) GetResult() AllocationResult {
	a.RLock()
	defer a.RUnlock()
	return a.result
}

// SetResult sets the result of this allocation
func (a *Allocation) SetResult(result AllocationResult) {
	a.Lock()
	defer a.Unlock()
	a.result = result
}

// GetReleasesClone returns a clone of the release list
func (a *Allocation) GetReleasesClone() []*Allocation {
	a.RLock()
	defer a.RUnlock()
	result := make([]*Allocation, len(a.releases))
	copy(result, a.releases)
	return result
}

// GetFirstRelease returns the first release for this allocation
func (a *Allocation) GetFirstRelease() *Allocation {
	a.RLock()
	defer a.RUnlock()
	return a.releases[0]
}

// GetReleaseCount gets the number of releases associated with this allocation
func (a *Allocation) GetReleaseCount() int {
	a.RLock()
	defer a.RUnlock()
	return len(a.releases)
}

// ClearReleases removes all releases from this allocation
func (a *Allocation) ClearReleases() {
	a.Lock()
	defer a.Unlock()
	a.releases = nil
}

// AddRelease adds a new release to this allocation
func (a *Allocation) AddRelease(release *Allocation) {
	a.Lock()
	defer a.Unlock()
	a.releases = append(a.releases, release)
}

func (a *Allocation) SetRelease(release *Allocation) {
	a.Lock()
	defer a.Unlock()
	a.releases = []*Allocation{release}
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
