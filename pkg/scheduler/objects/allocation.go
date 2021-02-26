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

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type allocationResult int

const (
	None allocationResult = iota
	Allocated
	AllocatedReserved
	Reserved
	Unreserved
	Replaced
	PlaceholderExpired
)

func (ar allocationResult) String() string {
	return [...]string{"None", "Allocated", "AllocatedReserved", "Reserved", "Unreserved", "Replaced", "PlaceholderExpired"}[ar]
}

/* Related to Allocation */
type Allocation struct {
	Ask               *AllocationAsk
	ApplicationID     string
	AllocationKey     string
	QueueName         string // CLEANUP: why do we need this? the app is linked to the queue
	NodeID            string
	ReservedNodeID    string
	PartitionName     string
	UUID              string
	Tags              map[string]string
	Priority          int32
	AllocatedResource *resources.Resource
	Result            allocationResult
	Releases          []*Allocation
	placeholder       bool
	taskGroupName     string
	released          bool
}

func NewAllocation(uuid, nodeID string, ask *AllocationAsk) *Allocation {
	return &Allocation{
		Ask:               ask,
		AllocationKey:     ask.AllocationKey,
		ApplicationID:     ask.ApplicationID,
		QueueName:         ask.QueueName,
		NodeID:            nodeID,
		PartitionName:     common.GetPartitionNameWithoutClusterID(ask.PartitionName),
		UUID:              uuid,
		Tags:              ask.Tags,
		Priority:          ask.priority,
		AllocatedResource: ask.AllocatedResource.Clone(),
		taskGroupName:     ask.taskGroupName,
		placeholder:       ask.placeholder,
		Result:            Allocated,
	}
}

func newReservedAllocation(result allocationResult, nodeID string, ask *AllocationAsk) *Allocation {
	alloc := NewAllocation("", nodeID, ask)
	alloc.Result = result
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
			zap.String("SI alloc", alloc.String()))
		return nil
	}

	ask := &AllocationAsk{
		AllocationKey:     alloc.AllocationKey,
		ApplicationID:     alloc.ApplicationID,
		PartitionName:     alloc.PartitionName,
		AllocatedResource: resources.NewResourceFromProto(alloc.ResourcePerAlloc),
		Tags:              alloc.AllocationTags,
		priority:          alloc.Priority.GetPriorityValue(),
		pendingRepeatAsk:  0,
		maxAllocations:    1,
		taskGroupName:     alloc.TaskGroupName,
		placeholder:       alloc.Placeholder,
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
		NodeID:           a.NodeID,
		ApplicationID:    a.ApplicationID,
		AllocationKey:    a.AllocationKey,
		UUID:             a.UUID,
		ResourcePerAlloc: a.AllocatedResource.ToProto(), // needed in tests for restore
		TaskGroupName:    a.taskGroupName,
		Placeholder:      a.placeholder,
	}
}

func (a *Allocation) String() string {
	if a == nil {
		return "nil allocation"
	}
	uuid := a.UUID
	if a.Result == Reserved || a.Result == Unreserved {
		uuid = "N/A"
	}
	return fmt.Sprintf("ApplicationID=%s, UUID=%s, AllocationKey=%s, Node=%s, Result=%s", a.ApplicationID, uuid, a.AllocationKey, a.NodeID, a.Result.String())
}

func (a *Allocation) IsPlaceholder() bool {
	return a.placeholder
}

func (a *Allocation) getTaskGroup() string {
	return a.taskGroupName
}
