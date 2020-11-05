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

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type allocationResult int

const (
	None allocationResult = iota
	Allocated
	AllocatedReserved
	Reserved
	Unreserved
)

func (ar allocationResult) String() string {
	return [...]string{"None", "Allocated", "AllocatedReserved", "Reserved", "Unreserved"}[ar]
}

/* Related to Allocation */
type Allocation struct {
	Ask               *AllocationAsk
	ApplicationID     string
	AllocationKey     string
	QueueName         string
	NodeID            string
	ReservedNodeID    string
	PartitionName     string
	UUID              string
	Tags              map[string]string
	Priority          int32
	AllocatedResource *resources.Resource
	Result            allocationResult
	Releases          []*Allocation
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
		AllocatedResource: ask.AllocatedResource,
		Result:            Allocated,
	}
}

func newReservedAllocation(result allocationResult, nodeID string, ask *AllocationAsk) *Allocation {
	return &Allocation{
		Ask:               ask,
		AllocationKey:     ask.AllocationKey,
		ApplicationID:     ask.ApplicationID,
		NodeID:            nodeID,
		PartitionName:     common.GetPartitionNameWithoutClusterID(ask.PartitionName),
		AllocatedResource: ask.AllocatedResource,
		Result:            result,
	}
}

func NewAllocationFromSI(alloc *si.Allocation) *Allocation {
	return &Allocation{
		NodeID:            alloc.NodeID,
		ApplicationID:     alloc.ApplicationID,
		PartitionName:     common.GetPartitionNameWithoutClusterID(alloc.PartitionName),
		AllocatedResource: resources.NewResourceFromProto(alloc.ResourcePerAlloc),
		AllocationKey:     alloc.AllocationKey,
		Tags:              alloc.AllocationTags,
		Priority:          alloc.Priority.GetPriorityValue(),
		UUID:              alloc.UUID,
		Result:            Allocated,
	}
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
		NodeID:        a.NodeID,
		ApplicationID: a.ApplicationID,
		AllocationKey: a.AllocationKey,
		UUID:          a.UUID,
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
