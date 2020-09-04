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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type allocationResult int

const (
	none allocationResult = iota
	allocated
	allocatedReserved
	reserved
	unreserved
)

func (ar allocationResult) String() string {
	return [...]string{"none", "allocated", "allocatedReserved", "reserved", "unreserved"}[ar]
}

type schedulingAllocation struct {
	SchedulingAsk  *schedulingAllocationAsk
	repeats        int32
	nodeID         string
	reservedNodeID string
	releases       []*ReleaseAllocation
	result         allocationResult

	// Mutable part, need protection
	uuid           string

	sync.RWMutex
}

func newSchedulingAllocation(ask *schedulingAllocationAsk, nodeID string) *schedulingAllocation {
	return &schedulingAllocation{
		SchedulingAsk: ask,
		nodeID:        nodeID,
		repeats:       1,
		result:        none,
		uuid:          "", // UUID will be set later
	}
}

func newSchedulingAllocationFromProto(allocation *si.Allocation) *schedulingAllocation {
	return &schedulingAllocation{
		SchedulingAsk: &schedulingAllocationAsk{
			AskProto: &si.AllocationAsk{
				AllocationKey:  allocation.AllocationKey,
				ApplicationID:  allocation.ApplicationID,
				PartitionName:  allocation.PartitionName,
				ResourceAsk:    allocation.ResourcePerAlloc,
				MaxAllocations: 1,
				Priority:       allocation.Priority,
				Tags:           allocation.AllocationTags,
			},
			AllocatedResource: resources.NewResourceFromProto(allocation.ResourcePerAlloc),
			ApplicationID:     allocation.ApplicationID,
			PartitionName:     allocation.GetPartitionName(),
			QueueName:         allocation.GetQueueName(),
			createTime:        time.Time{},
			priority:          allocation.Priority.GetPriorityValue(),
			pendingRepeatAsk:  0,
			RWMutex:           sync.RWMutex{},
		},
		repeats:  0,
		nodeID:   allocation.NodeID,
		releases: nil,
		result:   allocated,
		uuid:     allocation.UUID,
	}
}

func (sa *schedulingAllocation) String() string {
	return fmt.Sprintf("AllocatioKey=%s, repeats=%d, node=%s, result=%s", sa.SchedulingAsk.AskProto.AllocationKey, sa.repeats, sa.nodeID, sa.result.String())
}

func (sa* schedulingAllocation) GetUUID() string {
	sa.RLock()
	defer sa.RUnlock()

	return sa.uuid
}

func (sa* schedulingAllocation) SetUUID(uuid string) {
	sa.Lock()
	defer sa.Unlock()

	sa.uuid = uuid
}

func (sa* schedulingAllocation) GetNodeId() string {


	return sa.nodeID
}

func (sa* schedulingAllocation) GetAllocationDAO() *dao.AllocationDAOInfo {
	sa.RLock()
	defer sa.RUnlock()

	ask := sa.SchedulingAsk
	askProto := ask.AskProto
	allocInfo := dao.AllocationDAOInfo{
		AllocationKey:    askProto.AllocationKey,
		AllocationTags:   askProto.Tags,
		UUID:             sa.uuid,
		ResourcePerAlloc: strings.Trim(ask.AllocatedResource.String(), "map"),
		Priority:         askProto.Priority.String(),
		QueueName:        ask.QueueName,
		NodeID:           sa.nodeID,
		ApplicationID:    ask.ApplicationID,
		Partition:        ask.PartitionName,
	}

	return &allocInfo
}