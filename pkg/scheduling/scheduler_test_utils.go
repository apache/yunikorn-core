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
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// AllocationInfo for tests inside and outside the cache
func createMockAllocationInfo(appID string, res *resources.Resource, uuid string, queueName string, nodeID string) *schedulingAllocation {
	alloc :=
		newSchedulingAllocation(&schedulingAllocationAsk{
			AskProto:          nil,
			AllocatedResource: res,
			ApplicationID:     appID,
			PartitionName:     "",
			QueueName:         queueName,
			createTime:        time.Time{},
			priority:          0,
			pendingRepeatAsk:  0,
			RWMutex:           sync.RWMutex{},
		}, nodeID)

	alloc.uuid = uuid

	return alloc
}

// Node to test with sorters (setting available resources)
func newNodeForSort(nodeID string, availResource *resources.Resource) *SchedulingNode {
	return newNodeForTestInternal(nodeID, resources.NewResource(), availResource)
}

// Node to test with anything but the sorters (setting total resources)
func newNodeForTest(nodeID string, totalResource *resources.Resource) *SchedulingNode {
	return newNodeForTestInternal(nodeID, totalResource, totalResource.Clone())
}

// Create a new application
func newSchedulingAppWithId(appID string) *SchedulingApplication {
	return newSchedulingAppInternal(appID, "default", "root.default", security.UserGroup{}, nil)
}

func newNodeForTestInternal(nodeID string, totalResource, availResource *resources.Resource) *SchedulingNode {
	proto := si.NewNodeInfo{
		NodeID:               nodeID,
		Attributes:           make(map[string]string),
		SchedulableResource:  totalResource.ToProto(),
		OccupiedResource:     resources.NewResource().ToProto(),
	}
	node := newSchedulingNode(&proto)
	node.availableResource = availResource
	return node
}