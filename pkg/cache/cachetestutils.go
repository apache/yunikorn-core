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

package cache

import (
	"fmt"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// AllocationInfo for tests inside the cache
func createMockAllocationInfo(appID string, res *resources.Resource, uuid string, queueName string, nodeID string) *AllocationInfo {
	info := &AllocationInfo{ApplicationID: appID, AllocatedResource: res,
		AllocationProto: &si.Allocation{UUID: uuid, QueueName: queueName, NodeID: nodeID}}
	return info
}

// Create a partition for testing from a yaml configuration
func CreatePartitionInfo(data []byte) (*PartitionInfo, error) {
	// create config from string
	configs.MockSchedulerConfigByData(data)
	conf, err := configs.SchedulerConfigLoader("default-policy-group")
	if err != nil {
		return nil, fmt.Errorf("error when loading config %v", err)
	}
	pi, err := NewPartitionInfo(conf.Partitions[0], "rm1", nil)
	if err != nil {
		return nil, fmt.Errorf("error when loading ParttionInfo from config %v", err)
	}
	return pi, nil
}

// Node to test with sorters (setting available resources)
func NewNodeForSort(nodeID string, availResource *resources.Resource) *NodeInfo {
	return newNodeForTest(nodeID, resources.NewResource(), availResource)
}

// Node to test with anything but the sorters (setting total resources)
func NewNodeForTest(nodeID string, totalResource *resources.Resource) *NodeInfo {
	return newNodeForTest(nodeID, totalResource, totalResource.Clone())
}

// Internal function to create the nodeInfo
func newNodeForTest(nodeID string, totalResource, availResource *resources.Resource) *NodeInfo {
	node := &NodeInfo{}
	// set the basics
	node.NodeID = nodeID
	node.allocations = make(map[string]*AllocationInfo)
	node.schedulable = true
	node.Partition = "default"
	// make sure they are independent objects
	node.TotalResource = totalResource
	node.availableResource = availResource
	node.allocatedResource = resources.NewResource()

	return node
}
