/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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
    "github.com/cloudera/yunikorn-core/pkg/common/configs"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
)

func CreateMockAllocationInfo(appId string, res *resources.Resource, uuid string, queueName string, nodeId string) *AllocationInfo {
    info := &AllocationInfo{ApplicationId: appId, AllocatedResource: res,
        AllocationProto: &si.Allocation{Uuid: uuid, QueueName: queueName, NodeId: nodeId}}
    return info
}

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
func NewNodeForSort(nodeId string, availResource *resources.Resource) *NodeInfo {
    return newNodeForTest(nodeId, resources.NewResource(), availResource)
}

// Node to test with anything but the sorters (setting total resources)
func NewNodeForTest(nodeId string, totalResource *resources.Resource) *NodeInfo {
    return newNodeForTest(nodeId, totalResource, totalResource.Clone())
}

// Internal function to create the nodeInfo
func newNodeForTest(nodeId string, totalResource, availResource *resources.Resource) *NodeInfo {
    node := &NodeInfo{}
    // set the basics
    node.NodeId = nodeId
    node.allocations = make(map[string]*AllocationInfo)
    node.schedulable = true
    node.Partition = "default"
    // make sure they are independent objects
    node.TotalResource = totalResource
    node.availableResource = availResource
    node.allocatedResource = resources.NewResource()

    return node
}
