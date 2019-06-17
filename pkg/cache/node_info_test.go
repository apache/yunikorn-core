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
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "testing"
)

func newNodeInfoForTest(nodeId string, totalResource *resources.Resource, attributes map[string]string) *NodeInfo {
    m := &NodeInfo{}

    m.NodeId = nodeId
    m.TotalResource = totalResource
    m.allocatedResource = resources.NewResource()
    m.initializeAttribute(attributes)
    m.allocations = make(map[string]*AllocationInfo)

    return m
}

func TestNodeInfo(t *testing.T) {
    node := newNodeInfoForTest("node-123", resources.NewResourceFromMap(
        map[string]resources.Quantity{"a": 123, "b": 456}),
        map[string]string{api.HOSTNAME: "host1", api.RACKNAME: "rack1", api.NODE_PARTITION: "partition1"})

    if node.Hostname != "host1" {
        t.Errorf("Failed to initialize hostname")
    }

    if node.Rackname != "rack1" {
        t.Errorf("Failed to initialize hostname")
    }

    if node.Partition != "partition1" {
        t.Errorf("Failed to initialize hostname")
    }

    if node.TotalResource.Resources["a"] != 123 || node.TotalResource.Resources["b"] != 456 {
        t.Errorf("Failed to initialize resource")
    }

    if node.GetAllocatedResource().Resources["a"] != 0 {
        t.Errorf("Failed to initialize resource")
    }

    node.initializeAttribute(map[string]string{api.HOSTNAME: "host2", api.RACKNAME: "rack1", api.NODE_PARTITION: "partition1", "x": "y"})

    if node.Hostname != "host2" {
        t.Errorf("Failed to update hostname from attributes")
    }

    if node.GetAttribute("x") != "y" {
        t.Errorf("Failed to update attributes")
    }

    node.AddAllocation(CreateMockAllocationInfo("app1", resources.MockResource(100, 200), "1", "queue-1", "node-1"))
    if nil == node.GetAllocation("1") {
        t.Errorf("Failed to add allocations")
    }

    if !resources.CompareMockResource(node.GetAllocatedResource(), 100, 200) {
        t.Errorf("Failed to add allocations")
    }

    node.AddAllocation(CreateMockAllocationInfo("app1", resources.MockResource(20, 200), "2", "queue-1", "node-1"))
    if nil == node.GetAllocation("2") {
        t.Errorf("Failed to add allocations")
    }

    if !resources.CompareMockResource(node.GetAllocatedResource(), 120, 400) {
        t.Errorf("Failed to add allocations")
    }
}
