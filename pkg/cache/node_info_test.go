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
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/api"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func newProto(nodeID string, totalResource *resources.Resource, attributes map[string]string) *si.NewNodeInfo {
	proto := si.NewNodeInfo{
		NodeID:     nodeID,
		Attributes: attributes,
	}

	if totalResource != nil {
		proto.SchedulableResource = &si.Resource{
			Resources: map[string]*si.Quantity{},
		}
		for name, value := range totalResource.Resources {
			quantity := si.Quantity{Value: int64(value)}
			proto.SchedulableResource.Resources[name] = &quantity
		}
	}
	return &proto
}

func TestNewNodeInfo(t *testing.T) {
	// simple nil check
	node := NewNodeInfo(nil)
	if node != nil {
		t.Error("node not returned correctly: node is nul or incorrect name")
	}
	proto := newProto("testnode", nil, nil)
	node = NewNodeInfo(proto)
	if node == nil || node.NodeID != "testnode" {
		t.Error("node not returned correctly: node is nul or incorrect name")
	}

	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "second": 100})
	proto = newProto("testnode", totalRes, map[string]string{})
	node = NewNodeInfo(proto)
	if node == nil || node.NodeID != "testnode" {
		t.Fatal("node not returned correctly: node is nul or incorrect name")
	}
	if !resources.Equals(node.totalResource, totalRes) ||
		!resources.Equals(node.availableResource, totalRes) {
		t.Errorf("node resources not set correctly: %v expected got %v and %v",
			totalRes, node.totalResource, node.availableResource)
	}
	assert.Equal(t, node.Partition, "")

	// set special attributes and get a new node
	proto.Attributes = map[string]string{
		api.HostName:      "host1",
		api.RackName:      "rack1",
		api.NodePartition: "partition1",
	}
	node = NewNodeInfo(proto)
	if node == nil || node.NodeID != "testnode" {
		t.Fatal("node not returned correctly: node is nul or incorrect name")
	}
	assert.Equal(t, "host1", node.Hostname)
	assert.Equal(t, "rack1", node.Rackname)
	assert.Equal(t, "partition1", node.Partition)
}

func TestAttributes(t *testing.T) {
	proto := newProto("testnode", nil, map[string]string{
		api.NodePartition: "partition1",
		"something":       "just a text",
	})

	node := NewNodeInfo(proto)
	if node == nil || node.NodeID != "testnode" {
		t.Fatal("node not returned correctly: node is nul or incorrect name")
	}

	assert.Equal(t, "", node.Hostname)
	assert.Equal(t, "", node.Rackname)
	assert.Equal(t, "partition1", node.Partition)

	value := node.GetAttribute(api.NodePartition)
	assert.Equal(t, "partition1", value, "node attributes not set, expected 'partition1' got '%v'", value)
	value = node.GetAttribute("something")
	assert.Equal(t, "just a text", value, "node attributes not set, expected 'just a text' got '%v'", value)
}

func TestAddAllocation(t *testing.T) {
	total := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "second": 200})
	node := NewNodeForTest("node-123", total)
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}

	// check nil alloc
	node.AddAllocation(nil)
	if len(node.GetAllAllocations()) > 0 {
		t.Fatalf("nil allocation should not have been added: %v", node)
	}
	// allocate half of the resources available and check the calculation
	half := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "second": 100})
	node.AddAllocation(CreateMockAllocationInfo("app1", half, "1", "queue-1", "node-1"))
	if node.GetAllocation("1") == nil {
		t.Fatal("failed to add allocations: allocation not returned")
	}
	if !resources.Equals(node.GetAllocatedResource(), half) {
		t.Errorf("failed to add allocations expected %v, got %v", half, node.GetAllocatedResource())
	}
	if !resources.Equals(node.GetAvailableResource(), half) {
		t.Errorf("failed to update available resources expected %v, got %v", half, node.GetAvailableResource())
	}

	// second and check calculation
	piece := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25, "second": 50})
	node.AddAllocation(CreateMockAllocationInfo("app1", piece, "2", "queue-1", "node-1"))
	if node.GetAllocation("2") == nil {
		t.Fatal("failed to add allocations: allocation not returned")
	}
	piece.AddTo(half)
	if !resources.Equals(node.GetAllocatedResource(), piece) {
		t.Errorf("failed to add allocations expected %v, got %v", piece, node.GetAllocatedResource())
	}
	left := resources.Sub(total, piece)
	if !resources.Equals(node.GetAvailableResource(), left) {
		t.Errorf("failed to update available resources expected %v, got %v", left, node.GetAvailableResource())
	}
}

func TestRemoveAllocation(t *testing.T) {
	total := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "second": 200})
	node := NewNodeForTest("node-123", total)
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}

	// allocate half of the resources available and check the calculation
	half := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "second": 100})
	node.AddAllocation(CreateMockAllocationInfo("app1", half, "1", "queue-1", "node-1"))
	if node.GetAllocation("1") == nil {
		t.Fatal("failed to add allocations: allocation not returned")
	}
	// check empty alloc
	if node.RemoveAllocation("") != nil {
		t.Errorf("empty allocation should not have been removed: %v", node)
	}
	if node.RemoveAllocation("not exist") != nil {
		t.Errorf("'not exist' allocation should not have been removed: %v", node)
	}
	if !resources.Equals(node.GetAllocatedResource(), half) {
		t.Errorf("allocated resource not set correctly %v got %v", half, node.GetAllocatedResource())
	}

	// add second alloc and remove first check calculation
	piece := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25, "second": 50})
	node.AddAllocation(CreateMockAllocationInfo("app1", piece, "2", "queue-1", "node-1"))
	if node.GetAllocation("2") == nil {
		t.Fatal("failed to add allocations: allocation not returned")
	}
	alloc := node.RemoveAllocation("1")
	if alloc == nil {
		t.Error("allocation should have been removed but was not")
	}
	if !resources.Equals(node.GetAllocatedResource(), piece) {
		t.Errorf("allocated resource not set correctly %v got %v", piece, node.GetAllocatedResource())
	}
	left := resources.Sub(total, piece)
	if !resources.Equals(node.GetAvailableResource(), left) {
		t.Errorf("allocated resource not set correctly %v got %v", left, node.GetAvailableResource())
	}
}

func TestCanAllocate(t *testing.T) {
	total := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 20})
	node := NewNodeForTest("node-123", total)
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}
	// ask for a resource not available and check the calculation
	more := resources.NewResourceFromMap(map[string]resources.Quantity{"unknown": 10})
	if node.canAllocate(more) {
		t.Errorf("can allocate should not have allowed %v to be allocated", more)
	}
	// ask for more than the resources available and check the calculation
	more = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 25})
	if node.canAllocate(more) {
		t.Errorf("can allocate should not have allowed %v to be allocated", more)
	}

	// ask for less than the resources available and check the calculation
	less := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 10})
	if !node.canAllocate(less) {
		t.Errorf("can allocate should have allowed %v to be allocated", less)
	}
	node.AddAllocation(CreateMockAllocationInfo("app1", less, "1", "queue-1", "node-1"))
	if !resources.Equals(node.GetAllocatedResource(), less) {
		t.Errorf("allocated resource not set correctly %v got %v", less, node.GetAllocatedResource())
	}
	// ask for more than the total resources but more than available and check the calculation
	less = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8, "second": 5})
	if node.canAllocate(less) {
		t.Errorf("can allocate should not have allowed %v to be allocated", less)
	}
}

func TestGetAllocations(t *testing.T) {
	total := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "second": 200})
	node := NewNodeForTest("node-123", total)
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}

	// nothing allocated get an empty list
	allocs := node.GetAllAllocations()
	if allocs == nil || len(allocs) != 0 {
		t.Fatalf("allocation length should be 0 on new node")
	}

	// allocate
	node.AddAllocation(CreateMockAllocationInfo("app1", nil, "1", "queue-1", "node-1"))
	node.AddAllocation(CreateMockAllocationInfo("app1", nil, "2", "queue-1", "node-1"))
	assert.Equal(t, 2, len(node.GetAllAllocations()), "allocation length mismatch")
	// This should not happen in real code just making sure the code does do what is expected
	node.AddAllocation(CreateMockAllocationInfo("app1", nil, "2", "queue-1", "node-1"))
	assert.Equal(t, 2, len(node.GetAllAllocations()), "allocation length mismatch")
}

func TestSchedulingState(t *testing.T) {
	node := NewNodeInfo(newProto("node-123", nil, nil))
	if !node.IsSchedulable() {
		t.Error("failed to initialize node: not schedulable")
	}

	node.SetSchedulable(false)
	if node.IsSchedulable() {
		t.Error("failed to modify node state: schedulable")
	}
}
