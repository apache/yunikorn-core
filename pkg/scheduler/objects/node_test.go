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
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	evtMock "github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-core/pkg/mock"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const testNode = "testnode"

func TestNewNode(t *testing.T) {
	// simple nil check
	node := NewNode(nil)
	if node != nil {
		t.Error("node not returned correctly: node is nul or incorrect name")
	}
	proto := newProto(testNode, nil, nil, nil)
	node = NewNode(proto)
	if node == nil || node.NodeID != testNode {
		t.Error("node not returned correctly: node is nul or incorrect name")
	}

	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "second": 100})
	proto = newProto(testNode, totalRes, nil, map[string]string{})
	node = NewNode(proto)
	if node == nil || node.NodeID != testNode {
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
		common.HostName:      "host1",
		common.RackName:      "rack1",
		common.NodePartition: "partition1",
	}
	node = NewNode(proto)
	if node == nil || node.NodeID != testNode {
		t.Fatal("node not returned correctly: node is nul or incorrect name")
	}
	assert.Equal(t, "host1", node.Hostname)
	assert.Equal(t, "rack1", node.Rackname)
	assert.Equal(t, "partition1", node.Partition)

	// test capacity/available/occupied resources
	totalResources := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "second": 100})
	occupiedResources := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 30, "second": 20})
	availableResources := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 70, "second": 80})
	proto = newProto(testNode, totalResources, occupiedResources, map[string]string{})
	node = NewNode(proto)
	assert.Equal(t, node.NodeID, testNode, "node not returned correctly: node is nul or incorrect name")
	if !resources.Equals(node.GetCapacity(), totalResources) {
		t.Errorf("node total resources not set correctly: %v expected got %v",
			totalResources, node.GetCapacity())
	}
	if !resources.Equals(node.GetAvailableResource(), availableResources) {
		t.Errorf("node available resources not set correctly: %v expected got %v",
			availableResources, node.GetAvailableResource())
	}
	if !resources.Equals(node.GetOccupiedResource(), occupiedResources) {
		t.Errorf("node occupied resources not set correctly: %v expected got %v",
			occupiedResources, node.GetOccupiedResource())
	}
}

func TestCheckConditions(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 100, "second": 100})
	if node == nil || node.NodeID != nodeID1 {
		t.Fatalf("node create failed which should not have %v", node)
	}

	// Check if we can allocate on scheduling node (no plugins)
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	ask := newAllocationAsk("test", "app001", res)
	if !node.preAllocateConditions(ask) {
		t.Error("node with scheduling set to true no plugins should allow allocation")
	}

	// TODO add mock for plugin to extend tests
}

func TestPreAllocateCheck(t *testing.T) {
	nodeID := nodeID1
	resNode := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 1})
	node := newNode(nodeID, resNode.Resources)
	if node == nil || node.NodeID != nodeID {
		t.Fatalf("node create failed which should not have %v", node)
	}

	// special cases
	if node.preAllocateCheck(nil, "") {
		t.Errorf("nil resource should not have fitted on node")
	}
	resNeg := resources.NewResourceFromMap(map[string]resources.Quantity{"first": -1})
	if node.preAllocateCheck(resNeg, "") {
		t.Errorf("negative resource should not have fitted on node")
	}
	// Check if we can allocate on scheduling node
	resSmall := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	resLarge := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	assert.Assert(t, node.preAllocateCheck(resNode, ""), "node resource should have fitted on node")
	assert.Assert(t, node.preAllocateCheck(resSmall, ""), "small resource should have fitted on node")
	assert.Assert(t, !node.preAllocateCheck(resLarge, ""), "too large resource should not have fitted on node")

	// unknown resource type in request is rejected always
	resOther := resources.NewResourceFromMap(map[string]resources.Quantity{"unknown": 1})
	assert.Assert(t, !node.preAllocateCheck(resOther, ""), "unknown resource type should not have fitted on node")

	// set allocated resource
	alloc := newAllocation(appID1, nodeID, resSmall)
	node.AddAllocation(alloc)
	assert.Assert(t, node.preAllocateCheck(resSmall, ""), "small resource should have fitted in available allocation")
	assert.Assert(t, !node.preAllocateCheck(resNode, ""), "node resource should not have fitted in available allocation")

	// check if we can allocate on a reserved node
	q := map[string]resources.Quantity{"first": 0}
	res := resources.NewResourceFromMap(q)
	ask := newAllocationAsk(aKey, appID1, res)
	app := newApplication(appID1, "default", "root.unknown")

	// standalone reservation unreserve returns false as app is not reserved
	reserve := newReservation(node, app, ask, false)
	node.reservations[reserve.getKey()] = reserve
	assert.Assert(t, !node.preAllocateCheck(resSmall, "app-2"), "node was reserved for different app but check passed")
	assert.Assert(t, !node.preAllocateCheck(resSmall, "app-1|alloc-2"), "node was reserved for this app but not the alloc and check passed")
	assert.Assert(t, node.preAllocateCheck(resSmall, appID1), "node was reserved for this app but check did not pass check")
	assert.Assert(t, node.preAllocateCheck(resSmall, "app-1|alloc-1"), "node was reserved for this app/alloc but check did not pass check")

	// Check if we can allocate on non scheduling node
	node.SetSchedulable(false)
	assert.Assert(t, !node.preAllocateCheck(resSmall, ""), "node with scheduling set to false should not allow allocation")
}

// Only test the CanAllocate code, the used logic in preAllocateCheck has its own test
func TestCanAllocate(t *testing.T) {
	available := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})
	request := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 1})
	other := resources.NewResourceFromMap(map[string]resources.Quantity{"unknown": 1})
	tests := []struct {
		name      string
		available *resources.Resource
		request   *resources.Resource
		want      bool
	}{
		{"all nils", nil, nil, true},
		{"nil node available", nil, request, false},
		{"all matching, req smaller", available, request, true},
		{"partial match request", available, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}), true},
		{"all matching, req larger", available, resources.Add(request, available), false},
		{"partial match available", available, resources.Add(request, other), false},
		{"unmatched request", available, other, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sn := &Node{
				availableResource: tt.available,
			}
			assert.Equal(t, sn.CanAllocate(tt.request), tt.want, "unexpected node can run result")
		})
	}
}

func TestNodeReservation(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != nodeID1 {
		t.Fatalf("node create failed which should not have %v", node)
	}
	if node.IsReserved() {
		t.Fatal("new node should not have reservations")
	}
	if node.isReservedForApp("") {
		t.Error("new node should not have reservations for empty key")
	}
	if node.isReservedForApp("unknown") {
		t.Error("new node should not have reservations for unknown key")
	}

	// reserve illegal request
	err := node.Reserve(nil, nil)
	if err == nil {
		t.Errorf("illegal reservation requested but did not fail: error %v", err)
	}

	// too large for node
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	ask := newAllocationAsk(aKey, appID1, res)
	app := newApplication(appID1, "default", "root.unknown")
	err = node.Reserve(app, ask)
	if err == nil {
		t.Errorf("requested reservation does not fit in node resource but did not fail: error %v", err)
	}

	// resource type not available on node
	res = resources.NewResourceFromMap(map[string]resources.Quantity{"unknown": 1})
	ask = newAllocationAsk(aKey, appID1, res)
	app = newApplication(appID1, "default", "root.unknown")
	err = node.Reserve(app, ask)
	if err == nil {
		t.Errorf("requested reservation does not match node resource types but did not fail: error %v", err)
	}

	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask = newAllocationAsk(aKey, appID1, res)
	app = newApplication(appID1, "default", "root.unknown")
	// reserve that works
	err = node.Reserve(app, ask)
	assert.NilError(t, err, "reservation should not have failed")
	if node.isReservedForApp("") {
		t.Error("node should not have reservations for empty key")
	}
	if node.isReservedForApp("unknown") {
		t.Errorf("node should not have reservations for unknown key")
	}
	if node.IsReserved() && !node.isReservedForApp(appID1) {
		t.Errorf("node should have reservations for app-1")
	}

	// 2nd reservation on node
	err = node.Reserve(nil, nil)
	if err == nil {
		t.Errorf("reservation requested on already reserved node: error %v", err)
	}

	// unreserve different app
	_, err = node.unReserve(nil, nil)
	if err == nil {
		t.Errorf("illegal reservation release but did not fail: error %v", err)
	}
	ask2 := newAllocationAsk("alloc-2", appID2, res)
	app2 := newApplication(appID2, "default", "root.unknown")
	var num int
	num, err = node.unReserve(app2, ask2)
	assert.NilError(t, err, "un-reserve different app should have failed without error")
	assert.Equal(t, num, 0, "un-reserve different app should have failed without releases")
	num, err = node.unReserve(app, ask)
	assert.NilError(t, err, "un-reserve should not have failed")
	assert.Equal(t, num, 1, "un-reserve app should have released ")
}

func TestIsReservedForApp(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != nodeID1 {
		t.Fatalf("node create failed which should not have %v", node)
	}
	if node.IsReserved() {
		t.Fatal("new node should not have reservations")
	}

	// check if we can allocate on a reserved node
	q := map[string]resources.Quantity{"first": 0}
	res := resources.NewResourceFromMap(q)
	ask := newAllocationAsk(aKey, appID1, res)
	app := newApplication(appID1, "default", "root.unknown")

	// standalone reservation unreserve returns false as app is not reserved
	reserve := newReservation(node, app, ask, false)
	node.reservations[reserve.getKey()] = reserve
	if node.isReservedForApp("app-2") {
		t.Error("node was reserved for different app but check passed ")
	}
	if node.isReservedForApp("app-1|alloc-2") {
		t.Error("node was reserved for this app but not the alloc and check passed ")
	}
	if !node.isReservedForApp(appID1) {
		t.Error("node was reserved for this app but check did not passed ")
	}
	if !node.isReservedForApp("app-1|alloc-1") {
		t.Error("node was reserved for this app/alloc but check did not passed ")
	}
	// app name similarity check: chop of the last char to make sure we check the full name
	similar := appID1[:len(appID1)-1]
	if node.isReservedForApp(similar) {
		t.Errorf("similar app should not have reservations on node %s", similar)
	}
}

func TestAttributes(t *testing.T) {
	type outputFormat struct {
		hostname, rackname, partition string
		attribites                    map[string]string
	}
	attribitesOfNode1 := map[string]string{common.NodePartition: "partition1", "something": "just a text"}
	attribitesOfNode2 := map[string]string{common.HostName: "test", common.NodePartition: "partition2", "disk": "SSD", "GPU-type": "3090"}
	var tests = []struct {
		inputs   map[string]string
		expected outputFormat
	}{
		{
			attribitesOfNode1,
			outputFormat{"", "", "partition1", attribitesOfNode1},
		},
		{
			attribitesOfNode2,
			outputFormat{"test", "", "partition2", attribitesOfNode2},
		},
	}
	for index, tt := range tests {
		testname := fmt.Sprintf("Attributes in the node %d", index)
		t.Run(testname, func(t *testing.T) {
			nodename := fmt.Sprintf("%s-%d", testNode, index)
			node := NewNode(newProto(nodename, nil, nil, tt.inputs))
			if node == nil || node.NodeID != nodename {
				t.Error("node not returned correctly: node is nul or incorrect name")
			}

			if got, expect := node.Hostname, tt.expected.hostname; got != expect {
				t.Errorf("node hostname: got %s, expected %s", got, expect)
			}

			if got, expect := node.Rackname, tt.expected.rackname; got != expect {
				t.Errorf("node rackname: got %s, expected %s", got, expect)
			}

			if got, expect := node.Partition, tt.expected.partition; got != expect {
				t.Errorf("node partition: got %s, expected %s", got, expect)
			}

			attribites := node.GetAttributes()
			if got, expect := len(attribites), len(tt.expected.attribites); got != expect {
				t.Errorf("length of attributes: got %d, expected %d", got, expect)
			}

			for key, expect := range tt.expected.attribites {
				if got := node.GetAttribute(key); got != expect {
					t.Errorf("Attribute %s: got %s, expect %s", key, got, expect)
				}
				if got := attribites[key]; got != expect {
					t.Errorf("Attribute %s: got %s, expect %s", key, got, expect)
				}
			}
		})
	}
}

func TestGetInstanceType(t *testing.T) {
	proto := newProto(testNode, nil, nil, map[string]string{
		common.NodePartition: "partition1",
		"label1":             "key1",
		"label2":             "key2",
		common.InstanceType:  "HighMem",
	})

	node := NewNode(proto)
	if node == nil || node.NodeID != testNode {
		t.Fatal("node not returned correctly: node is nul or incorrect name")
	}

	assert.Equal(t, "", node.Hostname)
	assert.Equal(t, "", node.Rackname)
	assert.Equal(t, "partition1", node.Partition)

	value := node.GetInstanceType()
	assert.Equal(t, "HighMem", value, "node instanceType not set, expected 'HighMem' got '%v'", value)
}

func TestAddAllocation(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}

	// check nil alloc
	assert.Assert(t, !node.AddAllocation(nil), "nil allocation should not have been added: %v", node)
	// check alloc that does not match
	unknown := resources.NewResourceFromMap(map[string]resources.Quantity{"unknown": 1})
	alloc := newAllocation(appID1, nodeID1, unknown)
	assert.Assert(t, !node.AddAllocation(alloc), "unmatched resource type in allocation should not have been added: %v", node)

	// allocate half of the resources available and check the calculation
	half := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "second": 100})
	alloc = newAllocation(appID1, nodeID1, half)
	assert.Assert(t, node.AddAllocation(alloc), "add allocation 1 should not have failed")
	if node.GetAllocation(alloc.GetAllocationID()) == nil {
		t.Fatal("failed to add allocations: allocation not returned")
	}
	if !resources.Equals(node.GetAllocatedResource(), half) {
		t.Errorf("failed to add allocations expected %v, got %v", half, node.GetAllocatedResource())
	}
	if !resources.Equals(node.GetAvailableResource(), half) {
		t.Errorf("failed to update available resources expected %v, got %v", half, node.GetAvailableResource())
	}
	expectedUtilizedResource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "second": 50})
	if !resources.Equals(node.GetUtilizedResource(), expectedUtilizedResource) {
		t.Errorf("failed to get utilized resources expected %v, got %v", expectedUtilizedResource, node.GetUtilizedResource())
	}
	// second and check calculation
	piece := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25, "second": 50})
	alloc = newAllocation(appID1, nodeID1, piece)
	assert.Assert(t, node.AddAllocation(alloc), "add allocation 2 should not have failed")
	if node.GetAllocation(alloc.GetAllocationID()) == nil {
		t.Fatal("failed to add allocations: allocation not returned")
	}
	piece.AddTo(half)
	if !resources.Equals(node.GetAllocatedResource(), piece) {
		t.Errorf("failed to add allocations expected %v, got %v", piece, node.GetAllocatedResource())
	}
	left := resources.Sub(node.GetCapacity(), piece)
	if !resources.Equals(node.GetAvailableResource(), left) {
		t.Errorf("failed to update available resources expected %v, got %v", left, node.GetAvailableResource())
	}
	expectedUtilizedResource1 := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 75, "second": 75})
	if !resources.Equals(node.GetUtilizedResource(), expectedUtilizedResource1) {
		t.Errorf("failed to get utilized resources expected %v, got %v", expectedUtilizedResource1, node.GetUtilizedResource())
	}
}

func TestRemoveAllocation(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}

	// allocate half of the resources available and check the calculation
	half := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "second": 100})
	alloc1 := newAllocation(appID1, nodeID1, half)
	node.AddAllocation(alloc1)
	if node.GetAllocation(alloc1.GetAllocationID()) == nil {
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
	alloc2 := newAllocation(appID1, nodeID1, piece)
	node.AddAllocation(alloc2)
	if node.GetAllocation(alloc2.GetAllocationID()) == nil {
		t.Fatal("failed to add allocations: allocation not returned")
	}
	alloc := node.RemoveAllocation(alloc1.GetAllocationID())
	if alloc == nil {
		t.Error("allocation should have been removed but was not")
	}
	if !resources.Equals(node.GetAllocatedResource(), piece) {
		t.Errorf("allocated resource not set correctly %v got %v", piece, node.GetAllocatedResource())
	}
	left := resources.Sub(node.GetCapacity(), piece)
	if !resources.Equals(node.GetAvailableResource(), left) {
		t.Errorf("allocated resource not set correctly %v got %v", left, node.GetAvailableResource())
	}
	expectedUtilizedResource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25, "second": 25})
	if !resources.Equals(node.GetUtilizedResource(), expectedUtilizedResource) {
		t.Errorf("failed to get utilized resources expected %v, got %v", expectedUtilizedResource, node.GetUtilizedResource())
	}
}

func TestNodeReplaceAllocation(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "failed to initialize node")

	// allocate half of the resources available and check the calculation
	half := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "second": 100})
	ph := newPlaceholderAlloc(appID1, nodeID1, half)
	node.AddAllocation(ph)
	assert.Assert(t, node.GetAllocation(ph.GetAllocationID()) != nil, "failed to add placeholder allocation")
	assert.Assert(t, resources.Equals(node.GetAllocatedResource(), half), "allocated resource not set correctly %v got %v", half, node.GetAllocatedResource())
	assert.Assert(t, resources.Equals(node.GetAvailableResource(), half), "available resource not set correctly %v got %v", half, node.GetAvailableResource())

	piece := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25, "second": 50})
	alloc := newAllocation(appID1, nodeID1, piece)
	// calculate the delta: new allocation resource - placeholder (should be negative!)
	delta := resources.Sub(piece, half)
	assert.Assert(t, delta.HasNegativeValue(), "expected negative values in delta")
	// swap and check the calculation
	node.ReplaceAllocation(ph.GetAllocationID(), alloc, delta)
	assert.Assert(t, node.GetAllocation(alloc.GetAllocationID()) != nil, "failed to replace allocation: allocation not returned")
	assert.Assert(t, resources.Equals(node.GetAllocatedResource(), piece), "allocated resource not set correctly %v got %v", piece, node.GetAllocatedResource())
	assert.Assert(t, resources.Equals(node.GetAvailableResource(), resources.Sub(node.GetCapacity(), piece)), "available resource not set correctly %v got %v", resources.Sub(node.GetCapacity(), piece), node.GetAvailableResource())

	// clean up all should be zero
	assert.Assert(t, node.RemoveAllocation(alloc.GetAllocationID()) != nil, "allocation should have been removed but was not")
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "allocated resource not updated correctly")
	assert.Assert(t, resources.Equals(node.GetAvailableResource(), node.GetCapacity()), "available resource not set correctly %v got %v", node.GetCapacity(), node.GetAvailableResource())
}

func TestGetAllocation(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}
	// nothing allocated get a nil
	alloc := node.GetAllocation("")
	if alloc != nil {
		t.Fatalf("allocation should not have been found")
	}
	alloc = newAllocation(appID1, nodeID1, nil)
	node.AddAllocation(alloc)
	alloc = node.GetAllocation(alloc.GetAllocationID())
	if alloc == nil {
		t.Fatalf("allocation should have been found")
	}
	// unknown allocation get a nil
	alloc = node.GetAllocation("fake")
	if alloc != nil {
		t.Fatalf("allocation should not have been found (fake ID)")
	}
}

func TestGetAllocations(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}

	// nothing allocated get an empty list
	allocs := node.GetAllAllocations()
	if allocs == nil || len(allocs) != 0 {
		t.Fatalf("allocation length should be 0 on new node")
	}
	alloc1 := newAllocation(appID1, nodeID1, nil)
	alloc2 := newAllocation(appID1, nodeID1, nil)

	// allocate
	node.AddAllocation(alloc1)
	node.AddAllocation(alloc2)
	assert.Equal(t, 2, len(node.GetAllAllocations()), "allocation length mismatch")
	// This should not happen in real code just making sure the code does do what is expected
	node.AddAllocation(alloc2)
	assert.Equal(t, 2, len(node.GetAllAllocations()), "allocation length mismatch")
}

func TestSchedulingState(t *testing.T) {
	node := newNode("node-123", nil)
	if !node.IsSchedulable() {
		t.Error("failed to initialize node: not schedulable")
	}

	node.SetSchedulable(false)
	if node.IsSchedulable() {
		t.Error("failed to modify node state: schedulable")
	}
}

func TestUpdateResources(t *testing.T) {
	total := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})
	node := newNodeRes("node-123", total)
	if !resources.IsZero(node.occupiedResource) || !resources.IsZero(node.allocatedResource) || !resources.Equals(total, node.GetCapacity()) {
		t.Fatalf("node not initialised correctly")
	}

	occupied := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 1})
	node.SetOccupiedResource(occupied)
	if !resources.Equals(occupied, node.GetOccupiedResource()) {
		t.Errorf("occupied resources should have been updated to: %s, got %s", occupied, node.GetOccupiedResource())
	}
	available := resources.Sub(total, occupied)
	if !resources.Equals(available, node.GetAvailableResource()) {
		t.Errorf("available resources should have been updated to: %s, got %s", available, node.GetAvailableResource())
	}

	// adjust capacity check available updated
	total = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 5})
	node.SetCapacity(total)
	if !resources.Equals(total, node.GetCapacity()) {
		t.Errorf("total resources should have been updated to: %s, got %s", total, node.GetCapacity())
	}
	available = resources.Sub(total, occupied)
	if !resources.Equals(available, node.GetAvailableResource()) {
		t.Errorf("available resources should have been updated to: %s, got %s", available, node.GetAvailableResource())
	}

	// over allocate available should go negative and no error
	occupied = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 10})
	node.SetOccupiedResource(occupied)
	if !resources.Equals(occupied, node.GetOccupiedResource()) {
		t.Errorf("occupied resources should have been updated to: %s, got %s", occupied, node.GetOccupiedResource())
	}
	available = resources.Sub(total, occupied)
	if !resources.Equals(available, node.GetAvailableResource()) {
		t.Errorf("available resources should have been updated to: %s, got %s", available, node.GetAvailableResource())
	}

	// reset and check with allocated
	total = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})
	node = newNodeRes("node-123", total)
	if !resources.IsZero(node.occupiedResource) || !resources.IsZero(node.allocatedResource) || !resources.Equals(total, node.GetCapacity()) {
		t.Fatalf("node not initialised correctly")
	}
	alloc := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 5})
	node.allocatedResource = alloc.Clone()
	available = resources.Sub(total, alloc)
	// fake the update to recalculate available
	node.refreshAvailableResource()
	if !resources.Equals(available, node.GetAvailableResource()) {
		t.Errorf("available resources should have been updated to: %s, got %s", available, node.GetAvailableResource())
	}
	// set occupied and make sure available changes
	occupied = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 3, "second": 1})
	node.SetOccupiedResource(occupied)
	if !resources.Equals(occupied, node.GetOccupiedResource()) {
		t.Errorf("occupied resources should have been updated to: %s, got %s", occupied, node.GetOccupiedResource())
	}
	available.SubFrom(occupied)
	if !resources.Equals(available, node.GetAvailableResource()) {
		t.Errorf("available resources should have been updated to: %s, got %s", available, node.GetAvailableResource())
	}
}

type testListener struct {
	updateCount int
}

func (tl *testListener) NodeUpdated(_ *Node) {
	tl.updateCount++
}

func TestAddRemoveListener(t *testing.T) {
	tl := testListener{}
	total := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})
	node := newNodeRes("node-123", total)
	node.AddListener(&tl)
	assert.Equal(t, 0, tl.updateCount, "listener should not have fired")
	node.SetSchedulable(false)
	assert.Equal(t, 1, tl.updateCount, "listener should have fired once")
	node.RemoveListener(&tl)
	node.SetSchedulable(true)
	assert.Equal(t, 1, tl.updateCount, "listener should not have fired again")
}

func TestNodeEvents(t *testing.T) {
	mockEvents := evtMock.NewEventSystem()
	total := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 100, "memory": 100})
	occupied := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10, "memory": 10})
	proto := newProto(testNode, total, occupied, map[string]string{
		"ready": "true",
	})
	node := NewNode(proto)
	node.nodeEvents = newNodeEvents(node, mockEvents)

	node.SendNodeAddedEvent()
	assert.Equal(t, 1, len(mockEvents.Events))
	event := mockEvents.Events[0]
	assert.Equal(t, si.EventRecord_NODE, event.Type)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)

	mockEvents.Reset()
	node.SendNodeRemovedEvent()
	assert.Equal(t, 1, len(mockEvents.Events))
	event = mockEvents.Events[0]
	assert.Equal(t, si.EventRecord_NODE, event.Type)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)

	mockEvents.Reset()
	node.AddAllocation(&Allocation{
		allocatedResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 1, "memory": 1}),
		allocationKey:     aKey,
		allocationID:      "allocationid-0",
	})
	assert.Equal(t, 1, len(mockEvents.Events))
	event = mockEvents.Events[0]
	assert.Equal(t, si.EventRecord_NODE, event.Type)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_ALLOC, event.EventChangeDetail)

	mockEvents.Reset()
	node.RemoveAllocation("allocationid-0")
	event = mockEvents.Events[0]
	assert.Equal(t, si.EventRecord_NODE, event.Type)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_ALLOC, event.EventChangeDetail)

	mockEvents.Reset()
	node.SetOccupiedResource(resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 20, "memory": 20}))
	event = mockEvents.Events[0]
	assert.Equal(t, si.EventRecord_NODE, event.Type)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_OCCUPIED, event.EventChangeDetail)
	assert.Equal(t, int64(20), event.Resource.Resources["cpu"].Value)
	assert.Equal(t, int64(20), event.Resource.Resources["memory"].Value)

	mockEvents.Reset()
	node.SetCapacity(resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 90, "memory": 90}))
	event = mockEvents.Events[0]
	assert.Equal(t, si.EventRecord_NODE, event.Type)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_CAPACITY, event.EventChangeDetail)
	assert.Equal(t, int64(90), event.Resource.Resources["cpu"].Value)
	assert.Equal(t, int64(90), event.Resource.Resources["memory"].Value)

	mockEvents.Reset()
	node.SetSchedulable(false)
	event = mockEvents.Events[0]
	assert.Equal(t, si.EventRecord_NODE, event.Type)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_SCHEDULABLE, event.EventChangeDetail)

	mockEvents.Reset()
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10})
	ask := newAllocationAsk(aKey, appID1, res)
	app := newApplication(appID1, "default", "root.unknown")
	err := node.Reserve(app, ask)
	assert.NilError(t, err, "could not reserve")
	event = mockEvents.Events[0]
	assert.Equal(t, si.EventRecord_NODE, event.Type)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_RESERVATION, event.EventChangeDetail)

	mockEvents.Reset()
	_, err = node.unReserve(app, ask)
	assert.NilError(t, err, "could not unreserve")
	event = mockEvents.Events[0]
	assert.Equal(t, si.EventRecord_NODE, event.Type)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_RESERVATION, event.EventChangeDetail)
}

func TestNode_FitInNode(t *testing.T) {
	total := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})
	request := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 1})
	other := resources.NewResourceFromMap(map[string]resources.Quantity{"unknown": 1})
	tests := []struct {
		name       string
		totalRes   *resources.Resource
		resRequest *resources.Resource
		want       bool
	}{
		{"all nils", nil, nil, true},
		{"nil node size", nil, request, false},
		{"all matching, req smaller", total, request, true},
		{"partial match request", total, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}), true},
		{"all matching, req larger", total, resources.Add(request, total), false},
		{"partial match total", total, resources.Add(request, other), false},
		{"unmatched request", total, other, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sn := &Node{
				totalResource: tt.totalRes,
			}
			assert.Equal(t, sn.FitInNode(tt.resRequest), tt.want, "unexpected node fit result")
		})
	}
}

func TestPreconditions(t *testing.T) {
	current := plugins.GetResourceManagerCallbackPlugin()
	defer func() {
		plugins.RegisterSchedulerPlugin(current)
	}()

	plugins.RegisterSchedulerPlugin(mock.NewPredicatePlugin(true, map[string]int{}))
	total := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 100, "memory": 100})
	occupied := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10, "memory": 10})
	proto := newProto(testNode, total, occupied, map[string]string{
		"ready": "true",
	})
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	ask := newAllocationAsk("test", "app001", res)
	eventSystem := evtMock.NewEventSystem()
	ask.askEvents = newAskEvents(ask, eventSystem)
	node := NewNode(proto)

	// failure
	node.preConditions(ask, true)
	assert.Equal(t, 1, len(eventSystem.Events))
	assert.Equal(t, "Predicate failed for request 'test' with message: 'fake predicate plugin failed'", eventSystem.Events[0].Message)
	assert.Equal(t, 1, len(ask.allocLog))
	assert.Equal(t, "fake predicate plugin failed", ask.allocLog["fake predicate plugin failed"].Message)

	// pass
	eventSystem.Reset()
	plugins.RegisterSchedulerPlugin(mock.NewPredicatePlugin(false, map[string]int{}))
	node.preConditions(ask, true)
	assert.Equal(t, 0, len(eventSystem.Events))
	assert.Equal(t, 1, len(ask.allocLog))
}
