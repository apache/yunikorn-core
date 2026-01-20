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
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/google/go-cmp/cmp"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	evtMock "github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-core/pkg/mock"
	"github.com/apache/yunikorn-core/pkg/plugins"
	schedEvt "github.com/apache/yunikorn-core/pkg/scheduler/objects/events"
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
	proto := newProto(testNode, nil, nil)
	node = NewNode(proto)
	if node == nil || node.NodeID != testNode {
		t.Error("node not returned correctly: node is nul or incorrect name")
	}

	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "second": 100})
	proto = newProto(testNode, totalRes, map[string]string{})
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

	// test capacity/available resources
	totalResources := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "second": 100})
	availableResources := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 70, "second": 80})
	proto = newProto(testNode, totalResources, map[string]string{})
	node = NewNode(proto)
	assert.Equal(t, node.NodeID, testNode, "node not returned correctly: node is nul or incorrect name")
	if !resources.Equals(node.GetCapacity(), totalResources) {
		t.Errorf("node total resources not set correctly: %v expected got %v",
			totalResources, node.GetCapacity())
	}
	if !resources.Equals(node.GetAvailableResource(), totalResources) {
		t.Errorf("node available resources not set correctly: %v expected got %v",
			availableResources, node.GetAvailableResource())
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
	if node.preAllocateConditions(ask) != nil {
		t.Error("node with scheduling set to true no plugins should allow allocation")
	}
}

func TestPreAllocateCheck(t *testing.T) {
	resNode := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 1})
	node := newNode(nodeID1, resNode.Resources)
	if node == nil || node.NodeID != nodeID1 {
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
	alloc := newAllocation(appID1, nodeID1, resSmall)
	node.AddAllocation(alloc)
	assert.Assert(t, node.preAllocateCheck(resSmall, ""), "small resource should have fitted in available allocation")
	assert.Assert(t, !node.preAllocateCheck(resNode, ""), "node resource should not have fitted in available allocation")

	// check if we can allocate on a reserved node
	ask := newAllocationAsk(aKey, appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0}))
	app := newApplication(appID1, "default", "root.unknown")

	// standalone reservation unreserve returns false as app is not reserved
	reserve := newReservation(node, app, ask, false)
	node.reservations[reserve.allocKey] = reserve
	assert.Assert(t, !node.preAllocateCheck(resSmall, aKey2), "node was reserved for different app but check passed")
	assert.Assert(t, node.preAllocateCheck(resSmall, aKey), "node was reserved for this app/alloc but check did not pass check")

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
			assert.Equal(t, sn.CanAllocate(tt.request), tt.want, "unexpected node can run resultType")
		})
	}
}

func TestNodeReservation(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != nodeID1 {
		t.Fatalf("node create failed which should not have %v", node)
	}
	assert.Assert(t, !node.IsReserved(), "new node should not have reservations")
	assert.Assert(t, !node.isReservedForAllocation(""), "new node should not have reservations for empty key")
	assert.Assert(t, !node.isReservedForAllocation("unknown"), "new node should not have reservations for unknown key")

	// reserve illegal request
	err := node.Reserve(nil, nil)
	if err == nil {
		t.Fatal("illegal reservation requested but did not fail")
	}

	// too large for node
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	ask := newAllocationAsk(aKey, appID1, res)
	app := newApplication(appID1, "default", "root.unknown")
	err = node.Reserve(app, ask)
	if err == nil {
		t.Fatal("requested reservation does not fit in node resource but did not fail")
	}

	// resource type not available on node
	res = resources.NewResourceFromMap(map[string]resources.Quantity{"unknown": 1})
	ask = newAllocationAsk(aKey, appID1, res)
	app = newApplication(appID1, "default", "root.unknown")
	err = node.Reserve(app, ask)
	if err == nil {
		t.Fatal("requested reservation does not match node resource types but did not fail")
	}

	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask = newAllocationAsk(aKey, appID1, res)
	app = newApplication(appID1, "default", "root.unknown")
	// reserve that works
	err = node.Reserve(app, ask)
	assert.NilError(t, err, "reservation should not have failed")
	assert.Assert(t, !node.isReservedForAllocation(""), "node should not have reservations for empty key")
	assert.Assert(t, !node.isReservedForAllocation("unknown"), "node should not have reservations for unknown key")
	assert.Assert(t, node.IsReserved(), "node should have been reserved")
	assert.Assert(t, node.isReservedForAllocation(aKey), "node should have reservations for alloc-1")

	// 2nd reservation on node
	app2 := newApplication(appID2, "default", "root.unknown")
	ask2 := newAllocationAsk("alloc-2", appID2, res)
	err = node.Reserve(app2, ask2)
	if err == nil {
		t.Fatal("reservation requested on already reserved node")
	}

	// unreserve different alloc
	num := node.unReserve(nil)
	assert.Equal(t, num, 0, "un-reserve different alloc should have failed without releases")
	num = node.unReserve(ask2)
	assert.Equal(t, num, 0, "un-reserve different alloc should have failed without releases")
	num = node.unReserve(ask)
	assert.Equal(t, num, 1, "un-reserve app should have released ")
}

func TestRequiredNodeAfterReservation(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != nodeID1 {
		t.Fatalf("node create failed which should not have %v", node)
	}
	if node.IsReserved() {
		t.Fatal("new node should not have reservations")
	}
	// normal node reservation
	allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAsk(aKey, appID1, allocRes)
	app := newApplication(appID1, "default", "root.unknown")
	err := node.Reserve(app, ask)
	assert.NilError(t, err, "normal node reservation should not have failed")
	assert.Assert(t, node.IsReserved(), "node should have been reserved")
	assert.Assert(t, node.isReservedForAllocation(aKey), "node should have reservations for alloc-1")

	ask2 := newAllocationAsk(aKey2, appID1, allocRes)
	ask2.requiredNode = nodeID1
	app = newApplication(appID1, "default", "root.unknown")
	// required node as 2nd reservation
	err = node.Reserve(app, ask2)
	if err == nil {
		t.Fatalf("adding required node reservation should have failed")
	}
}

func TestMultiRequiredNodeReservation(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != nodeID1 {
		t.Fatalf("node create failed which should not have %v", node)
	}
	if node.IsReserved() {
		t.Fatal("new node should not have reservations")
	}

	// required node reservation
	allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAsk(aKey, appID1, allocRes)
	ask.requiredNode = nodeID1
	app := newApplication(appID1, "default", "root.unknown")
	err := node.Reserve(app, ask)
	assert.NilError(t, err, "required node reservation should not have failed")
	assert.Assert(t, node.IsReserved(), "node should have been reserved")
	assert.Assert(t, node.isReservedForAllocation(aKey), "node should have reservations for alloc-1")

	ask2 := newAllocationAsk(aKey2, appID1, allocRes)
	app = newApplication(appID1, "default", "root.unknown")
	// non required node as 2nd reservation
	err = node.Reserve(app, ask2)
	if err == nil {
		t.Fatalf("adding to required node reservation should have failed")
	}

	// required node as 2nd reservation on node
	ask2.requiredNode = nodeID1
	err = node.Reserve(app, ask2)
	assert.NilError(t, err, "required node reservation should not have failed")
	assert.Assert(t, node.IsReserved(), "node should have been reserved")
	assert.Assert(t, node.isReservedForAllocation(aKey2), "node should have reservations for alloc-2")
}

func TestIsReservedForAllocation(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != nodeID1 {
		t.Fatalf("node create failed which should not have %v", node)
	}
	assert.Assert(t, !node.IsReserved(), "new node should not have reservations")

	// check if we can allocate on a reserved node
	ask := newAllocationAsk(aKey, appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0}))
	app := newApplication(appID1, "default", "root.unknown")

	// standalone reservation unreserve returns false as app is not reserved
	reserve := newReservation(node, app, ask, false)
	node.reservations[reserve.allocKey] = reserve
	assert.Assert(t, !node.isReservedForAllocation(aKey2), "node was reserved for different alloc but check passed ")
	assert.Assert(t, node.isReservedForAllocation(aKey), "node was reserved for this alloc but check did not passed ")
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
			node := NewNode(newProto(nodename, nil, tt.inputs))
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
	proto := newProto(testNode, nil, map[string]string{
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
	node.AddAllocation(nil)
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "nil allocation should not have been added: %v", node)

	// check alloc that is over quota
	large := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 200})
	alloc := newAllocation(appID1, nodeID1, large)
	node.AddAllocation(alloc)
	if !resources.Equals(node.GetAllocatedResource(), large) {
		t.Errorf("failed to add large allocation expected %v, got %v", large, node.GetAllocatedResource())
	}
	node.RemoveAllocation(alloc.GetAllocationKey())
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "removal of large allocation should return to zero %v, got %v", node, node.GetAllocatedResource())

	// check alloc that is over quota
	half := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "second": 100})
	alloc = newAllocation(appID1, nodeID1, half)
	node.AddAllocation(alloc)
	if !resources.Equals(node.GetAllocatedResource(), half) {
		t.Errorf("failed to add half allocation expected %v, got %v", half, node.GetAllocatedResource())
	}
	node.RemoveAllocation(alloc.GetAllocationKey())
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "removal of half allocation should return to zero %v, got %v", node, node.GetAllocatedResource())
}

func TestAddForeignAllocation(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}

	// check foreign alloc that is over quota
	large := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 200})
	falloc1 := newForeignAllocation("foreign-1", "node-123", large)
	node.AddAllocation(falloc1)
	assert.Assert(t, resources.Equals(large, node.GetOccupiedResource()), "invalid occupied resource - expected %v got %v",
		large, node.GetOccupiedResource())
	assert.Assert(t, resources.Equals(resources.Zero, node.GetAllocatedResource()), "allocated resource is not zero: %v", node.GetAllocatedResource())
	assert.Assert(t, resources.Equals(resources.Zero, node.GetUtilizedResource()), "utilized resource is not zero: %v", node.GetUtilizedResource())
	node.RemoveAllocation("foreign-1")

	// check foreign alloc that is below quota
	half := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "second": 100})
	falloc2 := newForeignAllocation("foreign-2", "node-123", half)
	node.AddAllocation(falloc2)
	assert.Assert(t, resources.Equals(half, node.GetOccupiedResource()), "invalid occupied resource - expected %v got %v",
		large, node.GetOccupiedResource())
	assert.Assert(t, resources.Equals(resources.Zero, node.GetAllocatedResource()), "allocated resource is not zero: %v", node.GetAllocatedResource())
	assert.Assert(t, resources.Equals(resources.Zero, node.GetUtilizedResource()), "utilized resource is not zero: %v", node.GetUtilizedResource())
}

func TestTryAddAllocation(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}
	listener := &testListener{}
	node.AddListener(listener)

	// check nil alloc
	assert.Assert(t, !node.TryAddAllocation(nil), "nil allocation should not have been added: %v", node)
	assert.Equal(t, 0, listener.updateCount, "unexpected listener update")
	// check alloc that does not match
	unknown := resources.NewResourceFromMap(map[string]resources.Quantity{"unknown": 1})
	alloc := newAllocation(appID1, nodeID1, unknown)
	assert.Assert(t, !node.TryAddAllocation(alloc), "unmatched resource type in allocation should not have been added: %v", node)
	assert.Equal(t, 0, listener.updateCount, "unexpected listener update")

	// allocate half of the resources available and check the calculation
	half := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "second": 100})
	alloc = newAllocation(appID1, nodeID1, half)
	assert.Assert(t, node.TryAddAllocation(alloc), "add allocation 1 should not have failed")
	assert.Equal(t, 1, listener.updateCount, "listener update expected")
	if node.GetAllocation(alloc.GetAllocationKey()) == nil {
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
	assert.Assert(t, node.TryAddAllocation(alloc), "add allocation 2 should not have failed")
	assert.Equal(t, 2, listener.updateCount, "listener update expected")
	if node.GetAllocation(alloc.GetAllocationKey()) == nil {
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

func TestTryAddForeignAllocation(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}
	listener := &testListener{}
	node.AddListener(listener)

	// first allocation - doesn't fit
	fRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 200, "second": 200})
	fAlloc1 := newForeignAllocation("foreign-1", "node-123", fRes)
	assert.Assert(t, !node.TryAddAllocation(fAlloc1), "add allocation should have failed")
	assert.Equal(t, 0, listener.updateCount, "unexpected listener update")
	assert.Assert(t, resources.Equals(node.GetOccupiedResource(), resources.Zero), "occupied resource is not zero: %v", node.GetOccupiedResource())
	assert.Assert(t, resources.Equals(node.GetAllocatedResource(), resources.Zero), "allocated resource is not zero: %v", node.GetAllocatedResource())
	assert.Assert(t, resources.Equals(node.GetUtilizedResource(), resources.Zero), "utilized resource is not zero: %v", node.GetUtilizedResource())

	// second allocation - fits
	fRes = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 20, "second": 20})
	fAlloc2 := newForeignAllocation("foreign-2", "node-123", fRes)
	assert.Assert(t, node.TryAddAllocation(fAlloc2), "add allocation should have succeeded")
	assert.Equal(t, 0, listener.updateCount, "unexpected listener update")
	assert.Assert(t, node.GetAllocation("foreign-2") != nil, "failed to add allocations: allocation foreign-2 not returned")
	assert.Assert(t, resources.Equals(node.GetOccupiedResource(), fRes), "occupied resource is not zero: %v", node.GetOccupiedResource())
	assert.Assert(t, resources.Equals(node.GetAllocatedResource(), resources.Zero), "allocated resource is not zero: %v", node.GetAllocatedResource())
	assert.Assert(t, resources.Equals(node.GetUtilizedResource(), resources.Zero), "utilized resource is not zero: %v", node.GetUtilizedResource())

	// third allocation - fits
	fRes = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25, "second": 35})
	fAlloc3 := newForeignAllocation("foreign-3", "node-123", fRes)
	assert.Assert(t, node.TryAddAllocation(fAlloc3), "add allocation should have succeeded")
	assert.Equal(t, 0, listener.updateCount, "unexpected listener update")
	assert.Assert(t, node.GetAllocation("foreign-3") != nil, "failed to add allocations: allocation foreign-2 not returned")
	expectedOccupied := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 45, "second": 55})
	assert.Assert(t, resources.Equals(node.GetOccupiedResource(), expectedOccupied), "occupied resource calculation is invalid - expected %v got %v",
		expectedOccupied, node.GetOccupiedResource())
	assert.Assert(t, resources.Equals(node.GetAllocatedResource(), resources.Zero), "allocated resource is not zero: %v", node.GetAllocatedResource())
	assert.Assert(t, resources.Equals(node.GetUtilizedResource(), resources.Zero), "utilized resource is not zero: %v", node.GetUtilizedResource())
}

func TestRemoveAllocation(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}
	listener := &testListener{}
	node.AddListener(listener)

	// allocate half of the resources available and check the calculation
	half := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "second": 100})
	alloc1 := newAllocation(appID1, nodeID1, half)
	node.AddAllocation(alloc1)
	assert.Equal(t, 1, listener.updateCount, "listener update expected")
	if node.GetAllocation(alloc1.GetAllocationKey()) == nil {
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
	assert.Equal(t, 1, listener.updateCount, "unexpected listener update for removal")
	listener.updateCount = 0

	// add second alloc and remove first check calculation
	piece := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25, "second": 50})
	alloc2 := newAllocation(appID1, nodeID1, piece)
	node.AddAllocation(alloc2)
	if node.GetAllocation(alloc2.GetAllocationKey()) == nil {
		t.Fatal("failed to add allocations: allocation not returned")
	}
	alloc := node.RemoveAllocation(alloc1.GetAllocationKey())
	assert.Equal(t, 2, listener.updateCount, "update expected for add+removal")
	listener.updateCount = 0
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
	_ = node.RemoveAllocation(alloc2.GetAllocationKey())
	assert.Equal(t, 1, listener.updateCount, "update expected for removal")
	listener.updateCount = 0
}

func TestRemoveForeignAllocation(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}
	listener := &testListener{}
	node.AddListener(listener)

	fRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11, "second": 11})
	fAlloc1 := newForeignAllocation("foreign-1", "node-123", fRes)
	node.AddAllocation(fAlloc1)
	assert.Assert(t, node.GetAllocation("foreign-1") != nil, "failed to add allocations: allocation foreign-1 not returned")
	fAlloc2 := newForeignAllocation("foreign-2", "node-123", fRes)
	node.AddAllocation(fAlloc2)
	assert.Assert(t, node.GetAllocation("foreign-2") != nil, "failed to add allocations: allocation foreign-2 not returned")
	node.RemoveAllocation("foreign-1")
	assert.Equal(t, 0, listener.updateCount, "no update expected for foreign allocation add+removal")
	expectedOccupied := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11, "second": 11})
	assert.Assert(t, resources.Equals(node.GetOccupiedResource(), expectedOccupied), "occupied resource calculation is invalid - expected %v got %v",
		expectedOccupied, node.GetOccupiedResource())
	expectedAvailable := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 89, "second": 189})
	assert.Assert(t, resources.Equals(node.GetAvailableResource(), expectedAvailable), "available resource calculation is invalid - expected %v got %v",
		expectedAvailable, node.GetAvailableResource())
	assert.Assert(t, resources.Equals(node.GetAllocatedResource(), resources.Zero), "allocated resource calculation is invalid - expected zero got %v",
		node.GetAllocatedResource())
}

func TestNodeReplaceAllocation(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "failed to initialize node")

	// allocate half of the resources available and check the calculation
	half := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "second": 100})
	ph := newPlaceholderAlloc(appID1, nodeID1, half, "tg")
	node.AddAllocation(ph)
	assert.Assert(t, node.GetAllocation(ph.GetAllocationKey()) != nil, "failed to add placeholder allocation")
	assert.Assert(t, resources.Equals(node.GetAllocatedResource(), half), "allocated resource not set correctly %v got %v", half, node.GetAllocatedResource())
	assert.Assert(t, resources.Equals(node.GetAvailableResource(), half), "available resource not set correctly %v got %v", half, node.GetAvailableResource())

	piece := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25, "second": 50})
	alloc := newAllocation(appID1, nodeID1, piece)
	// calculate the delta: new allocation resource - placeholder (should be negative!)
	delta := resources.Sub(piece, half)
	assert.Assert(t, delta.HasNegativeValue(), "expected negative values in delta")
	// swap and check the calculation
	node.ReplaceAllocation(ph.GetAllocationKey(), alloc, delta)
	assert.Assert(t, node.GetAllocation(alloc.GetAllocationKey()) != nil, "failed to replace allocation: allocation not returned")
	assert.Assert(t, resources.Equals(node.GetAllocatedResource(), piece), "allocated resource not set correctly %v got %v", piece, node.GetAllocatedResource())
	assert.Assert(t, resources.Equals(node.GetAvailableResource(), resources.Sub(node.GetCapacity(), piece)), "available resource not set correctly %v got %v", resources.Sub(node.GetCapacity(), piece), node.GetAvailableResource())

	// clean up all should be zero
	assert.Assert(t, node.RemoveAllocation(alloc.GetAllocationKey()) != nil, "allocation should have been removed but was not")
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "allocated resource not updated correctly")
	assert.Assert(t, resources.Equals(node.GetAvailableResource(), node.GetCapacity()), "available resource not set correctly %v got %v", node.GetCapacity(), node.GetAvailableResource())
}

func TestNodeUpdateAllocatedResource(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "failed to initialize node")
	piece := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25, "second": 50})
	alloc := newAllocation(appID1, nodeID1, piece)
	node.AddAllocation(alloc)
	assert.Assert(t, resources.Equals(piece, node.GetAllocatedResource()), "node resources not set correctly")

	delta := resources.NewResourceFromMap(map[string]resources.Quantity{"first": -10, "second": 10})
	diff := resources.Add(piece, delta)
	node.UpdateAllocatedResource(delta)
	assert.Assert(t, resources.Equals(diff, node.GetAllocatedResource()), "node resources not updated correctly")
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
	alloc = node.GetAllocation(alloc.GetAllocationKey())
	if alloc == nil {
		t.Fatalf("allocation should have been found")
	}
	// unknown allocation get a nil
	alloc = node.GetAllocation("fake")
	if alloc != nil {
		t.Fatalf("allocation should not have been found (fake key)")
	}
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
	total = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10, "unknown": 0})
	prunedTotal := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})
	node = newNodeRes("node-123", total)
	if !resources.IsZero(node.occupiedResource) || !resources.IsZero(node.allocatedResource) || !resources.Equals(total, node.GetCapacity()) {
		t.Fatalf("node not initialised correctly")
	}
	alloc := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 5})
	node.allocatedResource = alloc.Clone()
	available = resources.Sub(total, alloc)
	// fake the update to recalculate available
	node.refreshAvailableResource()
	assert.Assert(t, resources.Equals(node.GetCapacity(), prunedTotal), "total resource should be equal to the pruned total")
	assert.Assert(t, resources.Equals(available, node.GetAvailableResource()), "available resources should have been updated to: %s, got %s", available, node.GetAvailableResource())

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
	proto := newProto(testNode, total, map[string]string{
		"ready": "true",
	})
	node := NewNode(proto)
	node.nodeEvents = schedEvt.NewNodeEvents(mockEvents)

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
	})
	assert.Equal(t, 1, len(mockEvents.Events))
	event = mockEvents.Events[0]
	assert.Equal(t, si.EventRecord_NODE, event.Type)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_ALLOC, event.EventChangeDetail)

	mockEvents.Reset()
	node.RemoveAllocation(aKey)
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
	assert.Equal(t, node.unReserve(ask), 1, "expected the reservation to be removed")
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
			assert.Equal(t, sn.FitInNode(tt.resRequest), tt.want, "unexpected node fit resultType")
		})
	}
}

func TestPreconditions(t *testing.T) {
	defer plugins.UnregisterSchedulerPlugins()

	plugins.RegisterSchedulerPlugin(mock.NewPredicatePlugin(true, map[string]int{}))
	total := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 100, "memory": 100})
	proto := newProto(testNode, total, map[string]string{
		"ready": "true",
	})
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	ask := newAllocationAsk("test", "app001", res)
	node := NewNode(proto)

	// failure
	err := node.preConditions(ask, true)
	assert.ErrorContains(t, err, "fake predicate plugin failed")
	assert.Equal(t, 1, len(ask.allocLog))
	assert.Equal(t, "fake predicate plugin failed", ask.allocLog["fake predicate plugin failed"].Message)

	// pass
	plugins.RegisterSchedulerPlugin(mock.NewPredicatePlugin(false, map[string]int{}))
	err = node.preConditions(ask, true)
	assert.NilError(t, err)
	assert.Equal(t, 1, len(ask.allocLog))
}

func TestGetAllocations(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 100, "second": 200})
	allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 20})

	// no allocations
	assert.Equal(t, 0, len(node.GetForeignAllocations()))
	assert.Equal(t, 0, len(node.GetYunikornAllocations()))

	alloc1 := newAllocationWithKey(aKey, appID1, nodeID1, nil)
	alloc2 := newAllocationWithKey(aKey2, appID1, nodeID1, nil)
	alloc3 := newForeignAllocation(foreignAlloc1, nodeID1, allocRes)
	alloc4 := newForeignAllocation(foreignAlloc2, nodeID1, allocRes)
	node.AddAllocation(alloc1)
	node.AddAllocation(alloc2)
	node.AddAllocation(alloc3)
	node.AddAllocation(alloc4)

	t.Run("GetYunikornAllocations", func(t *testing.T) {
		allocs := node.GetYunikornAllocations()
		assert.Equal(t, 2, len(allocs))
		m := map[string]bool{}
		m[allocs[0].allocationKey] = true
		m[allocs[1].allocationKey] = true
		assert.Assert(t, m[aKey])
		assert.Assert(t, m[aKey2])
	})

	t.Run("GetForeignAllocations", func(t *testing.T) {
		allocs := node.GetForeignAllocations()
		assert.Equal(t, 2, len(allocs))
		m := map[string]bool{}
		m[allocs[0].allocationKey] = true
		m[allocs[1].allocationKey] = true
		assert.Assert(t, m[foreignAlloc1])
		assert.Assert(t, m[foreignAlloc2])
	})
}

func TestUpdateForeignAllocation(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 100, "second": 200})
	allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 20})
	alloc := newForeignAllocation(foreignAlloc1, nodeID1, allocRes)
	node.AddAllocation(alloc)

	// update existing allocation
	updatedRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15, "second": 0})
	allocUpd := newForeignAllocation(foreignAlloc1, nodeID1, updatedRes)
	prev := node.UpdateForeignAllocation(allocUpd)
	expectedOccupied := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	expectedAvailable := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 85, "second": 200})
	assert.Assert(t, prev == alloc, "returned previous allocation is different")
	assert.Assert(t, resources.Equals(node.GetOccupiedResource(), expectedOccupied), "occupied resource has been updated incorrectly")
	assert.Assert(t, resources.Equals(node.GetAllocatedResource(), resources.Zero), "allocated resource has changed")
	assert.Assert(t, resources.Equals(node.GetAvailableResource(), expectedAvailable), "avaiable resource has been updated incorrectly")

	// update non-existing allocation
	alloc2 := newForeignAllocation(foreignAlloc2, nodeID1, allocRes)
	prev = node.UpdateForeignAllocation(alloc2)
	assert.Assert(t, prev == nil, "unexpected previous allocation returned")
	assert.Assert(t, node.GetAllocation(foreignAlloc2) == alloc2, "foreign allocation not found")
}

func TestTotalResourcePrune(t *testing.T) {
	total := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10, "memory": 10, "gpu": 0})
	prunedTotal := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 10, "memory": 10})
	t.Run("NewNode prunes during initialization", func(t *testing.T) {
		proto := newProto("node-with-zero-gpu", total, map[string]string{
			"ready": "true",
		})
		node := NewNode(proto)
		assert.Assert(t, !resources.DeepEquals(node.totalResource, total), "total resource should not be equal to the input")
		assert.Assert(t, resources.DeepEquals(node.totalResource, prunedTotal), "total resource should be equal to the input")
	})

	t.Run("NewNode will not prune if no zero values", func(t *testing.T) {
		proto := newProto("node-with-zero-gpu", prunedTotal, map[string]string{
			"ready": "true",
		})
		node := NewNode(proto)
		assert.Assert(t, resources.DeepEquals(node.totalResource, prunedTotal), "total resource should be equal to the input")
	})

	t.Run("GetCapacity prunes zero values", func(t *testing.T) {
		proto := newProto("node-with-zero-gpu", total, map[string]string{
			"ready": "true",
		})
		node := NewNode(proto)
		assert.Assert(t, resources.DeepEquals(node.GetCapacity(), prunedTotal), "total resource should be equal to the input")
	})

	t.Run("SetCapacity prunes zero values", func(t *testing.T) {
		setTotal := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 1, "memory": 10, "gpu": 0})
		setPrunedTotal := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 1, "memory": 10})
		proto := newProto("node-with-zero-gpu", total, map[string]string{
			"ready": "true",
		})
		node := NewNode(proto)
		node.SetCapacity(setTotal)
		assert.Assert(t, resources.DeepEquals(node.GetCapacity(), setPrunedTotal), "total resource should be equal to the input")
	})

	t.Run("SetOccupiedResource prunes zero values", func(t *testing.T) {
		proto := newProto("node-with-zero-gpu", total, map[string]string{
			"ready": "true",
		})
		node := NewNode(proto)

		// Force the total resource to be the same as the input
		node.totalResource = total
		node.SendNodeAddedEvent()
		assert.Assert(t, resources.DeepEquals(node.GetCapacity(), prunedTotal), "total resource should be equal to the input")
	})

	t.Run("prunes zero in total and available", func(t *testing.T) {
		sn := &Node{
			totalResource: total,
		}
		sn.refreshAvailableResource()
		assert.Assert(t, resources.DeepEquals(sn.totalResource, prunedTotal), "total resource should be equal to the pruned total")
		assert.Assert(t, resources.DeepEquals(sn.GetAvailableResource(), prunedTotal), "available resource should be equal to the input")
	})

	t.Run("prunes zero entries from totalResource (no gpu in result)", func(t *testing.T) {
		sn := &Node{
			totalResource:     total,
			availableResource: resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 1, "memory": 2}),
		}
		expectedUsageShares := map[string]float64{"cpu": 0.9, "memory": 0.8}
		usageShares := sn.GetResourceUsageShares()
		assert.Assert(t, cmp.Equal(usageShares, expectedUsageShares), "resource usage shares should be equal to the expected")
		assert.Assert(t, resources.DeepEquals(sn.totalResource, prunedTotal), "total resource should be equal to the pruned total")
	})

	t.Run("prunes totalResource and formats fields", func(t *testing.T) {
		sn := &Node{
			NodeID:            "n-1",
			Partition:         "default",
			schedulable:       true,
			totalResource:     total,
			allocatedResource: resources.NewResource(),
			allocations:       map[string]*Allocation{},
		}

		out := sn.String()
		assert.Assert(t, resources.DeepEquals(sn.totalResource, prunedTotal), "total resource should be equal to the pruned total")
		assert.Assert(t, strings.Contains(out, "Total map[cpu:10 memory:10]"))
	})
}
