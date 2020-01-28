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
	"testing"
	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
)

func newNode(nodeID string, totalMap map[string]resources.Quantity) *schedulingNode {
	// leverage the cache test code
	totalRes := resources.NewResourceFromMap(totalMap)
	nodeInfo := cache.NewNodeForTest(nodeID, totalRes)
	return newSchedulingNode(nodeInfo)
}

// Create a new node for testing only.
func TestNewSchedulingNode(t *testing.T) {
	node := newSchedulingNode(nil)
	if node != nil {
		t.Errorf("nil input should not return node %v", node)
	}

	// this is just a wrapper so we can use it to test the real new
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "second": 100})
	node = newNode("node-1", res.Resources)
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}
	// check the resource info all zero
	if !resources.IsZero(node.allocatingResource) && !resources.IsZero(node.preemptingResource) {
		t.Errorf("node resources should all be zero found: %v and %v", node.allocatingResource, node.preemptingResource)
	}
	if !node.cachedAvailableUpdateNeeded {
		t.Error("node available resource dirty should be set for new node")
	}
	if !resources.Equals(node.getAvailableResource(), res) {
		t.Errorf("node available resource not set to cached value got: %v", node.getAvailableResource())
	}
	if node.cachedAvailableUpdateNeeded {
		t.Error("node available resource dirty should be cleared after getAvailableResource call")
	}
}

func TestCheckConditions(t *testing.T) {
	node := newNode("node-1", map[string]resources.Quantity{"first": 100, "second": 100})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}

	// Check if we can allocate on scheduling node (no plugins)
	if !node.preAllocateConditions("test") {
		t.Error("node with scheduling set to true no plugins should allow allocation")
	}

	//TODO add mock for plugin to extend tests
}

func TestPreAllocateCheck(t *testing.T) {
	nodeID := "node-1"
	resNode := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 1})
	node := newNode(nodeID, resNode.Resources)
	if node == nil || node.NodeID != nodeID {
		t.Fatalf("node create failed which should not have %v", node)
	}

	// special cases
	assert.Check(t, node.preAllocateCheck(nil, false) == false, "nil resource should not have fitted on node (no preemption)")
	resNeg := resources.NewResourceFromMap(map[string]resources.Quantity{"first": -1})
	if node.preAllocateCheck(resNeg, false) {
		t.Error("negative resource should not have fitted on node (no preemption)")
	}

	// Check if we can allocate on scheduling node
	resSmall := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	resLarge := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	if !node.preAllocateCheck(resNode, false) {
		t.Error("node resource should have fitted on node (no preemption)")
	}
	if !node.preAllocateCheck(resSmall, false){
		t.Error("small resource should have fitted on node (no preemption)")
	}
	if node.preAllocateCheck(resLarge, false) {
		t.Error("too large resource should not have fitted on node (no preemption)")
	}

	// set allocated resource
	node.nodeInfo.AddAllocation(cache.CreateMockAllocationInfo("app-1", resSmall, "UUID1", "root.default", nodeID))
	if !node.preAllocateCheck(resSmall, false) {
		t.Error("small resource should have fitted in available allocation (no preemption)")
	}
	if node.preAllocateCheck(resNode, false) {
		t.Error("node resource should not have fitted in available allocation (no preemption)")
	}

	// set preempting resources
	node.preemptingResource = resSmall
	if !node.preAllocateCheck(resSmall, true) {
		t.Error("small resource should have fitted in available allocation (preemption)")
	}
	if !node.preAllocateCheck(resNode, true) {
		t.Error("node resource should have fitted in available allocation (preemption)")
	}
	if node.preAllocateCheck(resLarge, true) {
		t.Error("too large resource should not have fitted on node (preemption)")
	}

	// Check if we can allocate on non scheduling node
	node.nodeInfo.SetSchedulable(false)
	if node.preAllocateCheck(resSmall, false) {
		t.Error("node with scheduling set to false should not allow allocation")
	}
}

func TestCheckAllocate(t *testing.T) {
	node := newNode("node-1", map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}
	if !node.cachedAvailableUpdateNeeded {
		t.Error("node available resource dirty should be set for new node")
	}
	// normal alloc check dirty flag
	node.getAvailableResource() // unset the dirty flag
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	if !node.allocateResource(res, false) {
		t.Error("node should have accepted allocation")
	}
	if !node.cachedAvailableUpdateNeeded {
		t.Error("node available resource dirty should be set after allocateResource")
	}
	// add one that pushes node over its size
	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 6})
	if node.allocateResource(res, false) {
		t.Error("node should have rejected allocation (oversize)")
	}

	// check if preempting adds to available
	node = newNode("node-1", map[string]resources.Quantity{"first": 5})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}
	node.incPreemptingResource(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	// preemption alloc
	if !node.allocateResource(res, true) {
		t.Error("node with scheduling set to false should not allow allocation")
	}
}

func TestAllocatingResources(t *testing.T) {
	node := newNode("node-1", map[string]resources.Quantity{"first": 100})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}

	allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	node.incAllocatingResource(allocRes)
	node.incAllocatingResource(allocRes)
	expect := resources.Multiply(allocRes, 2)
	nodeAlloc := node.getAllocatingResource()
	if !resources.Equals(nodeAlloc, expect) {
		t.Errorf("allocating resources not set, expected %v got %v", expect, nodeAlloc)
	}
	// release one
	node.handleAllocationUpdate(allocRes)
	nodeAlloc = node.getAllocatingResource()
	if !resources.Equals(nodeAlloc, allocRes) {
		t.Errorf("allocating resources not decremented, expected %v got %v", expect, nodeAlloc)
	}
	// release allocating: should be back to zero
	node.handleAllocationUpdate(allocRes)
	nodeAlloc = node.getAllocatingResource()
	if !resources.IsZero(nodeAlloc) {
		t.Errorf("allocating resources not zero but %v", nodeAlloc)
	}
}

func TestPreemptingResources(t *testing.T) {
	node := newNode("node-1", map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}

	preemptRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	node.incPreemptingResource(preemptRes)
	node.incPreemptingResource(preemptRes)
	expect := resources.Multiply(preemptRes, 2)
	nodePreempt := node.getPreemptingResource()
	if !resources.Equals(nodePreempt, expect) {
		t.Errorf("preempting resources not set, expected %v got %v", expect, nodePreempt)
	}
	// release one preemption
	node.handlePreemptionUpdate(preemptRes)
	nodePreempt = node.getPreemptingResource()
	if !resources.Equals(nodePreempt, preemptRes) {
		t.Errorf("preempting resources not decremented, expected %v got %v", preemptRes, nodePreempt)
	}
	// release preemption: should be back to zero
	node.handlePreemptionUpdate(preemptRes)
	nodePreempt = node.getPreemptingResource()
	if !resources.IsZero(nodePreempt) {
		t.Errorf("preempting resources not zero but %v", nodePreempt)
	}
}

func TestAvailableDirty(t *testing.T) {
	node := newNode("node-1", map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}
	node.getAvailableResource()
	if node.cachedAvailableUpdateNeeded {
		t.Fatal("node available resource dirty should not be set after getAvailableResource")
	}

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	node.incAllocatingResource(res)
	if !node.cachedAvailableUpdateNeeded {
		t.Error("node available resource dirty should be set after incAllocatingResource")
	}
	node.getAvailableResource()

	node.handleAllocationUpdate(res)
	if !node.cachedAvailableUpdateNeeded {
		t.Error("node available resource dirty should be set after handleAllocationUpdate")
	}
}

func TestNodeReservation(t *testing.T) {
	node := newNode("node-1", map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}
	if node.isReserved() {
		t.Fatal("new node should not have reservations")
	}
	if node.isReservedForApp("unknown") {
		t.Error("new node should not have reservations for unknown app")
	}

	// reserve illegal request
	ok, err := node.reserve(nil, nil)
	if ok || err == nil {
		t.Errorf("illegal reservation requested but did not fail: status %t, error %v", ok, err)
	}

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})

	// too large for node
	ok, err = node.reserve(app, ask)
	if ok || err == nil {
		t.Errorf("requested reservation does not fit in node resource but did not fail: status %t, error %v", ok, err)
	}

	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask = newAllocationAsk("alloc-1", "app-1", res)
	app = newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})
	// reserve that works
	ok, err = node.reserve(app, ask)
	if !ok || err != nil {
		t.Errorf("reservation should not have failed: status %t, error %v", ok, err)
	}
	if node.isReservedForApp("unknown") {
		t.Errorf("node should not have reservations for unknown app")
	}
	if node.isReserved() && !node.isReservedForApp("app-1") {
		t.Errorf("node should have reservations for app-1")
	}

	// 2nd reservation on node
	ok, err = node.reserve(nil, nil)
	if ok || err == nil {
		t.Errorf("reservation requested on already reserved node: status %t, error %v", ok, err)
	}

	// unreserve different app
	ok, err = node.unReserve(nil, nil)
	if ok || err == nil {
		t.Errorf("illegal reservation release but did not fail: status %t, error %v", ok, err)
	}
	ask2 := newAllocationAsk("alloc-2", "app-2", res)
	app2 := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-2"})
	ok, err = node.unReserve(app2, ask2)
	if ok || err != nil {
		t.Errorf("un-reserve different app should have failed without error: status %t, error %v", ok, err)
	}
	ok, err = node.unReserve(app, ask)
	if !ok || err != nil {
		t.Errorf("un-reserve should not have failed: status %t, error %v", ok, err)
	}
}
