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

func newNode(nodeID string, totalMap map[string]resources.Quantity) *SchedulingNode {
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
	if !resources.IsZero(node.allocating) && !resources.IsZero(node.preempting) {
		t.Errorf("node resources should all be zero found: %v and %v", node.allocating, node.preempting)
	}
	if !node.cachedAvailableUpdateNeeded {
		t.Error("node available resource dirty should be set for new node")
	}
	if !resources.Equals(node.GetAvailableResource(), res) {
		t.Errorf("node available resource not set to cached value got: %v", node.GetAvailableResource())
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
	if err := node.preAllocateCheck(nil, "", false); err == nil {
		t.Errorf("nil resource should not have fitted on node (no preemption)")
	}
	resNeg := resources.NewResourceFromMap(map[string]resources.Quantity{"first": -1})
	if err := node.preAllocateCheck(resNeg, "", false); err == nil {
		t.Errorf("negative resource should not have fitted on node (no preemption)")
	}
	// Check if we can allocate on scheduling node
	resSmall := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	resLarge := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	err := node.preAllocateCheck(resNode, "", false)
	assert.NilError(t, err, "node resource should have fitted on node (no preemption)")
	err = node.preAllocateCheck(resSmall, "", false)
	assert.NilError(t, err, "small resource should have fitted on node (no preemption)")
	if err = node.preAllocateCheck(resLarge, "", false); err == nil {
		t.Errorf("too large resource should not have fitted on node (no preemption): %v", err)
	}

	// set allocated resource
	node.nodeInfo.AddAllocation(cache.CreateMockAllocationInfo("app-1", resSmall, "UUID1", "root.default", nodeID))
	err = node.preAllocateCheck(resSmall, "", false)
	assert.NilError(t, err, "small resource should have fitted in available allocation (no preemption)")
	if err = node.preAllocateCheck(resNode, "", false); err == nil {
		t.Errorf("node resource should not have fitted in available allocation (no preemption): %v", err)
	}

	// set preempting resources
	node.preempting = resSmall
	err = node.preAllocateCheck(resSmall, "", true)
	assert.NilError(t, err, "small resource should have fitted in available allocation (preemption)")
	err = node.preAllocateCheck(resNode, "", true)
	assert.NilError(t, err, "node resource should have fitted in available allocation (preemption)")
	if err = node.preAllocateCheck(resLarge, "", true); err == nil {
		t.Errorf("too large resource should not have fitted on node (preemption): %v", err)
	}

	// check if we can allocate on a reserved node
	q := map[string]resources.Quantity{"first": 0}
	res := resources.NewResourceFromMap(q)
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})

	// standalone reservation unreserve returns false as app is not reserved
	reserve := newReservation(node, app, ask, false)
	node.reservations[reserve.getKey()] = reserve
	if err = node.preAllocateCheck(resSmall, "app-2", true); err == nil {
		t.Errorf("node was reserved for different app but check passed: %v", err)
	}
	if err = node.preAllocateCheck(resSmall, "app-1|alloc-2", true); err == nil {
		t.Errorf("node was reserved for this app but not the alloc and check passed: %v", err)
	}
	err = node.preAllocateCheck(resSmall, "app-1", true)
	assert.NilError(t, err, "node was reserved for this app but check did not pass check")
	err = node.preAllocateCheck(resSmall, "app-1|alloc-1", true)
	assert.NilError(t, err, "node was reserved for this app/alloc but check did not pass check")

	// Check if we can allocate on non scheduling node
	node.nodeInfo.SetSchedulable(false)
	if err = node.preAllocateCheck(resSmall, "", false); err == nil {
		t.Errorf("node with scheduling set to false should not allow allocation: %v", err)
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
	node.GetAvailableResource() // unset the dirty flag
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
	node.decAllocatingResource(allocRes)
	nodeAlloc = node.getAllocatingResource()
	if !resources.Equals(nodeAlloc, allocRes) {
		t.Errorf("allocating resources not decremented, expected %v got %v", expect, nodeAlloc)
	}
	// release allocating: should be back to zero
	node.decAllocatingResource(allocRes)
	nodeAlloc = node.getAllocatingResource()
	if !resources.IsZero(nodeAlloc) {
		t.Errorf("allocating resources not zero but %v", nodeAlloc)
	}
	// release allocating again: should be zero
	node.decAllocatingResource(allocRes)
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
	node.decPreemptingResource(preemptRes)
	nodePreempt = node.getPreemptingResource()
	if !resources.Equals(nodePreempt, preemptRes) {
		t.Errorf("preempting resources not decremented, expected %v got %v", preemptRes, nodePreempt)
	}
	// release preemption: should be back to zero
	node.decPreemptingResource(preemptRes)
	nodePreempt = node.getPreemptingResource()
	if !resources.IsZero(nodePreempt) {
		t.Errorf("preempting resources not zero but %v", nodePreempt)
	}
	// release preemption again: should be zero
	node.decPreemptingResource(preemptRes)
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
	node.GetAvailableResource()
	if node.cachedAvailableUpdateNeeded {
		t.Fatal("node available resource dirty should not be set after getAvailableResource")
	}

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	node.incAllocatingResource(res)
	if !node.cachedAvailableUpdateNeeded {
		t.Error("node available resource dirty should be set after incPreemptingResource")
	}
	node.GetAvailableResource()

	node.decAllocatingResource(res)
	if !node.cachedAvailableUpdateNeeded {
		t.Error("node available resource dirty should be set after decAllocatingResource")
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
	if node.isReservedForApp("") {
		t.Error("new node should not have reservations for empty key")
	}
	if node.isReservedForApp("unknown") {
		t.Error("new node should not have reservations for unknown key")
	}

	// reserve illegal request
	err := node.reserve(nil, nil)
	if err == nil {
		t.Errorf("illegal reservation requested but did not fail: error %v", err)
	}

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})
	queue := createDefaultRootQueueProtected(t)
	app.queue = queue

	// too large for node
	err = node.reserve(app, ask)
	if err == nil {
		t.Errorf("requested reservation does not fit in node resource but did not fail: error %v", err)
	}

	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask = newAllocationAsk("alloc-1", "app-1", res)
	app = newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})
	// reserve that works
	err = node.reserve(app, ask)
	if err != nil {
		t.Errorf("reservation should not have failed: error %v", err)
	}
	if node.isReservedForApp("") {
		t.Error("node should not have reservations for empty key")
	}
	if node.isReservedForApp("unknown") {
		t.Errorf("node should not have reservations for unknown key")
	}
	if node.isReserved() && !node.isReservedForApp("app-1") {
		t.Errorf("node should have reservations for app-1")
	}

	// 2nd reservation on node
	err = node.reserve(nil, nil)
	if err == nil {
		t.Errorf("reservation requested on already reserved node: error %v", err)
	}

	// unreserve different app
	err = node.unReserve(nil, nil)
	if err == nil {
		t.Errorf("illegal reservation release but did not fail: error %v", err)
	}
	ask2 := newAllocationAsk("alloc-2", "app-2", res)
	app2 := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-2"})
	app2.queue = queue
	err = node.unReserve(app2, ask2)
	if err != nil {
		t.Errorf("un-reserve different app should have failed without error: error %v", err)
	}
	err = node.unReserve(app, ask)
	if err != nil {
		t.Errorf("un-reserve should not have failed: error %v", err)
	}
}

func TestUnReserveApps(t *testing.T) {
	node := newNode("node-1", map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}
	if node.isReserved() {
		t.Fatal("new node should not have reservations")
	}
	reservedKeys, ok := node.unReserveApps()
	if !ok || len(reservedKeys) != 0 {
		t.Fatal("new node should not fail remove all reservations")
	}

	// create some reservations and see it clean up via the app
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})
	queue := createDefaultRootQueueProtected(t)
	app.queue = queue
	delta, err := app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(res, delta) {
		t.Fatalf("ask should have been added to the app expected resource delta  %v got %v (err = %v)", res, delta, err)
	}
	err = app.reserve(node, ask)
	if err != nil {
		t.Errorf("reservation should not have failed: error %v", err)
	}
	assert.Equal(t, 1, len(node.reservations), "node should have reservation")
	reservedKeys, ok = node.unReserveApps()
	if !ok || len(reservedKeys) != 1 {
		t.Fatal("node should have removed reservation")
	}

	// reserve just the node
	err = node.reserve(app, ask)
	if err != nil {
		t.Errorf("reservation should not have failed: error %v", err)
	}
	assert.Equal(t, 1, len(node.reservations), "node should have reservation")
	reservedKeys, ok = node.unReserveApps()
	if !ok || len(reservedKeys) != 1 {
		t.Errorf("node should have removed reservation: status = %t, reservation keys = %v", ok, reservedKeys)
	}
}

func TestIsReservedForApp(t *testing.T) {
	node := newNode("node-1", map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}
	if node.isReserved() {
		t.Fatal("new node should not have reservations")
	}
	reservedKeys, ok := node.unReserveApps()
	if !ok || len(reservedKeys) != 0 {
		t.Fatalf("new node should not fail remove all reservations: status = %t, reservation keys = %v", ok, reservedKeys)
	}

	// check if we can allocate on a reserved node
	q := map[string]resources.Quantity{"first": 0}
	res := resources.NewResourceFromMap(q)
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})
	queue := createDefaultRootQueueProtected(t)
	app.queue = queue

	// standalone reservation unreserve returns false as app is not reserved
	reserve := newReservation(node, app, ask, false)
	node.reservations[reserve.getKey()] = reserve
	if node.isReservedForApp("app-2") {
		t.Error("node was reserved for different app but check passed ")
	}
	if node.isReservedForApp("app-1|alloc-2") {
		t.Error("node was reserved for this app but not the alloc and check passed ")
	}
	if !node.isReservedForApp("app-1") {
		t.Error("node was reserved for this app but check did not passed ")
	}
	if !node.isReservedForApp("app-1|alloc-1") {
		t.Error("node was reserved for this app/alloc but check did not passed ")
	}
}
