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
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/common"
)

func TestNewNode(t *testing.T) {
	// simple nil check
	node := NewNode(nil)
	if node != nil {
		t.Error("node not returned correctly: node is nul or incorrect name")
	}
	proto := newProto("testnode", nil, nil, nil)
	node = NewNode(proto)
	if node == nil || node.NodeID != "testnode" {
		t.Error("node not returned correctly: node is nul or incorrect name")
	}

	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "second": 100})
	proto = newProto("testnode", totalRes, nil, map[string]string{})
	node = NewNode(proto)
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
		common.HostName:      "host1",
		common.RackName:      "rack1",
		common.NodePartition: "partition1",
	}
	node = NewNode(proto)
	if node == nil || node.NodeID != "testnode" {
		t.Fatal("node not returned correctly: node is nul or incorrect name")
	}
	assert.Equal(t, "host1", node.Hostname)
	assert.Equal(t, "rack1", node.Rackname)
	assert.Equal(t, "partition1", node.Partition)

	// test capacity/available/occupied resources
	totalResources := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "second": 100})
	occupiedResources := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 30, "second": 20})
	availableResources := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 70, "second": 80})
	proto = newProto("testnode", totalResources, occupiedResources, map[string]string{})
	node = NewNode(proto)
	assert.Equal(t, node.NodeID, "testnode", "node not returned correctly: node is nul or incorrect name")
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
	node := newNode("node-1", map[string]resources.Quantity{"first": 100, "second": 100})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}

	// Check if we can allocate on scheduling node (no plugins)
	if !node.preAllocateConditions("test") {
		t.Error("node with scheduling set to true no plugins should allow allocation")
	}

	// TODO add mock for plugin to extend tests
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
	node.AddAllocation(newAllocation("app-1", "UUID1", nodeID, "root.default", resSmall))
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
	appID := "app-1"
	ask := newAllocationAsk("alloc-1", appID, res)
	app := newApplication(appID, "default", "root.unknown")

	// standalone reservation unreserve returns false as app is not reserved
	reserve := newReservation(node, app, ask, false)
	node.reservations[reserve.getKey()] = reserve
	if err = node.preAllocateCheck(resSmall, "app-2", true); err == nil {
		t.Errorf("node was reserved for different app but check passed: %v", err)
	}
	if err = node.preAllocateCheck(resSmall, "app-1|alloc-2", true); err == nil {
		t.Errorf("node was reserved for this app but not the alloc and check passed: %v", err)
	}
	err = node.preAllocateCheck(resSmall, appID, true)
	assert.NilError(t, err, "node was reserved for this app but check did not pass check")
	err = node.preAllocateCheck(resSmall, "app-1|alloc-1", true)
	assert.NilError(t, err, "node was reserved for this app/alloc but check did not pass check")

	// Check if we can allocate on non scheduling node
	node.SetSchedulable(false)
	if err = node.preAllocateCheck(resSmall, "", false); err == nil {
		t.Errorf("node with scheduling set to false should not allow allocation: %v", err)
	}
}

// Only test the CanAllocate code, the used logic in preAllocateCheck has its own test
func TestCanAllocate(t *testing.T) {
	node := newNode("node-1", map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}
	// normal alloc
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	if !node.CanAllocate(res, false) {
		t.Error("node should have accepted allocation")
	}
	// check one that pushes node over its size
	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11})
	if node.CanAllocate(res, false) {
		t.Error("node should have rejected allocation (oversize)")
	}

	// check if preempting adds to available
	node.IncPreemptingResource(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	// preemption alloc
	if !node.CanAllocate(res, true) {
		t.Error("resource should have fitted in with preemption set")
	}
}

func TestPreemptingResources(t *testing.T) {
	node := newNode("node-1", map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}

	preemptRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	node.IncPreemptingResource(preemptRes)
	node.IncPreemptingResource(preemptRes)
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

func TestNodeReservation(t *testing.T) {
	node := newNode("node-1", map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != "node-1" {
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

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	appID := "app-1"
	ask := newAllocationAsk("alloc-1", appID, res)
	app := newApplication(appID, "default", "root.unknown")

	// too large for node
	err = node.Reserve(app, ask)
	if err == nil {
		t.Errorf("requested reservation does not fit in node resource but did not fail: error %v", err)
	}

	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask = newAllocationAsk("alloc-1", appID, res)
	app = newApplication(appID, "default", "root.unknown")
	// reserve that works
	err = node.Reserve(app, ask)
	assert.NilError(t, err, "reservation should not have failed")
	if node.isReservedForApp("") {
		t.Error("node should not have reservations for empty key")
	}
	if node.isReservedForApp("unknown") {
		t.Errorf("node should not have reservations for unknown key")
	}
	if node.IsReserved() && !node.isReservedForApp("app-1") {
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
	appID = "app-2"
	ask2 := newAllocationAsk("alloc-2", appID, res)
	app2 := newApplication(appID, "default", "root.unknown")
	var num int
	num, err = node.unReserve(app2, ask2)
	assert.NilError(t, err, "un-reserve different app should have failed without error")
	assert.Equal(t, num, 0, "un-reserve different app should have failed without releases")
	num, err = node.unReserve(app, ask)
	assert.NilError(t, err, "un-reserve should not have failed")
	assert.Equal(t, num, 1, "un-reserve app should have released ")
}

func TestUnReserveApps(t *testing.T) {
	node := newNode("node-1", map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}
	if node.IsReserved() {
		t.Fatal("new node should not have reservations")
	}
	reservedKeys, releasedAsks := node.UnReserveApps()
	if len(reservedKeys) != 0 || len(releasedAsks) != 0 {
		t.Fatalf("new node should not fail remove all reservations: asks released = %v, reservation keys = %v", releasedAsks, reservedKeys)
	}

	// create some reservations and see it clean up via the app
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	appID := "app-1"
	ask := newAllocationAsk("alloc-1", appID, res)
	app := newApplication(appID, "default", "root.unknown")
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to the app")
	if !resources.Equals(res, app.GetPendingResource()) {
		t.Fatalf("expected resource delta  %v got %v", res, app.GetPendingResource())
	}
	err = app.Reserve(node, ask)
	assert.NilError(t, err, "reservation should not have failed")
	assert.Equal(t, 1, len(node.reservations), "node should have reservation")
	reservedKeys, releasedAsks = node.UnReserveApps()
	if len(reservedKeys) != 1 || len(releasedAsks) != 1 {
		t.Fatal("node should have removed reservation")
	}

	// reserve just the node
	err = node.Reserve(app, ask)
	assert.NilError(t, err, "reservation should not have failed")
	assert.Equal(t, 1, len(node.reservations), "node should have reservation")
	reservedKeys, releasedAsks = node.UnReserveApps()
	if len(reservedKeys) != 1 || len(releasedAsks) != 1 {
		t.Fatalf("node should have removed reservation: asks released = %v, reservation keys = %v", releasedAsks, reservedKeys)
	}
}

func TestIsReservedForApp(t *testing.T) {
	node := newNode("node-1", map[string]resources.Quantity{"first": 10})
	if node == nil || node.NodeID != "node-1" {
		t.Fatalf("node create failed which should not have %v", node)
	}
	if node.IsReserved() {
		t.Fatal("new node should not have reservations")
	}
	reservedKeys, releasedAsks := node.UnReserveApps()
	if len(reservedKeys) != 0 || len(releasedAsks) != 0 {
		t.Fatalf("new node should not fail remove all reservations: asks released = %v, reservation keys = %v", releasedAsks, reservedKeys)
	}

	// check if we can allocate on a reserved node
	q := map[string]resources.Quantity{"first": 0}
	res := resources.NewResourceFromMap(q)
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newApplication("app-1", "default", "root.unknown")

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

func TestAttributes(t *testing.T) {
	proto := newProto("testnode", nil, nil, map[string]string{
		common.NodePartition: "partition1",
		"something":          "just a text",
	})

	node := NewNode(proto)
	if node == nil || node.NodeID != "testnode" {
		t.Fatal("node not returned correctly: node is nul or incorrect name")
	}

	assert.Equal(t, "", node.Hostname)
	assert.Equal(t, "", node.Rackname)
	assert.Equal(t, "partition1", node.Partition)

	value := node.GetAttribute(common.NodePartition)
	assert.Equal(t, "partition1", value, "node attributes not set, expected 'partition1' got '%v'", value)
	value = node.GetAttribute("something")
	assert.Equal(t, "just a text", value, "node attributes not set, expected 'just a text' got '%v'", value)
}

func TestAddAllocation(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
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
	node.AddAllocation(newAllocation("app1", "1", "node-1", "queue-1", half))
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
	node.AddAllocation(newAllocation("app1", "2", "node-1", "queue-1", piece))
	if node.GetAllocation("2") == nil {
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
}

func TestRemoveAllocation(t *testing.T) {
	node := newNode("node-123", map[string]resources.Quantity{"first": 100, "second": 200})
	if !resources.IsZero(node.GetAllocatedResource()) {
		t.Fatal("Failed to initialize resource")
	}

	// allocate half of the resources available and check the calculation
	half := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "second": 100})
	node.AddAllocation(newAllocation("app1", "1", "node-1", "queue-1", half))
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
	node.AddAllocation(newAllocation("app1", "2", "node-1", "queue-1", piece))
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
	left := resources.Sub(node.GetCapacity(), piece)
	if !resources.Equals(node.GetAvailableResource(), left) {
		t.Errorf("allocated resource not set correctly %v got %v", left, node.GetAvailableResource())
	}
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
	node.AddAllocation(newAllocation("app1", "1", "node-1", "queue-1", nil))
	alloc = node.GetAllocation("1")
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

	// allocate
	node.AddAllocation(newAllocation("app1", "1", "node-1", "queue-1", nil))
	node.AddAllocation(newAllocation("app1", "2", "node-1", "queue-1", nil))
	assert.Equal(t, 2, len(node.GetAllAllocations()), "allocation length mismatch")
	// This should not happen in real code just making sure the code does do what is expected
	node.AddAllocation(newAllocation("app1", "2", "node-1", "queue-1", nil))
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
