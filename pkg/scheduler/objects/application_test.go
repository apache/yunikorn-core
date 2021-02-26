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
	"strconv"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/handler"
)

// test basic reservations
func TestAppReservation(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	if app.hasReserved() {
		t.Fatal("new app should not have reservations")
	}
	if app.IsReservedOnNode("") {
		t.Error("app should not have reservations for empty node ID")
	}
	if app.IsReservedOnNode("unknown") {
		t.Error("new app should not have reservations for unknown node")
	}

	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	// reserve illegal request
	err = app.Reserve(nil, nil)
	if err == nil {
		t.Errorf("illegal reservation requested but did not fail")
	}

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	ask := newAllocationAsk(aKey, appID1, res)
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 10})

	// too large for node
	err = app.Reserve(node, ask)
	if err == nil {
		t.Errorf("requested reservation does not fit in node resource but did not fail")
	}

	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask = newAllocationAsk(aKey, appID1, res)
	app = newApplication(appID1, "default", "root.unknown")
	app.queue = queue
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	// reserve that works
	err = app.Reserve(node, ask)
	assert.NilError(t, err, "reservation should not have failed")
	if app.IsReservedOnNode("") {
		t.Errorf("app should not have reservations for empty node ID")
	}
	if app.IsReservedOnNode("unknown") {
		t.Error("app should not have reservations for unknown node")
	}
	if app.hasReserved() && !app.IsReservedOnNode(nodeID1) {
		t.Errorf("app should have reservations for node %s", nodeID1)
	}

	// reserve the same reservation
	err = app.Reserve(node, ask)
	if err == nil {
		t.Errorf("reservation should have failed")
	}

	// unreserve unknown node/ask
	_, err = app.UnReserve(nil, nil)
	if err == nil {
		t.Errorf("illegal reservation release but did not fail")
	}

	// 2nd reservation for app
	ask2 := newAllocationAsk("alloc-2", appID1, res)
	node2 := newNode("node-2", map[string]resources.Quantity{"first": 10})
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "ask2 should have been added to app")
	err = app.Reserve(node, ask2)
	if err == nil {
		t.Errorf("reservation of node by second ask should have failed")
	}
	err = app.Reserve(node2, ask2)
	assert.NilError(t, err, "reservation of 2nd node should not have failed")
	_, err = app.UnReserve(node2, ask2)
	assert.NilError(t, err, "remove of reservation of 2nd node should not have failed")

	// unreserve the same should fail
	_, err = app.UnReserve(node2, ask2)
	assert.NilError(t, err, "remove twice of reservation of 2nd node should have failed")

	// failure case: remove reservation from node, app still needs cleanup
	var num int
	num, err = node.unReserve(app, ask)
	assert.NilError(t, err, "un-reserve on node should not have failed")
	assert.Equal(t, num, 1, "un-reserve on node should have removed reservation")
	num, err = app.UnReserve(node, ask)
	assert.NilError(t, err, "app has reservation should not have failed")
	assert.Equal(t, num, 1, "un-reserve on app should have removed reservation from app")
}

// test multiple reservations from one allocation
func TestAppAllocReservation(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	if app.hasReserved() {
		t.Fatal("new app should not have reservations")
	}
	if len(app.GetAskReservations("")) != 0 {
		t.Fatal("new app should not have reservation for empty allocKey")
	}
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	// reserve 1 allocate ask
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskRepeat(aKey, appID1, res, 2)
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 10})
	// reserve that works
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	err = app.Reserve(node1, ask)
	assert.NilError(t, err, "reservation should not have failed")
	if len(app.GetAskReservations("")) != 0 {
		t.Fatal("app should not have reservation for empty allocKey")
	}
	nodeKey1 := nodeID1 + "|" + aKey
	askReserved := app.GetAskReservations(aKey)
	if len(askReserved) != 1 || askReserved[0] != nodeKey1 {
		t.Errorf("app should have reservations for %s on %s and has not", aKey, nodeID1)
	}

	nodeID2 := "node-2"
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 10})
	err = app.Reserve(node2, ask)
	assert.NilError(t, err, "reservation should not have failed: error %v", err)
	nodeKey2 := nodeID2 + "|" + aKey
	askReserved = app.GetAskReservations(aKey)
	if len(askReserved) != 2 && (askReserved[0] != nodeKey2 || askReserved[1] != nodeKey2) {
		t.Errorf("app should have reservations for %s on %s and has not", aKey, nodeID2)
	}

	// check exceeding ask repeat: nothing should change
	if app.canAskReserve(ask) {
		t.Error("ask has maximum repeats reserved, reserve check should have failed")
	}
	node3 := newNode("node-3", map[string]resources.Quantity{"first": 10})
	err = app.Reserve(node3, ask)
	if err == nil {
		t.Errorf("reservation should have failed")
	}
	askReserved = app.GetAskReservations(aKey)
	if len(askReserved) != 2 && (askReserved[0] != nodeKey1 || askReserved[1] != nodeKey1) &&
		(askReserved[0] != nodeKey2 || askReserved[1] != nodeKey2) {
		t.Errorf("app should have reservations for node %s and %s and has not: %v", nodeID1, nodeID2, askReserved)
	}
	// clean up all asks and reservations
	reservedAsks := app.RemoveAllocationAsk("")
	if app.hasReserved() || node1.IsReserved() || node2.IsReserved() || reservedAsks != 2 {
		t.Errorf("ask removal did not clean up all reservations, reserved released = %d", reservedAsks)
	}
}

// test update allocation repeat
func TestUpdateRepeat(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	// failure cases
	delta, err := app.updateAskRepeat("", 0)
	if err == nil || delta != nil {
		t.Error("empty ask key should not have been found")
	}
	delta, err = app.updateAskRepeat("unknown", 0)
	if err == nil || delta != nil {
		t.Error("unknown ask key should not have been found")
	}

	// working cases
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskRepeat(aKey, appID1, res, 1)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	delta, err = app.updateAskRepeat(aKey, 0)
	if err != nil || !resources.IsZero(delta) {
		t.Errorf("0 increase should return zero delta and did not: %v, err %v", delta, err)
	}
	delta, err = app.updateAskRepeat(aKey, 1)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("increase did not return correct delta, err %v, expected %v got %v", err, res, delta)
	}

	// decrease to zero
	delta, err = app.updateAskRepeat(aKey, -2)
	if err != nil || !resources.Equals(resources.Multiply(res, -2), delta) {
		t.Errorf("decrease did not return correct delta, err %v, expected %v got %v", err, resources.Multiply(res, -2), delta)
	}
	// decrease to below zero
	delta, err = app.updateAskRepeat(aKey, -1)
	if err == nil || delta != nil {
		t.Errorf("decrease did not return correct delta, err %v, delta %v", err, delta)
	}
}

// test pending calculation and ask addition
func TestAddAllocAsk(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}

	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	// failure cases
	err = app.AddAllocationAsk(nil)
	if err == nil {
		t.Errorf("nil ask should not have been added to app")
	}
	res := resources.NewResource()
	ask := newAllocationAsk(aKey, appID1, res)
	err = app.AddAllocationAsk(ask)
	if err == nil {
		t.Errorf("zero resource ask should not have been added to app")
	}
	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask = newAllocationAskRepeat(aKey, appID1, res, 0)
	err = app.AddAllocationAsk(ask)
	if err == nil {
		t.Errorf("ask with zero repeat should not have been added to app")
	}

	// working cases
	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask = newAllocationAskRepeat(aKey, appID1, res, 1)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been updated on app")
	assert.Assert(t, app.IsAccepted(), "Application should be in accepted state")
	pending := app.GetPendingResource()
	if !resources.Equals(res, pending) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", res, pending)
	}
	ask = newAllocationAskRepeat(aKey, appID1, res, 2)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been updated on app")
	pending = app.GetPendingResource()
	if !resources.Equals(resources.Multiply(res, 2), pending) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", resources.Multiply(res, 2), pending)
	}

	// change both resource and count
	ask = newAllocationAskRepeat(aKey, appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 3}), 5)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been updated on app")
	pending = app.GetPendingResource()
	if !resources.Equals(resources.Multiply(res, 3), app.GetPendingResource()) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", resources.Multiply(res, 3), pending)
	}

	// test a decrease of repeat and back to start
	ask = newAllocationAskRepeat(aKey, appID1, res, 1)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been updated on app")
	if !resources.Equals(res, app.GetPendingResource()) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", res, app.GetPendingResource())
	}
	// after all this is must still be in an accepted state
	assert.Assert(t, app.IsAccepted(), "Application should have stayed in accepted state")
}

// test state change on add and remove ask
func TestAllocAskStateChange(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}

	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskRepeat(aKey, appID1, res, 1)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	assert.Assert(t, app.IsAccepted(), "Application should be in accepted state")
	// make sure the state changes to waiting
	assert.Equal(t, app.RemoveAllocationAsk(aKey), 0, "ask should have been removed, no reservations")
	assert.Assert(t, app.IsWaiting(), "Application should be in waiting state")

	// make sure the state changes back correctly
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	assert.Assert(t, app.IsRunning(), "Application should be in running state")

	// and back to waiting again, now from running
	assert.Equal(t, app.RemoveAllocationAsk(aKey), 0, "ask should have been removed, no reservations")
	assert.Assert(t, app.IsWaiting(), "Application should be in waiting state")
}

// test recover ask
func TestRecoverAllocAsk(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}

	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	// failure cases
	app.RecoverAllocationAsk(nil)
	assert.Equal(t, len(app.requests), 0, "nil ask should not be added")

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskRepeat(aKey, appID1, res, 1)
	app.RecoverAllocationAsk(ask)
	assert.Equal(t, len(app.requests), 1, "ask should have been added")
	assert.Assert(t, app.IsAccepted(), "Application should be in accepted state")

	ask = newAllocationAskRepeat("ask-2", appID1, res, 1)
	app.RecoverAllocationAsk(ask)
	assert.Equal(t, len(app.requests), 2, "ask should have been added, total should be 2")
	assert.Assert(t, app.IsAccepted(), "Application should have stayed in accepted state")
}

// test reservations removal by allocation
func TestRemoveReservedAllocAsk(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	// create app and allocs
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask1 := newAllocationAsk(aKey, appID1, res)
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err, "resource ask1 should have been added to app")

	allocKey := "alloc-2"
	ask2 := newAllocationAsk(allocKey, appID1, res)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "resource ask2 should have been added to app")

	// reserve one alloc and remove
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 10})
	err = app.Reserve(node, ask2)
	assert.NilError(t, err, "reservation should not have failed")
	if len(app.GetAskReservations(allocKey)) != 1 || !node.IsReserved() {
		t.Fatalf("app should have reservation for %v on node", allocKey)
	}
	before := app.GetPendingResource().Clone()
	reservedAsks := app.RemoveAllocationAsk(allocKey)
	delta := resources.Sub(before, app.GetPendingResource())
	if !resources.Equals(res, delta) || reservedAsks != 1 {
		t.Errorf("resource ask2 should have been removed from app: %v, (reserved released = %d)", delta, reservedAsks)
	}
	if app.hasReserved() || node.IsReserved() {
		t.Fatal("app and node should not have reservations")
	}

	// reserve again: then remove from node before remove from app
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "resource ask2 should have been added to app")

	err = app.Reserve(node, ask2)
	assert.NilError(t, err, "reservation should not have failed: error %v", err)
	if len(app.GetAskReservations(allocKey)) != 1 || !node.IsReserved() {
		t.Fatalf("app should have reservation for %v on node", allocKey)
	}
	var num int
	num, err = node.unReserve(app, ask2)
	assert.NilError(t, err, "un-reserve on node should not have failed")
	assert.Equal(t, num, 1, "un-reserve on node should have removed reservation")

	before = app.GetPendingResource().Clone()
	reservedAsks = app.RemoveAllocationAsk(allocKey)
	delta = resources.Sub(before, app.GetPendingResource())
	if !resources.Equals(res, delta) || reservedAsks != 1 {
		t.Errorf("resource ask2 should have been removed from app: %v, (reserved released = %d)", delta, reservedAsks)
	}
	// app reservation is removed even though the node removal failed
	if app.hasReserved() || node.IsReserved() {
		t.Fatal("app and node should not have reservations")
	}
	// add a new reservation: use the existing ask1
	err = app.Reserve(node, ask1)
	assert.NilError(t, err, "reservation should not have failed: error %v", err)
	// clean up
	reservedAsks = app.RemoveAllocationAsk("")
	if !resources.IsZero(app.GetPendingResource()) || reservedAsks != 1 {
		t.Errorf("all resource asks should have been removed from app: %v, (reserved released = %d)", app.GetPendingResource(), reservedAsks)
	}
	// app reservation is removed due to ask removal
	if app.hasReserved() || node.IsReserved() {
		t.Fatal("app and node should not have reservations")
	}
}

// test pending calculation and ask removal
func TestRemoveAllocAsk(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	// failures cases: things should not crash (nothing happens)
	reservedAsks := app.RemoveAllocationAsk("")
	if !resources.IsZero(app.GetPendingResource()) || reservedAsks != 0 {
		t.Errorf("pending resource not updated correctly removing all, expected zero but was: %v", app.GetPendingResource())
	}
	reservedAsks = app.RemoveAllocationAsk("unknown")
	if !resources.IsZero(app.GetPendingResource()) || reservedAsks != 0 {
		t.Errorf("pending resource not updated correctly removing unknown, expected zero but was: %v", app.GetPendingResource())
	}

	// setup the allocs
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskRepeat(aKey, appID1, res, 2)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask 1 should have been added to app")
	ask = newAllocationAskRepeat("alloc-2", appID1, res, 2)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask 2 should have been added to app")
	if len(app.requests) != 2 {
		t.Fatalf("missing asks from app expected 2 got %d", len(app.requests))
	}
	expected := resources.Multiply(res, 4)
	if !resources.Equals(expected, app.GetPendingResource()) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", expected, app.GetPendingResource())
	}

	// test removes unknown (nothing happens)
	reservedAsks = app.RemoveAllocationAsk("unknown")
	if reservedAsks != 0 {
		t.Errorf("asks released which did not exist: %d", reservedAsks)
	}
	delta := app.GetPendingResource().Clone()
	reservedAsks = app.RemoveAllocationAsk(aKey)
	delta.SubFrom(app.GetPendingResource())
	expected = resources.Multiply(res, 2)
	if !resources.Equals(delta, expected) || reservedAsks != 0 {
		t.Errorf("ask should have been removed from app, err %v, expected delta %v but was: %v, (reserved released = %d)", err, expected, delta, reservedAsks)
	}
	reservedAsks = app.RemoveAllocationAsk("")
	if len(app.requests) != 0 || reservedAsks != 0 {
		t.Fatalf("asks not removed as expected 0 got %d, (reserved released = %d)", len(app.requests), reservedAsks)
	}
	if !resources.IsZero(app.GetPendingResource()) {
		t.Errorf("pending resource not updated correctly, expected zero but was: %v", app.GetPendingResource())
	}
}

// This test must not test the sorter that is underlying.
// It tests the Application specific parts of the code only.
func TestSortRequests(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	if app.sortedRequests != nil {
		t.Fatalf("new app create should not have sorted requests: %v", app)
	}
	app.sortRequests(true)
	if app.sortedRequests != nil {
		t.Fatalf("after sort call (no pending resources) list must be nil: %v", app.sortedRequests)
	}

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	for i := 1; i < 4; i++ {
		num := strconv.Itoa(i)
		ask := newAllocationAsk("ask-"+num, appID1, res)
		ask.priority = int32(i)
		app.requests[ask.AllocationKey] = ask
	}
	app.sortRequests(true)
	if len(app.sortedRequests) != 3 {
		t.Fatalf("app sorted requests not correct: %v", app.sortedRequests)
	}
	allocKey := app.sortedRequests[0].AllocationKey
	delete(app.requests, allocKey)
	app.sortRequests(true)
	if len(app.sortedRequests) != 2 {
		t.Fatalf("app sorted requests not correct after removal: %v", app.sortedRequests)
	}
}

func TestStateChangeOnUpdate(t *testing.T) {
	// create a fake queue
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	// fake the queue assignment
	app.queue = queue
	// app should be new
	assert.Assert(t, app.IsNew(), "New application did not return new state: %s", app.CurrentState())
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	askID := "ask-1"
	ask := newAllocationAsk(askID, appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	// app with ask should be accepted
	assert.Assert(t, app.IsAccepted(), "application did not change to accepted state: %s", app.CurrentState())

	// removing the ask should move it to waiting
	released := app.RemoveAllocationAsk(askID)
	assert.Equal(t, released, 0, "allocation ask should not have been reserved")
	assert.Assert(t, app.IsWaiting(), "application did not change to waiting state: %s", app.CurrentState())

	// start with a fresh state machine
	app = newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	// fake the queue assignment
	app.queue = queue
	// app should be new
	assert.Assert(t, app.IsNew(), "New application did not return new state: %s", app.CurrentState())
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	// app with ask should be accepted
	assert.Assert(t, app.IsAccepted(), "Application did not change to accepted state: %s", app.CurrentState())
	// add an alloc
	uuid := "uuid-1"
	allocInfo := NewAllocation(uuid, nodeID1, ask)
	app.AddAllocation(allocInfo)
	// app should be starting
	assert.Assert(t, app.IsStarting(), "Application did not return starting state after alloc: %s", app.CurrentState())

	// removing the ask should not move anywhere as there is an allocation
	released = app.RemoveAllocationAsk(askID)
	assert.Equal(t, released, 0, "allocation ask should not have been reserved")
	assert.Assert(t, app.IsStarting(), "Application should have stayed same, changed unexpectedly: %s", app.CurrentState())

	// remove the allocation, ask has been removed so nothing left
	app.RemoveAllocation(uuid)
	assert.Assert(t, app.IsWaiting(), "Application did not change as expected: %s", app.CurrentState())
}

func TestStateChangeOnPlaceholderAdd(t *testing.T) {
	// create a fake queue
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	// fake the queue assignment
	app.queue = queue
	// app should be new
	assert.Assert(t, app.IsNew(), "New application did not return new state: %s", app.CurrentState())
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	askID := "ask-1"
	ask := newAllocationAskTG(askID, appID1, "TG1", res, 1)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	// app with ask, even for placeholder, should be accepted
	assert.Assert(t, app.IsAccepted(), "application did not change to accepted state: %s", app.CurrentState())

	// removing the ask should move it to waiting
	released := app.RemoveAllocationAsk(askID)
	assert.Equal(t, released, 0, "allocation ask should not have been reserved")
	assert.Assert(t, app.IsWaiting(), "application did not change to waiting state: %s", app.CurrentState())

	// start with a fresh state machine
	app = newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	// fake the queue assignment
	app.queue = queue
	// app should be new
	assert.Assert(t, app.IsNew(), "New application did not return new state: %s", app.CurrentState())
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	// app with ask should be accepted
	assert.Assert(t, app.IsAccepted(), "Application did not change to accepted state: %s", app.CurrentState())
	// add an alloc based on the placeholder ask
	uuid := "uuid-1"
	allocInfo := NewAllocation(uuid, nodeID1, ask)
	app.AddAllocation(allocInfo)
	// app should be in the same state as it was before as it is a placeholder allocation
	assert.Assert(t, app.IsAccepted(), "Application did not return accepted state after alloc: %s", app.CurrentState())
	assert.Assert(t, resources.Equals(app.GetPlaceholderResource(), res), "placeholder allocation not set as expected")
	assert.Assert(t, resources.IsZero(app.GetAllocatedResource()), "allocated resource should have been zero")

	// removing the ask should move the application into the waiting state, because the allocation is only a placeholder allocation
	released = app.RemoveAllocationAsk(askID)
	assert.Equal(t, released, 0, "allocation ask should not have been reserved")
	assert.Assert(t, app.IsWaiting(), "Application should have stayed same, changed unexpectedly: %s", app.CurrentState())

	// remove the allocation, ask has been removed so nothing left
	app.RemoveAllocation(uuid)
	assert.Assert(t, app.IsWaiting(), "Application did not change as expected: %s", app.CurrentState())
}

func TestAllocations(t *testing.T) {
	app := newApplication(appID1, "default", "root.a")

	// nothing allocated
	if !resources.IsZero(app.GetAllocatedResource()) {
		t.Error("new application has allocated resources")
	}
	// create an allocation and check the assignment
	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource with error")
	alloc := newAllocation(appID1, "uuid-1", nodeID1, "root.a", res)
	app.AddAllocation(alloc)
	if !resources.Equals(app.allocatedResource, res) {
		t.Errorf("allocated resources is not updated correctly: %v", app.allocatedResource)
	}
	allocs := app.GetAllAllocations()
	assert.Equal(t, len(allocs), 1)

	// add more allocations to test the removals
	alloc = newAllocation(appID1, "uuid-2", nodeID1, "root.a", res)
	app.AddAllocation(alloc)
	alloc = newAllocation(appID1, "uuid-3", nodeID1, "root.a", res)
	app.AddAllocation(alloc)
	allocs = app.GetAllAllocations()
	assert.Equal(t, len(allocs), 3)
	// remove one of the 3
	if alloc = app.RemoveAllocation("uuid-2"); alloc == nil {
		t.Error("returned allocations was nil allocation was not removed")
	}
	// try to remove a non existing alloc
	if alloc = app.RemoveAllocation("does-not-exist"); alloc != nil {
		t.Errorf("returned allocations was not allocation was incorrectly removed: %v", alloc)
	}
	// remove all left over allocations
	if allocs = app.RemoveAllAllocations(); allocs == nil || len(allocs) != 2 {
		t.Errorf("returned number of allocations was incorrect: %v", allocs)
	}
	allocs = app.GetAllAllocations()
	assert.Equal(t, len(allocs), 0)
}

func TestQueueUpdate(t *testing.T) {
	app := newApplication(appID1, "default", "root.a")

	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue")
	queue, err := createDynamicQueue(root, "test", false)
	assert.NilError(t, err, "failed to create test queue")
	app.SetQueue(queue)
	assert.Equal(t, app.QueueName, "root.test")
}

func TestStateTimeOut(t *testing.T) {
	startingTimeout = time.Microsecond * 100
	defer func() { startingTimeout = time.Minute * 5 }()
	app := newApplication(appID1, "default", "root.a")
	err := app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (timeout test)")
	err = app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected accepted to starting (timeout test)")
	// give it some time to run and progress
	time.Sleep(time.Millisecond * 100)
	if app.IsStarting() {
		t.Fatal("Starting state should have timed out")
	}
	if app.stateTimer != nil {
		t.Fatalf("Startup timer has not be cleared on time out as expected, %v", app.stateTimer)
	}

	startingTimeout = time.Millisecond * 100
	app = newApplication(appID1, "default", "root.a")
	err = app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (timeout test2)")
	err = app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected accepted to starting (timeout test2)")
	// give it some time to run and progress
	time.Sleep(time.Microsecond * 100)
	if !app.IsStarting() || app.stateTimer == nil {
		t.Fatalf("Starting state and timer should not have timed out yet, state: %s", app.stateMachine.Current())
	}
	err = app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected starting to run (timeout test2)")
	// give it some time to run and progress
	time.Sleep(time.Microsecond * 100)
	if !app.stateMachine.Is(Running.String()) || app.stateTimer != nil {
		t.Fatalf("State is not running or timer was not cleared, state: %s, timer %v", app.stateMachine.Current(), app.stateTimer)
	}
}

func TestCompleted(t *testing.T) {
	waitingTimeout = time.Millisecond * 100
	completedTimeout = time.Millisecond * 100
	defer func() {
		waitingTimeout = time.Second * 30
		completedTimeout = 30 * 24 * time.Hour
	}()
	app := newApplication(appID1, "default", "root.a")
	err := app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completed test)")
	err = app.HandleApplicationEvent(WaitApplication)
	assert.NilError(t, err, "no error expected accepted to waiting (completed test)")
	assert.Assert(t, app.IsWaiting(), "App should be waiting")
	// give it some time to run and progress
	err = common.WaitFor(10*time.Microsecond, time.Millisecond*200, app.IsCompleted)
	assert.NilError(t, err, "Application did not progress into Completed state")

	err = common.WaitFor(1*time.Millisecond, time.Millisecond*200, app.IsExpired)
	assert.NilError(t, err, "Application did not progress into Expired state")
}

func TestGetTag(t *testing.T) {
	app := newApplicationWithTags(appID1, "default", "root.a", nil)
	tag := app.GetTag("")
	assert.Equal(t, tag, "", "expected empty tag value if tags nil")
	tags := make(map[string]string)
	app = newApplicationWithTags(appID1, "default", "root.a", tags)
	tag = app.GetTag("")
	assert.Equal(t, tag, "", "expected empty tag value if no tags defined")
	tags["test"] = "test value"
	app = newApplicationWithTags(appID1, "default", "root.a", tags)
	tag = app.GetTag("notfound")
	assert.Equal(t, tag, "", "expected empty tag value if tag not found")
	tag = app.GetTag("test")
	assert.Equal(t, tag, "test value", "expected tag value")
}

func TestOnStatusChangeCalled(t *testing.T) {
	app := newApplication(appID1, "default", "root.a")
	assert.Equal(t, New.String(), app.CurrentState(), "new app not in New state")
	testHandler := &appEventHandler{}
	app.rmEventHandlers = handler.EventHandlers{
		RMProxyEventHandler: testHandler,
	}

	err := app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "error returned which was not expected")
	assert.Assert(t, testHandler.isHandled(), "handler did not get called as expected")

	// accepted to rejected: error expected
	err = app.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err != nil, "error expected and not seen")
	assert.Equal(t, app.CurrentState(), Accepted.String(), "application state has been changed unexpectedly")
	assert.Assert(t, !testHandler.isHandled(), "unexpected event send to the RM")
}

func TestReplaceAllocation(t *testing.T) {
	app := newApplication(appID1, "default", "root.a")
	assert.Equal(t, New.String(), app.CurrentState(), "new app not in New state")
	// state changes are not important
	app.SetState(Running.String())

	// non existing allocation
	var nilAlloc *Allocation
	alloc := app.ReplaceAllocation("")
	assert.Equal(t, alloc, nilAlloc, "expected nil to be returned got a real alloc: %s", alloc)
	// create an allocation and check the assignment
	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource with error")
	ph := newPlaceholderAlloc(appID1, "uuid-1", nodeID1, "root.a", res)
	// add the placeholder to the app
	app.AddAllocation(ph)
	assert.Equal(t, len(app.allocations), 1, "allocation not added as expected")
	assert.Assert(t, resources.IsZero(app.allocatedResource), "placeholder counted as real allocation")
	if !resources.Equals(app.allocatedPlaceholder, res) {
		t.Fatalf("placeholder allocation not updated as expected: got %s, expected %s", app.allocatedPlaceholder, res)
	}
	alloc = app.ReplaceAllocation("uuid-1")
	assert.Equal(t, alloc, nilAlloc, "placeholder without releases expected nil to be returned got a real alloc: %s", alloc)
	// add the placeholder back to the app, the failure test above changed state and removed the ph
	app.SetState(Running.String())
	app.AddAllocation(ph)
	// set the real one to replace the placeholder
	realAlloc := newAllocation(appID1, "uuid-2", nodeID1, "root.a", res)
	realAlloc.Result = Replaced
	ph.Releases = append(ph.Releases, realAlloc)
	alloc = app.ReplaceAllocation("uuid-1")
	assert.Equal(t, alloc, ph, "returned allocation is not the placeholder")
	assert.Assert(t, resources.IsZero(app.allocatedPlaceholder), "real allocation counted as placeholder")
	if !resources.Equals(app.allocatedResource, res) {
		t.Fatalf("real allocation not updated as expected: got %s, expected %s", app.allocatedResource, res)
	}

	// add the placeholder back to the app, the failure test above changed state and removed the ph
	app.SetState(Running.String())
	ph.Releases = nil
	app.AddAllocation(ph)
	// set multiple real allocations to replace the placeholder
	realAlloc = newAllocation(appID1, "uuid-3", nodeID1, "root.a", res)
	realAlloc.Result = Replaced
	ph.Releases = append(ph.Releases, realAlloc)
	realAlloc = newAllocation(appID1, "not-added", nodeID1, "root.a", res)
	realAlloc.Result = Replaced
	ph.Releases = append(ph.Releases, realAlloc)
	alloc = app.ReplaceAllocation("uuid-1")
	assert.Equal(t, alloc, ph, "returned allocation is not the placeholder")
	assert.Assert(t, resources.IsZero(app.allocatedPlaceholder), "real allocation counted as placeholder")
	// after the second replace we have 2 real allocations
	if !resources.Equals(app.allocatedResource, resources.Multiply(res, 2)) {
		t.Fatalf("real allocation not updated as expected: got %s, expected %s", app.allocatedResource, resources.Multiply(res, 2))
	}
	if _, ok := app.allocations["not-added"]; ok {
		t.Fatalf("real allocation added which shouldn't have been added")
	}
}
