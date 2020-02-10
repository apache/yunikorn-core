/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

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

package scheduler

import (
	"strconv"
	"testing"

	"gotest.tools/assert"

	"github.com/cloudera/yunikorn-core/pkg/cache"
	"github.com/cloudera/yunikorn-core/pkg/common/resources"
	"github.com/cloudera/yunikorn-core/pkg/common/security"
)

// test allocating calculation
func TestAppAllocating(t *testing.T) {
	appID := "app-1"
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
		t.Fatalf("app create failed which should not have %v", app)
	}
	if !resources.IsZero(app.allocating) {
		t.Fatalf("app should not have allocating resources: %v", app.allocating.Resources)
	}
	// simple one resource add
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	app.incAllocatingResource(res)
	assert.Equal(t, len(app.allocating.Resources), 1, "app allocating resources not showing correct resources numbers")
	if !resources.Equals(res, app.getAllocatingResource()) {
		t.Errorf("app allocating resources not incremented correctly: %v", app.allocating.Resources)
	}

	// inc with a second resource type: should merge
	res2 := resources.NewResourceFromMap(map[string]resources.Quantity{"second": 1})
	resTotal := resources.Add(res, res2)
	app.incAllocatingResource(res2)
	assert.Equal(t, len(app.allocating.Resources), 2, "app allocating resources not showing correct resources numbers")
	if !resources.Equals(resTotal, app.getAllocatingResource()) {
		t.Errorf("app allocating resources not incremented correctly: %v", app.allocating.Resources)
	}

	// dec just left with the second resource type
	app.decAllocatingResource(res)
	assert.Equal(t, len(app.allocating.Resources), 2, "app allocating resources not showing correct resources numbers")
	if !resources.Equals(res2, app.getAllocatingResource()) {
		t.Errorf("app allocating resources not decremented correctly: %v", app.allocating.Resources)
	}
	// dec with total: one resource type would go negative but cannot
	app.decAllocatingResource(resTotal)
	assert.Equal(t, len(app.allocating.Resources), 2, "app allocating resources not showing correct resources numbers")
	if !resources.IsZero(app.getAllocatingResource()) {
		t.Errorf("app should not have allocating resources: %v", app.allocating.Resources)
	}
}

// test basic reservations
func TestAppReservation(t *testing.T) {
	appID := "app-1"
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
		t.Fatalf("app create failed which should not have %v", app)
	}
	if app.hasReserved() {
		t.Fatal("new app should not have reservations")
	}
	if app.isReservedOnNode("") {
		t.Error("app should not have reservations for empty node ID")
	}
	if app.isReservedOnNode("unknown") {
		t.Error("new app should not have reservations for unknown node")
	}

	queue, err := createRootQueue(nil)
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
	app.queue = queue

	// reserve illegal request
	ok, err := app.reserve(nil, nil)
	if ok || err == nil {
		t.Errorf("illegal reservation requested but did not fail: status %t, error %v", ok, err)
	}

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	askKey := "alloc-1"
	ask := newAllocationAsk(askKey, appID, res)
	nodeID := "node-1"
	node := newNode(nodeID, map[string]resources.Quantity{"first": 10})

	// too large for node
	ok, err = app.reserve(node, ask)
	if ok || err == nil {
		t.Errorf("requested reservation does not fit in node resource but did not fail: status %t, error %v", ok, err)
	}

	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask = newAllocationAsk(askKey, appID, res)
	app = newSchedulingApplication(appInfo)
	app.queue = queue
	var delta *resources.Resource
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("ask should have been added to app, err %v, expected delta %v but was: %v", err, res, delta)
	}
	// reserve that works
	ok, err = app.reserve(node, ask)
	if !ok || err != nil {
		t.Errorf("reservation should not have failed: status %t, error %v", ok, err)
	}
	if app.isReservedOnNode("") {
		t.Errorf("app should not have reservations for empty node ID")
	}
	if app.isReservedOnNode("unknown") {
		t.Error("app should not have reservations for unknown node")
	}
	if app.hasReserved() && !app.isReservedOnNode(nodeID) {
		t.Errorf("app should have reservations for node %s", nodeID)
	}

	// reserve the same reservation
	ok, err = app.reserve(node, ask)
	if ok || err == nil {
		t.Errorf("reservation should have failed: status %t, error %v", ok, err)
	}

	// unreserve unknown node/ask
	ok, err = app.unReserve(nil, nil)
	if ok || err == nil {
		t.Errorf("illegal reservation release but did not fail: status %t, error %v", ok, err)
	}

	// 2nd reservation for app
	ask2 := newAllocationAsk("alloc-2", appID, res)
	node2 := newNode("node-2", map[string]resources.Quantity{"first": 10})
	delta, err = app.addAllocationAsk(ask2)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("ask2 should have been added to app, err %v, expected delta %v but was: %v", err, res, delta)
	}
	ok, err = app.reserve(node, ask2)
	if ok || err == nil {
		t.Errorf("reservation of node by second ask should have failed: status %t, error %v", ok, err)
	}
	ok, err = app.reserve(node2, ask2)
	if !ok || err != nil {
		t.Errorf("reservation of 2nd node should not have failed: status %t, error %v", ok, err)
	}
	ok, err = app.unReserve(node2, ask2)
	if !ok || err != nil {
		t.Errorf("remove of reservation of 2nd node should not have failed: status %t, error %v", ok, err)
	}
	// unreserve the same should fail
	ok, err = app.unReserve(node2, ask2)
	if ok && err == nil {
		t.Errorf("remove twice of reservation of 2nd node should have failed: status %t, error %v", ok, err)
	}

	// failure case: remove reservation from node
	ok, err = node.unReserve(app, ask)
	if !ok || err != nil {
		t.Fatalf("un-reserve on node should not have failed: status %t, error %v", ok, err)
	}
	ok, err = app.unReserve(node, ask)
	if ok || err != nil {
		t.Errorf("node does not have reservation removal of app reservation should have failed: status %t, error %v", ok, err)
	}
}

// test multiple reservations from one allocation
func TestAppAllocReservation(t *testing.T) {
	appID := "app-1"
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
		t.Fatalf("app create failed which should not have %v", app)
	}
	if app.hasReserved() {
		t.Fatal("new app should not have reservations")
	}
	if len(app.isAskReserved("")) != 0 {
		t.Fatal("new app should not have reservation for empty allocKey")
	}
	queue, err := createRootQueue(nil)
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
	app.queue = queue

	// reserve 1 allocate ask
	allocKey := "alloc-1"
	nodeID1 := "node-1"
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskRepeat(allocKey, appID, res, 2)
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 10})
	// reserve that works
	var delta *resources.Resource
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(resources.Multiply(res, 2), delta) {
		t.Errorf("ask should have been added to app, err %v, expected delta %v but was: %v", err, resources.Multiply(res, 2), delta)
	}
	var ok bool
	ok, err = app.reserve(node1, ask)
	if !ok || err != nil {
		t.Errorf("reservation should not have failed: status %t, error %v", ok, err)
	}
	if len(app.isAskReserved("")) != 0 {
		t.Fatal("app should not have reservation for empty allocKey")
	}
	nodeKey1 := nodeID1 + "|" + allocKey
	askReserved := app.isAskReserved(allocKey)
	if len(askReserved) != 1 || askReserved[0] != nodeKey1 {
		t.Errorf("app should have reservations for %s on %s and has not", allocKey, nodeID1)
	}

	nodeID2 := "node-2"
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 10})
	ok, err = app.reserve(node2, ask)
	if !ok || err != nil {
		t.Errorf("reservation should not have failed: status %t, error %v", ok, err)
	}
	nodeKey2 := nodeID2 + "|" + allocKey
	askReserved = app.isAskReserved(allocKey)
	if len(askReserved) != 2 && (askReserved[0] != nodeKey2 || askReserved[1] != nodeKey2) {
		t.Errorf("app should have reservations for %s on %s and has not", allocKey, nodeID2)
	}

	// check exceeding ask repeat: nothing should change
	if app.canAskReserve(ask) {
		t.Error("ask has maximum repeats reserved, reserve check should have failed")
	}
	node3 := newNode("node-3", map[string]resources.Quantity{"first": 10})
	ok, err = app.reserve(node3, ask)
	if ok || err == nil {
		t.Errorf("reservation should have failed: status %t, error %v", ok, err)
	}
	askReserved = app.isAskReserved(allocKey)
	if len(askReserved) != 2 && (askReserved[0] != nodeKey1 || askReserved[1] != nodeKey1) &&
		(askReserved[0] != nodeKey2 || askReserved[1] != nodeKey2) {
		t.Errorf("app should have reservations for node %s and %s and has not: %v", nodeID1, nodeID2, askReserved)
	}
	// clean up all asks and reservations
	app.removeAllocationAsk("")
	if app.hasReserved() || node1.isReserved() || node2.isReserved() {
		t.Error("ask removal did not clean up all reservations")
	}
}

// test update allocation repeat
func TestUpdateRepeat(t *testing.T) {
	appID := "app-1"
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
		t.Fatalf("app create failed which should not have %v", app)
	}
	queue, err := createRootQueue(nil)
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
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
	allocKey := "alloc-1"
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskRepeat(allocKey, appID, res, 1)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("ask should have been added to app, err %v, expected delta %v but was: %v", err, res, delta)
	}
	delta, err = app.updateAskRepeat(allocKey, 0)
	if err != nil || !resources.IsZero(delta) {
		t.Errorf("0 increase should return zero delta and did not: %v, err %v", delta, err)
	}
	delta, err = app.updateAskRepeat(allocKey, 1)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("increase did not return correct delta, err %v, expected %v got %v", err, res, delta)
	}

	// decrease to zero
	delta, err = app.updateAskRepeat(allocKey, -2)
	if err != nil || !resources.Equals(resources.Multiply(res, -2), delta) {
		t.Errorf("decrease did not return correct delta, err %v, expected %v got %v", err, resources.Multiply(res, -2), delta)
	}
	// decrease to below zero
	delta, err = app.updateAskRepeat(allocKey, -1)
	if err == nil || delta != nil {
		t.Errorf("decrease did not return correct delta, err %v, delta %v", err, delta)
	}
}

// test pending calculation and ask addition
func TestAddAllocAsk(t *testing.T) {
	appID := "app-1"
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
		t.Fatalf("app create failed which should not have %v", app)
	}

	queue, err := createRootQueue(nil)
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
	app.queue = queue

	// failure cases
	delta, err := app.addAllocationAsk(nil)
	if err == nil {
		t.Errorf("nil ask should not have been added to app, returned delta: %v", delta)
	}
	allocKey := "alloc-1"
	res := resources.NewResource()
	ask := newAllocationAsk(allocKey, appID, res)
	delta, err = app.addAllocationAsk(ask)
	if err == nil {
		t.Errorf("zero resource ask should not have been added to app, returned delta: %v", delta)
	}
	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask = newAllocationAskRepeat(allocKey, appID, res, 0)
	delta, err = app.addAllocationAsk(ask)
	if err == nil {
		t.Errorf("ask with zero repeat should not have been added to app, returned delta: %v", delta)
	}

	// working cases
	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask = newAllocationAskRepeat(allocKey, appID, res, 1)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("ask should have been added to app, err %v, expected delta %v but was: %v", err, res, delta)
	}
	if !resources.Equals(res, app.GetPendingResource()) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", res, delta)
	}
	ask = newAllocationAskRepeat(allocKey, appID, res, 2)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("ask should have been added to app, err %v, expected delta %v but was: %v", err, res, delta)
	}
	if !resources.Equals(resources.Multiply(res, 2), app.GetPendingResource()) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", resources.Multiply(res, 2), delta)
	}

	// change both resource and count
	ask = newAllocationAskRepeat(allocKey, appID, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 3}), 5)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("ask should have been added to app, err %v, expected delta %v but was: %v", err, res, delta)
	}
	if !resources.Equals(resources.Multiply(res, 3), app.GetPendingResource()) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", resources.Multiply(res, 3), delta)
	}

	// test a decrease of repeat and back to start
	ask = newAllocationAskRepeat(allocKey, appID, res, 1)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(resources.Multiply(res, -2), delta) {
		t.Errorf("ask should have been added to app, err %v, expected delta %v but was: %v", err, resources.Multiply(res, -2), delta)
	}
	if !resources.Equals(res, app.GetPendingResource()) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", res, delta)
	}
}

// test reservations removal by allocation
func TestRemoveReservedAllocAsk(t *testing.T) {
	appID := "app-1"
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
		t.Fatalf("app create failed which should not have %v", app)
	}
	queue, err := createRootQueue(nil)
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
	app.queue = queue

	// create app and allocs
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAsk("alloc-1", appID, res)
	delta, err := app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(res, delta) {
		t.Fatalf("resource ask1 should have been added to app: %v (err = %v)", delta, err)
	}
	allocKey := "alloc-2"
	ask = newAllocationAsk(allocKey, appID, res)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(res, delta) {
		t.Fatalf("resource ask2 should have been added to app: %v (err = %v)", delta, err)
	}
	// reserve one alloc and remove
	nodeID := "node-1"
	node := newNode(nodeID, map[string]resources.Quantity{"first": 10})
	var ok bool
	ok, err = app.reserve(node, ask)
	if !ok || err != nil {
		t.Errorf("reservation should not have failed: status %t, error %v", ok, err)
	}
	if len(app.isAskReserved(allocKey)) != 1 || !node.isReserved() {
		t.Fatalf("app should have reservation for %v on node", allocKey)
	}
	before := app.GetPendingResource().Clone()
	app.removeAllocationAsk(allocKey)
	delta = resources.Sub(before, app.GetPendingResource())
	if !resources.Equals(res, delta) {
		t.Errorf("resource ask2 should have been removed from app: %v", delta)
	}
	if app.hasReserved() || node.isReserved() {
		t.Fatal("app and node should not have reservations")
	}

	// reserve again: then remove from node before remove from app
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(res, delta) {
		t.Fatalf("resource ask2 should have been added to app: %v (err = %v)", delta, err)
	}
	ok, err = app.reserve(node, ask)
	if !ok || err != nil {
		t.Errorf("reservation should not have failed: status %t, error %v", ok, err)
	}
	if len(app.isAskReserved(allocKey)) != 1 || !node.isReserved() {
		t.Fatalf("app should have reservation for %v on node", allocKey)
	}
	ok, err = node.unReserve(app, ask)
	if !ok || err != nil {
		t.Errorf("unreserve on node should not have failed: status %t, error %v", ok, err)
	}
	before = app.GetPendingResource().Clone()
	app.removeAllocationAsk(allocKey)
	delta = resources.Sub(before, app.GetPendingResource())
	if !resources.Equals(res, delta) {
		t.Errorf("resource ask2 should have been removed from app: %v", delta)
	}
	// app reservation is not removed due to the node removal failure
	if !app.hasReserved() || node.isReserved() {
		t.Fatal("app should and node should not have reservations")
	}
	// clean up
	app.removeAllocationAsk("")
	if !resources.IsZero(app.GetPendingResource()) {
		t.Errorf("all resource asks should have been removed from app: %v", app.GetPendingResource())
	}
	// app reservation is still not removed due to the node removal failure
	if !app.hasReserved() || node.isReserved() {
		t.Fatal("app should and node should not have reservations")
	}
}

// test pending calculation and ask removal
func TestRemoveAllocAsk(t *testing.T) {
	appID := "app-1"
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
		t.Fatalf("app create failed which should not have %v", app)
	}
	queue, err := createRootQueue(nil)
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
	app.queue = queue

	// failures cases: things should not crash (nothing happens)
	app.removeAllocationAsk("")
	if !resources.IsZero(app.GetPendingResource()) {
		t.Errorf("pending resource not updated correctly removing all, expected zero but was: %v", app.GetPendingResource())
	}
	app.removeAllocationAsk("unknown")
	if !resources.IsZero(app.GetPendingResource()) {
		t.Errorf("pending resource not updated correctly removing unknown, expected zero but was: %v", app.GetPendingResource())
	}

	// setup the allocs
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskRepeat("alloc-1", appID, res, 2)
	var delta1 *resources.Resource
	delta1, err = app.addAllocationAsk(ask)
	if err != nil {
		t.Fatalf("ask 1 should have been added to app, returned delta: %v", delta1)
	}
	ask = newAllocationAskRepeat("alloc-2", appID, res, 2)
	var delta2 *resources.Resource
	delta2, err = app.addAllocationAsk(ask)
	if err != nil {
		t.Fatalf("ask 2 should have been added to app, returned delta: %v", delta2)
	}
	if len(app.requests) != 2 {
		t.Fatalf("missing asks from app expected 2 got %d", len(app.requests))
	}
	if !resources.Equals(resources.Add(delta1, delta2), app.GetPendingResource()) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", resources.Add(delta1, delta2), app.GetPendingResource())
	}

	// test removes unknown (nothing happens)
	app.removeAllocationAsk("unknown")
	before := app.GetPendingResource().Clone()
	app.removeAllocationAsk("alloc-1")
	delta := resources.Sub(before, app.GetPendingResource())
	if !resources.Equals(delta, delta1) {
		t.Errorf("ask should have been removed from app, err %v, expected delta %v but was: %v", err, delta1, delta)
	}
	app.removeAllocationAsk("")
	if len(app.requests) != 0 {
		t.Fatalf("asks not removed from as expected got %d", len(app.requests))
	}
	if !resources.IsZero(app.GetPendingResource()) {
		t.Errorf("pending resource not updated correctly, expected zero but was: %v", app.GetPendingResource())
	}
}

// test allocating and allocated calculation
func TestUnconfirmedAppCalc(t *testing.T) {
	appID := "app-1"
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
		t.Fatalf("app create failed which should not have %v", app)
	}
	unconfirmed := app.getUnconfirmedAllocated()
	if !resources.IsZero(unconfirmed) {
		t.Errorf("app unconfirmed and allocated resources not set as expected 0, got %v", unconfirmed)
	}
	res := map[string]string{"first": "1"}
	allocation, err := resources.NewResourceFromConf(res)
	if err != nil {
		t.Fatalf("failed to create basic resource: %v", err)
	}
	app.incAllocatingResource(allocation)
	unconfirmed = app.getUnconfirmedAllocated()
	if !resources.Equals(allocation, unconfirmed) {
		t.Errorf("app unconfirmed and allocated resources not set as expected %v, got %v", allocation, unconfirmed)
	}
	allocInfo := cache.CreateMockAllocationInfo("app-1", allocation, "uuid", "root.leaf", "node-1")
	cache.AddAllocationToApp(app.ApplicationInfo, allocInfo)
	unconfirmed = app.getUnconfirmedAllocated()
	allocation = resources.Multiply(allocation, 2)
	if !resources.Equals(allocation, unconfirmed) {
		t.Errorf("app unconfirmed and allocated resources not set as expected %v, got %v", allocation, unconfirmed)
	}
}

// This test must not test the sorter that is underlying.
// It tests the queue specific parts of the code only.
func TestSortRequests(t *testing.T) {
	appID := "app-1"
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
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
		ask := newAllocationAsk("ask-"+num, "app-1", res)
		ask.priority = int32(i)
		app.requests[ask.AskProto.AllocationKey] = ask
	}
	app.sortRequests(true)
	if len(app.sortedRequests) != 3 {
		t.Fatalf("app sorted requests not correct: %v", app.sortedRequests)
	}
	allocKey := app.sortedRequests[0].AskProto.AllocationKey
	delete(app.requests, allocKey)
	app.sortRequests(true)
	if len(app.sortedRequests) != 2 {
		t.Fatalf("app sorted requests not correct after removal: %v", app.sortedRequests)
	}
}
