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
	"testing"

	"gotest.tools/assert"

	"github.com/cloudera/yunikorn-core/pkg/cache"
	"github.com/cloudera/yunikorn-core/pkg/common/resources"
)

func TestAppAllocating(t *testing.T) {
	appID := "app-1"
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: appID})
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
		t.Fatalf("app create failed which should not have %v", app)
	}
	if !resources.IsZero(app.allocating) {
		t.Fatalf("app should not have allocating resources: %v", app.allocating.Resources)
	}
	// simple one resource add
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	app.incAllocating(res)
	assert.Equal(t, len(app.allocating.Resources), 1, "app allocating resources not showing correct resources numbers")
	if !resources.Equals(res, app.allocating) {
		t.Errorf("app allocating resources not incremented correctly: %v", app.allocating.Resources)
	}

	// inc with a second resource type: should merge
	res2 := resources.NewResourceFromMap(map[string]resources.Quantity{"second": 1})
	resTotal := resources.Add(res, res2)
	app.incAllocating(res2)
	assert.Equal(t, len(app.allocating.Resources), 2, "app allocating resources not showing correct resources numbers")
	if !resources.Equals(resTotal, app.allocating) {
		t.Errorf("app allocating resources not incremented correctly: %v", app.allocating.Resources)
	}

	// dec just left with the second resource type
	app.decAllocating(res)
	assert.Equal(t, len(app.allocating.Resources), 2, "app allocating resources not showing correct resources numbers")
	if !resources.Equals(res2, app.allocating) {
		t.Errorf("app allocating resources not decremented correctly: %v", app.allocating.Resources)
	}
	// dec with total: one resource type would go negative but cannot
	app.decAllocating(resTotal)
	assert.Equal(t, len(app.allocating.Resources), 2, "app allocating resources not showing correct resources numbers")
	if !resources.IsZero(app.allocating) {
		t.Errorf("app should not have allocating resources: %v", app.allocating.Resources)
	}
}

func TestAppReservation(t *testing.T) {
	appID := "app-1"
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: appID})
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
	app = newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: appID})
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

func TestAppAllocReservation(t *testing.T) {
	appID := "app-1"
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: appID})
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
		t.Fatalf("app create failed which should not have %v", app)
	}
	if app.hasReserved() {
		t.Fatal("new app should not have reservations")
	}
	if len(app.isAskReserved("")) != 0 {
		t.Fatal("new app should not have reservation for empty allocKey")
	}

	// reserve 1 allocate ask
	allocKey := "alloc-1"
	nodeID1 := "node-1"
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskRepeat(allocKey, appID, res, 2)
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 10})
	// reserve that works
	ok, err := app.reserve(node1, ask)
	if !ok || err != nil {
		t.Errorf("reservation should not have failed: status %t, error %v", ok, err)
	}
	if len(app.isAskReserved("")) != 0 {
		t.Fatal("app should not have reservation for empty allocKey")
	}
	askReserved := app.isAskReserved(allocKey)
	if len(askReserved) != 1 || askReserved[0] != nodeID1+"|"+allocKey {
		t.Errorf("app should have reservations for %s on %s and has not", allocKey, nodeID1)
	}

	nodeID2 := "node-2"
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 10})
	ok, err = app.reserve(node2, ask)
	if !ok || err != nil {
		t.Errorf("reservation should not have failed: status %t, error %v", ok, err)
	}
	askReserved = app.isAskReserved(allocKey)
	if len(askReserved) != 2 || askReserved[1] != nodeID2+"|"+allocKey {
		t.Errorf("app should have reservations for %s on %s and has not", allocKey, nodeID2)
	}

	// check exceeding ask repeat: nothing should change
	if app.canAskReserve(ask) {
		t.Error("ask has maximum repeats reserved")
	}
	node3 := newNode("node-3", map[string]resources.Quantity{"first": 10})
	ok, err = app.reserve(node3, ask)
	if ok || err == nil {
		t.Errorf("reservation should have failed: status %t, error %v", ok, err)
	}
	askReserved = app.isAskReserved(allocKey)
	if len(askReserved) != 2 || askReserved[0] != nodeID1+"|"+allocKey || askReserved[1] != nodeID2+"|"+allocKey {
		t.Errorf("app should have reservations for node %s and %s and has not", nodeID1, nodeID2)
	}
}

func TestUpdateRepeat(t *testing.T) {
	appID := "app-1"
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: appID})
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
		t.Fatalf("app create failed which should not have %v", app)
	}

	// failure cases
	delta, err := app.updateAllocationAskRepeat("", 0)
	if err == nil || delta != nil {
		t.Error("empty ask key should not have been found")
	}
	delta, err = app.updateAllocationAskRepeat("unknown", 0)
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
	delta, err = app.updateAllocationAskRepeat(allocKey, 0)
	if err != nil || !resources.IsZero(delta) {
		t.Errorf("0 increase should return zero delta and did not: %v, err %v", delta, err)
	}
	delta, err = app.updateAllocationAskRepeat(allocKey, 1)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("increase did not return correct delta, err %v, expected %v got %v", err, res, delta)
	}

	// decrease to zero
	delta, err = app.updateAllocationAskRepeat(allocKey, -2)
	if err != nil || !resources.Equals(resources.Multiply(res, -2), delta) {
		t.Errorf("decrease did not return correct delta, err %v, expected %v got %v", err, resources.Multiply(res, -2), delta)
	}
	// decrease to below zero
	delta, err = app.updateAllocationAskRepeat(allocKey, -1)
	if err == nil || delta != nil {
		t.Errorf("decrease did not return correct delta, err %v, delta %v", err, delta)
	}
}

func TestAddAllocAsk(t *testing.T) {
	appID := "app-1"
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: appID})
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
		t.Fatalf("app create failed which should not have %v", app)
	}

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
	if !resources.Equals(res, app.pending) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", res, delta)
	}
	ask = newAllocationAskRepeat(allocKey, appID, res, 2)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("ask should have been added to app, err %v, expected delta %v but was: %v", err, res, delta)
	}
	if !resources.Equals(resources.Multiply(res, 2), app.pending) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", resources.Multiply(res, 2), delta)
	}

	// change both resource and count
	ask = newAllocationAskRepeat(allocKey, appID, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 3}), 5)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("ask should have been added to app, err %v, expected delta %v but was: %v", err, res, delta)
	}
	if !resources.Equals(resources.Multiply(res, 3), app.pending) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", resources.Multiply(res, 3), delta)
	}

	// test a decrease of repeat and back to start
	ask = newAllocationAskRepeat(allocKey, appID, res, 1)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(resources.Multiply(res, -2), delta) {
		t.Errorf("ask should have been added to app, err %v, expected delta %v but was: %v", err, resources.Multiply(res, -2), delta)
	}
	if !resources.Equals(res, app.pending) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", res, delta)
	}
}

func TestRemoveAllocAsk(t *testing.T) {
	appID := "app-1"
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: appID})
	if app == nil || app.ApplicationInfo.ApplicationID != appID {
		t.Fatalf("app create failed which should not have %v", app)
	}

	// failures cases
	noDelta := app.removeAllocationAsk("")
	if noDelta != nil {
		t.Errorf("empty ask key should not have returned delta: %v", noDelta)
	}
	noDelta = app.removeAllocationAsk("unknown")
	if noDelta != nil {
		t.Errorf("unknown ask key should not have returned delta: %v", noDelta)
	}

	// setup the allocs
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskRepeat("alloc-1", appID, res, 2)
	delta, err := app.addAllocationAsk(ask)
	if err != nil {
		t.Fatalf("ask 1 should have been added to app, returned delta: %v", delta)
	}
	ask = newAllocationAskRepeat("alloc-2", appID, res, 2)
	delta, err = app.addAllocationAsk(ask)
	if delta == nil {
		t.Fatalf("ask 2 should have been added to app, returned delta: %v", delta)
	}
	if len(app.requests) != 2 {
		t.Fatalf("missing asks from app expected 2 got %d", len(app.requests))
	}

	// test removes
	noDelta = app.removeAllocationAsk("unknown")
	if noDelta != nil {
		t.Errorf("unknown ask key should not have returned delta: %v", noDelta)
	}
	delta = app.removeAllocationAsk("alloc-1")
	if !resources.Equals(resources.Multiply(res, -2), delta) {
		t.Errorf("ask should have been added to app, err %v, expected delta %v but was: %v", err, resources.Multiply(res, -2), delta)
	}
	delta = app.removeAllocationAsk("")
	if !resources.Equals(resources.Multiply(res, -2), delta) {
		t.Errorf("ask should have been added to app, err %v, expected delta %v but was: %v", err, resources.Multiply(res, -2), delta)
	}
	if len(app.requests) != 0 {
		t.Fatalf("asks not removed from as expected got %d", len(app.requests))
	}
}
