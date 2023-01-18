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
	"math"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/handler"
	"github.com/apache/yunikorn-core/pkg/rmproxy"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func setupUGM() {
	userManager := ugm.GetUserManager()
	userManager.ClearUserTrackers()
	userManager.ClearGroupTrackers()
}

// basic app creating with timeout checks
func TestNewApplication(t *testing.T) {
	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{},
	}
	siApp := &si.AddApplicationRequest{}
	app := NewApplication(siApp, user, nil, "")
	assert.Equal(t, app.ApplicationID, "", "application ID should not be set was not set in SI")
	assert.Equal(t, app.GetQueuePath(), "", "queue name should not be set was not set in SI")
	assert.Equal(t, app.Partition, "", "partition name should not be set was not set in SI")
	assert.Equal(t, app.rmID, "", "RM ID should not be set was not passed in")
	assert.Equal(t, app.rmEventHandler, handler.EventHandler(nil), "event handler should be nil")
	// just check one of the resources...
	assert.Assert(t, resources.IsZero(app.placeholderAsk), "placeholder ask should be zero")
	assert.Assert(t, app.IsNew(), "new application must be in new state")
	// with the basics check the one thing that can really change
	assert.Equal(t, app.execTimeout, defaultPlaceholderTimeout, "No timeout passed in should be default")
	siApp.ExecutionTimeoutMilliSeconds = -1
	app = NewApplication(siApp, user, nil, "")
	assert.Equal(t, app.execTimeout, defaultPlaceholderTimeout, "Negative timeout passed in should be default")
	siApp.ExecutionTimeoutMilliSeconds = math.MaxInt64
	app = NewApplication(siApp, user, nil, "")
	assert.Equal(t, app.execTimeout, defaultPlaceholderTimeout, "overly large timeout should be set to default")
	siApp.ExecutionTimeoutMilliSeconds = 60000
	app = NewApplication(siApp, user, nil, "")
	assert.Equal(t, app.execTimeout, 60*time.Second, "correct timeout should not set")

	originalPhTimeout := defaultPlaceholderTimeout
	defaultPlaceholderTimeout = 100 * time.Microsecond
	defer func() { defaultPlaceholderTimeout = originalPhTimeout }()
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	siApp = &si.AddApplicationRequest{
		ApplicationID:                "appID",
		QueueName:                    "some.queue",
		PartitionName:                "AnotherPartition",
		ExecutionTimeoutMilliSeconds: 0,
		PlaceholderAsk:               &si.Resource{Resources: map[string]*si.Quantity{"first": {Value: 1}}},
	}
	app = NewApplication(siApp, user, rmproxy.NewMockedRMProxy(), "myRM")
	assert.Equal(t, app.ApplicationID, "appID", "application ID should not be set to SI value")
	assert.Equal(t, app.GetQueuePath(), "some.queue", "queue name should not be set to SI value")
	assert.Equal(t, app.Partition, "AnotherPartition", "partition name should be set to SI value")
	if app.rmEventHandler == nil {
		t.Fatal("non nil handler was not set in the new app")
	}
	assertUserGroupResource(t, getTestUserGroup(), nil)
	assert.Assert(t, app.IsNew(), "new application must be in new state")
	assert.Equal(t, app.execTimeout, defaultPlaceholderTimeout, "no timeout passed in should be modified default")
	assert.Assert(t, resources.Equals(app.placeholderAsk, res), "placeholder ask not set as expected")
}

// test basic reservations
func TestAppReservation(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	if app.HasReserved() {
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
	if app.HasReserved() && !app.IsReservedOnNode(nodeID1) {
		t.Errorf("app should have reservations for node %s", nodeID1)
	}

	// node name similarity check: chop of the last char to make sure we check the full name
	similar := nodeID1[:len(nodeID1)-1]
	if app.HasReserved() && app.IsReservedOnNode(similar) {
		t.Errorf("similar app should not have reservations for node %s", similar)
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
	if app.HasReserved() {
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
	if app.HasReserved() || node1.IsReserved() || node2.IsReserved() || reservedAsks != 2 {
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
	var delta *resources.Resource
	delta, err = app.UpdateAskRepeat("", 0)
	if err == nil || delta != nil {
		t.Error("empty ask key should not have been found")
	}
	delta, err = app.UpdateAskRepeat("unknown", 0)
	if err == nil || delta != nil {
		t.Error("unknown ask key should not have been found")
	}

	// working cases
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskRepeat(aKey, appID1, res, 1)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	delta, err = app.UpdateAskRepeat(aKey, 0)
	if err != nil || !resources.IsZero(delta) {
		t.Errorf("0 increase should return zero delta and did not: %v, err %v", delta, err)
	}
	delta, err = app.UpdateAskRepeat(aKey, 1)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("increase did not return correct delta, err %v, expected %v got %v", err, res, delta)
	}

	// decrease to zero
	delta, err = app.UpdateAskRepeat(aKey, -2)
	if err != nil || !resources.Equals(resources.Multiply(res, -2), delta) {
		t.Errorf("decrease did not return correct delta, err %v, expected %v got %v", err, resources.Multiply(res, -2), delta)
	}
	// decrease to below zero
	delta, err = app.UpdateAskRepeat(aKey, -1)
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

	// test PlaceholderData
	tg1 := "tg-1"
	ask = newAllocationAskTG(aKey, appID1, tg1, res, 1)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been updated on app")
	app.SetTimedOutPlaceholder(tg1, 1)
	app.SetTimedOutPlaceholder("tg-2", 2)
	clonePlaceholderData := app.GetAllPlaceholderData()
	assert.Equal(t, len(clonePlaceholderData), 1)
	assert.Equal(t, len(app.placeholderData), 1)
	assert.Equal(t, clonePlaceholderData[0], app.placeholderData[tg1])
	assert.Equal(t, app.placeholderData[tg1].TaskGroupName, tg1)
	assert.Equal(t, app.placeholderData[tg1].Count, int64(1))
	assert.Equal(t, app.placeholderData[tg1].Replaced, int64(0))
	assert.Equal(t, app.placeholderData[tg1].TimedOut, int64(1))
	assert.DeepEqual(t, app.placeholderData[tg1].MinResource, res)

	ask = newAllocationAskTG(aKey, appID1, tg1, res, 1)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been updated on app")
	assert.Equal(t, len(app.placeholderData), 1)
	assert.Equal(t, app.placeholderData[tg1].TaskGroupName, tg1)
	assert.Equal(t, app.placeholderData[tg1].Count, int64(2))
	assert.Equal(t, app.placeholderData[tg1].Replaced, int64(0))
	assert.Equal(t, app.placeholderData[tg1].TimedOut, int64(1))
	assert.DeepEqual(t, app.placeholderData[tg1].MinResource, res)

	tg2 := "tg-2"
	ask = newAllocationAskTG(aKey, appID1, tg2, res, 1)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been updated on app")
	assert.Equal(t, len(app.placeholderData), 2)
	assert.Equal(t, app.placeholderData[tg2].TaskGroupName, tg2)
	assert.Equal(t, app.placeholderData[tg2].Count, int64(1))
	assert.Equal(t, app.placeholderData[tg2].Replaced, int64(0))
	assert.Equal(t, app.placeholderData[tg2].TimedOut, int64(0))
	assert.DeepEqual(t, app.placeholderData[tg2].MinResource, res)
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
	assert.Assert(t, app.IsCompleting(), "Application should be in waiting state")

	// make sure the state changes back correctly
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	assert.Assert(t, app.IsRunning(), "Application should be in running state")

	// and back to waiting again, now from running
	assert.Equal(t, app.RemoveAllocationAsk(aKey), 0, "ask should have been removed, no reservations")
	assert.Assert(t, app.IsCompleting(), "Application should be in waiting state")

	log := app.GetStateLog()
	assert.Equal(t, len(log), 4, "wrong number of app events")
	assert.Equal(t, log[0].ApplicationState, Accepted.String())
	assert.Equal(t, log[1].ApplicationState, Completing.String())
	assert.Equal(t, log[2].ApplicationState, Running.String())
	assert.Equal(t, log[3].ApplicationState, Completing.String())
	assertUserGroupResource(t, getTestUserGroup(), nil)
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
	assertUserGroupResource(t, getTestUserGroup(), nil)

	ask = newAllocationAskRepeat("ask-2", appID1, res, 1)
	app.RecoverAllocationAsk(ask)
	assert.Equal(t, len(app.requests), 2, "ask should have been added, total should be 2")
	assert.Assert(t, app.IsAccepted(), "Application should have stayed in accepted state")
	assertUserGroupResource(t, getTestUserGroup(), nil)
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
	if app.HasReserved() || node.IsReserved() {
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
	if app.HasReserved() || node.IsReserved() {
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
	if app.HasReserved() || node.IsReserved() {
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

// test pending calculation and ask removal
func TestRemoveAllocAskWithPlaceholders(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskRepeat(aKey, appID1, res, 2)
	ask.placeholder = true
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask 1 should have been added to app")

	ask = newAllocationAskRepeat("alloc-2", appID1, res, 2)
	ask.placeholder = true
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask 2 should have been added to app")

	reservedAsks := app.RemoveAllocationAsk("alloc-1")
	assert.Equal(t, 0, reservedAsks)
	assert.Equal(t, Accepted.String(), app.stateMachine.Current())
	assertUserGroupResource(t, getTestUserGroup(), nil)
	reservedAsks = app.RemoveAllocationAsk("alloc-2")
	assert.Equal(t, 0, reservedAsks)
	assert.Equal(t, Completing.String(), app.stateMachine.Current())
	assertUserGroupResource(t, getTestUserGroup(), nil)
}

func TestRemovePlaceholderAllocationWithNoRealAllocation(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskRepeat(aKey, appID1, res, 2)
	ask.placeholder = true
	allocInfo := NewAllocation("uuid-1", nodeID1, ask)
	app.AddAllocation(allocInfo)
	err := app.handleApplicationEventWithLocking(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted")

	app.RemoveAllocation("uuid-1", si.TerminationType_UNKNOWN_TERMINATION_TYPE)
	assert.Equal(t, app.stateMachine.Current(), Completing.String())
	assertUserGroupResource(t, getTestUserGroup(), nil)
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
		app.requests[ask.GetAllocationKey()] = ask
	}
	app.sortRequests(true)
	if len(app.sortedRequests) != 3 {
		t.Fatalf("app sorted requests not correct: %v", app.sortedRequests)
	}
	allocKey := app.sortedRequests[0].GetAllocationKey()
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
	assert.Assert(t, app.IsCompleting(), "application did not change to waiting state: %s", app.CurrentState())

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
	assertUserGroupResource(t, getTestUserGroup(), res)

	// removing the ask should not move anywhere as there is an allocation
	released = app.RemoveAllocationAsk(askID)
	assert.Equal(t, released, 0, "allocation ask should not have been reserved")
	assert.Assert(t, app.IsStarting(), "Application should have stayed same, changed unexpectedly: %s", app.CurrentState())

	// remove the allocation, ask has been removed so nothing left
	app.RemoveAllocation(uuid, si.TerminationType_UNKNOWN_TERMINATION_TYPE)
	assert.Assert(t, app.IsCompleting(), "Application did not change as expected: %s", app.CurrentState())
	assertUserGroupResource(t, getTestUserGroup(), nil)

	log := app.GetStateLog()
	assert.Equal(t, len(log), 3, "wrong number of app events")
	assert.Equal(t, log[0].ApplicationState, Accepted.String())
	assert.Equal(t, log[1].ApplicationState, Starting.String())
	assert.Equal(t, log[2].ApplicationState, Completing.String())
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
	assert.Assert(t, app.IsCompleting(), "application did not change to waiting state: %s", app.CurrentState())

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
	assertUserGroupResource(t, getTestUserGroup(), res)

	// first we have to remove the allocation itself
	alloc := app.RemoveAllocation(uuid, si.TerminationType_UNKNOWN_TERMINATION_TYPE)
	assert.Assert(t, alloc != nil, "Nil allocation was returned")
	assert.Assert(t, app.IsAccepted(), "Application should have stayed in Accepted, changed unexpectedly: %s", app.CurrentState())
	// removing the ask should move the application into the waiting state, because the allocation is only a placeholder allocation
	released = app.RemoveAllocationAsk(askID)
	assert.Equal(t, released, 0, "allocation ask should not have been reserved")
	assert.Assert(t, app.IsCompleting(), "Application should have stayed same, changed unexpectedly: %s", app.CurrentState())
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0}))

	log := app.GetStateLog()
	assert.Equal(t, len(log), 2, "wrong number of app events")
	assert.Equal(t, log[0].ApplicationState, Accepted.String())
	assert.Equal(t, log[1].ApplicationState, Completing.String())
}

func TestAllocations(t *testing.T) {
	setupUGM()
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
	assert.Assert(t, app.getPlaceholderTimer() == nil, "Placeholder timer should not be initialized as the allocation is not a placeholder")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))

	// add more allocations to test the removals
	alloc = newAllocation(appID1, "uuid-2", nodeID1, "root.a", res)
	app.AddAllocation(alloc)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	alloc = newAllocation(appID1, "uuid-3", nodeID1, "root.a", res)
	app.AddAllocation(alloc)
	allocs = app.GetAllAllocations()
	assert.Equal(t, len(allocs), 3)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 3))
	// remove one of the 3
	if alloc = app.RemoveAllocation("uuid-2", si.TerminationType_UNKNOWN_TERMINATION_TYPE); alloc == nil {
		t.Error("returned allocations was nil allocation was not removed")
	}
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	// try to remove a non existing alloc
	if alloc = app.RemoveAllocation("does-not-exist", si.TerminationType_UNKNOWN_TERMINATION_TYPE); alloc != nil {
		t.Errorf("returned allocations was not allocation was incorrectly removed: %v", alloc)
	}
	// remove all left over allocations
	if allocs = app.RemoveAllAllocations(); allocs == nil || len(allocs) != 2 {
		t.Errorf("returned number of allocations was incorrect: %v", allocs)
	}
	allocs = app.GetAllAllocations()
	assert.Equal(t, len(allocs), 0)
	assertUserGroupResource(t, getTestUserGroup(), nil)
}

func TestGangAllocChange(t *testing.T) {
	resMap := map[string]string{"first": "4"}
	totalPH, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource with error")

	app := newApplication(appID1, "default", "root.a")
	app.placeholderAsk = totalPH
	assert.Assert(t, app.IsNew(), "newly created app should be in new state")
	assert.Assert(t, resources.IsZero(app.GetAllocatedResource()), "new application has allocated resources")
	assert.Assert(t, resources.IsZero(app.GetPlaceholderResource()), "new application has placeholder allocated resources")
	assert.Assert(t, resources.Equals(app.GetPlaceholderAsk(), totalPH), "placeholder ask resource not set as expected")
	assertUserGroupResource(t, getTestUserGroup(), nil)

	// move the app to the accepted state as if we added an ask
	app.SetState(Accepted.String())
	// create an allocation and check the assignment
	resMap = map[string]string{"first": "2"}
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource with error")
	alloc := newAllocation(appID1, "uuid-1", nodeID1, "root.a", res)
	alloc.placeholder = true
	app.AddAllocation(alloc)
	assert.Assert(t, resources.Equals(app.allocatedPlaceholder, res), "allocated placeholders resources is not updated correctly: %s", app.allocatedPlaceholder.String())
	assert.Equal(t, len(app.GetAllAllocations()), 1)
	assert.Assert(t, app.IsAccepted(), "app should still be in accepted state")
	assertUserGroupResource(t, getTestUserGroup(), res)

	// add second placeholder this should trigger state update
	alloc = newAllocation(appID1, "uuid-2", nodeID1, "root.a", res)
	alloc.placeholder = true
	app.AddAllocation(alloc)
	assert.Assert(t, resources.Equals(app.allocatedPlaceholder, totalPH), "allocated placeholders resources is not updated correctly: %s", app.allocatedPlaceholder.String())
	assert.Equal(t, len(app.GetAllAllocations()), 2)
	assert.Assert(t, app.IsStarting(), "app should have changed to starting state")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	// add a real alloc this should NOT trigger state update
	alloc = newAllocation(appID1, "uuid-3", nodeID1, "root.a", res)
	alloc.SetResult(Replaced)
	app.AddAllocation(alloc)
	assert.Equal(t, len(app.GetAllAllocations()), 3)
	assert.Assert(t, app.IsStarting(), "app should still be in starting state")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 3))

	// add a second real alloc this should trigger state update
	alloc = newAllocation(appID1, "uuid-4", nodeID1, "root.a", res)
	alloc.SetResult(Replaced)
	app.AddAllocation(alloc)
	assert.Equal(t, len(app.GetAllAllocations()), 4)
	assert.Assert(t, app.IsRunning(), "app should be in running state")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 4))
}

func TestAllocChange(t *testing.T) {
	setupUGM()
	app := newApplication(appID1, "default", "root.a")
	assert.Assert(t, app.IsNew(), "newly created app should be in new state")
	assert.Assert(t, resources.IsZero(app.GetAllocatedResource()), "new application has allocated resources")
	assertUserGroupResource(t, getTestUserGroup(), nil)

	// move the app to the accepted state as if we added an ask
	app.SetState(Accepted.String())
	// create an allocation and check the assignment
	resMap := map[string]string{"first": "2"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource with error")
	alloc := newAllocation(appID1, "uuid-1", nodeID1, "root.a", res)
	// adding a normal allocation should change the state
	app.AddAllocation(alloc)
	assert.Assert(t, resources.Equals(app.allocatedResource, res), "allocated resources is not updated correctly: %s", app.allocatedResource.String())
	assert.Equal(t, len(app.GetAllAllocations()), 1)
	assert.Assert(t, app.IsStarting(), "app should be in starting state")
	assertUserGroupResource(t, getTestUserGroup(), res)

	// add a second real alloc this should trigger state update
	alloc = newAllocation(appID1, "uuid-2", nodeID1, "root.a", res)
	app.AddAllocation(alloc)
	assert.Equal(t, len(app.GetAllAllocations()), 2)
	assert.Assert(t, app.IsRunning(), "app should have changed to running` state")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
}

func TestQueueUpdate(t *testing.T) {
	app := newApplication(appID1, "default", "root.a")

	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue")
	queue, err := createDynamicQueue(root, "test", false)
	assert.NilError(t, err, "failed to create test queue")
	app.SetQueue(queue)
	assert.Equal(t, app.GetQueuePath(), "root.test")
}

func TestStateTimeOut(t *testing.T) {
	startingTimeout = time.Microsecond * 100
	defer func() { startingTimeout = time.Minute * 5 }()
	app := newApplication(appID1, "default", "root.a")
	err := app.handleApplicationEventWithLocking(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (timeout test)")
	err = app.handleApplicationEventWithLocking(RunApplication)
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
	err = app.handleApplicationEventWithLocking(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (timeout test2)")
	err = app.handleApplicationEventWithLocking(RunApplication)
	assert.NilError(t, err, "no error expected accepted to starting (timeout test2)")
	// give it some time to run and progress
	time.Sleep(time.Microsecond * 100)
	if !app.IsStarting() || app.stateTimer == nil {
		t.Fatalf("Starting state and timer should not have timed out yet, state: %s", app.stateMachine.Current())
	}
	err = app.handleApplicationEventWithLocking(RunApplication)
	assert.NilError(t, err, "no error expected starting to run (timeout test2)")
	// give it some time to run and progress
	time.Sleep(time.Microsecond * 100)
	if !app.stateMachine.Is(Running.String()) || app.stateTimer != nil {
		t.Fatalf("State is not running or timer was not cleared, state: %s, timer %v", app.stateMachine.Current(), app.stateTimer)
	}

	startingTimeout = time.Minute * 5
	app = newApplicationWithTags(appID2, "default", "root.a", map[string]string{siCommon.AppTagStateAwareDisable: "true"})
	err = app.handleApplicationEventWithLocking(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (timeout test)")
	err = app.handleApplicationEventWithLocking(RunApplication)
	assert.NilError(t, err, "no error expected accepted to starting (timeout test)")
	// give it some time to run and progress
	time.Sleep(time.Millisecond * 100)
	if app.IsStarting() {
		t.Fatal("Starting state should have timed out")
	}
	if app.stateTimer != nil {
		t.Fatalf("Startup timer has not be cleared on time out as expected, %v", app.stateTimer)
	}
	log := app.GetStateLog()
	assert.Equal(t, len(log), 3, "wrong number of app events")
	assert.Equal(t, log[0].ApplicationState, Accepted.String())
	assert.Equal(t, log[1].ApplicationState, Starting.String())
	assert.Equal(t, log[2].ApplicationState, Running.String())
}

func TestCompleted(t *testing.T) {
	completingTimeout = time.Millisecond * 100
	terminatedTimeout = time.Millisecond * 100
	defer func() {
		completingTimeout = time.Second * 30
		terminatedTimeout = 3 * 24 * time.Hour
	}()
	app := newApplication(appID1, "default", "root.a")
	app.requests = map[string]*AllocationAsk{
		"test": {},
	}
	app.sortedRequests = append(app.sortedRequests, &AllocationAsk{})
	err := app.handleApplicationEventWithLocking(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completed test)")
	err = app.handleApplicationEventWithLocking(CompleteApplication)
	assert.NilError(t, err, "no error expected accepted to completing (completed test)")
	assert.Assert(t, app.IsCompleting(), "App should be waiting")
	// give it some time to run and progress
	err = common.WaitFor(10*time.Microsecond, time.Millisecond*200, app.IsCompleted)
	assert.NilError(t, err, "Application did not progress into Completed state")

	err = common.WaitFor(1*time.Millisecond, time.Millisecond*200, app.IsExpired)
	assert.NilError(t, err, "Application did not progress into Expired state")

	assert.Assert(t, app.sortedRequests == nil)
	assert.Equal(t, 0, len(app.requests))
	log := app.GetStateLog()
	assert.Equal(t, len(log), 4, "wrong number of app events")
	assert.Equal(t, log[0].ApplicationState, Accepted.String())
	assert.Equal(t, log[1].ApplicationState, Completing.String())
	assert.Equal(t, log[2].ApplicationState, Completed.String())
	assert.Equal(t, log[3].ApplicationState, Expired.String())
}

func TestRejected(t *testing.T) {
	terminatedTimeout = time.Millisecond * 100
	defer func() {
		terminatedTimeout = 3 * 24 * time.Hour
	}()
	app := newApplication(appID1, "default", "root.a")
	rejectedMessage := fmt.Sprintf("Failed to place application %s: application rejected: no placement rule matched", app.ApplicationID)
	err := app.handleApplicationEventWithInfoLocking(RejectApplication, rejectedMessage)
	assert.NilError(t, err, "no error expected new to rejected")

	err = common.WaitFor(1*time.Millisecond, time.Millisecond*200, app.IsRejected)
	assert.NilError(t, err, "Application did not progress into Rejected state")
	assert.Assert(t, !app.FinishedTime().IsZero())
	assert.Equal(t, app.rejectedMessage, rejectedMessage)
	assert.Equal(t, app.GetRejectedMessage(), rejectedMessage)

	err = common.WaitFor(1*time.Millisecond, time.Millisecond*200, app.IsExpired)
	assert.NilError(t, err, "Application did not progress into Expired state")

	log := app.GetStateLog()
	assert.Equal(t, len(log), 2, "wrong number of app events")
	assert.Equal(t, log[0].ApplicationState, Rejected.String())
	assert.Equal(t, log[1].ApplicationState, Expired.String())
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
	app, testHandler := newApplicationWithHandler(appID1, "default", "root.a")
	assert.Equal(t, New.String(), app.CurrentState(), "new app not in New state")

	err := app.handleApplicationEventWithLocking(RunApplication)
	assert.NilError(t, err, "error returned which was not expected")
	assert.Assert(t, testHandler.IsHandled(), "handler did not get called as expected")

	// accepted to rejected: error expected
	err = app.handleApplicationEventWithLocking(RejectApplication)
	assert.Assert(t, err != nil, "error expected and not seen")
	assert.Equal(t, app.CurrentState(), Accepted.String(), "application state has been changed unexpectedly")
	assert.Assert(t, !testHandler.IsHandled(), "unexpected event send to the RM")

	log := app.GetStateLog()
	assert.Equal(t, len(log), 1, "wrong number of app events")
	assert.Equal(t, log[0].ApplicationState, Accepted.String())
}

func TestReplaceAllocation(t *testing.T) {
	setupUGM()
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
	// add PlaceholderData
	app.addPlaceholderDataWithLocking(ph.GetAsk())
	assert.Equal(t, len(app.placeholderData), 1)
	assert.Equal(t, app.placeholderData[""].TaskGroupName, "")
	assert.Equal(t, app.placeholderData[""].Count, int64(1))
	assert.Equal(t, app.placeholderData[""].Replaced, int64(0))
	assert.Equal(t, app.placeholderData[""].TimedOut, int64(0))
	assert.DeepEqual(t, app.placeholderData[""].MinResource, res)

	assert.Equal(t, len(app.allocations), 1, "allocation not added as expected")
	assert.Assert(t, resources.IsZero(app.allocatedResource), "placeholder counted as real allocation")
	if !resources.Equals(app.allocatedPlaceholder, res) {
		t.Fatalf("placeholder allocation not updated as expected: got %s, expected %s", app.allocatedPlaceholder, res)
	}
	assertUserGroupResource(t, getTestUserGroup(), res)
	alloc = app.ReplaceAllocation("uuid-1")
	assert.Equal(t, alloc, nilAlloc, "placeholder without releases expected nil to be returned got a real alloc: %s", alloc)
	assert.Equal(t, app.placeholderData[""].Replaced, int64(0))
	assertUserGroupResource(t, getTestUserGroup(), nil)
	// add the placeholder back to the app, the failure test above changed state and removed the ph
	app.SetState(Running.String())
	app.AddAllocation(ph)
	app.addPlaceholderDataWithLocking(ph.GetAsk())
	assert.Equal(t, app.placeholderData[""].Count, int64(2))
	assertUserGroupResource(t, getTestUserGroup(), res)

	// set the real one to replace the placeholder
	realAlloc := newAllocation(appID1, "uuid-2", nodeID1, "root.a", res)
	realAlloc.SetResult(Replaced)
	ph.AddRelease(realAlloc)
	alloc = app.ReplaceAllocation("uuid-1")
	assert.Equal(t, alloc, ph, "returned allocation is not the placeholder")
	assert.Assert(t, resources.IsZero(app.allocatedPlaceholder), "real allocation counted as placeholder")
	if !resources.Equals(app.allocatedResource, res) {
		t.Fatalf("real allocation not updated as expected: got %s, expected %s", app.allocatedResource, res)
	}
	assert.Equal(t, app.placeholderData[""].Replaced, int64(1))
	assert.Equal(t, realAlloc.GetPlaceholderCreateTime(), ph.GetCreateTime(), "real allocation's placeholder create time not updated as expected: got %s, expected %s", realAlloc.GetPlaceholderCreateTime(), ph.GetCreateTime())
	assertUserGroupResource(t, getTestUserGroup(), res)

	// add the placeholder back to the app, the failure test above changed state and removed the ph
	app.SetState(Running.String())
	ph.ClearReleases()
	app.AddAllocation(ph)
	app.addPlaceholderDataWithLocking(ph.GetAsk())
	assert.Equal(t, app.placeholderData[""].Count, int64(3))
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	// set multiple real allocations to replace the placeholder
	realAlloc = newAllocation(appID1, "uuid-3", nodeID1, "root.a", res)
	realAlloc.SetResult(Replaced)
	ph.AddRelease(realAlloc)
	realAllocNoAdd := newAllocation(appID1, "not-added", nodeID1, "root.a", res)
	realAllocNoAdd.SetResult(Replaced)
	ph.AddRelease(realAlloc)
	alloc = app.ReplaceAllocation("uuid-1")
	assert.Equal(t, alloc, ph, "returned allocation is not the placeholder")
	assert.Assert(t, resources.IsZero(app.allocatedPlaceholder), "real allocation counted as placeholder")
	assert.Equal(t, app.placeholderData[""].Replaced, int64(2))
	// after the second replace we have 2 real allocations
	if !resources.Equals(app.allocatedResource, resources.Multiply(res, 2)) {
		t.Fatalf("real allocation not updated as expected: got %s, expected %s", app.allocatedResource, resources.Multiply(res, 2))
	}
	assert.Equal(t, realAlloc.GetPlaceholderCreateTime(), ph.GetCreateTime(), "real allocation's placeholder create time not updated as expected: got %s, expected %s", realAlloc.GetPlaceholderCreateTime(), ph.GetCreateTime())
	if _, ok := app.allocations["not-added"]; ok {
		t.Fatalf("real allocation added which shouldn't have been added")
	}
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
}

func TestTimeoutPlaceholderSoftStyle(t *testing.T) {
	runTimeoutPlaceholderTest(t, Resuming.String(), Soft)
}

func TestTimeoutPlaceholderAllocAsk(t *testing.T) {
	runTimeoutPlaceholderTest(t, Failing.String(), Hard)
}

func runTimeoutPlaceholderTest(t *testing.T, expectedState string, gangSchedulingStyle string) {
	setupUGM()
	// create a fake queue
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	originalPhTimeout := defaultPlaceholderTimeout
	defaultPlaceholderTimeout = 5 * time.Millisecond
	defer func() { defaultPlaceholderTimeout = originalPhTimeout }()

	app, testHandler := newApplicationWithHandler(appID1, "default", "root.a")
	app.gangSchedulingStyle = gangSchedulingStyle
	assert.Assert(t, app.getPlaceholderTimer() == nil, "Placeholder timer should be nil on create")
	// fake the queue assignment (needed with ask)
	app.queue = queue

	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "Unexpected error when creating resource from map")
	// add the placeholder ask to the app
	tg1 := "tg-1"
	phAsk := newAllocationAskTG("ask-1", appID1, tg1, res, 1)
	err = app.AddAllocationAsk(phAsk)
	assert.NilError(t, err, "Application ask should have been added")
	assert.Assert(t, app.IsAccepted(), "Application should be in accepted state")

	// check PlaceHolderData
	assert.Equal(t, len(app.placeholderData), 1)
	assert.Equal(t, app.placeholderData[tg1].TaskGroupName, tg1)
	assert.Equal(t, app.placeholderData[tg1].Count, int64(1))
	assert.Equal(t, app.placeholderData[tg1].Replaced, int64(0))
	assert.Equal(t, app.placeholderData[tg1].TimedOut, int64(0))
	assert.DeepEqual(t, app.placeholderData[tg1].MinResource, res)

	// add the placeholder to the app
	ph := newPlaceholderAlloc(appID1, "uuid-1", nodeID1, "root.a", res)
	app.AddAllocation(ph)
	assertUserGroupResource(t, getTestUserGroup(), res)
	assert.Assert(t, app.getPlaceholderTimer() != nil, "Placeholder timer should be initiated after the first placeholder allocation")
	// add a second one to check the filter
	ph = newPlaceholderAlloc(appID1, "uuid-2", nodeID1, "root.a", res)
	app.AddAllocation(ph)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	err = common.WaitFor(10*time.Millisecond, 1*time.Second, func() bool {
		app.RLock()
		defer app.RUnlock()
		return app.placeholderTimer == nil
	})
	assert.Equal(t, app.placeholderData[tg1].TimedOut, app.placeholderData[tg1].Count, "When the app is in an accepted state, timeout should equal to count")
	assert.NilError(t, err, "Placeholder timeout cleanup did not trigger unexpectedly")
	assert.Equal(t, app.stateMachine.Current(), expectedState, "Application did not progress into expected state")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	events := testHandler.GetEvents()
	var found int
	for _, event := range events {
		if allocRelease, ok := event.(*rmevent.RMReleaseAllocationEvent); ok {
			assert.Equal(t, len(allocRelease.ReleasedAllocations), 2, "two allocations should have been released")
			found++
		}
		if askRelease, ok := event.(*rmevent.RMReleaseAllocationAskEvent); ok {
			assert.Equal(t, len(askRelease.ReleasedAllocationAsks), 1, "one allocation ask should have been released")
			found++
		}
	}
	// check if the Replaced of PlaceHolderData is 0
	assert.Equal(t, app.placeholderData[tg1].Replaced, int64(0))
	// Because the Count of PlaceHolderData is only added in AddAllocationAsk, so it is 1
	assert.Equal(t, app.placeholderData[tg1].Count, int64(1))

	assert.Equal(t, found, 2, "release allocation or ask event not found in list")
	// asks are completely cleaned up
	assert.Assert(t, resources.IsZero(app.GetPendingResource()), "pending placeholder resources should be zero")
	// a released placeholder still holds the resource until release confirmed by the RM
	assert.Assert(t, resources.Equals(app.GetPlaceholderResource(), resources.Multiply(res, 2)), "Unexpected placeholder resources for the app")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	log := app.GetStateLog()
	assert.Equal(t, len(log), 2, "wrong number of app events")
	assert.Equal(t, log[0].ApplicationState, Accepted.String())
	assert.Equal(t, log[1].ApplicationState, expectedState)
}

func TestTimeoutPlaceholderAllocReleased(t *testing.T) {
	setupUGM()
	originalPhTimeout := defaultPlaceholderTimeout
	defaultPlaceholderTimeout = 5 * time.Millisecond
	defer func() { defaultPlaceholderTimeout = originalPhTimeout }()

	app, testHandler := newApplicationWithHandler(appID1, "default", "root.a")
	assert.Assert(t, app.getPlaceholderTimer() == nil, "Placeholder timer should be nil on create")
	app.SetState(Accepted.String())

	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "Unexpected error when creating resource from map")
	// add the placeholders to the app: one released, one still available.
	ph := newPlaceholderAlloc(appID1, "released", nodeID1, "root.a", res)
	ph.SetReleased(true)
	app.AddAllocation(ph)
	// add PlaceholderData
	app.addPlaceholderDataWithLocking(ph.GetAsk())
	assert.Equal(t, len(app.placeholderData), 1)
	assert.Equal(t, app.placeholderData[""].TaskGroupName, "")
	assert.Equal(t, app.placeholderData[""].Count, int64(1))
	assert.Equal(t, app.placeholderData[""].Replaced, int64(0))
	assert.Equal(t, app.placeholderData[""].TimedOut, int64(0))
	assert.DeepEqual(t, app.placeholderData[""].MinResource, res)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))

	assert.Assert(t, app.getPlaceholderTimer() != nil, "Placeholder timer should be initiated after the first placeholder allocation")
	ph = newPlaceholderAlloc(appID1, "waiting", nodeID1, "root.a", res)
	app.AddAllocation(ph)
	app.addPlaceholderDataWithLocking(ph.GetAsk())
	assert.Equal(t, app.placeholderData[""].Count, int64(2))
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	alloc := newAllocation(appID1, "real", nodeID1, "root.a", res)
	app.AddAllocation(alloc)
	assert.Assert(t, app.IsStarting(), "App should be in starting state after the first allocation")
	err = common.WaitFor(10*time.Millisecond, 1*time.Second, func() bool {
		return app.getPlaceholderTimer() == nil
	})
	assert.NilError(t, err, "Placeholder timeout cleanup did not trigger unexpectedly")
	assert.Assert(t, app.IsStarting(), "App should be in starting state after the first allocation")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 3))
	// two state updates and 1 release event
	events := testHandler.GetEvents()
	var found bool
	for _, event := range events {
		if allocRelease, ok := event.(*rmevent.RMReleaseAllocationEvent); ok {
			assert.Equal(t, len(allocRelease.ReleasedAllocations), 1, "one allocation should have been released")
			assert.Equal(t, allocRelease.ReleasedAllocations[0].UUID, "waiting", "wrong placeholder allocation released on timeout")
			found = true
		}
		if _, ok := event.(*rmevent.RMReleaseAllocationAskEvent); ok {
			t.Fatal("unexpected release allocation ask event found in list of events")
		}
	}
	assert.Assert(t, found, "release allocation event not found in list")
	assert.Assert(t, resources.Equals(app.GetAllocatedResource(), res), "Unexpected allocated resources for the app")
	// a released placeholder still holds the resource until release confirmed by the RM
	assert.Assert(t, resources.Equals(app.GetPlaceholderResource(), resources.Multiply(res, 2)), "Unexpected placeholder resources for the app")
	assert.Equal(t, app.placeholderData[""].Replaced, int64(0))
	assert.Equal(t, app.placeholderData[""].TimedOut, int64(1))
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 3))
}

func TestTimeoutPlaceholderCompleting(t *testing.T) {
	setupUGM()
	phTimeout := common.ConvertSITimeout(5)
	app, testHandler := newApplicationWithPlaceholderTimeout(appID1, "default", "root.a", 5)
	assert.Assert(t, app.getPlaceholderTimer() == nil, "Placeholder timer should be nil on create")
	assert.Equal(t, app.execTimeout, phTimeout, "placeholder timeout not initialised correctly")
	app.SetState(Accepted.String())

	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "Unexpected error when creating resource from map")
	// add the placeholder to the app
	ph := newPlaceholderAlloc(appID1, "waiting", nodeID1, "root.a", res)
	app.AddAllocation(ph)
	assert.Assert(t, app.getPlaceholderTimer() != nil, "Placeholder timer should be initiated after the first placeholder allocation")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))
	// add a real allocation as well
	alloc := newAllocation(appID1, "uuid-1", nodeID1, "root.a", res)
	app.AddAllocation(alloc)
	// move on to running
	app.SetState(Running.String())
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	// remove allocation to trigger state change
	app.RemoveAllocation("uuid-1", si.TerminationType_UNKNOWN_TERMINATION_TYPE)
	assert.Assert(t, app.IsCompleting(), "App should be in completing state all allocs have been removed")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))
	// make sure the placeholders time out
	err = common.WaitFor(10*time.Millisecond, 1*time.Second, func() bool {
		return app.getPlaceholderTimer() == nil
	})
	assert.NilError(t, err, "Placeholder timer did not time out as expected")
	events := testHandler.GetEvents()
	var found bool
	for _, event := range events {
		if allocRelease, ok := event.(*rmevent.RMReleaseAllocationEvent); ok {
			assert.Equal(t, len(allocRelease.ReleasedAllocations), 1, "one allocation should have been released")
			found = true
		}
		if _, ok := event.(*rmevent.RMReleaseAllocationAskEvent); ok {
			t.Fatal("unexpected release allocation ask event found in list of events")
		}
	}
	assert.Assert(t, found, "release allocation event not found in list")
	assert.Assert(t, app.IsCompleting(), "App should still be in completing state")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))
}

func TestAppTimersAfterAppRemoval(t *testing.T) {
	setupUGM()
	phTimeout := common.ConvertSITimeout(50)
	app, _ := newApplicationWithPlaceholderTimeout(appID1, "default", "root.a", 50)
	assert.Assert(t, app.getPlaceholderTimer() == nil, "Placeholder timer should be nil on create")
	assert.Equal(t, app.execTimeout, phTimeout, "placeholder timeout not initialised correctly")
	app.SetState(Accepted.String())

	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "Unexpected error when creating resource from map")
	// add the placeholder to the app
	ph := newPlaceholderAlloc(appID1, "waiting", nodeID1, "root.a", res)
	app.AddAllocation(ph)
	assert.Assert(t, app.getPlaceholderTimer() != nil, "Placeholder timer should be initiated after the first placeholder allocation")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))
	// add a real allocation as well
	alloc := newAllocation(appID1, "uuid-1", nodeID1, "root.a", res)
	app.AddAllocation(alloc)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	// move on to running
	app.SetState(Running.String())
	app.RemoveAllAllocations()
	assert.Assert(t, app.IsCompleting(), "App should be in completing state all allocs have been removed")
	assertUserGroupResource(t, getTestUserGroup(), nil)
	if app.getPlaceholderTimer() != nil {
		t.Fatalf("Placeholder timer has not be cleared after app removal as expected, %v", app.getPlaceholderTimer())
	}
	if app.stateTimer != nil {
		t.Fatalf("State timer has not be cleared after app removal as expected, %v", app.stateTimer)
	}
}

func TestIncAndDecUserResourceUsage(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue
	assertUserGroupResource(t, getTestUserGroup(), nil)
	app.incUserResourceUsage(nil)
	assertUserGroupResource(t, getTestUserGroup(), nil)
	app.incUserResourceUsage(res)
	assertUserGroupResource(t, getTestUserGroup(), res)
	app.incUserResourceUsage(res)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	app.decUserResourceUsage(nil, false)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	app.decUserResourceUsage(res, false)
	assertUserGroupResource(t, getTestUserGroup(), res)
	app.decUserResourceUsage(res, false)
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0}))
}

func TestIncAndDecUserResourceUsageInSameGroup(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	testgroups := []string{"testgroup"}
	app := newApplicationWithUserGroup(appID1, "default", "root.unknown", "testuser", testgroups)
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	app2 := newApplicationWithUserGroup(appID2, "default", "root.unknown", "testuser2", testgroups)
	if app2 == nil || app2.ApplicationID != appID2 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue
	app2.queue = queue

	// Increase testuser
	app.incUserResourceUsage(res)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser", testgroups), res, res, 0)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser2", testgroups), nil, res, 0)

	// Increase both testuser and testuser2 with the same group testgroup
	app.incUserResourceUsage(res)
	app2.incUserResourceUsage(resources.Multiply(res, 2))
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser", testgroups), resources.Multiply(res, 2), resources.Multiply(res, 4), 0)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser2", testgroups), resources.Multiply(res, 2), resources.Multiply(res, 4), 0)

	// Increase nil
	app.decUserResourceUsage(nil, false)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser", testgroups), resources.Multiply(res, 2), resources.Multiply(res, 4), 0)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser2", testgroups), resources.Multiply(res, 2), resources.Multiply(res, 4), 0)

	// Decrease testuser
	app.decUserResourceUsage(res, false)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser", testgroups), res, resources.Multiply(res, 3), 0)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser2", testgroups), resources.Multiply(res, 2), resources.Multiply(res, 3), 0)

	// Decrease testuser2
	app2.decUserResourceUsage(res, false)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser", testgroups), res, resources.Multiply(res, 2), 0)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser2", testgroups), res, resources.Multiply(res, 2), 0)

	// Decrease testuser and testuser2 to 0
	app.decUserResourceUsage(res, false)
	app2.decUserResourceUsage(res, false)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser", testgroups), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0}), 0)
}

func TestGetAllRequests(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAsk(aKey, appID1, res)
	app := newApplication(appID1, "default", "root.unknown")
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue
	assert.Assert(t, len(app.getAllRequestsInternal()) == 0, "App should have no requests yet")
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "No error expected when adding an ask")
	assert.Assert(t, len(app.getAllRequestsInternal()) == 1, "App should have only one request")
	assert.Equal(t, app.getAllRequestsInternal()[0], ask, "Unexpected request found in the app")
}

func TestGetQueueNameAfterUnsetQueue(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	assert.Equal(t, app.GetQueuePath(), "root.unknown")

	// the queue is reset to nil but GetQueuePath should work well
	app.UnSetQueue()
	assert.Assert(t, app.queue == nil)
	assert.Equal(t, app.GetQueuePath(), "root.unknown")
}

func TestFinishedTime(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	assert.Assert(t, app.finishedTime.IsZero())
	assert.Assert(t, app.FinishedTime().IsZero())

	// sleep 1 second to make finished time bigger than zero
	time.Sleep(1 * time.Second)
	app.UnSetQueue()
	assert.Assert(t, !app.finishedTime.IsZero())
	assert.Assert(t, !app.FinishedTime().IsZero())

	// when app is rejected, finishedTime is rejectedTime
	app1 := newApplication(appID1, "default", "root.unknown")
	app1.SetState(Rejected.String())
	assert.Assert(t, !app.finishedTime.IsZero())
	assert.Assert(t, !app.FinishedTime().IsZero())
}

func TestCanReplace(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "Unexpected error when creating resource from map")

	tg1 := "available"
	tests := []struct {
		name string
		ask  *AllocationAsk
		want bool
	}{
		{"nil", nil, false},
		{"placeholder", newAllocationAskTG(aKey, appID1, tg1, res, 1), false},
		{"no TG", newAllocationAsk(aKey, appID1, res), false},
		{"no placeholder data", newAllocationAskAll(aKey, appID1, tg1, res, 1, false, 0), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, app.canReplace(tt.ask), "unexpected replacement")
		})
	}
	// add the placeholder data
	// available tg has one replacement open
	app.addPlaceholderDataWithLocking(newAllocationAskTG(aKey, appID1, tg1, res, 1))
	// unavailable tg has NO replacement open (replaced)
	tg2 := "unavailable"
	app.addPlaceholderDataWithLocking(newAllocationAskTG(aKey, appID1, tg2, res, 1))
	app.placeholderData[tg2].Replaced++
	// unavailable tg has NO replacement open (timedout)
	tg3 := "timedout"
	app.addPlaceholderDataWithLocking(newAllocationAskTG(aKey, appID1, tg3, res, 1))
	app.placeholderData[tg3].TimedOut++
	tests = []struct {
		name string
		ask  *AllocationAsk
		want bool
	}{
		{"no TG", newAllocationAsk(aKey, appID1, res), false},
		{"TG mismatch", newAllocationAskAll(aKey, appID1, "unknown", res, 1, false, 0), false},
		{"TG placeholder used", newAllocationAskAll(aKey, appID1, tg2, res, 1, false, 0), false},
		{"TG placeholder timed out", newAllocationAskAll(aKey, appID1, tg3, res, 1, false, 0), false},
		{"TG placeholder available", newAllocationAskAll(aKey, appID1, tg1, res, 1, false, 0), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, app.canReplace(tt.ask), "unexpected replacement")
		})
	}
}

func TestTryAllocateNoRequests(t *testing.T) {
	node := newNode("node1", map[string]resources.Quantity{"first": 5})
	nodes := []*Node{node}
	nodeMap := map[string]*Node{"node1": node}
	iterator := func() NodeIterator { return NewDefaultNodeIterator(nodes) }
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}

	app := newApplication(appID1, "default", "root.unknown")
	preemptionAttemptsRemaining := 0
	alloc := app.tryAllocate(node.GetAvailableResource(), 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Check(t, alloc == nil, "unexpected alloc")
}

func TestTryAllocateFit(t *testing.T) {
	node := newNode("node1", map[string]resources.Quantity{"first": 5})
	nodes := []*Node{node}
	nodeMap := map[string]*Node{"node1": node}
	iterator := func() NodeIterator { return NewDefaultNodeIterator(nodes) }
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}

	rootQ, err := createRootQueue(map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ, err := createManagedQueue(rootQ, "child", false, map[string]string{"first": "5"})
	assert.NilError(t, err)

	app := newApplication(appID1, "default", "root.child")
	app.SetQueue(childQ)
	childQ.applications[appID1] = app
	ask := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err)

	preemptionAttemptsRemaining := 0
	alloc := app.tryAllocate(node.GetAvailableResource(), 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, alloc != nil, "alloc expected")
	assert.Equal(t, "node1", alloc.GetNodeID(), "wrong node")
}

func TestTryAllocatePreemptQueue(t *testing.T) {
	node := newNode("node1", map[string]resources.Quantity{"first": 20})
	nodes := []*Node{node}
	nodeMap := map[string]*Node{"node1": node}
	iterator := func() NodeIterator { return NewDefaultNodeIterator(nodes) }
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}

	rootQ, err := createRootQueue(map[string]string{"first": "20"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "20"}, map[string]string{"first": "10"})
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)

	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	err = app1.AddAllocationAsk(ask1)
	assert.NilError(t, err)
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	err = app1.AddAllocationAsk(ask2)
	assert.NilError(t, err)

	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask3.allowPreemptOther = true
	err = app2.AddAllocationAsk(ask3)
	assert.NilError(t, err)

	preemptionAttemptsRemaining := 10

	alloc1 := app1.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}), 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, alloc1 != nil, "alloc1 expected")
	alloc2 := app1.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}), 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, alloc2 != nil, "alloc2 expected")

	// on first attempt, not enough time has passed
	alloc3 := app2.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0}), 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, alloc3 == nil, "alloc3 not expected")
	assert.Assert(t, !alloc2.IsPreempted(), "alloc2 should not have been preempted")

	// pass the time and try again
	ask3.createTime = ask3.createTime.Add(-30 * time.Second)
	alloc3 = app2.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0}), 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, alloc3 == nil, "alloc3 not expected")
	assert.Assert(t, alloc2.IsPreempted(), "alloc2 should have been preempted")
}

func TestTryAllocatePreemptNode(t *testing.T) {
	node1 := newNode("node1", map[string]resources.Quantity{"first": 20})
	node2 := newNode("node2", map[string]resources.Quantity{"first": 20})
	nodes := []*Node{node1, node2}
	nodeMap := map[string]*Node{"node1": node1, "node2": node2}
	iterator := func() NodeIterator { return NewDefaultNodeIterator(nodes) }
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}

	rootQ, err := createRootQueue(map[string]string{"first": "40"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "20"}, map[string]string{"first": "10"})
	assert.NilError(t, err)
	unlimitedQ, err := createManagedQueueGuaranteed(rootQ, "unlimited", false, nil, nil)
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)

	app0 := newApplication(appID0, "default", "root.unlimited")
	app0.SetQueue(unlimitedQ)
	unlimitedQ.applications[appID0] = app0
	ask00 := newAllocationAsk("alloc0-0", appID0, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11}))
	err = app0.AddAllocationAsk(ask00)
	assert.NilError(t, err)
	ask01 := newAllocationAsk("alloc0-1", appID0, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11}))
	err = app0.AddAllocationAsk(ask01)
	assert.NilError(t, err)

	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	err = app1.AddAllocationAsk(ask1)
	assert.NilError(t, err)
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	err = app1.AddAllocationAsk(ask2)
	assert.NilError(t, err)

	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask3.allowPreemptOther = true
	err = app2.AddAllocationAsk(ask3)
	assert.NilError(t, err)

	preemptionAttemptsRemaining := 10

	// consume capacity with 'unlimited' app
	alloc00 := app0.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 40}), 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, alloc00 != nil, "alloc00 expected")
	alloc01 := app0.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 39}), 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, alloc01 != nil, "alloc01 expected")

	// consume remainder of space but not quota
	alloc1 := app1.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 28}), 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, alloc1 != nil, "alloc1 expected")
	alloc2 := app1.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 23}), 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, alloc2 != nil, "alloc2 expected")

	// on first attempt, should see a reservation since we're after the reservation timeout
	ask3.createTime = ask3.createTime.Add(-10 * time.Second)
	alloc3 := app2.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 18}), 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, alloc3 != nil, "alloc3 expected")
	assert.Equal(t, "node1", alloc3.GetNodeID(), "wrong node assignment")
	assert.Equal(t, Reserved, alloc3.GetResult(), "expected reservation")
	assert.Assert(t, !alloc2.IsPreempted(), "alloc2 should not have been preempted")
	err = node1.Reserve(app2, ask3)
	assert.NilError(t, err)

	// pass the time and try again
	ask3.createTime = ask3.createTime.Add(-30 * time.Second)
	alloc3 = app2.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 18}), 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, alloc3 != nil, "alloc3 expected")
	assert.Assert(t, alloc1.IsPreempted(), "alloc1 should have been preempted")
}

func TestMaxAskPriority(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})

	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	// initial state
	assert.Equal(t, app.GetAskMaxPriority(), configs.MinPriority, "wrong default priority")

	// p=10 added
	ask1 := newAllocationAskPriority("prio-10", appID1, res, 10)
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err, "ask should have been updated on app")
	assert.Equal(t, app.GetAskMaxPriority(), int32(10), "wrong priority after adding p=10")

	// p=5 added
	ask2 := newAllocationAskPriority("prio-5", appID1, res, 5)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "ask should have been added to app")
	assert.Equal(t, app.GetAskMaxPriority(), int32(10), "wrong priority after adding p=5")

	// p=15 added
	ask3 := newAllocationAskPriority("prio-15", appID1, res, 15)
	err = app.AddAllocationAsk(ask3)
	assert.NilError(t, err, "ask should have been added to app")
	assert.Equal(t, app.GetAskMaxPriority(), int32(15), "wrong priority after adding p=15")

	// p=10 removed
	app.RemoveAllocationAsk(ask1.GetAllocationKey())
	assert.Equal(t, app.GetAskMaxPriority(), int32(15), "wrong priority after removing p=10")

	// p=15 removed
	app.RemoveAllocationAsk(ask3.GetAllocationKey())
	assert.Equal(t, app.GetAskMaxPriority(), int32(5), "wrong priority after removing p=15")

	// re-add removed asks
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err, "ask should have been added to app")
	err = app.AddAllocationAsk(ask3)
	assert.NilError(t, err, "ask should have been added to app")

	assert.Equal(t, app.GetAskMaxPriority(), int32(15), "wrong priority after re-adding asks")

	// update repeat to zero for p=15
	_, err = app.UpdateAskRepeat(ask3.GetAllocationKey(), -1)
	assert.NilError(t, err, "ask should have been updated")
	assert.Equal(t, app.GetAskMaxPriority(), int32(10), "wrong priority after updating p=15 repeat to 0")

	// update repeat to zero for p=5
	_, err = app.UpdateAskRepeat(ask2.GetAllocationKey(), -1)
	assert.NilError(t, err, "ask should have been updated")
	assert.Equal(t, app.GetAskMaxPriority(), int32(10), "wrong priority after updating p=5 repeat to 0")

	// update repeat to 1 for p=5
	_, err = app.UpdateAskRepeat(ask2.GetAllocationKey(), 1)
	assert.NilError(t, err, "ask should have been updated")
	assert.Equal(t, app.GetAskMaxPriority(), int32(10), "wrong priority after updating p=5 repeat to 1")

	// update repeat to 1 for p=15
	_, err = app.UpdateAskRepeat(ask3.GetAllocationKey(), 1)
	assert.NilError(t, err, "ask should have been updated")
	assert.Equal(t, app.GetAskMaxPriority(), int32(15), "wrong priority after updating p=15 repeat to 1")
}

func (sa *Application) addPlaceholderDataWithLocking(ask *AllocationAsk) {
	sa.Lock()
	defer sa.Unlock()
	sa.addPlaceholderData(ask)
}

func (sa *Application) getPlaceholderTimer() *time.Timer {
	sa.RLock()
	defer sa.RUnlock()
	return sa.placeholderTimer
}

func (sa *Application) handleApplicationEventWithLocking(event applicationEvent) error {
	sa.Lock()
	defer sa.Unlock()
	return sa.HandleApplicationEvent(event)
}

func (sa *Application) handleApplicationEventWithInfoLocking(event applicationEvent, info string) error {
	sa.Lock()
	defer sa.Unlock()
	return sa.HandleApplicationEventWithInfo(event, info)
}
