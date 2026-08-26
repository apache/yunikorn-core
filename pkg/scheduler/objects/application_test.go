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
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-core/pkg/handler"
	mockCommon "github.com/apache/yunikorn-core/pkg/mock"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-core/pkg/rmproxy"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	schedEvt "github.com/apache/yunikorn-core/pkg/scheduler/objects/events"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

var (
	nilNodeIterator = func() NodeIterator {
		return nil
	}
	nilGetNode = func(string) *Node {
		return nil
	}
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
	assert.Equal(t, app.backoffDeadline, time.Time{})
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

func TestNewApplicationWithAnnotationUpdate(t *testing.T) {
	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{},
	}
	// valid tags
	siApp := &si.AddApplicationRequest{}
	siApp.Tags = map[string]string{
		siCommon.AppTagNamespaceResourceQuota:      "{\"resources\":{\"validMaxRes\":{\"value\":11}}}",
		siCommon.AppTagNamespaceResourceGuaranteed: "{\"resources\":{\"validGuaranteed\":{\"value\":22}}}",
		siCommon.AppTagNamespaceResourceMaxApps:    "33",
	}

	app := NewApplication(siApp, user, nil, "")

	guaranteed := app.GetGuaranteedResource()
	maxResource := app.GetMaxResource()
	maxApps := app.GetMaxApps()
	assert.Assert(t, guaranteed != nil, "guaranteed resource has not been set")
	assert.Equal(t, 1, len(guaranteed.Resources), "more than one resource has been set")
	assert.Equal(t, resources.Quantity(22), guaranteed.Resources["validGuaranteed"])
	assert.Assert(t, maxResource != nil, "maximum resource has not been set")
	assert.Equal(t, 1, len(maxResource.Resources), "more than one resource has been set")
	assert.Equal(t, resources.Quantity(11), maxResource.Resources["validMaxRes"], "maximum resource is incorrect")
	assert.Assert(t, maxApps != 0, "maximum apps has not been set or incorrect")
	assert.Equal(t, uint64(33), maxApps, "maximum apps is incorrect")

	// valid tags without max apps
	siApp = &si.AddApplicationRequest{}
	siApp.Tags = map[string]string{
		siCommon.AppTagNamespaceResourceQuota:      "{\"resources\":{\"validMaxRes\":{\"value\":11}}}",
		siCommon.AppTagNamespaceResourceGuaranteed: "{\"resources\":{\"validGuaranteed\":{\"value\":22}}}",
	}
	app = NewApplication(siApp, user, nil, "")
	guaranteed = app.GetGuaranteedResource()
	maxResource = app.GetMaxResource()
	maxApps = app.GetMaxApps()
	assert.Assert(t, guaranteed != nil, "guaranteed resource has not been set")
	assert.Equal(t, 1, len(guaranteed.Resources), "more than one resource has been set")
	assert.Equal(t, resources.Quantity(22), guaranteed.Resources["validGuaranteed"])
	assert.Assert(t, maxResource != nil, "maximum resource has not been set")
	assert.Equal(t, 1, len(maxResource.Resources), "more than one resource has been set")
	assert.Equal(t, resources.Quantity(11), maxResource.Resources["validMaxRes"], "maximum resource is incorrect")
	assert.Assert(t, maxApps == 0, "maximum apps should have not been set")

	// invalid tags
	siApp = &si.AddApplicationRequest{}
	siApp.Tags = map[string]string{
		siCommon.AppTagNamespaceResourceQuota:      "{xxxxxx}",
		siCommon.AppTagNamespaceResourceGuaranteed: "{yyyyy}",
		siCommon.AppTagNamespaceResourceMaxApps:    "zzzzz",
	}
	app = NewApplication(siApp, user, nil, "")
	guaranteed = app.GetGuaranteedResource()
	maxResource = app.GetMaxResource()
	maxApps = app.GetMaxApps()
	assert.Assert(t, guaranteed == nil, "guaranteed resource should have not been set")
	assert.Assert(t, maxResource == nil, "maximum resource should have not been set")
	assert.Assert(t, maxApps == 0, "maximum apps should have not been set or incorrect")

	// negative values
	siApp = &si.AddApplicationRequest{}
	siApp.Tags = map[string]string{
		siCommon.AppTagNamespaceResourceQuota:      "{\"resources\":{\"negativeMax\":{\"value\":-11}}}",
		siCommon.AppTagNamespaceResourceGuaranteed: "{\"resources\":{\"negativeGuaranteed\":{\"value\":-22}}}",
		siCommon.AppTagNamespaceResourceMaxApps:    "-33",
	}
	app = NewApplication(siApp, user, nil, "")
	guaranteed = app.GetGuaranteedResource()
	maxResource = app.GetMaxResource()
	maxApps = app.GetMaxApps()
	assert.Assert(t, guaranteed == nil, "guaranteed resource should have not been set")
	assert.Assert(t, maxResource == nil, "maximum resource should have not been set")
	assert.Assert(t, maxApps == 0, "maximum apps should have not been set or incorrect")

	// valid max apps
	siApp = &si.AddApplicationRequest{}
	siApp.Tags = map[string]string{
		siCommon.AppTagNamespaceResourceMaxApps: "33",
	}
	app = NewApplication(siApp, user, nil, "")
	maxApps = app.GetMaxApps()
	guaranteed = app.GetGuaranteedResource()
	maxResource = app.GetMaxResource()
	assert.Assert(t, guaranteed == nil, "guaranteed resource should have not been set")
	assert.Assert(t, maxResource == nil, "maximum resource should have not been set")
	assert.Assert(t, maxApps != 0, "maximum apps has not been set or incorrect")
	assert.Equal(t, uint64(33), maxApps, "maximum apps is incorrect")

	// invalid max apps
	siApp = &si.AddApplicationRequest{}
	siApp.Tags = map[string]string{
		siCommon.AppTagNamespaceResourceMaxApps: "zzzzz",
	}
	app = NewApplication(siApp, user, nil, "")
	maxApps = app.GetMaxApps()
	maxResource = app.GetMaxResource()
	assert.Assert(t, guaranteed == nil, "guaranteed resource should have not been set")
	assert.Assert(t, maxResource == nil, "maximum resource should have not been set")
	assert.Assert(t, maxApps == 0, "maximum apps should have not been set or incorrect")

	// negative max apps
	siApp = &si.AddApplicationRequest{}
	siApp.Tags = map[string]string{
		siCommon.AppTagNamespaceResourceMaxApps: "-33",
	}
	app = NewApplication(siApp, user, nil, "")
	maxApps = app.GetMaxApps()
	maxResource = app.GetMaxResource()
	assert.Assert(t, guaranteed == nil, "guaranteed resource should have not been set")
	assert.Assert(t, maxResource == nil, "maximum resource should have not been set")
	assert.Assert(t, maxApps == 0, "maximum apps should have not been set or incorrect")
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
	if app.NodeReservedForAsk("") != "" {
		t.Error("app should not have reservations for empty ask")
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
	if app.NodeReservedForAsk("") != "" {
		t.Error("app should not have reservations for empty ask")
	}
	if app.HasReserved() && app.NodeReservedForAsk(aKey) != nodeID1 {
		t.Errorf("app should have reservations for node %s", nodeID1)
	}

	// reserve the same reservation
	err = app.Reserve(node, ask)
	if err == nil {
		t.Errorf("reservation should have failed")
	}

	// unreserve unknown node/alloc
	assert.Equal(t, app.UnReserve(nil, nil), 0, "illegal reservation release should have returned 0")

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
	assert.Equal(t, app.UnReserve(node2, ask2), 1, "remove of reservation of 2nd node should not have failed")

	// unreserve the same should fail
	assert.Equal(t, app.UnReserve(node2, ask2), 0, "remove twice of reservation of 2nd node should return 0")

	// failure case: remove reservation from node, app still needs cleanup
	var num int
	num = node.unReserve(ask)
	assert.Equal(t, num, 1, "un-reserve on node should have removed reservation")
	num = app.UnReserve(node, ask)
	assert.Equal(t, num, 1, "un-reserve on app should have removed reservation from app")
}

// test multiple reservations from one allocation
func TestAppAllocReservation(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	// Create event system after new application to avoid new application event.
	events.Init()
	eventSystem := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	eventSystem.StartServiceWithPublisher(false)
	app.disableStateChangeEvents()
	app.resetAppEvents()
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	assert.Assert(t, !app.HasReserved(), "new app should not have reservations")
	assert.Equal(t, len(app.GetReservations()), 0, "new app should not have reservation for empty allocKey")

	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	// reserve 1 allocate ask
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAsk(aKey, appID1, res)
	ask2 := newAllocationAsk(aKey2, appID1, res)
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 10})
	// reserve that works
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "ask2 should have been added to app")
	err = app.Reserve(node1, ask)
	assert.NilError(t, err, "reservation should not have failed")
	if app.reservations[""] != nil {
		t.Fatal("app should not have reservation for empty allocKey")
	}
	allocReserved := app.reservations[aKey]
	if allocReserved == nil || allocReserved.nodeID != nodeID1 {
		t.Fatalf("app should have reservations for %s on %s and has not", aKey, nodeID1)
	}
	assert.Equal(t, len(app.GetReservations()), 1, "app should have 1 reservation")

	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 10})
	err = app.Reserve(node2, ask2)
	assert.NilError(t, err, "reservation should not have failed: error %v", err)
	allocReserved = app.reservations[aKey2]
	if allocReserved == nil || allocReserved.nodeID != nodeID2 {
		t.Fatalf("app should have reservations for %s on %s and has not", aKey, nodeID2)
	}
	assert.Equal(t, len(app.GetReservations()), 2, "app should have 2 reservation")

	// check duplicate reserve: nothing should change
	assert.Assert(t, app.canAllocationReserve(ask) != nil, "alloc has already reserved, reserve check should have failed")
	node3 := newNode("node-3", map[string]resources.Quantity{"first": 10})
	err = app.Reserve(node3, ask)
	if err == nil {
		t.Fatal("reservation should have failed")
	}
	allocReserved = app.reservations[aKey]
	if allocReserved == nil || allocReserved.nodeID != nodeID1 {
		t.Fatalf("app should have reservations for node %s and has not: %v", nodeID1, allocReserved)
	}
	allocReserved = app.reservations[aKey2]
	if allocReserved == nil || allocReserved.nodeID != nodeID2 {
		t.Fatalf("app should have reservations for node %s and has not: %v", nodeID2, allocReserved)
	}
	assert.Equal(t, len(app.GetReservations()), 2, "app should have 2 reservation")
	// clean up all asks and reservations
	reservedRelease := app.RemoveAllocationAsk("")
	if app.HasReserved() || node1.IsReserved() || node2.IsReserved() || reservedRelease != 2 {
		t.Fatalf("ask removal did not clean up all reservations, reserved released = %d", reservedRelease)
	}
}

func TestAllocateDeallocate(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	if app == nil || app.ApplicationID != appID1 {
		t.Fatalf("app create failed which should not have %v", app)
	}
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	// failure cases
	var delta *resources.Resource
	if delta, err = app.AllocateAsk(""); err == nil || delta != nil {
		t.Error("empty ask key should not have been found by AllocateAsk()")
	}
	if delta, err = app.AllocateAsk("unknown"); err == nil || delta != nil {
		t.Error("unknown ask key should not have been found by AllocateAsk()")
	}
	if delta, err = app.DeallocateAsk(""); err == nil || delta != nil {
		t.Error("empty ask key should not have been found by DeallocateAsk()")
	}
	if delta, err = app.DeallocateAsk("unknown"); err == nil || delta != nil {
		t.Error("unknown ask key should not have been found by DeallocateAsk()")
	}

	// working cases
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAsk(aKey, appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	// allocate
	if delta, err := app.AllocateAsk(aKey); err != nil || !resources.Equals(res, delta) {
		t.Errorf("AllocateAsk() did not return correct delta, err %v, expected %v got %v", err, res, delta)
	}
	// allocate again should fail
	if delta, err := app.AllocateAsk(aKey); err == nil || delta != nil {
		t.Error("attempt to call Allocate() twice should have failed")
	}
	// deallocate
	if delta, err := app.DeallocateAsk(aKey); err != nil || !resources.Equals(res, delta) {
		t.Errorf("DeallocateAsk() did not return correct delta, err %v, expected %v got %v", err, res, delta)
	}
	// deallocate again should fail
	if delta, err := app.DeallocateAsk(aKey); err == nil || delta != nil {
		t.Error("attempt to call Deallocate() twice should have failed")
	}
}

// test pending calculation and ask addition
//
//nolint:funlen
func TestAddAllocAsk(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	// Create event system after new application to avoid new application event.
	events.Init()
	eventSystem := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	eventSystem.StartServiceWithPublisher(false)
	app.disableStateChangeEvents()
	app.resetAppEvents()
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

	// add alloc ask
	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask = newAllocationAsk(aKey, appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been updated on app")
	assert.Assert(t, app.IsAccepted(), "Application should be in accepted state")
	pending := app.GetPendingResource()
	if !resources.Equals(res, pending) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", res, pending)
	}

	// test add alloc ask event
	noEvents := uint64(0)
	err = common.WaitForCondition(10*time.Millisecond, time.Second, func() bool {
		noEvents = eventSystem.Store.CountStoredEvents()
		return noEvents == 2
	})
	assert.NilError(t, err, "expected 2 events, got %d", noEvents)
	records := eventSystem.Store.CollectEvents()
	if records == nil {
		t.Fatal("collecting eventChannel should return something")
	}
	assert.Equal(t, 2, len(records))
	record := records[1]
	assert.Equal(t, si.EventRecord_APP, record.Type, "incorrect event type, expect app")
	assert.Equal(t, appID1, record.ObjectID, "incorrect object ID, expected application ID")
	assert.Equal(t, aKey, record.ReferenceID, "incorrect reference ID, expected placeholder alloc ID")
	assert.Equal(t, si.EventRecord_ADD, record.EventChangeType, "incorrect change type, expected add")
	assert.Equal(t, si.EventRecord_APP_REQUEST, record.EventChangeDetail, "incorrect change detail, expected app request")
	eventSystem.Stop()

	// change resource
	ask = newAllocationAsk(aKey, appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been updated on app")
	pending = app.GetPendingResource()
	if !resources.Equals(resources.Multiply(res, 2), app.GetPendingResource()) {
		t.Errorf("pending resource not updated correctly, expected %v but was: %v", resources.Multiply(res, 2), pending)
	}

	// after all this is must still be in an accepted state
	assert.Assert(t, app.IsAccepted(), "Application should have stayed in accepted state")

	// test PlaceholderData
	ask = newAllocationAskTG(aKey, appID1, tg1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been updated on app")
	app.SetTimedOutPlaceholder(tg1, 1)
	app.SetTimedOutPlaceholder(tg2, 2)
	clonePlaceholderData := app.GetAllPlaceholderData()
	assert.Equal(t, len(clonePlaceholderData), 1)
	assert.Equal(t, len(app.placeholderData), 1)
	assert.Equal(t, clonePlaceholderData[0], app.placeholderData[tg1])
	assertPlaceholderData(t, app, tg1, 1, 1, 0, res)

	ask = newAllocationAskTG(aKey, appID1, tg1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been updated on app")
	assert.Equal(t, len(app.placeholderData), 1)
	assertPlaceholderData(t, app, tg1, 2, 1, 0, res)

	ask = newAllocationAskTG(aKey, appID1, tg2, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been updated on app")

	assert.Equal(t, len(app.placeholderData), 2)
	assertPlaceholderData(t, app, tg2, 1, 0, 0, res)
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
	ask := newAllocationAsk(aKey, appID1, res)
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
	ask := newAllocationAsk(aKey, appID1, res)
	app.RecoverAllocationAsk(ask)
	assert.Equal(t, len(app.requests), 1, "ask should have been added")
	assert.Assert(t, app.IsAccepted(), "Application should be in accepted state")
	assertUserGroupResource(t, getTestUserGroup(), nil)

	ask = newAllocationAsk("ask-2", appID1, res)
	app.RecoverAllocationAsk(ask)
	assert.Equal(t, len(app.requests), 2, "ask should have been added, total should be 2")
	assert.Assert(t, app.IsAccepted(), "Application should have stayed in accepted state")
	assertUserGroupResource(t, getTestUserGroup(), nil)

	assert.Equal(t, 0, len(app.placeholderData))
	ask = newAllocationAskTG("ask-3", appID1, "testGroup", res)
	app.RecoverAllocationAsk(ask)
	assertPlaceholderData(t, app, "testGroup", 1, 0, 0, res)
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
	if app.reservations[allocKey] == nil || !node.IsReserved() {
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
	if app.reservations[allocKey] == nil || !node.IsReserved() {
		t.Fatalf("app should have reservation for %v on node", allocKey)
	}
	num := node.unReserve(ask2)
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
	ask := newAllocationAsk(aKey, appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask 1 should have been added to app")
	ask = newAllocationAsk(aKey2, appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask 2 should have been added to app")
	if len(app.requests) != 2 {
		t.Fatalf("missing asks from app expected 2 got %d", len(app.requests))
	}
	expected := resources.Multiply(res, 2)
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
	if !resources.Equals(delta, res) || reservedAsks != 0 {
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
	ask := newAllocationAsk(aKey, appID1, res)
	ask.placeholder = true
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask 1 should have been added to app")

	ask = newAllocationAsk("alloc-2", appID1, res)
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
	allocInfo := newAllocationWithKey(aKey, appID1, nodeID1, res)
	allocInfo.placeholder = true
	app.AddAllocation(allocInfo)
	err := app.handleApplicationEventWithLocking(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted")

	app.RemoveAllocation("alloc-1", si.TerminationType_UNKNOWN_TERMINATION_TYPE)
	assert.Equal(t, app.stateMachine.Current(), Completing.String())
	assertUserGroupResource(t, getTestUserGroup(), nil)
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
	allocInfo := newAllocationWithKey(askID, appID1, nodeID1, res)
	app.AddAllocation(allocInfo)
	// app should be running
	assert.Assert(t, app.IsRunning(), "Application did not return running state after alloc: %s", app.CurrentState())
	assertUserGroupResource(t, getTestUserGroup(), res)

	// removing the ask should not move anywhere as there is an allocation
	released = app.RemoveAllocationAsk(askID)
	assert.Equal(t, released, 0, "allocation ask should not have been reserved")
	assert.Assert(t, app.IsRunning(), "Application should have stayed same, changed unexpectedly: %s", app.CurrentState())

	// remove the allocation, ask has been removed so nothing left
	app.RemoveAllocation(askID, si.TerminationType_UNKNOWN_TERMINATION_TYPE)
	assert.Assert(t, app.IsCompleting(), "Application did not change as expected: %s", app.CurrentState())
	assertUserGroupResource(t, getTestUserGroup(), nil)

	log := app.GetStateLog()
	assert.Equal(t, len(log), 3, "wrong number of app events")
	assert.Equal(t, log[0].ApplicationState, Accepted.String())
	assert.Equal(t, log[1].ApplicationState, Running.String())
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
	ask := newAllocationAskTG(askID, appID1, tg1, res)
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
	allocInfo := newAllocationAll(askID, appID1, nodeID1, tg1, res, true, 0)
	app.AddAllocation(allocInfo)
	// app should be in the same state as it was before as it is a placeholder allocation
	assert.Assert(t, app.IsAccepted(), "Application did not return accepted state after alloc: %s", app.CurrentState())
	assert.Assert(t, resources.Equals(app.GetPlaceholderResource(), res), "placeholder allocation not set as expected")
	assert.Assert(t, resources.IsZero(app.GetAllocatedResource()), "allocated resource should have been zero")
	assertUserGroupResource(t, getTestUserGroup(), res)

	// first we have to remove the allocation itself
	alloc := app.RemoveAllocation(askID, si.TerminationType_UNKNOWN_TERMINATION_TYPE)
	assert.Assert(t, alloc != nil, "Nil allocation was returned")
	assert.Assert(t, app.IsAccepted(), "Application should have stayed in Accepted, changed unexpectedly: %s", app.CurrentState())
	// removing the ask should move the application into the waiting state, because the allocation is only a placeholder allocation
	released = app.RemoveAllocationAsk(askID)
	assert.Equal(t, released, 0, "allocation ask should not have been reserved")
	assert.Assert(t, app.IsCompleting(), "Application should have stayed same, changed unexpectedly: %s", app.CurrentState())
	// zero resource should be pruned
	assertUserGroupResource(t, getTestUserGroup(), nil)

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
	alloc := newAllocation(appID1, nodeID1, res)
	app.AddAllocation(alloc)
	if !resources.Equals(app.allocatedResource, res) {
		t.Errorf("allocated resources is not updated correctly: %v", app.allocatedResource)
	}
	allocs := app.GetAllAllocations()
	assert.Equal(t, len(allocs), 1)
	assert.Assert(t, app.getPlaceholderTimer() == nil, "Placeholder timer should not be initialized as the allocation is not a placeholder")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))

	// add more allocations to test the removals
	alloc = newAllocation(appID1, nodeID1, res)
	app.AddAllocation(alloc)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	alloc = newAllocation(appID1, nodeID1, res)
	app.AddAllocation(alloc)
	allocs = app.GetAllAllocations()
	assert.Equal(t, len(allocs), 3)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 3))
	// remove one of the 3
	if alloc = app.RemoveAllocation(alloc.GetAllocationKey(), si.TerminationType_UNKNOWN_TERMINATION_TYPE); alloc == nil {
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
	alloc := newAllocation(appID1, nodeID1, res)
	alloc.placeholder = true
	app.AddAllocation(alloc)
	assert.Assert(t, resources.Equals(app.allocatedPlaceholder, res), "allocated placeholders resources is not updated correctly: %s", app.allocatedPlaceholder.String())
	assert.Equal(t, len(app.GetAllAllocations()), 1)
	assert.Assert(t, app.IsAccepted(), "app should still be in accepted state")
	assertUserGroupResource(t, getTestUserGroup(), res)

	// add second placeholder this should trigger state update
	alloc = newAllocation(appID1, nodeID1, res)
	alloc.placeholder = true
	app.AddAllocation(alloc)
	assert.Assert(t, resources.Equals(app.allocatedPlaceholder, totalPH), "allocated placeholders resources is not updated correctly: %s", app.allocatedPlaceholder.String())
	assert.Equal(t, len(app.GetAllAllocations()), 2)
	assert.Assert(t, app.IsRunning(), "app should have changed to running state")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	// add a real alloc this should NOT trigger state update
	alloc = newAllocation(appID1, nodeID1, res)
	app.AddAllocation(alloc)
	assert.Equal(t, len(app.GetAllAllocations()), 3)
	assert.Assert(t, app.IsRunning(), "app should still be in running state")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 3))

	// add a second real alloc this should NOT trigger state update
	alloc = newAllocation(appID1, nodeID1, res)
	app.AddAllocation(alloc)
	assert.Equal(t, len(app.GetAllAllocations()), 4)
	assert.Assert(t, app.IsRunning(), "app should still be in running state")
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
	alloc := newAllocation(appID1, nodeID1, res)
	// adding a normal allocation should change the state
	app.AddAllocation(alloc)
	assert.Assert(t, resources.Equals(app.allocatedResource, res), "allocated resources is not updated correctly: %s", app.allocatedResource.String())
	assert.Equal(t, len(app.GetAllAllocations()), 1)
	assert.Assert(t, app.IsRunning(), "app should be in running state")
	assertUserGroupResource(t, getTestUserGroup(), res)

	// add a second real alloc this should trigger state update
	alloc = newAllocation(appID1, nodeID1, res)
	app.AddAllocation(alloc)
	assert.Equal(t, len(app.GetAllAllocations()), 2)
	assert.Assert(t, app.IsRunning(), "app should have changed to running` state")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
}

func TestUpdateAllocationResourcePending(t *testing.T) {
	setupUGM()
	app := newApplication(appID1, "default", "root.a")
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue")
	queue, err := createDynamicQueue(root, "test", false, nil)
	assert.NilError(t, err, "failed to create test queue")
	app.SetQueue(queue)

	res, err := resources.NewResourceFromConf(map[string]string{"first": "2"})
	assert.NilError(t, err, "failed to create resource with error")
	ask1 := newAllocationAsk(alloc, appID1, res)
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err, "failed to add ask to app")
	assert.Check(t, resources.Equals(res, app.GetPendingResource()), "resources not on app")
	assert.Check(t, resources.Equals(res, queue.GetPendingResource()), "resources not on queue")

	// check nil alloc update
	err = app.UpdateAllocationResources(nil, false)
	assert.Check(t, err != nil, "error not returned on nil alloc")

	// check zero alloc update
	zero := newAllocationWithKey(alloc, appID1, nodeID1, resources.NewResource())
	err = app.UpdateAllocationResources(zero, false)
	assert.Check(t, err != nil, "error not returned on zero alloc")

	// check missing alloc update
	missing := newAllocationWithKey("missing", appID1, nodeID1, res)
	err = app.UpdateAllocationResources(missing, false)
	assert.Check(t, err != nil, "error not returned on missing alloc")

	// check zero delta
	same := newAllocationWithKey(alloc, appID1, nodeID1, res)
	err = app.UpdateAllocationResources(same, false)
	assert.NilError(t, err, "error returned on same alloc size")
	assert.Check(t, resources.Equals(res, app.GetPendingResource()), "resources not on app")
	assert.Check(t, resources.Equals(res, queue.GetPendingResource()), "resources not on queue")

	// check resource incremented
	res2, err := resources.NewResourceFromConf(map[string]string{"first": "3"})
	assert.NilError(t, err, "failed to create resource with error")
	inc := newAllocationWithKey(alloc, appID1, nodeID1, res2)
	err = app.UpdateAllocationResources(inc, false)
	assert.NilError(t, err, "error returned on incremented alloc")
	assert.Check(t, resources.Equals(res2, app.GetPendingResource()), "resources not updated on app")
	assert.Check(t, resources.Equals(res2, queue.GetPendingResource()), "resources not updated on queue")
}

func TestUpdateAllocationResourceAllocated(t *testing.T) {
	setupUGM()
	app := newApplication(appID1, "default", "root.a")
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue")
	queue, err := createDynamicQueue(root, "test", false, nil)
	assert.NilError(t, err, "failed to create test queue")
	app.SetQueue(queue)

	res, err := resources.NewResourceFromConf(map[string]string{"first": "2"})
	assert.NilError(t, err, "failed to create resource with error")
	alloc1 := newAllocationWithKey(alloc, appID1, nodeID1, res)
	queue.IncAllocatedResource(res, false)
	app.RecoverAllocationAsk(alloc1)
	app.AddAllocation(alloc1)
	assert.Check(t, resources.Equals(res, queue.GetAllocatedResource()), "resources not on queue")

	// check nil alloc update
	err = app.UpdateAllocationResources(nil, false)
	assert.Check(t, err != nil, "error not returned on nil alloc")

	// check zero alloc update
	zero := newAllocationWithKey(alloc, appID1, nodeID1, resources.NewResource())
	err = app.UpdateAllocationResources(zero, false)
	assert.Check(t, err != nil, "error not returned on zero alloc")

	// check missing alloc update
	missing := newAllocationWithKey("missing", appID1, nodeID1, res)
	err = app.UpdateAllocationResources(missing, false)
	assert.Check(t, err != nil, "error not returned on missing alloc")
	assert.Check(t, resources.Equals(res, app.GetAllocatedResource()), "resources not on app")
	assert.Check(t, resources.Equals(res, queue.GetAllocatedResource()), "resources not on queue")

	// check zero delta
	same := newAllocationWithKey(alloc, appID1, nodeID1, res)
	err = app.UpdateAllocationResources(same, false)
	assert.NilError(t, err, "error returned on same alloc size")
	assert.Check(t, resources.Equals(res, app.GetAllocatedResource()), "resources not on app")
	assert.Check(t, resources.Equals(res, queue.GetAllocatedResource()), "resources not on queue")

	// check resource incremented
	res2, err := resources.NewResourceFromConf(map[string]string{"first": "3"})
	assert.NilError(t, err, "failed to create resource with error")
	inc := newAllocationWithKey(alloc, appID1, nodeID1, res2)
	err = app.UpdateAllocationResources(inc, false)
	assert.NilError(t, err, "error returned on incremented alloc")
	assert.Check(t, resources.Equals(res2, app.GetAllocatedResource()), "resources not updated on app")
	assert.Check(t, resources.Equals(res2, queue.GetAllocatedResource()), "resources not updated on queue")
}

func TestQueueUpdate(t *testing.T) {
	app := newApplication(appID1, "default", "root.a")

	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue")
	queue, err := createDynamicQueue(root, "test", false, nil)
	assert.NilError(t, err, "failed to create test queue")
	app.SetQueue(queue)
	assert.Equal(t, app.GetQueuePath(), "root.test")
}

func TestCompleted(t *testing.T) {
	completingTimeout = time.Millisecond * 100
	terminatedTimeout = time.Millisecond * 100
	defer func() {
		completingTimeout = time.Second * 30
		terminatedTimeout = 3 * 24 * time.Hour
	}()
	app := newApplication(appID1, "default", "root.a")
	app.requests = map[string]*Allocation{
		"test": {},
	}
	app.sortedRequests = append(app.sortedRequests, &Allocation{})
	err := app.handleApplicationEventWithLocking(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completed test)")
	err = app.handleApplicationEventWithLocking(CompleteApplication)
	assert.NilError(t, err, "no error expected accepted to completing (completed test)")
	assert.Assert(t, app.IsCompleting(), "App should be waiting")
	// give it some time to run and progress
	err = common.WaitForCondition(10*time.Microsecond, time.Millisecond*200, app.IsCompleted)
	assert.NilError(t, err, "Application did not progress into Completed state")

	err = common.WaitForCondition(1*time.Millisecond, time.Millisecond*200, app.IsExpired)
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

func assertResourceUsage(t *testing.T, appSummary *ApplicationSummary, memorySeconds int64, vcoresSecconds int64) {
	detailedResource := appSummary.ResourceUsage.TrackedResourceMap[instType1]
	if detailedResource != nil {
		assert.Equal(t, memorySeconds, int64(detailedResource.Resources["memory"]))
		assert.Equal(t, vcoresSecconds, int64(detailedResource.Resources["vcores"]))
	}
}

func assertPlaceHolderResource(t *testing.T, appSummary *ApplicationSummary, memorySeconds int64, vcoresSecconds int64) {
	detailedResource := appSummary.PlaceholderResource.TrackedResourceMap[instType1]
	assert.Equal(t, memorySeconds, int64(detailedResource.Resources["memory"]))
	assert.Equal(t, vcoresSecconds, int64(detailedResource.Resources["vcores"]))
}

func assertPlaceholderData(t *testing.T, app *Application, taskGroup string, count, timedout, replaced int, res *resources.Resource) {
	assert.Assert(t, len(app.placeholderData) >= 1, "expected placeholder data to be set")
	phData, ok := app.placeholderData[taskGroup]
	assert.Assert(t, ok, "placeholder data not for the taskgroup: %s", taskGroup)
	assert.Equal(t, phData.Count, int64(count), "placeholder count does not match")
	assert.Equal(t, phData.Replaced, int64(replaced), "replaced count does not match")
	assert.Equal(t, phData.TimedOut, int64(timedout), "timedout count does not match")
	if res != nil {
		assert.Assert(t, resources.Equals(phData.MinResource, res), "resource for taskgroup is not correct")
	}
}

func TestResourceUsageAggregation(t *testing.T) {
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
	alloc := newAllocation(appID1, nodeID1, res)
	alloc.SetInstanceType(instType1)
	// Mock the time to be 3 seconds before
	alloc.SetBindTime(time.Now().Add(-3 * time.Second))
	app.AddAllocation(alloc)

	if !resources.Equals(app.allocatedResource, res) {
		t.Errorf("allocated resources is not updated correctly: %v", app.allocatedResource)
	}
	allocs := app.GetAllAllocations()
	assert.Equal(t, len(allocs), 1)
	assert.Assert(t, app.getPlaceholderTimer() == nil, "Placeholder timer should not be initialized as the allocation is not a placeholder")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))

	err = app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completed test)")

	appSummary := app.GetApplicationSummary("default")
	appSummary.DoLogging()
	assertResourceUsage(t, appSummary, 0, 0)

	// add more allocations to test the removals
	alloc = newAllocation(appID1, nodeID1, res)
	alloc.SetInstanceType(instType1)

	// Mock the time to be 3 seconds before
	alloc.SetBindTime(time.Now().Add(-3 * time.Second))
	app.AddAllocation(alloc)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	// remove one of the 2
	if alloc = app.RemoveAllocation(alloc.GetAllocationKey(), si.TerminationType_UNKNOWN_TERMINATION_TYPE); alloc == nil {
		t.Error("returned allocations was nil allocation was not removed")
	}
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))

	appSummary = app.GetApplicationSummary("default")
	appSummary.DoLogging()
	assertResourceUsage(t, appSummary, 300, 30)

	alloc = newAllocation(appID1, nodeID1, res)
	alloc.SetInstanceType(instType1)
	app.AddAllocation(alloc)
	allocs = app.GetAllAllocations()
	assert.Equal(t, len(allocs), 2)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	appSummary = app.GetApplicationSummary("default")
	appSummary.DoLogging()
	assertResourceUsage(t, appSummary, 300, 30)

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

	err = app.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected accepted to completing (completed test)")

	appSummary = app.GetApplicationSummary("default")
	appSummary.DoLogging()
	assertResourceUsage(t, appSummary, 600, 60)
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

	err = common.WaitForCondition(1*time.Millisecond, time.Millisecond*200, app.IsRejected)
	assert.NilError(t, err, "Application did not progress into Rejected state")
	assert.Assert(t, !app.FinishedTime().IsZero())
	assert.Equal(t, app.rejectedMessage, rejectedMessage)
	assert.Equal(t, app.GetRejectedMessage(), rejectedMessage)

	err = common.WaitForCondition(1*time.Millisecond, time.Millisecond*200, app.IsExpired)
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

func TestIsCreateForced(t *testing.T) {
	app := newApplicationWithTags(appID1, "default", "root.a", nil)
	assert.Check(t, !app.IsCreateForced(), "found forced app but tags nil")
	tags := make(map[string]string)
	app = newApplicationWithTags(appID1, "default", "root.a", tags)
	assert.Check(t, !app.IsCreateForced(), "found forced app but tags empty")
	tags[siCommon.AppTagCreateForce] = "false"
	app = newApplicationWithTags(appID1, "default", "root.a", tags)
	assert.Check(t, !app.IsCreateForced(), "found forced app but forced tag was false")
	tags[siCommon.AppTagCreateForce] = "unknown"
	app = newApplicationWithTags(appID1, "default", "root.a", tags)
	assert.Check(t, !app.IsCreateForced(), "found forced app but forced tag was invalid")
	tags[siCommon.AppTagCreateForce] = "true"
	app = newApplicationWithTags(appID1, "default", "root.a", tags)
	assert.Check(t, app.IsCreateForced(), "found unforced app but forced tag was set")
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
	ph := newPlaceholderAlloc(appID1, nodeID1, res, "tg")
	// add the placeholder to the app
	app.AddAllocation(ph)
	// add PlaceholderData
	app.addPlaceholderData(ph)
	assertPlaceholderData(t, app, "tg", 1, 0, 0, res)

	assert.Equal(t, len(app.allocations), 1, "allocation not added as expected")
	assert.Assert(t, resources.IsZero(app.allocatedResource), "placeholder counted as real allocation")
	if !resources.Equals(app.allocatedPlaceholder, res) {
		t.Fatalf("placeholder allocation not updated as expected: got %s, expected %s", app.allocatedPlaceholder, res)
	}
	assertUserGroupResource(t, getTestUserGroup(), res)
	// if the placeholder exists it should be removed even without real allocation linked
	alloc = app.ReplaceAllocation(ph.GetAllocationKey())
	assert.Equal(t, alloc, ph, "returned allocation is not the placeholder")
	// placeholder data must show replaced increase
	assertPlaceholderData(t, app, "tg", 1, 0, 1, res)
	assertUserGroupResource(t, getTestUserGroup(), nil)
	assert.Assert(t, resources.IsZero(app.allocatedPlaceholder), "placeholder should have been released")
	assert.Assert(t, resources.IsZero(app.allocatedResource), "no replacement made allocated should be zero")

	// add the placeholder back to the app, the failure test above changed state and removed the ph
	app.SetState(Running.String())
	app.AddAllocation(ph)
	app.addPlaceholderData(ph)
	assertPlaceholderData(t, app, "tg", 2, 0, 1, res)
	assertUserGroupResource(t, getTestUserGroup(), res)

	// set the real one to replace the placeholder
	realAlloc := newAllocation(appID1, nodeID1, res)
	ph.SetRelease(realAlloc)
	alloc = app.ReplaceAllocation(ph.GetAllocationKey())
	assert.Equal(t, alloc, ph, "returned allocation is not the placeholder")
	assert.Assert(t, resources.IsZero(app.allocatedPlaceholder), "real allocation counted as placeholder")
	assert.Assert(t, resources.Equals(app.allocatedResource, res), "real allocation not updated as expected")
	assertPlaceholderData(t, app, "tg", 2, 0, 2, res)
	assert.Equal(t, realAlloc.GetPlaceholderCreateTime(), ph.GetCreateTime(), "real allocation's placeholder create time not updated as expected: got %s, expected %s", realAlloc.GetPlaceholderCreateTime(), ph.GetCreateTime())
	assertUserGroupResource(t, getTestUserGroup(), res)

	// add the placeholder back to the app, the failure test above changed state and removed the ph
	app.SetState(Running.String())
	ph.ClearRelease()
	app.AddAllocation(ph)
	app.addPlaceholderData(ph)
	assertPlaceholderData(t, app, "tg", 3, 0, 2, res)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
}

func TestReplaceAllocationTracking(t *testing.T) {
	setupUGM()
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app := newApplication(appID1, "default", "root.a")
	app.queue = queue
	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource with error")
	ph1 := newPlaceholderAlloc(appID1, nodeID1, res, "tg")
	ph2 := newPlaceholderAlloc(appID1, nodeID1, res, "tg")
	ph3 := newPlaceholderAlloc(appID1, nodeID1, res, "tg")
	ph1.SetInstanceType(instType1)
	ph2.SetInstanceType(instType1)
	ph3.SetInstanceType(instType1)
	app.AddAllocation(ph1)
	assert.NilError(t, err, "could not add ask")
	app.addPlaceholderData(ph1)
	assert.Equal(t, true, app.HasPlaceholderAllocation())
	app.AddAllocation(ph2)
	assert.NilError(t, err, "could not add ask")
	app.addPlaceholderData(ph2)
	app.AddAllocation(ph3)
	assert.NilError(t, err, "could not add ask")
	app.addPlaceholderData(ph3)

	ph1.SetBindTime(time.Now().Add(-10 * time.Second))
	ph2.SetBindTime(time.Now().Add(-10 * time.Second))
	ph3.SetBindTime(time.Now().Add(-10 * time.Second))

	// replace placeholders
	realAlloc1 := newAllocation(appID1, nodeID1, res)
	ph1.SetRelease(realAlloc1)
	alloc1 := app.ReplaceAllocation(ph1.GetAllocationKey())
	app.RemoveAllocation(ph1.GetAllocationKey(), si.TerminationType_PLACEHOLDER_REPLACED)
	assert.Equal(t, ph1.GetAllocationKey(), alloc1.GetAllocationKey())
	assert.Equal(t, true, app.HasPlaceholderAllocation())
	realAlloc2 := newAllocation(appID1, nodeID1, res)
	ph2.SetRelease(realAlloc2)
	alloc2 := app.ReplaceAllocation(ph2.GetAllocationKey())
	app.RemoveAllocation(ph2.GetAllocationKey(), si.TerminationType_PLACEHOLDER_REPLACED)
	assert.Equal(t, ph2.GetAllocationKey(), alloc2.GetAllocationKey())
	assert.Equal(t, true, app.HasPlaceholderAllocation())
	realAlloc3 := newAllocation(appID1, nodeID1, res)
	ph3.SetRelease(realAlloc3)
	alloc3 := app.ReplaceAllocation(ph3.GetAllocationKey())
	app.RemoveAllocation(ph3.GetAllocationKey(), si.TerminationType_PLACEHOLDER_REPLACED)
	assert.Equal(t, ph3.GetAllocationKey(), alloc3.GetAllocationKey())
	assert.Equal(t, false, app.HasPlaceholderAllocation())

	// check placeholder resource usage
	appSummary := app.GetApplicationSummary("default")
	assertPlaceHolderResource(t, appSummary, 3000, 300)
}

func TestTimeoutPlaceholderSoft(t *testing.T) {
	runTimeoutPlaceholderTest(t, Resuming.String(), Soft)
}

func TestTimeoutPlaceholderHard(t *testing.T) {
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
	phAsk := newAllocationAskTG("ask-1", appID1, tg1, res)
	err = app.AddAllocationAsk(phAsk)
	assert.NilError(t, err, "Application ask should have been added")
	assertPlaceholderData(t, app, tg1, 1, 0, 0, res)
	assert.Assert(t, app.IsAccepted(), "Application should be in accepted state")

	// add the placeholder to the app
	ph1 := newPlaceholderAlloc(appID1, nodeID1, res, tg2)
	app.AddAllocation(ph1)
	app.addPlaceholderDataWithLocking(ph1)
	assertPlaceholderData(t, app, tg2, 1, 0, 0, res)
	assertUserGroupResource(t, getTestUserGroup(), res)
	assert.Assert(t, app.getPlaceholderTimer() != nil, "Placeholder timer should be initiated after the first placeholder allocation")
	// add a second one to check the filter
	ph2 := newPlaceholderAlloc(appID1, nodeID1, res, tg2)
	app.AddAllocation(ph2)
	app.addPlaceholderDataWithLocking(ph2)
	assertPlaceholderData(t, app, tg2, 2, 0, 0, res)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	assert.Assert(t, app.IsAccepted(), "Application should be in accepted state")
	err = common.WaitForCondition(10*time.Millisecond, 1*time.Second, func() bool {
		app.RLock()
		defer app.RUnlock()
		return app.placeholderTimer == nil
	})
	assert.NilError(t, err, "Placeholder timeout cleanup did not trigger unexpectedly")
	// pending updates immediately
	assertPlaceholderData(t, app, tg1, 1, 1, 0, res)
	// No changes until the removals are confirmed
	assertPlaceholderData(t, app, tg2, 2, 0, 0, res)

	assert.Equal(t, app.stateMachine.Current(), expectedState, "Application did not progress into expected state")
	log := app.GetStateLog()
	assert.Equal(t, len(log), 2, "wrong number of app events")
	assert.Equal(t, log[0].ApplicationState, Accepted.String())
	assert.Equal(t, log[1].ApplicationState, expectedState)

	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	// ordering of events is based on the order in which we call the release in the application
	// first are the allocated placeholders then the pending placeholders, always same values
	// See the timeoutPlaceholderProcessing() function
	expectedReleases := []int{2, 1}
	events := testHandler.GetEvents()
	var found int
	idx := 0
	for _, event := range events {
		if allocRelease, ok := event.(*rmevent.RMReleaseAllocationEvent); ok {
			assert.Equal(t, len(allocRelease.ReleasedAllocations), expectedReleases[idx], "wrong number of allocations released")
			found++
			idx++
		}
	}
	assert.Equal(t, found, 2, "release allocation or ask event not found in list")

	// asks are completely cleaned up
	assert.Assert(t, resources.IsZero(app.GetPendingResource()), "pending placeholder resources should be zero")
	// a released placeholder still holds the resource until release confirmed by the RM
	assert.Assert(t, resources.Equals(app.GetPlaceholderResource(), resources.Multiply(res, 2)), "Unexpected placeholder resources for the app")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	removed := app.RemoveAllocation(ph1.allocationKey, si.TerminationType_TIMEOUT)
	assert.Assert(t, removed != nil, "expected allocation got nil")
	assert.Equal(t, ph1.allocationKey, removed.allocationKey, "expected placeholder to be returned")
	removed = app.RemoveAllocation(ph2.allocationKey, si.TerminationType_TIMEOUT)
	assert.Assert(t, removed != nil, "expected allocation got nil")
	assert.Equal(t, ph2.allocationKey, removed.allocationKey, "expected placeholder to be returned")

	// Removals are confirmed: timeout should equal to count
	assertPlaceholderData(t, app, tg2, 2, 2, 0, res)
}

func TestTimeoutPlaceholderAllocReleased(t *testing.T) {
	setupUGM()

	originalPhTimeout := defaultPlaceholderTimeout
	defaultPlaceholderTimeout = 100 * time.Millisecond
	defer func() { defaultPlaceholderTimeout = originalPhTimeout }()

	app, testHandler := newApplicationWithHandler(appID1, "default", "root.a")
	assert.Assert(t, app.getPlaceholderTimer() == nil, "Placeholder timer should be nil on create")
	app.SetState(Accepted.String())

	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "Unexpected error when creating resource from map")
	// add the placeholders to the app: one released, one still available.
	phReleased := newPlaceholderAlloc(appID1, nodeID1, res, tg1)
	err = phReleased.SetReleased(true)
	assert.NilError(t, err, "Unexpected error when releasing placeholder")
	app.AddAllocation(phReleased)
	app.addPlaceholderDataWithLocking(phReleased)
	assertPlaceholderData(t, app, tg1, 1, 0, 0, res)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))

	assert.Assert(t, app.getPlaceholderTimer() != nil, "Placeholder timer should be initiated after the first placeholder allocation")
	ph := newPlaceholderAlloc(appID1, nodeID1, res, tg1)
	app.AddAllocation(ph)
	app.addPlaceholderDataWithLocking(ph)
	assertPlaceholderData(t, app, tg1, 2, 0, 0, res)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	alloc := newAllocation(appID1, nodeID1, res)
	app.AddAllocation(alloc)
	assert.Assert(t, app.IsRunning(), "App should be in running state after the first allocation")
	err = common.WaitForCondition(10*time.Millisecond, 1*time.Second, func() bool {
		return app.getPlaceholderTimer() == nil
	})
	assert.NilError(t, err, "Placeholder timeout cleanup did not trigger unexpectedly")
	assert.Assert(t, app.IsRunning(), "App should be in running state after the first allocation")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 3))
	// two state updates and 1 release event
	events := testHandler.GetEvents()
	var found bool
	for _, event := range events {
		if allocRelease, ok := event.(*rmevent.RMReleaseAllocationEvent); ok {
			assert.Equal(t, len(allocRelease.ReleasedAllocations), 1, "one allocation should have been released")
			assert.Equal(t, allocRelease.ReleasedAllocations[0].AllocationKey, ph.allocationKey, "wrong placeholder allocation released on timeout")
			found = true
		}
	}
	assert.Assert(t, found, "release allocation event not found in list")
	assert.Assert(t, resources.Equals(app.GetAllocatedResource(), res), "Unexpected allocated resources for the app")
	// a released placeholder still holds the resource until release confirmed by the RM
	assert.Assert(t, resources.Equals(app.GetPlaceholderResource(), resources.Multiply(res, 2)), "Unexpected placeholder resources for the app")
	// tracking data not updated until confirmed by the RM
	assertPlaceholderData(t, app, tg1, 2, 0, 0, res)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 3))
	// do what the RM does and respond to the release
	removed := app.RemoveAllocation(ph.allocationKey, si.TerminationType_TIMEOUT)
	assert.Assert(t, removed != nil, "expected allocation got nil")
	assert.Equal(t, ph.allocationKey, removed.allocationKey, "expected placeholder to be returned")
	assertPlaceholderData(t, app, tg1, 2, 1, 0, res)
	assert.Assert(t, resources.Equals(app.GetPlaceholderResource(), res), "placeholder resources still accounted for on the app")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	// process the replacement no real alloc linked account for that
	removed = app.ReplaceAllocation(phReleased.allocationKey)
	assert.Assert(t, removed != nil, "expected allocation got nil")
	assert.Equal(t, phReleased.allocationKey, removed.allocationKey, "expected placeholder to be returned")
	assertPlaceholderData(t, app, tg1, 2, 1, 1, res)
	assert.Assert(t, resources.IsZero(app.GetPlaceholderResource()), "placeholder resources still accounted for on the app")
	assertUserGroupResource(t, getTestUserGroup(), res)
}

func TestTimeoutPlaceholderAllocPreempted(t *testing.T) {
	setupUGM()

	originalPhTimeout := defaultPlaceholderTimeout
	defaultPlaceholderTimeout = 100 * time.Millisecond
	defer func() { defaultPlaceholderTimeout = originalPhTimeout }()

	app, testHandler := newApplicationWithHandler(appID1, "default", "root.a")
	assert.Assert(t, app.getPlaceholderTimer() == nil, "Placeholder timer should be nil on create")
	app.SetState(Accepted.String())

	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "Unexpected error when creating resource from map")
	// add the placeholders to the app: one released, one still available.
	phReleased := newPlaceholderAlloc(appID1, nodeID1, res, tg1)
	err = phReleased.SetReleased(true)
	assert.NilError(t, err, "Unexpected error when releasing placeholder")
	app.AddAllocation(phReleased)
	app.addPlaceholderDataWithLocking(phReleased)
	assertPlaceholderData(t, app, tg1, 1, 0, 0, res)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))

	assert.Assert(t, app.getPlaceholderTimer() != nil, "Placeholder timer should be initiated after the first placeholder allocation")
	ph := newPlaceholderAlloc(appID1, nodeID1, res, tg1)
	app.AddAllocation(ph)
	app.addPlaceholderDataWithLocking(ph)
	assertPlaceholderData(t, app, tg1, 2, 0, 0, res)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	alloc := newAllocation(appID1, nodeID1, res)
	app.AddAllocation(alloc)
	assert.Assert(t, app.IsRunning(), "App should be in running state after the first allocation")
	err = common.WaitForCondition(10*time.Millisecond, 1*time.Second, func() bool {
		// Preempt the placeholder in the meantime
		err = ph.MarkPreempted()
		assert.NilError(t, err, "Unexpected error when marking placeholder preempted")
		app.notifyRMAllocationReleased([]*Allocation{ph}, si.TerminationType_PREEMPTED_BY_SCHEDULER,
			"preempting allocations to free up resources to run ask: "+ph.GetAllocationKey())
		return app.getPlaceholderTimer() == nil
	})
	assert.NilError(t, err, "Placeholder timeout cleanup did not trigger unexpectedly")
	assert.Assert(t, app.IsRunning(), "App should be in running state after the first allocation")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 3))
	// two state updates and 1 release event
	events := testHandler.GetEvents()
	var found bool
	for _, event := range events {
		if allocRelease, ok := event.(*rmevent.RMReleaseAllocationEvent); ok {
			assert.Equal(t, len(allocRelease.ReleasedAllocations), 1, "one allocation should have been released")
			assert.Equal(t, allocRelease.ReleasedAllocations[0].GetTerminationType(), si.TerminationType_PREEMPTED_BY_SCHEDULER, "")
			assert.Equal(t, allocRelease.ReleasedAllocations[0].AllocationKey, ph.allocationKey, "wrong placeholder allocation released on timeout")
			found = true
		}
	}
	assert.Assert(t, found, "release allocation event not found in list")
	assert.Assert(t, resources.Equals(app.GetAllocatedResource(), res), "Unexpected allocated resources for the app")
	// a released placeholder still holds the resource until release confirmed by the RM
	assert.Assert(t, resources.Equals(app.GetPlaceholderResource(), resources.Multiply(res, 2)), "Unexpected placeholder resources for the app")
	// tracking data not updated until confirmed by the RM
	assertPlaceholderData(t, app, tg1, 2, 0, 0, res)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 3))
	// do what the RM does and respond to the release
	removed := app.RemoveAllocation(ph.allocationKey, si.TerminationType_TIMEOUT)
	assert.Assert(t, removed != nil, "expected allocation got nil")
	assert.Equal(t, ph.allocationKey, removed.allocationKey, "expected placeholder to be returned")
	assertPlaceholderData(t, app, tg1, 2, 1, 0, res)
	assert.Assert(t, resources.Equals(app.GetPlaceholderResource(), res), "placeholder resources still accounted for on the app")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	// process the replacement no real alloc linked account for that
	removed = app.ReplaceAllocation(phReleased.allocationKey)
	assert.Assert(t, removed != nil, "expected allocation got nil")
	assert.Equal(t, phReleased.allocationKey, removed.allocationKey, "expected placeholder to be returned")
	assertPlaceholderData(t, app, tg1, 2, 1, 1, res)
	assert.Assert(t, resources.IsZero(app.GetPlaceholderResource()), "placeholder resources still accounted for on the app")
	assertUserGroupResource(t, getTestUserGroup(), res)
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
	ph := newPlaceholderAlloc(appID1, nodeID1, res, tg1)
	app.AddAllocation(ph)
	assert.Assert(t, app.getPlaceholderTimer() != nil, "Placeholder timer should be initiated after the first placeholder allocation")
	app.addPlaceholderDataWithLocking(ph)
	assertPlaceholderData(t, app, tg1, 1, 0, 0, res)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))
	// add a real allocation as well
	alloc := newAllocation(appID1, nodeID1, res)
	app.AddAllocation(alloc)
	// move on to running
	app.SetState(Running.String())
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	// remove allocation to trigger state change
	app.RemoveAllocation(alloc.GetAllocationKey(), si.TerminationType_UNKNOWN_TERMINATION_TYPE)
	assert.Assert(t, app.IsCompleting(), "App should be in completing state all allocs have been removed")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))
	// make sure the placeholders time out
	err = common.WaitForCondition(10*time.Millisecond, 1*time.Second, func() bool {
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
	}
	assert.Assert(t, found, "release allocation event not found in list")
	assert.Assert(t, app.IsCompleting(), "App should be in completing state")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))
	// tracking data not updated until confirmed by the RM
	assertPlaceholderData(t, app, tg1, 1, 0, 0, res)
	// do what the RM does and respond to the release
	removed := app.RemoveAllocation(ph.allocationKey, si.TerminationType_TIMEOUT)
	assert.Assert(t, removed != nil, "expected allocation got nil")
	assert.Equal(t, ph.allocationKey, removed.allocationKey, "expected placeholder to be returned")
	assertPlaceholderData(t, app, tg1, 1, 1, 0, res)
}

func TestTimeoutPlaceholderCompletingWithPreemptedPh(t *testing.T) {
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
	tg := "tg-1"
	ph := newPlaceholderAlloc(appID1, nodeID1, res, tg)
	app.AddAllocation(ph)
	assert.Assert(t, app.getPlaceholderTimer() != nil, "Placeholder timer should be initiated after the first placeholder allocation")
	app.addPlaceholderDataWithLocking(ph)
	assertPlaceholderData(t, app, tg, 1, 0, 0, res)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))
	// add a real allocation as well
	alloc := newAllocation(appID1, nodeID1, res)
	app.AddAllocation(alloc)
	// move on to running
	app.SetState(Running.String())
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	// remove allocation to trigger state change
	app.RemoveAllocation(alloc.GetAllocationKey(), si.TerminationType_UNKNOWN_TERMINATION_TYPE)
	assert.Assert(t, app.IsCompleting(), "App should be in completing state all allocs have been removed")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))
	// make sure the placeholders time out
	err = common.WaitForCondition(10*time.Millisecond, 1*time.Second, func() bool {
		// Preempt the placeholder in the meantime
		err = ph.MarkPreempted()
		assert.NilError(t, err, "Unexpected error when marking placeholder preempted")
		app.notifyRMAllocationReleased([]*Allocation{ph}, si.TerminationType_PREEMPTED_BY_SCHEDULER,
			"preempting allocations to free up resources to run ask: "+ph.GetAllocationKey())
		return app.getPlaceholderTimer() == nil
	})
	assert.NilError(t, err, "Placeholder timer did not time out as expected")
	events := testHandler.GetEvents()
	var found bool
	for _, event := range events {
		if allocRelease, ok := event.(*rmevent.RMReleaseAllocationEvent); ok {
			assert.Equal(t, len(allocRelease.ReleasedAllocations), 1, "one allocation should have been released")
			assert.Equal(t, allocRelease.ReleasedAllocations[0].GetTerminationType(), si.TerminationType_PREEMPTED_BY_SCHEDULER, "")
			assert.Equal(t, allocRelease.ReleasedAllocations[0].AllocationKey, ph.allocationKey, "wrong placeholder allocation released on timeout")
			found = true
		}
	}
	assert.Assert(t, found, "release allocation event not found in list")
	assert.Assert(t, app.IsCompleting(), "App should be in completing state")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))
	// tracking data not updated until confirmed by the RM
	assertPlaceholderData(t, app, tg, 1, 0, 0, res)
	// do what the RM does and respond to the release
	removed := app.RemoveAllocation(ph.allocationKey, si.TerminationType_TIMEOUT)
	assert.Assert(t, removed != nil, "expected allocation got nil")
	assert.Equal(t, ph.allocationKey, removed.allocationKey, "expected placeholder to be returned")
	assertPlaceholderData(t, app, tg, 1, 1, 0, res)
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
	tg := "tg-1"
	ph := newPlaceholderAlloc(appID1, nodeID1, res, tg)
	app.AddAllocation(ph)
	assert.Assert(t, app.getPlaceholderTimer() != nil, "Placeholder timer should be initiated after the first placeholder allocation")
	app.addPlaceholderDataWithLocking(ph)
	assertPlaceholderData(t, app, tg, 1, 0, 0, res)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))
	// add a real allocation as well
	alloc := newAllocation(appID1, nodeID1, res)
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
	assertPlaceholderData(t, app, tg, 1, 1, 0, res)
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
	// zero resource should be pruned
	assertUserGroupResource(t, getTestUserGroup(), nil)
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
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser", testgroups), res, nil, 0)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser2", testgroups), nil, nil, 0)

	// Increase both testuser and testuser2 with the same group testgroup
	app.incUserResourceUsage(res)
	app2.incUserResourceUsage(resources.Multiply(res, 2))
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser", testgroups), resources.Multiply(res, 2), nil, 0)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser2", testgroups), resources.Multiply(res, 2), nil, 0)

	// Increase nil
	app.decUserResourceUsage(nil, false)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser", testgroups), resources.Multiply(res, 2), nil, 0)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser2", testgroups), resources.Multiply(res, 2), nil, 0)

	// Decrease testuser
	app.decUserResourceUsage(res, false)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser", testgroups), res, nil, 0)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser2", testgroups), resources.Multiply(res, 2), nil, 0)

	// Decrease testuser2
	app2.decUserResourceUsage(res, false)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser", testgroups), res, nil, 0)
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser2", testgroups), res, nil, 0)

	// Decrease testuser and testuser2 to 0
	app.decUserResourceUsage(res, false)
	app2.decUserResourceUsage(res, false)
	// zero resoure should be pruned
	assertUserResourcesAndGroupResources(t, getUserGroup("testuser", testgroups), nil, nil, 0)
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

	// Don't need sleep here, anytime finished, we will set finishedTime for now
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
	const (
		tg1 = "available"
		tg2 = "unavailable"
		tg3 = "timedout"
	)

	app := newApplication(appID1, "default", "root.unknown")
	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "Unexpected error when creating resource from map")

	tests := []struct {
		name string
		ask  *Allocation
		want bool
	}{
		{"nil", nil, false},
		{"placeholder", newAllocationAskTG(aKey, appID1, tg1, res), false},
		{"no TG", newAllocationAsk(aKey, appID1, res), false},
		{"no placeholder data", newAllocationAskAll(aKey, appID1, tg1, res, false, 0), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, app.canReplace(tt.ask), "unexpected replacement")
		})
	}
	// add the placeholder data
	// available tg has one replacement open
	app.addPlaceholderData(newAllocationAskTG(aKey, appID1, tg1, res))
	// unavailable tg has NO replacement open (replaced)
	app.addPlaceholderData(newAllocationAskTG(aKey, appID1, tg2, res))
	app.placeholderData[tg2].Replaced++
	// unavailable tg has NO replacement open (timedout)
	app.addPlaceholderData(newAllocationAskTG(aKey, appID1, tg3, res))
	app.placeholderData[tg3].TimedOut++
	tests = []struct {
		name string
		ask  *Allocation
		want bool
	}{
		{"no TG", newAllocationAsk(aKey, appID1, res), false},
		{"TG mismatch", newAllocationAskAll(aKey, appID1, "unknown", res, false, 0), false},
		{"TG placeholder used", newAllocationAskAll(aKey, appID1, tg2, res, false, 0), false},
		{"TG placeholder timed out", newAllocationAskAll(aKey, appID1, tg3, res, false, 0), false},
		{"TG placeholder available", newAllocationAskAll(aKey, appID1, tg1, res, false, 0), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, app.canReplace(tt.ask), "unexpected replacement")
		})
	}
}

func TestTryRequiredNode(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{nodeID1: node}
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}
	resMap := map[string]string{"first": "5"}
	rootQ, err := createRootQueue(resMap)
	assert.NilError(t, err, "unexpected error when creating root queue")
	var childQ *Queue
	childQ, err = createManagedQueue(rootQ, "child", false, resMap)
	assert.NilError(t, err, "unexpected error when creating child queue")

	app := newApplication(appID1, "default", "root.child")
	app.SetQueue(childQ)
	childQ.applications[appID1] = app
	allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 3})
	alloc := newAllocation(aKey, nodeID1, allocRes)
	app.AddAllocation(alloc)
	node.AddAllocation(alloc)

	ask := newAllocationAsk(aKey2, appID1, allocRes)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "unexpected error when adding an ask")
	err = app.Reserve(node, ask)
	assert.NilError(t, err, "unexpected error when reserving ask")

	// get a small enough allocation that fits after cancelling the reservation
	allocRes = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	ask = newAllocationAsk(aKey3, appID1, allocRes)
	ask.requiredNode = nodeID1
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "unexpected error when adding an ask")
	result := app.tryRequiredNode(ask, getNode)
	assert.Assert(t, result != nil, "alloc expected")
	assert.Assert(t, result.Request == ask, "alloc expected for the ask")
	assert.Equal(t, result.ResultType, Allocated, "expected allocated result")
	assert.Equal(t, result.CancelledReservations, 1, "expected 1 cancelled reservation")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	// the non required node one should be cancelled
	assert.Assert(t, !node.isReservedForAllocation(aKey2), "expecting no reservation for alloc-2 on node")
	assert.Equal(t, app.NodeReservedForAsk(aKey2), "", "expecting no reservation for alloc-2 on node-1")
}

func TestTryRequiredNodeReserved(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{nodeID1: node}
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}
	resMap := map[string]string{"first": "5"}
	rootQ, err := createRootQueue(resMap)
	assert.NilError(t, err, "unexpected error when creating root queue")
	var childQ *Queue
	childQ, err = createManagedQueue(rootQ, "child", false, resMap)
	assert.NilError(t, err, "unexpected error when creating child queue")

	app := newApplication(appID1, "default", "root.child")
	app.SetQueue(childQ)
	childQ.applications[appID1] = app
	allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2})
	ask := newAllocationAsk(aKey2, appID1, allocRes)
	ask.requiredNode = nodeID1
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "unexpected error when adding an ask")
	err = app.Reserve(node, ask)
	assert.NilError(t, err, "unexpected error when reserving ask")

	result := app.tryRequiredNode(ask, getNode)
	assert.Assert(t, result != nil, "alloc expected")
	assert.Assert(t, result.Request == ask, "alloc expected for the ask")
	assert.Equal(t, result.ResultType, AllocatedReserved, "expected allocated reserved result")
	assert.Equal(t, result.CancelledReservations, 0, "expected no cancelled reservation")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
}

func TestTryRequiredNodeReserve(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{nodeID1: node}
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}
	resMap := map[string]string{"first": "5"}
	rootQ, err := createRootQueue(resMap)
	assert.NilError(t, err, "unexpected error when creating root queue")
	var childQ *Queue
	childQ, err = createManagedQueue(rootQ, "child", false, resMap)
	assert.NilError(t, err, "unexpected error when creating child queue")

	app := newApplication(appID1, "default", "root.child")
	app.SetQueue(childQ)
	childQ.applications[appID1] = app
	allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 3})
	alloc := newAllocation(aKey, nodeID1, allocRes)
	app.AddAllocation(alloc)
	node.AddAllocation(alloc)

	ask := newAllocationAsk(aKey2, appID1, allocRes)
	ask.requiredNode = nodeID1
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "unexpected error when adding an ask")

	result := app.tryRequiredNode(ask, getNode)
	assert.Assert(t, result != nil, "alloc expected")
	assert.Assert(t, result.Request == ask, "alloc expected for the ask")
	assert.Equal(t, result.ResultType, Reserved, "expected reserved result")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
}

func TestTryRequiredNodeCancel(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{nodeID1: node}
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}
	resMap := map[string]string{"first": "5"}
	rootQ, err := createRootQueue(resMap)
	assert.NilError(t, err, "unexpected error when creating root queue")
	var childQ *Queue
	childQ, err = createManagedQueue(rootQ, "child", false, resMap)
	assert.NilError(t, err, "unexpected error when creating child queue")

	app := newApplication(appID1, "default", "root.child")
	app.SetQueue(childQ)
	childQ.applications[appID1] = app
	allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 3})
	alloc := newAllocation(aKey, nodeID1, allocRes)
	app.AddAllocation(alloc)
	node.AddAllocation(alloc)

	ask := newAllocationAsk(aKey2, appID1, allocRes)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "adding new allocation to app failed unexpected")
	err = app.reserveInternal(node, ask)
	assert.NilError(t, err, "reserving new allocation on app/node failed unexpected")
	assert.Assert(t, node.isReservedForAllocation(aKey2), "expecting alloc reservation on node")
	assert.Equal(t, app.NodeReservedForAsk(aKey2), nodeID1, "expecting app reservation on node")

	ask = newAllocationAsk(aKey3, appID1, allocRes)
	ask.requiredNode = nodeID1
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "adding new allocation to app failed unexpected")
	result := app.tryRequiredNode(ask, getNode)
	assert.Assert(t, result != nil, "alloc expected")
	assert.Assert(t, result.Request == ask, "alloc expected for the ask")
	assert.Equal(t, result.ResultType, Reserved, "expected allocated result")
	assert.Equal(t, result.CancelledReservations, 1, "expected 1 cancelled reservation")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	assert.Assert(t, !node.isReservedForAllocation(aKey2), "expecting no reservation for alloc-2 on node")
	assert.Equal(t, app.NodeReservedForAsk(aKey2), "", "expecting no reservation for alloc-2 on app")
}

func TestTryRequiredNodeAdd(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{nodeID1: node}
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}
	rootQ, err := createRootQueue(map[string]string{"first": "5"})
	assert.NilError(t, err, "unexpected error when creating root queue")
	var childQ *Queue
	childQ, err = createManagedQueue(rootQ, "child", false, map[string]string{"first": "5"})
	assert.NilError(t, err, "unexpected error when creating child queue")

	app := newApplication(appID1, "default", "root.child")
	app.SetQueue(childQ)
	childQ.applications[appID1] = app
	allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 3})
	alloc := newAllocation(aKey, nodeID1, allocRes)
	app.AddAllocation(alloc)
	node.AddAllocation(alloc)

	ask := newAllocationAsk(aKey2, appID1, allocRes)
	ask.requiredNode = nodeID1
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "adding new allocation to app failed unexpected")

	result := app.tryRequiredNode(ask, getNode)
	assert.Assert(t, result != nil, "alloc expected")
	assert.Assert(t, result.Request == ask, "alloc expected for the ask")
	assert.Equal(t, result.ResultType, Reserved, "expected reserved result")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")

	// finish processing do what the context would do
	err = app.reserveInternal(node, ask)
	assert.NilError(t, err, "reservation processing failed unexpected")

	ask = newAllocationAsk(aKey3, appID1, allocRes)
	ask.requiredNode = nodeID1
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "adding new allocation to app failed unexpected")
	result = app.tryRequiredNode(ask, getNode)
	assert.Assert(t, result != nil, "alloc expected")
	assert.Assert(t, result.Request == ask, "alloc expected for the ask")
	assert.Equal(t, result.ResultType, Reserved, "expected allocated result")
	assert.Equal(t, result.CancelledReservations, 0, "expected no cancelled reservation")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	assert.Assert(t, node.isReservedForAllocation(aKey2), "expecting reservation for alloc-2 on node")
	assert.Equal(t, app.NodeReservedForAsk(aKey2), nodeID1, "expecting reservation for alloc-2 on app")
	assert.Assert(t, !node.isReservedForAllocation(aKey3), "expecting no reservation for alloc-3 on node")
	assert.Equal(t, app.NodeReservedForAsk(aKey3), "", "expecting no reservation for alloc-3 on app")
}

func TestTryRequiredNodeExists(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{nodeID1: node}
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}
	rootQ, err := createRootQueue(map[string]string{"first": "5"})
	assert.NilError(t, err, "unexpected error when creating root queue")
	var childQ *Queue
	childQ, err = createManagedQueue(rootQ, "child", false, map[string]string{"first": "5"})
	assert.NilError(t, err, "unexpected error when creating child queue")

	app := newApplication(appID1, "default", "root.child")
	app.SetQueue(childQ)
	childQ.applications[appID1] = app
	allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 3})

	ask := newAllocationAsk(aKey2, appID1, allocRes)
	ask.requiredNode = nodeID2
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "adding new allocation to app failed unexpected")

	result := app.tryRequiredNode(ask, getNode)
	assert.Assert(t, result == nil, "alloc not expected")
}

func TestTryAllocateNoRequests(t *testing.T) {
	node := newNode("node1", map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{"node1": node}
	iterator := getNodeIteratorFn(node)
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}

	app := newApplication(appID1, "default", "root.unknown")
	preemptionAttemptsRemaining := 0
	result := app.tryAllocate(node.GetAvailableResource(), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Check(t, result == nil, "unexpected result")
}

func TestTryAllocateFit(t *testing.T) {
	node := newNode("node1", map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{"node1": node}
	iterator := getNodeIteratorFn(node)
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
	result := app.tryAllocate(node.GetAvailableResource(), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)

	assert.Assert(t, result != nil, "alloc expected")
	assert.Assert(t, result.Request != nil, "alloc expected")
	assert.Equal(t, "node1", result.NodeID, "wrong node")
}

func TestTryAllocatePreemptQueue(t *testing.T) {
	node := newNode("node1", map[string]resources.Quantity{"first": 20})
	nodeMap := map[string]*Node{"node1": node}
	iterator := getNodeIteratorFn(node)
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}
	appQueueMapping := NewAppQueueMapping()

	rootQ, err := createRootQueue(map[string]string{"first": "20"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "20"}, map[string]string{"first": "10"}, appQueueMapping)
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, map[string]string{"first": "5"}, appQueueMapping)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "5"}, appQueueMapping)
	assert.NilError(t, err)

	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.AddApplication(app1)
	appQueueMapping.AddAppQueueMapping(appID1, childQ1)
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	err = app1.AddAllocationAsk(ask1)
	assert.NilError(t, err)
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	err = app1.AddAllocationAsk(ask2)
	assert.NilError(t, err)

	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.AddApplication(app2)
	appQueueMapping.AddAppQueueMapping(appID2, childQ2)
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask3.allowPreemptOther = true
	err = app2.AddAllocationAsk(ask3)
	assert.NilError(t, err)

	preemptionAttemptsRemaining := 10

	result1 := app1.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, result1 != nil, "result1 expected")
	alloc1 := result1.Request
	assert.Assert(t, alloc1 != nil, "alloc1 expected")
	result2 := app1.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, result2 != nil, "result2 expected")
	alloc2 := result2.Request
	assert.Assert(t, alloc2 != nil, "alloc2 expected")

	// preemption max attempts exhausted
	maxAttemptsExhausted := 0
	result3 := app2.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0}), true, 30*time.Second, &maxAttemptsExhausted, iterator, iterator, getNode)
	assert.Assert(t, result3 == nil, "result3 not expected")
	assert.Assert(t, !alloc2.IsPreempted(), "alloc2 should not have been preempted")
	assertAllocationLog(t, ask3, []string{common.PreemptionMaxAttemptsExhausted, common.PreemptionDoesNotHelp})
	assert.Equal(t, maxAttemptsExhausted, 0)

	maxAttemptsDecrease := 10

	// on first attempt, not enough time has passed
	result3 = app2.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0}), true, 30*time.Second, &maxAttemptsDecrease, iterator, iterator, getNode)
	assert.Assert(t, result3 == nil, "result3 not expected")
	assert.Assert(t, !alloc2.IsPreempted(), "alloc2 should not have been preempted")
	assertAllocationLog(t, ask3, []string{common.PreemptionPreconditionsFailed, common.PreemptionDoesNotHelp})
	assert.Equal(t, maxAttemptsDecrease, 10)

	// pass the time and try again
	ask3.createTime = ask3.createTime.Add(-30 * time.Second)
	result3 = app2.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0}), true, 30*time.Second, &maxAttemptsDecrease, iterator, iterator, getNode)
	assert.Assert(t, result3 != nil && result3.Request != nil && result3.ResultType == Reserved, "alloc3 should be a reservation")
	assert.Assert(t, alloc2.IsPreempted(), "alloc2 should have been preempted")
	assert.Equal(t, maxAttemptsDecrease, 9)
}

func TestTryAllocatePreemptNode(t *testing.T) {
	iterator, getNode, _, ask3, _, app2, allocs := createPreemptNodeTestSetup(t)
	preemptionAttemptsRemaining := 10

	// preemption delay not yet passed, so preemption should fail
	result3 := app2.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 18}), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, result3 == nil, "result3 expected")
	assert.Assert(t, !allocs[1].IsPreempted(), "alloc1 should have been preempted")
	assertAllocationLog(t, ask3, []string{common.PreemptionPreconditionsFailed, common.PreemptionDoesNotHelp})

	// pass the time and try again
	ask3.createTime = ask3.createTime.Add(-30 * time.Second)
	result3 = app2.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 18}), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, result3 != nil, "result3 expected")
	assert.Equal(t, Reserved, result3.ResultType, "expected reservation")
	alloc3 := result3.Request
	assert.Assert(t, alloc3 != nil, "alloc3 expected")
	assert.Assert(t, allocs[0].IsPreempted(), "alloc1 should have been preempted")
}

func createPreemptNodeTestSetup(t *testing.T) (func() NodeIterator, func(NodeID string) *Node, *Queue, *Allocation, *Application, *Application, []*Allocation) {
	node1 := newNode("node1", map[string]resources.Quantity{"first": 20})
	node2 := newNode("node2", map[string]resources.Quantity{"first": 20})
	nodeMap := map[string]*Node{"node1": node1, "node2": node2}
	iterator := getNodeIteratorFn(node1, node2)
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}

	appQueueMapping := NewAppQueueMapping()

	rootQ, err := createRootQueue(map[string]string{"first": "40"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "20"}, map[string]string{"first": "10"}, appQueueMapping)
	assert.NilError(t, err)
	unlimitedQ, err := createManagedQueueGuaranteed(rootQ, "unlimited", false, nil, nil, appQueueMapping)
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, map[string]string{"first": "5"}, appQueueMapping)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "5"}, appQueueMapping)
	assert.NilError(t, err)

	app0 := newApplication(appID0, "default", "root.unlimited")
	app0.SetQueue(unlimitedQ)
	unlimitedQ.AddApplication(app0)
	appQueueMapping.AddAppQueueMapping(appID0, unlimitedQ)
	ask00 := newAllocationAsk("alloc0-0", appID0, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11}))
	err = app0.AddAllocationAsk(ask00)
	assert.NilError(t, err)
	ask01 := newAllocationAsk("alloc0-1", appID0, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11}))
	err = app0.AddAllocationAsk(ask01)
	assert.NilError(t, err)

	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.AddApplication(app1)
	appQueueMapping.AddAppQueueMapping(appID1, childQ1)
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	err = app1.AddAllocationAsk(ask1)
	assert.NilError(t, err)
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	err = app1.AddAllocationAsk(ask2)
	assert.NilError(t, err)

	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.AddApplication(app2)
	appQueueMapping.AddAppQueueMapping(appID2, childQ2)
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask3.allowPreemptOther = true
	err = app2.AddAllocationAsk(ask3)
	assert.NilError(t, err)

	preemptionAttemptsRemaining := 10

	// consume capacity with 'unlimited' app
	for _, r := range []*resources.Resource{resources.NewResourceFromMap(map[string]resources.Quantity{"first": 40}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 39})} {
		result0 := app0.tryAllocate(r, true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
		assert.Assert(t, result0 != nil, "result0 expected")
		alloc0 := result0.Request
		assert.Assert(t, alloc0 != nil, "alloc0 expected")
		alloc0.SetNodeID(result0.NodeID)
	}

	// consume remainder of space but not quota
	allocs := make([]*Allocation, 0)
	for _, r := range []*resources.Resource{resources.NewResourceFromMap(map[string]resources.Quantity{"first": 28}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 23})} {
		var alloc1 *Allocation
		result1 := app1.tryAllocate(r, true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
		assert.Assert(t, result1 != nil, "result1 expected")
		alloc1 = result1.Request
		assert.Assert(t, result1.Request != nil, "alloc1 expected")
		alloc1.SetNodeID(result1.NodeID)
		allocs = append(allocs, alloc1)
	}

	// on first attempt, should see a reservation since we're after the reservation timeout
	ask3.createTime = ask3.createTime.Add(-10 * time.Second)
	result3 := app2.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 18}), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, result3 != nil, "result3 expected")
	alloc3 := result3.Request
	assert.Assert(t, alloc3 != nil, "alloc3 not expected")
	assert.Equal(t, "node1", result3.NodeID, "wrong node assignment")
	assert.Equal(t, Reserved, result3.ResultType, "expected reservation")
	assert.Assert(t, !allocs[1].IsPreempted(), "alloc2 should not have been preempted")
	err = node1.Reserve(app2, ask3)
	assert.NilError(t, err)

	return iterator, getNode, childQ2, ask3, app1, app2, allocs
}

func createPreemptNodeWithReservationsTestSetup(t *testing.T) (func() NodeIterator, func(NodeID string) *Node, *Allocation, *Allocation, *Application, *Application, []*Allocation) {
	iterator, getNode, childQ2, ask3, app1, _, allocs := createPreemptNodeTestSetup(t)

	app3 := newApplication(appID3, "default", "root.parent.child2")
	app3.SetQueue(childQ2)
	childQ2.AddApplication(app3)
	ask4 := newAllocationAsk("alloc4", appID3, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask4.allowPreemptOther = true
	ask4.priority = math.MaxInt32
	err := app3.AddAllocationAsk(ask4)
	assert.NilError(t, err)

	return iterator, getNode, ask3, ask4, app1, app3, allocs
}

func TestTryAllocatePreemptNodeWithReservations(t *testing.T) {
	iterator, getNode, _, ask4, _, app3, allocs := createPreemptNodeWithReservationsTestSetup(t)

	preemptionAttemptsRemaining := 10

	// pass the time and try again
	ask4.createTime = ask4.createTime.Add(-30 * time.Second)
	defWaitTimeout := reservationWaitTimeout
	reservationWaitTimeout = 60 * time.Second
	defer func() {
		reservationWaitTimeout = defWaitTimeout
	}()
	result3 := app3.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 18}), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, result3 != nil, "result3 expected")
	assert.Equal(t, Reserved, result3.ResultType, "expected reservation")
	alloc3 := result3.Request
	assert.Assert(t, alloc3 != nil, "alloc3 expected")
	assert.Assert(t, allocs[1].IsPreempted(), "alloc2 should have been preempted")
}

func TestTryAllocatePreemptNodeWithReservationsWithHighPriority(t *testing.T) {
	iterator, getNode, _, ask4, _, app3, allocs := createPreemptNodeWithReservationsTestSetup(t)

	// Make ask (preemptor) priority lower than the reserved asks (victims) so that preemption would not yield positive outcome
	ask4.priority = -1

	preemptionAttemptsRemaining := 10

	// pass the time and try again
	ask4.createTime = ask4.createTime.Add(-30 * time.Second)
	defWaitTimeout := reservationWaitTimeout
	reservationWaitTimeout = 60 * time.Second
	defer func() {
		reservationWaitTimeout = defWaitTimeout
	}()
	result3 := app3.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 18}), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, result3 == nil, "result3 expected")

	// Set higher priority than the reserved ask priority
	ask4.priority = math.MaxInt32
	ask4.preemptCheckTime = ask4.preemptCheckTime.Add(-30 * time.Second)
	result4 := app3.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 18}), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, result4 != nil, "result3 expected")
	assert.Equal(t, Reserved, result4.ResultType, "expected reservation")
	alloc3 := result4.Request
	assert.Assert(t, alloc3 != nil, "alloc3 expected")
	assert.Assert(t, allocs[1].IsPreempted(), "alloc2 should have been preempted")
}

// TestTryAllocatePreemptNodeWithReservationsNotPossibleToCancel Ensures reservations cannot be cancelled because of the following constraints:
// 1. Reservation Wait Time out not exceeded
// 2. Reserved allocation has required node set
// 3. Reserved allocation has already marked with "triggered preemption" flag
// Preemption would start yielding results once above constraints has been resolved
func TestTryAllocatePreemptNodeWithReservationsNotPossibleToCancel(t *testing.T) {
	iterator, getNode, ask3, ask4, app1, app3, allocs := createPreemptNodeWithReservationsTestSetup(t)

	// Make reservation ask (victim) as already "triggered preemption" so that it won't be considered for cancellation.
	ask3.MarkTriggeredPreemption()

	// Make reservation ask (victim) as daemon-set so that it won't be considered for cancellation.
	ask5 := newAllocationAsk("alloc5", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask5.requiredNode = "node2"
	err := app1.AddAllocationAsk(ask5)
	assert.NilError(t, err)

	preemptionAttemptsRemaining := 10

	// on first attempt, should see a reservation on node2 since we're after the reservation timeout
	result1 := app1.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 18}), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, result1 != nil, "result expected")
	assert.Equal(t, "node2", result1.NodeID, "wrong node assignment")
	assert.Equal(t, Reserved, result1.ResultType, "expected reservation")
	alloc1 := result1.Request
	assert.Assert(t, alloc1 != nil, "expected allocation result")
	allocs = append(allocs, alloc1)
	assert.Equal(t, "alloc5", alloc1.allocationKey, "wrong allocation assignment")
	// fake the partition processing and reserve the node and app
	err = getNode("node2").Reserve(app1, ask5)
	assert.NilError(t, err, "node reservation should not have failed")
	err = app1.reserveInternal(getNode("node2"), ask5)
	assert.NilError(t, err, "app reservation should not have failed")

	assert.Equal(t, len(getNode("node1").GetReservations()), 1, "reservation expected on node1")

	// Set higher priority than the reserved ask priority but no preemption because reserved ask waiting time not exceeded
	ask4.priority = 1
	result3 := app3.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 18}), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, result3 == nil, "result3 expected")

	// Ensure reserved ask waiting time exceeds
	// Both Node 1 & 2 has reservations, one allocation has required node set and another had marked for "triggered preemption" flag
	// Still, preemption doesn't yield any positive outcome
	ask4.createTime = ask4.createTime.Add(-30 * time.Second)
	defWaitTimeout := reservationWaitTimeout
	reservationWaitTimeout = 0
	defer func() {
		reservationWaitTimeout = defWaitTimeout
	}()
	ask4.preemptCheckTime = ask4.preemptCheckTime.Add(-30 * time.Second)
	result4 := app3.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 18}), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, result4 == nil, "result4 expected")

	// Ensure reserved ask waiting time exceeds
	// Ensure reserved allocation doesn't have required node set and not marked for "triggered preemption" flag
	ask5.requiredNode = ""
	ask3.preemptionTriggered = false
	ask4.createTime = ask4.createTime.Add(-30 * time.Second)
	ask4.preemptCheckTime = ask4.preemptCheckTime.Add(-30 * time.Second)
	result5 := app3.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 18}), true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, result5 != nil, "result5 expected")
	assert.Equal(t, Reserved, result5.ResultType, "expected reservation")
	alloc3 := result5.Request
	assert.Assert(t, alloc3 != nil, "alloc3 expected")
	assert.Assert(t, allocs[1].IsPreempted(), "alloc2 should have been preempted")
}

// assertMaxPriorityConsistent recomputes askMaxPriority (and the pendingPriorities histogram it is
// derived from) via a full scan over sa.requests and compares against the incrementally maintained
// values. This guards the incremental accounting that replaced the old O(N^2)
// updateAskMaxPriority rescan.
func assertMaxPriorityConsistent(t *testing.T, app *Application) {
	t.Helper()
	app.RLock()
	defer app.RUnlock()
	wantMax := configs.MinPriority
	wantHistogram := make(map[int32]int)
	for _, req := range app.requests {
		if req.IsAllocated() {
			continue
		}
		wantMax = max(wantMax, req.GetPriority())
		wantHistogram[req.GetPriority()]++
	}
	assert.Equal(t, app.askMaxPriority, wantMax, "askMaxPriority inconsistent with full scan over requests")
	assert.Equal(t, len(app.pendingPriorities), len(wantHistogram), "pendingPriorities histogram size mismatch")
	for p, count := range wantHistogram {
		assert.Equal(t, app.pendingPriorities[p], count, "pendingPriorities count mismatch for priority %d", p)
	}
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
	assertMaxPriorityConsistent(t, app)

	// p=10 added
	ask1 := newAllocationAskPriority("prio-10", appID1, res, 10)
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err, "ask should have been updated on app")
	assert.Equal(t, app.GetAskMaxPriority(), int32(10), "wrong priority after adding p=10")
	assertMaxPriorityConsistent(t, app)

	// p=5 added
	ask2 := newAllocationAskPriority("prio-5", appID1, res, 5)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "ask should have been added to app")
	assert.Equal(t, app.GetAskMaxPriority(), int32(10), "wrong priority after adding p=5")
	assertMaxPriorityConsistent(t, app)

	// p=15 added
	ask3 := newAllocationAskPriority("prio-15", appID1, res, 15)
	err = app.AddAllocationAsk(ask3)
	assert.NilError(t, err, "ask should have been added to app")
	assert.Equal(t, app.GetAskMaxPriority(), int32(15), "wrong priority after adding p=15")
	assertMaxPriorityConsistent(t, app)

	// p=10 removed
	app.RemoveAllocationAsk(ask1.GetAllocationKey())
	assert.Equal(t, app.GetAskMaxPriority(), int32(15), "wrong priority after removing p=10")
	assertMaxPriorityConsistent(t, app)

	// p=15 removed
	app.RemoveAllocationAsk(ask3.GetAllocationKey())
	assert.Equal(t, app.GetAskMaxPriority(), int32(5), "wrong priority after removing p=15")
	assertMaxPriorityConsistent(t, app)

	// re-add removed asks
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err, "ask should have been added to app")
	err = app.AddAllocationAsk(ask3)
	assert.NilError(t, err, "ask should have been added to app")

	assert.Equal(t, app.GetAskMaxPriority(), int32(15), "wrong priority after re-adding asks")
	assertMaxPriorityConsistent(t, app)

	// update to allocated for p=15
	_, err = app.AllocateAsk(ask3.GetAllocationKey())
	assert.NilError(t, err, "ask should have been updated")
	assert.Equal(t, app.GetAskMaxPriority(), int32(10), "wrong priority after updating p=15 to allocated")
	assertMaxPriorityConsistent(t, app)

	// update to allocated for p=5
	_, err = app.AllocateAsk(ask2.GetAllocationKey())
	assert.NilError(t, err, "ask should have been updated")
	assert.Equal(t, app.GetAskMaxPriority(), int32(10), "wrong priority after updating p=5 to allocated")
	assertMaxPriorityConsistent(t, app)

	// update to unallocated for p=5
	_, err = app.DeallocateAsk(ask2.GetAllocationKey())
	assert.NilError(t, err, "ask should have been updated")
	assert.Equal(t, app.GetAskMaxPriority(), int32(10), "wrong priority after updating p=5 to unallocated")
	assertMaxPriorityConsistent(t, app)

	// update to unallocated for p=15
	_, err = app.DeallocateAsk(ask3.GetAllocationKey())
	assert.NilError(t, err, "ask should have been updated")
	assert.Equal(t, app.GetAskMaxPriority(), int32(15), "wrong priority after updating p=15 to unallocated")
	assertMaxPriorityConsistent(t, app)
}

// TestAddAllocationAskReplaceExistingPendingAsk covers the replace-existing-ask branch of
// AddAllocationAsk: re-submitting an ask under a key that is already tracked and still pending.
// The displaced ask has to leave the pending histogram before addAllocationAskInternal counts the
// replacement, or a single allocation key is counted twice and the priority it was originally
// submitted at never drops out again. The full rescan this replaced could not get that wrong: it
// derived the maximum from sa.requests, which only ever holds one ask per key.
func TestAddAllocationAskReplaceExistingPendingAsk(t *testing.T) {
	app := newApplication(appID1, "default", "root.default")
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	err = app.AddAllocationAsk(newAllocationAskPriority(aKey, appID1, res, 5))
	assert.NilError(t, err, "ask should have been added to app")
	assertMaxPriorityConsistent(t, app)

	// same key, still pending, different priority: the replace branch
	replacement := newAllocationAskPriority(aKey, appID1, res, 3)
	err = app.AddAllocationAsk(replacement)
	assert.NilError(t, err, "ask should have been updated on app")

	app.RLock()
	assert.Equal(t, len(app.pendingPriorities), 1, "pending histogram must only hold the replacement's priority")
	assert.Equal(t, app.pendingPriorities[3], 1, "wrong pending count for the replacement priority")
	app.RUnlock()
	assert.Equal(t, app.GetAskMaxPriority(), int32(3), "wrong priority after replacing p=5 with p=3")
	assertMaxPriorityConsistent(t, app)

	// allocating the only ask must empty the histogram: a double counted key would leave the
	// replaced ask's priority behind.
	_, err = app.AllocateAsk(aKey)
	assert.NilError(t, err, "ask should have been allocated")
	app.RLock()
	assert.Equal(t, len(app.pendingPriorities), 0, "allocating the only ask must empty the pending histogram")
	app.RUnlock()
	assert.Equal(t, app.GetAskMaxPriority(), configs.MinPriority, "wrong priority after allocating the only ask")
	assertMaxPriorityConsistent(t, app)
}

// TestRollbackAllocationAskNotTracked covers RollbackAllocation being handed a "ghost": an ask that
// sa.requests no longer holds while sa.allocations still does. That state is reachable in
// production: removeAsksInternal("") wipes sa.requests and the pending histogram but deliberately
// leaves sa.allocations alone until the shim confirms the releases, so a SCHEDULING_FAILED_ON_RM
// release arriving in that window reaches RollbackAllocation. Since YUNIKORN-3360 the rollback is
// rejected outright when the ask is not in sa.requests, and this test pins that guard: the call must
// return an error and leave the allocation, the allocated resource and the pending histogram
// untouched.
func TestRollbackAllocationAskNotTracked(t *testing.T) {
	setupUGM()
	defer setupUGM()
	app := newApplication(appID1, "default", "root.default")
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAskPriority(aKey, appID1, res, 5)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	_, err = app.AllocateAsk(aKey)
	assert.NilError(t, err, "ask should have been allocated")
	// confirm the allocation so it lands in sa.allocations, which is what RollbackAllocation looks in
	app.AddAllocation(ask)

	// wipe the asks: sa.allocations (and so the rollback target) survives this
	app.RemoveAllocationAsk("")
	assert.Assert(t, app.GetAllocationAsk(aKey) == nil, "ask should no longer be tracked in requests")
	assert.Assert(t, app.IsAccepted() || app.IsRunning(), "app must still be rollback-eligible, is %s", app.CurrentState())

	_, err = app.RollbackAllocation(aKey)
	assert.ErrorContains(t, err, "failed to locate ask", "rollback of an untracked ask should have been rejected")

	app.RLock()
	assert.Assert(t, app.allocations[aKey] == ask, "rejected rollback must leave the confirmed allocation in place")
	assert.Equal(t, len(app.pendingPriorities), 0, "rejected rollback must not change the pending histogram")
	app.RUnlock()
	assert.Assert(t, resources.Equals(app.GetAllocatedResource(), res), "rejected rollback must not change the allocated resource")
	assert.Equal(t, app.GetAskMaxPriority(), configs.MinPriority, "rejected rollback must not change askMaxPriority")
	assertMaxPriorityConsistent(t, app)
}

func TestAskEvents(t *testing.T) {
	app := newApplication(appID1, "default", "root.default")
	// Create event system after new application to avoid new app event.
	events.Init()
	eventSystem := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	eventSystem.StartServiceWithPublisher(false)
	app.disableStateChangeEvents()
	app.resetAppEvents()
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAsk(aKey, appID1, res)

	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err)
	app.RemoveAllocationAsk(ask.allocationKey)
	noEvents := uint64(0)
	err = common.WaitForCondition(10*time.Millisecond, time.Second, func() bool {
		noEvents = eventSystem.Store.CountStoredEvents()
		return noEvents == 3
	})
	assert.NilError(t, err, "expected 3 events, got %d", noEvents)
	records := eventSystem.Store.CollectEvents()
	assert.Equal(t, 3, len(records), "number of events")
	assert.Equal(t, si.EventRecord_APP, records[1].Type)
	assert.Equal(t, si.EventRecord_ADD, records[1].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_REQUEST, records[1].EventChangeDetail)
	assert.Equal(t, si.EventRecord_APP, records[2].Type)
	assert.Equal(t, si.EventRecord_REMOVE, records[2].EventChangeType)
	assert.Equal(t, si.EventRecord_REQUEST_CANCEL, records[2].EventChangeDetail)

	ask2 := newAllocationAsk("alloc-2", appID1, res)
	ask3 := newAllocationAsk("alloc-3", appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err)
	err = app.AddAllocationAsk(ask3)
	assert.NilError(t, err)
	app.removeAsksInternal("", si.EventRecord_REQUEST_TIMEOUT)
	err = common.WaitForCondition(10*time.Millisecond, time.Second, func() bool {
		noEvents = eventSystem.Store.CountStoredEvents()
		return noEvents == 6
	})
	assert.NilError(t, err, "expected 6 events, got %d", noEvents)
	records = eventSystem.Store.CollectEvents()
	refIdsRemoved := make(map[string]int) // order can change due to map iteration
	assert.Equal(t, 6, len(records), "number of events")
	assert.Equal(t, si.EventRecord_APP, records[3].Type)
	assert.Equal(t, si.EventRecord_REMOVE, records[3].EventChangeType)
	assert.Equal(t, si.EventRecord_REQUEST_TIMEOUT, records[3].EventChangeDetail)
	refIdsRemoved[records[3].ReferenceID]++
	assert.Equal(t, si.EventRecord_APP, records[4].Type)
	assert.Equal(t, si.EventRecord_REMOVE, records[4].EventChangeType)
	assert.Equal(t, si.EventRecord_REQUEST_TIMEOUT, records[4].EventChangeDetail)
	refIdsRemoved[records[4].ReferenceID]++
	assert.Equal(t, si.EventRecord_APP, records[5].Type)
	assert.Equal(t, si.EventRecord_REMOVE, records[5].EventChangeType)
	assert.Equal(t, si.EventRecord_REQUEST_TIMEOUT, records[5].EventChangeDetail)
	refIdsRemoved[records[5].ReferenceID]++
	assert.Equal(t, 1, refIdsRemoved["alloc-1"])
	assert.Equal(t, 1, refIdsRemoved["alloc-2"])
	assert.Equal(t, 1, refIdsRemoved["alloc-3"])
}

func TestAllocationEvents(t *testing.T) { //nolint:funlen
	app := newApplication(appID1, "default", "root.default")
	// Create event system after new application to avoid new app event.
	events.Init()
	eventSystem := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	eventSystem.StartServiceWithPublisher(false)
	app.disableStateChangeEvents()
	app.resetAppEvents()
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource with error")
	alloc1 := newAllocation(appID1, nodeID1, res)
	alloc2 := newAllocation(appID1, nodeID1, res)

	// add + remove
	app.AddAllocation(alloc1)
	app.AddAllocation(alloc2)
	app.RemoveAllocation(alloc1.GetAllocationKey(), si.TerminationType_STOPPED_BY_RM)
	app.RemoveAllocation(alloc2.GetAllocationKey(), si.TerminationType_PLACEHOLDER_REPLACED)
	noEvents := uint64(0)
	err = common.WaitForCondition(10*time.Millisecond, time.Second, func() bool {
		noEvents = eventSystem.Store.CountStoredEvents()
		return noEvents == 5
	})
	assert.NilError(t, err, "expected 5 events, got %d", noEvents)
	records := eventSystem.Store.CollectEvents()

	assert.Equal(t, 5, len(records), "number of events")
	assert.Equal(t, si.EventRecord_APP, records[1].Type)
	assert.Equal(t, si.EventRecord_ADD, records[1].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_ALLOC, records[1].EventChangeDetail)
	assert.Equal(t, alloc1.GetAllocationKey(), records[1].ReferenceID)
	assert.Equal(t, "app-1", records[1].ObjectID)
	assert.Equal(t, si.EventRecord_APP, records[2].Type)
	assert.Equal(t, si.EventRecord_ADD, records[2].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_ALLOC, records[2].EventChangeDetail)
	assert.Equal(t, alloc2.GetAllocationKey(), records[2].ReferenceID)
	assert.Equal(t, "app-1", records[2].ObjectID)
	assert.Equal(t, si.EventRecord_APP, records[3].Type)
	assert.Equal(t, si.EventRecord_REMOVE, records[3].EventChangeType)
	assert.Equal(t, si.EventRecord_ALLOC_CANCEL, records[3].EventChangeDetail)
	assert.Equal(t, alloc1.GetAllocationKey(), records[3].ReferenceID)
	assert.Equal(t, "app-1", records[3].ObjectID)
	assert.Equal(t, si.EventRecord_APP, records[4].Type)
	assert.Equal(t, si.EventRecord_REMOVE, records[4].EventChangeType)
	assert.Equal(t, si.EventRecord_ALLOC_REPLACED, records[4].EventChangeDetail)
	assert.Equal(t, alloc2.GetAllocationKey(), records[4].ReferenceID)
	assert.Equal(t, "app-1", records[4].ObjectID)

	// add + replace
	alloc1.placeholder = true
	app.AddAllocation(alloc1)
	app.ReplaceAllocation(alloc1.GetAllocationKey())
	noEvents = 0
	err = common.WaitForCondition(10*time.Millisecond, time.Second, func() bool {
		noEvents = eventSystem.Store.CountStoredEvents()
		return noEvents == 2
	})
	assert.NilError(t, err, "expected 2 events, got %d", noEvents)
	records = eventSystem.Store.CollectEvents()
	assert.Equal(t, 2, len(records), "number of events")
	assert.Equal(t, si.EventRecord_APP, records[0].Type)
	assert.Equal(t, si.EventRecord_ADD, records[0].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_ALLOC, records[0].EventChangeDetail)
	assert.Equal(t, alloc1.GetAllocationKey(), records[0].ReferenceID)
	assert.Equal(t, "app-1", records[0].ObjectID)
	assert.Equal(t, si.EventRecord_APP, records[1].Type)
	assert.Equal(t, si.EventRecord_REMOVE, records[1].EventChangeType)
	assert.Equal(t, si.EventRecord_ALLOC_REPLACED, records[1].EventChangeDetail)
	assert.Equal(t, alloc1.GetAllocationKey(), records[0].ReferenceID)
	assert.Equal(t, "app-1", records[0].ObjectID)

	// add + remove all
	app.AddAllocation(alloc1)
	app.AddAllocation(alloc2)
	app.RemoveAllAllocations()
	err = common.WaitForCondition(10*time.Millisecond, time.Second, func() bool {
		noEvents = eventSystem.Store.CountStoredEvents()
		return noEvents == 4
	})
	assert.NilError(t, err, "expected 4 events, got %d", noEvents)
	records = eventSystem.Store.CollectEvents()
	refIdsRemoved := make(map[string]int) // order can change due to map iteration
	assert.Equal(t, 4, len(records), "number of events")
	assert.Equal(t, si.EventRecord_APP, records[0].Type)
	assert.Equal(t, si.EventRecord_ADD, records[0].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_ALLOC, records[0].EventChangeDetail)
	assert.Equal(t, alloc1.GetAllocationKey(), records[0].ReferenceID)
	assert.Equal(t, "app-1", records[0].ObjectID)
	assert.Equal(t, si.EventRecord_APP, records[1].Type)
	assert.Equal(t, si.EventRecord_ADD, records[1].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_ALLOC, records[1].EventChangeDetail)
	assert.Equal(t, alloc2.GetAllocationKey(), records[1].ReferenceID)
	assert.Equal(t, "app-1", records[1].ObjectID)
	assert.Equal(t, si.EventRecord_APP, records[2].Type)
	assert.Equal(t, si.EventRecord_REMOVE, records[2].EventChangeType)
	assert.Equal(t, si.EventRecord_ALLOC_CANCEL, records[2].EventChangeDetail)
	refIdsRemoved[records[2].ReferenceID]++
	assert.Equal(t, "app-1", records[2].ObjectID)
	assert.Equal(t, si.EventRecord_APP, records[3].Type)
	assert.Equal(t, si.EventRecord_REMOVE, records[3].EventChangeType)
	assert.Equal(t, si.EventRecord_ALLOC_CANCEL, records[3].EventChangeDetail)
	refIdsRemoved[records[3].ReferenceID]++
	assert.Equal(t, "app-1", records[3].ObjectID)
	assert.Equal(t, 1, refIdsRemoved[alloc1.GetAllocationKey()])
	assert.Equal(t, 1, refIdsRemoved[alloc2.GetAllocationKey()])
}

func TestPlaceholderLargerEvent(t *testing.T) {
	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource with error")
	smallerResMap := map[string]string{"memory": "50", "vcores": "10"}
	smallerRes, err := resources.NewResourceFromConf(smallerResMap)
	assert.NilError(t, err, "failed to create resource with error")

	app := newApplication(appID1, "default", "root.default")
	// Create event system after new application to avoid new application event.
	events.Init()
	eventSystem := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	eventSystem.StartServiceWithPublisher(false)
	app.disableStateChangeEvents()
	app.resetAppEvents()
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue
	// smallerRes < res in the same task group, so delta will be -50 memory
	alloc1 := newAllocation(appID1, nodeID1, smallerRes)
	alloc1.placeholder = true
	alloc1.taskGroupName = "testGroup"
	app.AddAllocation(alloc1)
	ask := newAllocationAsk("alloc-0", "app-1", res)
	ask.taskGroupName = "testGroup"
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err)

	app.tryPlaceholderAllocate(func() NodeIterator {
		return nil
	}, func(s string) *Node {
		return nil
	})

	noEvents := uint64(0)
	err = common.WaitForCondition(10*time.Millisecond, time.Second, func() bool {
		noEvents = eventSystem.Store.CountStoredEvents()
		return noEvents == 4
	})
	assert.NilError(t, err, "expected 4 events, got %d", noEvents)
	records := eventSystem.Store.CollectEvents()
	assert.Equal(t, si.EventRecord_REQUEST, records[3].Type)
	assert.Equal(t, si.EventRecord_NONE, records[3].EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, records[3].EventChangeDetail)
	assert.Equal(t, "app-1", records[3].ReferenceID)
}

func TestRequestDoesNotFitQueueEvents(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"memory": "100", "vcores": "10"})
	assert.NilError(t, err)
	headroom, err := resources.NewResourceFromConf(map[string]string{"memory": "0", "vcores": "0"})
	assert.NilError(t, err)
	ask := newAllocationAsk("alloc-0", "app-1", res)
	app := newApplication(appID1, "default", "root.default")
	eventSystem := mock.NewEventSystem()
	ask.askEvents = schedEvt.NewAskEvents(eventSystem)
	app.disableStateChangeEvents()
	app.resetAppEvents()
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue
	sr := sortedRequests{}
	sr.insert(ask)
	app.sortedRequests = sr
	attempts := 0

	// try to allocate
	app.tryAllocate(headroom, true, time.Second, &attempts, nilNodeIterator, nilNodeIterator, nilGetNode)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "app-1", event.ReferenceID)
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, "Request 'alloc-0' does not fit in queue 'root.default' (requested map[memory:100 vcores:10], available map[memory:0 vcores:0])", event.Message)

	// second attempt - no new event
	app.tryAllocate(headroom, true, time.Second, &attempts, nilNodeIterator, nilNodeIterator, nilGetNode)
	assert.Equal(t, 1, len(eventSystem.Events))

	// third attempt with enough headroom - new event
	eventSystem.Reset()
	headroom, err = resources.NewResourceFromConf(map[string]string{"memory": "1000", "vcores": "1000"})
	assert.NilError(t, err)
	app.tryAllocate(headroom, true, time.Second, &attempts, nilNodeIterator, nilNodeIterator, nilGetNode)
	assert.Equal(t, 1, len(eventSystem.Events))
	event = eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "app-1", event.ReferenceID)
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, "Request 'alloc-0' has become schedulable in queue 'root.default'", event.Message)
}

func TestRequestDoesNotFitUserQuotaQueueEvents(t *testing.T) {
	setupUGM()
	// create config with resource limits for "testuser"
	conf := configs.QueueConfig{
		Name:      "root",
		Parent:    true,
		SubmitACL: "*",
		Limits: []configs.Limit{
			{
				Limit: "leaf queue limit",
				Users: []string{
					"testuser",
				},
				MaxResources: map[string]string{
					"memory": "1",
					"vcores": "1",
				},
			},
		},
	}
	err := ugm.GetUserManager().UpdateConfig(conf, "root")
	assert.NilError(t, err)

	res, err := resources.NewResourceFromConf(map[string]string{"memory": "100", "vcores": "10"})
	assert.NilError(t, err)
	headroom, err := resources.NewResourceFromConf(map[string]string{"memory": "1000", "vcores": "1000"})
	assert.NilError(t, err)
	ask := newAllocationAsk("alloc-0", "app-1", res)
	app := newApplication(appID1, "default", "root")
	eventSystem := mock.NewEventSystem()
	ask.askEvents = schedEvt.NewAskEvents(eventSystem)
	app.disableStateChangeEvents()
	app.resetAppEvents()
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue
	sr := sortedRequests{}
	sr.insert(ask)
	app.sortedRequests = sr
	attempts := 0

	// try to allocate
	app.tryAllocate(headroom, true, time.Second, &attempts, nilNodeIterator, nilNodeIterator, nilGetNode)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "app-1", event.ReferenceID)
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, "Request 'alloc-0' exceeds the available user quota (requested map[memory:100 vcores:10], available map[memory:1 vcores:1])", event.Message)

	// second attempt - no new event
	app.tryAllocate(headroom, true, time.Second, &attempts, nilNodeIterator, nilNodeIterator, nilGetNode)
	assert.Equal(t, 1, len(eventSystem.Events))

	// third attempt with enough headroom - new event
	eventSystem.Reset()
	conf.Limits[0].MaxResources = nil
	err = ugm.GetUserManager().UpdateConfig(conf, "root")
	assert.NilError(t, err)
	app.tryAllocate(headroom, true, time.Second, &attempts, nilNodeIterator, nilNodeIterator, nilGetNode)
	assert.Equal(t, 1, len(eventSystem.Events))
	event = eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "app-1", event.ReferenceID)
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, "Request 'alloc-0' fits in the available user quota", event.Message)
}

func TestAllocationFailures(t *testing.T) {
	setupUGM()

	res, err := resources.NewResourceFromConf(map[string]string{"memory": "100", "vcores": "10"})
	assert.NilError(t, err)
	headroom, err := resources.NewResourceFromConf(map[string]string{"memory": "0", "vcores": "0"})
	assert.NilError(t, err)
	ask := newAllocationAsk("alloc-0", "app-1", res)
	app := newApplication(appID1, "default", "root")
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue
	sr := sortedRequests{}
	sr.insert(ask)
	app.sortedRequests = sr
	attempts := 0

	// case #1: not enough queue headroom
	app.tryAllocate(headroom, true, time.Second, &attempts, nilNodeIterator, nilNodeIterator, nilGetNode)
	assert.Equal(t, 1, len(ask.allocLog))
	assert.Equal(t, int32(1), ask.allocLog[NotEnoughQueueQuota].Count)

	// case #2: not enough user quota
	// create config with resource limits for "testuser"
	conf := configs.QueueConfig{
		Name:      "root",
		Parent:    true,
		SubmitACL: "*",
		Limits: []configs.Limit{
			{
				Limit: "leaf queue limit",
				Users: []string{
					"testuser",
				},
				MaxResources: map[string]string{
					"memory": "1",
					"vcores": "1",
				},
			},
		},
	}
	err = ugm.GetUserManager().UpdateConfig(conf, "root")
	assert.NilError(t, err)
	headroom, err = resources.NewResourceFromConf(map[string]string{"memory": "1000", "vcores": "1000"})
	assert.NilError(t, err)
	app.tryAllocate(headroom, true, time.Second, &attempts, nilNodeIterator, nilNodeIterator, nilGetNode)
	assert.Equal(t, 2, len(ask.allocLog))
	assert.Equal(t, int32(1), ask.allocLog[NotEnoughUserQuota].Count)
}

func TestGetOutstandingRequests(t *testing.T) {
	// Create a sample Resource and Allocation
	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource with error")

	allocationAsk1 := newAllocationAsk("alloc-1", "app-1", res)
	allocationAsk2 := newAllocationAsk("alloc-2", "app-1", res)
	allocationAsk1.SetSchedulingAttempted(true)
	allocationAsk2.SetSchedulingAttempted(true)

	// Create an Application instance
	app := &Application{
		ApplicationID: "app-1",
		queuePath:     "default",
	}

	app.user = security.UserGroup{
		User:   "user1",
		Groups: []string{"group1"},
	}

	// Test Case 1: check with no requests
	queueHeadroom, err := resources.NewResourceFromConf(map[string]string{"memory": "250", "vcores": "25"})
	assert.NilError(t, err, "failed to create queue headroom resource with error")
	userHeadroom, err := resources.NewResourceFromConf(map[string]string{"memory": "50", "vcores": "5"})
	assert.NilError(t, err, "failed to create user headroom resource with error")
	var total0 []*Allocation
	resTotal := app.getOutstandingRequests(queueHeadroom, userHeadroom, &total0)
	assert.Assert(t, resources.Equals(resTotal, resources.Zero), "unexpected amount of collected resources %v", resTotal)

	// Set up the Application's sortedRequests with AllocationAsks
	sr := sortedRequests{}
	sr.insert(allocationAsk1)
	sr.insert(allocationAsk2)
	app.sortedRequests = sr

	// Test Case 2: queueHeadroom meets, but userHeadroom does not
	var total1 []*Allocation
	resTotal = app.getOutstandingRequests(queueHeadroom, userHeadroom, &total1)
	assert.Equal(t, 0, len(total1), "expected one outstanding request for TestCase 1")
	assert.Assert(t, resources.Equals(resTotal, resources.Zero), "unexpected amount of collected resources %v", resTotal)

	// Test Case 3: Both queueHeadroom and userHeadroom meet
	queueHeadroom2, err := resources.NewResourceFromConf(map[string]string{"memory": "250", "vcores": "25"})
	assert.NilError(t, err, "failed to create queue headroom resource with error")
	userHeadroom2, err := resources.NewResourceFromConf(map[string]string{"memory": "250", "vcores": "25"})
	assert.NilError(t, err, "failed to create user headroom resource with error")
	var total2 []*Allocation
	resTotal = app.getOutstandingRequests(queueHeadroom2, userHeadroom2, &total2)
	assert.Equal(t, 2, len(total2), "expected two outstanding requests for TestCase 2")
	expectedResTotal, err := resources.NewResourceFromConf(map[string]string{"memory": "200", "vcores": "20"})
	assert.NilError(t, err)
	assert.Assert(t, resources.Equals(resTotal, expectedResTotal), "total collected resources is %v, expected %v", resTotal, expectedResTotal)

	// Test Case 4: queueHeadroom does not meet, but userHeadroom meets
	queueHeadroom3, err := resources.NewResourceFromConf(map[string]string{"memory": "50", "vcores": "5"})
	assert.NilError(t, err, "failed to create queue headroom resource with error")
	userHeadroom3, err := resources.NewResourceFromConf(map[string]string{"memory": "250", "vcores": "25"})
	assert.NilError(t, err, "failed to create user headroom resource with error")
	var total3 []*Allocation
	resTotal = app.getOutstandingRequests(queueHeadroom3, userHeadroom3, &total3)
	assert.Equal(t, 0, len(total3), "expected one outstanding request for TestCase 3")
	assert.Assert(t, resources.Equals(resTotal, resources.Zero), "unexpected amount of collected resources %v", resTotal)

	// Test Case 5: Neither queueHeadroom nor userHeadroom meets
	queueHeadroom4, err := resources.NewResourceFromConf(map[string]string{"memory": "50", "vcores": "5"})
	assert.NilError(t, err, "failed to create queue headroom resource with error")
	userHeadroom4, err := resources.NewResourceFromConf(map[string]string{"memory": "80", "vcores": "8"})
	assert.NilError(t, err, "failed to create user headroom resource with error")
	var total4 []*Allocation
	resTotal = app.getOutstandingRequests(queueHeadroom4, userHeadroom4, &total4)
	assert.Equal(t, 0, len(total4), "expected no outstanding requests for TestCase 4")
	assert.Assert(t, resources.Equals(resTotal, resources.Zero), "unexpected amount of collected resources %v", resTotal)
}

func TestGetOutstandingRequests_NoSchedulingAttempt(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1})

	allocationAsk1 := newAllocationAsk("alloc-1", "app-1", res)
	allocationAsk2 := newAllocationAsk("alloc-2", "app-1", res)
	allocationAsk3 := newAllocationAsk("alloc-3", "app-1", res)
	allocationAsk4 := newAllocationAsk("alloc-4", "app-1", res)
	allocationAsk2.SetSchedulingAttempted(true)
	allocationAsk4.SetSchedulingAttempted(true)
	app := &Application{
		ApplicationID: "app-1",
		queuePath:     "default",
	}
	sr := sortedRequests{}
	sr.insert(allocationAsk1)
	sr.insert(allocationAsk2)
	sr.insert(allocationAsk3)
	sr.insert(allocationAsk4)
	app.sortedRequests = sr

	var total []*Allocation
	headroom := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 10})
	userHeadroom := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 8})
	resTotal := app.getOutstandingRequests(headroom, userHeadroom, &total)
	expectedTotal := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 2})
	assert.Assert(t, resources.Equals(resTotal, expectedTotal), "expected resource %v, but got %v", expectedTotal, resTotal)
	assert.Equal(t, 2, len(total))
	assert.Equal(t, "alloc-2", total[0].allocationKey)
	assert.Equal(t, "alloc-4", total[1].allocationKey)
}

func TestGetOutstandingRequests_RequestTriggeredPreemptionHasRequiredNode(t *testing.T) {
	// Test that we decrease headrooms even if the requests have triggered upscaling or
	// the ask is a DaemonSet pod (requiredNode != "")
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1})

	allocationAsk1 := newAllocationAsk("alloc-1", "app-1", res)
	allocationAsk2 := newAllocationAsk("alloc-2", "app-1", res)
	allocationAsk3 := newAllocationAsk("alloc-3", "app-1", res)
	allocationAsk4 := newAllocationAsk("alloc-4", "app-1", res)
	allocationAsk1.SetSchedulingAttempted(true)
	allocationAsk2.SetSchedulingAttempted(true)
	allocationAsk3.SetSchedulingAttempted(true)
	allocationAsk4.SetSchedulingAttempted(true) // hasn't triggered scaling, no required node --> picked
	allocationAsk1.SetScaleUpTriggered(true)    // triggered scaling, no required node --> not selected
	allocationAsk2.SetScaleUpTriggered(true)    // triggered scaling, has required node --> not selected
	allocationAsk2.SetRequiredNode("node-1")
	allocationAsk3.SetRequiredNode("node-1") // hasn't triggered scaling, has required node --> not selected

	app := &Application{
		ApplicationID: "app-1",
		queuePath:     "default",
	}
	sr := sortedRequests{}
	sr.insert(allocationAsk1)
	sr.insert(allocationAsk2)
	sr.insert(allocationAsk3)
	sr.insert(allocationAsk4)
	app.sortedRequests = sr

	var total []*Allocation
	headroom := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 10})
	userHeadroom := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 8})
	resTotal := app.getOutstandingRequests(headroom, userHeadroom, &total)
	expectedTotal := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1})
	assert.Assert(t, resources.Equals(resTotal, expectedTotal), "expected resource %v, but got %v", expectedTotal, resTotal)
	assert.Equal(t, 1, len(total))
	assert.Equal(t, "alloc-4", total[0].allocationKey)
}

func TestGetOutstandingRequests_AskReplaceable(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1})

	allocationAsk1 := newAllocationAsk("alloc-1", "app-1", res) // replaceable
	allocationAsk2 := newAllocationAsk("alloc-2", "app-1", res) // replaceable
	allocationAsk3 := newAllocationAsk("alloc-3", "app-1", res) // non-replaceable
	allocationAsk1.SetSchedulingAttempted(true)
	allocationAsk2.SetSchedulingAttempted(true)
	allocationAsk3.SetSchedulingAttempted(true)
	allocationAsk1.taskGroupName = "testgroup"
	allocationAsk2.taskGroupName = "testgroup"

	app := &Application{
		ApplicationID: "app-1",
		queuePath:     "default",
	}
	sr := sortedRequests{}
	sr.insert(allocationAsk1)
	sr.insert(allocationAsk2)
	sr.insert(allocationAsk3)
	app.sortedRequests = sr
	app.addPlaceholderData(allocationAsk1)
	app.addPlaceholderData(allocationAsk2)

	var total []*Allocation
	headroom := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 10})
	userHeadroom := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 8})
	resTotal := app.getOutstandingRequests(headroom, userHeadroom, &total)
	expectedTotal := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1})
	assert.Assert(t, resources.Equals(resTotal, expectedTotal), "expected resource %v, but got %v", expectedTotal, resTotal)
	assert.Equal(t, 1, len(total))
	assert.Equal(t, "alloc-3", total[0].allocationKey)
}

func TestGetRateLimitedAppLog(t *testing.T) {
	l := getRateLimitedAppLog()
	assert.Check(t, l != nil)
}

func TestTryAllocateWithReservedHeadRoomChecking(t *testing.T) {
	setupUGM()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("reserved headroom test regression: %v", r)
		}
	}()

	res, err := resources.NewResourceFromConf(map[string]string{"memory": "2G"})
	assert.NilError(t, err, "failed to create basic resource")
	var headRoom *resources.Resource
	headRoom, err = resources.NewResourceFromConf(map[string]string{"memory": "1G"})
	assert.NilError(t, err, "failed to create basic resource")

	app := newApplication(appID1, "default", "root")
	ask := newAllocationAsk(aKey, appID1, res)
	var queue *Queue
	queue, err = createRootQueue(map[string]string{"memory": "1G"})
	assert.NilError(t, err, "queue create failed")
	app.queue = queue
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")

	node1 := newNodeRes(nodeID1, res)
	node2 := newNodeRes(nodeID2, res)
	// reserve that works
	err = app.Reserve(node1, ask)
	assert.NilError(t, err, "reservation should not have failed")

	iter := getNodeIteratorFn(node1, node2)
	result := app.tryReservedAllocate(headRoom, iter)
	assert.Assert(t, result == nil, "result is expected to be nil due to insufficient headroom")
	assert.Equal(t, len(app.reservations), 1)

	// pass the time and try again
	app.reservations[ask.allocationKey].createTime = time.Now().Add(-90 * time.Minute)
	result = app.tryReservedAllocate(headRoom, iter)
	assert.Assert(t, result == nil, "result is expected to be nil due to insufficient headroom")
	assert.Equal(t, len(app.reservations), 0)

	// reset wait timeout
}

// TestTryReservedAllocateRequiredNodeNotCancelledOnTimeout verifies that required node reservations
// are not cancelled even if they exceed the reservationWaitTimeout.
func TestTryReservedAllocateRequiredNodeNotCancelledOnTimeout(t *testing.T) {
	setupUGM()

	res, err := resources.NewResourceFromConf(map[string]string{"memory": "2G"})
	assert.NilError(t, err, "failed to create basic resource")
	headRoom, err := resources.NewResourceFromConf(map[string]string{"memory": "1G"})
	assert.NilError(t, err, "failed to create basic resource")

	app := newApplication(appID1, "default", "root")
	ask := newAllocationAsk(aKey, appID1, res)
	ask.SetRequiredNode(nodeID1)
	queue, err := createRootQueue(map[string]string{"memory": "1G"})
	assert.NilError(t, err, "queue create failed")
	app.queue = queue
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")

	node1 := newNodeRes(nodeID1, res)
	node2 := newNodeRes(nodeID2, res)
	// reserve on the required node
	err = app.Reserve(node1, ask)
	assert.NilError(t, err, "reservation should not have failed")

	iter := getNodeIteratorFn(node1, node2)

	// headroom is insufficient, but reservation should NOT be cancelled (required node)
	result := app.tryReservedAllocate(headRoom, iter)
	assert.Assert(t, result == nil, "result is expected to be nil due to insufficient headroom")
	assert.Equal(t, len(app.reservations), 1, "required node reservation should not be cancelled")

	// set the reservation time to well beyond the timeout
	app.reservations[ask.allocationKey].createTime = time.Now().Add(-90 * time.Minute)

	// even after timeout, required node reservation must NOT be cancelled
	result = app.tryReservedAllocate(headRoom, iter)
	assert.Assert(t, result == nil, "result is expected to be nil due to insufficient headroom")
	assert.Equal(t, len(app.reservations), 1, "required node reservation must not be cancelled on timeout")
}

func TestUpdateRunnableStatus(t *testing.T) {
	app := newApplication(appID0, "default", "root.unknown")
	assert.Assert(t, app.runnableInQueue)
	assert.Assert(t, app.runnableByUserLimit)
	eventSystem := mock.NewEventSystem()
	app.appEvents = schedEvt.NewApplicationEvents(eventSystem)

	// App runnable - no events
	app.updateRunnableStatus(true, true)
	assert.Equal(t, 0, len(eventSystem.Events))

	// App not runnable in queue
	eventSystem.Reset()
	app.updateRunnableStatus(false, true)
	assert.Equal(t, 1, len(eventSystem.Events))
	assert.Equal(t, si.EventRecord_APP_CANNOTRUN_QUEUE, eventSystem.Events[0].EventChangeDetail)
	// Try again - no new events
	app.updateRunnableStatus(false, true)
	assert.Equal(t, 1, len(eventSystem.Events))

	// App becomes runnable in queue
	eventSystem.Reset()
	app.updateRunnableStatus(true, true)
	assert.Equal(t, 1, len(eventSystem.Events))
	assert.Equal(t, si.EventRecord_APP_RUNNABLE_QUEUE, eventSystem.Events[0].EventChangeDetail)

	// Try again - no new events
	app.updateRunnableStatus(true, true)
	assert.Equal(t, 1, len(eventSystem.Events))

	// App not runnable by UG quota
	eventSystem.Reset()
	app.updateRunnableStatus(true, false)
	assert.Equal(t, 1, len(eventSystem.Events))
	assert.Equal(t, si.EventRecord_APP_CANNOTRUN_QUOTA, eventSystem.Events[0].EventChangeDetail)
	// Try again - no new events
	app.updateRunnableStatus(true, false)
	assert.Equal(t, 1, len(eventSystem.Events))

	// App becomes runnable by user quota
	eventSystem.Reset()
	app.updateRunnableStatus(true, true)
	assert.Equal(t, si.EventRecord_APP_RUNNABLE_QUOTA, eventSystem.Events[0].EventChangeDetail)
	// Try again - no new events
	app.updateRunnableStatus(true, true)
	assert.Equal(t, 1, len(eventSystem.Events))

	// Both false
	eventSystem.Reset()
	app.updateRunnableStatus(false, false)
	assert.Equal(t, 2, len(eventSystem.Events))
	assert.Equal(t, si.EventRecord_APP_CANNOTRUN_QUEUE, eventSystem.Events[0].EventChangeDetail)
	assert.Equal(t, si.EventRecord_APP_CANNOTRUN_QUOTA, eventSystem.Events[1].EventChangeDetail)
}

func TestPredicateFailedEvents(t *testing.T) {
	setupUGM()

	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err)
	headroom, err := resources.NewResourceFromConf(map[string]string{"first": "40"})
	assert.NilError(t, err)
	ask := newAllocationAsk("alloc-0", "app-1", res)
	app := newApplication(appID1, "default", "root")
	eventSystem := mock.NewEventSystem()
	ask.askEvents = schedEvt.NewAskEvents(eventSystem)
	app.disableStateChangeEvents()
	app.resetAppEvents()
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue
	sr := sortedRequests{}
	sr.insert(ask)
	app.sortedRequests = sr
	attempts := 0

	mockPlugin := mockCommon.NewPredicatePlugin(true, nil)
	plugins.RegisterSchedulerPlugin(mockPlugin)
	defer plugins.UnregisterSchedulerPlugins()

	app.tryAllocate(headroom, false, time.Second, &attempts, func() NodeIterator {
		return &testIterator{}
	}, nilNodeIterator, nilGetNode)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "app-1", event.ReferenceID)
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, "Unschedulable request 'alloc-0': fake predicate plugin failed (2x); ", event.Message)
}

func TestRequiredNodePreemption(t *testing.T) {
	// tests successful RequiredNode (DaemonSet) preemption
	app := newApplication(appID0, "default", "root.default")
	var releaseEvents []*rmevent.RMReleaseAllocationEvent
	app.rmEventHandler = &mockAppEventHandler{
		callback: func(ev interface{}) {
			if rmEvent, ok := ev.(*rmevent.RMReleaseAllocationEvent); ok {
				releaseEvents = append(releaseEvents, rmEvent)
				go func() {
					rmEvent.Channel <- &rmevent.Result{
						Succeeded: true,
					}
				}()
			}
		},
	}
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 20})
	node.nodeEvents = schedEvt.NewNodeEvents(mock.NewEventSystemDisabled())
	iterator := getNodeIteratorFn(node)
	getNode := func(nodeID string) *Node {
		return node
	}
	appQueueMapping := NewAppQueueMapping()

	// set queue
	rootQ, err := createRootQueue(map[string]string{"first": "20"})
	assert.NilError(t, err)
	childQ, err := createManagedQueueWithAppQueueMapping(rootQ, "default", false, map[string]string{"first": "20"}, appQueueMapping)
	assert.NilError(t, err)
	app.SetQueue(childQ)
	appQueueMapping.AddAppQueueMapping(app.ApplicationID, childQ)

	// add an ask
	mockEvents := mock.NewEventSystem()
	askRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	ask1 := newAllocationAsk("ask-1", "app-1", askRes)
	ask1.askEvents = schedEvt.NewAskEvents(mockEvents)
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err, "could not add ask-1")
	preemptionAttemptsRemaining := 0

	// allocate ask
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})
	result := app.tryAllocate(headRoom, true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Equal(t, result.ResultType, Allocated, "could not allocate ask-1")
	assert.Equal(t, result.Request.allocationKey, "ask-1", "unexpected allocation key")

	// add ask2 with required node
	ask2 := newAllocationAsk("ask-2", "app-1", askRes)
	ask2.askEvents = schedEvt.NewAskEvents(mockEvents)
	ask2.requiredNode = nodeID1
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "could not add ask-2")

	// try to allocate ask2 with node being full - expect a reservation
	result = app.tryAllocate(headRoom, true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Equal(t, result.ResultType, Reserved, "allocation result is not reserved")
	assert.Equal(t, result.Request.allocationKey, "ask-2", "unexpected allocation key")
	err = app.Reserve(node, ask2)
	assert.NilError(t, err, "reservation failed")

	// preemption
	assert.Assert(t, app.tryReservedAllocate(headRoom, iterator) == nil, "unexpected result from reserved allocation")
	assert.Assert(t, ask1.IsPreempted(), "ask1 has not been preempted")
	assert.Assert(t, ask2.HasTriggeredPreemption(), "ask2 has not triggered preemption")
	assert.Equal(t, 1, len(releaseEvents), "unexpected number of release events")
	assert.Equal(t, 1, len(releaseEvents[0].ReleasedAllocations), "unexpected number of release allocations")
	assert.Equal(t, "ask-1", releaseEvents[0].ReleasedAllocations[0].AllocationKey, "allocation key")

	// 2nd attempt - no preemption this time
	releaseEvents = nil
	assert.Assert(t, app.tryReservedAllocate(headRoom, iterator) == nil, "unexpected result from reserved allocation")
	assert.Assert(t, releaseEvents == nil, "unexpected release event")

	// check for preemption related events
	for _, event := range mockEvents.Events {
		assert.Assert(t, !strings.Contains(strings.ToLower(event.Message), "preemption"), "received a preemption related event")
	}
}

func TestRequiredNodePreemptionWithPredicates(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 20})
	iterator := getNodeIteratorFn(node)
	getNode := func(nodeID string) *Node {
		return node
	}
	appQueueMapping := NewAppQueueMapping()

	// set queue
	rootQ, err := createRootQueue(map[string]string{"first": "20"})
	assert.NilError(t, err)
	childQ, err := createManagedQueueWithAppQueueMapping(rootQ, "default", false, map[string]string{"first": "20"}, appQueueMapping)
	assert.NilError(t, err)

	// register predicate handler
	preemptions := []mockCommon.Preemption{
		mockCommon.NewPreemption(true, "ask-2", nodeID1, []string{"ask-1"}, 0, 0),
	}
	wrongNodes := make(map[string]int, 1)
	wrongNodes[nodeID2] = 10
	rightNodes := make(map[string]int, 1)
	rightNodes[nodeID1] = 10
	tests := []struct {
		name            string
		mockPlugin      *mockCommon.PreemptionPredicatePlugin
		result          bool
		mockPluginError error
	}{
		{"nil plugin, so no predicate checks", nil, true, nil},
		{"prefilter fails", mockCommon.NewPreemptionPredicatePlugin(preemptions, nil, true, false), false, errors.New("fail")},
		{"prefilter passes but none of the node from iterator is available in feasible nodes", mockCommon.NewPreemptionPredicatePlugin(preemptions, wrongNodes, false, false), false, nil},
		{"prefilter pass with expected feasible nodes, filter fails", mockCommon.NewPreemptionPredicatePlugin(preemptions, rightNodes, false, true), false, errors.New("fail")},
		{"both prefilter and filter passes with correct feasible nodes", mockCommon.NewPreemptionPredicatePlugin(preemptions, rightNodes, false, false), true, nil},
		{"both prefilter and filter passes with empty feasible nodes", mockCommon.NewPreemptionPredicatePlugin(preemptions, nil, false, false), true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newApplication(appID1, "default", "root.default")
			app.SetQueue(childQ)
			childQ.AddApplication(app)
			appQueueMapping.AddAppQueueMapping(app.ApplicationID, childQ)

			// add an ask
			askRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
			ask1 := newAllocationAsk("ask-1", "app-1", askRes)
			err = app.AddAllocationAsk(ask1)
			assert.NilError(t, err, "could not add ask-1")
			preemptionAttemptsRemaining := 1

			// allocate ask
			headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})
			result := app.tryAllocate(headRoom, true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
			assert.Equal(t, result.ResultType, Allocated, "could not allocate ask-1")
			assert.Equal(t, result.Request.allocationKey, "ask-1", "unexpected allocation key")

			// add ask2 with required node
			ask2 := newAllocationAsk("ask-2", "app-1", askRes)
			ask2.requiredNode = nodeID1
			err = app.AddAllocationAsk(ask2)
			assert.NilError(t, err, "could not add ask-2")

			// try to allocate ask2 with node being full - expect a reservation
			result = app.tryAllocate(headRoom, true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
			assert.Equal(t, result.ResultType, Reserved, "allocation result is not reserved")
			assert.Equal(t, result.Request.allocationKey, "ask-2", "unexpected allocation key")
			err = app.Reserve(node, ask2)
			assert.NilError(t, err, "reservation failed")

			if tt.mockPlugin != nil {
				plugins.RegisterSchedulerPlugin(tt.mockPlugin)
			}
			app.tryReservedAllocate(headRoom, iterator)
			if tt.result {
				assert.Assert(t, ask1.IsPreempted(), "ask1 has not been preempted")
				assert.Assert(t, ask2.HasTriggeredPreemption(), "ask2 has not triggered preemption")
			} else {
				assert.Assert(t, !ask1.IsPreempted(), "ask1 preempted")
				assert.Assert(t, !ask2.HasTriggeredPreemption(), "ask2 triggered preemption")
			}
			if tt.mockPluginError != nil {
				assert.ErrorContains(t, tt.mockPlugin.GetPredicateError(), tt.mockPluginError.Error())
			}
			// reset
			resetQ(t, childQ)
			resetNode(node)
			appQueueMapping.RemoveAppQueueMapping(app.ApplicationID)
			if tt.mockPlugin != nil {
				plugins.UnregisterSchedulerPlugins()
			}
		})
	}
}

func TestRequiredNodePreemptionFailed(t *testing.T) {
	// tests RequiredNode (DaemonSet) preemption where the victim pod has a high priority, hence preemption is not possible
	app := newApplication(appID0, "default", "root.default")
	var releaseEvents []*rmevent.RMReleaseAllocationEvent
	app.rmEventHandler = &mockAppEventHandler{
		callback: func(ev interface{}) {
			if rmEvent, ok := ev.(*rmevent.RMReleaseAllocationEvent); ok {
				releaseEvents = append(releaseEvents, rmEvent)
				go func() {
					rmEvent.Channel <- &rmevent.Result{
						Succeeded: true,
					}
				}()
			}
		},
	}
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 20})
	node.nodeEvents = schedEvt.NewNodeEvents(mock.NewEventSystemDisabled())
	iterator := getNodeIteratorFn(node)
	getNode := func(nodeID string) *Node {
		return node
	}

	// set queue
	rootQ, err := createRootQueue(map[string]string{"first": "20"})
	assert.NilError(t, err)
	childQ, err := createManagedQueue(rootQ, "default", false, map[string]string{"first": "20"})
	assert.NilError(t, err)
	app.SetQueue(childQ)

	// add an ask with high priority
	mockEvents := mock.NewEventSystem()
	askRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	ask1 := newAllocationAsk("ask-1", "app-1", askRes)
	ask1.askEvents = schedEvt.NewAskEvents(mockEvents)
	ask1.priority = 1000
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err, "could not add ask-1")
	preemptionAttemptsRemaining := 0

	// allocate ask
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})
	result := app.tryAllocate(headRoom, true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Equal(t, result.ResultType, Allocated, "could not allocate ask-1")
	assert.Equal(t, result.Request.allocationKey, "ask-1", "unexpected allocation key")

	// add ask2 with required node
	ask2 := newAllocationAsk("ask-2", "app-1", askRes)
	ask2.askEvents = schedEvt.NewAskEvents(mockEvents)
	ask2.requiredNode = nodeID1
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "could not add ask-2")

	// try to allocate ask2 with node being full - expect a reservation
	result = app.tryAllocate(headRoom, true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Equal(t, result.ResultType, Reserved, "allocation result is not reserved")
	assert.Equal(t, result.Request.allocationKey, "ask-2", "unexpected allocation key")
	err = app.Reserve(node, ask2)
	assert.NilError(t, err, "reservation failed")

	// try preemption - should not succeed
	assert.Assert(t, app.tryReservedAllocate(headRoom, iterator) == nil, "unexpected result from reserved allocation")
	assert.Assert(t, !ask1.IsPreempted(), "unexpected preemption of ask1")
	assert.Assert(t, !ask2.HasTriggeredPreemption(), "unexpected preemption triggered from ask2")
	assert.Equal(t, 0, len(releaseEvents), "unexpected number of release events")
	// check for events
	noEvents := 0
	var requestEvt *si.EventRecord
	for _, event := range mockEvents.Events {
		if event.Type == si.EventRecord_REQUEST && strings.Contains(strings.ToLower(event.Message), "preemption") {
			noEvents++
			requestEvt = event
		}
	}
	assert.Equal(t, 1, noEvents, "unexpected number of REQUEST events")
	assert.Equal(t, "Unschedulable request 'ask-2' with required node 'node-1', no preemption victim found", requestEvt.Message)
	assert.Equal(t, 1, len(ask2.allocLog), "unexpected number of entries in the allocation log")
	assert.Equal(t, int32(1), ask2.allocLog[common.NoVictimForRequiredNode].Count, "incorrect number of entry count")
	assert.Equal(t, common.NoVictimForRequiredNode, ask2.allocLog[common.NoVictimForRequiredNode].Message, "unexpected log message")

	// check counting & event throttling
	assert.Assert(t, app.tryReservedAllocate(headRoom, iterator) == nil, "unexpected result from reserved allocation")
	assert.Assert(t, app.tryReservedAllocate(headRoom, iterator) == nil, "unexpected result from reserved allocation")
	assert.Assert(t, app.tryReservedAllocate(headRoom, iterator) == nil, "unexpected result from reserved allocation")
	assert.Equal(t, 1, noEvents, "unexpected number of REQUEST events")
	assert.Equal(t, int32(4), ask2.allocLog[common.NoVictimForRequiredNode].Count, "incorrect number of entry count")
}

type testIterator struct{}

func (testIterator) ForEachNode(fn func(*Node) bool) {
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 20})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 20})
	fn(node1)
	fn(node2)
}

func TestGetMaxResourceFromTag(t *testing.T) {
	app := newApplication(appID0, "default", "root.unknown")
	testGetResourceFromTag(t, siCommon.AppTagNamespaceResourceQuota, app.tags, app.GetMaxResource)
}

func TestGuaranteedResourceFromTag(t *testing.T) {
	app := newApplication(appID0, "default", "root.unknown")
	testGetResourceFromTag(t, siCommon.AppTagNamespaceResourceGuaranteed, app.tags, app.GetGuaranteedResource)
}

func testGetResourceFromTag(t *testing.T, tagName string, tags map[string]string, getResource func() *resources.Resource) {
	assert.Equal(t, 0, len(tags), "tags are not empty")

	// no value for tag
	res := getResource()
	assert.Assert(t, res == nil, "unexpected resource")

	// empty value
	tags[tagName] = ""
	res = getResource()
	assert.Assert(t, res == nil, "unexpected resource")

	// valid value
	tags[tagName] = "{\"resources\":{\"vcore\":{\"value\":111}}}"
	res = getResource()
	assert.Assert(t, res != nil)
	assert.Equal(t, 1, len(res.Resources))
	assert.Equal(t, resources.Quantity(111), res.Resources["vcore"])

	// zero
	tags[tagName] = "{\"resources\":{\"vcore\":{\"value\":0}}}"
	res = getResource()
	assert.Assert(t, res == nil)

	// negative
	tags[tagName] = "{\"resources\":{\"vcore\":{\"value\":-12}}}"
	res = getResource()
	assert.Assert(t, res == nil, "unexpected resource")

	// invalid value
	tags[tagName] = "{xyz}"
	res = getResource()
	assert.Assert(t, res == nil, "unexpected resource")
}

func (sa *Application) addPlaceholderDataWithLocking(ask *Allocation) {
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

func (sa *Application) disableStateChangeEvents() {
	sa.Lock()
	defer sa.Unlock()
	sa.sendStateChangeEvents = false
}

func (sa *Application) resetAppEvents() {
	sa.Lock()
	defer sa.Unlock()
	sa.appEvents = schedEvt.NewApplicationEvents(events.GetEventSystem())
}

func TestGetUint64Tag(t *testing.T) {
	app := &Application{
		tags: map[string]string{
			"validUintTag":    "12345",
			"negativeUintTag": "-12345",
			"invalidUintTag":  "not-a-number",
			"emptyUintTag":    "",
		},
	}

	tests := []struct {
		name     string
		tag      string
		expected uint64
	}{
		{"Valid uint64 tag", "validUintTag", uint64(12345)},
		{"Negative uint64 tag", "negativeUintTag", uint64(0)},
		{"Invalid uint64 tag", "invalidUintTag", uint64(0)},
		{"Empty tag", "emptyUintTag", uint64(0)},
		{"Non-existent tag", "nonExistentTag", uint64(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := app.getUint64Tag(tt.tag)
			assert.Equal(t, tt.expected, result)
		})
	}
}

type mockAppEventHandler struct {
	callback func(ev interface{})
}

func (m mockAppEventHandler) HandleEvent(ev interface{}) {
	m.callback(ev)
}

func TestApplication_canAllocationReserve(t *testing.T) {
	res := resources.NewResource()
	tests := []struct {
		name    string
		alloc   *Allocation
		wantErr bool
	}{
		{"new", newAllocationWithKey(aKey, appID1, "", res), false},
		{"allocated", newAllocationWithKey(aKey2, appID1, nodeID1, res), true},
		{"duplicate", newAllocationWithKey(aKey3, appID1, "", res), true},
	}
	app := newApplication(appID0, "default", "root.unknown")
	app.reservations[aKey3] = &reservation{
		nodeID:   nodeID1,
		allocKey: aKey,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := app.canAllocationReserve(tt.alloc); (err != nil) != tt.wantErr {
				t.Errorf("canAllocationReserve() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTryPlaceHolderAllocateNoPlaceHolders(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{nodeID1: node}
	iterator := getNodeIteratorFn(node)
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}

	app := newApplication(appID0, "default", "root.default")

	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAsk(aKey, appID0, res)
	ask.taskGroupName = tg1
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")

	result := app.tryPlaceholderAllocate(iterator, getNode)
	assert.Assert(t, result == nil, "result should be nil since there are no placeholders to allocate")
}

func TestTryPlaceHolderAllocateSmallerRequest(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{nodeID1: node}
	iterator := getNodeIteratorFn(node)
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}

	app := newApplication(appID0, "default", "root.default")

	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ph := newPlaceholderAlloc(appID0, nodeID1, res, tg1)
	app.AddAllocation(ph)
	app.addPlaceholderData(ph)
	assertPlaceholderData(t, app, tg1, 1, 0, 0, res)

	// allocation request is smaller than placeholder
	smallerRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	ask := newAllocationAsk(aKey, appID0, smallerRes)
	ask.taskGroupName = tg1
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")

	result := app.tryPlaceholderAllocate(iterator, getNode)
	assert.Assert(t, result != nil, "result should not be nil since the ask is smaller than the placeholder")
	assert.Equal(t, Replaced, result.ResultType, "result type should be Replaced")
	assert.Equal(t, nodeID1, result.NodeID, "result should be on the same node as placeholder")
	assert.Equal(t, ask, result.Request, "result should contain the ask")
	assert.Equal(t, ph, result.Request.GetRelease(), "real allocation should link to placeholder")
	assert.Equal(t, result.Request, ph.GetRelease(), "placeholder should link to real allocation")
	// placeholder data remains unchanged until RM confirms the replacement
	assertPlaceholderData(t, app, tg1, 1, 0, 0, res)
}

func TestTryPlaceHolderAllocateLargerRequest(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{nodeID1: node}
	iterator := getNodeIteratorFn(node)
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}

	app := newApplication(appID0, "default", "root.default")

	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ph := newPlaceholderAlloc(appID0, nodeID1, res, tg1)
	app.AddAllocation(ph)
	app.addPlaceholderData(ph)
	assertPlaceholderData(t, app, tg1, 1, 0, 0, res)

	// allocation request is larger than placeholder
	largerRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	ask := newAllocationAsk(aKey, appID0, largerRes)
	ask.taskGroupName = tg1
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")

	result := app.tryPlaceholderAllocate(iterator, getNode)
	assert.Assert(t, result == nil, "result should be nil since the ask is larger than the placeholder")
	assert.Assert(t, ph.IsReleased(), "placeholder should have been released")
	// placeholder data remains unchanged until RM confirms the release
	assertPlaceholderData(t, app, tg1, 1, 0, 0, res)
}

func TestTryPlaceHolderAllocateDifferentTaskGroups(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{nodeID1: node}
	iterator := getNodeIteratorFn(node)
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}

	app := newApplication(appID0, "default", "root.default")

	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ph := newPlaceholderAlloc(appID0, nodeID1, res, tg1)
	app.AddAllocation(ph)
	app.addPlaceholderData(ph)
	assertPlaceholderData(t, app, tg1, 1, 0, 0, res)

	// allocation request has a different task group
	ask := newAllocationAsk(aKey, appID0, res)
	ask.taskGroupName = tg2
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")

	result := app.tryPlaceholderAllocate(iterator, getNode)
	assert.Assert(t, result == nil, "result should be nil since the ask has a different task group")
}

func TestTryPlaceHolderAllocateDifferentNodes(t *testing.T) {
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 5})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{nodeID1: node1, nodeID2: node2}
	iterator := getNodeIteratorFn(node1, node2)
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}

	app := newApplication(appID0, "default", "root.default")

	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ph := newPlaceholderAlloc(appID0, nodeID1, res, tg1)
	app.AddAllocation(ph)
	app.addPlaceholderData(ph)
	assertPlaceholderData(t, app, tg1, 1, 0, 0, res)

	// predicate check fails on node1 and passes on node2
	mockPlugin := mockCommon.NewPredicatePlugin(false, map[string]int{nodeID1: 0})
	plugins.RegisterSchedulerPlugin(mockPlugin)
	defer plugins.UnregisterSchedulerPlugins()

	// should allocate on node2
	ask := newAllocationAsk(aKey, appID0, res)
	ask.taskGroupName = tg1
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")

	result := app.tryPlaceholderAllocate(iterator, getNode)
	assert.Assert(t, result != nil, "result should not be nil")
	assert.Equal(t, Replaced, result.ResultType, "result type should be Replaced")
	assert.Equal(t, nodeID2, result.NodeID, "result should be on node2")
	assert.Equal(t, ask, result.Request, "result should contain the ask")
	assert.Equal(t, ph, result.Request.GetRelease(), "real allocation should link to placeholder")
	assert.Equal(t, result.Request, ph.GetRelease(), "placeholder should link to real allocation")
	// placeholder data remains unchanged until RM confirms the replacement
	assertPlaceholderData(t, app, tg1, 1, 0, 0, res)
}

// revertTriggerPredicatePlugin is a test-local predicate plugin whose Predicates call has the side
// effect of marking a specific placeholder as preempted, then returning nil (i.e. the predicate
// check itself still "passes"). This is used to reach the narrow window in tryPlaceholderAllocate
// between the loop's IsPreempted() guard and the ph.SetReleased(true) call, so that SetReleased
// fails and the revert path is exercised (see TestTryPlaceHolderAllocateRevertsOnPreemptedPlaceholder).
type revertTriggerPredicatePlugin struct {
	mockCommon.ResourceManagerCallback
	ph *Allocation
}

func (p *revertTriggerPredicatePlugin) Predicates(_ *si.PredicatesArgs) error {
	_ = p.ph.MarkPreempted() //nolint:errcheck
	return nil
}

// TestTryPlaceHolderAllocateRevertsOnPreemptedPlaceholder drives tryPlaceholderAllocate down the
// path where ph.SetReleased(true) fails because the placeholder became preempted in between the
// loop's own IsPreempted() guard and the SetReleased(true) call. This exercises the FIRST revert
// branch inside the phAllocs loop (injected via node.preReserveConditions): sa.deallocateAsk(request),
// ClearRelease() on both sides, and continue.
//
// Because ph is registered only at the Application level (app.AddAllocation/addPlaceholderData) and
// never actually consumes capacity on the *Node object itself (matching the pattern of every other
// TestTryPlaceHolderAllocate* test in this file, none of which call node.AddAllocation(ph) either),
// the node's tracked available resources are unaffected by ph and still show room for reqFit. As a
// result, once the first branch reverts and falls through to the fallback ForEachNode retry
// (phFit/reqFit, using node.preAllocateCheck/preAllocateConditions), that retry also finds room,
// proceeds to TryAddAllocation + allocateAsk + SetReleased(true) on the SAME still-preempted ph, and
// that SetReleased(true) call fails again for the same reason - so the SECOND/fallback revert branch
// (node.RemoveAllocation, deallocateAsk, ClearRelease on both sides) is exercised too, verified by the
// log line "allocation is already preempted, so not proceeding further and reverting to old state"
// appearing twice when run with -v. The function then returns nil with all state reverted, which is
// exactly what the assertions below check.
func TestTryPlaceHolderAllocateRevertsOnPreemptedPlaceholder(t *testing.T) {
	// node capacity equals the placeholder's (and the real ask's) resource size; this keeps the
	// scenario minimal (single node, single ph, single ask) while both revert branches above still
	// get exercised, since node-level capacity tracking is independent of the ph's Application-level
	// bookkeeping in this test setup.
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{nodeID1: node}
	iterator := getNodeIteratorFn(node)
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}

	app := newApplication(appID0, "default", "root.default")

	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ph := newPlaceholderAlloc(appID0, nodeID1, res, tg1)
	app.AddAllocation(ph)
	app.addPlaceholderData(ph)
	assertPlaceholderData(t, app, tg1, 1, 0, 0, res)

	// same size as the placeholder so we go down the main swap path, not the "placeholder is
	// larger, cancel it" branch.
	ask := newAllocationAsk(aKey, appID0, res)
	ask.taskGroupName = tg1
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")

	// register a plugin whose Predicates call marks ph preempted right before SetReleased(true)
	// is invoked, forcing that call to fail and the revert path to run.
	plugin := &revertTriggerPredicatePlugin{ph: ph}
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result := app.tryPlaceholderAllocate(iterator, getNode)
	assert.Assert(t, result == nil, "result should be nil: both the first swap attempt and the fallback retry failed SetReleased(true) on the still-preempted placeholder and reverted")
	assert.Assert(t, ph.IsPreempted(), "placeholder should remain marked preempted (side effect stuck)")
	assert.Assert(t, !ph.IsReleased(), "placeholder should not be released: the failing SetReleased(true) call never set it")
	assert.Assert(t, !ask.IsAllocated(), "ask should have been reverted back to pending by deallocateAsk")
	assert.Assert(t, ask.GetRelease() == nil, "ask's release link should have been cleared by the revert")
	assert.Assert(t, ph.GetRelease() == nil, "placeholder's release link should have been cleared by the revert")

	assertMaxPriorityConsistent(t, app)
}

func TestTryNodesNoReserve(t *testing.T) {
	app := newApplication(appID0, "default", "root.default")

	queue, err := createRootQueue(map[string]string{"first": "5"})
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask := newAllocationAsk(aKey, appID0, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")

	// reserve the allocation on node1
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 5})
	err = app.Reserve(node1, ask)
	assert.NilError(t, err, "reservation failed")

	// case 1: node is the reserved node
	iterator := getNodeIteratorFn(node1)
	result := app.tryNodesNoReserve(ask, iterator(), node1.NodeID)
	assert.Assert(t, result == nil, "result should be nil since node1 is the reserved node")

	// case 2: node is unschedulable
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 5})
	node2.schedulable = false
	iterator = getNodeIteratorFn(node2)
	result = app.tryNodesNoReserve(ask, iterator(), node1.NodeID)
	assert.Assert(t, result == nil, "result should be nil since node2 is unschedulable")

	// case 3: node does not have enough resources
	node3 := newNode(nodeID3, map[string]resources.Quantity{"first": 1})
	iterator = getNodeIteratorFn(node3)
	result = app.tryNodesNoReserve(ask, iterator(), node1.NodeID)
	assert.Assert(t, result == nil, "result should be nil since node3 does not have enough resources")

	// case 4: node fails predicate
	mockPlugin := mockCommon.NewPredicatePlugin(false, map[string]int{nodeID4: 1})
	plugins.RegisterSchedulerPlugin(mockPlugin)
	defer plugins.UnregisterSchedulerPlugins()
	node4 := newNode(nodeID4, map[string]resources.Quantity{"first": 5})
	iterator = getNodeIteratorFn(node4)
	result = app.tryNodesNoReserve(ask, iterator(), node1.NodeID)
	assert.Assert(t, result == nil, "result should be nil since node4 fails predicate")

	// case 5: success
	node5 := newNode(nodeID5, map[string]resources.Quantity{"first": 5})
	iterator = getNodeIteratorFn(node5)
	result = app.tryNodesNoReserve(ask, iterator(), node1.NodeID)
	assert.Assert(t, result != nil, "result should not be nil")
	assert.Equal(t, node5.NodeID, result.NodeID, "result should be on node5")
	assert.Equal(t, result.ResultType, AllocatedReserved, "result type should be AllocatedReserved")
	assert.Equal(t, result.ReservedNodeID, node1.NodeID, "reserved node should be node1")
}

func TestAppSubmissionTime(t *testing.T) {
	app := newApplication(appID0, "default", "root.default")
	queue, err := createRootQueue(map[string]string{"first": "5"})
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	// asks
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask1 := newAllocationAsk(aKey, appID1, res)
	ask1.createTime = time.Unix(0, 100)
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err)
	ask2 := newAllocationAsk(aKey2, appID1, res)
	ask2.createTime = time.Unix(0, 200)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err)
	assert.Equal(t, app.submissionTime, time.Unix(0, 100), "app submission time is not set properly")
	ask3 := newAllocationAsk(aKey3, appID1, res)
	ask3.createTime = time.Unix(0, 50)
	err = app.AddAllocationAsk(ask3)
	assert.NilError(t, err)
	assert.Equal(t, app.submissionTime, time.Unix(0, 50), "app submission time is not set properly")

	// allocations
	alloc1 := newAllocation(appID1, nodeID1, res)
	alloc1.createTime = time.Unix(0, 60)
	app.AddAllocation(alloc1)
	assert.Equal(t, app.submissionTime, time.Unix(0, 50), "app submission time is not set properly")

	alloc2 := newAllocation(appID1, nodeID1, res)
	alloc2.createTime = time.Unix(0, 30)
	app.AddAllocation(alloc2)
	assert.Equal(t, app.submissionTime, time.Unix(0, 30), "app submission time is not set properly")
}

// TestGetAllocationNodeID verifies the node ID lookup for confirmed allocations.
func TestGetAllocationNodeID(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")

	// Unknown key returns empty string.
	assert.Equal(t, "", app.GetAllocationNodeID("nonexistent"), "unknown key should return empty string")

	// Empty key returns empty string.
	assert.Equal(t, "", app.GetAllocationNodeID(""), "empty key should return empty string")

	// Existing allocation returns the correct node ID.
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	alloc := newAllocationWithKey(aKey, appID1, nodeID1, res)
	app.allocations[aKey] = alloc
	assert.Equal(t, nodeID1, app.GetAllocationNodeID(aKey), "existing allocation should return its node ID")

	// A second allocation with a different node returns its own node ID independently.
	alloc2 := newAllocationWithKey(aKey2, appID1, nodeID2, res)
	app.allocations[aKey2] = alloc2
	assert.Equal(t, nodeID2, app.GetAllocationNodeID(aKey2), "second allocation should return its own node ID")
	assert.Equal(t, nodeID1, app.GetAllocationNodeID(aKey), "first allocation node ID should be unchanged")

	// After removing an allocation, the key returns empty string again.
	delete(app.allocations, aKey)
	assert.Equal(t, "", app.GetAllocationNodeID(aKey), "removed allocation should return empty string")
}

// TestRollbackAllocationInvalidState verifies that RollbackAllocation returns an error
// when the application is not in Accepted or Running state.
func TestRollbackAllocationInvalidState(t *testing.T) {
	setupUGM()
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})

	for _, state := range []string{New.String(), Completing.String(), Completed.String(), Rejected.String(), Failed.String()} {
		app := newApplication(appID1, "default", "root.unknown")
		app.queue = queue
		app.SetState(state)
		// inject a fake ask and allocation so the state check is the only failure path
		ask := newAllocationAsk(aKey, appID1, res)
		app.requests[aKey] = ask
		alloc := newAllocationWithKey(aKey, appID1, nodeID1, res)
		app.allocations[aKey] = alloc

		_, rollbackErr := app.RollbackAllocation(aKey)
		assert.Assert(t, rollbackErr != nil, "expected error for state %s", state)
		assert.Assert(t, strings.Contains(rollbackErr.Error(), state), "error should mention the invalid state %q, got: %s", state, rollbackErr.Error())
	}
}

// TestRollbackAllocationAskNotFound verifies that RollbackAllocation returns an error
// when the ask key does not exist in the application's request map.
func TestRollbackAllocationAskNotFound(t *testing.T) {
	setupUGM()
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app := newApplication(appID1, "default", "root.unknown")
	app.queue = queue
	app.SetState(Accepted.String())

	_, err = app.RollbackAllocation("nonexistent-key")
	assert.Assert(t, err != nil, "expected error for missing ask key")
	assert.Assert(t, strings.Contains(err.Error(), "nonexistent-key"), "error should mention the missing key")
}

// TestRollbackAllocationAllocationNotFound verifies that RollbackAllocation returns an
// error when the ask exists in requests but no corresponding allocation has been recorded.
func TestRollbackAllocationAllocationNotFound(t *testing.T) {
	setupUGM()
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app := newApplication(appID1, "default", "root.unknown")
	app.queue = queue
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})

	ask := newAllocationAsk(aKey, appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	assert.Assert(t, app.IsAccepted(), "app should be in Accepted state")
	// ask is in requests but no allocation has been added

	_, err = app.RollbackAllocation(aKey)
	assert.Assert(t, err != nil, "expected error when no allocation exists for the ask")
	assert.Assert(t, strings.Contains(err.Error(), aKey), "error should mention the missing allocation key")
}

// TestRollbackAllocationFromAccepted verifies a successful rollback when the application
// is in Accepted state. The allocation is injected directly into internal state to avoid
// the Running-state transition triggered by AddAllocation.
func TestRollbackAllocationFromAccepted(t *testing.T) {
	setupUGM()
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app := newApplication(appID1, "default", "root.unknown")
	app.queue = queue
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})

	// Add ask → app moves to Accepted, ask enters requests.
	ask := newAllocationAsk(aKey, appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	assert.Assert(t, app.IsAccepted(), "app should be in Accepted state")

	// Simulate the core allocating the ask: mark it allocated and record the allocation
	// in internal state without triggering the Running state transition.
	_, err = app.AllocateAsk(aKey)
	assert.NilError(t, err, "AllocateAsk should succeed")
	ask.SetNodeID(nodeID1)

	app.Lock()
	app.allocations[aKey] = ask
	app.allocatedResource = resources.Add(app.allocatedResource, res)
	app.incUserResourceUsage(res)
	app.Unlock()

	assert.Assert(t, app.IsAccepted(), "app should still be in Accepted state before rollback")

	// Execute rollback.
	returned, err := app.RollbackAllocation(aKey)
	assert.NilError(t, err, "rollback should succeed from Accepted state")
	assert.Assert(t, resources.Equals(returned, res), "returned resource should match allocation resource, got %v", returned)

	// Allocation is removed from the map.
	assert.Assert(t, app.allocations[aKey] == nil, "allocation should have been removed from map")

	// Pending resource is restored.
	assert.Assert(t, resources.Equals(app.GetPendingResource(), res), "pending resource should be restored, got %v", app.GetPendingResource())

	// Ask still exists in requests (rolled back to pending, not deleted).
	assert.Assert(t, app.GetAllocationAsk(aKey) != nil, "ask should still exist in requests after rollback")

	// allocatedResource is back to zero.
	assert.Assert(t, resources.IsZero(app.GetAllocatedResource()), "allocated resource should be zero after rollback, got %v", app.GetAllocatedResource())

	// Node ID is cleared on the ask so it can be rescheduled on a different node.
	assert.Equal(t, app.GetAllocationAsk(aKey).GetNodeID(), "", "ask node ID should be cleared after rollback")

	// App remains in Accepted state (pending > 0 prevents Completing transition).
	assert.Assert(t, app.IsAccepted(), "app should remain in Accepted state after rollback")
}

// TestRollbackAllocationFromRunning verifies a successful rollback from Running state,
// including full resource accounting and UGM tracking validation.
func TestRollbackAllocationFromRunning(t *testing.T) {
	setupUGM()
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app := newApplication(appID1, "default", "root.unknown")
	app.queue = queue
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})

	// Standard allocation flow: ask → Accepted, allocate → Running.
	ask := newAllocationAsk(aKey, appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	assert.Assert(t, app.IsAccepted(), "app should be in Accepted state after adding ask")

	delta, err := app.AllocateAsk(aKey)
	assert.NilError(t, err, "AllocateAsk should succeed")
	assert.Assert(t, resources.Equals(delta, res), "AllocateAsk delta should equal res, got %v", delta)
	ask.SetNodeID(nodeID1)

	app.AddAllocation(ask)
	assert.Assert(t, app.IsRunning(), "app should be in Running state after AddAllocation")
	assert.Assert(t, resources.IsZero(app.GetPendingResource()), "pending should be zero before rollback")
	assert.Assert(t, resources.Equals(app.GetAllocatedResource(), res), "allocated should equal res before rollback")
	assertUserGroupResource(t, getTestUserGroup(), res)

	// Execute rollback.
	returned, err := app.RollbackAllocation(aKey)
	assert.NilError(t, err, "rollback should succeed from Running state")
	assert.Assert(t, resources.Equals(returned, res), "returned resource should match allocation resource, got %v", returned)

	// Allocation is removed.
	assert.Assert(t, app.allocations[aKey] == nil, "allocation should have been removed from map")

	// Pending resource is restored.
	assert.Assert(t, resources.Equals(app.GetPendingResource(), res), "pending resource should be restored after rollback, got %v", app.GetPendingResource())

	// Ask still exists in requests.
	assert.Assert(t, app.GetAllocationAsk(aKey) != nil, "ask should still exist in requests after rollback")

	// allocatedResource is back to zero.
	assert.Assert(t, resources.IsZero(app.GetAllocatedResource()), "allocated resource should be zero after rollback, got %v", app.GetAllocatedResource())

	// Node ID is cleared so the ask can land on a different node.
	assert.Equal(t, app.GetAllocationAsk(aKey).GetNodeID(), "", "ask node ID should be cleared after rollback")

	// UGM resource usage is decremented.
	assertUserGroupResource(t, getTestUserGroup(), nil)
}

// TestRollbackAllocationPartial verifies that rolling back one of several allocations
// leaves the remaining allocations and their accounting untouched.
func TestRollbackAllocationPartial(t *testing.T) {
	setupUGM()
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app := newApplication(appID1, "default", "root.unknown")
	app.queue = queue
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})

	// Allocate first ask.
	ask1 := newAllocationAsk(aKey, appID1, res)
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err, "ask1 should have been added")
	_, err = app.AllocateAsk(aKey)
	assert.NilError(t, err, "AllocateAsk for ask1 should succeed")
	ask1.SetNodeID(nodeID1)
	app.AddAllocation(ask1)

	// Allocate second ask.
	ask2 := newAllocationAsk(aKey2, appID1, res)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "ask2 should have been added")
	_, err = app.AllocateAsk(aKey2)
	assert.NilError(t, err, "AllocateAsk for ask2 should succeed")
	ask2.SetNodeID(nodeID2)
	app.AddAllocation(ask2)

	doubleRes := resources.Multiply(res, 2)
	assert.Assert(t, app.IsRunning(), "app should be Running with two allocations")
	assert.Assert(t, resources.Equals(app.GetAllocatedResource(), doubleRes), "allocated should be 2x res, got %v", app.GetAllocatedResource())
	assertUserGroupResource(t, getTestUserGroup(), doubleRes)

	// Roll back only the first allocation.
	returned, err := app.RollbackAllocation(aKey)
	assert.NilError(t, err, "partial rollback should succeed")
	assert.Assert(t, resources.Equals(returned, res), "returned resource should equal res, got %v", returned)

	// Second allocation is untouched.
	assert.Assert(t, app.allocations[aKey2] != nil, "second allocation should still exist")
	assert.Assert(t, resources.Equals(app.GetAllocatedResource(), res), "allocated should be 1x res after partial rollback, got %v", app.GetAllocatedResource())

	// First ask is back to pending; second ask remains allocated.
	assert.Assert(t, resources.Equals(app.GetPendingResource(), res), "pending should be restored to res for the rolled-back ask, got %v", app.GetPendingResource())

	// UGM reflects only the remaining allocation.
	assertUserGroupResource(t, getTestUserGroup(), res)

	// App remains Running because the second allocation is still active.
	assert.Assert(t, app.IsRunning(), "app should remain Running after partial rollback")
}

func TestApplicationBackoff(t *testing.T) {
	rootQ, err := createRootQueue(map[string]string{"first": "10"})
	assert.NilError(t, err, "unexpected error when creating root queue")
	props := map[string]string{
		configs.ApplicationUnschedulableAsksBackoff: "2",
	}
	leaf, err := createManagedQueueWithProps(rootQ, "leaf", false, map[string]string{"first": "5"}, props)
	assert.NilError(t, err, "unexpected error when creating leaf queue")

	// create app
	app := newApplication(appID0, "default", "root.leaf")
	app.SetQueue(leaf)

	// create asks
	askRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 6})
	ask1 := newAllocationAsk(aKey, appID0, askRes)
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err)
	ask2 := newAllocationAsk(aKey2, appID0, askRes)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err)
	ask3 := newAllocationAsk(aKey3, appID0, askRes)
	err = app.AddAllocationAsk(ask3)
	assert.NilError(t, err)

	// create nodes (enough cluster capacity, but asks cannot be placed)
	node1 := newNode("node1", map[string]resources.Quantity{"first": 5})
	node2 := newNode("node1", map[string]resources.Quantity{"first": 5})
	nodeMap := map[string]*Node{
		"node1": node1,
		"node2": node2,
	}
	iterator := getNodeIteratorFn(node1, node2)
	getNode := func(nodeID string) *Node {
		return nodeMap[nodeID]
	}
	preemptionAttemptsRemaining := 0
	beforeTryAlloc := time.Now()
	available := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	result := app.tryAllocate(available, true, 30*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Assert(t, result == nil)
	assert.Assert(t, app.GetBackoffDeadline().After(beforeTryAlloc))
}

func TestReservationReleasedCallback(t *testing.T) {
	app := newApplication(appID1, "default", "root.unknown")
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	// callback not set - should not panic
	app.executeReservationReleasedCallback(5)

	// callback with released = 0 - should not be called
	called := make(chan int, 1)
	app.SetReservationReleasedCallback(func(released int) {
		called <- released
	})
	app.executeReservationReleasedCallback(0)
	select {
	case <-called:
		t.Fatal("callback should not be called when released = 0")
	case <-time.After(10 * time.Millisecond):
		// expected
	}

	// callback with released > 0 - should be called with correct value
	app.executeReservationReleasedCallback(3)
	select {
	case val := <-called:
		assert.Equal(t, 3, val, "callback called with wrong value")
	case <-time.After(time.Second):
		t.Fatal("callback was not called")
	}
}

func TestUnreserveForApp(t *testing.T) {
	resMap := map[string]string{"first": "10"}
	rootQ, err := createRootQueue(resMap)
	assert.NilError(t, err)
	childQ, err := createManagedQueue(rootQ, "child", false, resMap)
	assert.NilError(t, err)

	node := newNode(nodeID1, map[string]resources.Quantity{"first": 10})
	allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 3})

	// app1 holds the reservation, app2 is the triggering app
	app1 := newApplication(appID1, "default", childQ.QueuePath)
	app1.SetQueue(childQ)
	childQ.applications[appID1] = app1
	ask1 := newAllocationAsk(aKey, appID1, allocRes)
	err = app1.AddAllocationAsk(ask1)
	assert.NilError(t, err)
	err = app1.reserveInternal(node, ask1)
	assert.NilError(t, err)

	app2 := newApplication(appID2, "default", childQ.QueuePath)
	app2.SetQueue(childQ)
	childQ.applications[appID2] = app2

	// different app: unreserveForApp uses the locked UnReserve path
	res := app1.reservations[aKey]
	assert.Assert(t, res != nil)
	num := app2.unreserveForApp(res)
	assert.Equal(t, num, 1, "expected reservation to be released")
	assert.Equal(t, app1.NodeReservedForAsk(aKey), "", "reservation should be removed from app1")

	// same app: re-reserve and use same-app path
	err = app1.reserveInternal(node, ask1)
	assert.NilError(t, err)
	res = app1.reservations[aKey]
	assert.Assert(t, res != nil)
	num = app1.unreserveForApp(res)
	assert.Equal(t, num, 1, "expected reservation to be released via internal path")
	assert.Equal(t, app1.NodeReservedForAsk(aKey), "", "reservation should be removed")
}

func TestCancelMatchingReservations(t *testing.T) {
	resMap := map[string]string{"first": "10"}
	rootQ, err := createRootQueue(resMap)
	assert.NilError(t, err)
	childQ, err := createManagedQueue(rootQ, "child", false, resMap)
	assert.NilError(t, err)

	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 10})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 10})
	allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2})

	app := newApplication(appID1, "default", childQ.QueuePath)
	app.SetQueue(childQ)
	childQ.applications[appID1] = app

	// reserve ask1 on node1, ask2 on node2
	ask1 := newAllocationAsk(aKey, appID1, allocRes)
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err)
	err = app.reserveInternal(node1, ask1)
	assert.NilError(t, err)

	ask2 := newAllocationAsk(aKey2, appID1, allocRes)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err)
	err = app.reserveInternal(node2, ask2)
	assert.NilError(t, err)

	// gather both reservations
	reservations := append(node1.GetReservations(), node2.GetReservations()...)
	assert.Equal(t, len(reservations), 2, "expected 2 reservations total")

	// cancel only the first ask's reservation using predicate on allocKey
	released, remaining := app.cancelMatchingReservations(reservations, func(res *reservation) bool {
		return res.allocKey == aKey
	})
	assert.Equal(t, released, 1, "expected 1 reservation released")
	assert.Equal(t, remaining, 1, "expected 1 reservation remaining")
	assert.Equal(t, app.NodeReservedForAsk(aKey), "", "first reservation should be gone")
	assert.Equal(t, app.NodeReservedForAsk(aKey2), nodeID2, "second reservation should remain")

	// cancel all remaining
	reservations = node2.GetReservations()
	released, remaining = app.cancelMatchingReservations(reservations, func(res *reservation) bool {
		return true
	})
	assert.Equal(t, released, 1, "expected 1 reservation released")
	assert.Equal(t, remaining, 0, "expected no reservations remaining")

	// empty list returns zero
	released, remaining = app.cancelMatchingReservations(nil, func(res *reservation) bool {
		return true
	})
	assert.Equal(t, released, 0)
	assert.Equal(t, remaining, 0)
}
