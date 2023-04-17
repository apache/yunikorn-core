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
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects/template"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

// base test for creating a managed queue
func TestQueueBasics(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	// check the state of the queue
	if !root.IsManaged() && !root.IsLeafQueue() && !root.IsRunning() {
		t.Error("root queue status is incorrect")
	}
	// allocations should be nil
	if !resources.IsZero(root.pending) {
		t.Error("root queue must not have allocations set on create")
	}
}

func TestManagedSubQueues(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// single parent under root
	var parent *Queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	if parent.IsLeafQueue() || !parent.IsManaged() || !parent.IsRunning() {
		t.Error("parent queue is not marked as running managed parent")
	}
	if len(root.children) == 0 {
		t.Error("parent queue is not added to the root queue")
	}
	if parent.isRoot() {
		t.Error("parent queue says it is the root queue which is incorrect")
	}
	if parent.RemoveQueue() || len(root.children) != 1 {
		t.Error("parent queue should not have been removed as it is running")
	}

	// add a leaf under the parent
	var leaf *Queue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(parent.children) == 0 {
		t.Error("leaf queue is not added to the parent queue")
	}
	if !leaf.IsLeafQueue() || !leaf.IsManaged() {
		t.Error("leaf queue is not marked as managed leaf")
	}

	// cannot remove child with app in it
	app := newApplication(appID1, "default", "root.parent.leaf")
	leaf.AddApplication(app)

	// both parent and leaf are marked for removal
	parent.MarkQueueForRemoval()
	if !leaf.IsDraining() || !parent.IsDraining() {
		t.Error("queues are not marked for removal (not in draining state)")
	}
	// try to remove the parent
	if parent.RemoveQueue() {
		t.Error("parent queue should not have been removed as it has a child")
	}
	// try to remove the child
	if leaf.RemoveQueue() {
		t.Error("leaf queue should not have been removed")
	}
	// remove the app (dirty way)
	delete(leaf.applications, appID1)
	if !leaf.RemoveQueue() && len(parent.children) != 0 {
		t.Error("leaf queue should have been removed and parent updated and was not")
	}
}

func TestDynamicSubQueues(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// single parent under root
	var parent *Queue
	parent, err = createDynamicQueue(root, "parent", true)
	assert.NilError(t, err, "failed to create parent queue")
	if parent.IsLeafQueue() || parent.IsManaged() {
		t.Errorf("parent queue is not marked as parent")
	}
	if len(root.children) == 0 {
		t.Errorf("parent queue is not added to the root queue")
	}
	// add a leaf under the parent
	var leaf *Queue
	leaf, err = createDynamicQueue(parent, "leaf", false)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(parent.children) == 0 {
		t.Error("leaf queue is not added to the parent queue")
	}
	if !leaf.IsLeafQueue() || leaf.IsManaged() {
		t.Error("leaf queue is not marked as managed leaf")
	}

	// cannot remove child with app in it
	app := newApplication(appID1, "default", "root.parent.leaf")
	leaf.AddApplication(app)

	// try to mark parent and leaf for removal
	parent.MarkQueueForRemoval()
	if leaf.IsDraining() || parent.IsDraining() {
		t.Error("queues are marked for removal (draining state not for Dynamic queues)")
	}
	// try to remove the parent
	if parent.RemoveQueue() {
		t.Error("parent queue should not have been removed as it has a child")
	}
	// try to remove the child
	if leaf.RemoveQueue() {
		t.Error("leaf queue should not have been removed")
	}
	// remove the app (dirty way)
	delete(leaf.applications, appID1)
	if !leaf.RemoveQueue() && len(parent.children) != 0 {
		t.Error("leaf queue should have been removed and parent updated and was not")
	}
}

func TestPriorityCalcWithFencedQueue(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// single parent under root
	var parent *Queue
	parent, err = createManagedQueueWithProps(root, "parent", true, nil, map[string]string{
		configs.PriorityOffset: "5",
	})
	assert.NilError(t, err, "failed to create parent queue")

	// add a leaf under the parent
	var leaf *Queue
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, map[string]string{
		configs.PriorityOffset: "3",
		configs.PriorityPolicy: policies.FencePriorityPolicy.String(),
	})
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, parent.GetCurrentPriority(), configs.MinPriority, "initial parent priority wrong")
	assert.Equal(t, leaf.GetCurrentPriority(), configs.MinPriority, "initial leaf priority wrong")

	app := newApplication(appID1, "default", "root.parent.leaf")
	app.SetQueue(leaf)
	leaf.AddApplication(app)

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")

	err = app.AddAllocationAsk(newAllocationAskPriority("alloc-1", appID1, res, 0))
	assert.NilError(t, err, "failed to add app")
	assert.Equal(t, parent.GetCurrentPriority(), int32(8), "parent priority wrong after alloc add")
	assert.Equal(t, leaf.GetCurrentPriority(), int32(3), "leaf priority wrong after alloc add")

	app2 := newApplication(appID2, "default", "root.parent.leaf")
	app2.SetQueue(leaf)
	leaf.AddApplication(app2)
	err = app2.AddAllocationAsk(newAllocationAskPriority("alloc-2", appID2, res, 5))
	assert.NilError(t, err, "failed to add app")
	assert.Equal(t, parent.GetCurrentPriority(), int32(8), "parent priority wrong after alloc add 2")
	assert.Equal(t, leaf.GetCurrentPriority(), int32(3), "leaf priority wrong after alloc add 2")

	leaf.RemoveApplication(app2)
	assert.Equal(t, parent.GetCurrentPriority(), int32(8), "parent priority wrong after app 2 removed")
	assert.Equal(t, leaf.GetCurrentPriority(), int32(3), "leaf priority wrong after app 2 removed")

	leaf.RemoveApplication(app)
	assert.Equal(t, parent.GetCurrentPriority(), configs.MinPriority, "final parent priority wrong")
	assert.Equal(t, leaf.GetCurrentPriority(), configs.MinPriority, "final leaf priority wrong")
}

func TestPriorityCalc(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// single parent under root
	var parent *Queue
	parent, err = createManagedQueueWithProps(root, "parent", true, nil, map[string]string{
		configs.PriorityOffset: "5",
	})
	assert.NilError(t, err, "failed to create parent queue")

	// add a leaf under the parent
	var leaf *Queue
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, map[string]string{
		configs.PriorityOffset: "3",
	})
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, parent.GetCurrentPriority(), configs.MinPriority, "initial parent priority wrong")
	assert.Equal(t, leaf.GetCurrentPriority(), configs.MinPriority, "initial leaf priority wrong")

	app := newApplication(appID1, "default", "root.parent.leaf")
	app.SetQueue(leaf)
	leaf.AddApplication(app)

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")

	err = app.AddAllocationAsk(newAllocationAskPriority("alloc-1", appID1, res, 0))
	assert.NilError(t, err, "failed to add app")
	assert.Equal(t, parent.GetCurrentPriority(), int32(8), "parent priority wrong after alloc add")
	assert.Equal(t, leaf.GetCurrentPriority(), int32(3), "leaf priority wrong after alloc add")

	app2 := newApplication(appID2, "default", "root.parent.leaf")
	app2.SetQueue(leaf)
	leaf.AddApplication(app2)
	err = app2.AddAllocationAsk(newAllocationAskPriority("alloc-2", appID2, res, 5))
	assert.NilError(t, err, "failed to add app")
	assert.Equal(t, parent.GetCurrentPriority(), int32(13), "parent priority wrong after alloc add 2")
	assert.Equal(t, leaf.GetCurrentPriority(), int32(8), "leaf priority wrong after alloc add 2")

	leaf.RemoveApplication(app2)
	assert.Equal(t, parent.GetCurrentPriority(), int32(8), "parent priority wrong after app 2 removed")
	assert.Equal(t, leaf.GetCurrentPriority(), int32(3), "leaf priority wrong after app 2 removed")

	leaf.RemoveApplication(app)
	assert.Equal(t, parent.GetCurrentPriority(), configs.MinPriority, "final parent priority wrong")
	assert.Equal(t, leaf.GetCurrentPriority(), configs.MinPriority, "final leaf priority wrong")
}

func TestPendingCalc(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")

	res := map[string]string{"memory": "100", "vcores": "10"}
	var allocRes *resources.Resource
	allocRes, err = resources.NewResourceFromConf(res)
	assert.NilError(t, err, "failed to create basic resource")
	leaf.incPendingResource(allocRes)
	if !resources.Equals(root.pending, allocRes) {
		t.Errorf("root queue pending allocation failed to increment expected %v, got %v", allocRes, root.pending)
	}
	if !resources.Equals(leaf.pending, allocRes) {
		t.Errorf("leaf queue pending allocation failed to increment expected %v, got %v", allocRes, leaf.pending)
	}
	leaf.decPendingResource(allocRes)
	if !resources.IsZero(root.pending) {
		t.Errorf("root queue pending allocation failed to decrement expected 0, got %v", root.pending)
	}
	if !resources.IsZero(leaf.pending) {
		t.Errorf("leaf queue pending allocation failed to decrement expected 0, got %v", leaf.pending)
	}
	// Not allowed to go negative: both will be zero after this
	newRes := resources.Multiply(allocRes, 2)
	root.pending = newRes
	leaf.decPendingResource(newRes)
	// using the get function to access the value
	if !resources.IsZero(root.GetPendingResource()) {
		t.Errorf("root queue pending allocation failed to decrement expected zero, got %v", root.pending)
	}
	if !resources.IsZero(leaf.GetPendingResource()) {
		t.Errorf("leaf queue pending allocation should have failed to decrement expected zero, got %v", leaf.pending)
	}
}

func TestGetChildQueueInfo(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var parent *Queue
	parent, err = createManagedQueue(root, "parent-man", true, nil)
	assert.NilError(t, err, "failed to create managed parent queue")
	for i := 0; i < 10; i++ {
		_, err = createManagedQueue(parent, "leaf-man"+strconv.Itoa(i), false, nil)
		if err != nil {
			t.Errorf("failed to create managed queue: %v", err)
		}
	}
	if len(parent.children) != 10 {
		t.Errorf("managed leaf queues are not added to the parent queue, expected 10 children got %d", len(parent.children))
	}

	parent, err = createDynamicQueue(root, "parent-un", true)
	assert.NilError(t, err, "failed to create dynamic parent queue")
	for i := 0; i < 10; i++ {
		_, err = createDynamicQueue(parent, "leaf-un-"+strconv.Itoa(i), false)
		if err != nil {
			t.Errorf("failed to create dynamic queue: %v", err)
		}
	}
	if len(parent.children) != 10 {
		t.Errorf("dynamic leaf queues are not added to the parent queue, expected 10 children got %d", len(parent.children))
	}

	// check the root queue
	if len(root.children) != 2 {
		t.Errorf("parent queues are not added to the root queue, expected 2 children got %d", len(root.children))
	}
}

func TestAddApplication(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf-man", false, nil)
	assert.NilError(t, err, "failed to create managed leaf queue")
	pending := resources.NewResourceFromMap(
		map[string]resources.Quantity{
			common.Memory: 10,
		})
	app := newApplication(appID1, "default", "root.parent.leaf")
	app.pending = pending
	// adding the app must not update pending resources
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not zero")

	// add the same app again should not increase the number of apps
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not replaced in the queue as expected")
}

func TestAddApplicationWithTag(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	// only need to test leaf queues as we will never add an app to a parent
	var leaf, leafUn *Queue
	leaf, err = createManagedQueue(root, "leaf-man", false, nil)
	assert.NilError(t, err, "failed to create managed leaf queue")
	leafUn, err = createDynamicQueue(root, "leaf-unman", false)
	assert.NilError(t, err, "failed to create Dynamic leaf queue")
	app := newApplication(appID1, "default", "root.leaf-man")

	// adding the app to managed/Dynamic queue must not update queue settings, works
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the managed queue as expected")
	if leaf.GetMaxResource() != nil {
		t.Errorf("Max resources should not be set on managed queue got: %s", leaf.GetMaxResource().String())
	}
	app = newApplication("app-2", "default", "root.leaf-un")
	leafUn.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the Dynamic queue as expected")
	if leafUn.GetMaxResource() != nil {
		t.Errorf("Max resources should not be set on Dynamic queue got: %s", leafUn.GetMaxResource().String())
	}

	maxRes := resources.NewResourceFromMap(
		map[string]resources.Quantity{
			"first": 10,
		})
	tags := make(map[string]string)
	tags[common.AppTagNamespaceResourceQuota] = "{\"resources\":{\"first\":{\"value\":10}}}"
	// add apps again now with the tag set
	app = newApplicationWithTags("app-3", "default", "root.leaf-man", tags)
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 2, "Application was not added to the managed queue as expected")
	if leaf.GetMaxResource() != nil {
		t.Errorf("Max resources should not be set on managed queue got: %s", leaf.GetMaxResource().String())
	}
	app = newApplicationWithTags("app-4", "default", "root.leaf-un", tags)
	leafUn.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 2, "Application was not added to the Dynamic queue as expected")
	if !resources.Equals(leafUn.GetMaxResource(), maxRes) {
		t.Errorf("Max resources not set as expected: %s got: %v", maxRes.String(), leafUn.GetMaxResource())
	}

	// set to illegal limit (0 value)
	tags[common.AppTagNamespaceResourceQuota] = "{\"resources\":{\"first\":{\"value\":0}}}"
	app = newApplicationWithTags("app-4", "default", "root.leaf-un", tags)
	leafUn.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 2, "Application was not added to the Dynamic queue as expected")
	if !resources.Equals(leafUn.GetMaxResource(), maxRes) {
		t.Errorf("Max resources not set as expected: %s got: %v", maxRes.String(), leafUn.GetMaxResource())
	}
}

func TestRemoveApplication(t *testing.T) {
	// create the root
	root, err := createRootQueue(map[string]string{"first": "100"})
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf-man", false, nil)
	assert.NilError(t, err, "failed to create managed leaf queue")
	// try removing a non existing app
	nonExist := newApplication("test", "", "")
	leaf.RemoveApplication(nonExist)
	assert.Equal(t, len(leaf.applications), 0, "Removal of non existing app updated unexpected")

	// add an app and remove it
	app := newApplication("exists", "default", "root.leaf-man")
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not zero")
	leaf.RemoveApplication(nonExist)
	assert.Equal(t, len(leaf.applications), 1, "Non existing application was removed from the queue")
	assert.Equal(t, len(leaf.GetCopyOfApps()), 1, "Non existing application was removed from the queue")
	leaf.RemoveApplication(app)
	assert.Equal(t, len(leaf.applications), 0, "Application was not removed from the queue as expected")
	assert.Equal(t, len(leaf.GetCopyOfApps()), 0, "Application was not removed from the queue as expected")

	// try the same again now with pending resources set
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	app.pending.AddTo(res)
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not zero")
	// update pending resources for the hierarchy
	leaf.incPendingResource(res)
	leaf.RemoveApplication(app)
	assert.Equal(t, len(leaf.applications), 0, "Application was not removed from the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not updated correctly")
	assert.Assert(t, resources.IsZero(root.pending), "root queue pending resource not updated correctly")

	app.allocatedResource.AddTo(res)
	app.pending = resources.NewResource()
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.allocatedResource), "leaf queue allocated resource not zero")
	// update allocated resources for the hierarchy
	err = leaf.IncAllocatedResource(res, false)
	assert.NilError(t, err, "increment of allocated resource on queue should not have failed")
	leaf.RemoveApplication(app)
	assert.Equal(t, len(leaf.applications), 0, "Application was not removed from the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.allocatedResource), "leaf queue allocated resource not updated correctly")
	assert.Assert(t, resources.IsZero(root.allocatedResource), "root queue allocated resource not updated correctly")

	// remove an application with allocated placeholders
	app.allocatedPlaceholder.AddTo(res)
	app.allocatedResource.SubFrom(res)
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.allocatedResource), "leaf queue allocated resource not zero")
	err = leaf.IncAllocatedResource(res, false)
	assert.NilError(t, err, "increment of allocated resource on queue should not have failed")
	assert.Assert(t, resources.Equals(leaf.allocatedResource, res), "leaf queue allocated resource not updated as expected")
	leaf.RemoveApplication(app)
	assert.Equal(t, len(leaf.applications), 0, "Application was not removed from the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.allocatedResource), "leaf queue allocated resource not updated correctly")
	assert.Assert(t, resources.IsZero(root.allocatedResource), "root queue allocated resource not updated correctly")
}

func TestQueueStates(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// add a leaf under the root
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	err = leaf.handleQueueEvent(Stop)
	if err != nil || !leaf.IsStopped() {
		t.Errorf("leaf queue is not marked stopped: %v", err)
	}
	err = leaf.handleQueueEvent(Start)
	if err != nil || !leaf.IsRunning() {
		t.Errorf("leaf queue is not marked running: %v", err)
	}
	err = leaf.handleQueueEvent(Remove)
	if err != nil || !leaf.IsDraining() {
		t.Errorf("leaf queue is not marked draining: %v", err)
	}
	err = leaf.handleQueueEvent(Start)
	if err == nil || !leaf.IsDraining() {
		t.Errorf("leaf queue changed state which should not happen: %v", err)
	}
}

// This test must not test the sorter that is underlying.
// It tests the queue specific parts of the code only.
func TestSortApplications(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf, parent *Queue
	// empty parent queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue: %v")
	if apps := parent.sortApplications(true); apps != nil {
		t.Errorf("parent queue should not return sorted apps: %v", apps)
	}

	// empty leaf queue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(leaf.sortApplications(true)) != 0 {
		t.Errorf("empty queue should return no app from sort: %v", leaf)
	}
	// new app does not have pending res, does not get returned
	app := newApplication(appID1, "default", leaf.QueuePath)
	app.queue = leaf
	leaf.AddApplication(app)
	if len(leaf.sortApplications(true)) != 0 {
		t.Errorf("app without ask should not be in sorted apps: %v", app)
	}
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	// add an ask app must be returned
	err = app.AddAllocationAsk(newAllocationAsk("alloc-1", appID1, res))
	assert.NilError(t, err, "failed to add allocation ask")
	sortedApp := leaf.sortApplications(true)
	if len(sortedApp) != 1 || sortedApp[0].ApplicationID != appID1 {
		t.Errorf("sorted application is missing expected app: %v", sortedApp)
	}
	// set 0 repeat
	_, err = app.UpdateAskRepeat("alloc-1", -1)
	if err != nil || len(leaf.sortApplications(true)) != 0 {
		t.Errorf("app with ask but 0 pending resources should not be in sorted apps: %v (err = %v)", app, err)
	}
}

func TestSortApplicationsWithoutFiltering(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	var leaf *Queue
	properties := map[string]string{configs.ApplicationSortPolicy: "stateaware"}
	leaf, err = createManagedQueueWithProps(root, "leaf", false, nil, properties)
	assert.NilError(t, err, "failed to create queue: %v", err)

	// new app does not have pending res, does not get returned
	app1 := newApplication(appID1, "default", leaf.QueuePath)
	app1.queue = leaf
	leaf.AddApplication(app1)

	app2 := newApplication(appID2, "default", leaf.QueuePath)
	app2.queue = leaf
	leaf.AddApplication(app2)

	// both apps have no pending resource, they will be excluded by the sorting result
	apps := leaf.sortApplications(true)
	assertAppListLength(t, apps, []string{}, "sort with the filter")
	apps = leaf.sortApplications(false)
	assertAppListLength(t, apps, []string{}, "sort without the filter")

	// add pending ask to app1
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	// add an ask app must be returned
	err = app1.AddAllocationAsk(newAllocationAsk("app1-alloc-1", appID1, res))
	assert.NilError(t, err, "failed to add allocation ask")

	// the sorting result will return app1
	apps = leaf.sortApplications(true)
	assertAppListLength(t, apps, []string{appID1}, "sort with the filter")
	apps = leaf.sortApplications(false)
	assertAppListLength(t, apps, []string{appID1}, "sort without the filter")

	// add pending ask to app2
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	// add an ask app must be returned
	err = app2.AddAllocationAsk(newAllocationAsk("app2-alloc-1", appID2, res))
	assert.NilError(t, err, "failed to add allocation ask")

	// now there are 2 apps in the queue
	// according to the state aware policy, if we sort with the filter
	// only 1 app will be returning in the result; if sort without the filter
	// it should return the both 2 apps
	apps = leaf.sortApplications(true)
	assertAppListLength(t, apps, []string{appID1}, "sort with the filter")
	apps = leaf.sortApplications(false)
	assertAppListLength(t, apps, []string{appID1, appID2}, "sort without the filter")
}

// This test must not test the sorter that is underlying.
// It tests the queue specific parts of the code only.
func TestSortQueue(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	var leaf, parent *Queue
	// empty parent queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	if len(parent.sortQueues()) != 0 {
		t.Errorf("parent queue should return sorted queues: %v", parent)
	}

	// leaf queue must be nil
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	if queues := leaf.sortQueues(); queues != nil {
		t.Errorf("leaf queue should return sorted queues: %v", queues)
	}
	if queues := parent.sortQueues(); len(queues) != 0 {
		t.Errorf("parent queue with leaf returned unexpectd queues: %v", queues)
	}
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	leaf.incPendingResource(res)
	if queues := parent.sortQueues(); len(queues) != 1 {
		t.Errorf("parent queue did not return expected queues: %v", queues)
	}
	err = leaf.handleQueueEvent(Stop)
	assert.NilError(t, err, "failed to stop queue")
	if queues := parent.sortQueues(); len(queues) != 0 {
		t.Errorf("parent queue returned stopped queue: %v", queues)
	}
}

func TestHeadroom(t *testing.T) {
	// create the root: nil test
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	headRoom := root.getHeadRoom()
	if headRoom != nil {
		t.Errorf("empty cluster with root queue should not have headroom: %v", headRoom)
	}

	var parent *Queue
	// empty parent queue: nil test
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	headRoom = parent.getHeadRoom()
	if headRoom != nil {
		t.Errorf("empty cluster with parent queue should not have headroom: %v", headRoom)
	}

	// recreate the structure to pick up changes on max etc
	// structure is:
	// root			max resource 20,10;	alloc 10,6	head 10,4
	// - parent		max resource 20,8;	alloc 10,6	head 10,2
	//   - leaf1	max resource ---;	alloc 5,3	head 10,2 * parent headroom used
	//   - leaf2	max resource ---;	alloc 5,3	head 10,2 * parent headroom used
	// set the max on the root
	resMap := map[string]string{"first": "20", "second": "10"}
	root, err = createRootQueue(resMap)
	assert.NilError(t, err, "failed to create root queue with limit")
	// set the max on the parent
	resMap = map[string]string{"first": "20", "second": "8"}
	parent, err = createManagedQueue(root, "parent", true, resMap)
	assert.NilError(t, err, "failed to create parent queue")
	// leaf1 queue no limit
	var leaf1, leaf2 *Queue
	leaf1, err = createManagedQueue(parent, "leaf1", false, nil)
	assert.NilError(t, err, "failed to create leaf1 queue")
	// leaf2 queue no limit
	leaf2, err = createManagedQueue(parent, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	// allocating and allocated
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "3"})
	assert.NilError(t, err, "failed to create resource")
	err = leaf1.IncAllocatedResource(res, true)
	assert.NilError(t, err, "failed to set allocated resource on leaf1")
	err = leaf2.IncAllocatedResource(res, true)
	assert.NilError(t, err, "failed to set allocated resource on leaf2")

	// headRoom root should be this (max 20-10 - alloc 10-6)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "4"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = root.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "root queue head room not as expected %v, got: %v", res, headRoom)

	maxHeadRoom := root.getMaxHeadRoom()
	assert.Assert(t, maxHeadRoom == nil, "root queue max headroom should be nil")

	// headRoom parent should be this (max 20-10 - alloc 8-6)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "2"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = parent.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "parent queue head room not as expected %v, got: %v", res, headRoom)
	maxHeadRoom = parent.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, maxHeadRoom), "parent queue max head room not as expected %v, got: %v", res, maxHeadRoom)

	// headRoom for any leaves will be at most the parent headroom
	// since leaf1 does not have a max it will be the same
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "2"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = leaf1.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "leaf1 queue head room not as expected %v, got: %v", res, headRoom)
	maxHeadRoom = leaf1.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, maxHeadRoom), "leaf1 queue max head room not as expected %v, got: %v", res, maxHeadRoom)
	// since leaf2 does not have a max it will be the same
	headRoom = leaf2.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "leaf2 queue head room not as expected %v, got: %v", res, headRoom)
	maxHeadRoom = leaf2.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, maxHeadRoom), "leaf2 queue max head room not as expected %v, got: %v", res, maxHeadRoom)
}

func TestHeadroomMerge(t *testing.T) {
	// recreate the structure, set max capacity in parent and a leaf queue
	// structure is:
	// root (max: nil)               (alloc a:5, b:5, c:10, d:5)  (headroom: nil)
	//   - parent (max: a:20, b:10)  (alloc a:5, b:5, c:10, d:5)  (headroom a:15 b:5)
	//     - leaf1 (max: a:10, c:10) (alloc a:5, b:5, c:5)        (headroom a:5 b:5 c:5)
	//     - leaf2 (max: d:10)       (alloc c:5 d:5)              (headroom a:15 b:5 d:5)
	resMap := map[string]string{"first": "20", "second": "10"}
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	var parent, leaf1, leaf2 *Queue
	parent, err = createManagedQueue(root, "parent", true, resMap)
	assert.NilError(t, err, "failed to create parent queue")
	resMap = map[string]string{"first": "10", "third": "10"}
	leaf1, err = createManagedQueue(parent, "leaf1", false, resMap)
	assert.NilError(t, err, "failed to create leaf1 queue")
	resMap = map[string]string{"fourth": "10"}
	leaf2, err = createManagedQueue(parent, "leaf2", false, resMap)
	assert.NilError(t, err, "failed to create leaf2 queue")

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "5", "third": "5"})
	assert.NilError(t, err, "failed to create resource")
	err = leaf1.IncAllocatedResource(res, true)
	assert.NilError(t, err, "failed to set allocated resource on leaf1")
	res, err = resources.NewResourceFromConf(map[string]string{"third": "5", "fourth": "5"})
	assert.NilError(t, err, "failed to create resource")
	err = leaf2.IncAllocatedResource(res, true)
	assert.NilError(t, err, "failed to set allocated resource on leaf2")

	// root headroom should be nil
	headRoom := root.getHeadRoom()
	assert.Assert(t, headRoom == nil, "headRoom of root should be nil because no max set")
	maxHeadRoom := root.getMaxHeadRoom()
	assert.Assert(t, maxHeadRoom == nil, "maxHeadRoom of root should be nil because no max")

	res, err = resources.NewResourceFromConf(map[string]string{"first": "15", "second": "5"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = parent.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "parent queue max head room not as expected %v, got: %v", res, headRoom)
	maxHeadRoom = parent.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, maxHeadRoom), "parent queue max head room not as expected %v, got: %v", res, maxHeadRoom)

	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "5", "third": "5"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = leaf1.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "leaf1 queue head room not as expected %v, got: %v", res, headRoom)
	maxHeadRoom = leaf1.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, maxHeadRoom), "leaf1 queue max head room not as expected %v, got: %v", res, maxHeadRoom)

	res, err = resources.NewResourceFromConf(map[string]string{"first": "15", "second": "5", "fourth": "5"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = leaf2.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "leaf2 queue head room not as expected %v, got: %v", res, headRoom)
	maxHeadRoom = leaf2.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, maxHeadRoom), "leaf2 queue max head room not as expected %v, got: %v", res, maxHeadRoom)
}

func TestMaxHeadroomNoMax(t *testing.T) {
	// create the root: nil test
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	headRoom := root.getMaxHeadRoom()
	if headRoom != nil {
		t.Errorf("empty cluster with root queue should not have headroom: %v", headRoom)
	}

	var parent *Queue
	// empty parent queue: nil test
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	headRoom = parent.getMaxHeadRoom()
	if headRoom != nil {
		t.Errorf("empty cluster with parent queue should not have headroom: %v", headRoom)
	}

	// recreate the structure, all queues have no max capacity set
	// structure is:
	// root (max: nil)
	//   - parent (max: nil)
	//     - leaf1 (max: nil)  (alloc: 5,3)
	//     - leaf2 (max: nil)  (alloc: 5,3)
	root, err = createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	var leaf1, leaf2 *Queue
	leaf1, err = createManagedQueue(parent, "leaf1", false, nil)
	assert.NilError(t, err, "failed to create leaf1 queue")
	leaf2, err = createManagedQueue(parent, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "3"})
	assert.NilError(t, err, "failed to create resource")
	err = leaf1.IncAllocatedResource(res, true)
	assert.NilError(t, err, "failed to set allocated resource on leaf1")
	err = leaf2.IncAllocatedResource(res, true)
	assert.NilError(t, err, "failed to set allocated resource on leaf2")

	headRoom = root.getMaxHeadRoom()
	assert.Assert(t, headRoom == nil, "headRoom of root should be nil because no max set for all queues")

	headRoom = parent.getMaxHeadRoom()
	assert.Assert(t, headRoom == nil, "headRoom of parent should be nil because no max set for all queues")

	headRoom = leaf1.getMaxHeadRoom()
	assert.Assert(t, headRoom == nil, "headRoom of leaf1 should be nil because no max set for all queues")

	headRoom = leaf2.getMaxHeadRoom()
	assert.Assert(t, headRoom == nil, "headRoom of leaf2 should be nil because no max set for all queues")
}

func TestMaxHeadroomMax(t *testing.T) {
	// recreate the structure, set max capacity in parent and a leaf queue
	// structure is:
	// root (max: nil)
	//   - parent (max: 20,8)
	//     - leaf1 (max: 10, 8)  (alloc: 5,3)
	//     - leaf2 (max: nil)    (alloc: 6,4)
	resMap := map[string]string{"first": "20", "second": "8"}
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	var parent, leaf1, leaf2 *Queue
	parent, err = createManagedQueue(root, "parent", true, resMap)
	assert.NilError(t, err, "failed to create parent queue")
	resMap = map[string]string{"first": "10", "second": "8"}
	leaf1, err = createManagedQueue(parent, "leaf1", false, resMap)
	assert.NilError(t, err, "failed to create leaf1 queue")
	leaf2, err = createManagedQueue(parent, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "3"})
	assert.NilError(t, err, "failed to create resource")
	err = leaf1.IncAllocatedResource(res, true)
	assert.NilError(t, err, "failed to set allocated resource on leaf1")
	err = leaf2.IncAllocatedResource(res, true)
	assert.NilError(t, err, "failed to set allocated resource on leaf2")

	// root headroom should be nil
	headRoom := root.getMaxHeadRoom()
	assert.Assert(t, headRoom == nil, "headRoom of root should be nil because no max set for all queues")

	// parent headroom = parentMax - leaf1Allocated - leaf2Allocated
	// parent headroom = (20 - 5 - 5, 8 - 3 - 3) = (10, 2)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "2"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = parent.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "parent queue head room not as expected %v, got: %v", res, headRoom)

	// leaf1 headroom = MIN(parentHeadRoom, leaf1Max - leaf1Allocated)
	// leaf1 headroom = MIN((10,2), (10-5, 8-3)) = MIN((10,2), (5,5)) = MIN(5, 2)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "2"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = leaf1.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "leaf1 queue head room not as expected %v, got: %v", res, headRoom)

	// leaf2 headroom = parentMax - leaf1Allocated - leaf2Allocated
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "2"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = leaf2.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "leaf2 queue head room not as expected %v, got: %v", res, headRoom)
}

func TestGetMaxUsage(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	maxUsage := root.GetMaxResource()
	if maxUsage != nil {
		t.Errorf("empty cluster with root queue should not have max set: %v", maxUsage)
	}

	var parent *Queue
	// empty parent queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	maxUsage = parent.GetMaxResource()
	if maxUsage != nil {
		t.Errorf("empty cluster parent queue should not have max set: %v", maxUsage)
	}

	// set the max on the root: recreate the structure to pick up changes
	var res *resources.Resource
	resMap := map[string]string{"first": "10", "second": "5"}
	res, err = resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource")
	root, err = createRootQueue(resMap)
	assert.NilError(t, err, "failed to create root queue with limit")
	maxUsage = root.GetMaxResource()
	if !resources.Equals(res, maxUsage) {
		t.Errorf("root queue should have max set expected %v, got: %v", res, maxUsage)
	}
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	maxUsage = parent.GetMaxResource()
	if !resources.Equals(res, maxUsage) {
		t.Errorf("parent queue should have max from root set expected %v, got: %v", res, maxUsage)
	}

	// leaf queue with limit: contrary to root should get min from both merged
	var leaf *Queue
	resMap = map[string]string{"first": "5", "second": "10"}
	leaf, err = createManagedQueue(parent, "leaf", false, resMap)
	assert.NilError(t, err, "failed to create leaf queue")
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "5"})
	assert.NilError(t, err, "failed to create resource")
	maxUsage = leaf.GetMaxResource()
	assert.Assert(t, resources.Equals(res, maxUsage), "leaf queue should have merged max set expected %v, got: %v", res, maxUsage)

	// replace parent with one with limit on different resource
	resMap = map[string]string{"third": "2"}
	parent, err = createManagedQueue(root, "parent2", true, resMap)
	assert.NilError(t, err, "failed to create parent2 queue")
	res, err = resources.NewResourceFromConf(map[string]string{"first": "0", "second": "0", "third": "0"})
	assert.NilError(t, err, "failed to create resource")
	maxUsage = parent.GetMaxResource()
	assert.Assert(t, resources.Equals(res, maxUsage), "parent2 queue should have max from root set expected %v, got: %v", res, maxUsage)
	resMap = map[string]string{"first": "5", "second": "10"}
	leaf, err = createManagedQueue(parent, "leaf2", false, resMap)
	assert.NilError(t, err, "failed to create leaf2 queue")
	res, err = resources.NewResourceFromConf(map[string]string{"first": "0", "second": "0", "third": "0"})
	assert.NilError(t, err, "failed to create resource")
	maxUsage = leaf.GetMaxResource()
	assert.Assert(t, resources.Equals(res, maxUsage), "leaf2 queue should have reset merged max set expected %v, got: %v", res, maxUsage)
}

func TestGetMaxQueueSet(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var nilRes *resources.Resource
	assert.Equal(t, root.GetMaxQueueSet(), nilRes, "root queue should always return max set nil")

	var parent *Queue
	// empty parent queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Equal(t, parent.GetMaxQueueSet(), nilRes, "parent queue should not have max")

	// set the max on the root: recreate the structure to pick up changes
	resMap := map[string]string{"first": "10", "second": "10"}
	assert.NilError(t, err, "failed to create resource")
	root, err = createRootQueue(resMap)
	assert.NilError(t, err, "failed to create root queue with limit set")
	assert.Equal(t, root.GetMaxQueueSet(), nilRes, "root queue should always return max set nil")
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Equal(t, parent.GetMaxQueueSet(), nilRes, "parent queue should not have max even with root set")

	// leaf queue with limit
	// parent has no limit and root is ignored: expect the leaf limit returned
	var leaf *Queue
	resMap = map[string]string{"first": "5", "second": "5"}
	leaf, err = createManagedQueue(parent, "leaf", false, resMap)
	assert.NilError(t, err, "failed to create leaf queue")
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource")
	maxSet := leaf.GetMaxQueueSet()
	assert.Assert(t, resources.Equals(res, maxSet), "leaf queue should have max set expected %v, got: %v", res, maxSet)

	// replace parent with one with limit on multiple resource
	resMap = map[string]string{"second": "5", "third": "2"}
	parent, err = createManagedQueue(root, "parent2", true, resMap)
	assert.NilError(t, err, "failed to create parent2 queue")
	res, err = resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource")
	maxSet = parent.GetMaxQueueSet()
	assert.Assert(t, resources.Equals(res, maxSet), "parent2 queue should have max excluding root expected %v, got: %v", res, maxSet)

	// a leaf with max set on different resource than the parent.
	// The parent has limit and root is ignored: expect the merged parent and leaf to be returned (0 for missing on either)
	resMap = map[string]string{"first": "5", "second": "10"}
	leaf, err = createManagedQueue(parent, "leaf2", false, resMap)
	assert.NilError(t, err, "failed to create leaf2 queue")
	res, err = resources.NewResourceFromConf(map[string]string{"first": "0", "second": "5", "third": "0"})
	assert.NilError(t, err, "failed to create resource")
	maxSet = leaf.GetMaxQueueSet()
	assert.Assert(t, resources.Equals(res, maxSet), "leaf2 queue should have reset merged max set expected %v, got: %v", res, maxSet)
}

func TestReserveApp(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, len(leaf.reservedApps), 0, "new queue should not have reserved apps")
	// no checks this works for everything
	appName := "something"
	leaf.Reserve(appName)
	assert.Equal(t, len(leaf.reservedApps), 1, "app should have been reserved")
	assert.Equal(t, leaf.reservedApps[appName], 1, "app should have one reservation")
	leaf.Reserve(appName)
	assert.Equal(t, leaf.reservedApps[appName], 2, "app should have two reservations")
	leaf.UnReserve(appName, 2)
	assert.Equal(t, len(leaf.reservedApps), 0, "queue should not have any reserved apps, all reservations were removed")

	leaf.UnReserve("unknown", 1)
	assert.Equal(t, len(leaf.reservedApps), 0, "unreserve of unknown app should not have changed count or added app")
}

func TestGetApp(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	// check for init of the map
	if unknown := leaf.GetApplication("unknown"); unknown != nil {
		t.Errorf("un registered app found using appID which should not happen: %v", unknown)
	}

	// add app and check proper returns
	app := newApplication(appID1, "default", leaf.QueuePath)
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "queue should have one app registered")
	if leaf.GetApplication(appID1) == nil {
		t.Errorf("registered app not found using appID")
	}
	if unknown := leaf.GetApplication("unknown"); unknown != nil {
		t.Errorf("un registered app found using appID which should not happen: %v", unknown)
	}
}

func TestIsEmpty(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	assert.Equal(t, root.IsEmpty(), true, "new root queue should have been empty")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, root.IsEmpty(), false, "root queue with child leaf should not have been empty")
	assert.Equal(t, leaf.IsEmpty(), true, "new leaf should have been empty")

	// add app and check proper returns
	app := newApplication(appID1, "default", leaf.QueuePath)
	leaf.AddApplication(app)
	assert.Equal(t, leaf.IsEmpty(), false, "queue with registered app should not be empty")
}

func TestGetOutstandingRequestMax(t *testing.T) {
	// queue structure:
	// root
	//   - queue1 (max.cpu = 10)
	//   - queue2 (max.cpu = 5)
	//
	// submit app1 to root.queue1, app2 to root.queue2
	// app1 asks for 20 1x1CPU requests, app2 asks for 20 1x1CPU requests
	// verify the outstanding requests for each of the queue is up to its max capacity
	// root: 15, queue1: 10 and queue2: 5
	// add an allocation for 5 CPU to queue1 and check the reduced numbers
	// root: 10, queue1: 5 and queue2: 5
	alloc, err := resources.NewResourceFromConf(map[string]string{"cpu": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	var used *resources.Resource
	used, err = resources.NewResourceFromConf(map[string]string{"cpu": "5"})
	assert.NilError(t, err, "failed to create basic resource")
	testOutstanding(t, alloc, used)
}

func TestGetOutstandingUntracked(t *testing.T) {
	// same test as TestGetOutstandingRequestMax but adding an unlimited resource to the
	// allocations to make sure it does not affect the calculations
	// queue structure:
	// root
	//   - queue1 (max.cpu = 10)
	//   - queue2 (max.cpu = 5)
	//
	// submit app1 to root.queue1, app2 to root.queue2
	// app1 asks for 20 1x1CPU, 1xPOD requests, app2 asks for 20 1x1CPU, 1xPOD requests
	// verify the outstanding requests for each of the queue is up to its max capacity
	// root: 15, queue1: 10 and queue2: 5
	// add an allocation for 5 CPU to queue1 and check the reduced numbers
	// root: 10, queue1: 5 and queue2: 5
	alloc, err := resources.NewResourceFromConf(map[string]string{"cpu": "1", "pods": "2"})
	assert.NilError(t, err, "failed to create basic resource")
	var used *resources.Resource
	used, err = resources.NewResourceFromConf(map[string]string{"cpu": "5", "pods": "10"})
	assert.NilError(t, err, "failed to create basic resource")
	testOutstanding(t, alloc, used)
}

func testOutstanding(t *testing.T, alloc, used *resources.Resource) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	var queue1, queue2 *Queue
	queue1, err = createManagedQueue(root, "queue1", false, map[string]string{"cpu": "10"})
	assert.NilError(t, err, "failed to create queue1 queue")
	queue2, err = createManagedQueue(root, "queue2", false, map[string]string{"cpu": "5"})
	assert.NilError(t, err, "failed to create queue2 queue")

	app1 := newApplication(appID1, "default", "root.queue1")
	app1.queue = queue1
	queue1.AddApplication(app1)
	for i := 0; i < 20; i++ {
		err = app1.AddAllocationAsk(
			newAllocationAsk(fmt.Sprintf("alloc-%d", i), appID1, alloc))
		assert.NilError(t, err, "failed to add allocation ask")
	}

	app2 := newApplication(appID2, "default", "root.queue2")
	app2.queue = queue2
	queue2.AddApplication(app2)
	for i := 0; i < 20; i++ {
		err = app2.AddAllocationAsk(
			newAllocationAsk(fmt.Sprintf("alloc-%d", i), appID2, alloc))
		assert.NilError(t, err, "failed to add allocation ask")
	}

	// verify get outstanding requests for root, and child queues
	rootTotal := make([]*AllocationAsk, 0)
	root.GetQueueOutstandingRequests(&rootTotal)
	assert.Equal(t, len(rootTotal), 15)

	queue1Total := make([]*AllocationAsk, 0)
	queue1.GetQueueOutstandingRequests(&queue1Total)
	assert.Equal(t, len(queue1Total), 10)

	queue2Total := make([]*AllocationAsk, 0)
	queue2.GetQueueOutstandingRequests(&queue2Total)
	assert.Equal(t, len(queue2Total), 5)

	// simulate queue1 has some allocated resources
	// after allocation, the max available becomes to be 5
	err = queue1.IncAllocatedResource(used, false)
	assert.NilError(t, err, "failed to increment allocated resources")

	queue1Total = make([]*AllocationAsk, 0)
	queue1.GetQueueOutstandingRequests(&queue1Total)
	assert.Equal(t, len(queue1Total), 5)

	queue2Total = make([]*AllocationAsk, 0)
	queue2.GetQueueOutstandingRequests(&queue2Total)
	assert.Equal(t, len(queue2Total), 5)

	rootTotal = make([]*AllocationAsk, 0)
	root.GetQueueOutstandingRequests(&rootTotal)
	assert.Equal(t, len(rootTotal), 10)

	// remove app2 from queue2
	queue2.RemoveApplication(app2)
	queue2Total = make([]*AllocationAsk, 0)
	queue2.GetQueueOutstandingRequests(&queue2Total)
	assert.Equal(t, len(queue2Total), 0)

	rootTotal = make([]*AllocationAsk, 0)
	root.GetQueueOutstandingRequests(&rootTotal)
	assert.Equal(t, len(rootTotal), 5)
}

func TestGetOutstandingOnlyUntracked(t *testing.T) {
	// all outstanding pods use only an unlimited resource type
	// max is set for a different resource type and fully allocated
	// queue structure:
	// root
	//   - queue1 (max.cpu = 10)
	//
	// submit app1 to root.queue1, app1 asks for 20 1xPOD requests
	// verify the outstanding requests of the queue is all outstanding requests
	// add an allocation that uses all limited resources
	// verify the outstanding requests of the queue is still all outstanding requests
	alloc, err := resources.NewResourceFromConf(map[string]string{"pods": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	var used *resources.Resource
	used, err = resources.NewResourceFromConf(map[string]string{"cpu": "10", "pods": "10"})
	assert.NilError(t, err, "failed to create basic resource")
	var root, queue1 *Queue
	root, err = createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	queue1, err = createManagedQueue(root, "queue1", false, map[string]string{"cpu": "10"})
	assert.NilError(t, err, "failed to create queue1 queue")

	app1 := newApplication(appID1, "default", "root.queue1")
	app1.queue = queue1
	queue1.AddApplication(app1)
	for i := 0; i < 20; i++ {
		err = app1.AddAllocationAsk(
			newAllocationAsk(fmt.Sprintf("alloc-%d", i), appID1, alloc))
		assert.NilError(t, err, "failed to add allocation ask")
	}

	// verify get outstanding requests for root, and child queues
	rootTotal := make([]*AllocationAsk, 0)
	root.GetQueueOutstandingRequests(&rootTotal)
	assert.Equal(t, len(rootTotal), 20)

	queue1Total := make([]*AllocationAsk, 0)
	queue1.GetQueueOutstandingRequests(&queue1Total)
	assert.Equal(t, len(queue1Total), 20)

	// simulate queue1 has some allocated resources
	// after allocation, the max available becomes to be 5
	err = queue1.IncAllocatedResource(used, false)
	assert.NilError(t, err, "failed to increment allocated resources")

	queue1Total = make([]*AllocationAsk, 0)
	queue1.GetQueueOutstandingRequests(&queue1Total)
	assert.Equal(t, len(queue1Total), 20)
	headRoom := queue1.getHeadRoom()
	assert.Assert(t, resources.IsZero(headRoom), "headroom should have been zero")

	rootTotal = make([]*AllocationAsk, 0)
	root.GetQueueOutstandingRequests(&rootTotal)
	assert.Equal(t, len(rootTotal), 20)
	headRoom = root.getHeadRoom()
	assert.Assert(t, resources.IsZero(headRoom), "headroom should have been zero")
}

func TestGetOutstandingRequestNoMax(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	var queue1, queue2 *Queue
	queue1, err = createManagedQueue(root, "queue1", false, nil)
	assert.NilError(t, err, "failed to create queue1 queue")
	queue2, err = createManagedQueue(root, "queue2", false, nil)
	assert.NilError(t, err, "failed to create queue2 queue")

	app1 := newApplication(appID1, "default", "root.queue1")
	app1.queue = queue1
	queue1.AddApplication(app1)
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"cpu": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	for i := 0; i < 10; i++ {
		err = app1.AddAllocationAsk(
			newAllocationAsk(fmt.Sprintf("alloc-%d", i), appID1, res))
		assert.NilError(t, err, "failed to add allocation ask")
	}

	app2 := newApplication(appID2, "default", "root.queue2")
	app2.queue = queue2
	queue2.AddApplication(app2)
	for i := 0; i < 20; i++ {
		err = app2.AddAllocationAsk(
			newAllocationAsk(fmt.Sprintf("alloc-%d", i), appID2, res))
		assert.NilError(t, err, "failed to add allocation ask")
	}

	rootTotal := make([]*AllocationAsk, 0)
	root.GetQueueOutstandingRequests(&rootTotal)
	assert.Equal(t, len(rootTotal), 30)

	queue1Total := make([]*AllocationAsk, 0)
	queue1.GetQueueOutstandingRequests(&queue1Total)
	assert.Equal(t, len(queue1Total), 10)

	queue2Total := make([]*AllocationAsk, 0)
	queue2.GetQueueOutstandingRequests(&queue2Total)
	assert.Equal(t, len(queue2Total), 20)
}

func TestAllocationCalcRoot(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"memory": "100", "vcores": "10"})
	assert.NilError(t, err, "failed to create basic resource")
	err = root.IncAllocatedResource(res, false)
	assert.NilError(t, err, "root queue allocation failed on increment")
	err = root.DecAllocatedResource(res)
	assert.NilError(t, err, "root queue allocation failed on decrement")
	if !resources.IsZero(root.allocatedResource) {
		t.Errorf("root queue allocations are not zero: %v", root.allocatedResource)
	}
	err = root.DecAllocatedResource(res)
	if err == nil {
		t.Error("root queue allocation should have failed to decrement")
	}
}

func TestAllocationCalcSub(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *Queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"memory": "100", "vcores": "10"})
	assert.NilError(t, err, "failed to create basic resource")
	err = parent.IncAllocatedResource(res, false)
	assert.NilError(t, err, "parent queue allocation failed on increment")
	err = parent.DecAllocatedResource(res)
	assert.NilError(t, err, "parent queue allocation failed on decrement")
	if !resources.IsZero(root.allocatedResource) {
		t.Errorf("root queue allocations are not zero: %v", root.allocatedResource)
	}
	err = root.DecAllocatedResource(res)
	if err == nil {
		t.Error("root queue allocation should have failed to decrement")
	}

	// add to the parent, remove from root and then try to remove from parent: root should complain
	err = parent.IncAllocatedResource(res, false)
	assert.NilError(t, err, "parent queue allocation failed on increment")
	err = root.DecAllocatedResource(res)
	assert.NilError(t, err, "root queue allocation failed on decrement")
	err = parent.DecAllocatedResource(res)
	if err == nil {
		t.Errorf("parent queue allocation should have failed on decrement %v, %v", root.allocatedResource, parent.allocatedResource)
	}
}

func TestQueueProps(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *Queue
	props := map[string]string{"first": "value", "second": "other value"}
	parent, err = createManagedQueueWithProps(root, "parent", true, nil, props)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Assert(t, !parent.isLeaf, "parent queue is not marked as a parent")
	assert.Equal(t, len(root.children), 1, "parent queue is not added to the root queue")
	assert.Equal(t, len(parent.properties), 2, "parent queue properties expected 2, got %v", parent.properties)

	var leaf *Queue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, len(parent.children), 1, "leaf queue is not added to the parent queue")
	assert.Assert(t, leaf.isLeaf && leaf.isManaged, "leaf queue is not marked as managed leaf")
	assert.Equal(t, len(leaf.properties), 2, "leaf queue properties size incorrect")

	props = map[string]string{"first": "not inherited", configs.ApplicationSortPolicy: "stateaware"}
	parent, err = createManagedQueueWithProps(root, "parent2", true, nil, props)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Equal(t, len(parent.properties), 2, "parent queue properties size incorrect")
	leaf, err = createDynamicQueue(parent, "leaf", false)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Assert(t, leaf.isLeaf && !leaf.isManaged, "leaf queue is not marked as unmanaged leaf")
	assert.Equal(t, leaf.properties[configs.ApplicationSortPolicy], "stateaware", "leaf queue property value not as expected")

	props = map[string]string{}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.priorityPolicy, policies.DefaultPriorityPolicy)
	assert.Equal(t, leaf.priorityOffset, int32(0))
	assert.Equal(t, leaf.prioritySortEnabled, true)

	props = map[string]string{"priority.policy": "default", "priority.offset": "3", "application.sort.priority": "enabled"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.priorityPolicy, policies.DefaultPriorityPolicy)
	assert.Equal(t, leaf.priorityOffset, int32(3))
	assert.Equal(t, leaf.prioritySortEnabled, true)

	props = map[string]string{"priority.policy": "fence", "priority.offset": "-3", "application.sort.priority": "disabled"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.priorityPolicy, policies.FencePriorityPolicy)
	assert.Equal(t, leaf.priorityOffset, int32(-3))
	assert.Equal(t, leaf.prioritySortEnabled, false)

	props = map[string]string{"priority.policy": "invalid", "priority.offset": "invalid", "application.sort.priority": "invalid"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.priorityPolicy, policies.DefaultPriorityPolicy)
	assert.Equal(t, leaf.priorityOffset, int32(0))
	assert.Equal(t, leaf.prioritySortEnabled, true)

	props = map[string]string{"preemption.policy": "default", "preemption.delay": "10s"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.preemptionPolicy, policies.DefaultPreemptionPolicy)
	assert.Equal(t, leaf.preemptionDelay, time.Second*10)

	props = map[string]string{"preemption.policy": "disabled", "preemption.delay": "-1s"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.preemptionPolicy, policies.DisabledPreemptionPolicy)
	assert.Equal(t, leaf.preemptionDelay, time.Second*30)

	props = map[string]string{"preemption.policy": "fence", "preemption.delay": "xxxx"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.preemptionPolicy, policies.FencePreemptionPolicy)
	assert.Equal(t, leaf.preemptionDelay, time.Second*30)

	props = map[string]string{"preemption.policy": "invalid"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.preemptionPolicy, policies.DefaultPreemptionPolicy)
}

func TestInheritedQueueProps(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *Queue
	props := map[string]string{
		"key":               "value",
		"priority.policy":   "fence",
		"priority.offset":   "100",
		"preemption.policy": "fence",
	}
	parent, err = createManagedQueueWithProps(root, "parent", true, nil, props)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Equal(t, parent.properties["key"], "value")
	assert.Equal(t, parent.properties["priority.policy"], "fence")
	assert.Equal(t, parent.properties["priority.offset"], "100")
	assert.Equal(t, parent.properties["preemption.policy"], "fence")
	assert.Equal(t, parent.priorityPolicy, policies.FencePriorityPolicy)
	assert.Equal(t, parent.priorityOffset, int32(100))
	assert.Equal(t, parent.preemptionPolicy, policies.FencePreemptionPolicy)

	var leaf *Queue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.properties["key"], "value")
	assert.Equal(t, leaf.properties["priority.policy"], "default")
	assert.Equal(t, leaf.properties["priority.offset"], "0")
	assert.Equal(t, leaf.priorityPolicy, policies.DefaultPriorityPolicy)
	assert.Equal(t, leaf.priorityOffset, int32(0))
	assert.Equal(t, leaf.preemptionPolicy, policies.DefaultPreemptionPolicy)

	props = map[string]string{
		"preemption.policy": "disabled",
	}

	parent, err = createManagedQueueWithProps(root, "parent", true, nil, props)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Equal(t, parent.preemptionPolicy, policies.DisabledPreemptionPolicy)

	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.preemptionPolicy, policies.DisabledPreemptionPolicy)
}

func TestMaxResource(t *testing.T) {
	resMap := map[string]string{"first": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create basic resource")
	// create the root
	var root, parent *Queue
	root, err = createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create basic managed parent queue")
	// Nothing set max should be nil
	if root.GetMaxResource() != nil || parent.GetMaxResource() != nil {
		t.Errorf("empty cluster should not have max set on root queue")
	}
	// try setting on the parent (nothing should change)
	parent.SetMaxResource(res)
	if parent.GetMaxResource() != nil {
		t.Errorf("parent queue change should have been rejected parent: %v", parent.GetMaxResource())
	}
	// Set on the root should change
	root.SetMaxResource(res)
	if !resources.Equals(res, root.GetMaxResource()) {
		t.Errorf("root max setting not picked up by parent queue expected %v, got %v", res, parent.GetMaxResource())
	}
}

func TestGetQueueInfo(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue: %v", err)
	var rootMax *resources.Resource
	rootMax, err = resources.NewResourceFromConf(map[string]string{"memory": "2048", "vcores": "10"})
	assert.NilError(t, err, "failed to create configuration: %v", err)
	root.SetMaxResource(rootMax)

	var parentUsed *resources.Resource
	parentUsed, err = resources.NewResourceFromConf(map[string]string{"memory": "1012", "vcores": "2"})
	assert.NilError(t, err, "failed to create resource: %v", err)
	var parent *Queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	err = parent.IncAllocatedResource(parentUsed, false)
	assert.NilError(t, err, "failed to increment allocated resource: %v", err)

	var child1used *resources.Resource
	child1used, err = resources.NewResourceFromConf(map[string]string{"memory": "1012", "vcores": "2"})
	assert.NilError(t, err, "failed to create resource: %v", err)
	var child1 *Queue
	child1, err = createManagedQueue(parent, "child1", true, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	err = child1.IncAllocatedResource(child1used, false)
	assert.NilError(t, err, "failed to increment allocated resource: %v", err, err)

	var child2 *Queue
	child2, err = createManagedQueue(parent, "child2", true, nil)
	assert.NilError(t, err, "failed to create child queue: %v", err)
	child2.SetMaxResource(resources.NewResource())
	rootDaoInfo := root.GetQueueInfo()

	compareQueueInfoWithDAO(t, root, rootDaoInfo)
	parentDaoInfo := rootDaoInfo.ChildQueues[0]
	compareQueueInfoWithDAO(t, parent, parentDaoInfo)
	for _, childDao := range parentDaoInfo.ChildQueues {
		name := childDao.QueueName
		child := parent.children[name]
		if child == nil {
			t.Fail()
		}
		compareQueueInfoWithDAO(t, child, childDao)
	}
}

func TestGetQueueInfoPropertiesSet(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue: %v", err)

	// managed parent queue with property set
	properties := map[string]string{configs.ApplicationSortPolicy: "fifo"}
	var parent *Queue
	parent, err = createManagedQueueWithProps(root, "parent", true, nil, properties)
	assert.NilError(t, err, "failed to create queue: %v", err)

	// managed leaf queue with some properties set
	properties = map[string]string{configs.ApplicationSortPolicy: "fair",
		"some_property": "some_value"}
	var _ *Queue
	_, err = createManagedQueueWithProps(parent, "child1", true, nil, properties)
	assert.NilError(t, err, "failed to create queue: %v", err)

	// managed leaf queue with some properties set
	properties = map[string]string{configs.ApplicationSortPolicy: "state_aware"}
	_, err = createManagedQueueWithProps(parent, "child2", true, nil, properties)
	assert.NilError(t, err, "failed to create child queue: %v", err)

	// dynamic leaf queue picks up from parent
	_, err = createDynamicQueue(parent, "child3", true)
	assert.NilError(t, err, "failed to create child queue: %v", err)

	rootDaoInfo := root.GetQueueInfo()

	compareQueueInfoWithDAO(t, root, rootDaoInfo)
	parentDaoInfo := rootDaoInfo.ChildQueues[0]
	compareQueueInfoWithDAO(t, parent, parentDaoInfo)
	for _, childDao := range parentDaoInfo.ChildQueues {
		name := childDao.QueueName
		child := parent.children[name]
		if child == nil {
			t.Fail()
		}
		compareQueueInfoWithDAO(t, child, childDao)
	}
}

func compareQueueInfoWithDAO(t *testing.T, queue *Queue, dao dao.QueueDAOInfo) {
	assert.Equal(t, queue.Name, dao.QueueName)
	assert.Equal(t, len(queue.children), len(dao.ChildQueues))
	assert.Equal(t, queue.stateMachine.Current(), dao.Status)
	emptyRes := map[string]int64{}
	if queue.allocatedResource == nil {
		assert.DeepEqual(t, emptyRes, dao.Capacities.UsedCapacity)
	} else {
		assert.DeepEqual(t, queue.allocatedResource.DAOMap(), dao.Capacities.UsedCapacity)
	}
	if queue.maxResource == nil {
		assert.DeepEqual(t, emptyRes, dao.Capacities.MaxCapacity)
	} else {
		assert.DeepEqual(t, queue.maxResource.DAOMap(), dao.Capacities.MaxCapacity)
	}
	if queue.guaranteedResource == nil {
		assert.DeepEqual(t, emptyRes, dao.Capacities.Capacity)
	} else {
		assert.DeepEqual(t, queue.guaranteedResource.DAOMap(), dao.Capacities.Capacity)
	}
	assert.Equal(t, len(queue.properties), len(dao.Properties))
	if len(queue.properties) > 0 {
		for k, v := range queue.properties {
			assert.Equal(t, v, dao.Properties[k])
		}
	}
}

func TestSupportTaskGroup(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue: %v", err)
	assert.Assert(t, !root.SupportTaskGroup(), "root queue should not support task group (parent)")

	// parent with sort policy set
	properties := map[string]string{configs.ApplicationSortPolicy: "fifo"}
	var parent *Queue
	parent, err = createManagedQueueWithProps(root, "parent", true, nil, properties)
	assert.NilError(t, err, "failed to create queue: %v", err)
	assert.Assert(t, !parent.SupportTaskGroup(), "parent queue (FIFO policy) should not support task group")

	var leaf *Queue
	leaf, err = createManagedQueueWithProps(parent, "leaf1", false, nil, properties)
	assert.NilError(t, err, "failed to create queue: %v", err)
	assert.Assert(t, leaf.SupportTaskGroup(), "leaf queue (FIFO policy) should support task group")

	properties = map[string]string{configs.ApplicationSortPolicy: "StateAware"}
	leaf, err = createManagedQueueWithProps(parent, "leaf2", false, nil, properties)
	assert.NilError(t, err, "failed to create queue: %v", err)
	assert.Assert(t, leaf.SupportTaskGroup(), "leaf queue (StateAware policy) should support task group")

	properties = map[string]string{configs.ApplicationSortPolicy: "fair"}
	leaf, err = createManagedQueueWithProps(parent, "leaf3", false, nil, properties)
	assert.NilError(t, err, "failed to create queue: %v", err)
	assert.Assert(t, !leaf.SupportTaskGroup(), "leaf queue (FAIR policy) should not support task group")
}

func TestGetPartitionQueueDAOInfo(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue: %v", err)

	// test properties
	root.properties = getProperties()
	assert.Assert(t, reflect.DeepEqual(root.properties, root.GetPartitionQueueDAOInfo().Properties))

	// test template
	root.template, err = template.FromConf(&configs.ChildTemplate{
		Properties: getProperties(),
		Resources: configs.Resources{
			Max:        getResourceConf(),
			Guaranteed: getResourceConf(),
		},
	})
	assert.NilError(t, err)
	assert.Assert(t, reflect.DeepEqual(root.template.GetProperties(), root.GetPartitionQueueDAOInfo().TemplateInfo.Properties))
	assert.DeepEqual(t, root.template.GetMaxResource().DAOMap(), root.template.GetMaxResource().DAOMap())
	assert.DeepEqual(t, root.template.GetGuaranteedResource().DAOMap(), root.template.GetGuaranteedResource().DAOMap())

	// test resources
	root.maxResource = getResource(t)
	root.guaranteedResource = getResource(t)
	assert.DeepEqual(t, root.GetMaxResource().DAOMap(), root.GetMaxResource().DAOMap())
	assert.DeepEqual(t, root.GetGuaranteedResource().DAOMap(), root.GetGuaranteedResource().DAOMap())

	// test allocatingAcceptedApps
	root.allocatingAcceptedApps = getAllocatingAcceptedApps()
	assert.Equal(t, len(root.allocatingAcceptedApps), 2, "allocatingAcceptedApps size")
	assert.Equal(t, len(root.GetPartitionQueueDAOInfo().AllocatingAcceptedApps), 1, "AllocatingAcceptedApps size")
	assert.Equal(t, root.GetPartitionQueueDAOInfo().AllocatingAcceptedApps[0], appID1)
}

func getAllocatingAcceptedApps() map[string]bool {
	allocatingAcceptedApps := make(map[string]bool)
	allocatingAcceptedApps[appID1] = true
	allocatingAcceptedApps[appID2] = false
	return allocatingAcceptedApps
}

func getResourceConf() map[string]string {
	resource := make(map[string]string)
	resource["memory"] = strconv.Itoa(time.Now().Second()%1000 + 100)
	return resource
}

func getResource(t *testing.T) *resources.Resource {
	r, err := resources.NewResourceFromConf(getResourceConf())
	assert.NilError(t, err, "failed to parse resource: %v", err)
	return r
}

func getZeroResourceConf() map[string]string {
	resource := make(map[string]string)
	resource["memory"] = "0"
	resource["vcore"] = "0"
	return resource
}

func getProperties() map[string]string {
	properties := make(map[string]string)
	properties[strconv.Itoa(time.Now().Second())] = strconv.Itoa(time.Now().Second())
	return properties
}

func TestSetResources(t *testing.T) {
	queue, err := createManagedQueueWithProps(nil, "tmp", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue queue: %v", err)

	guaranteedResource := getResourceConf()
	maxResource := getResourceConf()

	// case 0: normal case
	err = queue.setResources(configs.Resources{
		Guaranteed: guaranteedResource,
		Max:        maxResource,
	})
	assert.NilError(t, err, "failed to set resources: %v", err)

	expectedGuaranteedResource, err := resources.NewResourceFromConf(guaranteedResource)
	assert.NilError(t, err, "failed to parse resource: %v", err)
	assert.Assert(t, reflect.DeepEqual(queue.guaranteedResource, expectedGuaranteedResource))

	expectedMaxResource, err := resources.NewResourceFromConf(maxResource)
	assert.NilError(t, err, "failed to parse resource: %v", err)
	assert.Assert(t, reflect.DeepEqual(queue.maxResource, expectedMaxResource))

	// case 1: empty resource won't change the resources
	err = queue.setResources(configs.Resources{
		Guaranteed: make(map[string]string),
		Max:        make(map[string]string),
	})
	assert.NilError(t, err, "failed to set resources: %v", err)
	assert.Assert(t, reflect.DeepEqual(queue.guaranteedResource, expectedGuaranteedResource))
	assert.Assert(t, reflect.DeepEqual(queue.maxResource, expectedMaxResource))

	// case 2: zero resource won't change the resources
	err = queue.setResources(configs.Resources{
		Guaranteed: getZeroResourceConf(),
		Max:        getZeroResourceConf(),
	})
	assert.NilError(t, err, "failed to set resources: %v", err)
	assert.Assert(t, reflect.DeepEqual(queue.guaranteedResource, expectedGuaranteedResource))
	assert.Assert(t, reflect.DeepEqual(queue.maxResource, expectedMaxResource))
}

func TestPreemptingResource(t *testing.T) {
	one, err := resources.NewResourceFromConf(map[string]string{"memory": "10000000", "vcores": "1"})
	assert.NilError(t, err, "failed to create resource")
	two, err := resources.NewResourceFromConf(map[string]string{"memory": "20000000", "vcores": "2"})
	assert.NilError(t, err, "failed to create resource")
	three, err := resources.NewResourceFromConf(map[string]string{"memory": "30000000", "vcores": "3"})
	assert.NilError(t, err, "failed to create resource")

	queue, err := createManagedQueueWithProps(nil, "tmp", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue queue: %v", err)

	assert.Check(t, resources.IsZero(queue.GetPreemptingResource()), "initial value should be zero")
	queue.IncPreemptingResource(one)
	assert.Check(t, resources.Equals(one, queue.GetPreemptingResource()), "wrong value after increment")
	queue.IncPreemptingResource(two)
	assert.Check(t, resources.Equals(three, queue.GetPreemptingResource()), "wrong value after increment")
	queue.DecPreemptingResource(one)
	assert.Check(t, resources.Equals(two, queue.GetPreemptingResource()), "wrong value after decrement")
	queue.DecPreemptingResource(two)
	assert.Check(t, resources.IsZero(queue.GetPreemptingResource()), "final value should be zero")
}

func TestPreemptionDelay(t *testing.T) {
	queue, err := createManagedQueueWithProps(nil, "tmp", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue queue: %v", err)

	assert.Equal(t, configs.DefaultPreemptionDelay, queue.GetPreemptionDelay(), "initial preemption delay incorrect")

	twice := 2 * configs.DefaultPreemptionDelay
	queue.preemptionDelay = twice
	assert.Equal(t, twice, queue.GetPreemptionDelay(), "preemption delay not updated correctly")
}

func TestFindQueueByAppID(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create queue")
	parent1, err := createManagedQueue(root, "parent1", true, nil)
	assert.NilError(t, err, "failed to create queue")
	parent2, err := createManagedQueue(root, "parent2", true, nil)
	assert.NilError(t, err, "failed to create queue")
	leaf1, err := createManagedQueue(parent1, "leaf1", false, nil)
	assert.NilError(t, err, "failed to create queue")
	leaf2, err := createManagedQueue(parent2, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create queue")

	app := newApplication(appID1, "default", "root.parent.leaf")
	app.pending = resources.NewResourceFromMap(map[string]resources.Quantity{common.Memory: 10})
	leaf1.AddApplication(app)

	// we should be able to find the queue from any other given the appID
	assert.Equal(t, leaf1, root.FindQueueByAppID(appID1), "failed to find queue from root")
	assert.Equal(t, leaf1, parent1.FindQueueByAppID(appID1), "failed to find queue from parent1")
	assert.Equal(t, leaf1, parent2.FindQueueByAppID(appID1), "failed to find queue from parent2")
	assert.Equal(t, leaf1, leaf1.FindQueueByAppID(appID1), "failed to find queue from leaf1")
	assert.Equal(t, leaf1, leaf2.FindQueueByAppID(appID1), "failed to find queue from leaf2")

	// non-existent queue should be nil
	var none *Queue = nil
	assert.Equal(t, none, root.FindQueueByAppID("missing"), "found queue reference in root")
	assert.Equal(t, none, parent1.FindQueueByAppID("missing"), "found queue reference in parent1")
	assert.Equal(t, none, parent2.FindQueueByAppID("missing"), "found queue reference in parent2")
	assert.Equal(t, none, leaf1.FindQueueByAppID("missing"), "found queue reference in leaf1")
	assert.Equal(t, none, leaf2.FindQueueByAppID("missing"), "found queue reference in leaf2")
}

// nolint: funlen
func TestFindEligiblePreemptionVictims(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{common.Memory: 100})
	parentMax := map[string]string{common.Memory: "200"}
	parentGuar := map[string]string{common.Memory: "100"}
	ask := createAllocationAsk("ask1", appID1, true, true, 0, res)
	ask.pendingAskRepeat = 1
	ask2 := createAllocationAsk("ask2", appID2, true, true, -1000, res)
	ask2.pendingAskRepeat = 1
	alloc2 := NewAllocation("alloc-2", nodeID1, ask2)
	ask3 := createAllocationAsk("ask3", appID2, true, true, -1000, res)
	ask3.pendingAskRepeat = 1
	alloc3 := NewAllocation("alloc-3", nodeID1, ask3)
	root, err := createRootQueue(map[string]string{common.Memory: "1000"})
	assert.NilError(t, err, "failed to create queue")
	parent1, err := createManagedQueueGuaranteed(root, "parent1", true, parentMax, parentGuar)
	assert.NilError(t, err, "failed to create queue")
	parent2, err := createManagedQueueGuaranteed(root, "parent2", true, parentMax, parentGuar)
	assert.NilError(t, err, "failed to create queue")
	leaf1, err := createManagedQueueGuaranteed(parent1, "leaf1", false, nil, nil)
	assert.NilError(t, err, "failed to create queue")
	leaf2, err := createManagedQueueGuaranteed(parent2, "leaf2", false, nil, nil)
	assert.NilError(t, err, "failed to create queue")

	// verify no victims when no allocations exist
	snapshot := leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 3, len(snapshot), "wrong snapshot count") // leaf1, parent1, root
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")

	// add two lower-priority allocs in leaf2
	app2 := newApplication(appID2, "default", "root.parent.leaf")
	app2.pending = res
	leaf2.AddApplication(app2)
	app2.SetQueue(leaf2)
	err = app2.AddAllocationAsk(ask2)
	assert.NilError(t, err, "failed to add ask")
	err = app2.AddAllocationAsk(ask3)
	assert.NilError(t, err, "failed to add ask")
	app2.AddAllocation(alloc2)
	app2.AddAllocation(alloc3)
	err = leaf2.IncAllocatedResource(alloc2.allocatedResource, false)
	assert.NilError(t, err, "failed to inc allocated resources")
	err = leaf2.IncAllocatedResource(alloc3.allocatedResource, false)
	assert.NilError(t, err, "failed to inc allocated resources")

	// verify victims
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 5, len(snapshot), "wrong snapshot count") // leaf1, parent1, root
	assert.Equal(t, 2, len(victims(snapshot)), "wrong victim count")
	assert.Equal(t, alloc2.allocationKey, victims(snapshot)[0].allocationKey, "wrong alloc")
	assert.Equal(t, alloc3.allocationKey, victims(snapshot)[1].allocationKey, "wrong alloc")

	// disabling preemption on victim queue should remove victims from consideration
	leaf2.preemptionPolicy = policies.DisabledPreemptionPolicy
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	leaf2.preemptionPolicy = policies.DefaultPreemptionPolicy

	// fencing parent1 queue should limit scope
	parent1.preemptionPolicy = policies.FencePreemptionPolicy
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	parent1.preemptionPolicy = policies.DefaultPreemptionPolicy

	// fencing leaf1 queue should limit scope
	leaf1.preemptionPolicy = policies.FencePreemptionPolicy
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	leaf1.preemptionPolicy = policies.DefaultPreemptionPolicy

	// fencing parent2 queue should not limit scope
	parent2.preemptionPolicy = policies.FencePreemptionPolicy
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 2, len(victims(snapshot)), "wrong victim count")
	parent2.preemptionPolicy = policies.DefaultPreemptionPolicy

	// fencing leaf2 queue should not limit scope
	leaf2.preemptionPolicy = policies.FencePreemptionPolicy
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 2, len(victims(snapshot)), "wrong victim count")
	leaf2.preemptionPolicy = policies.DefaultPreemptionPolicy

	// requiring a specific node take alloc out of consideration
	alloc2.GetAsk().SetRequiredNode(nodeID1)
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 1, len(victims(snapshot)), "wrong victim count")
	assert.Equal(t, alloc3.allocationKey, victims(snapshot)[0].allocationKey, "wrong alloc")
	alloc2.GetAsk().SetRequiredNode("")

	// setting priority offset on parent2 queue should remove leaf2 victims
	parent2.priorityOffset = 1001
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	parent2.priorityOffset = 0

	// priority-fencing parent2 with positive offset should remove leaf2 victims
	parent2.priorityOffset = 1
	parent2.priorityPolicy = policies.FencePriorityPolicy
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	parent2.priorityOffset = 0
	parent2.priorityPolicy = policies.DefaultPriorityPolicy

	// priority-fencing parent2 with negative offset should not affect leaf2 victims
	parent2.priorityOffset = -1
	parent2.priorityPolicy = policies.FencePriorityPolicy
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 2, len(victims(snapshot)), "wrong victim count")
	parent2.priorityOffset = 0
	parent2.priorityPolicy = policies.DefaultPriorityPolicy

	// priority-fencing parent1 with small negative offset should not affect leaf2 victims
	parent1.priorityOffset = -1000
	parent1.priorityPolicy = policies.FencePriorityPolicy
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 2, len(victims(snapshot)), "wrong victim count")
	parent1.priorityOffset = 0
	parent1.priorityPolicy = policies.DefaultPriorityPolicy

	// priority-fencing parent1 with larger negative offset should remove leaf2 victims
	parent1.priorityOffset = -1001
	parent1.priorityPolicy = policies.FencePriorityPolicy
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	parent1.priorityOffset = 0
	parent1.priorityPolicy = policies.DefaultPriorityPolicy

	// increasing parent2 guaranteed resources should remove leaf2 victims
	parent2.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{common.Memory: 200})
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	parent2.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{common.Memory: 100})
}

func victims(snapshot map[string]*QueuePreemptionSnapshot) []*Allocation {
	results := make([]*Allocation, 0)
	for _, entry := range snapshot {
		results = append(results, entry.PotentialVictims...)
	}
	sort.SliceStable(results, func(i, j int) bool {
		return results[i].allocationKey < results[j].allocationKey
	})
	return results
}

func TestSetTemplate(t *testing.T) {
	queue, err := createManagedQueueWithProps(nil, "tmp", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue queue: %v", err)

	properties := getProperties()
	guaranteedResource := getResourceConf()
	expectedGuaranteedResource, err := resources.NewResourceFromConf(guaranteedResource)
	assert.NilError(t, err, "failed to parse resource: %v", err)
	maxResource := getResourceConf()
	expectedMaxResource, err := resources.NewResourceFromConf(maxResource)
	assert.NilError(t, err, "failed to parse resource: %v", err)

	checkTemplate := func(queue *Queue) {
		assert.Assert(t, reflect.DeepEqual(queue.template.GetProperties(), properties))
		assert.Assert(t, reflect.DeepEqual(queue.template.GetGuaranteedResource(), expectedGuaranteedResource))
		assert.Assert(t, reflect.DeepEqual(queue.template.GetMaxResource(), expectedMaxResource))
	}

	// case 0: normal case
	err = queue.setTemplate(configs.ChildTemplate{
		Properties: properties,
		Resources: configs.Resources{
			Guaranteed: guaranteedResource,
			Max:        maxResource,
		},
	})
	assert.NilError(t, err, "failed to set resources: %v", err)
	checkTemplate(queue)

	// case 1: empty config does nothing
	err = queue.setTemplate(configs.ChildTemplate{
		Properties: make(map[string]string),
		Resources: configs.Resources{
			Guaranteed: make(map[string]string),
			Max:        make(map[string]string),
		},
	})
	assert.NilError(t, err)
	assert.Assert(t, queue.template == nil)
}

func TestApplyTemplate(t *testing.T) {
	childTemplate, err := template.FromConf(&configs.ChildTemplate{
		Properties: getProperties(),
		Resources: configs.Resources{
			Max:        getResourceConf(),
			Guaranteed: getResourceConf(),
		},
	})
	assert.NilError(t, err)

	// case 0: leaf queue can apply template
	leaf, err := createManagedQueueWithProps(nil, "tmp", false, nil, nil)
	assert.NilError(t, err, "failed to create basic queue queue: %v", err)
	leaf.applyTemplate(childTemplate)
	assert.Assert(t, leaf.template == nil)
	assert.Assert(t, reflect.DeepEqual(leaf.properties, childTemplate.GetProperties()))
	assert.Assert(t, reflect.DeepEqual(leaf.guaranteedResource, childTemplate.GetGuaranteedResource()))
	assert.Assert(t, reflect.DeepEqual(leaf.maxResource, childTemplate.GetMaxResource()))

	// case 1: zero resource template generates nil resource
	leaf2, err := createManagedQueueWithProps(nil, "tmp", false, nil, nil)
	assert.NilError(t, err, "failed to create basic queue queue: %v", err)
	zeroTemplate, err := template.FromConf(&configs.ChildTemplate{
		Properties: getProperties(),
		Resources: configs.Resources{
			Max:        getZeroResourceConf(),
			Guaranteed: getZeroResourceConf(),
		},
	})
	assert.NilError(t, err)
	leaf2.applyTemplate(zeroTemplate)
	assert.Assert(t, leaf2.template == nil)
	assert.Assert(t, leaf2.maxResource == nil)
	assert.Assert(t, leaf2.guaranteedResource == nil)
}

func TestApplyConf(t *testing.T) {
	// cover error cases
	errQueue, err := createManagedQueueWithProps(nil, "errConf", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	// wrong submitACL
	errConf1 := configs.QueueConfig{
		SubmitACL: "error Submit ACL",
	}
	err = errQueue.ApplyConf(errConf1)
	assert.ErrorContains(t, err, "multiple spaces found in ACL")

	// wrong AdminACL
	errConf2 := configs.QueueConfig{
		AdminACL: "error Admin ACL",
	}
	err = errQueue.ApplyConf(errConf2)
	assert.ErrorContains(t, err, "multiple spaces found in ACL")

	// isManaged is changed from unmanaged to managed
	childConf := configs.QueueConfig{}

	parent, err := createManagedQueueWithProps(nil, "parent", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	child, err := NewDynamicQueue("child", true, parent)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	err = child.ApplyConf(childConf)
	assert.NilError(t, err, "failed to parse conf: %v", err)
	assert.Equal(t, child.IsManaged(), true)

	// isLeaf is set to false while Queues length > 0
	parentConf := configs.QueueConfig{
		Queues: []configs.QueueConfig{
			{
				Name:   "child",
				Parent: true,
				Queues: nil,
			},
		},
	}
	parent, err = createManagedQueueWithProps(nil, "parent", false, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	err = parent.ApplyConf(parentConf)
	assert.NilError(t, err, "failed to parse conf: %v", err)
	assert.Equal(t, parent.IsLeafQueue(), false)

	conf := configs.QueueConfig{
		SubmitACL: "",
		AdminACL:  "",
		Resources: configs.Resources{
			Max:        getResourceConf(),
			Guaranteed: getResourceConf(),
		},
		ChildTemplate: configs.ChildTemplate{
			Properties: make(map[string]string),
			Resources: configs.Resources{
				Max:        getResourceConf(),
				Guaranteed: getResourceConf(),
			},
		},
	}

	// case 0: leaf can't set template
	leaf, err := createManagedQueueWithProps(nil, "tmp", false, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	validTemplate, err := template.FromConf(&conf.ChildTemplate)
	assert.NilError(t, err, "failed to parse conf: %v", err)
	assert.Assert(t, validTemplate != nil)

	conf.Parent = false
	err = leaf.ApplyConf(conf)
	assert.NilError(t, err, "failed to apply conf: %v", err)
	assert.Assert(t, leaf.template == nil)
	assert.Assert(t, leaf.maxResource != nil)
	assert.Assert(t, leaf.guaranteedResource != nil)

	// case 1-1: non-leaf queue can have template
	queue, err := createManagedQueueWithProps(nil, "tmp", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	conf.Parent = true
	err = queue.ApplyConf(conf)
	assert.NilError(t, err, "failed to apply conf: %v", err)
	assert.Assert(t, queue.template != nil)
	assert.Assert(t, queue.maxResource != nil)
	assert.Assert(t, queue.guaranteedResource != nil)

	// root can't set resources
	root, err := createManagedQueueWithProps(nil, "root", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	err = queue.ApplyConf(conf)
	assert.NilError(t, err, "failed to apply conf: %v", err)
	assert.Assert(t, root.maxResource == nil)
	assert.Assert(t, root.guaranteedResource == nil)
}

func TestNewConfiguredQueue(t *testing.T) {
	// check variable assignment
	properties := getProperties()
	resourceConf := getResourceConf()
	// turn resouce config into resource struct
	resourceStruct, err := resources.NewResourceFromConf(resourceConf)
	assert.NilError(t, err, "failed to create new resource from config: %v", err)

	parentConfig := configs.QueueConfig{
		Name:            "PARENT_QUEUE",
		Parent:          true,
		MaxApplications: uint64(32),
		ChildTemplate: configs.ChildTemplate{
			Properties: properties,
			Resources: configs.Resources{
				Max:        resourceConf,
				Guaranteed: resourceConf,
			},
		},
	}
	parent, err := NewConfiguredQueue(parentConfig, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	assert.Equal(t, parent.Name, "parent_queue")
	assert.Equal(t, parent.QueuePath, "parent_queue")
	assert.Equal(t, parent.isManaged, true)
	assert.Equal(t, parent.maxRunningApps, uint64(32))
	assert.Assert(t, reflect.DeepEqual(properties, parent.template.GetProperties()))
	assert.Assert(t, resources.Equals(resourceStruct, parent.template.GetMaxResource()))
	assert.Assert(t, resources.Equals(resourceStruct, parent.template.GetGuaranteedResource()))

	// case 0: leaf can use template
	leafConfig := configs.QueueConfig{
		Name:       "leaf_queue",
		Parent:     false,
		Properties: getProperties(),
		Resources: configs.Resources{
			Max:        getResourceConf(),
			Guaranteed: getResourceConf(),
		},
	}
	childLeaf, err := NewConfiguredQueue(leafConfig, parent)
	assert.NilError(t, err, "failed to create queue: %v", err)
	assert.Equal(t, childLeaf.QueuePath, "parent_queue.leaf_queue")
	assert.Assert(t, childLeaf.template == nil)
	assert.Assert(t, reflect.DeepEqual(childLeaf.properties, parent.template.GetProperties()))
	assert.Assert(t, resources.Equals(childLeaf.maxResource, parent.template.GetMaxResource()))
	assert.Assert(t, resources.Equals(childLeaf.guaranteedResource, parent.template.GetGuaranteedResource()))

	// case 1: non-leaf can't use template but it can inherit template from parent
	NonLeafConfig := configs.QueueConfig{
		Name:   "nonleaf_queue",
		Parent: true,
	}
	childNonLeaf, err := NewConfiguredQueue(NonLeafConfig, parent)
	assert.NilError(t, err, "failed to create queue: %v", err)
	assert.Equal(t, childNonLeaf.QueuePath, "parent_queue.nonleaf_queue")
	assert.Assert(t, reflect.DeepEqual(childNonLeaf.template, parent.template))
	assert.Equal(t, len(childNonLeaf.properties), 0)
	assert.Assert(t, childNonLeaf.guaranteedResource == nil)
	assert.Assert(t, childNonLeaf.maxResource == nil)
}

func TestNewDynamicQueue(t *testing.T) {
	parent, err := createManagedQueueWithProps(nil, "parent", true, nil, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	parent.template, err = template.FromConf(&configs.ChildTemplate{
		Properties: getProperties(),
		Resources: configs.Resources{
			Max:        getResourceConf(),
			Guaranteed: getResourceConf(),
		},
	})
	assert.NilError(t, err)

	// case 0: leaf can use template
	childLeaf, err := NewDynamicQueue("leaf", true, parent)
	assert.NilError(t, err, "failed to create dynamic queue: %v", err)
	assert.Assert(t, childLeaf.template == nil)
	assert.Assert(t, reflect.DeepEqual(childLeaf.properties, parent.template.GetProperties()))
	assert.Assert(t, reflect.DeepEqual(childLeaf.maxResource, parent.template.GetMaxResource()))
	assert.Assert(t, reflect.DeepEqual(childLeaf.guaranteedResource, parent.template.GetGuaranteedResource()))
	assert.Assert(t, childLeaf.prioritySortEnabled)
	assert.Equal(t, childLeaf.priorityPolicy, policies.DefaultPriorityPolicy)
	assert.Equal(t, childLeaf.preemptionPolicy, policies.DefaultPreemptionPolicy)

	// case 1: non-leaf can't use template but it can inherit template from parent
	childNonLeaf, err := NewDynamicQueue("nonleaf", false, parent)
	assert.NilError(t, err, "failed to create dynamic queue: %v", err)
	assert.Assert(t, reflect.DeepEqual(childNonLeaf.template, parent.template))
	assert.Equal(t, len(childNonLeaf.properties), 0)
	assert.Assert(t, childNonLeaf.guaranteedResource == nil)
	assert.Assert(t, childNonLeaf.maxResource == nil)
	assert.Assert(t, childNonLeaf.prioritySortEnabled)
	assert.Equal(t, childNonLeaf.priorityPolicy, policies.DefaultPriorityPolicy)
	assert.Equal(t, childNonLeaf.preemptionPolicy, policies.DefaultPreemptionPolicy)
}

func TestTemplateIsNotOverrideByParent(t *testing.T) {
	parent, err := createManagedQueueWithProps(nil, "parent", true, nil, nil)
	assert.NilError(t, err)
	parent.template, err = template.FromConf(&configs.ChildTemplate{
		Properties: map[string]string{
			"k": "v",
		},
	})
	assert.NilError(t, err)

	leaf, err := createManagedQueueWithProps(nil, "leaf", false, nil, nil)
	assert.NilError(t, err)
	leaf.template, err = template.FromConf(&configs.ChildTemplate{
		Properties: map[string]string{
			"k0": "v0",
		},
	})
	assert.NilError(t, err)

	err = parent.addChildQueue(leaf)
	assert.NilError(t, err)

	assert.Assert(t, !reflect.DeepEqual(leaf.template, parent.template))
}

func TestGetHeadRoomFromTwoQueues(t *testing.T) {
	allocatedResource := &resources.Resource{
		Resources: map[string]resources.Quantity{
			"vcore":  2000,
			"memory": 2000,
		},
	}

	parent, err := createManagedQueueWithProps(nil, "parent", true, nil, nil)
	assert.NilError(t, err)
	parent.maxResource = &resources.Resource{
		Resources: map[string]resources.Quantity{
			"vcore":  3000,
			"memory": 3000,
		},
	}
	// make sure parent queue see all allocated resources
	parent.allocatedResource = allocatedResource

	// this child is not set with max memory, so it should follow parent max memory
	child, err := createManagedQueueWithProps(parent, "child", true, nil, nil)
	assert.NilError(t, err)
	child.maxResource = &resources.Resource{
		Resources: map[string]resources.Quantity{
			"vcore": 3000,
		},
	}
	child.allocatedResource = allocatedResource

	result := child.getHeadRoom()

	assert.Equal(t, resources.Quantity(1000), result.Resources["vcore"])
	assert.Equal(t, resources.Quantity(1000), result.Resources["memory"])
}

func TestGetHeadRoomFromThreeQueues(t *testing.T) {
	allocatedResource := &resources.Resource{
		Resources: map[string]resources.Quantity{
			"vcore":  2000,
			"memory": 2000,
			"pods":   1,
			"other":  1,
		},
	}

	rootQueue, err := createManagedQueueWithProps(nil, "rootQueue", true, nil, nil)
	assert.NilError(t, err)
	rootQueue.maxResource = &resources.Resource{
		Resources: map[string]resources.Quantity{
			"vcore":  3000,
			"memory": 3000,
			"other":  100,
		},
	}
	// make sure rootQueue queue see all allocated resources
	rootQueue.allocatedResource = allocatedResource

	parent, err := createManagedQueueWithProps(rootQueue, "parent", true, nil, nil)
	assert.NilError(t, err)
	// this parent has a limit for one specific resource
	parent.maxResource = &resources.Resource{
		Resources: make(map[string]resources.Quantity),
	}
	parent.maxResource = &resources.Resource{
		Resources: map[string]resources.Quantity{
			"pods": 100,
		},
	}
	// make sure parent queue see all allocated resources
	parent.allocatedResource = allocatedResource

	// this child is not set with max memory, so it should follow rootQueue max memory
	child, err := createManagedQueueWithProps(parent, "child", true, nil, nil)
	assert.NilError(t, err)
	child.maxResource = &resources.Resource{
		Resources: map[string]resources.Quantity{
			"vcore": 3000,
		},
	}
	child.allocatedResource = allocatedResource

	result := child.getHeadRoom()

	assert.Equal(t, resources.Quantity(1000), result.Resources["vcore"])
	assert.Equal(t, resources.Quantity(1000), result.Resources["memory"])
	assert.Equal(t, resources.Quantity(99), result.Resources["pods"])
	assert.Equal(t, resources.Quantity(99), result.Resources["other"])
}

func TestQueue_canRunApp(t *testing.T) {
	// create the root
	root, err := createManagedQueueMaxApps(nil, "root", true, nil, 1)
	assert.NilError(t, err, "queue create failed")
	var leaf, leaf2 *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	leaf2, err = createManagedQueue(root, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	// ignore allocatingAcceptedApps
	assert.Assert(t, leaf.canRunApp(""), "unlimited queue should be able to run app")
	assert.Assert(t, root.canRunApp(""), "queue should be able to run app (root max is 1)")
	root.incRunningApps("")
	assert.Assert(t, !leaf.canRunApp(""), "running apps max reached on root, should be denied")
	root.maxRunningApps = 2
	assert.Assert(t, leaf.canRunApp(""), "root and leave allowed")
	leaf.maxRunningApps = 1
	leaf.incRunningApps("")
	assert.Assert(t, !leaf.canRunApp(""), "leaf should not be able to run an application")

	leaf2.incRunningApps("")
	assert.Assert(t, !leaf2.canRunApp(""), "leaf2 should not be able to run an application (root max reached)")

	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil queue canRunApp")
		}
	}()
	var q *Queue
	q.canRunApp("")
}

func TestQueue_incRunningApps(t *testing.T) {
	// create the root
	root, err := createManagedQueueMaxApps(nil, "root", true, nil, 2)
	assert.NilError(t, err, "queue create failed")
	var leaf, leaf2 *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	leaf2, err = createManagedQueue(root, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	assert.Equal(t, leaf.runningApps, uint64(0), "default max running apps is 0")
	root.incRunningApps("")
	assert.Equal(t, root.runningApps, uint64(1), "root should have 1 app running")
	leaf.incRunningApps("")
	assert.Equal(t, leaf.runningApps, uint64(1), "leaf should have 1 app running")
	assert.Equal(t, root.runningApps, uint64(2), "root should have 2 apps running")
	leaf.incRunningApps("")
	assert.Equal(t, leaf.runningApps, uint64(2), "leaf should have 2 app running")
	assert.Equal(t, root.runningApps, uint64(2), "root should not have changed")

	leaf2.incRunningApps("")
	assert.Equal(t, leaf2.runningApps, uint64(1), "leaf2 should have 1 app running")
	assert.Equal(t, root.runningApps, uint64(2), "root should have 1 app running")

	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil queue incRunningApps")
		}
	}()
	var q *Queue
	q.incRunningApps("")
}

func TestQueue_decRunningApps(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")

	leaf.decRunningApps()
	assert.Equal(t, leaf.runningApps, uint64(0), "default running apps is 0, should not wrap")
	assert.Equal(t, root.runningApps, uint64(0), "default running apps is 0, should not wrap")
	root.runningApps = 2
	leaf.runningApps = 2
	leaf.decRunningApps()
	assert.Equal(t, leaf.runningApps, uint64(1), "leaf should have 1 app running")
	assert.Equal(t, root.runningApps, uint64(1), "root should have 1 apps running")
	root.decRunningApps()
	assert.Equal(t, leaf.runningApps, uint64(1), "leaf should have 1 app running")
	assert.Equal(t, root.runningApps, uint64(0), "root should have no apps")
	leaf.decRunningApps()
	assert.Equal(t, leaf.runningApps, uint64(0), "leaf should have 0 app running")
	assert.Equal(t, root.runningApps, uint64(0), "root running apps is 0, should not wrap")

	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil queue decRunningApps")
		}
	}()
	var q *Queue
	q.decRunningApps()
}

func TestQueue_setAllocatingAccepted(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf, leaf2 *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	leaf2, err = createManagedQueue(root, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	leaf.setAllocatingAccepted("test-1")
	assert.Equal(t, len(leaf.allocatingAcceptedApps), 1, "expected 1 app in leaf list")
	assert.Equal(t, len(root.allocatingAcceptedApps), 1, "expected 1 app in root list")
	leaf.setAllocatingAccepted("test-1")
	assert.Equal(t, len(leaf.allocatingAcceptedApps), 1, "expected no change after adding same app again")
	leaf.setAllocatingAccepted("test-2")
	assert.Equal(t, len(leaf.allocatingAcceptedApps), 2, "expected 2 apps in leaf list")
	assert.Equal(t, len(root.allocatingAcceptedApps), 2, "expected 2 apps in root list")

	leaf2.setAllocatingAccepted("test-3")
	assert.Equal(t, len(leaf2.allocatingAcceptedApps), 1, "expected 1 app in leaf2 list")
	assert.Equal(t, len(root.allocatingAcceptedApps), 3, "expected 3 apps in root list")

	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil queue setAllocatingAccepted")
		}
	}()
	var q *Queue
	q.setAllocatingAccepted("")
}
