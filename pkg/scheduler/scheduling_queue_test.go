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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
)

// create the root queue, base for all testing
func createRootQueue(maxRes map[string]string) (*SchedulingQueue, error) {
	rootConf := configs.QueueConfig{
		Name:       "root",
		Parent:     true,
		Queues:     nil,
		Properties: make(map[string]string),
	}
	if maxRes != nil {
		rootConf.Resources = configs.Resources{
			Max: maxRes,
		}
	}
	return NewPreConfiguredQueue(rootConf, nil)
}

// wrapper around the create calls using the same syntax as an unmanaged queue
func createManagedQueue(parentQI *SchedulingQueue, name string, parent bool, maxRes map[string]string) (*SchedulingQueue, error) {
	return createManagedQueueWithProps(parentQI, name, parent, maxRes, nil)
}


// create managed queue with props set
func createManagedQueueWithProps(parentQI *SchedulingQueue, name string, parent bool, maxRes map[string]string, props map[string]string) (*SchedulingQueue, error) {
	childConf := configs.QueueConfig{
		Name:       name,
		Parent:     parent,
		Queues:     nil,
		Properties: props,
	}
	if maxRes != nil {
		childConf.Resources = configs.Resources{
			Max: maxRes,
		}
	}
	return NewPreConfiguredQueue(childConf, parentQI)
}

// base test for creating a managed queue
func TestQueueBasics(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	// check the state of the queue
	if !root.isManaged() && !root.isLeafQueue() && !root.isRunning() {
		t.Error("root queue status is incorrect")
	}
	// allocations should be nil
	if !resources.IsZero(root.preempting) && !resources.IsZero(root.pending) {
		t.Error("root queue must not have allocations set on create")
	}
}

// FIXME: There're 2 very similar UTs
func TestManagedSubQueues_1(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// single parent under root
	var parent *SchedulingQueue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	if parent.isLeafQueue() || !parent.isManaged() || !parent.isRunning() {
		t.Error("parent queue is not marked as running managed parent")
	}
	if len(root.childrenQueues) == 0 {
		t.Error("parent queue is not added to the root queue")
	}
	if parent.isRoot() {
		t.Error("parent queue says it is the root queue which is incorrect")
	}
	if parent.removeQueue() || len(root.childrenQueues) != 1 {
		t.Error("parent queue should not have been removed as it is running")
	}

	// add a leaf under the parent
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(parent.childrenQueues) == 0 {
		t.Error("leaf queue is not added to the parent queue")
	}
	if !leaf.isLeafQueue() || !leaf.isManaged() {
		t.Error("leaf queue is not marked as managed leaf")
	}

	// cannot remove child with app in it
	app := newSchedulingAppWithId("test")
	leaf.addSchedulingApplication(app)

	// both parent and leaf are marked for removal
	parent.MarkQueueForRemoval()
	if !leaf.isDraining() || !parent.isDraining() {
		t.Error("queues are not marked for removal (not in draining state)")
	}
	// try to remove the parent
	if parent.removeQueue() {
		t.Error("parent queue should not have been removed as it has a child")
	}
	// try to remove the child
	if leaf.removeQueue() {
		t.Error("leaf queue should not have been removed")
	}
	// remove the app (dirty way)
	delete(leaf.applications, "test")
	if !leaf.removeQueue() && len(parent.childrenQueues) != 0 {
		t.Error("leaf queue should have been removed and parent updated and was not")
	}
}

// FIXME: There're 2 very similar UTs
func TestManagedSubQueues_2(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *SchedulingQueue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	if parent.isLeaf {
		t.Errorf("parent queue is not marked as a parent")
	}
	if len(root.childrenQueues) == 0 {
		t.Errorf("parent queue is not added to the root queue")
	}
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(parent.childrenQueues) == 0 {
		t.Errorf("leaf queue is not added to the parent queue")
	}
	if !leaf.isLeaf || !leaf.managed {
		t.Errorf("leaf queue is not marked as managed leaf")
	}

	// both parent and leaf are marked for removal
	parent.MarkQueueForRemoval()
	if !leaf.IsDraining() || !parent.IsDraining() {
		t.Errorf("queues are not marked for removal (not in draining state)")
	}
	// try to remove the parent
	if parent.RemoveQueue() {
		t.Errorf("parent queue should not have been removed as it has a child")
	}
	// remove the child
	if !leaf.RemoveQueue() && len(parent.childrenQueues) != 0 {
		t.Errorf("leaf queue should have been removed and parent updated and was not")
	}
	// now set some allocation in the parent and try removal again
	res := map[string]string{"memory": "100", "vcores": "10"}
	var allocation *resources.Resource
	allocation, err = resources.NewResourceFromConf(res)
	assert.NilError(t, err, "failed to create basic resource")
	err = parent.IncAllocatedResource(allocation, false)
	if err != nil {
		t.Errorf("allocation increase failed on parent: %v", err)
	}
	if parent.RemoveQueue() {
		t.Errorf("parent queue should not have been removed as it has an allocation")
	}
	err = parent.decAllocatedResource(allocation)
	if err != nil {
		t.Errorf("parent queue allocation failed on decrement %v", err)
	}
	if !parent.RemoveQueue() {
		t.Errorf("parent queue should have been removed and was not")
	}
}

func TestMergeProperties(t *testing.T) {
	base := map[string]string{"first": "first value", "second": "second value"}
	// merge same values should not change anything
	merged := mergeProperties(base, base)
	if len(merged) != 2 {
		t.Errorf("merge failed not exactly 2 keys: %v", merged)
	}
	change := map[string]string{"third": "third value"}
	merged = mergeProperties(base, change)
	if len(merged) != 3 {
		t.Errorf("merge failed not exactly 3 keys: %v", merged)
	}
	change = map[string]string{"third": "changed"}
	merged = mergeProperties(base, change)
	if len(merged) != 3 {
		t.Errorf("merge failed not exactly 3 keys: %v", merged)
	}
}

// FIXME, need to remove one of them
func TestUnManagedSubQueues_2(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// single parent under root
	var parent *SchedulingQueue
	parent, err = NewDynamicQueue("parent", true, root)
	assert.NilError(t, err, "failed to create parent queue")
	if parent.isLeafQueue() || parent.isManaged() {
		t.Errorf("parent queue is not marked as parent")
	}
	if len(root.childrenQueues) == 0 {
		t.Errorf("parent queue is not added to the root queue")
	}
	// add a leaf under the parent
	var leaf *SchedulingQueue
	leaf, err = NewDynamicQueue("leaf", false, parent)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(parent.childrenQueues) == 0 {
		t.Error("leaf queue is not added to the parent queue")
	}
	if !leaf.isLeafQueue() || leaf.isManaged() {
		t.Error("leaf queue is not marked as managed leaf")
	}

	// cannot remove child with app in it
	app := newSchedulingAppWithId("test")
	leaf.addSchedulingApplication(app)

	// try to mark parent and leaf for removal
	parent.MarkQueueForRemoval()
	if leaf.isDraining() || parent.isDraining() {
		t.Error("queues are marked for removal (draining state not for unmanaged queues)")
	}
	// try to remove the parent
	if parent.removeQueue() {
		t.Error("parent queue should not have been removed as it has a child")
	}
	// try to remove the child
	if leaf.removeQueue() {
		t.Error("leaf queue should not have been removed")
	}
	// remove the app (dirty way)
	delete(leaf.applications, "test")
	if !leaf.removeQueue() && len(parent.childrenQueues) != 0 {
		t.Error("leaf queue should have been removed and parent updated and was not")
	}
}

func TestPendingCalc(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")

	res := map[string]string{"memory": "100", "vcores": "10"}
	var allocation *resources.Resource
	allocation, err = resources.NewResourceFromConf(res)
	assert.NilError(t, err, "failed to create basic resource")
	leaf.incPendingResource(allocation)
	if !resources.Equals(root.pending, allocation) {
		t.Errorf("root queue pending allocation failed to increment expected %v, got %v", allocation, root.pending)
	}
	if !resources.Equals(leaf.pending, allocation) {
		t.Errorf("leaf queue pending allocation failed to increment expected %v, got %v", allocation, leaf.pending)
	}
	leaf.decPendingResource(allocation)
	if !resources.IsZero(root.pending) {
		t.Errorf("root queue pending allocation failed to decrement expected 0, got %v", root.pending)
	}
	if !resources.IsZero(leaf.pending) {
		t.Errorf("leaf queue pending allocation failed to decrement expected 0, got %v", leaf.pending)
	}
	// Not allowed to go negative: both will be zero after this
	newRes := resources.Multiply(allocation, 2)
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

func TestGetChildQueueInfos(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var parent *SchedulingQueue
	parent, err = createManagedQueue(root, "parent-man", true, nil)
	assert.NilError(t, err, "failed to create managed parent queue")
	for i := 0; i < 10; i++ {
		_, err = createManagedQueue(parent, "leaf-man"+strconv.Itoa(i), false, nil)
		if err != nil {
			t.Errorf("failed to create managed queue: %v", err)
		}
	}
	if len(parent.childrenQueues) != 10 {
		t.Errorf("managed leaf queues are not added to the parent queue, expected 10 children got %d", len(parent.childrenQueues))
	}

	parent, err = NewDynamicQueue("parent-un", true, root)
	assert.NilError(t, err, "failed to create unamanged parent queue")
	for i := 0; i < 10; i++ {
		_, err = NewDynamicQueue("leaf-un-"+strconv.Itoa(i), false, parent)
		if err != nil {
			t.Errorf("failed to create unmanaged queue: %v", err)
		}
	}
	if len(parent.childrenQueues) != 10 {
		t.Errorf("unmanaged leaf queues are not added to the parent queue, expected 10 children got %d", len(parent.childrenQueues))
	}

	// check the root queue
	if len(root.childrenQueues) != 2 {
		t.Errorf("parent queues are not added to the root queue, expected 2 children got %d", len(root.childrenQueues))
	}
}

func TestAddApplication(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf-man", false, nil)
	assert.NilError(t, err, "failed to create managed leaf queue")
	pending := resources.NewResourceFromMap(
		map[string]resources.Quantity{
			resources.MEMORY: 10,
		})
	app := newSchedulingAppWithId("test")
	app.pending = pending
	// adding the app must not update pending resources
	leaf.addSchedulingApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not zero")

	// add the same app again should not increase the number of apps
	leaf.addSchedulingApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not replaced in the queue as expected")
}

func TestAddApplicationWithTag(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	// only need to test leaf queues as we will never add an app to a parent
	var leaf, leafUn *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf-man", false, nil)
	assert.NilError(t, err, "failed to create managed leaf queue")
	leafUn, err = NewDynamicQueue("leaf-unman", false, root)
	assert.NilError(t, err, "failed to create unmanaged leaf queue")
	app1 := newSchedulingAppInternal("app-1", "default", "root.leaf-man", security.UserGroup{}, nil)

	// adding the app to managed/unmanaged queue must not update queue settings, works
	leaf.addSchedulingApplication(app1)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the managed queue as expected")
	if leaf.getMaxResource() != nil {
		t.Errorf("Max resources should not be set on managed queue got: %s", leaf.getMaxResource().String())
	}
	app2 := newSchedulingAppInternal("app-2", "default", "root.leaf-un", security.UserGroup{}, nil)
	leafUn.addSchedulingApplication(app2)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the unmanaged queue as expected")
	if leafUn.getMaxResource() != nil {
		t.Errorf("Max resources should not be set on unmanaged queue got: %s", leafUn.getMaxResource().String())
	}

	maxRes := resources.NewResourceFromMap(
		map[string]resources.Quantity{
			"first": 10,
		})
	tags := make(map[string]string)
	tags[appTagNamespaceResourceQuota] = "{\"resources\":{\"first\":{\"value\":10}}}"
	// add apps again now with the tag set
	app3 := newSchedulingAppInternal("app-3", "default", "root.leaf-man", security.UserGroup{}, tags)
	leaf.addSchedulingApplication(app3)
	assert.Equal(t, len(leaf.applications), 2, "Application was not added to the managed queue as expected")
	if leaf.getMaxResource() != nil {
		t.Errorf("Max resources should not be set on managed queue got: %s", leaf.getMaxResource().String())
	}
	app4 := newSchedulingAppInternal("app-4", "default", "root.leaf-un", security.UserGroup{}, tags)
	leafUn.addSchedulingApplication(app4)
	assert.Equal(t, len(leaf.applications), 2, "Application was not added to the unmanaged queue as expected")
	if !resources.Equals(leafUn.getMaxResource(), maxRes) {
		t.Errorf("Max resources not set as expected: %s got: %v", maxRes.String(), leafUn.getMaxResource())
	}

	// set to illegal limit (0 value)
	tags[appTagNamespaceResourceQuota] = "{\"resources\":{\"first\":{\"value\":0}}}"
	app4 = newSchedulingAppInternal("app-4", "default", "root.leaf-un", security.UserGroup{}, tags)
	leafUn.addSchedulingApplication(app4)
	assert.Equal(t, len(leaf.applications), 2, "Application was not added to the unmanaged queue as expected")
	if !resources.Equals(leafUn.getMaxResource(), maxRes) {
		t.Errorf("Max resources not set as expected: %s got: %v", maxRes.String(), leafUn.getMaxResource())
	}
}

func TestRemoveApplication(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf-man", false, nil)
	assert.NilError(t, err, "failed to create managed leaf queue")
	// try removing a non existing app
	nonExist := newSchedulingAppWithId("test")
	leaf.removeSchedulingApplication(nonExist)
	assert.Equal(t, len(leaf.applications), 0, "Removal of non existing app updated unexpected")

	// add an app and remove it
	app := newSchedulingAppWithId("exists")
	leaf.addSchedulingApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not zero")
	leaf.removeSchedulingApplication(nonExist)
	assert.Equal(t, len(leaf.applications), 1, "Non existing application was removed from the queue")
	leaf.removeSchedulingApplication(app)
	assert.Equal(t, len(leaf.applications), 0, "Application was not removed from the queue as expected")

	// try the same again now with pending resources set
	pending := resources.NewResourceFromMap(
		map[string]resources.Quantity{
			resources.MEMORY: 10,
		})
	app.pending.AddTo(pending)
	leaf.addSchedulingApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not zero")
	// update pending resources for the hierarchy
	leaf.incPendingResource(pending)
	leaf.removeSchedulingApplication(app)
	assert.Equal(t, len(leaf.applications), 0, "Application was not removed from the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not updated correctly")
	assert.Assert(t, resources.IsZero(root.pending), "root queue pending resource not updated correctly")
}

func TestQueueStates(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// add a leaf under the root
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	err = leaf.HandleQueueEvent(Stop)
	if err != nil || !leaf.isStopped() {
		t.Errorf("leaf queue is not marked stopped: %v", err)
	}
	err = leaf.HandleQueueEvent(Start)
	if err != nil || !leaf.isRunning() {
		t.Errorf("leaf queue is not marked running: %v", err)
	}
	err = leaf.HandleQueueEvent(Remove)
	if err != nil || !leaf.isDraining() {
		t.Errorf("leaf queue is not marked draining: %v", err)
	}
	err = leaf.HandleQueueEvent(Start)
	if err == nil || !leaf.isDraining() {
		t.Errorf("leaf queue changed state which should not happen: %v", err)
	}
}

func TestAllocatingCalc(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")

	res := map[string]string{"first": "1"}
	var allocation *resources.Resource
	allocation, err = resources.NewResourceFromConf(res)
	assert.NilError(t, err, "failed to create basic resource")
	leaf.incAllocatingResource(allocation)
	if !resources.Equals(root.allocating, allocation) {
		t.Errorf("root queue allocating failed to increment expected %v, got %v", allocation, root.allocating)
	}
	if !resources.Equals(leaf.allocating, allocation) {
		t.Errorf("leaf queue allocating failed to increment expected %v, got %v", allocation, leaf.allocating)
	}
	leaf.decAllocatingResource(allocation)
	if !resources.IsZero(root.allocating) {
		t.Errorf("root queue allocating failed to decrement expected 0, got %v", root.allocating)
	}
	// Not allowed to go negative: both will be zero after this
	root.incAllocatingResource(allocation)
	leaf.decAllocatingResource(allocation)
	// using the get function to access the value
	if !resources.IsZero(root.getAllocatingResource()) {
		t.Errorf("root queue allocating failed to decrement expected zero, got %v", root.allocating)
	}
	if !resources.IsZero(leaf.getAllocatingResource()) {
		t.Errorf("leaf queue allocating should have failed to decrement expected zero, got %v", leaf.allocating)
	}
}

func TestPreemptingCalc(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")

	res := map[string]string{"first": "1"}
	var allocation *resources.Resource
	allocation, err = resources.NewResourceFromConf(res)
	assert.NilError(t, err, "failed to create basic resource")
	if !resources.IsZero(leaf.preempting) {
		t.Errorf("leaf queue preempting resources not set as expected 0, got %v", leaf.preempting)
	}
	if !resources.IsZero(root.preempting) {
		t.Errorf("root queue preempting resources not set as expected 0, got %v", root.preempting)
	}
	// preempting does not filter up the hierarchy, check that
	leaf.incPreemptingResource(allocation)
	// using the get function to access the value
	if !resources.Equals(allocation, leaf.getPreemptingResource()) {
		t.Errorf("queue preempting resources not set as expected %v, got %v", allocation, leaf.preempting)
	}
	if !resources.IsZero(root.getPreemptingResource()) {
		t.Errorf("root queue preempting resources not set as expected 0, got %v", root.preempting)
	}
	newRes := resources.Multiply(allocation, 2)
	leaf.decPreemptingResource(newRes)
	if !resources.IsZero(leaf.getPreemptingResource()) {
		t.Errorf("queue preempting resources not set as expected 0, got %v", leaf.preempting)
	}
	leaf.setPreemptingResource(newRes)
	if !resources.Equals(leaf.getPreemptingResource(), resources.Multiply(allocation, 2)) {
		t.Errorf("queue preempting resources not set as expected %v, got %v", newRes, leaf.preempting)
	}
}

func TestAssumedQueueCalc(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assumed := leaf.getAssumeAllocated()
	if !resources.IsZero(assumed) {
		t.Errorf("queue unconfirmed and allocated resources not set as expected 0, got %v", assumed)
	}
	res := map[string]string{"first": "1"}
	var allocation *resources.Resource
	allocation, err = resources.NewResourceFromConf(res)
	assert.NilError(t, err, "failed to create basic resource")
	leaf.incAllocatingResource(allocation)
	assumed = leaf.getAssumeAllocated()
	if !resources.Equals(assumed, allocation) {
		t.Errorf("root queue allocating failed to increment expected %v, got %v", allocation, assumed)
	}
	// increase the allocated queue resource, use nodeReported true to bypass checks
	err = leaf.IncAllocatedResource(allocation, true)
	assert.NilError(t, err, "failed to increase cache queue allocated resource")
	assumed = leaf.getAssumeAllocated()
	allocation = resources.Multiply(allocation, 2)
	if !resources.Equals(assumed, allocation) {
		t.Errorf("root queue allocating failed to increment expected %v, got %v", allocation, assumed)
	}
}

// This test must not test the sorter that is underlying.
// It tests the queue specific parts of the code only.
func TestSortApplications(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf, parent *SchedulingQueue
	// empty parent queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue: %v")
	if apps := parent.sortApplications(); apps != nil {
		t.Errorf("parent queue should not return sorted apps: %v", apps)
	}

	// empty leaf queue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(leaf.sortApplications()) != 0 {
		t.Errorf("empty queue should return no app from sort: %v", leaf)
	}
	// new app does not have pending res, does not get returned
	app := newSchedulingAppInternal("app-1", "default", leaf.QueuePath, security.UserGroup{}, nil)
	app.queue = leaf
	leaf.addSchedulingApplication(app)
	if len(leaf.sortApplications()) != 0 {
		t.Errorf("app without ask should not be in sorted apps: %v", app)
	}
	var res, delta *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	// add an ask app must be returned
	delta, err = app.addAllocationAsk(newAllocationAsk("alloc-1", "app-1", res))
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("allocation ask delta expected %v, got %v (err = %v)", res, delta, err)
	}
	sortedApp := leaf.sortApplications()
	if len(sortedApp) != 1 || sortedApp[0].ApplicationID != "app-1" {
		t.Errorf("sorted application is missing expected app: %v", sortedApp)
	}
	// set 0 repeat
	_, err = app.updateAskRepeat("alloc-1", -1)
	if err != nil || len(leaf.sortApplications()) != 0 {
		t.Errorf("app with ask but 0 pending resources should not be in sorted apps: %v (err = %v)", app, err)
	}
}

// This test must not test the sorter that is underlying.
// It tests the queue specific parts of the code only.
func TestSortQueue(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	var leaf, parent *SchedulingQueue
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
	err = leaf.HandleQueueEvent(Stop)
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

	var parent *SchedulingQueue
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
	//   - leaf1	max resource ---;	alloc 5,3	head 15,5 * parent used
	//   - leaf2	max resource ---;	alloc 5,3	head 15,5 * parent used
	// set the max on the root
	resMap := map[string]string{"first": "20", "second": "10"}
	root, err = createRootQueue(resMap)
	assert.NilError(t, err, "failed to create root queue with limit")
	// set the max on the parent
	resMap = map[string]string{"first": "20", "second": "8"}
	parent, err = createManagedQueue(root, "parent", true, resMap)
	assert.NilError(t, err, "failed to create parent queue")
	// leaf1 queue no limit
	var leaf1, leaf2 *SchedulingQueue
	leaf1, err = createManagedQueue(parent, "leaf1", false, nil)
	assert.NilError(t, err, "failed to create leaf1 queue")
	// leaf2 queue no limit
	leaf2, err = createManagedQueue(parent, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	// allocating and allocated
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1", "second": "1"})
	assert.NilError(t, err, "failed to create resource")
	leaf1.incAllocatingResource(res)
	leaf2.incAllocatingResource(res)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "4", "second": "2"})
	assert.NilError(t, err, "failed to create resource")
	err = leaf1.IncAllocatedResource(res, true)
	assert.NilError(t, err, "failed to set allocated resource on leaf1")
	err = leaf2.IncAllocatedResource(res, true)
	assert.NilError(t, err, "failed to set allocated resource on leaf2")

	// headRoom root should be this (20-10, 10-6)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "4"})
	headRoom = root.getHeadRoom()
	if err != nil || !resources.Equals(res, headRoom) {
		t.Errorf("root queue head room not as expected %v, got: %v (err %v)", res, headRoom, err)
	}

	maxHeadRoom := root.getMaxHeadRoom()
	assert.Assert(t, maxHeadRoom == nil, "root queue max headroom should be nil")

	// headRoom parent should be this (20-10, 8-6)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "2"})
	headRoom = parent.getHeadRoom()
	maxHeadRoom = parent.getMaxHeadRoom()
	if err != nil || !resources.Equals(res, headRoom) || !resources.Equals(res, maxHeadRoom) {
		t.Errorf("parent queue head room not as expected %v, got: %v (err %v)", res, headRoom, err)
	}

	// headRoom leaf1 will be smaller of this
	// leaf1 (20-5, 8-3) & parent (20-10, 8-6)
	// parent queue has lower head room and leaf1 gets limited to parent headroom
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "2"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = leaf1.getHeadRoom()
	maxHeadRoom = leaf1.getMaxHeadRoom()
	if !resources.Equals(res, headRoom) || !resources.Equals(res, maxHeadRoom) {
		t.Errorf("leaf1 queue head room not as expected %v, got: %v (err %v)", res, headRoom, err)
	}
	headRoom = leaf2.getHeadRoom()
	maxHeadRoom = leaf2.getMaxHeadRoom()
	if !resources.Equals(res, headRoom) || !resources.Equals(res, maxHeadRoom) {
		t.Errorf("leaf1 queue head room not as expected %v, got: %v (err %v)", res, headRoom, err)
	}
}

//nolint: funlen
func TestMaxHeadroom(t *testing.T) {
	// create the root: nil test
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	headRoom := root.getMaxHeadRoom()
	if headRoom != nil {
		t.Errorf("empty cluster with root queue should not have headroom: %v", headRoom)
	}

	var parent *SchedulingQueue
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
	var leaf1, leaf2 *SchedulingQueue
	leaf1, err = createManagedQueue(parent, "leaf1", false, nil)
	assert.NilError(t, err, "failed to create leaf1 queue")
	leaf2, err = createManagedQueue(parent, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	// allocating and allocated
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1", "second": "1"})
	assert.NilError(t, err, "failed to create resource")
	leaf1.incAllocatingResource(res)
	leaf2.incAllocatingResource(res)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "4", "second": "2"})
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

	// recreate the structure, set max capacity in parent and a leaf queue
	// structure is:
	// root (max: nil)
	//   - parent (max: 20,8)
	//     - leaf1 (max: 10, 8)  (alloc: 5,3)
	//     - leaf2 (max: nil)    (alloc: 6,4)
	resMap := map[string]string{"first": "20", "second": "8"}
	root, err = createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	parent, err = createManagedQueue(root, "parent", true, resMap)
	assert.NilError(t, err, "failed to create parent queue")
	resMap = map[string]string{"first": "10", "second": "8"}
	leaf1, err = createManagedQueue(parent, "leaf1", false, resMap)
	assert.NilError(t, err, "failed to create leaf1 queue")
	leaf2, err = createManagedQueue(parent, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	// allocating and allocated
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1", "second": "1"})
	assert.NilError(t, err, "failed to create resource")
	leaf1.incAllocatingResource(res)
	leaf2.incAllocatingResource(res)
	leaf2.incAllocatingResource(res)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "4", "second": "2"})
	assert.NilError(t, err, "failed to create resource")
	err = leaf1.IncAllocatedResource(res, true)
	assert.NilError(t, err, "failed to set allocated resource on leaf1")
	err = leaf2.IncAllocatedResource(res, true)
	assert.NilError(t, err, "failed to set allocated resource on leaf2")

	// root headroom should be nil
	headRoom = root.getMaxHeadRoom()
	assert.Assert(t, headRoom == nil, "headRoom of root should be nil because no max set for all queues")

	// parent headroom = parentMax - leaf1Allocated - leaf2Allocated
	// parent headroom = (20 - 5 - 6, 8 - 3 - 4) = (9, 1)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "9", "second": "1"})
	headRoom = parent.getMaxHeadRoom()
	if err != nil || !resources.Equals(res, headRoom) {
		t.Errorf("parent queue head room not as expected %v, got: %v (err %v)", res, headRoom, err)
	}

	// leaf1 headroom = MIN(parentHeadRoom, leaf1Max - leaf1Allocated)
	// leaf1 headroom = MIN((9,1), (10-5, 8-3)) = MIN((9,1), (5,5)) = MIN(5, 1)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "1"})
	headRoom = leaf1.getMaxHeadRoom()
	if err != nil || !resources.Equals(res, headRoom) {
		t.Errorf("leaf1 queue head room not as expected %v, got: %v (err %v)", res, headRoom, err)
	}

	// leaf2 headroom = parentMax - leaf1Allocated - leaf2Allocated
	res, err = resources.NewResourceFromConf(map[string]string{"first": "9", "second": "1"})
	headRoom = leaf2.getMaxHeadRoom()
	if err != nil || !resources.Equals(res, headRoom) {
		t.Errorf("leaf2 queue head room not as expected %v, got: %v (err %v)", res, headRoom, err)
	}
}

func TestGetMaxUsage(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	maxUsage := root.getMaxResource()
	if maxUsage != nil {
		t.Errorf("empty cluster with root queue should not have max set: %v", maxUsage)
	}

	var parent *SchedulingQueue
	// empty parent queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	maxUsage = parent.getMaxResource()
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
	maxUsage = root.getMaxResource()
	if !resources.Equals(res, maxUsage) {
		t.Errorf("root queue should have max set expected %v, got: %v", res, maxUsage)
	}
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	maxUsage = parent.getMaxResource()
	if !resources.Equals(res, maxUsage) {
		t.Errorf("parent queue should have max from root set expected %v, got: %v", res, maxUsage)
	}

	// leaf queue with limit: contrary to root should get min from both merged
	var leaf *SchedulingQueue
	resMap = map[string]string{"first": "5", "second": "10"}
	leaf, err = createManagedQueue(parent, "leaf", false, resMap)
	assert.NilError(t, err, "failed to create leaf queue")
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "5"})
	assert.NilError(t, err, "failed to create resource")
	maxUsage = leaf.getMaxResource()
	if !resources.Equals(res, maxUsage) {
		t.Errorf("leaf queue should have merged max set expected %v, got: %v", res, maxUsage)
	}

	// replace parent with one with limit on different resource
	resMap = map[string]string{"third": "2"}
	parent, err = createManagedQueue(root, "parent2", true, resMap)
	assert.NilError(t, err, "failed to create parent2 queue")
	maxUsage = parent.getMaxResource()
	res, err = resources.NewResourceFromConf(map[string]string{"first": "0", "second": "0", "third": "0"})
	if err != nil || !resources.Equals(res, maxUsage) {
		t.Errorf("parent2 queue should have max from root set expected %v, got: %v (err %v)", res, maxUsage, err)
	}
	resMap = map[string]string{"first": "5", "second": "10"}
	leaf, err = createManagedQueue(parent, "leaf2", false, resMap)
	assert.NilError(t, err, "failed to create leaf2 queue")
	maxUsage = leaf.getMaxResource()
	res, err = resources.NewResourceFromConf(map[string]string{"first": "0", "second": "0", "third": "0"})
	if err != nil || !resources.Equals(res, maxUsage) {
		t.Errorf("leaf2 queue should have reset merged max set expected %v, got: %v (err %v)", res, maxUsage, err)
	}
}

func TestReserveApp(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, len(leaf.reservedApps), 0, "new queue should not have reserved apps")
	// no checks this works for everything
	appName := "something"
	leaf.reserve(appName)
	assert.Equal(t, len(leaf.reservedApps), 1, "app should have been reserved")
	assert.Equal(t, leaf.reservedApps[appName], 1, "app should have one reservation")
	leaf.reserve(appName)
	assert.Equal(t, leaf.reservedApps[appName], 2, "app should have two reservations")
	leaf.unReserve(appName, 2)
	assert.Equal(t, len(leaf.reservedApps), 0, "queue should not have any reserved apps, all reservations were removed")

	leaf.unReserve("unknown", 1)
	assert.Equal(t, len(leaf.reservedApps), 0, "unreserve of unknown app should not have changed count or added app")
}

func TestGetApp(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	// check for init of the map
	if unknown := leaf.getApplication("unknown"); unknown != nil {
		t.Errorf("un registered app found using appID which should not happen: %v", unknown)
	}

	// add app and check proper returns
	app := newSchedulingAppWithId("app-1")
	leaf.addSchedulingApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "queue should have one app registered")
	if leaf.getApplication("app-1") == nil {
		t.Errorf("registered app not found using appID")
	}
	if unknown := leaf.getApplication("unknown"); unknown != nil {
		t.Errorf("un registered app found using appID which should not happen: %v", unknown)
	}
}

func TestIsEmpty(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	assert.Equal(t, root.isEmpty(), true, "new root queue should have been empty")
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, root.isEmpty(), false, "root queue with child leaf should not have been empty")
	assert.Equal(t, leaf.isEmpty(), true, "new leaf should have been empty")

	// add app and check proper returns
	app := newSchedulingAppWithId("app-1")
	leaf.addSchedulingApplication(app)
	assert.Equal(t, leaf.isEmpty(), false, "queue with registered app should not be empty")
}

func TestGetOutstandingRequest(t *testing.T) {
	const app1ID = "app1"
	const app2ID = "app1"

	// queue structure:
	// root
	//   - queue1 (max.cpu = 10)
	//   - queue2 (max.cpu = 5)
	//
	// submit app1 to root.queue1, app2 to root.queue2
	// app1 asks for 20 1x1CPU requests, app2 asks for 20 1x1CPU requests
	// verify the outstanding requests for each of the queue is up to its max capacity, 10/5 respectively
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	var queue1, queue2 *SchedulingQueue
	queue1, err = createManagedQueue(root, "queue1", false, map[string]string{"cpu": "10"})
	assert.NilError(t, err, "failed to create queue1 queue")
	queue2, err = createManagedQueue(root, "queue2", false, map[string]string{"cpu": "5"})
	assert.NilError(t, err, "failed to create queue2 queue")

	app1 := newSchedulingAppInternal(app1ID, "default", "root.queue1", security.UserGroup{}, nil)
	app1.queue = queue1
	queue1.addSchedulingApplication(app1)
	res, err := resources.NewResourceFromConf(map[string]string{"cpu": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	for i := 0; i < 20; i++ {
		_, err = app1.addAllocationAsk(
			newAllocationAsk(fmt.Sprintf("alloc-%d", i), app1ID, res))
		assert.NilError(t, err, "failed to add allocation ask")
	}

	app2 := newSchedulingAppInternal(app2ID, "default", "root.queue2", security.UserGroup{}, nil)
	app2.queue = queue2
	queue2.addSchedulingApplication(app2)
	for i := 0; i < 20; i++ {
		_, err = app2.addAllocationAsk(
			newAllocationAsk(fmt.Sprintf("alloc-%d", i), app2ID, res))
		assert.NilError(t, err, "failed to add allocation ask")
	}

	// verify get outstanding requests for root, and child queues
	rootTotal := make([]*schedulingAllocationAsk, 0)
	root.getQueueOutstandingRequests(&rootTotal)
	assert.Equal(t, len(rootTotal), 15)

	queue1Total := make([]*schedulingAllocationAsk, 0)
	queue1.getQueueOutstandingRequests(&queue1Total)
	assert.Equal(t, len(queue1Total), 10)

	queue2Total := make([]*schedulingAllocationAsk, 0)
	queue2.getQueueOutstandingRequests(&queue2Total)
	assert.Equal(t, len(queue2Total), 5)

	// simulate queue1 has some allocating resources
	// after allocation, the max available becomes to be 5
	allocatingRest := map[string]string{"cpu": "5"}
	allocation, err := resources.NewResourceFromConf(allocatingRest)
	assert.NilError(t, err, "failed to create basic resource")
	queue1.incAllocatingResource(allocation)

	queue1Total = make([]*schedulingAllocationAsk, 0)
	queue1.getQueueOutstandingRequests(&queue1Total)
	assert.Equal(t, len(queue1Total), 5)

	queue2Total = make([]*schedulingAllocationAsk, 0)
	queue2.getQueueOutstandingRequests(&queue2Total)
	assert.Equal(t, len(queue2Total), 5)

	rootTotal = make([]*schedulingAllocationAsk, 0)
	root.getQueueOutstandingRequests(&rootTotal)
	assert.Equal(t, len(rootTotal), 10)

	// remove app2 from queue2
	queue2.removeSchedulingApplication(app2)
	queue2Total = make([]*schedulingAllocationAsk, 0)
	queue2.getQueueOutstandingRequests(&queue2Total)
	assert.Equal(t, len(queue2Total), 0)

	rootTotal = make([]*schedulingAllocationAsk, 0)
	root.getQueueOutstandingRequests(&rootTotal)
	assert.Equal(t, len(rootTotal), 5)

	// queue structure:
	// root
	//   - queue1
	//   - queue2
	//
	// both queues have no max capacity set
	// submit app1 to root.queue1, app2 to root.queue2
	// app1 asks for 10 1x1CPU requests, app2 asks for 20 1x1CPU requests
	// verify all these requests are outstanding
	root, err = createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	queue1, err = createManagedQueue(root, "queue1", false, nil)
	assert.NilError(t, err, "failed to create queue1 queue")
	queue2, err = createManagedQueue(root, "queue2", false, nil)
	assert.NilError(t, err, "failed to create queue2 queue")

	app1 = newSchedulingAppInternal(app1ID, "default", "root.queue1", security.UserGroup{}, nil)
	app1.queue = queue1
	queue1.addSchedulingApplication(app1)
	res, err = resources.NewResourceFromConf(map[string]string{"cpu": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	for i := 0; i < 10; i++ {
		_, err = app1.addAllocationAsk(
			newAllocationAsk(fmt.Sprintf("alloc-%d", i), app1ID, res))
		assert.NilError(t, err, "failed to add allocation ask")
	}

	app2 = newSchedulingAppInternal(app2ID, "default", "root.queue2", security.UserGroup{}, nil)
	app2.queue = queue2
	queue2.addSchedulingApplication(app2)
	for i := 0; i < 20; i++ {
		_, err = app2.addAllocationAsk(
			newAllocationAsk(fmt.Sprintf("alloc-%d", i), app2ID, res))
		assert.NilError(t, err, "failed to add allocation ask")
	}

	rootTotal = make([]*schedulingAllocationAsk, 0)
	root.getQueueOutstandingRequests(&rootTotal)
	assert.Equal(t, len(rootTotal), 30)

	queue1Total = make([]*schedulingAllocationAsk, 0)
	queue1.getQueueOutstandingRequests(&queue1Total)
	assert.Equal(t, len(queue1Total), 10)

	queue2Total = make([]*schedulingAllocationAsk, 0)
	queue2.getQueueOutstandingRequests(&queue2Total)
	assert.Equal(t, len(queue2Total), 20)
}

// base test for creating a managed queue
func TestQueueInfo(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	// check the state of the queue
	if !root.managed && !root.isLeaf && !root.IsRunning() {
		t.Errorf("root queue status is incorrect")
	}
	// allocations should be nil
	if !resources.IsZero(root.GetAllocatedResource()) {
		t.Errorf("root queue must not have allocations set on create")
	}
}

func TestAllocationCalcRoot(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	res := map[string]string{"memory": "100", "vcores": "10"}
	var allocation *resources.Resource
	allocation, err = resources.NewResourceFromConf(res)
	assert.NilError(t, err, "failed to create basic resource")
	err = root.IncAllocatedResource(allocation, false)
	if err != nil {
		t.Errorf("root queue allocation failed on increment %v", err)
	}
	err = root.decAllocatedResource(allocation)
	if err != nil {
		t.Errorf("root queue allocation failed on decrement %v", err)
	}
	if !resources.IsZero(root.allocatedResource) {
		t.Errorf("root queue allocations are not zero: %v", root.allocatedResource)
	}
	err = root.decAllocatedResource(allocation)
	if err == nil {
		t.Errorf("root queue allocation should have failed to decrement %v", err)
	}
}

func TestAllocationCalcSub(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *SchedulingQueue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")

	res := map[string]string{"memory": "100", "vcores": "10"}
	var allocation *resources.Resource
	allocation, err = resources.NewResourceFromConf(res)
	assert.NilError(t, err, "failed to create basic resource")
	err = parent.IncAllocatedResource(allocation, false)
	if err != nil {
		t.Errorf("parent queue allocation failed on increment %v", err)
	}
	err = parent.decAllocatedResource(allocation)
	if err != nil {
		t.Errorf("parent queue allocation failed on decrement %v", err)
	}
	if !resources.IsZero(root.allocatedResource) {
		t.Errorf("root queue allocations are not zero: %v", root.allocatedResource)
	}
	err = root.decAllocatedResource(allocation)
	if err == nil {
		t.Errorf("root queue allocation should have failed to decrement %v", root.allocatedResource)
	}

	// add to the parent, remove from root and then try to remove from parent: root should complain
	err = parent.IncAllocatedResource(allocation, false)
	if err != nil {
		t.Errorf("parent queue allocation failed on increment %v", err)
	}
	err = root.decAllocatedResource(allocation)
	if err != nil {
		t.Errorf("root queue allocation failed on decrement %v", err)
	}
	err = parent.decAllocatedResource(allocation)
	if err == nil {
		t.Errorf("parent queue allocation should have failed on decrement %v, %v", root.allocatedResource, parent.allocatedResource)
	}
}

func TestQueueProps(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *SchedulingQueue
	props := map[string]string{"first": "value", "second": "other value"}
	parent, err = createManagedQueueWithProps(root, "parent", true, nil, props)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Assert(t, !parent.isLeaf, "parent queue is not marked as a parent")
	assert.Equal(t, len(root.childrenQueues), 1, "parent queue is not added to the root queue")
	assert.Equal(t, len(parent.properties), 2, "parent queue properties expected 2, got %v", parent.properties)

	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, len(parent.childrenQueues), 1, "leaf queue is not added to the parent queue")
	assert.Assert(t, leaf.isLeaf && leaf.managed, "leaf queue is not marked as managed leaf")
	assert.Equal(t, len(leaf.properties), 2, "leaf queue properties size incorrect")

	props = map[string]string{"first": "not inherited", configs.ApplicationSortPolicy: "stateaware"}
	parent, err = createManagedQueueWithProps(root, "parent2", true, nil, props)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Equal(t, len(parent.properties), 2, "parent queue properties size incorrect")
	leaf, err = NewDynamicQueue("leaf", false, parent)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Assert(t, leaf.isLeaf && !leaf.managed, "leaf queue is not marked as unmanaged leaf")
	assert.Equal(t, len(leaf.properties), 1, "leaf queue properties size incorrect")
	assert.Equal(t, leaf.properties[configs.ApplicationSortPolicy], "stateaware", "leaf queue property value not as expected")
}

func TestUnManagedSubQueues(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *SchedulingQueue
	parent, err = NewDynamicQueue("parent-man", true, root)
	assert.NilError(t, err, "failed to create parent queue")
	if parent.isLeaf {
		t.Errorf("parent queue is not marked as a parent")
	}
	if len(root.childrenQueues) == 0 {
		t.Errorf("parent queue is not added to the root queue")
	}
	var leaf *SchedulingQueue
	leaf, err = NewDynamicQueue("leaf", false, parent)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(parent.childrenQueues) == 0 {
		t.Errorf("leaf queue is not added to the parent queue")
	}
	if !leaf.isLeaf || leaf.managed {
		t.Errorf("leaf queue is not marked as managed leaf")
	}

	// try to mark parent and leaf for removal
	parent.MarkQueueForRemoval()
	if leaf.IsDraining() || parent.IsDraining() {
		t.Errorf("queues are marked for removal (draining state not for unmanaged queues)")
	}
	// try to remove the parent
	if parent.RemoveQueue() {
		t.Errorf("parent queue should not have been removed as it has a child")
	}
	// remove the child
	if !leaf.RemoveQueue() && len(parent.childrenQueues) != 0 {
		t.Errorf("leaf queue should have been removed and parent updated and was not")
	}
	// now set some allocation in the parent and try removal again
	res := map[string]string{"memory": "100", "vcores": "10"}
	var allocation *resources.Resource
	allocation, err = resources.NewResourceFromConf(res)
	assert.NilError(t, err, "failed to create basic resource")
	err = parent.IncAllocatedResource(allocation, false)
	if err != nil {
		t.Errorf("allocation increase failed on parent: %v", err)
	}
	if parent.RemoveQueue() {
		t.Errorf("parent queue should not have been removed as it has an allocation")
	}
	err = parent.decAllocatedResource(allocation)
	if err != nil {
		t.Errorf("parent queue allocation failed on decrement %v", err)
	}
	if !parent.RemoveQueue() {
		t.Errorf("parent queue should have been removed and was not")
	}
}

func TestMaxResource(t *testing.T) {
	resMap := map[string]string{"first": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create basic resource")
	// create the root
	var root, parent *SchedulingQueue
	root, err = createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create basic managed parent queue")
	// Nothing set max should be nil
	if root.GetMaxResource() != nil || parent.GetMaxResource() != nil {
		t.Errorf("empty cluster should not have max set on root queue")
	}
	// try setting on the parent (nothing should change)
	parent.setMaxResource(res)
	if parent.GetMaxResource() != nil {
		t.Errorf("parent queue change should have been rejected parent: %v", parent.GetMaxResource())
	}
	// Set on the root should change
	root.setMaxResource(res)
	if !resources.Equals(res, root.GetMaxResource()) {
		t.Errorf("root max setting not picked up by parent queue expected %v, got %v", res, parent.GetMaxResource())
	}
}

func TestUpdateUnManagedMaxResource(t *testing.T) {
	resMap := map[string]string{"first": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create basic resource")
	// create the root
	var root, parent, leaf *SchedulingQueue
	root, err = createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	// Nothing set max should be nil
	if root.GetMaxResource() != nil {
		t.Errorf("empty cluster should not have max set on root queue")
	}

	// try setting on the parent or managed leaf (nothing should change)
	parent, err = NewDynamicQueue("parent-un", true, root)
	assert.NilError(t, err, "failed to create basic unmanaged parent queue")
	parent.UpdateUnManagedMaxResource(res)
	if parent.GetMaxResource() != nil {
		t.Errorf("unmanaged parent queue change should have been rejected expected nil got: %v", parent.GetMaxResource())
	}
	parent, err = createManagedQueue(root, "parent-man", true, nil)
	assert.NilError(t, err, "failed to create basic unmanaged parent queue")
	parent.UpdateUnManagedMaxResource(res)
	if parent.GetMaxResource() != nil {
		t.Errorf("managed parent queue change should have been rejected expected nil got: %v", parent.GetMaxResource())
	}

	leaf, err = createManagedQueue(root, "leaf-man", false, nil)
	assert.NilError(t, err, "failed to create basic managed leaf queue")
	// Set on the root should change
	leaf.UpdateUnManagedMaxResource(res)
	if leaf.GetMaxResource() != nil {
		t.Errorf("managed leaf queue change should have been rejected expected nil got: %v", parent.GetMaxResource())
	}

	// try setting on the unmanaged leaf (should change)
	leaf, err = NewDynamicQueue("leaf-un", false, root)
	assert.NilError(t, err, "failed to create basic unmanaged leaf queue")
	leaf.UpdateUnManagedMaxResource(res)
	if !resources.Equals(res, leaf.GetMaxResource()) {
		t.Errorf("leaf max setting not set as expected %v, got %v", res, leaf.GetMaxResource())
	}
}

func TestGetQueueInfos(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue: %v", err)
	var rootMax *resources.Resource
	rootMax, err = resources.NewResourceFromConf(map[string]string{"memory": "2048", "vcores": "10"})
	assert.NilError(t, err, "failed to create configuration: %v", err)
	root.setMaxResource(rootMax)

	var parentUsed *resources.Resource
	parentUsed, err = resources.NewResourceFromConf(map[string]string{"memory": "1012", "vcores": "2"})
	assert.NilError(t, err, "failed to create resource: %v", err)
	var parent *SchedulingQueue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	err = parent.IncAllocatedResource(parentUsed, false)
	assert.NilError(t, err, "failed to increment allocated resource: %v", err)

	var child1used *resources.Resource
	child1used, err = resources.NewResourceFromConf(map[string]string{"memory": "1012", "vcores": "2"})
	assert.NilError(t, err, "failed to create resource: %v", err)
	var child1 *SchedulingQueue
	child1, err = createManagedQueue(parent, "child1", true, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	err = child1.IncAllocatedResource(child1used, false)
	assert.NilError(t, err, "failed to increment allocated resource: %v", err, err)

	var child2 *SchedulingQueue
	child2, err = createManagedQueue(parent, "child2", true, nil)
	assert.NilError(t, err, "failed to create child queue: %v", err)
	child2.setMaxResource(resources.NewResource())
	rootDaoInfo := root.GetQueueInfos()

	compareQueueInfoWithDAO(t, root, rootDaoInfo)
	parentDaoInfo := rootDaoInfo.ChildQueues[0]
	compareQueueInfoWithDAO(t, parent, parentDaoInfo)
	for _, childDao := range parentDaoInfo.ChildQueues {
		name := childDao.QueueName
		child := parent.childrenQueues[name]
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
	var parent *SchedulingQueue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	parent.properties = make(map[string]string)
	parent.properties[configs.ApplicationSortPolicy] = "fifo"

	// managed leaf queue with some properties set
	var child1 *SchedulingQueue
	child1, err = createManagedQueue(parent, "child1", true, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	child1.properties = make(map[string]string)
	child1.properties["some_property"] = "some_value"
	child1.properties[configs.ApplicationSortPolicy] = "fair"

	// maanaged leaf queue with some properties set
	var child2 *SchedulingQueue
	child2, err = createManagedQueue(parent, "child2", true, nil)
	assert.NilError(t, err, "failed to create child queue: %v", err)
	child2.properties = make(map[string]string)
	child2.properties[configs.ApplicationSortPolicy] = "state_aware"

	// un managed leaf queue with some properties set
	var unmanaged *SchedulingQueue
	unmanaged, err = NewDynamicQueue("child3", true, parent)
	assert.NilError(t, err, "failed to create child queue: %v", err)
	unmanaged.properties = make(map[string]string)
	unmanaged.properties[configs.ApplicationSortPolicy] = "fifo"

	// un managed leaf queue with no properties set (empty property set)
	var unmanaged2 *SchedulingQueue
	unmanaged2, err = NewDynamicQueue("child4", true, parent)
	assert.NilError(t, err, "failed to create child queue: %v", err)
	unmanaged2.properties = make(map[string]string)

	rootDaoInfo := root.GetQueueInfos()

	compareQueueInfoWithDAO(t, root, rootDaoInfo)
	parentDaoInfo := rootDaoInfo.ChildQueues[0]
	compareQueueInfoWithDAO(t, parent, parentDaoInfo)
	for _, childDao := range parentDaoInfo.ChildQueues {
		name := childDao.QueueName
		child := parent.childrenQueues[name]
		if child == nil {
			t.Fail()
		}
		compareQueueInfoWithDAO(t, child, childDao)
	}
}

func compareQueueInfoWithDAO(t *testing.T, queueInfo *SchedulingQueue, dao dao.QueueDAOInfo) {
	assert.Equal(t, queueInfo.QueuePath, dao.QueueName)
	assert.Equal(t, len(queueInfo.childrenQueues), len(dao.ChildQueues))
	assert.Equal(t, queueInfo.stateMachine.Current(), dao.Status)
	emptyRes := "[]"
	if queueInfo.allocatedResource == nil {
		assert.Equal(t, emptyRes, dao.Capacities.UsedCapacity)
	} else {
		assert.Equal(t, strings.Trim(queueInfo.allocatedResource.String(), "map"), dao.Capacities.UsedCapacity)
	}
	if queueInfo.maxResource == nil {
		assert.Equal(t, emptyRes, dao.Capacities.MaxCapacity)
	} else {
		assert.Equal(t, strings.Trim(queueInfo.maxResource.String(), "map"), dao.Capacities.MaxCapacity)
	}
	if queueInfo.guaranteedResource == nil {
		assert.Equal(t, emptyRes, dao.Capacities.Capacity)
	} else {
		assert.Equal(t, strings.Trim(queueInfo.guaranteedResource.String(), "map"), dao.Capacities.Capacity)
	}
	assert.Equal(t, len(queueInfo.properties), len(dao.Properties))
	if len(queueInfo.properties) > 0 {
		for k, v := range queueInfo.properties {
			assert.Equal(t, v, dao.Properties[k])
		}
	}
}