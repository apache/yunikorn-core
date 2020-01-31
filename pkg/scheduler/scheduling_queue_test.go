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
	"strconv"
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
)

// create the root queue, base for all testing
func createRootQueue() (*SchedulingQueue, error) {
	rootConf := configs.QueueConfig{
		Name:       "root",
		Parent:     true,
		Queues:     nil,
		Properties: make(map[string]string),
	}
	root, err := cache.NewManagedQueue(rootConf, nil)
	return NewSchedulingQueueInfo(root, nil), err
}

// wrapper around the create calls using the same syntax as an unmanaged queue
func createManagedQueue(parentQI *SchedulingQueue, name string, parent bool) (*SchedulingQueue, error) {
	childConf := configs.QueueConfig{
		Name:       name,
		Parent:     parent,
		Queues:     nil,
		Properties: make(map[string]string),
	}
	child, err := cache.NewManagedQueue(childConf, parentQI.CachedQueueInfo)
	return NewSchedulingQueueInfo(child, parentQI), err
}

// wrapper around the create calls using the same syntax as a managed queue
func createUnManagedQueue(parentQI *SchedulingQueue, name string, parent bool) (*SchedulingQueue, error) {
	child, err := cache.NewUnmanagedQueue(name, !parent, parentQI.CachedQueueInfo)
	return NewSchedulingQueueInfo(child, parentQI), err
}

// base test for creating a managed queue
func TestQueueBasics(t *testing.T) {
	// create the root
	root, err := createRootQueue()
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}
	// check the state of the queue
	if !root.isManaged() && !root.isLeafQueue() && !root.isRunning() {
		t.Error("root queue status is incorrect")
	}
	// allocations should be nil
	if !resources.IsZero(root.allocatingResource) && !resources.IsZero(root.pendingResource) {
		t.Error("root queue must not have allocations set on create")
	}
}

func TestManagedSubQueues(t *testing.T) {
	// create the root
	root, err := createRootQueue()
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}

	// single parent under root
	var parent *SchedulingQueue
	parent, err = createManagedQueue(root, "parent", true)
	if err != nil {
		t.Fatalf("failed to create parent queue: %v", err)
	}
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
	leaf, err = createManagedQueue(parent, "leaf", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	if len(parent.childrenQueues) == 0 {
		t.Error("leaf queue is not added to the parent queue")
	}
	if !leaf.isLeafQueue() || !leaf.isManaged() {
		t.Error("leaf queue is not marked as managed leaf")
	}

	// cannot remove child with app in it
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "test"})
	leaf.addSchedulingApplication(app)

	// both parent and leaf are marked for removal
	parent.CachedQueueInfo.MarkQueueForRemoval()
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

func TestUnManagedSubQueues(t *testing.T) {
	// create the root
	root, err := createRootQueue()
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}

	// single parent under root
	var parent *SchedulingQueue
	parent, err = createUnManagedQueue(root, "parent", true)
	if err != nil {
		t.Fatalf("failed to create parent queue: %v", err)
	}
	if parent.isLeafQueue() || parent.isManaged() {
		t.Errorf("parent queue is not marked as parent")
	}
	if len(root.childrenQueues) == 0 {
		t.Errorf("parent queue is not added to the root queue")
	}
	// add a leaf under the parent
	var leaf *SchedulingQueue
	leaf, err = createUnManagedQueue(parent, "leaf", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	if len(parent.childrenQueues) == 0 {
		t.Error("leaf queue is not added to the parent queue")
	}
	if !leaf.isLeafQueue() || leaf.isManaged() {
		t.Error("leaf queue is not marked as managed leaf")
	}

	// cannot remove child with app in it
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "test"})
	leaf.addSchedulingApplication(app)

	// try to mark parent and leaf for removal
	parent.CachedQueueInfo.MarkQueueForRemoval()
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
	root, err := createRootQueue()
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}
	var parent *SchedulingQueue
	parent, err = createManagedQueue(root, "parent", true)
	if err != nil {
		t.Fatalf("failed to create parent queue: %v", err)
	}

	res := map[string]string{"memory": "100", "vcores": "10"}
	var allocation *resources.Resource
	allocation, err = resources.NewResourceFromConf(res)
	if err != nil {
		t.Fatalf("failed to create basic resource: %v", err)
	}
	parent.incPendingResource(allocation)
	if !resources.Equals(root.pendingResource, allocation) {
		t.Errorf("root queue pending allocation failed to increment expected %v, got %v", allocation, root.pendingResource)
	}
	parent.decPendingResource(allocation)
	if !resources.IsZero(root.pendingResource) {
		t.Errorf("root queue pending allocation failed to decrement expected 0, got %v", root.pendingResource)
	}
	// Not allowed to go negative: both will be zero after this
	root.incPendingResource(allocation)
	parent.decPendingResource(allocation)
	if !resources.IsZero(root.pendingResource) {
		t.Errorf("root queue pending allocation failed to decrement expected zero, got %v", root.pendingResource)
	}
	if !resources.IsZero(parent.pendingResource) {
		t.Errorf("parent queue pending allocation should have failed to decrement expected zero, got %v", parent.pendingResource)
	}
}

func TestGetChildQueueInfos(t *testing.T) {
	// create the root
	root, err := createRootQueue()
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}
	var parent *SchedulingQueue
	parent, err = createManagedQueue(root, "parent-man", true)
	if err != nil {
		t.Fatalf("failed to create managed parent queue: %v", err)
	}
	for i := 0; i < 10; i++ {
		_, err = createManagedQueue(parent, "leaf-man"+strconv.Itoa(i), false)
		if err != nil {
			t.Errorf("failed to create managed queue: %v", err)
		}
	}
	if len(parent.childrenQueues) != 10 {
		t.Errorf("managed leaf queues are not added to the parent queue, expected 10 children got %d", len(parent.childrenQueues))
	}

	parent, err = createUnManagedQueue(root, "parent-un", true)
	if err != nil {
		t.Fatalf("failed to create unamanged parent queue: %v", err)
	}
	for i := 0; i < 10; i++ {
		_, err = createUnManagedQueue(parent, "leaf-un-"+strconv.Itoa(i), false)
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
	root, err := createRootQueue()
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf-man", false)
	if err != nil {
		t.Fatalf("failed to create managed leaf queue: %v", err)
	}
	pending := resources.NewResourceFromMap(
		map[string]resources.Quantity{
			resources.MEMORY: 10,
		})
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "test"})
	app.pending = pending
	// adding the app must not update pending resources
	leaf.addSchedulingApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pendingResource), "leaf queue pending resource not zero")

	// add the same app again should not increase the number of apps
	leaf.addSchedulingApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not replaced in the queue as expected")
}

func TestRemoveApplication(t *testing.T) {
	// create the root
	root, err := createRootQueue()
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf-man", false)
	if err != nil {
		t.Fatalf("failed to create managed leaf queue: %v", err)
	}
	// try removing a non existing app
	nonExist := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "test"})
	if leaf.removeSchedulingAppInternal("test") {
		t.Error("Removal of non existing app did not fail")
	}
	leaf.removeSchedulingApplication(nonExist)
	assert.Equal(t, len(leaf.applications), 0, "Removal of non existing app updated unexpected")

	// add an app and remove it
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "exists"})
	leaf.addSchedulingApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pendingResource), "leaf queue pending resource not zero")
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
	assert.Assert(t, resources.IsZero(leaf.pendingResource), "leaf queue pending resource not zero")
	// update pending resources for the hierarchy
	leaf.incPendingResource(pending)
	leaf.removeSchedulingApplication(app)
	assert.Equal(t, len(leaf.applications), 0, "Application was not removed from the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pendingResource), "leaf queue pending resource not updated correctly")
	assert.Assert(t, resources.IsZero(root.pendingResource), "root queue pending resource not updated correctly")
}

func TestQueueStates(t *testing.T) {
	// create the root
	root, err := createRootQueue()
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}

	// add a leaf under the root
	var leaf *SchedulingQueue
	leaf, err = createManagedQueue(root, "leaf", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	err = leaf.CachedQueueInfo.HandleQueueEvent(cache.Stop)
	if err != nil || !leaf.isStopped() {
		t.Errorf("leaf queue is not marked stopped: %v", err)
	}
	err = leaf.CachedQueueInfo.HandleQueueEvent(cache.Start)
	if err != nil || !leaf.isRunning() {
		t.Errorf("leaf queue is not marked running: %v", err)
	}
	err = leaf.CachedQueueInfo.HandleQueueEvent(cache.Remove)
	if err != nil || !leaf.isDraining() {
		t.Errorf("leaf queue is not marked draining: %v", err)
	}
	err = leaf.CachedQueueInfo.HandleQueueEvent(cache.Start)
	if err == nil || !leaf.isDraining() {
		t.Errorf("leaf queue changed state which should not happen: %v", err)
	}
}