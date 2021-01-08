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
	"strconv"
	"strings"
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
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
	if !resources.IsZero(root.preempting) && !resources.IsZero(root.pending) {
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
	leaf.applications.RemoveApplication(appID1)
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
	leaf.applications.RemoveApplication(appID1)
	if !leaf.RemoveQueue() && len(parent.children) != 0 {
		t.Error("leaf queue should have been removed and parent updated and was not")
	}
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

func TestGetChildQueueInfos(t *testing.T) {
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
			resources.MEMORY: 10,
		})
	app := newApplication(appID1, "default", "root.parent.leaf")
	app.pending = pending
	// adding the app must not update pending resources
	leaf.AddApplication(app)
	assert.Equal(t, leaf.applications.Size(), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not zero")

	// add the same app again should not increase the number of apps
	leaf.AddApplication(app)
	assert.Equal(t, leaf.applications.Size(), 1, "Application was not replaced in the queue as expected")
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
	assert.Equal(t, leaf.applications.Size(), 1, "Application was not added to the managed queue as expected")
	if leaf.GetMaxResource() != nil {
		t.Errorf("Max resources should not be set on managed queue got: %s", leaf.GetMaxResource().String())
	}
	app = newApplication("app-2", "default", "root.leaf-un")
	leafUn.AddApplication(app)
	assert.Equal(t, leaf.applications.Size(), 1, "Application was not added to the Dynamic queue as expected")
	if leafUn.GetMaxResource() != nil {
		t.Errorf("Max resources should not be set on Dynamic queue got: %s", leafUn.GetMaxResource().String())
	}

	maxRes := resources.NewResourceFromMap(
		map[string]resources.Quantity{
			"first": 10,
		})
	tags := make(map[string]string)
	tags[appTagNamespaceResourceQuota] = "{\"resources\":{\"first\":{\"value\":10}}}"
	// add apps again now with the tag set
	app = newApplicationWithTags("app-3", "default", "root.leaf-man", tags)
	leaf.AddApplication(app)
	assert.Equal(t, leaf.applications.Size(), 2, "Application was not added to the managed queue as expected")
	if leaf.GetMaxResource() != nil {
		t.Errorf("Max resources should not be set on managed queue got: %s", leaf.GetMaxResource().String())
	}
	app = newApplicationWithTags("app-4", "default", "root.leaf-un", tags)
	leafUn.AddApplication(app)
	assert.Equal(t, leaf.applications.Size(), 2, "Application was not added to the Dynamic queue as expected")
	if !resources.Equals(leafUn.GetMaxResource(), maxRes) {
		t.Errorf("Max resources not set as expected: %s got: %v", maxRes.String(), leafUn.GetMaxResource())
	}

	// set to illegal limit (0 value)
	tags[appTagNamespaceResourceQuota] = "{\"resources\":{\"first\":{\"value\":0}}}"
	app = newApplicationWithTags("app-4", "default", "root.leaf-un", tags)
	leafUn.AddApplication(app)
	assert.Equal(t, leaf.applications.Size(), 2, "Application was not added to the Dynamic queue as expected")
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
	assert.Equal(t, leaf.applications.Size(), 0, "Removal of non existing app updated unexpected")

	// add an app and remove it
	app := newApplication("exists", "default", "root.leaf-man")
	leaf.AddApplication(app)
	assert.Equal(t, leaf.applications.Size(), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not zero")
	leaf.RemoveApplication(nonExist)
	assert.Equal(t, leaf.applications.Size(), 1, "Non existing application was removed from the queue")
	leaf.RemoveApplication(app)
	assert.Equal(t, leaf.applications.Size(), 0, "Application was not removed from the queue as expected")

	// try the same again now with pending resources set
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	app.pending.AddTo(res)
	leaf.AddApplication(app)
	assert.Equal(t, leaf.applications.Size(), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not zero")
	// update pending resources for the hierarchy
	leaf.incPendingResource(res)
	leaf.RemoveApplication(app)
	assert.Equal(t, leaf.applications.Size(), 0, "Application was not removed from the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not updated correctly")
	assert.Assert(t, resources.IsZero(root.pending), "root queue pending resource not updated correctly")

	app.allocatedResource.AddTo(res)
	app.pending = resources.NewResource()
	leaf.AddApplication(app)
	assert.Equal(t, leaf.applications.Size(), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.allocatedResource), "leaf queue pending resource not zero")
	// update allocated resources for the hierarchy
	err = leaf.IncAllocatedResource(res, false)
	assert.NilError(t, err, "increment of allocated resource on queue should not have failed")
	leaf.RemoveApplication(app)
	assert.Equal(t, leaf.applications.Size(), 0, "Application was not removed from the queue as expected")
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

func TestPreemptingCalc(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")

	res := map[string]string{"first": "1"}
	var allocRes *resources.Resource
	allocRes, err = resources.NewResourceFromConf(res)
	assert.NilError(t, err, "failed to create basic resource")
	if !resources.IsZero(leaf.preempting) {
		t.Errorf("leaf queue preempting resources not set as expected 0, got %v", leaf.preempting)
	}
	if !resources.IsZero(root.preempting) {
		t.Errorf("root queue preempting resources not set as expected 0, got %v", root.preempting)
	}
	// preempting does not filter up the hierarchy, check that
	leaf.IncPreemptingResource(allocRes)
	// using the get function to access the value
	if !resources.Equals(allocRes, leaf.GetPreemptingResource()) {
		t.Errorf("queue preempting resources not set as expected %v, got %v", allocRes, leaf.preempting)
	}
	if !resources.IsZero(root.GetPreemptingResource()) {
		t.Errorf("root queue preempting resources not set as expected 0, got %v", root.preempting)
	}
	newRes := resources.Multiply(allocRes, 2)
	leaf.decPreemptingResource(newRes)
	if !resources.IsZero(leaf.GetPreemptingResource()) {
		t.Errorf("queue preempting resources not set as expected 0, got %v", leaf.preempting)
	}
	leaf.setPreemptingResource(newRes)
	if !resources.Equals(leaf.GetPreemptingResource(), resources.Multiply(allocRes, 2)) {
		t.Errorf("queue preempting resources not set as expected %v, got %v", newRes, leaf.preempting)
	}
}

// This test must not test the sorter that is underlying.
// It tests the DefaultQueueRequestManager specific parts of the code only.
func TestSortApplications(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf, parent *Queue
	// empty parent queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue: %v")
	appIt := parent.GetApplications().SortForAllocation()
	assert.Assert(t, !appIt.HasNext() && appIt.Size() == 0, "parent queue should return no app")

	// empty leaf queue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	appIt = leaf.GetApplications().SortForAllocation()
	assert.Assert(t, !appIt.HasNext() && appIt.Size() == 0, "empty queue should return no app")

	// new app does not have pending res, does not get returned
	app := newApplication(appID1, "default", leaf.QueuePath)
	app.queue = leaf
	leaf.AddApplication(app)
	appIt = leaf.GetApplications().SortForAllocation()
	assert.Assert(t, !appIt.HasNext() && appIt.Size() == 0, "app without ask should not be in sorted apps")

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	// add an ask app must be returned
	err = app.AddAllocationAsk(newAllocationAsk("alloc-1", appID1, res))
	assert.NilError(t, err, "failed to add allocation ask")
	appIt = leaf.GetApplications().SortForAllocation()
	assert.Assert(t, appIt.HasNext() && appIt.Size() == 1, "sorted application is missing expected app")
	checkApp := appIt.Next()
	assert.Assert(t, checkApp.GetApplicationID() == appID1, "sorted application is missing expected app")

	// set 0 repeat
	_, err = app.updateAskRepeat("alloc-1", -1)
	if err != nil {
		t.Errorf("failed to update ask repeat: %v (err = %v)", app, err)
	}
	appIt = leaf.GetApplications().SortForAllocation()
	assert.Assert(t, !appIt.HasNext() && appIt.Size() == 0, "app with ask but 0 pending resources should not be in sorted apps")
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
	headRoom = parent.getMaxHeadRoom()
	if err != nil || !resources.Equals(res, headRoom) {
		t.Errorf("parent queue head room not as expected %v, got: %v (err %v)", res, headRoom, err)
	}

	// leaf1 headroom = MIN(parentHeadRoom, leaf1Max - leaf1Allocated)
	// leaf1 headroom = MIN((10,2), (10-5, 8-3)) = MIN((10,2), (5,5)) = MIN(5, 2)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "2"})
	headRoom = leaf1.getMaxHeadRoom()
	if err != nil || !resources.Equals(res, headRoom) {
		t.Errorf("leaf1 queue head room not as expected %v, got: %v (err %v)", res, headRoom, err)
	}

	// leaf2 headroom = parentMax - leaf1Allocated - leaf2Allocated
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "2"})
	headRoom = leaf2.getMaxHeadRoom()
	if err != nil || !resources.Equals(res, headRoom) {
		t.Errorf("leaf2 queue head room not as expected %v, got: %v (err %v)", res, headRoom, err)
	}
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
	if !resources.Equals(res, maxUsage) {
		t.Errorf("leaf queue should have merged max set expected %v, got: %v", res, maxUsage)
	}

	// replace parent with one with limit on different resource
	resMap = map[string]string{"third": "2"}
	parent, err = createManagedQueue(root, "parent2", true, resMap)
	assert.NilError(t, err, "failed to create parent2 queue")
	maxUsage = parent.GetMaxResource()
	res, err = resources.NewResourceFromConf(map[string]string{"first": "0", "second": "0", "third": "0"})
	if err != nil || !resources.Equals(res, maxUsage) {
		t.Errorf("parent2 queue should have max from root set expected %v, got: %v (err %v)", res, maxUsage, err)
	}
	resMap = map[string]string{"first": "5", "second": "10"}
	leaf, err = createManagedQueue(parent, "leaf2", false, resMap)
	assert.NilError(t, err, "failed to create leaf2 queue")
	maxUsage = leaf.GetMaxResource()
	res, err = resources.NewResourceFromConf(map[string]string{"first": "0", "second": "0", "third": "0"})
	if err != nil || !resources.Equals(res, maxUsage) {
		t.Errorf("leaf2 queue should have reset merged max set expected %v, got: %v (err %v)", res, maxUsage, err)
	}
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
	if unknown := leaf.getApplication("unknown"); unknown != nil {
		t.Errorf("un registered app found using appID which should not happen: %v", unknown)
	}

	// add app and check proper returns
	app := newApplication(appID1, "default", leaf.QueuePath)
	leaf.AddApplication(app)
	assert.Equal(t, leaf.applications.Size(), 1, "queue should have one app registered")
	if leaf.getApplication(appID1) == nil {
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
	// verify the outstanding requests for each of the queue is up to its max capacity, 10/5 respectively
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
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"cpu": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	for i := 0; i < 20; i++ {
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
	res, err = resources.NewResourceFromConf(map[string]string{"cpu": "5"})
	assert.NilError(t, err, "failed to create basic resource")
	err = queue1.IncAllocatedResource(res, false)
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

func TestGetOutstandingRequestNoMax(t *testing.T) {
	// queue structure:
	// root
	//   - queue1
	//   - queue2
	//
	// both queues have no max capacity set
	// submit app1 to root.queue1, app2 to root.queue2
	// app1 asks for 10 1x1CPU requests, app2 asks for 20 1x1CPU requests
	// verify all these requests are outstanding
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
	assert.Equal(t, len(leaf.properties), 1, "leaf queue properties size incorrect")
	assert.Equal(t, leaf.properties[configs.ApplicationSortPolicy], "stateaware", "leaf queue property value not as expected")
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

func TestGetQueueInfos(t *testing.T) {
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
	rootDaoInfo := root.GetQueueInfos()

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

	rootDaoInfo := root.GetQueueInfos()

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
	emptyRes := "[]"
	if queue.allocatedResource == nil {
		assert.Equal(t, emptyRes, dao.Capacities.UsedCapacity)
	} else {
		assert.Equal(t, strings.Trim(queue.allocatedResource.String(), "map"), dao.Capacities.UsedCapacity)
	}
	if queue.maxResource == nil {
		assert.Equal(t, emptyRes, dao.Capacities.MaxCapacity)
	} else {
		assert.Equal(t, strings.Trim(queue.maxResource.String(), "map"), dao.Capacities.MaxCapacity)
	}
	if queue.guaranteedResource == nil {
		assert.Equal(t, emptyRes, dao.Capacities.Capacity)
	} else {
		assert.Equal(t, strings.Trim(queue.guaranteedResource.String(), "map"), dao.Capacities.Capacity)
	}
	assert.Equal(t, len(queue.properties), len(dao.Properties))
	if len(queue.properties) > 0 {
		for k, v := range queue.properties {
			assert.Equal(t, v, dao.Properties[k])
		}
	}
}
