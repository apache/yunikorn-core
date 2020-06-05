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

package cache

import (
	"strconv"
	"strings"
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
)

// create the root queue, base for all testing
func createRootQueue() (*QueueInfo, error) {
	rootConf := configs.QueueConfig{
		Name:       "root",
		Parent:     true,
		Queues:     nil,
		Properties: make(map[string]string),
	}
	return NewManagedQueue(rootConf, nil)
}

// wrapper around the create call using the same syntax as an unmanaged queue
func createManagedQueue(parentQI *QueueInfo, name string, parent bool) (*QueueInfo, error) {
	return createManagedQueueProps(parentQI, name, parent, make(map[string]string))
}

// create managed queue with props set
func createManagedQueueProps(parentQI *QueueInfo, name string, parent bool, props map[string]string) (*QueueInfo, error) {
	childConf := configs.QueueConfig{
		Name:       name,
		Parent:     parent,
		Queues:     nil,
		Properties: props,
	}
	return NewManagedQueue(childConf, parentQI)
}

// wrapper around the create call using the same syntax as a managed queue
func createUnManagedQueue(parentQI *QueueInfo, name string, parent bool) (*QueueInfo, error) {
	return NewUnmanagedQueue(name, !parent, parentQI)
}

// base test for creating a managed queue
func TestQueueInfo(t *testing.T) {
	// create the root
	root, err := createRootQueue()
	assert.NilError(t, err, "failed to create basic root queue")
	// check the state of the queue
	if !root.isManaged && !root.isLeaf && !root.IsRunning() {
		t.Errorf("root queue status is incorrect")
	}
	// allocations should be nil
	if !resources.IsZero(root.GetAllocatedResource()) {
		t.Errorf("root queue must not have allocations set on create")
	}
}

func TestAllocationCalcRoot(t *testing.T) {
	// create the root
	root, err := createRootQueue()
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
	root, err := createRootQueue()
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *QueueInfo
	parent, err = createManagedQueue(root, "parent", true)
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

func TestManagedSubQueues(t *testing.T) {
	// create the root
	root, err := createRootQueue()
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *QueueInfo
	parent, err = createManagedQueue(root, "parent", true)
	assert.NilError(t, err, "failed to create parent queue")
	if parent.isLeaf {
		t.Errorf("parent queue is not marked as a parent")
	}
	if len(root.children) == 0 {
		t.Errorf("parent queue is not added to the root queue")
	}
	var leaf *QueueInfo
	leaf, err = createManagedQueue(parent, "leaf", false)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(parent.children) == 0 {
		t.Errorf("leaf queue is not added to the parent queue")
	}
	if !leaf.isLeaf || !leaf.isManaged {
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
	if !leaf.RemoveQueue() && len(parent.children) != 0 {
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

func TestQueueProps(t *testing.T) {
	// create the root
	root, err := createRootQueue()
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *QueueInfo
	props := map[string]string{"first": "value", "second": "other value"}
	parent, err = createManagedQueueProps(root, "parent", true, props)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Assert(t, !parent.isLeaf, "parent queue is not marked as a parent")
	assert.Equal(t, len(root.children), 1, "parent queue is not added to the root queue")
	assert.Equal(t, len(parent.properties), 2, "parent queue properties expected 2, got %v", parent.properties)

	var leaf *QueueInfo
	leaf, err = createManagedQueue(parent, "leaf", false)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, len(parent.children), 1, "leaf queue is not added to the parent queue")
	assert.Assert(t, leaf.isLeaf && leaf.isManaged, "leaf queue is not marked as managed leaf")
	assert.Equal(t, len(leaf.properties), 2, "leaf queue properties size incorrect")

	props = map[string]string{"first": "not inherited", configs.ApplicationSortPolicy: "stateaware"}
	parent, err = createManagedQueueProps(root, "parent2", true, props)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Equal(t, len(parent.properties), 2, "parent queue properties size incorrect")
	leaf, err = createUnManagedQueue(parent, "leaf", false)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Assert(t, leaf.isLeaf && !leaf.isManaged, "leaf queue is not marked as unmanaged leaf")
	assert.Equal(t, len(leaf.properties), 1, "leaf queue properties size incorrect")
	assert.Equal(t, leaf.properties[configs.ApplicationSortPolicy], "stateaware", "leaf queue property value not as expected")
}

func TestUnManagedSubQueues(t *testing.T) {
	// create the root
	root, err := createRootQueue()
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *QueueInfo
	parent, err = createUnManagedQueue(root, "parent-man", true)
	assert.NilError(t, err, "failed to create parent queue")
	if parent.isLeaf {
		t.Errorf("parent queue is not marked as a parent")
	}
	if len(root.children) == 0 {
		t.Errorf("parent queue is not added to the root queue")
	}
	var leaf *QueueInfo
	leaf, err = createUnManagedQueue(parent, "leaf", false)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(parent.children) == 0 {
		t.Errorf("leaf queue is not added to the parent queue")
	}
	if !leaf.isLeaf || leaf.isManaged {
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
	if !leaf.RemoveQueue() && len(parent.children) != 0 {
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

func TestGetChildQueueInfos(t *testing.T) {
	// create the root
	root, err := createRootQueue()
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *QueueInfo
	parent, err = createManagedQueue(root, "parent-man", true)
	assert.NilError(t, err, "failed to create basic managed parent queue")
	for i := 0; i < 10; i++ {
		_, err = createManagedQueue(parent, "leaf-man"+strconv.Itoa(i), false)
		assert.NilError(t, err, "failed to create managed queue")
	}
	if len(parent.children) != 10 {
		t.Errorf("managed leaf queues are not added to the parent queue, expected 10 children got %d", len(parent.children))
	}

	parent, err = createUnManagedQueue(root, "parent-un", true)
	assert.NilError(t, err, "failed to create basic unmanaged parent queue")
	for i := 0; i < 10; i++ {
		_, err = createUnManagedQueue(parent, "leaf-un-"+strconv.Itoa(i), false)
		assert.NilError(t, err, "failed to create basic unmanaged queue")
	}
	if len(parent.children) != 10 {
		t.Errorf("unmanaged leaf queues are not added to the parent queue, expected 10 children got %d", len(parent.children))
	}

	// check the root queue
	if len(root.children) != 2 {
		t.Errorf("parent queues are not added to the root queue, expected 2 children got %d", len(root.children))
	}
}

func TestMaxResource(t *testing.T) {
	resMap := map[string]string{"first": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create basic resource")
	// create the root
	var root, parent *QueueInfo
	root, err = createRootQueue()
	assert.NilError(t, err, "failed to create basic root queue")
	parent, err = createManagedQueue(root, "parent", true)
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
	var root, parent, leaf *QueueInfo
	root, err = createRootQueue()
	assert.NilError(t, err, "failed to create basic root queue")
	// Nothing set max should be nil
	if root.GetMaxResource() != nil {
		t.Errorf("empty cluster should not have max set on root queue")
	}

	// try setting on the parent or managed leaf (nothing should change)
	parent, err = createUnManagedQueue(root, "parent-un", true)
	assert.NilError(t, err, "failed to create basic unmanaged parent queue")
	parent.UpdateUnManagedMaxResource(res)
	if parent.GetMaxResource() != nil {
		t.Errorf("unmanaged parent queue change should have been rejected expected nil got: %v", parent.GetMaxResource())
	}
	parent, err = createManagedQueue(root, "parent-man", true)
	assert.NilError(t, err, "failed to create basic unmanaged parent queue")
	parent.UpdateUnManagedMaxResource(res)
	if parent.GetMaxResource() != nil {
		t.Errorf("managed parent queue change should have been rejected expected nil got: %v", parent.GetMaxResource())
	}

	leaf, err = createManagedQueue(root, "leaf-man", false)
	assert.NilError(t, err, "failed to create basic managed leaf queue")
	// Set on the root should change
	leaf.UpdateUnManagedMaxResource(res)
	if leaf.GetMaxResource() != nil {
		t.Errorf("managed leaf queue change should have been rejected expected nil got: %v", parent.GetMaxResource())
	}

	// try setting on the unmanaged leaf (should change)
	leaf, err = createUnManagedQueue(root, "leaf-un", false)
	assert.NilError(t, err, "failed to create basic unmanaged leaf queue")
	leaf.UpdateUnManagedMaxResource(res)
	if !resources.Equals(res, leaf.GetMaxResource()) {
		t.Errorf("leaf max setting not set as expected %v, got %v", res, leaf.GetMaxResource())
	}
}

func TestGetQueueInfos(t *testing.T) {
	root, err := createRootQueue()
	assert.NilError(t, err, "failed to create basic root queue: %v", err)
	var rootMax *resources.Resource
	rootMax, err = resources.NewResourceFromConf(map[string]string{"memory": "2048", "vcores": "10"})
	assert.NilError(t, err, "failed to create configuration: %v", err)
	root.setMaxResource(rootMax)

	var parentUsed *resources.Resource
	parentUsed, err = resources.NewResourceFromConf(map[string]string{"memory": "1012", "vcores": "2"})
	assert.NilError(t, err, "failed to create resource: %v", err)
	var parent *QueueInfo
	parent, err = createManagedQueue(root, "parent", true)
	assert.NilError(t, err, "failed to create queue: %v", err)
	err = parent.IncAllocatedResource(parentUsed, false)
	assert.NilError(t, err, "failed to increment allocated resource: %v", err)

	var child1used *resources.Resource
	child1used, err = resources.NewResourceFromConf(map[string]string{"memory": "1012", "vcores": "2"})
	assert.NilError(t, err, "failed to create resource: %v", err)
	var child1 *QueueInfo
	child1, err = createManagedQueue(parent, "child1", true)
	assert.NilError(t, err, "failed to create queue: %v", err)
	err = child1.IncAllocatedResource(child1used, false)
	assert.NilError(t, err, "failed to increment allocated resource: %v", err, err)

	var child2 *QueueInfo
	child2, err = createManagedQueue(parent, "child2", true)
	assert.NilError(t, err, "failed to create child queue: %v", err)
	child2.setMaxResource(resources.NewResource())

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

func compareQueueInfoWithDAO(t *testing.T, queueInfo *QueueInfo, dao dao.QueueDAOInfo) {
	assert.Equal(t, queueInfo.Name, dao.QueueName)
	assert.Equal(t, len(queueInfo.children), len(dao.ChildQueues))
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
}
