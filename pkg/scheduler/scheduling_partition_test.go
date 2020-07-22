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
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
)

func newTestPartition() (*partitionSchedulingContext, error) {
	rootSched, err := createRootQueue(nil)
	if err != nil {
		return nil, err
	}
	rootInfo := rootSched.QueueInfo
	info := &cache.PartitionInfo{
		Name: "default",
		Root: rootInfo,
		RmID: "test",
	}

	return newPartitionSchedulingContext(info, rootSched), nil
}

func TestNewPartition(t *testing.T) {
	partition := newPartitionSchedulingContext(nil, nil)
	if partition != nil {
		t.Fatal("nil input should not have returned partition")
	}
	partition = newPartitionSchedulingContext(&cache.PartitionInfo{}, nil)
	if partition != nil {
		t.Fatal("nil root queue should not have returned partition")
	}

	_, err := newTestPartition()
	if err != nil {
		t.Errorf("test partition create failed with error: %v ", err)
	}
}

func TestAddNode(t *testing.T) {
	partition, err := newTestPartition()
	assert.NilError(t, err, "test partition create failed with error")
	partition.addSchedulingNode(nil)
	assert.Equal(t, 0, len(partition.nodes), "nil node should not be added")
	node := cache.NewNodeForTest("test1", resources.NewResource())
	partition.addSchedulingNode(node)
	assert.Equal(t, 1, len(partition.nodes), "node list not correct")
	// add the same node nothing changes
	partition.addSchedulingNode(node)
	assert.Equal(t, 1, len(partition.nodes), "node list not correct")
	node = cache.NewNodeForTest("test2", resources.NewResource())
	partition.addSchedulingNode(node)
	assert.Equal(t, 2, len(partition.nodes), "node list not correct")
}

func TestRemoveNode(t *testing.T) {
	partition, err := newTestPartition()
	assert.NilError(t, err, "test partition create failed with error")
	partition.addSchedulingNode(cache.NewNodeForTest("test", resources.NewResource()))
	assert.Equal(t, 1, len(partition.nodes), "node list not correct")

	// remove non existing node
	partition.removeSchedulingNode("")
	assert.Equal(t, 1, len(partition.nodes), "nil node should not remove anything")
	partition.removeSchedulingNode("does not exist")
	assert.Equal(t, 1, len(partition.nodes), "non existing node was removed")

	partition.removeSchedulingNode("test")
	assert.Equal(t, 0, len(partition.nodes), "node was not removed")
}

func TestGetNodes(t *testing.T) {
	partition, err := newTestPartition()
	assert.NilError(t, err, "test partition create failed with error")

	nodes := partition.getSchedulableNodes()
	assert.Equal(t, 0, len(nodes), "list should have been empty")

	node1 := "node-1"
	partition.addSchedulingNode(cache.NewNodeForTest(node1, resources.NewResource()))
	node2 := "node-2"
	partition.addSchedulingNode(cache.NewNodeForTest(node2, resources.NewResource()))
	// add one node that will not be returned in the node list
	node3 := "notScheduling"
	node := cache.NewNodeForTest(node3, resources.NewResource())
	node.SetSchedulable(false)
	partition.addSchedulingNode(node)
	node4 := "reserved"
	node = cache.NewNodeForTest(node4, resources.NewResource())
	partition.addSchedulingNode(node)
	// get individual nodes
	schedNode := partition.getSchedulingNode("")
	if schedNode != nil {
		t.Errorf("existing node returned for nil name: %v", schedNode)
	}
	schedNode = partition.getSchedulingNode("does not exist")
	if schedNode != nil {
		t.Errorf("existing node returned for non existing name: %v", schedNode)
	}
	schedNode = partition.getSchedulingNode(node3)
	if schedNode == nil || schedNode.NodeID != node3 {
		t.Error("failed to retrieve existing non scheduling node")
	}
	schedNode = partition.getSchedulingNode(node4)
	if schedNode == nil || schedNode.NodeID != node4 {
		t.Error("failed to retrieve existing reserved node")
	}
	if schedNode != nil {
		schedNode.reservations["app-1|alloc-1"] = &reservation{"", "app-1", "alloc-1", nil, nil, nil}
	}

	assert.Equal(t, 4, len(partition.nodes), "node list not correct")
	nodes = partition.getSchedulableNodes()
	// returned list should be only two long
	assert.Equal(t, 2, len(nodes), "node list not filtered")
	// map iteration is random so don't know which we get first
	for _, schedNode = range nodes {
		if schedNode.NodeID != node1 && schedNode.NodeID != node2 {
			t.Fatalf("unexpected node returned in list: %s", node.NodeID)
		}
	}
	nodes = partition.getSchedulingNodes(false)
	// returned list should be 3 long: no reserved filter
	assert.Equal(t, 3, len(nodes), "node list was incorrectly filtered")
	// check if we have all nodes: since there is a backing map we cannot have duplicates
	for _, schedNode = range nodes {
		if schedNode.NodeID == node3 {
			t.Fatalf("unexpected node returned in list: %s", node.NodeID)
		}
	}
}

func TestGetQueue(t *testing.T) {
	// get the partition
	partition, err := newTestPartition()
	assert.NilError(t, err, "test partition create failed with error")
	var nilQueue *SchedulingQueue
	// test partition has a root queue
	queue := partition.GetQueue("")
	assert.Equal(t, queue, nilQueue, "partition with just root returned not nil for empty request: %v", queue)
	queue = partition.GetQueue("unknown")
	assert.Equal(t, queue, nilQueue, "partition returned not nil for unqualified unknown request: %v", queue)
	queue = partition.GetQueue("root")
	if queue == nil {
		t.Fatalf("partition did not return root as requested")
	}
	resMap := map[string]string{"first": "100"}
	_, err = createManagedQueue(queue, "parent", true, resMap)
	assert.NilError(t, err, "failed to create parent queue")
	queue = partition.GetQueue("root.unknown")
	assert.Equal(t, queue, nilQueue, "partition returned not nil for non existing queue name request: %v", queue)
	queue = partition.GetQueue("root.parent")
	assert.Equal(t, queue == nilQueue, false, "partition returned nil for existing queue name request")
}

// partition is expected to add a basic hierarchy
// root -> parent -> leaf1
//      -> leaf2
// and 2 nodes: node-1 & node-2
func createQueuesNodes(t *testing.T) *partitionSchedulingContext {
	partition, err := newTestPartition()
	assert.NilError(t, err, "test partition create failed with error")
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10"})
	assert.NilError(t, err, "failed to create basic resource")
	node1 := "node-1"
	partition.addSchedulingNode(cache.NewNodeForTest(node1, res))
	node2 := "node-2"
	partition.addSchedulingNode(cache.NewNodeForTest(node2, res))
	// create the root
	var root, parent *SchedulingQueue
	resMap := map[string]string{"first": "100"}
	root, err = createRootQueue(resMap)
	assert.NilError(t, err, "failed to create basic root queue")
	// fake adding the queue structure, just add the root
	partition.root = root
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	_, err = createManagedQueue(parent, "leaf1", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	_, err = createManagedQueue(root, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create parent queue")
	return partition
}

func TestTryAllocate(t *testing.T) {
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %v", alloc.String())
	}

	// create a set of queues and apps: app-1 2 asks; app-2 1 ask (same size)
	// leaf1 will have an app with 2 requests and thus more unconfirmed resources compared to leaf2
	// this should filter up in the parent and the 1st allocate should show an app-1 allocation
	// the ask with the higher priority is the second one added alloc-2 for app-1
	leaf := partition.getQueue("root.parent.leaf1")
	if leaf == nil {
		t.Fatal("leaf queue create failed")
	}
	appID1 := "app-1"
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	appInfo := cache.NewApplicationInfo(appID1, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || err != nil {
		t.Fatalf("failed to create app (%v) and or resource: %v (err = %v)", app, res, err)
	}
	app.queue = leaf

	// fake adding to the partition
	leaf.addSchedulingApplication(app)
	partition.applications[appID1] = app
	var delta *resources.Resource
	delta, err = app.addAllocationAsk(newAllocationAsk("alloc-1", appID1, res))
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("failed to add ask to app resource added: %v expected %v (err = %v)", delta, res, err)
	}
	ask1 := newAllocationAsk("alloc-2", appID1, res)
	if ask1 == nil {
		t.Fatal("ask creation failed for ask1")
	}
	ask1.priority = 2
	delta, err = app.addAllocationAsk(ask1)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("failed to add ask 2 to app 1 resource added: %v expected %v (err = %v)", delta, res, err)
	}

	appID2 := "app-2"
	appInfo = cache.NewApplicationInfo(appID2, "default", "root.unknown", security.UserGroup{}, nil)
	app2 := newSchedulingApplication(appInfo)
	ask2 := newAllocationAsk("alloc-1", appID2, res)
	if app2 == nil || ask2 == nil {
		t.Fatal("failed to create app2 and ask2")
	}
	leaf2 := partition.getQueue("root.leaf2")
	if leaf2 == nil {
		t.Fatal("leaf2 queue create failed")
	}
	app2.queue = leaf2

	// fake adding to the partition
	leaf2.addSchedulingApplication(app2)
	partition.applications[appID2] = app2

	ask2.priority = 2
	delta, err = app2.addAllocationAsk(ask2)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("failed to add ask to app resource added: %v expected %v (err = %v)", delta, res, err)
	}

	// first allocation should be app-1 and alloc-2
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.result, allocated, "result is not the expected allocated")
	assert.Equal(t, len(alloc.releases), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.schedulingAsk.ApplicationID, appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.schedulingAsk.AskProto.AllocationKey, "alloc-2", "expected ask alloc-2 to be allocated")

	// process the allocation like the scheduler does after a try
	toCache := partition.allocate(alloc)
	if !toCache {
		t.Fatalf("normal allocation should be passed back to cache")
	}

	// second allocation should be app-2 and alloc-1: higher up in the queue hierarchy
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.result, allocated, "result is not the expected allocated")
	assert.Equal(t, len(alloc.releases), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.schedulingAsk.ApplicationID, appID2, "expected application app-2 to be allocated")
	assert.Equal(t, alloc.schedulingAsk.AskProto.AllocationKey, "alloc-1", "expected ask alloc-1 to be allocated")

	// process the allocation like the scheduler does after a try
	toCache = partition.allocate(alloc)
	if !toCache {
		t.Fatalf("second normal allocation should be passed back to cache")
	}

	// third allocation should be app-1 and alloc-1
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.result, allocated, "result is not the expected allocated")
	assert.Equal(t, len(alloc.releases), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.schedulingAsk.ApplicationID, appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.schedulingAsk.AskProto.AllocationKey, "alloc-1", "expected ask alloc-1 to be allocated")

	// process the allocation like the scheduler does after a try
	toCache = partition.allocate(alloc)
	if !toCache {
		t.Fatalf("third normal allocation should be passed back to cache")
	}
	if !resources.IsZero(partition.root.GetPendingResource()) {
		t.Fatalf("pending allocations should be set to zero")
	}
}

func TestTryAllocateLarge(t *testing.T) {
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %v", alloc.String())
	}

	// override the reservation delay, and cleanup when done
	OverrideReservationDelay(time.Nanosecond)
	defer OverrideReservationDelay(2 * time.Second)

	leaf := partition.getQueue("root.parent.leaf1")
	if leaf == nil {
		t.Fatal("leaf queue create failed")
	}
	appID := "app-1"
	res, err := resources.NewResourceFromConf(map[string]string{"first": "100"})
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || err != nil {
		t.Fatalf("failed to create app (%v) and or resource: %v (err = %v)", app, res, err)
	}
	app.queue = leaf

	// fake adding to the partition
	leaf.addSchedulingApplication(app)
	partition.applications[appID] = app
	var delta *resources.Resource
	delta, err = app.addAllocationAsk(newAllocationAsk("alloc-1", appID, res))
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("failed to add ask to app resource added: %v expected %v (err = %v)", delta, res, err)
	}
	alloc := partition.tryAllocate()
	if alloc != nil {
		t.Fatalf("allocation did return allocation which does not fit: %s", alloc.String())
	}
	assert.Equal(t, 0, len(app.reservations), "ask should not have been reserved")
}

func TestAllocReserveNewNode(t *testing.T) {
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %v", alloc.String())
	}

	// override the reservation delay, and cleanup when done
	OverrideReservationDelay(time.Nanosecond)
	defer OverrideReservationDelay(2 * time.Second)

	// turn off the second node
	node2 := partition.getSchedulingNode("node-2")
	node2.nodeInfo.SetSchedulable(false)
	leaf := partition.getQueue("root.parent.leaf1")
	if leaf == nil {
		t.Fatal("leaf queue create failed")
	}
	appID := "app-1"
	// only one resource for alloc fits on a node
	res, err := resources.NewResourceFromConf(map[string]string{"first": "8"})
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || err != nil {
		t.Fatalf("failed to create app (%v) and or resource: %v (err = %v)", app, res, err)
	}
	app.queue = leaf

	// fake adding to the partition
	leaf.addSchedulingApplication(app)
	partition.applications[appID] = app
	var delta *resources.Resource
	ask := newAllocationAskRepeat("alloc-1", appID, res, 2)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(resources.Multiply(res, 2), delta) {
		t.Errorf("failed to add ask to app resource added: %v expected %v (err = %v)", delta, res, err)
	}
	// the first one should be allocated
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatalf("1st allocation did not return the correct allocation")
	}
	assert.Equal(t, allocated, alloc.result, "allocation result should have been allocated")
	toCache := partition.allocate(alloc)
	if !toCache {
		t.Fatalf("1st normal allocation should be passed back to cache")
	}
	// the second one should be reserved as the 2nd node is not scheduling
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatalf("2nd allocation did not return the correct allocation")
	}
	assert.Equal(t, reserved, alloc.result, "allocation result should have been reserved")
	nodeReserved := alloc.nodeID
	toCache = partition.allocate(alloc)
	if toCache {
		t.Fatalf("2nd allocation reservation should not be passed back to cache")
	}
	// check if updated (must be after allocate call)
	assert.Equal(t, 1, len(app.reservations), "ask should have been reserved")

	// turn on 2nd node
	node2.nodeInfo.SetSchedulable(true)
	alloc = partition.tryReservedAllocate()
	assert.Equal(t, allocatedReserved, alloc.result, "allocation result should have been allocatedReserved")
	assert.Equal(t, alloc.reservedNodeID, nodeReserved, "node should be set from reserved with new node")
	toCache = partition.allocate(alloc)
	if !toCache {
		t.Fatalf("allocation from reservation should be passed back to cache")
	}
	// check if updated (must be after allocate call)
	assert.Equal(t, 0, len(app.reservations), "ask should have been reserved")
}

func TestTryAllocateReserve(t *testing.T) {
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryReservedAllocate(); alloc != nil {
		t.Fatalf("empty cluster reserved allocate returned allocation: %v", alloc.String())
	}

	// create a set of queues and apps: app-1 2 asks; app-2 1 ask (same size)
	// leaf1 will have an app with 2 requests and thus more unconfirmed resources compared to leaf2
	// this should filter up in the parent and the 1st allocate should show an app-1 allocation
	// the ask with the higher priority is the second one added alloc-2 for app-1
	leaf := partition.getQueue("root.parent.leaf1")
	if leaf == nil {
		t.Fatal("leaf queue create failed")
	}
	appID := "app-1"
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || err != nil {
		t.Fatalf("failed to create app (%v) and or resource: %v (err = %v)", app, res, err)
	}
	app.queue = leaf

	// fake adding to the partition
	leaf.addSchedulingApplication(app)
	partition.applications[appID] = app
	var delta *resources.Resource
	delta, err = app.addAllocationAsk(newAllocationAsk("alloc-1", appID, res))
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("failed to add ask to app resource added: %v expected %v (err = %v)", delta, res, err)
	}
	ask2 := newAllocationAsk("alloc-2", appID, res)
	delta, err = app.addAllocationAsk(ask2)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("failed to add ask to app resource added: %v expected %v (err = %v)", delta, res, err)
	}
	node2 := partition.getSchedulingNode("node-2")
	if node2 == nil {
		t.Fatal("expected node-2 to be returned got nil")
	}
	partition.reserve(app, node2, ask2)
	if !app.isReservedOnNode(node2.NodeID) || len(app.isAskReserved("alloc-2")) == 0 {
		t.Fatalf("reservation failure for ask2 and node2")
	}

	// first allocation should be app-1 and alloc-2
	alloc := partition.tryReservedAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.result, allocatedReserved, "result is not the expected allocated from reserved")
	assert.Equal(t, alloc.reservedNodeID, "", "node should not be set for allocated from reserved")
	assert.Equal(t, len(alloc.releases), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.schedulingAsk.ApplicationID, appID, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.schedulingAsk.AskProto.AllocationKey, "alloc-2", "expected ask alloc-2 to be allocated")

	// process the allocation like the scheduler does after a try
	toCache := partition.allocate(alloc)
	if !toCache {
		t.Fatalf("allocation from reserved should be passed back to cache")
	}
	// reservations should have been removed: it is in progress
	if app.isReservedOnNode(node2.NodeID) || len(app.isAskReserved("alloc-2")) != 0 {
		t.Fatalf("reservation removal failure for ask2 and node2")
	}

	// no reservations left this should return nil
	alloc = partition.tryReservedAllocate()
	if alloc != nil {
		t.Fatalf("reserved allocation should not return any allocation: %v, '%s'", alloc, alloc.reservedNodeID)
	}
	// try non reserved this should allocate
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.result, allocated, "result is not the expected allocated")
	assert.Equal(t, len(alloc.releases), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.schedulingAsk.ApplicationID, appID, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.schedulingAsk.AskProto.AllocationKey, "alloc-1", "expected ask alloc-1 to be allocated")
	if !resources.IsZero(partition.root.GetPendingResource()) {
		t.Fatalf("pending allocations should be set to zero")
	}
}

func TestTryAllocateWithReserved(t *testing.T) {
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryReservedAllocate(); alloc != nil {
		t.Fatalf("empty cluster reserved allocate returned allocation: %v", alloc.String())
	}

	leaf := partition.getQueue("root.parent.leaf1")
	if leaf == nil {
		t.Fatal("leaf queue create failed")
	}
	appID := "app-1"
	res, err := resources.NewResourceFromConf(map[string]string{"first": "5"})
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || err != nil {
		t.Fatalf("failed to create app (%v) and or resource: %v (err = %v)", app, res, err)
	}
	app.queue = leaf

	// fake adding to the partition
	leaf.addSchedulingApplication(app)
	partition.applications[appID] = app
	var delta *resources.Resource
	ask := newAllocationAskRepeat("alloc-1", appID, res, 2)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(resources.Multiply(res, 2), delta) {
		t.Errorf("failed to add ask to app resource added: %v expected %v (err = %v)", delta, resources.Multiply(res, 2), err)
	}
	// reserve one node: scheduling should happen on the other
	node2 := partition.getSchedulingNode("node-2")
	if node2 == nil {
		t.Fatal("expected node-2 to be returned got nil")
	}
	partition.reserve(app, node2, ask)
	if !app.isReservedOnNode(node2.NodeID) || len(app.isAskReserved("alloc-1")) == 0 {
		t.Fatalf("reservation failure for ask and node2")
	}
	assert.Equal(t, 1, len(partition.reservedApps), "partition should have reserved app")
	alloc := partition.tryAllocate()
	if alloc == nil || alloc.reservedNodeID == "" {
		t.Fatalf("allocation did not return correct allocation %v, %s", alloc, alloc.reservedNodeID)
	}
	assert.Equal(t, allocatedReserved, alloc.result, "expected reserved allocation to be returned")
	assert.Equal(t, node2.NodeID, alloc.reservedNodeID, "expected nodeID to be set on tryAllocate")

	// confirm the outcome
	partition.allocate(alloc)
	assert.Equal(t, 0, len(node2.reservations), "reservation should have been removed from node")
	assert.Equal(t, false, app.isReservedOnNode(node2.NodeID), "reservation cleanup for ask on app failed")

	// node2 is unreserved now so the next one should allocate on the 2nd node (fair sharing)
	alloc = partition.tryAllocate()
	if alloc == nil || alloc.reservedNodeID != "" {
		t.Fatalf("allocation did not return correct allocation %v, %s", alloc, alloc.reservedNodeID)
	}
	assert.Equal(t, allocated, alloc.result, "expected allocated allocation to be returned")
	assert.Equal(t, node2.NodeID, alloc.nodeID, "expected allocation on node2 to be returned")
}

// remove the reserved ask while allocating in flight for the ask
func TestScheduleRemoveReservedAsk(t *testing.T) {
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %v", alloc.String())
	}

	// override the reservation delay, and cleanup when done
	OverrideReservationDelay(time.Nanosecond)
	defer OverrideReservationDelay(2 * time.Second)

	leaf := partition.getQueue("root.parent.leaf1")
	if leaf == nil {
		t.Fatal("leaf queue create failed")
	}
	appID := "app-1"
	res, err := resources.NewResourceFromConf(map[string]string{"first": "4"})
	appInfo := cache.NewApplicationInfo(appID, "default", "root.unknown", security.UserGroup{}, nil)
	app := newSchedulingApplication(appInfo)
	if app == nil || err != nil {
		t.Fatalf("failed to create app (%v) and or resource: %v (err = %v)", app, res, err)
	}
	app.queue = leaf

	// fake adding to the partition
	leaf.addSchedulingApplication(app)
	partition.applications[appID] = app
	var delta *resources.Resource
	ask := newAllocationAskRepeat("alloc-1", appID, res, 4)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(resources.Multiply(res, 4), delta) {
		t.Errorf("failed to add ask to app resource added: %v expected %v (err = %v)", delta, resources.Multiply(res, 4), err)
	}
	// allocate the ask
	for i := 1; i <= 4; i++ {
		alloc := partition.tryAllocate()
		if alloc == nil || alloc.result != allocated {
			t.Fatalf("expected allocated allocation to be returned (step %d) %v", i, alloc)
		}
		partition.allocate(alloc)
	}

	// add a asks which should reserve
	ask = newAllocationAskRepeat("alloc-2", appID, res, 1)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("failed to add ask 2 to app resource added: %v expected %v (err = %v)", delta, resources.Multiply(res, 2), err)
	}
	ask = newAllocationAskRepeat("alloc-3", appID, res, 1)
	delta, err = app.addAllocationAsk(ask)
	if err != nil || !resources.Equals(res, delta) {
		t.Errorf("failed to add ask 3 to app resource added: %v expected %v (err = %v)", delta, resources.Multiply(res, 2), err)
	}
	// allocate so we get reservations
	for i := 1; i <= 2; i++ {
		alloc := partition.tryAllocate()
		if alloc == nil || alloc.result != reserved {
			t.Fatalf("expected reserved allocation to be returned (step %d) %v", i, alloc)
		}
		partition.allocate(alloc)
	}
	assert.Equal(t, len(partition.reservedApps), 1, "partition should have reserved app")
	assert.Equal(t, len(app.reservations), 2, "application reservations should be 2")

	// add a node
	node3 := "node-3"
	partition.addSchedulingNode(cache.NewNodeForTest(node3, res))
	// try to allocate one of the reservation
	alloc := partition.tryReservedAllocate()
	if alloc == nil || alloc.result != allocatedReserved {
		t.Fatalf("expected allocatedReserved allocation to be returned %v", alloc)
	}
	// before confirming remove the ask: do what the scheduler does when it gets a request from a
	// shim in processAllocationReleaseByAllocationKey()
	// make sure we are counting correctly and leave the other reservation intact
	removeAskID := "alloc-2"
	if alloc.schedulingAsk.AskProto.AllocationKey == "alloc-3" {
		removeAskID = "alloc-3"
	}
	released := app.removeAllocationAsk(removeAskID)
	assert.Equal(t, released, 1, "expected one reservations to be released")
	partition.unReserveUpdate(appID, released)
	assert.Equal(t, len(partition.reservedApps), 1, "partition should still have reserved app")
	assert.Equal(t, len(app.reservations), 1, "application reservations should be 1")

	// now confirm the allocation: this should not remove the reservation
	partition.allocate(alloc)
	assert.Equal(t, len(partition.reservedApps), 1, "partition should still have reserved app")
	assert.Equal(t, len(app.reservations), 1, "application reservations should be kept at 1")
}
