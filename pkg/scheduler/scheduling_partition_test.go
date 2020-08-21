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
	"reflect"
	"testing"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common/commonevents"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
)

func newTestPartition() (*PartitionSchedulingContext, error) {
	config, _ := createSchedulerConfig([]byte(configDefault))
	return newSchedulingPartitionFromConfig(config.Partitions[0], "test")
}

func TestNewPartition(t *testing.T) {
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
	node := newNodeForTest("test1", resources.NewResource())
	partition.addSchedulingNode(node)
	assert.Equal(t, 1, len(partition.nodes), "node list not correct")
	// add the same node nothing changes
	partition.addSchedulingNode(node)
	assert.Equal(t, 1, len(partition.nodes), "node list not correct")
	node = newNodeForTest("test2", resources.NewResource())
	partition.addSchedulingNode(node)
	assert.Equal(t, 2, len(partition.nodes), "node list not correct")
}

//FIXME: should merge the two methods
func TestRemoveNode_2(t *testing.T) {
	partition, err := newTestPartition()
	assert.NilError(t, err, "test partition create failed with error")
	partition.addSchedulingNode(newNodeForTest("test", resources.NewResource()))
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
	partition.addSchedulingNode(newNodeForTest(node1, resources.NewResource()))
	node2 := "node-2"
	partition.addSchedulingNode(newNodeForTest(node2, resources.NewResource()))
	// add one node that will not be returned in the node list
	node3 := "notScheduling"
	node := newNodeForTest(node3, resources.NewResource())
	node.SetSchedulable(false)
	partition.addSchedulingNode(node)
	node4 := "reserved"
	node = newNodeForTest(node4, resources.NewResource())
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
func createQueuesNodes(t *testing.T) *PartitionSchedulingContext {
	partition, err := newTestPartition()
	assert.NilError(t, err, "test partition create failed with error")
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10"})
	assert.NilError(t, err, "failed to create basic resource")
	node1 := "node-1"
	partition.addSchedulingNode(newNodeForTest(node1, res))
	node2 := "node-2"
	partition.addSchedulingNode(newNodeForTest(node2, res))
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
	app := newSchedulingAppInternal(appID1, "default", "root.unknown", security.UserGroup{}, nil)
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
	app2 := newSchedulingAppInternal(appID2, "default", "root.unknown", security.UserGroup{}, nil)
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
	assert.Equal(t, alloc.SchedulingAsk.ApplicationID, appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.SchedulingAsk.AskProto.AllocationKey, "alloc-2", "expected ask alloc-2 to be allocated")

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
	assert.Equal(t, alloc.SchedulingAsk.ApplicationID, appID2, "expected application app-2 to be allocated")
	assert.Equal(t, alloc.SchedulingAsk.AskProto.AllocationKey, "alloc-1", "expected ask alloc-1 to be allocated")

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
	assert.Equal(t, alloc.SchedulingAsk.ApplicationID, appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.SchedulingAsk.AskProto.AllocationKey, "alloc-1", "expected ask alloc-1 to be allocated")

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
	OverrideReservationDelay(10 * time.Nanosecond)
	defer OverrideReservationDelay(2 * time.Second)

	leaf := partition.getQueue("root.parent.leaf1")
	if leaf == nil {
		t.Fatal("leaf queue create failed")
	}
	appID := "app-1"
	res, err := resources.NewResourceFromConf(map[string]string{"first": "100"})
	app := newSchedulingAppInternal(appID, "default", "root.unknown", security.UserGroup{}, nil)
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
	OverrideReservationDelay(10 * time.Nanosecond)
	defer OverrideReservationDelay(2 * time.Second)

	// turn off the second node
	node2 := partition.getSchedulingNode("node-2")
	node2.SetSchedulable(false)
	leaf := partition.getQueue("root.parent.leaf1")
	if leaf == nil {
		t.Fatal("leaf queue create failed")
	}
	appID := "app-1"
	// only one resource for alloc fits on a node
	res, err := resources.NewResourceFromConf(map[string]string{"first": "8"})
	app := newSchedulingAppInternal(appID, "default", "root.unknown", security.UserGroup{}, nil)
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
	node2.SetSchedulable(true)
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
	app := newSchedulingAppInternal(appID, "default", "root.unknown", security.UserGroup{}, nil)
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
	assert.Equal(t, alloc.SchedulingAsk.ApplicationID, appID, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.SchedulingAsk.AskProto.AllocationKey, "alloc-2", "expected ask alloc-2 to be allocated")

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
	assert.Equal(t, alloc.SchedulingAsk.ApplicationID, appID, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.SchedulingAsk.AskProto.AllocationKey, "alloc-1", "expected ask alloc-1 to be allocated")
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
	app := newSchedulingAppInternal(appID, "default", "root.unknown", security.UserGroup{}, nil)
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
	OverrideReservationDelay(10 * time.Nanosecond)
	defer OverrideReservationDelay(2 * time.Second)

	leaf := partition.getQueue("root.parent.leaf1")
	if leaf == nil {
		t.Fatal("leaf queue create failed")
	}
	appID := "app-1"
	res, err := resources.NewResourceFromConf(map[string]string{"first": "4"})
	app := newSchedulingAppInternal(appID, "default", "root.unknown", security.UserGroup{}, nil)

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
	partition.addSchedulingNode(newNodeForTest(node3, res))
	// try to allocate one of the reservation
	alloc := partition.tryReservedAllocate()
	if alloc == nil || alloc.result != allocatedReserved {
		t.Fatalf("expected allocatedReserved allocation to be returned %v", alloc)
	}
	// before confirming remove the ask: do what the scheduler does when it gets a request from a
	// shim in processAllocationAsksToRelease()
	// make sure we are counting correctly and leave the other reservation intact
	removeAskID := "alloc-2"
	if alloc.SchedulingAsk.AskProto.AllocationKey == "alloc-3" {
		removeAskID = "alloc-3"
	}
	released := app.removeAllocationAskInternal(removeAskID)
	assert.Equal(t, released, 1, "expected one reservations to be released")
	partition.unReserveUpdate(appID, released)
	assert.Equal(t, len(partition.reservedApps), 1, "partition should still have reserved app")
	assert.Equal(t, len(app.reservations), 1, "application reservations should be 1")

	// now confirm the allocation: this should not remove the reservation
	partition.allocate(alloc)
	assert.Equal(t, len(partition.reservedApps), 1, "partition should still have reserved app")
	assert.Equal(t, len(app.reservations), 1, "application reservations should be kept at 1")
}

const configDefault = `
partitions:
  - name: default
    queues:
      - name: root
        queues:
        - name: default
`

func createAllocation(queue, nodeID, allocID, appID string) *si.Allocation {
	resAlloc := &si.Resource{
		Resources: map[string]*si.Quantity{
			resources.MEMORY: {Value: 1},
		},
	}
	return &si.Allocation{
		AllocationKey:    allocID,
		ResourcePerAlloc: resAlloc,
		QueueName:        queue,
		NodeID:           nodeID,
		ApplicationID:    appID,
	}
}

func createAllocationProposal(queue, nodeID, allocID, appID string) *commonevents.AllocationProposal {
	resAlloc := resources.NewResourceFromMap(
		map[string]resources.Quantity{
			resources.MEMORY: 1,
		})

	return &commonevents.AllocationProposal{
		NodeID:            nodeID,
		ApplicationID:     appID,
		QueueName:         queue,
		AllocatedResource: resAlloc,
		AllocationKey:     allocID,
	}
}

func waitForPartitionState(t *testing.T, partition *PartitionSchedulingContext, state string, timeoutMs int) {
	for i := 0; i*100 < timeoutMs; i++ {
		if !partition.stateMachine.Is(state) {
			time.Sleep(time.Duration(100 * time.Millisecond))
		} else {
			return
		}
	}
	t.Fatalf("Failed to wait for partition %s state change: %s", partition.Name, partition.stateMachine.Current())
}

func TestLoadPartitionConfig(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: production
      - name: test
        queues:
          - name: admintest
`

	partition, err := createMockPartitionSchedulingContextFromConfig([]byte(data))
	assert.NilError(t, err, "partition create failed")
	// There is a queue setup as the config must be valid when we run
	root := partition.getQueue("root")
	if root == nil {
		t.Errorf("root queue not found in partition")
	}
	adminTest := partition.getQueue("root.test.admintest")
	if adminTest == nil {
		t.Errorf("root.test.adminTest queue not found in partition")
	}
}

func TestLoadDeepQueueConfig(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: root
        queues:
          - name: level1
            queues:
              - name: level2
                queues:
                  - name: level3
                    queues:
                      - name: level4
                        queues:
                          - name: level5
`

	partition, err := createMockPartitionSchedulingContextFromConfig([]byte(data))
	assert.NilError(t, err, "partition create failed")
	// There is a queue setup as the config must be valid when we run
	root := partition.getQueue("root")
	if root == nil {
		t.Errorf("root queue not found in partition")
	}
	adminTest := partition.getQueue("root.level1.level2.level3.level4.level5")
	if adminTest == nil {
		t.Errorf("root.level1.level2.level3.level4.level5 queue not found in partition")
	}
}

func TestAddNewNode(t *testing.T) {
	partition, err := createMockPartitionSchedulingContextFromConfig([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")
	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := newNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))
	// add a node this must work
	err = partition.addNewNode(node1, nil)
	if err != nil || partition.GetNode(nodeID) == nil {
		t.Errorf("add node to partition should not have failed: %v", err)
	}
	// check partition resources
	memRes := partition.totalPartitionResource.Resources[resources.MEMORY]
	if memRes != memVal {
		t.Errorf("add node to partition did not update total resources expected %d got %d", memVal, memRes)
	}

	// add the same node this must fail
	err = partition.addNewNode(node1, nil)
	if err == nil {
		t.Errorf("add same node to partition should have failed, node count is %d", partition.GetTotalNodeCount())
	}

	// mark partition stopped, no new node can be added
	if err = partition.handlePartitionEvent(Stop); err != nil {
		t.Fatalf("partition state change failed: %v", err)
	}
	waitForPartitionState(t, partition, Stopped.String(), 1000)
	nodeID = "node-2"
	node2 := newNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))

	// add a second node to the test
	err = partition.addNewNode(node2, nil)
	if err == nil || partition.GetNode(nodeID) != nil {
		t.Errorf("add new node to stopped partition should have failed")
	}

	// mark partition active again, the new node can be added
	if err = partition.handlePartitionEvent(Start); err != nil {
		t.Fatalf("partition state change failed: %v", err)
	}
	waitForPartitionState(t, partition, Active.String(), 1000)
	err = partition.addNewNode(node2, nil)

	if err != nil || partition.GetNode(nodeID) == nil {
		t.Errorf("add node to partition should not have failed: %v", err)
	}
	// check partition resources
	memRes = partition.totalPartitionResource.Resources[resources.MEMORY]
	if memRes != 2*memVal {
		t.Errorf("add node to partition did not update total resources expected %d got %d", 2*memVal, memRes)
	}
	if partition.GetTotalNodeCount() != 2 {
		t.Errorf("node list was not updated, incorrect number of nodes registered expected 2 got %d", partition.GetTotalNodeCount())
	}

	// mark partition stopped, no new node can be added
	if err = partition.handlePartitionEvent(Remove); err != nil {
		t.Fatalf("partition state change failed: %v", err)
	}
	waitForPartitionState(t, partition, Draining.String(), 1000)
	nodeID = "node-3"
	node3 := newNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))
	// add a third node to the test
	err = partition.addNewNode(node3, nil)
	if err == nil || partition.GetNode(nodeID) != nil {
		t.Errorf("add new node to removed partition should have failed")
	}
}

func TestRemoveNode(t *testing.T) {
	partition, err := createMockPartitionSchedulingContextFromConfig([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")
	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := newNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))
	// add a node this must work
	err = partition.addNewNode(node1, nil)
	if err != nil || partition.GetNode(nodeID) == nil {
		t.Errorf("add node to partition should not have failed: %v", err)
	}
	// check partition resources
	memRes := partition.totalPartitionResource.Resources[resources.MEMORY]
	if memRes != memVal {
		t.Errorf("add node to partition did not update total resources expected %d got %d", memVal, memRes)
	}

	// remove a bogus node should not do anything: returns nil for allocations
	released := partition.RemoveNode("does-not-exist")
	if partition.GetTotalNodeCount() != 1 && released != nil {
		t.Errorf("node list was updated, node was removed expected 1 nodes got %d, released allocations: %v",
			partition.GetTotalNodeCount(), released)
	}

	// remove the node this cannot fail: must return an empty array, not nil
	released = partition.RemoveNode(nodeID)
	if released == nil || len(released) != 0 {
		t.Errorf("node released wrong allocation info, expected nothing got %v", released)
	}
	if partition.GetTotalNodeCount() != 0 {
		t.Errorf("node list was not updated, node was not removed expected 0 got %d", partition.GetTotalNodeCount())
	}
	// check partition resources
	memRes = partition.totalPartitionResource.Resources[resources.MEMORY]
	if memRes != 0 {
		t.Errorf("remove node from partition did not update total resources expected 0 got %d", memRes)
	}
}

func TestRemoveNodeWithAllocations(t *testing.T) {
	partition, err := createMockPartitionSchedulingContextFromConfig([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")
	// add a new app
	appID := "app-1"
	queueName := "root.default"
	appInfo := newSchedulingAppTestOnly(appID, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	if err != nil {
		t.Errorf("add application to partition should not have failed: %v", err)
	}

	// add a node with allocations: must have the correct app added already
	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := newNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))
	alloc := createAllocation(queueName, nodeID, "alloc-1", appID)
	alloc.UUID = "alloc-1-uuid"
	allocs := []*si.Allocation{alloc}
	// add a node this must work
	err = partition.addNewNode(node1, allocs)
	if err != nil || partition.GetNode(nodeID) == nil {
		t.Fatalf("add node to partition should not have failed: %v", err)
	}
	// get what was allocated
	allocated := node1.GetAllAllocations()
	if len(allocated) != 1 {
		t.Fatalf("allocation not added correctly expected 1 got: %v", allocated)
	}
	allocUUID := allocated[0].GetUUID()

	// add broken allocations
	res := resources.NewResourceFromMap(map[string]resources.Quantity{resources.MEMORY: 1})
	node1.allocations["notanapp"] = createMockAllocationInfo("notanapp", res, "noanapp", "root.default", nodeID)
	node1.allocations["notanalloc"] = createMockAllocationInfo(appID, res, "notanalloc", "root.default", nodeID)

	// remove the node this cannot fail
	released := partition.RemoveNode(nodeID)
	if partition.GetTotalNodeCount() != 0 {
		t.Errorf("node list was not updated, node was not removed expected 0 got %d", partition.GetTotalNodeCount())
	}
	if len(released) != 1 {
		t.Errorf("node did not release correct allocation expected 1 got %d", len(released))
	}
	assert.Equal(t, released[0].GetUUID(), allocUUID, "UUID returned by release not the same as on allocation")
}

func TestAddNewApplication(t *testing.T) {
	partition, err := createMockPartitionSchedulingContextFromConfig([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")

	// add a new app
	appInfo := newSchedulingAppTestOnly("app-1", "default", "root.default")
	err = partition.addNewApplication(appInfo, true)
	if err != nil {
		t.Errorf("add application to partition should not have failed: %v", err)
	}
	// add the same app with failIfExist true should fail
	err = partition.addNewApplication(appInfo, true)
	if err == nil {
		t.Errorf("add same application to partition should have failed but did not")
	}
	// add the same app with failIfExist false should not fail
	err = partition.addNewApplication(appInfo, false)
	if err != nil {
		t.Errorf("add same application with failIfExist false should not have failed but did %v", err)
	}

	// mark partition stopped, no new application can be added
	if err = partition.handlePartitionEvent(Stop); err != nil {
		t.Fatalf("partition state change failed: %v", err)
	}
	waitForPartitionState(t, partition, Stopped.String(), 1000)

	appInfo = newSchedulingAppTestOnly("app-2", "default", "root.default")
	err = partition.addNewApplication(appInfo, true)
	if err == nil || partition.getApplication("app-2") != nil {
		t.Errorf("add application on stopped partition should have failed but did not")
	}

	// mark partition for deletion, no new application can be added
	partition.stateMachine.SetState(Active.String())
	if err = partition.handlePartitionEvent(Remove); err != nil {
		t.Fatalf("partition state change failed: %v", err)
	}
	waitForPartitionState(t, partition, Draining.String(), 1000)
	appInfo = newSchedulingAppTestOnly("app-3", "default", "root.default")
	err = partition.addNewApplication(appInfo, true)
	if err == nil || partition.getApplication("app-3") != nil {
		t.Errorf("add application on draining partition should have failed but did not")
	}
}

func TestAddNodeWithAllocations(t *testing.T) {
	partition, err := createMockPartitionSchedulingContextFromConfig([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")

	// add a new app
	appID := "app-1"
	queueName := "root.default"
	appInfo := newSchedulingAppTestOnly(appID, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	if err != nil {
		t.Errorf("add application to partition should not have failed: %v", err)
	}

	// add a node with allocations: must have the correct app added already
	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := newNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))
	alloc := createAllocation(queueName, nodeID, "alloc-1", appID)
	alloc.UUID = "alloc-1-uuid"
	allocs := []*si.Allocation{alloc}
	// add a node this must work
	err = partition.addNewNode(node1, allocs)
	if err != nil || partition.GetNode(nodeID) == nil {
		t.Fatalf("add node to partition should not have failed: %v", err)
	}
	// check partition resources
	memRes := partition.totalPartitionResource.Resources[resources.MEMORY]
	if memRes != memVal {
		t.Errorf("add node to partition did not update total resources expected %d got %d", memVal, memRes)
	}
	// check partition allocation count
	if partition.GetTotalAllocationCount() != 1 {
		t.Errorf("add node to partition did not add allocation expected 1 got %d", partition.GetTotalAllocationCount())
	}

	// check the leaf queue usage
	qi := partition.getQueue(queueName)
	if qi.allocatedResource.Resources[resources.MEMORY] != 1 {
		t.Errorf("add node to partition did not add queue %s allocation expected 1 got %d",
			qi.QueuePath, qi.allocatedResource.Resources[resources.MEMORY])
	}
	// check the root queue usage
	qi = partition.getQueue("root")
	if qi.allocatedResource.Resources[resources.MEMORY] != 1 {
		t.Errorf("add node to partition did not add queue %s allocation expected 1 got %d",
			qi.QueuePath, qi.allocatedResource.Resources[resources.MEMORY])
	}

	nodeID = "node-partial-fail"
	node2 := newNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))
	allocs = []*si.Allocation{createAllocation(queueName, nodeID, "alloc-2", "app-2")}
	// add a node this partially fails: node is added, allocation is not added and thus we have an error
	err = partition.addNewNode(node2, allocs)
	if err == nil {
		t.Errorf("add node to partition should have returned an error for allocations")
	}
	if partition.GetNode(nodeID) != nil {
		t.Errorf("node should not have been added to partition and was")
	}
}

func TestAddNewAllocation(t *testing.T) {
	partition, err := createMockPartitionSchedulingContextFromConfig([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")

	// add a new app
	appID := "app-1"
	queueName := "root.default"
	appInfo := newSchedulingAppTestOnly(appID, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	if err != nil {
		t.Errorf("add application to partition should not have failed: %v", err)
	}

	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := newNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))
	// add a node this must work
	err = partition.addNewNode(node1, nil)
	if err != nil || partition.GetNode(nodeID) == nil {
		t.Fatalf("add node to partition should not have failed: %v", err)
	}

	resAlloc := resources.NewResourceFromMap(
		map[string]resources.Quantity{
			resources.MEMORY: 1,
		})
	var alloc *schedulingAllocation
	alloc, err = partition.addNewAllocation(createMockAllocationInfo(appID, resAlloc, "", queueName, nodeID))

	if err != nil {
		t.Errorf("adding allocation failed and should not have failed: %v", err)
	}
	if partition.allocations[alloc.GetUUID()] == nil {
		t.Errorf("add allocation to partition not found in the allocation list")
	}
	// check the leaf queue usage
	qi := partition.getQueue(queueName)
	if qi.allocatedResource.Resources[resources.MEMORY] != 1 {
		t.Errorf("add allocation to partition did not add queue %s allocation expected 1 got %d",
			qi.QueuePath, qi.allocatedResource.Resources[resources.MEMORY])
	}

	alloc, err = partition.addNewAllocation(createMockAllocationInfo(appID, resAlloc, "", queueName, nodeID))
	if err != nil || partition.allocations[alloc.GetUUID()] == nil {
		t.Errorf("adding allocation failed and should not have failed: %v", err)
	}
	// check the root queue usage
	qi = partition.getQueue(queueName)
	if qi.allocatedResource.Resources[resources.MEMORY] != 2 {
		t.Errorf("add allocation to partition did not add queue %s allocation expected 2 got %d",
			qi.QueuePath, qi.allocatedResource.Resources[resources.MEMORY])
	}

	alloc, err = partition.addNewAllocation(createMockAllocationInfo("does-not-exist-app", resAlloc, "", queueName, nodeID))
	if err == nil || alloc != nil || len(partition.allocations) != 2 {
		t.Errorf("adding allocation worked and should have failed: %v", alloc)
	}

	// mark the node as not schedulable: allocation must fail
	node1.SetSchedulable(false)
	alloc, err = partition.addNewAllocation(createMockAllocationInfo(appID, resAlloc, "", queueName, nodeID))
	if err == nil || alloc != nil {
		t.Errorf("adding allocation worked and should have failed: %v", alloc)
	}
	// reset the state so it cannot affect further tests
	node1.SetSchedulable(true)

	// mark partition stopped, no new application can be added
	if err = partition.handlePartitionEvent(Stop); err != nil {
		t.Fatalf("partition state change failed: %v", err)
	}
	waitForPartitionState(t, partition, Stopped.String(), 1000)
	alloc, err = partition.addNewAllocation(createMockAllocationInfo(appID, resAlloc, "", queueName, nodeID))
	if err == nil || alloc != nil || len(partition.allocations) != 2 {
		t.Errorf("adding allocation worked and should have failed: %v", alloc)
	}
	// mark partition for removal, no new application can be added
	partition.stateMachine.SetState(Active.String())
	if err = partition.handlePartitionEvent(Remove); err != nil {
		t.Fatalf("partition state change failed: %v", err)
	}
	waitForPartitionState(t, partition, Draining.String(), 1000)
	alloc, err = partition.addNewAllocation(createMockAllocationInfo(appID, resAlloc, "", queueName, nodeID))
	if err != nil || alloc == nil || len(partition.allocations) != 3 {
		t.Errorf("adding allocation did not work and should have (allocation length %d: %v", len(partition.allocations), err)
	}
}

// this test is to verify the node recovery of the existing allocations
// after it adds a node to partition with existing allocations,
// it verifies the allocations are added to the node, resources are counted correct
func TestAddNodeWithExistingAllocations(t *testing.T) {
	const appID = "app-1"
	const queueName = "root.default"
	const nodeID = "node-1"
	const nodeID2 = "node-2"
	const allocationUUID1 = "alloc-UUID-1"
	const allocationUUID2 = "alloc-UUID-2"

	partition, err := createMockPartitionSchedulingContextFromConfig([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")

	// add a new app
	appInfo := newSchedulingAppTestOnly(appID, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	if err != nil {
		t.Errorf("add application to partition should not have failed: %v", err)
	}

	// add a node with 2 existing allocations
	alloc1Res := &si.Resource{Resources: map[string]*si.Quantity{resources.MEMORY: {Value: 200}}}
	alloc2Res := &si.Resource{Resources: map[string]*si.Quantity{resources.MEMORY: {Value: 300}}}
	nodeTotal := &si.Resource{Resources: map[string]*si.Quantity{resources.MEMORY: {Value: 1000}}}
	node1 := newNodeForTest(nodeID, resources.NewResourceFromProto(nodeTotal))
	err = partition.addNewNode(node1, []*si.Allocation{
		{
			AllocationKey:    allocationUUID1,
			AllocationTags:   map[string]string{"a": "b"},
			UUID:             allocationUUID1,
			ResourcePerAlloc: alloc1Res,
			Priority:         nil,
			QueueName:        queueName,
			NodeID:           nodeID,
			ApplicationID:    appID,
			PartitionName:    "default",
		},
		{
			AllocationKey:    allocationUUID2,
			AllocationTags:   map[string]string{"a": "b"},
			UUID:             allocationUUID2,
			ResourcePerAlloc: alloc2Res,
			Priority:         nil,
			QueueName:        queueName,
			NodeID:           nodeID,
			ApplicationID:    appID,
			PartitionName:    "default",
		},
	})
	assert.NilError(t, err, "add node failed")

	// verify existing allocations are added to the node
	assert.Equal(t, len(node1.GetAllAllocations()), 2)

	alloc1 := node1.GetAllocation(allocationUUID1)
	alloc2 := node1.GetAllocation(allocationUUID2)
	assert.Assert(t, alloc1 != nil)
	assert.Assert(t, alloc2 != nil)
	assert.Equal(t, alloc1.GetUUID(), "alloc-UUID-1")
	assert.Equal(t, alloc2.GetUUID(), "alloc-UUID-2")
	assert.Assert(t, resources.Equals(alloc1.SchedulingAsk.AllocatedResource, resources.NewResourceFromProto(alloc1Res)))
	assert.Assert(t, resources.Equals(alloc2.SchedulingAsk.AllocatedResource, resources.NewResourceFromProto(alloc2Res)))
	assert.Equal(t, alloc1.SchedulingAsk.ApplicationID, "app-1")
	assert.Equal(t, alloc2.SchedulingAsk.ApplicationID, "app-1")

	// verify node resource
	assert.Assert(t, resources.Equals(node1.GetCapacity(), resources.NewResourceFromProto(nodeTotal)))
	assert.Assert(t, resources.Equals(node1.GetAllocatedResource(),
		resources.Add(resources.NewResourceFromProto(alloc1Res), resources.NewResourceFromProto(alloc2Res))))

	// add a node with 1 existing allocation that does not have a UUID, must fail
	node2 := newNodeForTest(nodeID2, resources.NewResourceFromProto(nodeTotal))
	err = partition.addNewNode(node2, []*si.Allocation{
		{
			AllocationKey:    "fails-to-restore",
			AllocationTags:   map[string]string{"a": "b"},
			UUID:             "",
			ResourcePerAlloc: alloc1Res,
			Priority:         nil,
			QueueName:        queueName,
			NodeID:           nodeID2,
			ApplicationID:    appID,
			PartitionName:    "default",
		},
	})
	if err == nil {
		t.Fatal("Adding a node with existing allocation without UUID should have failed")
	}
}

func TestRemoveApp(t *testing.T) {
	partition, err := createMockPartitionSchedulingContextFromConfig([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")
	// add a new app that will just sit around to make sure we remove the right one
	appNotRemoved := "will_not_remove"
	queueName := "root.default"
	appInfo := newSchedulingAppTestOnly(appNotRemoved, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	assert.NilError(t, err, "add application to partition should not have failed")
	// add a node to allow adding an allocation
	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := newNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))
	// add a node this must work
	err = partition.addNewNode(node1, nil)
	if err != nil || partition.GetNode(nodeID) == nil {
		t.Fatalf("add node to partition should not have failed: %v", err)
	}

	resAlloc := resources.NewResourceFromMap(
		map[string]resources.Quantity{
			resources.MEMORY: 1,
		})
	var alloc *schedulingAllocation
	alloc, err = partition.addNewAllocation(createMockAllocationInfo(appNotRemoved, resAlloc, "", queueName, nodeID))

	assert.NilError(t, err, "add allocation to partition should not have failed")
	uuid := alloc.GetUUID()

	app, allocs := partition.removeApplication("does_not_exist")
	if app != nil && len(allocs) != 0 {
		t.Errorf("non existing application returned unexpected values: application info %v (allocs = %v)", app, allocs)
	}

	// add another new app
	appID := "app-1"
	appInfo = newSchedulingAppTestOnly(appID, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	if err != nil {
		t.Errorf("add application to partition should not have failed: %v", err)
	}

	// remove the newly added app (no allocations)
	app, allocs = partition.removeApplication(appID)
	if app == nil && len(allocs) != 0 {
		t.Errorf("existing application without allocations returned allocations %v", allocs)
	}
	if len(partition.applications) != 1 {
		t.Fatalf("existing application was not removed")
	}

	// add the application again and then an allocation
	err = partition.addNewApplication(appInfo, true)
	if err != nil {
		t.Errorf("add application to partition should not have failed: %v", err)
	}
	alloc, err = partition.addNewAllocation(createMockAllocationInfo(appID, resAlloc, "", queueName, nodeID))
	if err != nil || alloc == nil {
		t.Fatalf("add allocation to partition should not have failed: %v", err)
	}

	// remove the newly added app
	app, allocs = partition.removeApplication(appID)
	if app == nil && len(allocs) != 1 {
		t.Errorf("existing application with allocations returned unexpected allocations %v", allocs)
	}
	if len(partition.applications) != 1 {
		t.Errorf("existing application was not removed")
	}
	if partition.allocations[uuid] == nil {
		t.Errorf("allocation that should have been left was removed")
	}
}

func TestRemoveAppAllocs(t *testing.T) {
	partition, err := createMockPartitionSchedulingContextFromConfig([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")
	// add a new app that will just sit around to make sure we remove the right one
	appNotRemoved := "will_not_remove"
	queueName := "root.default"
	appInfo := newSchedulingAppTestOnly(appNotRemoved, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	assert.NilError(t, err, "add application to partition should not have failed")
	// add a node to allow adding an allocation
	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := newNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))
	// add a node this must work
	err = partition.addNewNode(node1, nil)
	if err != nil || partition.GetNode(nodeID) == nil {
		t.Fatalf("add node to partition should not have failed: %v", err)
	}

	resAlloc := resources.NewResourceFromMap(
		map[string]resources.Quantity{
			resources.MEMORY: 1,
		})
	var alloc *schedulingAllocation
	alloc, err = partition.addNewAllocation(createMockAllocationInfo(appNotRemoved, resAlloc, "", queueName, nodeID))

	if err != nil || alloc == nil {
		t.Fatalf("add allocation to partition should not have failed: %v", err)
	}
	alloc, err = partition.addNewAllocation(createMockAllocationInfo(appNotRemoved, resAlloc, "", queueName, nodeID))
	assert.NilError(t, err, "add allocation to partition should not have failed")
	uuid := alloc.GetUUID()

	allocs := partition.releaseAllocationsForApplication(nil)
	if len(allocs) != 0 {
		t.Errorf("empty removal request returned allocations: %v", allocs)
	}
	// create a new release without app: should just return
	toRelease := commonevents.NewReleaseAllocation("", "", partition.Name, "", si.AllocationReleaseResponse_TerminationType(0))
	allocs = partition.releaseAllocationsForApplication(toRelease)
	if len(allocs) != 0 {
		t.Errorf("removal request for non existing application returned allocations: %v", allocs)
	}
	// create a new release with app, non existing allocation: should just return
	toRelease = commonevents.NewReleaseAllocation("does_not exist", appNotRemoved, partition.Name, "", si.AllocationReleaseResponse_TerminationType(0))
	allocs = partition.releaseAllocationsForApplication(toRelease)
	if len(allocs) != 0 {
		t.Errorf("removal request for non existing allocation returned allocations: %v", allocs)
	}
	// create a new release with app, existing allocation: should return 1 alloc
	toRelease = commonevents.NewReleaseAllocation(uuid, appNotRemoved, partition.Name, "", si.AllocationReleaseResponse_TerminationType(0))
	allocs = partition.releaseAllocationsForApplication(toRelease)
	if len(allocs) != 1 {
		t.Errorf("removal request for existing allocation returned wrong allocations: %v", allocs)
	}
	// create a new release with app, no uuid: should return last left alloc
	toRelease = commonevents.NewReleaseAllocation("", appNotRemoved, partition.Name, "", si.AllocationReleaseResponse_TerminationType(0))
	allocs = partition.releaseAllocationsForApplication(toRelease)
	if len(allocs) != 1 {
		t.Errorf("removal request for existing allocation returned wrong allocations: %v", allocs)
	}
	if len(partition.allocations) != 0 {
		t.Errorf("removal requests did not remove all allocations: %v", partition.allocations)
	}
}

func TestCreateQueues(t *testing.T) {
	partition, err := createMockPartitionSchedulingContextFromConfig([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")
	// top level should fail
	err = partition.CreateQueues("test")
	if err == nil {
		t.Errorf("top level queue creation did not fail")
	}

	// create below leaf
	err = partition.CreateQueues("root.default.test")
	if err == nil {
		t.Errorf("'root.default.test' queue creation did not fail")
	}

	// single level create
	err = partition.CreateQueues("root.test")
	if err != nil {
		t.Errorf("'root.test' queue creation failed")
	}
	queue := partition.getQueue("root.test")
	if queue == nil {
		t.Errorf("'root.test' queue creation failed without error")
	}
	if queue != nil && !queue.isLeaf && !queue.IsManaged() {
		t.Errorf("'root.test' queue creation failed not created with correct settings: %v", queue)
	}

	// multiple level create
	err = partition.CreateQueues("root.parent.test")
	if err != nil {
		t.Errorf("'root.parent.test' queue creation failed")
	}
	queue = partition.getQueue("root.parent.test")
	if queue == nil {
		t.Fatalf("'root.parent.test' queue creation failed without error")
	}
	if !queue.isLeaf && !queue.IsManaged() {
		t.Errorf("'root.parent.test' queue not created with correct settings: %v", queue)
	}
	queue = queue.parent
	if queue == nil {
		t.Errorf("'root.parent' queue creation failed: parent is not set correctly")
	}
	if queue != nil && queue.isLeaf && !queue.IsManaged() {
		t.Errorf("'root.parent' parent queue not created with correct settings: %v", queue)
	}

	// deep level create
	err = partition.CreateQueues("root.parent.next.level.test.leaf")
	if err != nil {
		t.Errorf("'root.parent.next.level.test.leaf' queue creation failed")
	}
	queue = partition.getQueue("root.parent.next.level.test.leaf")
	if queue == nil {
		t.Errorf("'root.parent.next.level.test.leaf' queue creation failed without error")
	}
	if queue != nil && !queue.isLeaf && !queue.IsManaged() {
		t.Errorf("'root.parent.next.level.test.leaf' queue not created with correct settings: %v", queue)
	}
}

func TestCalculateNodesUsage(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: production
      - name: test
        queues:
          - name: admintest
            resources:
              guaranteed:
                memory: 200
                vcore: 200
              max:
                memory: 200
                vcore: 200
`

	partition, err := createMockPartitionSchedulingContextFromConfig([]byte(data))
	assert.NilError(t, err, "partition create failed")

	n1 := newNodeForTest("host1", resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 100, "vcore": 100}))
	n2 := newNodeForTest("host2", resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 100, "vcore": 100}))
	if err = partition.addNewNode(n1, nil); err != nil {
		t.Error(err)
	}

	if err = partition.addNewNode(n2, nil); err != nil {
		t.Error(err)
	}

	m := partition.CalculateNodesResourceUsage()
	assert.Equal(t, len(m), 2)
	assert.Assert(t, reflect.DeepEqual(m["memory"], []int{2, 0, 0, 0, 0, 0, 0, 0, 0, 0}))

	n1.allocatedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 100, "vcore": 100})
	m = partition.CalculateNodesResourceUsage()
	assert.Assert(t, reflect.DeepEqual(m["memory"], []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 1}))

	n1.allocatedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 50, "vcore": 50})
	m = partition.CalculateNodesResourceUsage()
	assert.Assert(t, reflect.DeepEqual(m["memory"], []int{1, 0, 0, 0, 1, 0, 0, 0, 0, 0}))

	n1.allocatedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 12, "vcore": 12})
	n2.allocatedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 88, "vcore": 88})
	m = partition.CalculateNodesResourceUsage()
	assert.Assert(t, reflect.DeepEqual(m["memory"], []int{0, 1, 0, 0, 0, 0, 0, 0, 1, 0}))

	n3 := newNodeForTest("host3", resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 100, "vcore": 100}))
	if err = partition.addNewNode(n3, nil); err != nil {
		t.Error(err)
	}

	m = partition.CalculateNodesResourceUsage()
	assert.Equal(t, len(m), 2)
	assert.Assert(t, reflect.DeepEqual(m["memory"], []int{1, 1, 0, 0, 0, 0, 0, 0, 1, 0}))
}
