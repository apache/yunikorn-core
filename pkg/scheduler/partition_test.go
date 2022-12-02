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
	"time"

	"gotest.tools/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func setupUGM() {
	userManager := ugm.GetUserManager()
	userManager.ClearUserTrackers()
	userManager.ClearGroupTrackers()
}

func setupNode(t *testing.T, nodeID string, partition *PartitionContext, nodeRes *resources.Resource) *objects.Node {
	err := partition.AddNode(newNodeMaxResource(nodeID, nodeRes), nil)
	assert.NilError(t, err, "test "+nodeID+" add failed unexpected")
	node := partition.GetNode(nodeID)
	if node == nil {
		t.Fatal("new node was not found on the partition")
	}
	return node
}

func TestNewPartition(t *testing.T) {
	partition, err := newPartitionContext(configs.PartitionConfig{}, "", nil)
	if err == nil || partition != nil {
		t.Fatal("nil inputs should not have returned partition")
	}
	conf := configs.PartitionConfig{Name: "test"}
	partition, err = newPartitionContext(conf, "", nil)
	if err == nil || partition != nil {
		t.Fatal("named partition without RM should not have returned partition")
	}
	partition, err = newPartitionContext(conf, "test", &ClusterContext{})
	if err == nil || partition != nil {
		t.Fatal("partition without root queue should not have returned partition")
	}

	conf = configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:   "test",
				Parent: true,
			},
		},
	}
	partition, err = newPartitionContext(conf, "test", &ClusterContext{})
	if err == nil || partition != nil {
		t.Fatal("partition without root queue should not have returned partition")
	}

	conf = configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues:    nil,
			},
		},
	}
	partition, err = newPartitionContext(conf, "test", &ClusterContext{})
	assert.NilError(t, err, "partition create should not have failed with error")
	if partition.root.QueuePath != "root" {
		t.Fatal("partition root queue not set as expected")
	}
	assert.Assert(t, !partition.placementManager.IsInitialised(), "partition should not have initialised placement manager")
}

func TestNewWithPlacement(t *testing.T) {
	confWith := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues:    nil,
			},
		},
		PlacementRules: []configs.PlacementRule{
			{
				Name:   "provided",
				Create: true,
			},
		},
		Limits:         nil,
		Preemption:     configs.PartitionPreemptionConfig{},
		NodeSortPolicy: configs.NodeSortingPolicy{},
	}
	partition, err := newPartitionContext(confWith, rmID, nil)
	assert.NilError(t, err, "test partition create failed with error")
	assert.Assert(t, partition.placementManager.IsInitialised(), "partition should have initialised placement manager")
	assert.Equal(t, len(*partition.rules), 1, "Placement rules not set as expected ")

	// add a rule and check if it is updated
	confWith = configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues:    nil,
			},
		},
		PlacementRules: []configs.PlacementRule{
			{
				Name:   "provided",
				Create: false,
			},
			{
				Name:   "user",
				Create: true,
			},
		},
	}
	err = partition.updatePartitionDetails(confWith)
	assert.NilError(t, err, "update partition failed unexpected with error")
	assert.Assert(t, partition.placementManager.IsInitialised(), "partition should have initialised placement manager")
	assert.Equal(t, len(*partition.rules), 2, "Placement rules not updated as expected ")

	// update to turn off placement manager
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues:    nil,
			},
		},
	}
	err = partition.updatePartitionDetails(conf)
	assert.NilError(t, err, "update partition failed unexpected with error")
	assert.Assert(t, !partition.placementManager.IsInitialised(), "partition should not have initialised placement manager")
	assert.Equal(t, len(*partition.rules), 0, "Placement rules not updated as expected ")

	// set the old config back this should turn on the placement again
	err = partition.updatePartitionDetails(confWith)
	assert.NilError(t, err, "update partition failed unexpected with error")
	assert.Assert(t, partition.placementManager.IsInitialised(), "partition should have initialised placement manager")
	assert.Equal(t, len(*partition.rules), 2, "Placement rules not updated as expected ")
}

func TestAddNode(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "test partition create failed with error")
	err = partition.AddNode(nil, nil)
	if err == nil {
		t.Fatal("nil node add did not return error")
	}
	assert.Equal(t, 0, partition.nodes.GetNodeCount(), "nil node should not be added")
	node := newNodeMaxResource("test1", resources.NewResource())
	// stop the partition node should be rejected
	partition.markPartitionForRemoval()
	assert.Assert(t, partition.isDraining(), "partition should have been marked as draining")
	err = partition.AddNode(node, nil)
	if err == nil {
		t.Error("test node add to draining partition should have failed")
	}
	assert.Equal(t, partition.nodes.GetNodeCount(), 0, "node list not correct")

	// reset the state (hard no checks)
	partition.stateMachine.SetState(objects.Active.String())
	err = partition.AddNode(node, nil)
	assert.NilError(t, err, "test node add failed unexpected")
	assert.Equal(t, partition.nodes.GetNodeCount(), 1, "node list not correct")
	// add the same node nothing changes
	err = partition.AddNode(node, nil)
	if err == nil {
		t.Fatal("add same test node worked unexpected")
	}
	assert.Equal(t, partition.nodes.GetNodeCount(), 1, "node list not correct")
	err = partition.AddNode(newNodeMaxResource("test2", resources.NewResource()), nil)
	assert.NilError(t, err, "test node2 add failed unexpected")
	assert.Equal(t, partition.nodes.GetNodeCount(), 2, "node list not correct")
}

func TestAddNodeWithAllocations(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	q := partition.GetQueue(defQueue)
	if q == nil {
		t.Fatal("expected default queue not found")
	}

	// add a new app
	app := newApplication(appID1, "default", defQueue)
	appRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000})
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	// add a node with allocations
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 10000})
	node := newNodeMaxResource(nodeID1, nodeRes)

	// fail with an unknown app
	ask := newAllocationAsk("alloc-1", "unknown", appRes)
	alloc := objects.NewAllocation("alloc-1-uuid", nodeID1, ask)
	allocs := []*objects.Allocation{alloc}
	err = partition.AddNode(node, allocs)
	if err == nil {
		t.Errorf("add node to partition should have failed (app missing)")
	}
	assert.Equal(t, partition.nodes.GetNodeCount(), 0, "error returned but node still added to the partition (app)")

	// fail with a broken alloc
	ask = newAllocationAsk("alloc-1", appID1, appRes)
	alloc = objects.NewAllocation("", nodeID1, ask)
	allocs = []*objects.Allocation{alloc}
	err = partition.AddNode(node, allocs)
	if err == nil {
		t.Errorf("add node to partition should have failed (uuid missing)")
	}
	assert.Equal(t, partition.nodes.GetNodeCount(), 0, "error returned but node still added to the partition (uuid)")
	assertUserGroupResource(t, getTestUserGroup(), nil)

	// fix the alloc add the node will work now
	alloc = objects.NewAllocation("alloc-1-uuid", nodeID1, ask)
	allocs = []*objects.Allocation{alloc}
	// add a node this must work
	err = partition.AddNode(node, allocs)
	// check the partition
	assert.NilError(t, err, "add node to partition should not have failed")
	assert.Equal(t, partition.nodes.GetNodeCount(), 1, "no error returned but node not added to the partition")
	assert.Assert(t, resources.Equals(nodeRes, partition.GetTotalPartitionResource()), "add node to partition did not update total resources expected %v got %d", nodeRes, partition.GetTotalPartitionResource())
	assert.Equal(t, partition.GetTotalAllocationCount(), 1, "add node to partition did not add allocation")

	// check the queue usage
	assert.Assert(t, resources.Equals(q.GetAllocatedResource(), appRes), "add node to partition did not update queue as expected")
	assertUserGroupResource(t, getTestUserGroup(), appRes)
}

func TestRemoveNode(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "test partition create failed with error")
	err = partition.AddNode(newNodeMaxResource("test", resources.NewResource()), nil)
	assert.NilError(t, err, "test node add failed unexpected")
	assert.Equal(t, 1, partition.nodes.GetNodeCount(), "node list not correct")

	// remove non existing node
	_, _ = partition.removeNode("")
	assert.Equal(t, 1, partition.nodes.GetNodeCount(), "nil node should not remove anything")
	_, _ = partition.removeNode("does not exist")
	assert.Equal(t, 1, partition.nodes.GetNodeCount(), "non existing node was removed")

	_, _ = partition.removeNode("test")
	assert.Equal(t, 0, partition.nodes.GetNodeCount(), "node was not removed")
}

func TestRemoveNodeWithAllocations(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app
	app := newApplication(appID1, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	// add a node with allocations: must have the correct app added already
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000000})
	node := newNodeMaxResource(nodeID1, nodeRes)
	appRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000})
	ask := newAllocationAsk("alloc-1", appID1, appRes)
	allocUUID := "alloc-1-uuid"
	alloc := objects.NewAllocation(allocUUID, nodeID1, ask)
	allocs := []*objects.Allocation{alloc}
	err = partition.AddNode(node, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")
	// get what was allocated
	allocated := node.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly")
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	// add broken allocations
	ask = newAllocationAsk("alloc-na", "not-an-app", appRes)
	alloc = objects.NewAllocation("alloc-na-uuid", nodeID1, ask)
	node.AddAllocation(alloc)
	ask = newAllocationAsk("alloc-2", appID1, appRes)
	alloc = objects.NewAllocation("alloc-2-uuid", nodeID1, ask)
	node.AddAllocation(alloc)
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	// remove the node this cannot fail
	released, confirmed := partition.removeNode(nodeID1)
	assert.Equal(t, 0, partition.GetTotalNodeCount(), "node list was not updated, node was not removed")
	assert.Equal(t, 1, len(released), "node did not release correct allocation")
	assert.Equal(t, 0, len(confirmed), "node did not confirm correct allocation")
	assert.Equal(t, released[0].GetUUID(), allocUUID, "uuid returned by release not the same as on allocation")
	assertUserGroupResource(t, getTestUserGroup(), nil)
}

// test with a replacement of a placeholder: placeholder and real on the same node that gets removed
func TestRemoveNodeWithPlaceholders(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app
	app := newApplication(appID1, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	// add a node with allocation: must have the correct app added already
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	node1 := newNodeMaxResource(nodeID1, nodeRes)
	appRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	ask := newAllocationAskTG("placeholder", appID1, taskGroup, appRes, true)
	ph := objects.NewAllocation(phID, nodeID1, ask)
	allocs := []*objects.Allocation{ph}
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node1 to partition should not have failed")
	// get what was allocated
	allocated := node1.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node1 expected 1 got: %v", allocated)
	assert.Assert(t, resources.Equals(node1.GetAllocatedResource(), appRes), "allocation not added correctly to node1")
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	// fake an ask that is used
	ask = newAllocationAskAll(allocID, appID1, taskGroup, appRes, 1, 1, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should be added to app")
	_, err = app.UpdateAskRepeat(allocID, -1)
	assert.NilError(t, err, "ask should have been updated without error")
	assert.Assert(t, resources.IsZero(app.GetPendingResource()), "app should not have pending resources")
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	// add real allocation that is replacing the placeholder
	alloc := objects.NewAllocation(allocID, nodeID1, ask)
	alloc.SetRelease(ph)
	// double link as if the replacement is ongoing
	ph.SetRelease(alloc)
	alloc.SetResult(objects.Replaced)

	allocs = app.GetAllAllocations()
	assert.Equal(t, len(allocs), 1, "expected one allocation for the app (placeholder)")
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	// remove the node that has both placeholder and real allocation
	released, confirmed := partition.removeNode(nodeID1)
	assert.Equal(t, 0, partition.GetTotalNodeCount(), "node list was not updated, node was not removed")
	assert.Equal(t, 1, len(released), "node removal did not release correct allocation")
	assert.Equal(t, 0, len(confirmed), "node removal should not have confirmed allocation")
	assert.Equal(t, phID, released[0].GetUUID(), "uuid returned by release not the same as the placeholder")
	allocs = app.GetAllAllocations()
	assert.Equal(t, 0, len(allocs), "expected no allocations for the app")
	assert.Assert(t, resources.Equals(app.GetPendingResource(), appRes), "app should have updated pending resources")
	// check the interim state of the placeholder involved
	assert.Equal(t, 0, ph.GetReleaseCount(), "placeholder should have no releases linked anymore")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0}))
}

func TestCalculateNodesResourceUsage(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	oldCapacity := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100})
	node := newNodeMaxResource(nodeID1, oldCapacity)
	err = partition.AddNode(node, nil)
	assert.NilError(t, err)

	occupiedResources := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})
	alloc := objects.NewAllocation(allocID, nodeID1, newAllocationAsk("key", "appID", occupiedResources))
	node.AddAllocation(alloc)
	usageMap := partition.calculateNodesResourceUsage()
	assert.Equal(t, node.GetAvailableResource().Resources["first"], resources.Quantity(50))
	assert.Equal(t, usageMap["first"][4], 1)

	occupiedResources = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})
	alloc = objects.NewAllocation(allocID, nodeID1, newAllocationAsk("key", "appID", occupiedResources))
	node.AddAllocation(alloc)
	usageMap = partition.calculateNodesResourceUsage()
	assert.Equal(t, node.GetAvailableResource().Resources["first"], resources.Quantity(0))
	assert.Equal(t, usageMap["first"][9], 1)

	newCapacity := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 80})
	node.SetCapacity(newCapacity)
	usageMap = partition.calculateNodesResourceUsage()
	assert.Assert(t, node.GetAvailableResource().HasNegativeValue())
	assert.Equal(t, usageMap["first"][9], 1)
}

// test with a replacement of a placeholder: placeholder on the removed node, real on the 2nd node
func TestRemoveNodeWithReplacement(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app
	app := newApplication(appID1, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	// add a node with allocation: must have the correct app added already
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	node1 := newNodeMaxResource(nodeID1, nodeRes)
	appRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	ask := newAllocationAskAll("placeholder", appID1, taskGroup, appRes, 0, 1, true)
	ph := objects.NewAllocation(phID, nodeID1, ask)
	allocs := []*objects.Allocation{ph}
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node1 to partition should not have failed")
	// get what was allocated
	allocated := node1.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node1")
	assertUserGroupResource(t, getTestUserGroup(), appRes)
	assert.Assert(t, resources.Equals(node1.GetAllocatedResource(), appRes), "allocation not added correctly to node1")

	node2 := setupNode(t, nodeID2, partition, nodeRes)
	assert.Equal(t, 2, partition.GetTotalNodeCount(), "node list was not updated as expected")

	// fake an ask that is used
	ask = newAllocationAskAll(allocID, appID1, taskGroup, appRes, 1, 1, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should be added to app")
	_, err = app.UpdateAskRepeat(allocID, -1)
	assert.NilError(t, err, "ask should have been updated without error")
	assert.Assert(t, resources.IsZero(app.GetPendingResource()), "app should not have pending resources")

	// add real allocation that is replacing the placeholder on the 2nd node
	alloc := objects.NewAllocation(allocID, nodeID2, ask)
	alloc.SetRelease(ph)
	alloc.SetResult(objects.Replaced)
	node2.AddAllocation(alloc)
	allocated = node2.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node2")
	assert.Assert(t, resources.Equals(node2.GetAllocatedResource(), appRes), "allocation not added correctly to node2 (resource count)")
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	// double link as if the replacement is ongoing
	ph.SetRelease(alloc)

	allocs = app.GetAllAllocations()
	assert.Equal(t, len(allocs), 1, "expected one allocation for the app (placeholder)")
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	// remove the node with the placeholder
	released, confirmed := partition.removeNode(nodeID1)
	assert.Equal(t, 1, partition.GetTotalNodeCount(), "node list was not updated, node was not removed")
	assert.Equal(t, 1, len(node2.GetAllAllocations()), "remaining node should have allocation")
	assert.Equal(t, 1, len(released), "node removal did not release correct allocation")
	assert.Equal(t, 1, len(confirmed), "node removal did not confirm correct allocation")
	assert.Equal(t, phID, released[0].GetUUID(), "uuid returned by release not the same as the placeholder")
	assert.Equal(t, allocID, confirmed[0].GetUUID(), "uuid returned by confirmed not the same as the real allocation")
	assert.Assert(t, resources.IsZero(app.GetPendingResource()), "app should not have pending resources")
	allocs = app.GetAllAllocations()
	assert.Equal(t, 1, len(allocs), "expected one allocation for the app (real)")
	assert.Equal(t, allocID, allocs[0].GetUUID(), "uuid for the app is not the same as the real allocation")
	assert.Equal(t, objects.Allocated, allocs[0].GetResult(), "allocation state should be allocated")
	assert.Equal(t, 0, allocs[0].GetReleaseCount(), "real allocation should have no releases linked anymore")
	assertUserGroupResource(t, getTestUserGroup(), appRes)
}

// test with a replacement of a placeholder: real on the removed node placeholder on the 2nd node
func TestRemoveNodeWithReal(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app
	app := newApplication(appID1, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	// add a node with allocation: must have the correct app added already
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	node1 := newNodeMaxResource(nodeID1, nodeRes)
	appRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	ask := newAllocationAskAll("placeholder", appID1, taskGroup, appRes, 0, 1, true)
	ph := objects.NewAllocation(phID, nodeID1, ask)
	allocs := []*objects.Allocation{ph}
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node1 to partition should not have failed")
	// get what was allocated
	allocated := node1.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node1")
	assert.Assert(t, resources.Equals(node1.GetAllocatedResource(), appRes), "allocation not added correctly to node1")
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	node2 := setupNode(t, nodeID2, partition, nodeRes)
	assert.Equal(t, 2, partition.GetTotalNodeCount(), "node list was not updated as expected")

	// fake an ask that is used
	ask = newAllocationAskAll(allocID, appID1, taskGroup, appRes, 1, 1, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should be added to app")
	_, err = app.UpdateAskRepeat(allocID, -1)
	assert.NilError(t, err, "ask should have been updated without error")
	assert.Assert(t, resources.IsZero(app.GetPendingResource()), "app should not have pending resources")

	// add real allocation that is replacing the placeholder on the 2nd node
	alloc := objects.NewAllocation(allocID, nodeID2, ask)
	alloc.SetRelease(ph)
	alloc.SetResult(objects.Replaced)
	node2.AddAllocation(alloc)
	allocated = node2.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node2")
	assert.Assert(t, resources.Equals(node2.GetAllocatedResource(), appRes), "allocation not added correctly to node2 (resource count)")
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	// double link as if the replacement is ongoing
	ph.SetRelease(alloc)

	allocs = app.GetAllAllocations()
	assert.Equal(t, len(allocs), 1, "expected one allocation for the app (placeholder)")

	// remove the node with the real allocation
	released, confirmed := partition.removeNode(nodeID2)
	assert.Equal(t, 1, partition.GetTotalNodeCount(), "node list was not updated, node was not removed")
	assert.Equal(t, 0, len(released), "node removal did not release correct allocation")
	assert.Equal(t, 0, len(confirmed), "node removal did not confirm correct allocation")
	assert.Assert(t, resources.Equals(app.GetPendingResource(), appRes), "app should have updated pending resources")
	allocs = app.GetAllAllocations()
	assert.Equal(t, 1, len(allocs), "expected one allocation for the app (placeholder")
	assert.Equal(t, phID, allocs[0].GetUUID(), "uuid for the app is not the same as the real allocation")
	assert.Equal(t, 0, ph.GetReleaseCount(), "no inflight replacements linked")
	assertUserGroupResource(t, getTestUserGroup(), appRes)
}

func TestAddApp(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app
	app := newApplication(appID1, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")
	// add the same app
	err = partition.AddApplication(app)
	if err == nil {
		t.Errorf("add same application to partition should have failed but did not")
	}

	// mark partition stopped, no new application can be added
	err = partition.handlePartitionEvent(objects.Stop)
	assert.NilError(t, err, "partition state change failed unexpectedly")

	app = newApplication(appID2, "default", defQueue)
	err = partition.AddApplication(app)
	if err == nil || partition.getApplication(appID2) != nil {
		t.Errorf("add application on stopped partition should have failed but did not")
	}

	// mark partition for deletion, no new application can be added
	partition.stateMachine.SetState(objects.Active.String())
	err = partition.handlePartitionEvent(objects.Remove)
	assert.NilError(t, err, "partition state change failed unexpectedly")
	app = newApplication(appID3, "default", defQueue)
	err = partition.AddApplication(app)
	if err == nil || partition.getApplication(appID3) != nil {
		t.Errorf("add application on draining partition should have failed but did not")
	}
}

func TestAddAppTaskGroup(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app: TG specified with resource no max set on the queue
	task := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 10000})
	app := newApplicationTG(appID1, "default", defQueue, task)
	assert.Assert(t, resources.Equals(app.GetPlaceholderAsk(), task), "placeholder ask not set as expected")
	// queue sort policy is FIFO this should work
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application with zero task group to partition should not have failed")

	app = newApplicationTG(appID2, "default", defQueue, task)
	assert.Assert(t, resources.Equals(app.GetPlaceholderAsk(), task), "placeholder ask not set as expected")

	// queue now has fair as sort policy app add should fail
	queue := partition.GetQueue(defQueue)
	err = queue.ApplyConf(configs.QueueConfig{
		Name:       "default",
		Parent:     false,
		Queues:     nil,
		Properties: map[string]string{configs.ApplicationSortPolicy: "fair"},
	})
	assert.NilError(t, err, "updating queue should not have failed")
	queue.UpdateSortType()
	err = partition.AddApplication(app)
	if err == nil || partition.getApplication(appID2) != nil {
		t.Errorf("add application should have failed due to queue sort policy but did not")
	}

	// queue with stateaware as sort policy, with a max set smaller than placeholder ask: app add should fail
	err = queue.ApplyConf(configs.QueueConfig{
		Name:       "default",
		Parent:     false,
		Queues:     nil,
		Properties: map[string]string{configs.ApplicationSortPolicy: "stateaware"},
		Resources:  configs.Resources{Max: map[string]string{"vcore": "5"}},
	})
	assert.NilError(t, err, "updating queue should not have failed (stateaware & max)")
	queue.UpdateSortType()
	err = partition.AddApplication(app)
	if err == nil || partition.getApplication(appID2) != nil {
		t.Errorf("add application should have failed due to max queue resource but did not")
	}

	// queue with stateaware as sort policy, with a max set larger than placeholder ask: app add works
	err = queue.ApplyConf(configs.QueueConfig{
		Name:      "default",
		Parent:    false,
		Queues:    nil,
		Resources: configs.Resources{Max: map[string]string{"vcore": "20"}},
	})
	assert.NilError(t, err, "updating queue should not have failed (max resource)")
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")
}

func TestRemoveApp(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app that will just sit around to make sure we remove the right one
	appNotRemoved := "will_not_remove"
	app := newApplication(appNotRemoved, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")
	// add a node to allow adding an allocation
	setupNode(t, nodeID1, partition, resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000000}))

	appRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000})
	ask := newAllocationAsk("alloc-nr", appNotRemoved, appRes)
	uuid := "alloc-nr-uuid"
	alloc := objects.NewAllocation(uuid, nodeID1, ask)
	err = partition.addAllocation(alloc)
	assert.NilError(t, err, "add allocation to partition should not have failed")
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	allocs := partition.removeApplication("does_not_exist")
	if allocs != nil {
		t.Errorf("non existing application returned unexpected values: allocs = %v", allocs)
	}

	// add another new app
	app = newApplication(appID1, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	// remove the newly added app (no allocations)
	allocs = partition.removeApplication(appID1)
	assert.Equal(t, 0, len(allocs), "existing application without allocations returned allocations %v", allocs)
	assert.Equal(t, 1, len(partition.applications), "existing application was not removed")
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	// add the application again and then an allocation
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	ask = newAllocationAsk("alloc-1", appID1, appRes)
	alloc = objects.NewAllocation("alloc-1-uuid", nodeID1, ask)
	err = partition.addAllocation(alloc)
	assert.NilError(t, err, "add allocation to partition should not have failed")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(appRes, 2))

	// remove the newly added app
	allocs = partition.removeApplication(appID1)
	assert.Equal(t, 1, len(allocs), "existing application with allocations returned unexpected allocations %v", allocs)
	assert.Equal(t, 1, len(partition.applications), "existing application was not removed")
	if partition.GetTotalAllocationCount() != 1 {
		t.Errorf("allocation that should have been left was removed")
	}
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	allocs = partition.removeApplication("will_not_remove")
	assert.Equal(t, 1, len(allocs), "existing application with allocations returned unexpected allocations %v", allocs)
	assertUserGroupResource(t, getTestUserGroup(), nil)
}

func TestRemoveAppAllocs(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app that will just sit around to make sure we remove the right one
	appNotRemoved := "will_not_remove"
	app := newApplication(appNotRemoved, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")
	// add a node to allow adding an allocation
	setupNode(t, nodeID1, partition, resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000000}))

	appRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000})
	ask := newAllocationAsk("alloc-nr", appNotRemoved, appRes)
	alloc := objects.NewAllocation("alloc-nr-uuid", nodeID1, ask)
	err = partition.addAllocation(alloc)
	assertUserGroupResource(t, getTestUserGroup(), appRes)

	ask = newAllocationAsk("alloc-1", appNotRemoved, appRes)
	uuid := "alloc-1-uuid"
	alloc = objects.NewAllocation(uuid, nodeID1, ask)
	err = partition.addAllocation(alloc)
	assert.NilError(t, err, "add allocation to partition should not have failed")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(appRes, 2))
	release := &si.AllocationRelease{
		PartitionName:   "default",
		ApplicationID:   "",
		UUID:            "",
		TerminationType: si.TerminationType_STOPPED_BY_RM,
	}

	allocs, _ := partition.removeAllocation(release)
	assert.Equal(t, 0, len(allocs), "empty removal request returned allocations: %v", allocs)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(appRes, 2))
	// create a new release without app: should just return
	release.ApplicationID = "does_not_exist"
	allocs, _ = partition.removeAllocation(release)
	assert.Equal(t, 0, len(allocs), "removal request for non existing application returned allocations: %v", allocs)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(appRes, 2))
	// create a new release with app, non existing allocation: should just return
	release.ApplicationID = appNotRemoved
	release.UUID = "does_not_exist"
	allocs, _ = partition.removeAllocation(release)
	assert.Equal(t, 0, len(allocs), "removal request for non existing allocation returned allocations: %v", allocs)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(appRes, 2))
	// create a new release with app, existing allocation: should return 1 alloc
	assert.Equal(t, 2, partition.GetTotalAllocationCount(), "pre-remove allocation list incorrect: %v", partition.allocations)
	release.UUID = uuid
	allocs, _ = partition.removeAllocation(release)
	assert.Equal(t, 1, len(allocs), "removal request for existing allocation returned wrong allocations: %v", allocs)
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "allocation removal requests removed more than expected: %v", partition.allocations)
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(appRes, 1))
	// create a new release with app, no uuid: should return last left alloc
	release.UUID = ""
	allocs, _ = partition.removeAllocation(release)
	assert.Equal(t, 1, len(allocs), "removal request for existing allocation returned wrong allocations: %v", allocs)
	assert.Equal(t, 0, partition.GetTotalAllocationCount(), "removal requests did not remove all allocations: %v", partition.allocations)
	assertUserGroupResource(t, getTestUserGroup(), nil)
}

// Dynamic queue creation based on the name from the rules
func TestCreateQueue(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	// top level should fail
	_, err = partition.createQueue("test", security.UserGroup{})
	if err == nil {
		t.Errorf("top level queue creation did not fail")
	}

	// create below leaf
	_, err = partition.createQueue("root.default.test", security.UserGroup{})
	if err == nil {
		t.Errorf("'root.default.test' queue creation did not fail")
	}

	// single level create
	var queue *objects.Queue
	queue, err = partition.createQueue("root.test", security.UserGroup{})
	assert.NilError(t, err, "'root.test' queue creation failed")
	if queue == nil {
		t.Errorf("'root.test' queue creation failed without error")
	}
	if queue != nil && !queue.IsLeafQueue() && queue.IsManaged() {
		t.Errorf("'root.test' queue creation failed not created with correct settings: %v", queue)
	}

	// multiple level create
	queue, err = partition.createQueue("root.parent.test", security.UserGroup{})
	assert.NilError(t, err, "'root.parent.test' queue creation failed")
	if queue == nil {
		t.Fatalf("'root.parent.test' queue creation failed without error")
	}
	if !queue.IsLeafQueue() && queue.IsManaged() {
		t.Errorf("'root.parent.test' queue not created with correct settings: %v", queue)
	}
	queue = partition.GetQueue("root.parent")
	if queue == nil {
		t.Errorf("'root.parent' queue creation failed: parent is not set correctly")
	}
	if queue != nil && queue.IsLeafQueue() && queue.IsManaged() {
		t.Errorf("'root.parent' parent queue not created with correct settings: %v", queue)
	}

	// deep level create
	queue, err = partition.createQueue("root.parent.next.level.test.leaf", security.UserGroup{})
	assert.NilError(t, err, "'root.parent.next.level.test.leaf' queue creation failed")
	if queue == nil {
		t.Errorf("'root.parent.next.level.test.leaf' queue creation failed without error")
	}
	if queue != nil && !queue.IsLeafQueue() && queue.IsManaged() {
		t.Errorf("'root.parent.next.level.test.leaf' queue not created with correct settings: %v", queue)
	}
}

// Managed queue creation based on the config
func TestCreateDeepQueueConfig(t *testing.T) {
	conf := make([]configs.QueueConfig, 0)
	conf = append(conf, configs.QueueConfig{
		Name:   "level1",
		Parent: true,
		Queues: []configs.QueueConfig{
			{
				Name:   "level2",
				Parent: true,
				Queues: []configs.QueueConfig{
					{
						Name:   "level3",
						Parent: true,
						Queues: []configs.QueueConfig{
							{
								Name:   "level4",
								Parent: true,
								Queues: []configs.QueueConfig{
									{
										Name:   "level5",
										Parent: false,
										Queues: nil,
									},
								},
							},
						},
					},
				},
			},
		},
	})

	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	// There is a queue setup as the config must be valid when we run
	root := partition.GetQueue("root")
	if root == nil {
		t.Error("root queue not found in partition")
	}
	err = partition.addQueue(conf, root)
	assert.NilError(t, err, "'root.level1.level2.level3.level4.level5' queue creation from config failed")
	queue := partition.GetQueue("root.level1.level2.level3.level4.level5")
	if queue == nil {
		t.Fatal("root.level1.level2.level3.level4.level5 queue not found in partition")
	}
	assert.Equal(t, "root.level1.level2.level3.level4.level5", queue.GetQueuePath(), "root.level1.level2.level3.level4.level5 queue not found in partition")
}

func TestUpdateQueues(t *testing.T) {
	conf := []configs.QueueConfig{
		{
			Name:   "parent",
			Parent: false,
			Queues: nil,
		},
	}

	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	// There is a queue setup as the config must be valid when we run
	root := partition.GetQueue("root")
	if root == nil {
		t.Error("root queue not found in partition")
	}
	err = partition.updateQueues(conf, root)
	assert.NilError(t, err, "queue update from config failed")
	def := partition.GetQueue(defQueue)
	if def == nil {
		t.Fatal("default queue should still exist")
	}
	assert.Assert(t, def.IsDraining(), "'root.default' queue should have been marked for removal")

	var resExpect *resources.Resource
	resMap := map[string]string{"vcore": "1"}
	resExpect, err = resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "resource from conf failed")

	conf = []configs.QueueConfig{
		{
			Name:      "parent",
			Parent:    true,
			Resources: configs.Resources{Max: resMap},
			Queues: []configs.QueueConfig{
				{
					Name:   "leaf",
					Parent: false,
					Queues: nil,
				},
			},
		},
	}
	err = partition.updateQueues(conf, root)
	assert.NilError(t, err, "queue update from config failed")
	parent := partition.GetQueue("root.parent")
	if parent == nil {
		t.Fatal("parent queue should still exist")
	}
	assert.Assert(t, resources.Equals(parent.GetMaxResource(), resExpect), "parent queue max resource should have been updated")
	leaf := partition.GetQueue("root.parent.leaf")
	if leaf == nil {
		t.Fatal("leaf queue should have been created")
	}
}

func TestGetQueue(t *testing.T) {
	// get the partition
	partition, err := newBasePartition()
	assert.NilError(t, err, "test partition create failed with error")
	var nilQueue *objects.Queue
	// test partition has a root queue
	queue := partition.GetQueue("")
	assert.Equal(t, queue, nilQueue, "partition with just root returned not nil for empty request: %v", queue)
	queue = partition.GetQueue("unknown")
	assert.Equal(t, queue, nilQueue, "partition returned not nil for unqualified unknown request: %v", queue)
	queue = partition.GetQueue("root")
	assert.Equal(t, queue, partition.root, "partition did not return root as requested")

	parentConf := configs.QueueConfig{
		Name:   "parent",
		Parent: true,
		Queues: nil,
	}
	var parent *objects.Queue
	// manually add the queue in below the root
	parent, err = objects.NewConfiguredQueue(parentConf, queue)
	assert.NilError(t, err, "failed to create parent queue")
	queue = partition.GetQueue("root.unknown")
	assert.Equal(t, queue, nilQueue, "partition returned not nil for non existing queue name request: %v", queue)
	queue = partition.GetQueue("root.parent")
	assert.Equal(t, queue, parent, "partition returned nil for existing queue name request")
}

func TestGetStateDumpFilePath(t *testing.T) {
	// get the partition
	partition, err := newBasePartition()
	partition.RLock()
	defer partition.RUnlock()

	assert.NilError(t, err, "test partition create failed with error")
	stateDumpFilePath := partition.GetStateDumpFilePath()
	assert.Equal(t, stateDumpFilePath, "")

	partition, err = newStateDumpFilePartition()
	assert.NilError(t, err, "test partition create failed with error")
	fileName := "yunikorn-state.txt"
	stateDumpFilePath = partition.GetStateDumpFilePath()
	assert.Equal(t, stateDumpFilePath, fileName)
}

func TestTryAllocate(t *testing.T) {
	setupUGM()
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %v", alloc.String())
	}

	// create a set of queues and apps: app-1 2 asks; app-2 1 ask (same size)
	// sub-leaf will have an app with 2 requests and thus more unconfirmed resources compared to leaf2
	// this should filter up in the parent and the 1st allocate should show an app-1 allocation
	// the ask with the higher priority is the second one added alloc-2 for app-1
	app := newApplication(appID1, "default", "root.parent.sub-leaf")

	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "1"})
	assert.NilError(t, err, "failed to create resource")

	// add to the partition
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-1 to partition")
	err = app.AddAllocationAsk(newAllocationAsk(allocID, appID1, res))
	assert.NilError(t, err, "failed to add ask alloc-1 to app-1")
	err = app.AddAllocationAsk(newAllocationAskPriority("alloc-2", appID1, res, 1, 2))
	assert.NilError(t, err, "failed to add ask alloc-2 to app-1")

	app = newApplication(appID2, "default", "root.leaf")
	// add to the partition
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-2 to partition")
	err = app.AddAllocationAsk(newAllocationAskPriority(allocID, appID2, res, 1, 2))
	assert.NilError(t, err, "failed to add ask alloc-1 to app-2")
	assertUserGroupResource(t, getTestUserGroup(), nil)

	// first allocation should be app-1 and alloc-2
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), allocID2, "expected ask alloc-2 to be allocated")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 1))

	// second allocation should be app-2 and alloc-1: higher up in the queue hierarchy
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID2, "expected application app-2 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), allocID, "expected ask alloc-1 to be allocated")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	// third allocation should be app-1 and alloc-1
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), allocID, "expected ask alloc-1 to be allocated")
	assert.Assert(t, resources.IsZero(partition.root.GetPendingResource()), "pending resources should be set to zero")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 3))
}

// allocate ask request with required node
func TestRequiredNodeReservation(t *testing.T) {
	setupUGM()
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %v", alloc.String())
	}

	app := newApplication(appID1, "default", "root.parent.sub-leaf")
	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "8"})
	assert.NilError(t, err, "failed to create resource")

	// add to the partition
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-1 to partition")
	ask := newAllocationAsk(allocID, appID1, res)
	ask.SetRequiredNode(nodeID1)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app-1")
	assertUserGroupResource(t, getTestUserGroup(), nil)

	// first allocation should be app-1 and alloc-1
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), allocID, "expected ask alloc-1 to be allocated")
	assertUserGroupResource(t, getTestUserGroup(), res)

	ask2 := newAllocationAsk(allocID2, appID1, res)
	ask2.SetRequiredNode(nodeID1)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "failed to add ask alloc-2 to app-1")
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("allocation attempt should have returned an allocation")
	}
	// check if updated (must be after allocate call)
	assert.Equal(t, 1, len(app.GetReservations()), "app should have one reserved ask")
	assert.Equal(t, 1, len(app.GetAskReservations(allocID2)), "ask should have been reserved")
	assertUserGroupResource(t, getTestUserGroup(), res)

	// allocation that fits on the node should not be allocated
	var res2 *resources.Resource
	res2, err = resources.NewResourceFromConf(map[string]string{"vcore": "1"})
	assert.NilError(t, err, "failed to create resource")

	ask3 := newAllocationAsk("alloc-3", appID1, res2)
	ask3.SetRequiredNode(nodeID1)
	err = app.AddAllocationAsk(ask3)
	assert.NilError(t, err, "failed to add ask alloc-3 to app-1")
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("allocation attempt should not have returned an allocation")
	}

	// reservation count remains same as last try allocate should have failed to find a reservation
	assert.Equal(t, 1, len(app.GetReservations()), "ask should not have been reserved, count changed")
	assertUserGroupResource(t, getTestUserGroup(), res)
}

func TestRequiredNodeNotExist(t *testing.T) {
	setupUGM()
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %v", alloc.String())
	}

	// add app to the partition
	app := newApplication(appID1, "default", "root.parent.sub-leaf")
	err := partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-1 to partition")

	// normal ask with node that does not exist
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"vcore": "1"})
	assert.NilError(t, err, "failed to create resource")
	ask := newAllocationAsk(allocID, appID1, res)
	ask.SetRequiredNode("unknown")
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app-1")

	// try to allocate on the unknown node (handle panic if needed)
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil required node")
		}
	}()
	alloc := partition.tryAllocate()
	if alloc != nil {
		t.Fatal("allocation should not have worked on unknown node")
	}
	assertUserGroupResource(t, getTestUserGroup(), nil)
}

// basic ds scheduling on specific node in first allocate run itself (without any need for reservation)
func TestRequiredNodeAllocation(t *testing.T) {
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %v", alloc.String())
	}

	app := newApplication(appID1, "default", "root.parent.sub-leaf")
	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "4"})
	assert.NilError(t, err, "failed to create resource")

	// add to the partition
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-1 to partition")

	// normal ask
	ask := newAllocationAsk(allocID, appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app-1")

	// first allocation should be app-1 and alloc-1
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), allocID, "expected ask alloc-1 to be allocated")
	assertUserGroupResource(t, getTestUserGroup(), res)

	// required node set on ask
	ask2 := newAllocationAsk(allocID2, appID1, res)
	ask2.SetRequiredNode(nodeID1)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "failed to add ask alloc-2 to app-1")

	// since node-1 available resource is larger than required ask gets allocated
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	// ensure there is no reservations
	assert.Equal(t, 0, len(app.GetReservations()), "ask should not have been reserved")
	assert.Equal(t, alloc.GetAllocationKey(), allocID2, "expected ask alloc-2 to be allocated")
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
}

// Preemption followed by a normal allocation
func TestPreemptionForRequiredNodeNormalAlloc(t *testing.T) {
	setupUGM()
	// setup the partition so we can try the real allocation
	partition, app := setupPreemptionForRequiredNode(t)
	// now try the allocation again: the normal path
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation attempt should have returned an allocation")
	}
	// check if updated (must be after allocate call)
	assert.Equal(t, 0, len(app.GetReservations()), "ask should have no longer be reserved")
	assert.Equal(t, alloc.GetResult(), objects.AllocatedReserved, "result is not the expected AllocatedReserved")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetAllocationKey(), allocID2, "expected ask alloc-2 to be allocated")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 8000}))
}

// Preemption followed by a reserved allocation
func TestPreemptionForRequiredNodeReservedAlloc(t *testing.T) {
	setupUGM()
	// setup the partition so we can try the real allocation
	partition, app := setupPreemptionForRequiredNode(t)
	// now try the allocation again: the reserved path
	alloc := partition.tryReservedAllocate()
	if alloc == nil {
		t.Fatal("allocation attempt should have returned an allocation")
	}
	// check if updated (must be after allocate call)
	assert.Equal(t, 0, len(app.GetReservations()), "ask should have no longer be reserved")
	assert.Equal(t, alloc.GetResult(), objects.AllocatedReserved, "result is not the expected AllocatedReserved")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetAllocationKey(), allocID2, "expected ask alloc-2 to be allocated")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 8000}))
}

func TestMultiplePreemptionAttemptAvoided(t *testing.T) {
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}

	app, testHandler := newApplicationWithHandler(appID1, "default", "root.parent.sub-leaf")
	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "8"})
	assert.NilError(t, err, "failed to create resource")

	// add to the partition
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-1 to partition")

	// normal ask
	ask := newAllocationAsk(allocID, appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app-1")

	// first allocation should be app-1 and alloc-1
	alloc := partition.tryAllocate()

	// required node set on ask
	ask2 := newAllocationAsk(allocID2, appID1, res)
	ask2.SetRequiredNode(nodeID1)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "failed to add ask alloc-2 to app-1")
	partition.tryAllocate()

	// try multiple reserved allocation
	partition.tryReservedAllocate()
	partition.tryReservedAllocate()
	partition.tryReservedAllocate()
	partition.tryReservedAllocate()

	var eventCount int
	for _, event := range testHandler.GetEvents() {
		if allocRelease, ok := event.(*rmevent.RMReleaseAllocationEvent); ok {
			if allocRelease.ReleasedAllocations[0].AllocationKey == allocID {
				eventCount++
			}
		}
	}
	assert.Equal(t, 1, eventCount)
	assert.Equal(t, true, ask2.HasTriggeredPreemption())
	assert.Equal(t, true, alloc.IsPreempted())
}

// setup the partition in a state that we need for multiple tests
func setupPreemptionForRequiredNode(t *testing.T) (*PartitionContext, *objects.Application) {
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %v", alloc.String())
	}

	app, testHandler := newApplicationWithHandler(appID1, "default", "root.parent.sub-leaf")
	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "8"})
	assert.NilError(t, err, "failed to create resource")

	// add to the partition
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-1 to partition")

	// normal ask
	ask := newAllocationAsk(allocID, appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app-1")

	// first allocation should be app-1 and alloc-1
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), allocID, "expected ask alloc-1 to be allocated")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 8000}))
	uuid := alloc.GetUUID()

	// required node set on ask
	ask2 := newAllocationAsk(allocID2, appID1, res)
	ask2.SetRequiredNode(nodeID1)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "failed to add ask alloc-2 to app-1")

	// since node-1 available resource is less than needed, reserve the node for alloc-2
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("allocation attempt should not have returned an allocation")
	}
	// check if updated (must be after allocate call)
	assert.Equal(t, 1, len(app.GetReservations()), "ask should have been reserved")
	assert.Equal(t, 1, len(app.GetAskReservations(allocID2)), "ask should have been reserved")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 8000}))

	// try through reserved scheduling cycle this should trigger preemption
	alloc = partition.tryReservedAllocate()
	if alloc != nil {
		t.Fatal("reserved allocation attempt should not have returned an allocation")
	}

	// check if there is a release event for the expected allocation
	var found bool
	for _, event := range testHandler.GetEvents() {
		if allocRelease, ok := event.(*rmevent.RMReleaseAllocationEvent); ok {
			found = allocRelease.ReleasedAllocations[0].AllocationKey == allocID
			break
		}
	}
	assert.Assert(t, found, "release allocation event not found in list")
	// release allocation: do what the context would do after the shim processing
	release := &si.AllocationRelease{
		PartitionName:   partition.Name,
		ApplicationID:   appID1,
		UUID:            uuid,
		TerminationType: si.TerminationType_PREEMPTED_BY_SCHEDULER,
	}
	releases, _ := partition.removeAllocation(release)
	assert.Equal(t, 1, len(releases), "unexpected number of allocations released")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 0}))
	return partition, app
}

func TestTryAllocateLarge(t *testing.T) {
	setupUGM()
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %v", alloc.String())
	}

	// override the reservation delay, and cleanup when done
	objects.SetReservationDelay(10 * time.Nanosecond)
	defer objects.SetReservationDelay(2 * time.Second)

	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "100"})
	assert.NilError(t, err, "failed to create resource")

	app := newApplication(appID1, "default", "root.parent.sub-leaf")
	// add to the partition
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-1 to partition")

	err = app.AddAllocationAsk(newAllocationAsk("alloc-1", appID1, res))
	assert.NilError(t, err, "failed to add large ask to app")
	assert.Assert(t, resources.Equals(res, app.GetPendingResource()), "pending resource not set as expected")
	alloc := partition.tryAllocate()
	if alloc != nil {
		t.Fatalf("allocation did return allocation which does not fit: %s", alloc)
	}
	assert.Equal(t, 0, len(app.GetReservations()), "ask should not have been reserved")
	assertUserGroupResource(t, getTestUserGroup(), nil)
}

func TestAllocReserveNewNode(t *testing.T) {
	setupUGM()
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %s", alloc)
	}

	// override the reservation delay, and cleanup when done
	objects.SetReservationDelay(10 * time.Nanosecond)
	defer objects.SetReservationDelay(2 * time.Second)

	// turn off the second node
	node2 := partition.GetNode(nodeID2)
	node2.SetSchedulable(false)

	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "8"})
	assert.NilError(t, err, "failed to create resource")

	// only one resource for alloc fits on a node
	app := newApplication(appID1, "default", "root.parent.sub-leaf")
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-1 to partition")

	ask := newAllocationAskRepeat("alloc-1", appID1, res, 2)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask to app")
	// calculate the resource size using the repeat request (reuse is possible using proto conversions in ask)
	res.MultiplyTo(2)
	assert.Assert(t, resources.Equals(res, app.GetPendingResource()), "pending resource not set as expected")
	assert.Assert(t, resources.Equals(res, partition.root.GetPendingResource()), "pending resource not set as expected on root queue")

	// the first one should be allocated
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("1st allocation did not return the correct allocation")
	}
	assert.Equal(t, objects.Allocated, alloc.GetResult(), "allocation result should have been allocated")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 8000}))

	// the second one should be reserved as the 2nd node is not scheduling
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("2nd allocation did not return the correct allocation")
	}
	// check if updated (must be after allocate call)
	assert.Equal(t, 1, len(app.GetReservations()), "ask should have been reserved")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 8000}))

	// turn on 2nd node
	node2.SetSchedulable(true)
	alloc = partition.tryReservedAllocate()
	assert.Equal(t, objects.AllocatedReserved, alloc.GetResult(), "allocation result should have been allocatedReserved")
	assert.Equal(t, "", alloc.GetReservedNodeID(), "reserved node should be reset after processing")
	assert.Equal(t, node2.NodeID, alloc.GetNodeID(), "allocation should be fulfilled on new node")
	// check if all updated
	node1 := partition.GetNode(nodeID1)
	assert.Equal(t, 0, len(node1.GetReservations()), "old node should have no more reservations")
	assert.Equal(t, 0, len(app.GetReservations()), "ask should have been reserved")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 16000}))
}

func TestTryAllocateReserve(t *testing.T) {
	setupUGM()
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryReservedAllocate(); alloc != nil {
		t.Fatalf("empty cluster reserved allocate returned allocation: %s", alloc)
	}

	// create a set of queues and apps: app-1 2 asks; app-2 1 ask (same size)
	// sub-leaf will have an app with 2 requests and thus more unconfirmed resources compared to leaf2
	// this should filter up in the parent and the 1st allocate should show an app-1 allocation
	// the ask with the higher priority is the second one added alloc-2 for app-1
	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "1"})
	assert.NilError(t, err, "failed to create resource")

	app := newApplication(appID1, "default", "root.parent.sub-leaf")
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-1 to partition")

	err = app.AddAllocationAsk(newAllocationAsk("alloc-1", appID1, res))
	assert.NilError(t, err, "failed to add ask alloc-1 to app")

	ask := newAllocationAsk("alloc-2", appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-2 to app")
	node2 := partition.GetNode(nodeID2)
	if node2 == nil {
		t.Fatal("expected node-2 to be returned got nil")
	}
	partition.reserve(app, node2, ask)
	if !app.IsReservedOnNode(node2.NodeID) || len(app.GetAskReservations("alloc-2")) == 0 {
		t.Fatalf("reservation failure for ask2 and node2")
	}

	// first allocation should be app-1 and alloc-2
	alloc := partition.tryReservedAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.AllocatedReserved, "result is not the expected allocated from reserved")
	assert.Equal(t, alloc.GetReservedNodeID(), "", "node should not be set for allocated from reserved")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), "alloc-2", "expected ask alloc-2 to be allocated")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000}))

	// reservations should have been removed: it is in progress
	if app.IsReservedOnNode(node2.NodeID) || len(app.GetAskReservations("alloc-2")) != 0 {
		t.Fatalf("reservation removal failure for ask2 and node2")
	}

	// no reservations left this should return nil
	alloc = partition.tryReservedAllocate()
	if alloc != nil {
		t.Fatalf("reserved allocation should not return any allocation: %s, '%s'", alloc, alloc.GetReservedNodeID())
	}
	// try non reserved this should allocate
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), "alloc-1", "expected ask alloc-1 to be allocated")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 2000}))

	if !resources.IsZero(partition.root.GetPendingResource()) {
		t.Fatalf("pending allocations should be set to zero")
	}
}

func TestTryAllocateWithReserved(t *testing.T) {
	setupUGM()
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryReservedAllocate(); alloc != nil {
		t.Fatalf("empty cluster reserved allocate returned allocation: %v", alloc)
	}

	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "5"})
	assert.NilError(t, err, "failed to create resource")

	app := newApplication(appID1, "default", "root.parent.sub-leaf")
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-1 to partition")

	ask := newAllocationAskRepeat("alloc-1", appID1, res, 2)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app")

	// reserve one node: scheduling should happen on the other
	node2 := partition.GetNode(nodeID2)
	if node2 == nil {
		t.Fatal("expected node-2 to be returned got nil")
	}
	partition.reserve(app, node2, ask)
	if !app.IsReservedOnNode(node2.NodeID) || len(app.GetAskReservations("alloc-1")) == 0 {
		t.Fatal("reservation failure for ask and node2")
	}
	assert.Equal(t, 1, len(partition.reservedApps), "partition should have reserved app")
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return correct an allocation")
	}
	assert.Equal(t, objects.AllocatedReserved, alloc.GetResult(), "expected reserved allocation to be returned")
	assert.Equal(t, "", alloc.GetReservedNodeID(), "reserved node should be reset after processing")
	assert.Equal(t, 0, len(node2.GetReservations()), "reservation should have been removed from node")
	assert.Equal(t, false, app.IsReservedOnNode(node2.NodeID), "reservation cleanup for ask on app failed")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 5000}))

	// node2 is unreserved now so the next one should allocate on the 2nd node (fair sharing)
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return correct allocation")
	}
	assert.Equal(t, objects.Allocated, alloc.GetResult(), "expected allocated allocation to be returned")
	assert.Equal(t, node2.NodeID, alloc.GetNodeID(), "expected allocation on node2 to be returned")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 10000}))
}

// remove the reserved ask while allocating in flight for the ask
func TestScheduleRemoveReservedAsk(t *testing.T) {
	setupUGM()
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %s", alloc)
	}

	// override the reservation delay, and cleanup when done
	objects.SetReservationDelay(10 * time.Nanosecond)
	defer objects.SetReservationDelay(2 * time.Second)

	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "4"})
	assert.NilError(t, err, "resource creation failed")
	app := newApplication(appID1, "default", "root.parent.sub-leaf")
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app app-1 to partition")
	ask := newAllocationAskRepeat("alloc-1", appID1, res, 4)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app")

	// calculate the resource size using the repeat request
	pending := resources.Multiply(res, 4)
	assert.Assert(t, resources.Equals(pending, app.GetPendingResource()), "pending resource not set as expected")

	// allocate the ask
	for i := 1; i <= 4; i++ {
		alloc := partition.tryAllocate()
		if alloc == nil || alloc.GetResult() != objects.Allocated {
			t.Fatalf("expected allocated allocation to be returned (step %d) %s", i, alloc)
		}
	}
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 16000}))

	// add a asks which should reserve
	ask = newAllocationAskRepeat("alloc-2", appID1, res, 1)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-2 to app")
	ask = newAllocationAskRepeat("alloc-3", appID1, res, 1)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-3 to app")
	pending = resources.Multiply(res, 2)
	assert.Assert(t, resources.Equals(pending, app.GetPendingResource()), "pending resource not set as expected")
	// allocate so we get reservations
	for i := 1; i <= 2; i++ {
		alloc := partition.tryAllocate()
		if alloc != nil {
			t.Fatalf("expected reservations to be created not allocation to be returned (step %d) %s", i, alloc)
		}
		assert.Equal(t, len(app.GetReservations()), i, "application reservations incorrect")
	}
	assert.Equal(t, len(partition.reservedApps), 1, "partition should have reserved app")
	assert.Equal(t, len(app.GetReservations()), 2, "application reservations should be 2")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 16000}))

	// add a node
	node := newNodeMaxResource("node-3", res)
	err = partition.AddNode(node, nil)
	assert.NilError(t, err, "failed to add node node-3 to the partition")
	// Try to allocate one of the reservation. We go directly to the root queue not using the partition otherwise
	// we confirm before we get back in the test code and cannot remove the ask
	alloc := partition.root.TryReservedAllocate(partition.GetNodeIterator)
	if alloc == nil || alloc.GetResult() != objects.AllocatedReserved {
		t.Fatalf("expected allocatedReserved allocation to be returned %v", alloc)
	}
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 20000}))

	// before confirming remove the ask: do what the scheduler does when it gets a request from a
	// shim in processAllocationReleaseByAllocationKey()
	// make sure we are counting correctly and leave the other reservation intact
	removeAskID := "alloc-2"
	if alloc.GetAllocationKey() == "alloc-3" {
		removeAskID = "alloc-3"
	}
	released := app.RemoveAllocationAsk(removeAskID)
	assert.Equal(t, released, 1, "expected one reservations to be released")
	partition.unReserveCount(appID1, released)
	assert.Equal(t, len(partition.reservedApps), 1, "partition should still have reserved app")
	assert.Equal(t, len(app.GetReservations()), 1, "application reservations should be 1")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 20000}))

	// now confirm the allocation: this should not remove the reservation
	rmAlloc := partition.allocate(alloc)
	assert.Equal(t, "", rmAlloc.GetReservedNodeID(), "reserved node should be reset after processing")
	assert.Equal(t, len(partition.reservedApps), 1, "partition should still have reserved app")
	assert.Equal(t, len(app.GetReservations()), 1, "application reservations should be kept at 1")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 20000}))
}

// update the config with nodes registered and make sure that the root max and guaranteed are not changed
func TestUpdateRootQueue(t *testing.T) {
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "20"})
	assert.NilError(t, err, "resource creation failed")
	assert.Assert(t, resources.Equals(res, partition.totalPartitionResource), "partition resource not set as expected")
	assert.Assert(t, resources.Equals(res, partition.root.GetMaxResource()), "root max resource not set as expected")

	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues:    nil,
			},
		},
		PlacementRules: nil,
		Limits:         nil,
		Preemption:     configs.PartitionPreemptionConfig{},
		NodeSortPolicy: configs.NodeSortingPolicy{},
	}

	err = partition.updatePartitionDetails(conf)
	assert.NilError(t, err, "partition update failed")
	// resources should not have changed
	assert.Assert(t, resources.Equals(res, partition.totalPartitionResource), "partition resource not set as expected")
	assert.Assert(t, resources.Equals(res, partition.root.GetMaxResource()), "root max resource not set as expected")
	// make sure the update went through
	assert.Equal(t, partition.GetQueue("root.leaf").CurrentState(), objects.Draining.String(), "leaf queue should have been marked for removal")
	assert.Equal(t, partition.GetQueue("root.parent").CurrentState(), objects.Draining.String(), "parent queue should have been marked for removal")
}

// transition an application to completed state and wait for it to be processed into the completedApplications map
func completeApplicationAndWait(app *objects.Application, pc *PartitionContext) error {
	currentCount := pc.GetTotalCompletedApplicationCount()
	err := app.HandleApplicationEvent(objects.CompleteApplication)
	if err != nil {
		return err
	}

	err = common.WaitFor(10*time.Millisecond, time.Duration(1000)*time.Millisecond, func() bool {
		newCount := pc.GetTotalCompletedApplicationCount()
		return newCount == currentCount+1
	})

	return err
}

func TestCompleteApp(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	app := newApplication("completed", "default", defQueue)
	app.SetState(objects.Completing.String())
	err = partition.AddApplication(app)
	assert.NilError(t, err, "no error expected while adding the application")
	assert.Assert(t, len(partition.applications) == 1, "the partition should have 1 app")
	assert.Assert(t, len(partition.completedApplications) == 0, "the partition should not have any completed apps")
	// complete the application
	err = completeApplicationAndWait(app, partition)
	assert.NilError(t, err, "the completed application should have been processed")
	assert.Assert(t, len(partition.applications) == 0, "the partition should have no active app")
	assert.Assert(t, len(partition.completedApplications) == 1, "the partition should have 1 completed app")
}

func TestCleanupFailedApps(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	newApp1 := newApplication("newApp1", "default", defQueue)
	newApp2 := newApplication("newApp2", "default", defQueue)

	err = partition.AddApplication(newApp1)
	assert.NilError(t, err, "no error expected while adding the app")
	err = partition.AddApplication(newApp2)
	assert.NilError(t, err, "no error expected while adding the app")

	assert.Assert(t, len(partition.applications) == 2, "the partition should have 2 apps")

	newApp1.SetState(objects.Expired.String())
	partition.cleanupExpiredApps()

	assert.Assert(t, len(partition.applications) == 1, "the partition should have 1 app")
	assert.Assert(t, len(partition.GetAppsByState(objects.Expired.String())) == 0, "the partition should have 0 expired apps")
}

func TestCleanupCompletedApps(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	completedApp1 := newApplication("completedApp1", "default", defQueue)
	completedApp2 := newApplication("completedApp2", "default", defQueue)

	err = partition.AddApplication(completedApp1)
	assert.NilError(t, err, "no error expected while adding the app")
	err = partition.AddApplication(completedApp2)
	assert.NilError(t, err, "no error expected while adding the app")

	assert.Assert(t, len(partition.applications) == 2, "the partition should have 2 apps")
	assert.Assert(t, len(partition.completedApplications) == 0, "the partition should have 0 completed apps")

	// complete the applications using the event system
	completedApp1.SetState(objects.Completing.String())
	err = completeApplicationAndWait(completedApp1, partition)
	assert.NilError(t, err, "the completed application should have been processed")
	completedApp2.SetState(objects.Completing.String())
	err = completeApplicationAndWait(completedApp2, partition)
	assert.NilError(t, err, "the completed application should have been processed")

	assert.Assert(t, len(partition.applications) == 0, "the partition should have 0 apps")
	assert.Assert(t, len(partition.completedApplications) == 2, "the partition should have 2 completed apps")

	// mark the app for removal
	completedApp1.SetState(objects.Expired.String())
	partition.cleanupExpiredApps()
	assert.Assert(t, len(partition.completedApplications) == 1, "the partition should have 1 completed app")
	assert.Assert(t, len(partition.GetCompletedAppsByState(objects.Expired.String())) == 0, "the partition should have 0 expired apps")
}

func TestCleanupRejectedApps(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	rejectedApp := newApplication("new", "default", defQueue)
	rejectedMessage := fmt.Sprintf("Failed to place application %s: application rejected: no placement rule matched", rejectedApp.ApplicationID)

	partition.AddRejectedApplication(rejectedApp, rejectedMessage)
	cloneRejectedApp := partition.getRejectedApplication(rejectedApp.ApplicationID)
	assert.Equal(t, rejectedApp, cloneRejectedApp)
	assert.Equal(t, partition.rejectedApplications[rejectedApp.ApplicationID], cloneRejectedApp)
	assert.Equal(t, cloneRejectedApp.GetRejectedMessage(), rejectedMessage)
	assert.Equal(t, cloneRejectedApp.CurrentState(), objects.Rejected.String())
	assert.Assert(t, !cloneRejectedApp.FinishedTime().IsZero())

	assert.Assert(t, len(partition.rejectedApplications) == 1, "the rejectedApplications of the partition should have 1 app")
	assert.Assert(t, len(partition.GetAppsByState(objects.Rejected.String())) == 1, "the partition should have 1 rejected app")
	assert.Assert(t, len(partition.GetRejectedAppsByState(objects.Rejected.String())) == 1, "the partition should have 1 rejected app")

	rejectedApp.SetState(objects.Expired.String())
	partition.cleanupExpiredApps()
	assert.Assert(t, len(partition.rejectedApplications) == 0, "the partition should not have app")
	assert.Assert(t, partition.getRejectedApplication(rejectedApp.ApplicationID) == nil, "rejected application should have been deleted")
	assert.Assert(t, len(partition.GetAppsByState(objects.Rejected.String())) == 0, "the partition should have 0 rejected app")
	assert.Assert(t, len(partition.GetRejectedAppsByState(objects.Rejected.String())) == 0, "the partition should have 0 rejected app")
	assert.Assert(t, len(partition.GetRejectedAppsByState(objects.Expired.String())) == 0, "the partition should have 0 expired app")
}

func TestUpdateNode(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "test partition create failed with error")

	newRes, err := resources.NewResourceFromConf(map[string]string{"memory": "400M", "vcore": "30"})
	assert.NilError(t, err, "failed to create resource")

	err = partition.AddNode(newNodeMaxResource("test", newRes), nil)
	assert.NilError(t, err, "test node add failed unexpected")
	assert.Equal(t, 1, partition.nodes.GetNodeCount(), "node list not correct")

	if !resources.Equals(newRes, partition.GetTotalPartitionResource()) {
		t.Errorf("Expected partition resource %s, doesn't match with actual partition resource %s", newRes, partition.GetTotalPartitionResource())
	}

	// delta resource for a node with mem as 450 and vcores as 40 (both mem and vcores has increased)
	delta, err := resources.NewResourceFromConf(map[string]string{"memory": "50M", "vcore": "10"})
	assert.NilError(t, err, "failed to create resource")
	partition.updatePartitionResource(delta)

	expectedRes, err := resources.NewResourceFromConf(map[string]string{"memory": "450M", "vcore": "40"})
	assert.NilError(t, err, "failed to create resource")

	if !resources.Equals(expectedRes, partition.GetTotalPartitionResource()) {
		t.Errorf("Expected partition resource %s, doesn't match with actual partition resource %s", expectedRes, partition.GetTotalPartitionResource())
	}

	// delta resource for a node with mem as 400 and vcores as 30 (both mem and vcores has decreased)
	delta = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": -50000000, "vcore": -10000})
	partition.updatePartitionResource(delta)

	expectedRes, err = resources.NewResourceFromConf(map[string]string{"memory": "400M", "vcore": "30"})
	assert.NilError(t, err, "failed to create resource")

	if !resources.Equals(expectedRes, partition.GetTotalPartitionResource()) {
		t.Errorf("Expected partition resource %s, doesn't match with actual partition resource %s", expectedRes, partition.GetTotalPartitionResource())
	}

	// delta resource for a node with mem as 450 and vcores as 10 (mem has increased but vcores has decreased)
	delta = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 50000000, "vcore": -20000})
	partition.updatePartitionResource(delta)

	expectedRes, err = resources.NewResourceFromConf(map[string]string{"memory": "450M", "vcore": "10"})
	assert.NilError(t, err, "failed to create resource")

	if !resources.Equals(expectedRes, partition.GetTotalPartitionResource()) {
		t.Errorf("Expected partition resource %s, doesn't match with actual partition resource %s", expectedRes, partition.GetTotalPartitionResource())
	}
}

func TestAddTGApplication(t *testing.T) {
	limit := map[string]string{"vcore": "1"}
	partition, err := newLimitedPartition(limit)
	assert.NilError(t, err, "partition create failed")
	// add a app with TG that does not fit in the queue
	var tgRes *resources.Resource
	tgRes, err = resources.NewResourceFromConf(map[string]string{"vcore": "10"})
	assert.NilError(t, err, "failed to create resource")
	app := newApplicationTG(appID1, "default", "root.limited", tgRes)
	err = partition.AddApplication(app)
	if err == nil {
		t.Error("app-1 should be rejected due to TG request")
	}

	// add a app with TG that does fit in the queue
	limit = map[string]string{"vcore": "100"}
	partition, err = newLimitedPartition(limit)
	assert.NilError(t, err, "partition create failed")
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-1 should have been added to the partition")
	assert.Equal(t, partition.getApplication(appID1), app, "partition failed to add app incorrect app returned")

	// add a app with TG that does fit in the queue as the resource is not limited in the queue
	limit = map[string]string{"second": "100"}
	partition, err = newLimitedPartition(limit)
	assert.NilError(t, err, "partition create failed")
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-1 should have been added to the partition")
	assert.Equal(t, partition.getApplication(appID1), app, "partition failed to add app incorrect app returned")
}

func TestAddTGAppDynamic(t *testing.T) {
	partition, err := newPlacementPartition()
	assert.NilError(t, err, "partition create failed")
	// add a app with TG that does fit in the dynamic queue (no limit)
	var tgRes *resources.Resource
	tgRes, err = resources.NewResourceFromConf(map[string]string{"vcore": "10"})
	assert.NilError(t, err, "failed to create resource")
	tags := map[string]string{"taskqueue": "unlimited"}
	app := newApplicationTGTags(appID1, "default", "unknown", tgRes, tags)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-1 should have been added to the partition")
	assert.Equal(t, app.GetQueuePath(), "root.unlimited", "app-1 not placed in expected queue")

	jsonRes := "{\"resources\":{\"vcore\":{\"value\":10000}}}"
	tags = map[string]string{"taskqueue": "same", siCommon.AppTagNamespaceResourceQuota: jsonRes}
	app = newApplicationTGTags(appID2, "default", "unknown", tgRes, tags)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-2 should have been added to the partition")
	assert.Equal(t, partition.getApplication(appID2), app, "partition failed to add app incorrect app returned")
	assert.Equal(t, app.GetQueuePath(), "root.same", "app-2 not placed in expected queue")

	jsonRes = "{\"resources\":{\"vcore\":{\"value\":1000}}}"
	tags = map[string]string{"taskqueue": "smaller", siCommon.AppTagNamespaceResourceQuota: jsonRes}
	app = newApplicationTGTags(appID3, "default", "unknown", tgRes, tags)
	err = partition.AddApplication(app)
	if err == nil {
		t.Error("app-3 should not have been added to the partition: TG & dynamic limit")
	}
	if partition.getApplication(appID3) != nil {
		t.Fatal("partition added app incorrectly should have failed")
	}
	queue := partition.GetQueue("root.smaller")
	if queue == nil {
		t.Fatal("queue should have been added, even if app failed")
	}
}

func TestPlaceholderSmallerThanReal(t *testing.T) {
	setupUGM()
	events.CreateAndSetEventCache()
	cache := events.GetEventCache()
	cache.StartService()

	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	tgRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 5})
	phRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 2})

	// add node to allow allocation
	node := setupNode(t, nodeID1, partition, tgRes)

	// add the app with placeholder request
	app := newApplicationTG(appID1, "default", "root.default", tgRes)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-1 should have been added to the partition")
	// add an ask for a placeholder and allocate
	ask := newAllocationAskTG(phID, appID1, taskGroup, phRes, true)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add placeholder ask ph-1 to app")
	// try to allocate a placeholder via normal allocate
	ph := partition.tryAllocate()
	if ph == nil {
		t.Fatal("expected placeholder ph-1 to be allocated")
	}
	assert.Equal(t, phID, ph.GetAllocationKey(), "expected allocation of ph-1 to be returned")
	assert.Assert(t, resources.Equals(phRes, app.GetQueue().GetAllocatedResource()), "placeholder size should be allocated on queue")
	assert.Assert(t, resources.Equals(phRes, node.GetAllocatedResource()), "placeholder size should be allocated on node")
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "placeholder allocation should be registered on the partition")
	assertUserGroupResource(t, getTestUserGroup(), phRes)

	// add an ask which is larger than the placeholder
	ask = newAllocationAskTG(allocID, appID1, taskGroup, tgRes, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app with correct TG")
	// allocate should trigger release of placeholder nothing else
	if partition.tryPlaceholderAllocate() != nil {
		t.Fatal("allocation should not have matched placeholder")
	}
	assert.Assert(t, ph.IsReleased(), "placeholder should be released")
	assertUserGroupResource(t, getTestUserGroup(), phRes)

	// wait for events to be processed
	err = common.WaitFor(1*time.Millisecond, 10*time.Millisecond, func() bool {
		return cache.Store.CountStoredEvents() == 1
	})
	assert.NilError(t, err, "the event should have been processed")

	records := cache.Store.CollectEvents()
	if records == nil {
		t.Fatal("collecting eventChannel should return something")
	}
	assert.Equal(t, 1, len(records), "expecting one event for placeholder mismatch")
	record := records[0]
	assert.Equal(t, si.EventRecord_REQUEST, record.Type, "incorrect event type")
	assert.Equal(t, phID, record.ObjectID, "incorrect allocation ID, expected placeholder alloc ID")
	assert.Equal(t, appID1, record.GroupID, "event should reference application ID")
	assert.Assert(t, strings.Contains(record.Reason, "releasing placeholder"), "reason should contain 'releasing placeholder'")
	assertUserGroupResource(t, getTestUserGroup(), phRes)

	// release placeholder: do what the context would do after the shim processing
	release := &si.AllocationRelease{
		PartitionName:   ph.GetPartitionName(),
		ApplicationID:   appID1,
		UUID:            ph.GetUUID(),
		TerminationType: si.TerminationType_TIMEOUT,
	}
	released, _ := partition.removeAllocation(release)
	assert.Equal(t, 0, len(released), "expected no releases")
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "nothing should be allocated on node")
	assert.Assert(t, resources.IsZero(app.GetQueue().GetAllocatedResource()), "nothing should be allocated on queue")
	assert.Equal(t, 0, partition.GetTotalAllocationCount(), "no allocation should be registered on the partition")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0, "second": 0}))
}

// one real allocation should trigger cleanup of all placeholders
func TestPlaceholderSmallerMulti(t *testing.T) {
	setupUGM()
	events.CreateAndSetEventCache()
	cache := events.GetEventCache()
	cache.StartService()

	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	phCount := 5
	phRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 2})
	tgRes := resources.Multiply(phRes, int64(phCount))

	// add node to allow allocation
	node := setupNode(t, nodeID1, partition, tgRes)

	// add the app with placeholder request
	app := newApplicationTG(appID1, "default", "root.default", tgRes)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-1 should have been added to the partition")
	phs := make(map[string]*objects.Allocation, 5)
	for i := 0; i < phCount; i++ {
		// add an ask for a placeholder and allocate
		id := "ph-" + strconv.Itoa(i)
		ask := newAllocationAskTG(id, appID1, taskGroup, phRes, true)
		err = app.AddAllocationAsk(ask)
		assert.NilError(t, err, "failed to add placeholder ask %s to app", id)
		// try to allocate a placeholder via normal allocate
		ph := partition.tryAllocate()
		if ph == nil {
			t.Fatalf("expected placeholder %s to be allocated", id)
		}
		assert.Equal(t, id, ph.GetAllocationKey(), "expected allocation of %s to be returned", id)
		phs[id] = ph
	}
	assert.Assert(t, resources.Equals(tgRes, app.GetQueue().GetAllocatedResource()), "all placeholders should be allocated on queue")
	assert.Assert(t, resources.Equals(tgRes, node.GetAllocatedResource()), "all placeholders should be allocated on node")
	assert.Equal(t, phCount, partition.GetTotalAllocationCount(), "placeholder allocation should be registered on the partition")
	assertUserGroupResource(t, getTestUserGroup(), tgRes)

	// add an ask which is larger than the placeholder
	ask := newAllocationAskTG(allocID, appID1, taskGroup, tgRes, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app with correct TG")
	// allocate should trigger release of placeholder nothing else
	if partition.tryPlaceholderAllocate() != nil {
		t.Fatal("allocation should not have matched placeholder")
	}
	for id, ph := range phs {
		assert.Assert(t, ph.IsReleased(), "placeholder %s should be released", id)
	}

	// wait for events to be processed
	err = common.WaitFor(1*time.Millisecond, 10*time.Millisecond, func() bool {
		fmt.Printf("checking event length: %d\n", cache.Store.CountStoredEvents())
		return cache.Store.CountStoredEvents() == phCount
	})
	assert.NilError(t, err, "the events should have been processed")

	records := cache.Store.CollectEvents()
	if records == nil {
		t.Fatal("collecting eventChannel should return something")
	}
	assert.Equal(t, phCount, len(records), "expecting %d events for placeholder mismatch", phCount)
	assertUserGroupResource(t, getTestUserGroup(), tgRes)

	// release placeholders: do what the context would do after the shim processing
	for id, ph := range phs {
		assert.Assert(t, ph.IsReleased(), "placeholder %s should be released", id)
		release := &si.AllocationRelease{
			PartitionName:   ph.GetPartitionName(),
			ApplicationID:   appID1,
			UUID:            ph.GetUUID(),
			TerminationType: si.TerminationType_TIMEOUT,
		}
		released, _ := partition.removeAllocation(release)
		assert.Equal(t, 0, len(released), "expected no releases")
	}
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "nothing should be allocated on node")
	assert.Assert(t, resources.IsZero(app.GetQueue().GetAllocatedResource()), "nothing should be allocated on queue")
	assert.Equal(t, 0, partition.GetTotalAllocationCount(), "no allocation should be registered on the partition")
	assertUserGroupResource(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0, "second": 0}))
}

func TestPlaceholderBiggerThanReal(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	if alloc := partition.tryPlaceholderAllocate(); alloc != nil {
		t.Fatalf("empty cluster placeholder allocate returned allocation: %s", alloc)
	}

	tgRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 5})
	phRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 2})
	smallRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 1})

	// add node to allow allocation
	node := setupNode(t, nodeID1, partition, tgRes)

	// add the app with placeholder request
	app := newApplicationTG(appID1, "default", "root.default", tgRes)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-1 should have been added to the partition")
	// add an ask for a placeholder and allocate
	ask := newAllocationAskTG(phID, appID1, taskGroup, phRes, true)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add placeholder ask ph-1 to app")
	// try to allocate a placeholder via normal allocate
	ph := partition.tryAllocate()
	if ph == nil {
		t.Fatal("expected placeholder ph-1 to be allocated")
	}
	assert.Equal(t, phID, ph.GetAllocationKey(), "expected allocation of ph-1 to be returned")
	assert.Assert(t, resources.Equals(phRes, app.GetQueue().GetAllocatedResource()), "placeholder size should be allocated on queue")
	assert.Assert(t, resources.Equals(phRes, node.GetAllocatedResource()), "placeholder size should be allocated on node")
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "placeholder allocation should be registered on the partition")
	assertUserGroupResource(t, getTestUserGroup(), phRes)

	// add a new ask with smaller request and allocate
	ask = newAllocationAskTG(allocID, appID1, taskGroup, smallRes, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app with correct TG")
	alloc := partition.tryPlaceholderAllocate()
	if alloc == nil {
		t.Fatal("allocation should have matched placeholder")
	}
	assert.Equal(t, objects.Replaced, alloc.GetResult(), "expected replacement result to be returned")
	assert.Equal(t, 1, alloc.GetReleaseCount(), "placeholder should have been linked")
	// no updates yet on queue and node
	assert.Assert(t, resources.Equals(phRes, app.GetQueue().GetAllocatedResource()), "placeholder size should still be allocated on queue")
	assert.Assert(t, resources.Equals(phRes, node.GetAllocatedResource()), "placeholder size should still be allocated on node")
	assertUserGroupResource(t, getTestUserGroup(), phRes)

	// replace the placeholder: do what the context would do after the shim processing
	release := &si.AllocationRelease{
		PartitionName:   ph.GetPartitionName(),
		ApplicationID:   appID1,
		UUID:            ph.GetUUID(),
		TerminationType: si.TerminationType_PLACEHOLDER_REPLACED,
	}
	released, confirmed := partition.removeAllocation(release)
	assert.Equal(t, 0, len(released), "no allocation should be released")
	if confirmed == nil {
		t.Fatal("one allocation should be confirmed")
	}
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "real allocation should be registered on the partition")
	assert.Assert(t, resources.Equals(smallRes, app.GetQueue().GetAllocatedResource()), "real size should be allocated on queue")
	assert.Assert(t, resources.Equals(smallRes, node.GetAllocatedResource()), "real size should be allocated on node")
	assertUserGroupResource(t, getTestUserGroup(), smallRes)
}

func TestPlaceholderMatch(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	tgRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})
	phRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 2})

	// add node to allow allocation
	setupNode(t, nodeID1, partition, tgRes)

	// add the app with placeholder request
	app := newApplicationTG(appID1, "default", "root.default", tgRes)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-1 should have been added to the partition")
	// add an ask for a placeholder and allocate
	ask := newAllocationAskTG(phID, appID1, taskGroup, phRes, true)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add placeholder ask ph-1 to app")
	// try to allocate a placeholder via normal allocate
	ph := partition.tryAllocate()
	if ph == nil {
		t.Fatal("expected placeholder ph-1 to be allocated")
	}
	phUUID := ph.GetUUID()
	assert.Equal(t, phID, ph.GetAllocationKey(), "expected allocation of ph-1 to be returned")
	assert.Equal(t, 1, len(app.GetAllPlaceholderData()), "placeholder data should be created on allocate")
	assertUserGroupResource(t, getTestUserGroup(), phRes)

	// add a new ask with an unknown task group (should allocate directly)
	ask = newAllocationAskTG(allocID, appID1, "unknown", phRes, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-2 to app")
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("expected ask to be allocated (unmatched task group)")
	}
	assert.Equal(t, allocID, alloc.GetAllocationKey(), "expected allocation of alloc-1 to be returned")
	assert.Equal(t, 1, len(app.GetAllPlaceholderData()), "placeholder data should not be updated")
	assert.Equal(t, int64(1), app.GetAllPlaceholderData()[0].Count, "placeholder data should show 1 available placeholder")
	assert.Equal(t, int64(0), app.GetAllPlaceholderData()[0].Replaced, "placeholder data should show no replacements")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(phRes, 2))

	// add a new ask the same task group as the placeholder
	ask = newAllocationAskTG(allocID2, appID1, taskGroup, phRes, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-2 to app")
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("expected ask not to be allocated (matched task group)")
	}
	assert.Equal(t, 1, len(app.GetAllPlaceholderData()), "placeholder data should not be updated")
	assert.Equal(t, int64(1), app.GetAllPlaceholderData()[0].Count, "placeholder data should show 1 available placeholder")
	assert.Equal(t, int64(0), app.GetAllPlaceholderData()[0].Replaced, "placeholder data should show no replacements")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(phRes, 2))

	// replace the placeholder should work
	alloc = partition.tryPlaceholderAllocate()
	if alloc == nil {
		t.Fatal("allocation should have matched placeholder")
	}
	assert.Equal(t, allocID2, alloc.GetAllocationKey(), "expected allocation of alloc-2 to be returned")
	assert.Equal(t, int64(1), app.GetAllPlaceholderData()[0].Count, "placeholder data should show 1 available placeholder")
	assert.Equal(t, int64(0), app.GetAllPlaceholderData()[0].Replaced, "placeholder data should show no replacements yet")
	// release placeholder: do what the context would do after the shim processing
	release := &si.AllocationRelease{
		PartitionName:   "test",
		ApplicationID:   appID1,
		UUID:            phUUID,
		TerminationType: si.TerminationType_PLACEHOLDER_REPLACED,
	}
	released, confirmed := partition.removeAllocation(release)
	assert.Equal(t, len(released), 0, "not expecting any released allocations")
	if confirmed == nil {
		t.Fatal("confirmed allocation should not be nil")
	}
	assert.Equal(t, int64(1), app.GetAllPlaceholderData()[0].Replaced, "placeholder data should show the replacement")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(phRes, 2))

	// add a new ask the same task group as the placeholder
	// all placeholders are used so direct allocation is expected
	ask = newAllocationAskTG("no_ph_used", appID1, taskGroup, phRes, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask no_ph_used to app")
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("expected ask to be allocated no placeholders left")
	}
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(phRes, 3))
}

// simple direct replace with one node
func TestTryPlaceholderAllocate(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	if alloc := partition.tryPlaceholderAllocate(); alloc != nil {
		t.Fatalf("empty cluster placeholder allocate returned allocation: %s", alloc)
	}

	var tgRes, res *resources.Resource
	tgRes, err = resources.NewResourceFromConf(map[string]string{"vcore": "10"})
	assert.NilError(t, err, "failed to create resource")
	res, err = resources.NewResourceFromConf(map[string]string{"vcore": "1"})
	assert.NilError(t, err, "failed to create resource")

	// add node to allow allocation
	node := setupNode(t, nodeID1, partition, tgRes)

	// add the app with placeholder request
	app := newApplicationTG(appID1, "default", "root.default", tgRes)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-1 should have been added to the partition")
	// add an ask for a placeholder and allocate
	ask := newAllocationAskTG(phID, appID1, taskGroup, res, true)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add placeholder ask ph-1 to app")
	// try to allocate placeholder should just return
	alloc := partition.tryPlaceholderAllocate()
	if alloc != nil {
		t.Fatalf("placeholder ask should not be allocated: %s", alloc)
	}
	assertUserGroupResource(t, getTestUserGroup(), nil)
	// try to allocate a placeholder via normal allocate
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("expected first placeholder to be allocated")
	}
	assert.Equal(t, node.GetAllocation(alloc.GetUUID()), alloc, "placeholder allocation not found on node")
	assert.Assert(t, alloc.IsPlaceholder(), "placeholder alloc should return a placeholder allocation")
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "placeholder alloc should return an allocated result")
	if !resources.Equals(app.GetPlaceholderResource(), res) {
		t.Fatalf("placeholder allocation not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), res)
	}
	assert.Equal(t, partition.GetTotalAllocationCount(), 1, "placeholder allocation should be counted as alloc")
	assertUserGroupResource(t, getTestUserGroup(), res)
	// add a second ph ask and run it again it should not match the already allocated placeholder
	ask = newAllocationAskTG("ph-2", appID1, taskGroup, res, true)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add placeholder ask ph-2 to app")
	// try to allocate placeholder should just return
	alloc = partition.tryPlaceholderAllocate()
	if alloc != nil {
		t.Fatalf("placeholder ask should not be allocated: %s", alloc)
	}
	assertUserGroupResource(t, getTestUserGroup(), res)
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("expected 2nd placeholder to be allocated")
	}
	assert.Equal(t, node.GetAllocation(alloc.GetUUID()), alloc, "placeholder allocation 2 not found on node")
	if !resources.Equals(app.GetPlaceholderResource(), resources.Multiply(res, 2)) {
		t.Fatalf("placeholder allocation not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), resources.Multiply(res, 2))
	}
	assert.Equal(t, partition.GetTotalAllocationCount(), 2, "placeholder allocation should be counted as alloc")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	// not mapping to the same taskgroup should not do anything
	ask = newAllocationAskTG(allocID, appID1, "tg-unk", res, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app")
	alloc = partition.tryPlaceholderAllocate()
	if alloc != nil {
		t.Fatalf("allocation should not have matched placeholder: %s", alloc)
	}
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))

	// add an ask with the TG
	ask = newAllocationAskTG(allocID2, appID1, taskGroup, res, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-2 to app with correct TG")
	alloc = partition.tryPlaceholderAllocate()
	if alloc == nil {
		t.Fatal("allocation should have matched placeholder")
	}
	assert.Equal(t, partition.GetTotalAllocationCount(), 2, "placeholder replacement should not be counted as alloc")
	assert.Equal(t, alloc.GetResult(), objects.Replaced, "result is not the expected allocated replaced")
	assert.Equal(t, alloc.GetReleaseCount(), 1, "released allocations should have been 1")
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
	phUUID := alloc.GetFirstRelease().GetUUID()
	// placeholder is not released until confirmed by the shim
	if !resources.Equals(app.GetPlaceholderResource(), resources.Multiply(res, 2)) {
		t.Fatalf("placeholder allocation not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), resources.Multiply(res, 2))
	}
	assert.Assert(t, resources.IsZero(app.GetAllocatedResource()), "allocated resources should still be zero")
	// release placeholder: do what the context would do after the shim processing
	release := &si.AllocationRelease{
		PartitionName:   "test",
		ApplicationID:   appID1,
		UUID:            phUUID,
		TerminationType: si.TerminationType_PLACEHOLDER_REPLACED,
	}
	released, confirmed := partition.removeAllocation(release)
	assert.Equal(t, partition.GetTotalAllocationCount(), 2, "still should have 2 allocation after 1 placeholder release")
	assert.Equal(t, len(released), 0, "not expecting any released allocations")
	if confirmed == nil {
		t.Fatal("confirmed allocation should not be nil")
	}
	assert.Equal(t, confirmed.GetUUID(), alloc.GetUUID(), "confirmed allocation has unexpected uuid")
	if !resources.Equals(app.GetPlaceholderResource(), res) {
		t.Fatalf("placeholder allocations not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), res)
	}
	if !resources.Equals(app.GetAllocatedResource(), res) {
		t.Fatalf("allocations not updated as expected: got %s, expected %s", app.GetAllocatedResource(), res)
	}
	assertUserGroupResource(t, getTestUserGroup(), resources.Multiply(res, 2))
}

// The failure is triggered by the predicate plugin and is hidden in the alloc handling
func TestFailReplacePlaceholder(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	if alloc := partition.tryPlaceholderAllocate(); alloc != nil {
		t.Fatalf("empty cluster placeholder allocate returned allocation: %s", alloc)
	}
	// plugin to let the pre check fail on node-1 only, means we cannot replace the placeholder
	plugin := newFakePredicatePlugin(false, map[string]int{nodeID1: -1})
	plugins.RegisterSchedulerPlugin(plugin)
	var tgRes, res *resources.Resource
	tgRes, err = resources.NewResourceFromConf(map[string]string{"vcore": "10"})
	assert.NilError(t, err, "failed to create resource")
	res, err = resources.NewResourceFromConf(map[string]string{"vcore": "1"})
	assert.NilError(t, err, "failed to create resource")

	// add node to allow allocation
	node := setupNode(t, nodeID1, partition, tgRes)

	// add the app with placeholder request
	app := newApplicationTG(appID1, "default", "root.default", tgRes)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-1 should have been added to the partition")
	// add an ask for a placeholder and allocate
	ask := newAllocationAskTG(phID, appID1, taskGroup, res, true)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add placeholder ask ph-1 to app")

	// try to allocate a placeholder via normal allocate
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("expected first placeholder to be allocated")
	}
	assert.Equal(t, partition.GetTotalAllocationCount(), 1, "placeholder allocation should be counted as alloc")
	assert.Equal(t, node.GetAllocation(alloc.GetUUID()), alloc, "placeholder allocation not found on node")
	assert.Assert(t, alloc.IsPlaceholder(), "placeholder alloc should return a placeholder allocation")
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "placeholder alloc should return an allocated result")
	assert.Equal(t, alloc.GetNodeID(), nodeID1, "should be allocated on node-1")
	if !resources.Equals(app.GetPlaceholderResource(), res) {
		t.Fatalf("placeholder allocation not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), res)
	}
	assertUserGroupResource(t, getTestUserGroup(), res)

	// add 2nd node to allow allocation
	node2 := setupNode(t, nodeID2, partition, tgRes)
	assertUserGroupResource(t, getTestUserGroup(), res)
	// add an ask with the TG
	ask = newAllocationAskTG(allocID, appID1, taskGroup, res, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app with correct TG")
	alloc = partition.tryPlaceholderAllocate()
	if alloc == nil {
		t.Fatal("allocation should have matched placeholder")
	}
	assert.Equal(t, partition.GetTotalAllocationCount(), 1, "placeholder replacement should not be counted as alloc")
	assert.Equal(t, alloc.GetResult(), objects.Replaced, "result is not the expected allocated replaced")
	assert.Equal(t, alloc.GetReleaseCount(), 1, "released allocations should have been 1")
	// allocation must be added as it is on a different node
	assert.Equal(t, alloc.GetNodeID(), nodeID2, "should be allocated on node-2")
	assert.Assert(t, resources.IsZero(app.GetAllocatedResource()), "allocated resources should be zero")
	if !resources.Equals(node.GetAllocatedResource(), res) {
		t.Fatalf("node-1 allocation not updated as expected: got %s, expected %s", node.GetAllocatedResource(), res)
	}
	if !resources.Equals(node2.GetAllocatedResource(), res) {
		t.Fatalf("node-2 allocation not updated as expected: got %s, expected %s", node2.GetAllocatedResource(), res)
	}

	phUUID := alloc.GetFirstRelease().GetUUID()
	// placeholder is not released until confirmed by the shim
	if !resources.Equals(app.GetPlaceholderResource(), res) {
		t.Fatalf("placeholder allocation not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), resources.Multiply(res, 2))
	}
	assertUserGroupResource(t, getTestUserGroup(), res)

	// release placeholder: do what the context would do after the shim processing
	release := &si.AllocationRelease{
		PartitionName:   "test",
		ApplicationID:   appID1,
		UUID:            phUUID,
		TerminationType: si.TerminationType_PLACEHOLDER_REPLACED,
	}
	released, confirmed := partition.removeAllocation(release)
	assert.Equal(t, partition.GetTotalAllocationCount(), 1, "still should have 1 allocation after placeholder release")
	assert.Equal(t, len(released), 0, "not expecting any released allocations")
	if confirmed == nil {
		t.Fatal("confirmed allocation should not be nil")
	}
	assert.Equal(t, confirmed.GetUUID(), alloc.GetUUID(), "confirmed allocation has unexpected uuid")
	assert.Assert(t, resources.IsZero(app.GetPlaceholderResource()), "placeholder resources should be zero")
	if !resources.Equals(app.GetAllocatedResource(), res) {
		t.Fatalf("allocations not updated as expected: got %s, expected %s", app.GetAllocatedResource(), res)
	}
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "node-1 allocated resources should be zero")
	if !resources.Equals(node2.GetAllocatedResource(), res) {
		t.Fatalf("node-2 allocations not set as expected: got %s, expected %s", node2.GetAllocatedResource(), res)
	}
	assertUserGroupResource(t, getTestUserGroup(), res)
}

func TestAddAllocationAsk(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	err = partition.addAllocationAsk(nil)
	assert.NilError(t, err, "nil ask should not return an error")
	err = partition.addAllocationAsk(&si.AllocationAsk{})
	if err == nil {
		t.Fatal("empty ask should have returned application not found error")
	}

	// add the app to add an ask to
	app := newApplication(appID1, "default", "root.default")
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-1 should have been added to the partition")
	// a simple ask (no repeat should fail)
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"vcore": "10"})
	assert.NilError(t, err, "failed to create resource")
	askKey := "ask-key-1"
	ask := si.AllocationAsk{
		AllocationKey:  askKey,
		ApplicationID:  appID1,
		ResourceAsk:    res.ToProto(),
		MaxAllocations: 0,
	}
	err = partition.addAllocationAsk(&ask)
	if err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("0 repeat ask should have returned invalid ask error: %v", err)
	}

	// set the repeat and retry this should work
	ask.MaxAllocations = 1
	err = partition.addAllocationAsk(&ask)
	assert.NilError(t, err, "failed to add ask to app")
	if !resources.Equals(app.GetPendingResource(), res) {
		t.Fatal("app not updated by adding ask, no error thrown")
	}
	assertUserGroupResource(t, getTestUserGroup(), nil)
}

func TestRemoveAllocationAsk(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	// add the app
	app := newApplication(appID1, "default", "root.default")
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-1 should have been added to the partition")
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"vcore": "10"})
	assert.NilError(t, err, "failed to create resource")
	askKey := "ask-key-1"
	ask := newAllocationAsk(askKey, appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to ask to application")

	// we should not panic on nil
	partition.removeAllocationAsk(nil)
	// we don't care about the partition name as we test just the partition code
	release := &si.AllocationAskRelease{
		ApplicationID:   "fake-app",
		AllocationKey:   askKey,
		TerminationType: si.TerminationType_STOPPED_BY_RM,
	}
	// unknown app should do nothing
	partition.removeAllocationAsk(release)
	if !resources.Equals(app.GetPendingResource(), res) {
		t.Fatal("wrong app updated removing ask")
	}

	// known app, unknown ask no change
	release.ApplicationID = appID1
	release.AllocationKey = "fake"
	partition.removeAllocationAsk(release)
	if !resources.Equals(app.GetPendingResource(), res) {
		t.Fatal("app updated removing unknown ask")
	}

	// known app, known ask, ignore timeout as it originates in the core
	release.AllocationKey = askKey
	release.TerminationType = si.TerminationType_TIMEOUT
	partition.removeAllocationAsk(release)
	if !resources.Equals(app.GetPendingResource(), res) {
		t.Fatal("app updated removing timed out ask, should not have changed")
	}

	// correct remove of a known ask
	release.TerminationType = si.TerminationType_STOPPED_BY_RM
	partition.removeAllocationAsk(release)
	assert.Assert(t, resources.IsZero(app.GetPendingResource()), "app should not have pending asks")
	assertUserGroupResource(t, getTestUserGroup(), nil)
}

func TestUpdateNodeSortingPolicy(t *testing.T) {
	partition, err := newBasePartition()
	if err != nil {
		t.Errorf("Partition creation failed: %s", err.Error())
	}

	if partition.nodes.GetNodeSortingPolicy().PolicyType().String() != policies.FairnessPolicy.String() {
		t.Error("Node policy is not set with the default policy which is fair policy.")
	}

	var tests = []struct {
		name  string
		input string
		want  string
	}{
		{"Set binpacking policy", policies.BinPackingPolicy.String(), policies.BinPackingPolicy.String()},
		{"Set fair policy", policies.FairnessPolicy.String(), policies.FairnessPolicy.String()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partition.updateNodeSortingPolicy(configs.PartitionConfig{
				Name: "test",
				Queues: []configs.QueueConfig{
					{
						Name:      "root",
						Parent:    true,
						SubmitACL: "*",
						Queues: []configs.QueueConfig{
							{
								Name:   "default",
								Parent: false,
								Queues: nil,
							},
						},
					},
				},
				PlacementRules: nil,
				Limits:         nil,
				Preemption:     configs.PartitionPreemptionConfig{},
				NodeSortPolicy: configs.NodeSortingPolicy{Type: tt.input},
			})

			ans := partition.nodes.GetNodeSortingPolicy().PolicyType().String()
			if ans != tt.want {
				t.Errorf("got %s, want %s", ans, tt.want)
			}
		})
	}
}

func TestUpdateStateDumpFilePath(t *testing.T) {
	partition, err := newBasePartition()
	partition.RLock()
	defer partition.RUnlock()
	assert.NilError(t, err, "partition create failed")

	assert.Equal(t, partition.GetStateDumpFilePath(), "")
	stateDumpFilePath := "yunikorn-state.txt"

	partition.updateStateDumpFilePath(configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:   "default",
						Parent: false,
						Queues: nil,
					},
				},
			},
		},
		PlacementRules:    nil,
		Limits:            nil,
		Preemption:        configs.PartitionPreemptionConfig{},
		NodeSortPolicy:    configs.NodeSortingPolicy{},
		StateDumpFilePath: stateDumpFilePath,
	})

	assert.Equal(t, partition.GetStateDumpFilePath(), stateDumpFilePath)
}

// A Test Case of get function in object/node_cellection
func TestGetNodeSortingPolicyWhenNewPartitionFromConfig(t *testing.T) {
	var tests = []struct {
		name  string
		input string
		want  string
	}{
		{"Default policy", "", policies.FairnessPolicy.String()},
		{"Fair policy", policies.FairnessPolicy.String(), policies.FairnessPolicy.String()},
		{"Binpacking policy", policies.BinPackingPolicy.String(), policies.BinPackingPolicy.String()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := configs.PartitionConfig{
				Name: "test",
				Queues: []configs.QueueConfig{
					{
						Name:      "root",
						Parent:    true,
						SubmitACL: "*",
						Queues: []configs.QueueConfig{
							{
								Name:   "default",
								Parent: false,
								Queues: nil,
							},
						},
					},
				},
				PlacementRules: nil,
				Limits:         nil,
				Preemption:     configs.PartitionPreemptionConfig{},
				NodeSortPolicy: configs.NodeSortingPolicy{
					Type: tt.input,
				},
			}

			p, err := newPartitionContext(conf, rmID, nil)
			if err != nil {
				t.Errorf("Partition creation fail: %s", err.Error())
			}

			ans := p.nodes.GetNodeSortingPolicy().PolicyType().String()
			if ans != tt.want {
				t.Errorf("got %s, want %s", ans, tt.want)
			}
		})
	}
}

func TestTryAllocateMaxRunning(t *testing.T) {
	const resType = "vcore"
	partition := createQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %v", alloc.String())
	}

	// set max running apps
	root := partition.getQueueInternal("root")
	root.SetMaxRunningApps(2)
	parent := partition.getQueueInternal("root.parent")
	parent.SetMaxRunningApps(1)

	// add first app to the partition
	appRes, err := resources.NewResourceFromConf(map[string]string{resType: "2"})
	assert.NilError(t, err, "app resource creation failed")
	app := newApplicationTG(appID1, "default", "root.parent.sub-leaf", appRes)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-1 to partition")
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{resType: "1"})
	assert.NilError(t, err, "failed to create resource")
	err = app.AddAllocationAsk(newAllocationAskTG(allocID, appID1, "ph1", res, true))
	assert.NilError(t, err, "failed to add ask alloc-1 to app-1")
	// first allocation should move the app to accepted
	assert.Equal(t, app.CurrentState(), objects.Accepted.String(), "application should have moved to accepted state")

	// allocate should work: app stays in accepted state (placeholder!)
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), allocID, "expected ask alloc to be allocated")
	assert.Equal(t, app.CurrentState(), objects.Accepted.String(), "application should have moved to accepted state")

	// add second app to the partition
	app2 := newApplication(appID2, "default", "root.parent.sub-leaf")
	err = partition.AddApplication(app2)
	assert.NilError(t, err, "failed to add app-2 to partition")
	err = app2.AddAllocationAsk(newAllocationAsk(allocID, appID2, res))
	assert.NilError(t, err, "failed to add ask alloc-1 to app-2")

	// allocation should fail max running app is reached on parent via accepted allocating
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("allocation should not have returned as parent limit is reached")
	}

	// allocate should work: app moves to Starting all placeholder allocated
	err = app.AddAllocationAsk(newAllocationAskTG("alloc-2", appID1, "ph1", res, true))
	assert.NilError(t, err, "failed to add ask alloc-2 to app-1")
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), "alloc-2", "expected ask alloc-2 to be allocated")
	assert.Equal(t, app.CurrentState(), objects.Starting.String(), "application should have moved to starting state")

	// allocation should still fail: max running apps on parent reached
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("allocation should not have returned as parent limit is reached")
	}

	// update the parent queue max running
	parent.SetMaxRunningApps(2)
	// allocation works
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID2, "expected application app-2 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), allocID, "expected ask alloc-1 to be allocated")
}
