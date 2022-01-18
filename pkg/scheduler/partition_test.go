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

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/events"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

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
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	q := partition.GetQueue(defQueue)
	if q == nil {
		t.Fatal("expected default queue not found")
	}

	// add a new app
	app := newApplication(appID1, "default", defQueue)
	appRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1})
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	// add a node with allocations
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 10})
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
}

func TestRemoveNode(t *testing.T) {
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
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app
	app := newApplication(appID1, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	// add a node with allocations: must have the correct app added already
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000})
	node := newNodeMaxResource(nodeID1, nodeRes)
	appRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1})
	ask := newAllocationAsk("alloc-1", appID1, appRes)
	allocUUID := "alloc-1-uuid"
	alloc := objects.NewAllocation(allocUUID, nodeID1, ask)
	allocs := []*objects.Allocation{alloc}
	err = partition.AddNode(node, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")
	// get what was allocated
	allocated := node.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly")

	// add broken allocations
	ask = newAllocationAsk("alloc-na", "not-an-app", appRes)
	alloc = objects.NewAllocation("alloc-na-uuid", nodeID1, ask)
	node.AddAllocation(alloc)
	ask = newAllocationAsk("alloc-2", appID1, appRes)
	alloc = objects.NewAllocation("alloc-2-uuid", nodeID1, ask)
	node.AddAllocation(alloc)

	// remove the node this cannot fail
	released, confirmed := partition.removeNode(nodeID1)
	assert.Equal(t, 0, partition.GetTotalNodeCount(), "node list was not updated, node was not removed")
	assert.Equal(t, 1, len(released), "node did not release correct allocation")
	assert.Equal(t, 0, len(confirmed), "node did not confirm correct allocation")
	assert.Equal(t, released[0].UUID, allocUUID, "UUID returned by release not the same as on allocation")
}

// test with a replacement of a placeholder: placeholder and real on the same node that gets removed
func TestRemoveNodeWithPlaceholders(t *testing.T) {
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

	// fake an ask that is used
	ask = newAllocationAskAll(allocID, appID1, taskGroup, appRes, 1, 1, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should be added to app")
	_, err = app.UpdateAskRepeat(allocID, -1)
	assert.NilError(t, err, "ask should have been updated without error")
	assert.Assert(t, resources.IsZero(app.GetPendingResource()), "app should not have pending resources")

	// add real allocation that is replacing the placeholder
	alloc := objects.NewAllocation(allocID, nodeID1, ask)
	alloc.Releases = []*objects.Allocation{ph}
	// double link as if the replacement is ongoing
	ph.Releases = []*objects.Allocation{alloc}
	alloc.Result = objects.Replaced

	allocs = app.GetAllAllocations()
	assert.Equal(t, len(allocs), 1, "expected one allocation for the app (placeholder)")

	// remove the node that has both placeholder and real allocation
	released, confirmed := partition.removeNode(nodeID1)
	assert.Equal(t, 0, partition.GetTotalNodeCount(), "node list was not updated, node was not removed")
	assert.Equal(t, 1, len(released), "node removal did not release correct allocation")
	assert.Equal(t, 0, len(confirmed), "node removal should not have confirmed allocation")
	assert.Equal(t, phID, released[0].UUID, "UUID returned by release not the same as the placeholder")
	allocs = app.GetAllAllocations()
	assert.Equal(t, 0, len(allocs), "expected no allocations for the app")
	assert.Assert(t, resources.Equals(app.GetPendingResource(), appRes), "app should have updated pending resources")
	// check the interim state of the placeholder involved
	assert.Equal(t, 0, len(ph.Releases), "placeholder should have no releases linked anymore")
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
	assert.Assert(t, resources.Equals(node1.GetAllocatedResource(), appRes), "allocation not added correctly to node1")

	node2 := newNodeMaxResource(nodeID2, nodeRes)
	err = partition.AddNode(node2, nil)
	assert.NilError(t, err, "add node2 to partition should not have failed")
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
	alloc.Releases = []*objects.Allocation{ph}
	alloc.Result = objects.Replaced
	node2.AddAllocation(alloc)
	allocated = node2.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node2")
	assert.Assert(t, resources.Equals(node2.GetAllocatedResource(), appRes), "allocation not added correctly to node2 (resource count)")

	// double link as if the replacement is ongoing
	ph.Releases = []*objects.Allocation{alloc}

	allocs = app.GetAllAllocations()
	assert.Equal(t, len(allocs), 1, "expected one allocation for the app (placeholder)")

	// remove the node with the placeholder
	released, confirmed := partition.removeNode(nodeID1)
	assert.Equal(t, 1, partition.GetTotalNodeCount(), "node list was not updated, node was not removed")
	assert.Equal(t, 1, len(node2.GetAllAllocations()), "remaining node should have allocation")
	assert.Equal(t, 1, len(released), "node removal did not release correct allocation")
	assert.Equal(t, 1, len(confirmed), "node removal did not confirm correct allocation")
	assert.Equal(t, phID, released[0].UUID, "UUID returned by release not the same as the placeholder")
	assert.Equal(t, allocID, confirmed[0].UUID, "UUID returned by confirmed not the same as the real allocation")
	assert.Assert(t, resources.IsZero(app.GetPendingResource()), "app should not have pending resources")
	allocs = app.GetAllAllocations()
	assert.Equal(t, 1, len(allocs), "expected one allocation for the app (real)")
	assert.Equal(t, allocID, allocs[0].UUID, "UUID for the app is not the same as the real allocation")
	assert.Equal(t, objects.Allocated, allocs[0].Result, "allocation state should be allocated")
	assert.Equal(t, 0, len(allocs[0].Releases), "real allocation should have no releases linked anymore")
}

// test with a replacement of a placeholder: real on the removed node placeholder on the 2nd node
func TestRemoveNodeWithReal(t *testing.T) {
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

	node2 := newNodeMaxResource(nodeID2, nodeRes)
	err = partition.AddNode(node2, nil)
	assert.NilError(t, err, "add node2 to partition should not have failed")
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
	alloc.Releases = []*objects.Allocation{ph}
	alloc.Result = objects.Replaced
	node2.AddAllocation(alloc)
	allocated = node2.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node2")
	assert.Assert(t, resources.Equals(node2.GetAllocatedResource(), appRes), "allocation not added correctly to node2 (resource count)")

	// double link as if the replacement is ongoing
	ph.Releases = []*objects.Allocation{alloc}

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
	assert.Equal(t, phID, allocs[0].UUID, "UUID for the app is not the same as the real allocation")
	assert.Equal(t, 0, len(ph.Releases), "no inflight replacements linked")
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
	task := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 10})
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
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app that will just sit around to make sure we remove the right one
	appNotRemoved := "will_not_remove"
	app := newApplication(appNotRemoved, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")
	// add a node to allow adding an allocation
	node1 := newNodeMaxResource(nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000}))
	// add a node this must work
	err = partition.AddNode(node1, nil)
	assert.NilError(t, err, "add node to partition should not have failed")
	if partition.GetNode(nodeID1) == nil {
		t.Fatalf("node not added to partition as expected (node nil)")
	}

	appRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1})
	ask := newAllocationAsk("alloc-nr", appNotRemoved, appRes)
	uuid := "alloc-nr-uuid"
	alloc := objects.NewAllocation(uuid, nodeID1, ask)
	err = partition.addAllocation(alloc)
	assert.NilError(t, err, "add allocation to partition should not have failed")

	allocs := partition.removeApplication("does_not_exist")
	if allocs != nil {
		t.Errorf("non existing application returned unexpected values: allocs = %v", allocs)
	}

	// add another new app
	app = newApplication(appID1, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	// remove the newly added app (no allocations)
	allocs = partition.removeApplication(appID1)
	assert.Equal(t, 0, len(allocs), "existing application without allocations returned allocations %v", allocs)
	assert.Equal(t, 1, len(partition.applications), "existing application was not removed")

	// add the application again and then an allocation
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	ask = newAllocationAsk("alloc-1", appID1, appRes)
	alloc = objects.NewAllocation("alloc-1-uuid", nodeID1, ask)
	err = partition.addAllocation(alloc)
	assert.NilError(t, err, "add allocation to partition should not have failed")

	// remove the newly added app
	allocs = partition.removeApplication(appID1)
	assert.Equal(t, 1, len(allocs), "existing application with allocations returned unexpected allocations %v", allocs)
	assert.Equal(t, 1, len(partition.applications), "existing application was not removed")
	if partition.GetTotalAllocationCount() != 1 {
		t.Errorf("allocation that should have been left was removed")
	}
}

func TestRemoveAppAllocs(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app that will just sit around to make sure we remove the right one
	appNotRemoved := "will_not_remove"
	app := newApplication(appNotRemoved, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")
	// add a node to allow adding an allocation
	node1 := newNodeMaxResource(nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000}))
	// add a node this must work
	err = partition.AddNode(node1, nil)
	assert.NilError(t, err, "add node to partition should not have failed")
	if partition.GetNode(nodeID1) == nil {
		t.Fatalf("node not added to partition as expected (node nil)")
	}
	appRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1})
	ask := newAllocationAsk("alloc-nr", appNotRemoved, appRes)
	alloc := objects.NewAllocation("alloc-nr-uuid", nodeID1, ask)
	err = partition.addAllocation(alloc)
	assert.NilError(t, err, "add allocation to partition should not have failed")

	ask = newAllocationAsk("alloc-1", appNotRemoved, appRes)
	uuid := "alloc-1-uuid"
	alloc = objects.NewAllocation(uuid, nodeID1, ask)
	err = partition.addAllocation(alloc)
	assert.NilError(t, err, "add allocation to partition should not have failed")
	release := &si.AllocationRelease{
		PartitionName:   "default",
		ApplicationID:   "",
		UUID:            "",
		TerminationType: si.TerminationType_STOPPED_BY_RM,
	}

	allocs, _ := partition.removeAllocation(release)
	assert.Equal(t, 0, len(allocs), "empty removal request returned allocations: %v", allocs)
	// create a new release without app: should just return
	release.ApplicationID = "does_not_exist"
	allocs, _ = partition.removeAllocation(release)
	assert.Equal(t, 0, len(allocs), "removal request for non existing application returned allocations: %v", allocs)
	// create a new release with app, non existing allocation: should just return
	release.ApplicationID = appNotRemoved
	release.UUID = "does_not_exist"
	allocs, _ = partition.removeAllocation(release)
	assert.Equal(t, 0, len(allocs), "removal request for non existing allocation returned allocations: %v", allocs)
	// create a new release with app, existing allocation: should return 1 alloc
	assert.Equal(t, 2, partition.GetTotalAllocationCount(), "pre-remove allocation list incorrect: %v", partition.allocations)
	release.UUID = uuid
	allocs, _ = partition.removeAllocation(release)
	assert.Equal(t, 1, len(allocs), "removal request for existing allocation returned wrong allocations: %v", allocs)
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "allocation removal requests removed more than expected: %v", partition.allocations)
	// create a new release with app, no uuid: should return last left alloc
	release.UUID = ""
	allocs, _ = partition.removeAllocation(release)
	assert.Equal(t, 1, len(allocs), "removal request for existing allocation returned wrong allocations: %v", allocs)
	assert.Equal(t, 0, partition.GetTotalAllocationCount(), "removal requests did not remove all allocations: %v", partition.allocations)
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

func TestTryAllocate(t *testing.T) {
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
	err = app.AddAllocationAsk(newAllocationAsk("alloc-1", appID1, res))
	assert.NilError(t, err, "failed to add ask alloc-1 to app-1")
	err = app.AddAllocationAsk(newAllocationAskPriority("alloc-2", appID1, res, 1, 2))
	assert.NilError(t, err, "failed to add ask alloc-2 to app-1")

	app = newApplication(appID2, "default", "root.leaf")
	// add to the partition
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-2 to partition")
	err = app.AddAllocationAsk(newAllocationAskPriority("alloc-1", appID2, res, 1, 2))
	assert.NilError(t, err, "failed to add ask alloc-1 to app-2")

	// first allocation should be app-1 and alloc-2
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.Result, objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, len(alloc.Releases), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.ApplicationID, appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.AllocationKey, "alloc-2", "expected ask alloc-2 to be allocated")

	// second allocation should be app-2 and alloc-1: higher up in the queue hierarchy
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.Result, objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, len(alloc.Releases), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.ApplicationID, appID2, "expected application app-2 to be allocated")
	assert.Equal(t, alloc.AllocationKey, "alloc-1", "expected ask alloc-1 to be allocated")

	// third allocation should be app-1 and alloc-1
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.Result, objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, len(alloc.Releases), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.ApplicationID, appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.AllocationKey, "alloc-1", "expected ask alloc-1 to be allocated")
	assert.Assert(t, resources.IsZero(partition.root.GetPendingResource()), "pending resources should be set to zero")
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
}

func TestAllocReserveNewNode(t *testing.T) {
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
	assert.Equal(t, objects.Allocated, alloc.Result, "allocation result should have been allocated")
	// the second one should be reserved as the 2nd node is not scheduling
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("2nd allocation did not return the correct allocation")
	}
	// check if updated (must be after allocate call)
	assert.Equal(t, 1, len(app.GetReservations()), "ask should have been reserved")

	// turn on 2nd node
	node2.SetSchedulable(true)
	alloc = partition.tryReservedAllocate()
	assert.Equal(t, objects.AllocatedReserved, alloc.Result, "allocation result should have been allocatedReserved")
	assert.Equal(t, "", alloc.ReservedNodeID, "reserved node should be reset after processing")
	assert.Equal(t, node2.NodeID, alloc.NodeID, "allocation should be fulfilled on new node")
	// check if all updated
	node1 := partition.GetNode(nodeID1)
	assert.Equal(t, 0, len(node1.GetReservations()), "old node should have no more reservations")
	assert.Equal(t, 0, len(app.GetReservations()), "ask should have been reserved")
}

func TestTryAllocateReserve(t *testing.T) {
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
	assert.Equal(t, alloc.Result, objects.AllocatedReserved, "result is not the expected allocated from reserved")
	assert.Equal(t, alloc.ReservedNodeID, "", "node should not be set for allocated from reserved")
	assert.Equal(t, len(alloc.Releases), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.ApplicationID, appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.AllocationKey, "alloc-2", "expected ask alloc-2 to be allocated")
	// reservations should have been removed: it is in progress
	if app.IsReservedOnNode(node2.NodeID) || len(app.GetAskReservations("alloc-2")) != 0 {
		t.Fatalf("reservation removal failure for ask2 and node2")
	}

	// no reservations left this should return nil
	alloc = partition.tryReservedAllocate()
	if alloc != nil {
		t.Fatalf("reserved allocation should not return any allocation: %s, '%s'", alloc, alloc.ReservedNodeID)
	}
	// try non reserved this should allocate
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.Result, objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, len(alloc.Releases), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.ApplicationID, appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.AllocationKey, "alloc-1", "expected ask alloc-1 to be allocated")
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
	assert.Equal(t, objects.AllocatedReserved, alloc.Result, "expected reserved allocation to be returned")
	assert.Equal(t, "", alloc.ReservedNodeID, "reserved node should be reset after processing")
	assert.Equal(t, 0, len(node2.GetReservations()), "reservation should have been removed from node")
	assert.Equal(t, false, app.IsReservedOnNode(node2.NodeID), "reservation cleanup for ask on app failed")

	// node2 is unreserved now so the next one should allocate on the 2nd node (fair sharing)
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return correct allocation")
	}
	assert.Equal(t, objects.Allocated, alloc.Result, "expected allocated allocation to be returned")
	assert.Equal(t, node2.NodeID, alloc.NodeID, "expected allocation on node2 to be returned")
}

// remove the reserved ask while allocating in flight for the ask
func TestScheduleRemoveReservedAsk(t *testing.T) {
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
		if alloc == nil || alloc.Result != objects.Allocated {
			t.Fatalf("expected allocated allocation to be returned (step %d) %s", i, alloc)
		}
	}

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

	// add a node
	node := newNodeMaxResource("node-3", res)
	err = partition.AddNode(node, nil)
	assert.NilError(t, err, "failed to add node node-3 to the partition")
	// Try to allocate one of the reservation. We go directly to the root queue not using the partition otherwise
	// we confirm before we get back in the test code and cannot remove the ask
	alloc := partition.root.TryReservedAllocate(partition.GetNodeIterator)
	if alloc == nil || alloc.Result != objects.AllocatedReserved {
		t.Fatalf("expected allocatedReserved allocation to be returned %v", alloc)
	}
	// before confirming remove the ask: do what the scheduler does when it gets a request from a
	// shim in processAllocationReleaseByAllocationKey()
	// make sure we are counting correctly and leave the other reservation intact
	removeAskID := "alloc-2"
	if alloc.AllocationKey == "alloc-3" {
		removeAskID = "alloc-3"
	}
	released := app.RemoveAllocationAsk(removeAskID)
	assert.Equal(t, released, 1, "expected one reservations to be released")
	partition.unReserveCount(appID1, released)
	assert.Equal(t, len(partition.reservedApps), 1, "partition should still have reserved app")
	assert.Equal(t, len(app.GetReservations()), 1, "application reservations should be 1")

	// now confirm the allocation: this should not remove the reservation
	rmAlloc := partition.allocate(alloc)
	assert.Equal(t, "", rmAlloc.ReservedNodeID, "reserved node should be reset after processing")
	assert.Equal(t, len(partition.reservedApps), 1, "partition should still have reserved app")
	assert.Equal(t, len(app.GetReservations()), 1, "application reservations should be kept at 1")
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
	err = app.HandleApplicationEvent(objects.CompleteApplication)
	assert.NilError(t, err, "no error expected while transitioning the app from waiting to completed state")
	err = common.WaitFor(10*time.Millisecond, time.Duration(1000)*time.Millisecond, func() bool {
		partition.RLock()
		defer partition.RUnlock()
		return len(partition.completedApplications) > 0
	})
	assert.NilError(t, err, "the completed application should have been processed")
	assert.Assert(t, len(partition.applications) == 0, "the partition should have no active app")
	assert.Assert(t, len(partition.completedApplications) == 1, "the partition should have 1 completed app")
}

func TestCleanupCompletedApps(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	completedApp := newApplication("completed", "default", defQueue)
	completedApp.SetState(objects.Completed.String())

	newApp := newApplication("running", "default", defQueue)
	err = partition.AddApplication(completedApp)
	assert.NilError(t, err, "no error expected while adding the application")
	err = partition.AddApplication(newApp)
	assert.NilError(t, err, "no error expected while adding the application")

	assert.Assert(t, len(partition.applications) == 2, "the partition should have 2 apps")
	// mark the app for removal
	completedApp.SetState(objects.Expired.String())
	partition.cleanupExpiredApps()
	assert.Assert(t, len(partition.applications) == 1, "the partition should have 1 app")
	assert.Assert(t, partition.getApplication(completedApp.ApplicationID) == nil, "completed application should have been deleted")
	assert.Assert(t, partition.getApplication(newApp.ApplicationID) != nil, "new application should still be in the partition")
	assert.Assert(t, len(partition.GetAppsByState(objects.Completed.String())) == 0, "the partition should have 0 completed app")
	assert.Assert(t, len(partition.GetAppsByState(objects.Expired.String())) == 0, "the partition should have 0 expired app")
}

func TestUpdateNode(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "test partition create failed with error")

	newRes, err := resources.NewResourceFromConf(map[string]string{"memory": "400", "vcore": "30"})
	assert.NilError(t, err, "failed to create resource")

	err = partition.AddNode(newNodeMaxResource("test", newRes), nil)
	assert.NilError(t, err, "test node add failed unexpected")
	assert.Equal(t, 1, partition.nodes.GetNodeCount(), "node list not correct")

	if !resources.Equals(newRes, partition.GetTotalPartitionResource()) {
		t.Errorf("Expected partition resource %s, doesn't match with actual partition resource %s", newRes, partition.GetTotalPartitionResource())
	}

	// delta resource for a node with mem as 450 and vcores as 40 (both mem and vcores has increased)
	delta, err := resources.NewResourceFromConf(map[string]string{"memory": "50", "vcore": "10"})
	assert.NilError(t, err, "failed to create resource")
	partition.updatePartitionResource(delta)

	expectedRes, err := resources.NewResourceFromConf(map[string]string{"memory": "450", "vcore": "40"})
	assert.NilError(t, err, "failed to create resource")

	if !resources.Equals(expectedRes, partition.GetTotalPartitionResource()) {
		t.Errorf("Expected partition resource %s, doesn't match with actual partition resource %s", expectedRes, partition.GetTotalPartitionResource())
	}

	// delta resource for a node with mem as 400 and vcores as 30 (both mem and vcores has decreased)
	delta = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": -50, "vcore": -10})
	partition.updatePartitionResource(delta)

	expectedRes, err = resources.NewResourceFromConf(map[string]string{"memory": "400", "vcore": "30"})
	assert.NilError(t, err, "failed to create resource")

	if !resources.Equals(expectedRes, partition.GetTotalPartitionResource()) {
		t.Errorf("Expected partition resource %s, doesn't match with actual partition resource %s", expectedRes, partition.GetTotalPartitionResource())
	}

	// delta resource for a node with mem as 450 and vcores as 10 (mem has increased but vcores has decreased)
	delta = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 50, "vcore": -20})
	partition.updatePartitionResource(delta)

	expectedRes, err = resources.NewResourceFromConf(map[string]string{"memory": "450", "vcore": "10"})
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

	jsonRes := "{\"resources\":{\"vcore\":{\"value\":10}}}"
	tags = map[string]string{"taskqueue": "same", objects.AppTagNamespaceResourceQuota: jsonRes}
	app = newApplicationTGTags(appID2, "default", "unknown", tgRes, tags)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-2 should have been added to the partition")
	assert.Equal(t, partition.getApplication(appID2), app, "partition failed to add app incorrect app returned")
	assert.Equal(t, app.GetQueuePath(), "root.same", "app-2 not placed in expected queue")

	jsonRes = "{\"resources\":{\"vcore\":{\"value\":1}}}"
	tags = map[string]string{"taskqueue": "smaller", objects.AppTagNamespaceResourceQuota: jsonRes}
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
	events.CreateAndSetEventCache()
	cache := events.GetEventCache()
	cache.StartService()

	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	tgRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 5})
	phRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 2})

	// add node to allow allocation
	err = partition.AddNode(newNodeMaxResource(nodeID1, tgRes), nil)
	assert.NilError(t, err, "test node-1 add failed unexpected")
	node := partition.GetNode(nodeID1)
	if node == nil {
		t.Fatal("new node was not found on the partition")
	}

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
	assert.Equal(t, phID, ph.AllocationKey, "expected allocation of ph-1 to be returned")
	assert.Assert(t, resources.Equals(phRes, app.GetQueue().GetAllocatedResource()), "placeholder size should be allocated on queue")
	assert.Assert(t, resources.Equals(phRes, node.GetAllocatedResource()), "placeholder size should be allocated on node")
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "placeholder allocation should be registered on the partition")

	// add an ask which is larger than the placeholder
	ask = newAllocationAskTG(allocID, appID1, taskGroup, tgRes, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app with correct TG")
	// allocate should trigger release of placeholder nothing else
	if partition.tryPlaceholderAllocate() != nil {
		t.Fatal("allocation should not have matched placeholder")
	}
	assert.Assert(t, ph.IsReleased(), "placeholder should be released")

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

	// release placeholder: do what the context would do after the shim processing
	release := &si.AllocationRelease{
		PartitionName:   ph.PartitionName,
		ApplicationID:   appID1,
		UUID:            ph.UUID,
		TerminationType: si.TerminationType_TIMEOUT,
	}
	released, _ := partition.removeAllocation(release)
	assert.Equal(t, 1, len(released), "one allocation should be released")
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "nothing should be allocated on node")
	assert.Assert(t, resources.IsZero(app.GetQueue().GetAllocatedResource()), "nothing should be allocated on queue")
	assert.Equal(t, 0, partition.GetTotalAllocationCount(), "no allocation should be registered on the partition")
}

// one real allocation should trigger cleanup of all placeholders
func TestPlaceholderSmallerMulti(t *testing.T) {
	events.CreateAndSetEventCache()
	cache := events.GetEventCache()
	cache.StartService()

	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	phCount := 5
	phRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 2})
	tgRes := resources.Multiply(phRes, int64(phCount))

	// add node to allow allocation
	err = partition.AddNode(newNodeMaxResource(nodeID1, tgRes), nil)
	assert.NilError(t, err, "test node-1 add failed unexpected")
	node := partition.GetNode(nodeID1)
	if node == nil {
		t.Fatal("new node was not found on the partition")
	}

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
		assert.Equal(t, id, ph.AllocationKey, "expected allocation of %s to be returned", id)
		phs[id] = ph
	}
	assert.Assert(t, resources.Equals(tgRes, app.GetQueue().GetAllocatedResource()), "all placeholders should be allocated on queue")
	assert.Assert(t, resources.Equals(tgRes, node.GetAllocatedResource()), "all placeholders should be allocated on node")
	assert.Equal(t, phCount, partition.GetTotalAllocationCount(), "placeholder allocation should be registered on the partition")

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

	// release placeholders: do what the context would do after the shim processing
	for id, ph := range phs {
		assert.Assert(t, ph.IsReleased(), "placeholder %s should be released", id)
		release := &si.AllocationRelease{
			PartitionName:   ph.PartitionName,
			ApplicationID:   appID1,
			UUID:            ph.UUID,
			TerminationType: si.TerminationType_TIMEOUT,
		}
		released, _ := partition.removeAllocation(release)
		assert.Equal(t, 1, len(released), "one allocation should be released")
	}
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "nothing should be allocated on node")
	assert.Assert(t, resources.IsZero(app.GetQueue().GetAllocatedResource()), "nothing should be allocated on queue")
	assert.Equal(t, 0, partition.GetTotalAllocationCount(), "no allocation should be registered on the partition")
}

func TestPlaceholderBiggerThanReal(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	if alloc := partition.tryPlaceholderAllocate(); alloc != nil {
		t.Fatalf("empty cluster placeholder allocate returned allocation: %s", alloc)
	}

	tgRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 5})
	phRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 2})
	smallRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 1})

	// add node to allow allocation
	err = partition.AddNode(newNodeMaxResource(nodeID1, tgRes), nil)
	assert.NilError(t, err, "test node-1 add failed unexpected")
	node := partition.GetNode(nodeID1)
	if node == nil {
		t.Fatal("new node was not found on the partition")
	}

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
	assert.Equal(t, phID, ph.AllocationKey, "expected allocation of ph-1 to be returned")
	assert.Assert(t, resources.Equals(phRes, app.GetQueue().GetAllocatedResource()), "placeholder size should be allocated on queue")
	assert.Assert(t, resources.Equals(phRes, node.GetAllocatedResource()), "placeholder size should be allocated on node")
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "placeholder allocation should be registered on the partition")
	// add a new ask with smaller request and allocate
	ask = newAllocationAskTG(allocID, appID1, taskGroup, smallRes, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app with correct TG")
	alloc := partition.tryPlaceholderAllocate()
	if alloc == nil {
		t.Fatal("allocation should have matched placeholder")
	}
	assert.Equal(t, objects.Replaced, alloc.Result, "expected replacement result to be returned")
	assert.Equal(t, 1, len(alloc.Releases), "placeholder should have been linked")
	// no updates yet on queue and node
	assert.Assert(t, resources.Equals(phRes, app.GetQueue().GetAllocatedResource()), "placeholder size should still be allocated on queue")
	assert.Assert(t, resources.Equals(phRes, node.GetAllocatedResource()), "placeholder size should still be allocated on node")

	// replace the placeholder: do what the context would do after the shim processing
	release := &si.AllocationRelease{
		PartitionName:   ph.PartitionName,
		ApplicationID:   appID1,
		UUID:            ph.UUID,
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
}

// simple direct replace with one node
func TestTryPlaceholderAllocate(t *testing.T) {
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
	err = partition.AddNode(newNodeMaxResource(nodeID1, tgRes), nil)
	assert.NilError(t, err, "test node-1 add failed unexpected")
	node := partition.GetNode(nodeID1)
	if node == nil {
		t.Fatal("new node was not found on the partition")
	}

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

	// try to allocate a placeholder via normal allocate
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("expected first placeholder to be allocated")
	}
	assert.Equal(t, node.GetAllocation(alloc.UUID), alloc, "placeholder allocation not found on node")
	assert.Assert(t, alloc.IsPlaceholder(), "placeholder alloc should return a placeholder allocation")
	assert.Equal(t, alloc.Result, objects.Allocated, "placeholder alloc should return an allocated result")
	if !resources.Equals(app.GetPlaceholderResource(), res) {
		t.Fatalf("placeholder allocation not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), res)
	}
	assert.Equal(t, partition.GetTotalAllocationCount(), 1, "placeholder allocation should be counted as alloc")

	// add a second ph ask and run it again it should not match the already allocated placeholder
	ask = newAllocationAskTG("ph-2", appID1, taskGroup, res, true)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add placeholder ask ph-2 to app")
	// try to allocate placeholder should just return
	alloc = partition.tryPlaceholderAllocate()
	if alloc != nil {
		t.Fatalf("placeholder ask should not be allocated: %s", alloc)
	}
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("expected 2nd placeholder to be allocated")
	}
	assert.Equal(t, node.GetAllocation(alloc.UUID), alloc, "placeholder allocation 2 not found on node")
	if !resources.Equals(app.GetPlaceholderResource(), resources.Multiply(res, 2)) {
		t.Fatalf("placeholder allocation not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), resources.Multiply(res, 2))
	}
	assert.Equal(t, partition.GetTotalAllocationCount(), 2, "placeholder allocation should be counted as alloc")

	// not mapping to the same taskgroup should not do anything
	ask = newAllocationAskTG(allocID, appID1, "tg-unk", res, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app")
	alloc = partition.tryPlaceholderAllocate()
	if alloc != nil {
		t.Fatalf("allocation should not have matched placeholder: %s", alloc)
	}

	// add an ask with the TG
	ask = newAllocationAskTG(allocID2, appID1, taskGroup, res, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-2 to app with correct TG")
	alloc = partition.tryPlaceholderAllocate()
	if alloc == nil {
		t.Fatal("allocation should have matched placeholder")
	}
	assert.Equal(t, partition.GetTotalAllocationCount(), 2, "placeholder replacement should not be counted as alloc")
	assert.Equal(t, alloc.Result, objects.Replaced, "result is not the expected allocated replaced")
	assert.Equal(t, len(alloc.Releases), 1, "released allocations should have been 1")
	phUUID := alloc.Releases[0].UUID
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
	assert.Equal(t, confirmed.UUID, alloc.UUID, "confirmed allocation has unexpected UUID")
	if !resources.Equals(app.GetPlaceholderResource(), res) {
		t.Fatalf("placeholder allocations not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), res)
	}
	if !resources.Equals(app.GetAllocatedResource(), res) {
		t.Fatalf("allocations not updated as expected: got %s, expected %s", app.GetAllocatedResource(), res)
	}
}

// The failure is triggered by the predicate plugin and is hidden in the alloc handling
func TestFailReplacePlaceholder(t *testing.T) {
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
	err = partition.AddNode(newNodeMaxResource(nodeID1, tgRes), nil)
	assert.NilError(t, err, "test node-1 add failed unexpected")
	node := partition.GetNode(nodeID1)
	if node == nil {
		t.Fatal("new node was not found on the partition")
	}

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
	assert.Equal(t, node.GetAllocation(alloc.UUID), alloc, "placeholder allocation not found on node")
	assert.Assert(t, alloc.IsPlaceholder(), "placeholder alloc should return a placeholder allocation")
	assert.Equal(t, alloc.Result, objects.Allocated, "placeholder alloc should return an allocated result")
	assert.Equal(t, alloc.NodeID, nodeID1, "should be allocated on node-1")
	if !resources.Equals(app.GetPlaceholderResource(), res) {
		t.Fatalf("placeholder allocation not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), res)
	}
	// add 2nd node to allow allocation
	err = partition.AddNode(newNodeMaxResource(nodeID2, tgRes), nil)
	assert.NilError(t, err, "test node-2 add failed unexpected")
	node2 := partition.GetNode(nodeID2)
	if node2 == nil {
		t.Fatal("new node was not found on the partition")
	}
	// add an ask with the TG
	ask = newAllocationAskTG(allocID, appID1, taskGroup, res, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app with correct TG")
	alloc = partition.tryPlaceholderAllocate()
	if alloc == nil {
		t.Fatal("allocation should have matched placeholder")
	}
	assert.Equal(t, partition.GetTotalAllocationCount(), 1, "placeholder replacement should not be counted as alloc")
	assert.Equal(t, alloc.Result, objects.Replaced, "result is not the expected allocated replaced")
	assert.Equal(t, len(alloc.Releases), 1, "released allocations should have been 1")
	// allocation must be added as it is on a different node
	assert.Equal(t, alloc.NodeID, nodeID2, "should be allocated on node-2")
	assert.Assert(t, resources.IsZero(app.GetAllocatedResource()), "allocated resources should be zero")
	if !resources.Equals(node.GetAllocatedResource(), res) {
		t.Fatalf("node-1 allocation not updated as expected: got %s, expected %s", node.GetAllocatedResource(), res)
	}
	if !resources.Equals(node2.GetAllocatedResource(), res) {
		t.Fatalf("node-2 allocation not updated as expected: got %s, expected %s", node2.GetAllocatedResource(), res)
	}

	phUUID := alloc.Releases[0].UUID
	// placeholder is not released until confirmed by the shim
	if !resources.Equals(app.GetPlaceholderResource(), res) {
		t.Fatalf("placeholder allocation not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), resources.Multiply(res, 2))
	}

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
	assert.Equal(t, confirmed.UUID, alloc.UUID, "confirmed allocation has unexpected UUID")
	assert.Assert(t, resources.IsZero(app.GetPlaceholderResource()), "placeholder resources should be zero")
	if !resources.Equals(app.GetAllocatedResource(), res) {
		t.Fatalf("allocations not updated as expected: got %s, expected %s", app.GetAllocatedResource(), res)
	}
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "node-1 allocated resources should be zero")
	if !resources.Equals(node2.GetAllocatedResource(), res) {
		t.Fatalf("node-2 allocations not set as expected: got %s, expected %s", node2.GetAllocatedResource(), res)
	}
}

func TestAddAllocationAsk(t *testing.T) {
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
}

func TestRemoveAllocationAsk(t *testing.T) {
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
		Allocationkey:   askKey,
		TerminationType: si.TerminationType_STOPPED_BY_RM,
	}
	// unknown app should do nothing
	partition.removeAllocationAsk(release)
	if !resources.Equals(app.GetPendingResource(), res) {
		t.Fatal("wrong app updated removing ask")
	}

	// known app, unknown ask no change
	release.ApplicationID = appID1
	release.Allocationkey = "fake"
	partition.removeAllocationAsk(release)
	if !resources.Equals(app.GetPendingResource(), res) {
		t.Fatal("app updated removing unknown ask")
	}

	// known app, known ask, ignore timeout as it originates in the core
	release.Allocationkey = askKey
	release.TerminationType = si.TerminationType_TIMEOUT
	partition.removeAllocationAsk(release)
	if !resources.Equals(app.GetPendingResource(), res) {
		t.Fatal("app updated removing timed out ask, should not have changed")
	}

	// correct remove of a known ask
	release.TerminationType = si.TerminationType_STOPPED_BY_RM
	partition.removeAllocationAsk(release)
	assert.Assert(t, resources.IsZero(app.GetPendingResource()), "app should not have pending asks")
}

func TestUpdateNodeSortingPolicy(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	assert.Equal(t, partition.nodes.GetNodeSortingPolicy().PolicyType(), policies.FairnessPolicy)

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
		NodeSortPolicy: configs.NodeSortingPolicy{Type: policies.BinPackingPolicy.String()},
	})

	assert.Equal(t, partition.nodes.GetNodeSortingPolicy().PolicyType(), policies.BinPackingPolicy)
}

// only user limit is configured
func TestUserMaxAppLimit(t *testing.T) {
	// 'testuser' user max apps limit is 2. so 'testuser' is allowed to run max 2 apps.
	partition, err := newUserLimitsPartition()
	assert.NilError(t, err, "partition create failed")

	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{},
	}

	// add 2 apps by 'testuser' user
	for i := 1; i <= 2; i++ {
		appID := "app-" + strconv.Itoa(i)
		app := newApplicationWithUser(appID, "default", "root.default", user)

		err = partition.AddApplication(app)
		assert.NilError(t, err, appID+" should have been added to the partition")
	}

	// adding the app3 by 'testuser' user fails because user max app limit has reached
	app3 := newApplicationWithUser("app-3", "default", "root.default", user)
	err = partition.AddApplication(app3)
	assert.Error(t, err, "user 'testuser' has reached max applications limit in queue root.default")

	user1 := security.UserGroup{
		User:   "testuser1",
		Groups: []string{},
	}

	// add 5 apps by 'testuser1' user to ensure other restrictions on 'testuser' doesn't apply for other users
	for i := 1; i <= 5; i++ {
		appID := "app-testuser1-" + strconv.Itoa(i)
		app := newApplicationWithUser(appID, "default", "root.default", user1)

		err = partition.AddApplication(app)
		assert.NilError(t, err, appID+" should have been added to the partition")
	}

	// remove the app1
	allocs := partition.removeApplication(appID1)
	assert.Equal(t, 0, len(allocs), "existing application with allocations returned unexpected allocations %v", allocs)

	// adding app3 again by 'testuser' user succeeds as user's current running apps count equals or below the max app limit
	app3 = newApplicationWithUser("app-3", "default", "root.default", user)
	err = partition.AddApplication(app3)
	assert.NilError(t, err, "app-3 should have been added to the partition")

	// add 2 apps by 'testuser' user in dynamic queue 'root.child'
	for i := 1; i <= 2; i++ {
		appID := "app-dynamic-" + strconv.Itoa(i)
		app := newApplicationWithUser(appID, "default", "root.child", user)

		err = partition.AddApplication(app)
		assert.NilError(t, err, appID+" should have been added to the partition")
	}

	// adding the app3 by 'testuser' user fails because user max app limit has reached
	app3 = newApplicationWithUser("app-dynamic-3", "default", "root.child", user)
	err = partition.AddApplication(app3)
	assert.Error(t, err, "user 'testuser' has reached max applications limit in queue root.child")
}

// only user limit is configured as "*"
func TestWildCharUserMaxAppLimit(t *testing.T) {
	// user limit configured as '*' with max app count 2. so, each user is allowed to run max 2 apps.
	partition, err := newUserWildCharLimitsPartition()
	assert.NilError(t, err, "partition create failed")

	// add 2 apps by 'testuser1' & 'testuser2' users
	for j := 1; j <= 2; j++ {
		user := security.UserGroup{
			User:   "testuser" + strconv.Itoa(j),
			Groups: []string{},
		}
		for i := 1; i <= 2; i++ {
			appID := "app-testuser-" + strconv.Itoa(j) + "-" + strconv.Itoa(i)
			app := newApplicationWithUser(appID, "default", "root.default", user)

			err = partition.AddApplication(app)
			assert.NilError(t, err, appID+" should have been added to the partition")
		}
		// adding the app3 by 'testuser1' & 'testuser2' users fails because user max app limit has reached
		app3 := newApplicationWithUser("app-testuser-"+strconv.Itoa(j)+"-3", "default", "root.default", user)
		err = partition.AddApplication(app3)
		assert.Error(t, err, "user 'testuser"+strconv.Itoa(j)+"' has reached max applications limit in queue root.default")
	}

	// remove app 'app-testuser-1-1' added by "testuser1"
	appID := "app-testuser-1-1"
	allocs := partition.removeApplication(appID)
	assert.Equal(t, 0, len(allocs), "existing application with allocations returned unexpected allocations %v", allocs)

	// adding app 'app-testuser-1-1' again by 'testuser1' succeeds as 'testuser1' user current running apps equals or falls below the max apps count
	appID = "app-testuser-1-3"
	user := security.UserGroup{
		User:   "testuser1",
		Groups: []string{},
	}
	app := newApplicationWithUser(appID, "default", "root.default", user)
	err = partition.AddApplication(app)
	assert.NilError(t, err, appID+" should have been added to the partition")
}

// only group limit is configured
func TestGroupMaxAppLimit(t *testing.T) {
	// 'testgroup' group max apps limit is 2
	partition, err := newGroupLimitsPartition()
	assert.NilError(t, err, "partition create failed")

	for i := 1; i <= 2; i++ {
		// 'testuser1' & 'testuser2' users belong to 'testgroup' group.
		user := security.UserGroup{
			User:   "testuser" + strconv.Itoa(i),
			Groups: []string{"testgroup"},
		}

		appID := "app-" + strconv.Itoa(i)
		app := newApplicationWithUser(appID, "default", "root.default", user)

		err = partition.AddApplication(app)
		assert.NilError(t, err, appID+" should have been added to the partition")
	}

	// 'testuser3' belong to 'testgroup' group.
	user := security.UserGroup{
		User:   "testuser3",
		Groups: []string{"testgroup"},
	}

	// adding the app3 by 'testuser1' user fails because group's max app limit has reached
	app3 := newApplicationWithUser("app-3", "default", "root.default", user)
	err = partition.AddApplication(app3)
	assert.Error(t, err, "group 'testgroup' to which user 'testuser3' belongs to has reached max applications limit in queue root.default")

	// 'testuser4' belong to 'testgroup1' group.
	user1 := security.UserGroup{
		User:   "testuser4",
		Groups: []string{"testgroup1"},
	}

	// add 5 apps by 'testuser4' user to ensure other restrictions on 'testgroup' doesn't apply for other groups
	for i := 1; i <= 5; i++ {
		appID := "app-testuser4-" + strconv.Itoa(i)
		app := newApplicationWithUser(appID, "default", "root.default", user1)

		err = partition.AddApplication(app)
		assert.NilError(t, err, appID+" should have been added to the partition")
	}

	// remove the app
	allocs := partition.removeApplication(appID1)
	assert.Equal(t, 0, len(allocs), "existing application with allocations returned unexpected allocations %v", allocs)

	// adding app3 again by 'testuser' user succeeds as group's current running apps count equals or below the max app limit
	app3 = newApplicationWithUser("app-3", "default", "root.default", user)
	err = partition.AddApplication(app3)
	assert.NilError(t, err, "app-3 should have been added to the partition")
}

// only group limit is configured as "*"
func TestWildCharGroupMaxAppLimit(t *testing.T) {
	// group limit configured as '*' with max app count 2. so, each group is allowed to run max 2 apps.
	partition, err := newGroupWildCharLimitsPartition()
	assert.NilError(t, err, "partition create failed")

	// add 2 apps by 'testuser1' (belong to 'testgroup1') & 'testuser2' (belong to 'testgroup2') users
	for j := 1; j <= 2; j++ {
		user := security.UserGroup{
			User:   "testuser" + strconv.Itoa(j),
			Groups: []string{"testgroup" + strconv.Itoa(j)},
		}
		for i := 1; i <= 2; i++ {
			appID := "app-testuser-" + strconv.Itoa(j) + "-" + strconv.Itoa(i)
			app := newApplicationWithUser(appID, "default", "root.default", user)

			err = partition.AddApplication(app)
			assert.NilError(t, err, appID+" should have been added to the partition")
		}
		// adding the app3 by 'testuser1' & 'testuser2' users fails because each user's group max app limit has reached
		app3 := newApplicationWithUser("app-testuser-"+strconv.Itoa(j)+"-3", "default", "root.default", user)
		err = partition.AddApplication(app3)
		assert.Error(t, err, "group 'testgroup"+strconv.Itoa(j)+"' to which user 'testuser"+strconv.Itoa(j)+"' belongs to has reached max applications limit in queue root.default")
	}

	// remove app 'app-testuser-1-1' added by "testuser1"
	appID := "app-testuser-1-1"
	allocs := partition.removeApplication(appID)
	assert.Equal(t, 0, len(allocs), "existing application with allocations returned unexpected allocations %v", allocs)

	// adding app 'app-testuser-3-3' by 'testuser3' succeeds as 'testgroup1' group current running apps equals or falls below the max apps count
	appID = "app-testuser-3-3"
	user := security.UserGroup{
		User:   "testuser3",
		Groups: []string{"testgroup1"},
	}
	app := newApplicationWithUser(appID, "default", "root.default", user)
	err = partition.AddApplication(app)
	assert.NilError(t, err, appID+" should have been added to the partition")
}

// both user and group limit is configured
func TestUserGroupMaxAppLimit(t *testing.T) {
	// 'testuser1', 'testuser2' users and 'testgroup3' group max apps limit is 2
	partition, err := newUserGroupLimitsPartition()
	assert.NilError(t, err, "partition create failed")

	// user 'testuser1' belong to group 'testgroup1', user 'testuser2' belong to group 'testgroup2'
	user1 := security.UserGroup{
		User:   "testuser1",
		Groups: []string{"testgroup1"},
	}
	user2 := security.UserGroup{
		User:   "testuser2",
		Groups: []string{"testgroup2"},
	}

	for i := 1; i <= 2; i++ {
		user1 = security.UserGroup{
			User:   "testuser" + strconv.Itoa(i),
			Groups: []string{"testgroup" + strconv.Itoa(i)},
		}
		for j := 1; j <= 2; j++ {
			appID := "app-testuser-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
			app := newApplicationWithUser(appID, "default", "root.default", user1)

			err = partition.AddApplication(app)
			assert.NilError(t, err, appID+" should have been added to the partition")
		}
	}

	// 'testuser1', 'testuser2' users are not allowed to run as they have reached max apps limit
	app3 := newApplicationWithUser("app-testuser-3", "default", "root.default", user1)
	err = partition.AddApplication(app3)
	assert.Error(t, err, "user 'testuser1' has reached max applications limit in queue root.default")

	app4 := newApplicationWithUser("app-testuser-3", "default", "root.default", user2)
	err = partition.AddApplication(app4)
	assert.Error(t, err, "user 'testuser2' has reached max applications limit in queue root.default")

	// user 'testuser3' belong to group 'testgroup3'
	user3 := security.UserGroup{
		User:   "testuser3",
		Groups: []string{"testgroup3"},
	}

	// Ensure 'testgroup3' group has reached max apps limit
	for j := 1; j <= 2; j++ {
		appID := "app-testuser3-" + strconv.Itoa(j)
		app := newApplicationWithUser(appID, "default", "root.default", user3)

		err = partition.AddApplication(app)
		assert.NilError(t, err, appID+" should have been added to the partition")
	}

	// user 'testuser4' belong to group 'testgroup3'
	user4 := security.UserGroup{
		User:   "testuser4",
		Groups: []string{"testgroup3"},
	}

	// user 'testuser4' is not allowed to run as 'testgroup3' group has reached max apps limit
	app5 := newApplicationWithUser("app-3", "default", "root.default", user4)
	err = partition.AddApplication(app5)
	assert.Error(t, err, "group 'testgroup3' to which user 'testuser4' belongs to has reached max applications limit in queue root.default")
}

// both user and group is configured as "*"
func TestWildCharUserGroupMaxAppLimit(t *testing.T) {
	// user and group limit configured as '*' with max app count 2. so, each user and group is allowed to run max 2 apps.
	partition, err := newUserGroupWildCharLimitsPartition()
	assert.NilError(t, err, "partition create failed")

	for i := 1; i <= 2; i++ {
		user1 := security.UserGroup{
			User:   "testuser" + strconv.Itoa(i),
			Groups: []string{"testgroup" + strconv.Itoa(i)},
		}
		for j := 1; j <= 2; j++ {
			appID := "app-testuser-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
			app := newApplicationWithUser(appID, "default", "root.default", user1)

			err = partition.AddApplication(app)
			assert.NilError(t, err, appID+" should have been added to the partition")
		}
	}

	// user 'testuser3' belong to group 'testgroup1',
	user3 := security.UserGroup{
		User:   "testuser3",
		Groups: []string{"testgroup1"},
	}

	// 'testuser3' user is not allowed to run as 'testgroup1' group has reached max apps limit
	app1 := newApplicationWithUser("app-testuser-3", "default", "root.default", user3)
	err = partition.AddApplication(app1)
	assert.Error(t, err, "group 'testgroup1' to which user 'testuser3' belongs to has reached max applications limit in queue root.default")

	// user 'testuser4' belong to group 'testgroup2',
	user4 := security.UserGroup{
		User:   "testuser4",
		Groups: []string{"testgroup2"},
	}

	// 'testuser4' user is not allowed to run as 'testgroup2' group has reached max apps limit
	app2 := newApplicationWithUser("app-testuser-3", "default", "root.default", user4)
	err = partition.AddApplication(app2)
	assert.Error(t, err, "group 'testgroup2' to which user 'testuser4' belongs to has reached max applications limit in queue root.default")
}
