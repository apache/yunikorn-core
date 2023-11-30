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

	"gotest.tools/v3/assert"

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
	userManager.ClearConfigLimits()
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
				Limits: []configs.Limit{
					{
						Limit: "root queue limit",
						Users: []string{
							"user1",
						},
						Groups: []string{
							"group1",
						},
						MaxResources: map[string]string{
							"memory": "10",
							"vcores": "10",
						},
						MaxApplications: 2,
					},
				},
			},
		},
	}
	partition, err = newPartitionContext(conf, "test", &ClusterContext{})
	assert.NilError(t, err, "partition create should not have failed with error")
	if partition.root.QueuePath != "root" {
		t.Fatal("partition root queue not set as expected")
	}
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
		NodeSortPolicy: configs.NodeSortingPolicy{},
	}
	partition, err := newPartitionContext(confWith, rmID, nil)
	assert.NilError(t, err, "test partition create failed with error")
	assert.Equal(t, len(*partition.rules), 1, "Placement rules not set as expected")

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
	assert.Equal(t, len(*partition.rules), 2, "Placement rules not updated as expected")

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
	assert.Equal(t, len(*partition.rules), 0, "Placement rules not updated as expected")

	// set the old config back this should turn on the placement again
	err = partition.updatePartitionDetails(confWith)
	assert.NilError(t, err, "update partition failed unexpected with error")
	assert.Equal(t, len(*partition.rules), 2, "Placement rules not updated as expected")
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
	ask := newAllocationAsk("alloc-1-uuid", "unknown", appRes)
	alloc := objects.NewAllocation(nodeID1, ask)
	allocs := []*objects.Allocation{alloc}
	err = partition.AddNode(node, allocs)
	if err == nil {
		t.Errorf("add node to partition should have failed (app missing)")
	}
	assert.Equal(t, partition.nodes.GetNodeCount(), 0, "error returned but node still added to the partition (app)")

	// fail with a broken alloc
	ask = newAllocationAsk("alloc-1-uuid", appID1, appRes)
	alloc = objects.NewAllocation(nodeID1, ask)
	assert.Equal(t, alloc.GetAllocationID(), "alloc-1-uuid-0")
	// reset uuid to empty
	alloc.SetAllocationID("")
	assert.Equal(t, alloc.GetAllocationID(), "")
	allocs = []*objects.Allocation{alloc}
	err = partition.AddNode(node, allocs)
	if err == nil {
		t.Errorf("add node to partition should have failed (uuid missing)")
	}
	assert.Equal(t, partition.nodes.GetNodeCount(), 0, "error returned but node still added to the partition (uuid)")
	assertLimits(t, getTestUserGroup(), nil)

	// fix the alloc add the node will work now
	alloc = objects.NewAllocation(nodeID1, ask)
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
	assertLimits(t, getTestUserGroup(), appRes)
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
	alloc := objects.NewAllocation(nodeID1, ask)
	allocUUID := alloc.GetAllocationID()
	allocs := []*objects.Allocation{alloc}
	err = partition.AddNode(node, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")
	// get what was allocated
	allocated := node.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly")
	assertLimits(t, getTestUserGroup(), appRes)

	// add broken allocations
	ask = newAllocationAsk("alloc-na", "not-an-app", appRes)
	alloc = objects.NewAllocation(nodeID1, ask)
	node.AddAllocation(alloc)
	ask = newAllocationAsk("alloc-2", appID1, appRes)
	alloc = objects.NewAllocation(nodeID1, ask)
	node.AddAllocation(alloc)
	assertLimits(t, getTestUserGroup(), appRes)

	// remove the node this cannot fail
	released, confirmed := partition.removeNode(nodeID1)
	assert.Equal(t, 0, partition.GetTotalNodeCount(), "node list was not updated, node was not removed")
	assert.Equal(t, 1, len(released), "node did not release correct allocation")
	assert.Equal(t, 0, len(confirmed), "node did not confirm correct allocation")
	assert.Equal(t, released[0].GetUUID(), allocUUID, "uuid returned by release not the same as on allocation")
	assertLimits(t, getTestUserGroup(), resources.Zero)

	assert.NilError(t, err, "the event should have been processed")
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
	ph := objects.NewAllocation(nodeID1, ask)
	allocs := []*objects.Allocation{ph}
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node1 to partition should not have failed")
	// get what was allocated
	allocated := node1.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node1 expected 1 got: %v", allocated)
	assert.Assert(t, resources.Equals(node1.GetAllocatedResource(), appRes), "allocation not added correctly to node1")
	assertLimits(t, getTestUserGroup(), appRes)
	assert.Equal(t, 1, partition.getPhAllocationCount(), "number of active placeholders")

	// fake an ask that is used
	ask = newAllocationAskAll(allocID, appID1, taskGroup, appRes, 1, 1, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should be added to app")
	_, err = app.UpdateAskRepeat(allocID, -1)
	assert.NilError(t, err, "ask should have been updated without error")
	assert.Assert(t, resources.IsZero(app.GetPendingResource()), "app should not have pending resources")
	assertLimits(t, getTestUserGroup(), appRes)

	// add real allocation that is replacing the placeholder
	alloc := objects.NewAllocation(nodeID1, ask)
	alloc.SetRelease(ph)
	// double link as if the replacement is ongoing
	ph.SetRelease(alloc)
	alloc.SetResult(objects.Replaced)

	allocs = app.GetAllAllocations()
	assert.Equal(t, len(allocs), 1, "expected one allocation for the app (placeholder)")
	assertLimits(t, getTestUserGroup(), appRes)

	// remove the node that has both placeholder and real allocation
	released, confirmed := partition.removeNode(nodeID1)
	assert.Equal(t, 0, partition.GetTotalNodeCount(), "node list was not updated, node was not removed")
	assert.Equal(t, 1, len(released), "node removal did not release correct allocation")
	assert.Equal(t, 0, len(confirmed), "node removal should not have confirmed allocation")
	assert.Equal(t, ph.GetAllocationID(), released[0].GetAllocationID(), "uuid returned by release not the same as the placeholder")
	assert.Equal(t, 0, partition.getPhAllocationCount(), "number of active placeholders")
	allocs = app.GetAllAllocations()
	assert.Equal(t, 0, len(allocs), "expected no allocations for the app")
	assert.Assert(t, resources.Equals(app.GetPendingResource(), appRes), "app should have updated pending resources")
	// check the interim state of the placeholder involved
	assert.Equal(t, 0, ph.GetReleaseCount(), "placeholder should have no releases linked anymore")
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0}))
}

func TestCalculateNodesResourceUsage(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	oldCapacity := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100})
	node := newNodeMaxResource(nodeID1, oldCapacity)
	err = partition.AddNode(node, nil)
	assert.NilError(t, err)

	occupiedResources := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})
	alloc := objects.NewAllocation(nodeID1, newAllocationAsk("key", "appID", occupiedResources))
	node.AddAllocation(alloc)
	usageMap := partition.calculateNodesResourceUsage()
	assert.Equal(t, node.GetAvailableResource().Resources["first"], resources.Quantity(50))
	assert.Equal(t, usageMap["first"][4], 1)

	occupiedResources = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})
	alloc = objects.NewAllocation(nodeID1, newAllocationAsk("key", "appID", occupiedResources))
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

// test basic placeholder preemption
// setup:
// queue quota max size: 16GB / 16cpu
// nodes: 2 * 8GB / 8 cpu
// create an application with allocation: 4 GB / 4 cpu
// create a gang application requesting: 7 * 2GB / 2cpu
// create a daemon set pod for one of the nodes asking for 2GB / 2 cpu
//
// ensure placeholder has been preempted and released resources has been given to the request asked for
// ensure preempted placeholder has been accounted under timed out in gang app placeholder data
func TestPlaceholderDataWithPlaceholderPreemption(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app1
	app1, _ := newApplicationWithHandler(appID1, "default", defQueue)
	err = partition.AddApplication(app1)
	assert.NilError(t, err, "add application to partition should not have failed")

	// add a node with allocation: must have the correct app1 added already
	resMap := map[string]string{"mem": "2M", "vcore": "2"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "Unexpected error when creating resource from map")
	appRes := res.Clone()
	phRes := res.Clone()
	newRes := res.Clone()
	appRes.MultiplyTo(2)
	newRes.MultiplyTo(4)
	phRes.MultiplyTo(7)

	ask := newAllocationAskAll("ask-1", appID1, taskGroup, appRes, 0, 1, false)
	alloc := objects.NewAllocation(nodeID1, ask)
	allocs := []*objects.Allocation{alloc}

	node1 := newNodeMaxResource(nodeID1, newRes)
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node1 to partition should not have failed")
	assert.Equal(t, 1, partition.nodes.GetNodeCount(), "node list not correct")

	// get what was allocated
	allocated := node1.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node1")
	assert.Assert(t, resources.Equals(node1.GetAllocatedResource(), appRes), "allocation not added correctly to node1")

	node2 := newNodeMaxResource(nodeID2, newRes)
	err = partition.AddNode(node2, nil)
	assert.NilError(t, err, "test node add failed unexpected")
	assert.Equal(t, 2, partition.nodes.GetNodeCount(), "node list not correct")
	assert.Assert(t, resources.Equals(partition.GetQueue(defQueue).GetAllocatedResource(), appRes), "Queue allocated resource is not correct")

	// add the app1 with 6 placeholder request
	gangApp := newApplicationTGTagsWithPhTimeout(appID2, "default", defQueue, phRes, nil, 0)
	err = partition.AddApplication(gangApp)
	assert.NilError(t, err, "app1-1 should have been added to the partition")

	var lastPh string
	for i := 1; i <= 6; i++ {
		// add an ask for a placeholder and allocate
		lastPh = phID + strconv.Itoa(i)
		ask = newAllocationAskTG(lastPh, appID2, taskGroup, res, true)
		err = gangApp.AddAllocationAsk(ask)
		assert.NilError(t, err, "failed to add placeholder ask %s to app1", lastPh)
		// try to allocate a placeholder via normal allocate
		ph := partition.tryAllocate()
		assert.Assert(t, ph != nil, "expected placeholder to be allocated")
	}
	assert.Equal(t, 7, partition.GetTotalAllocationCount(), "placeholder allocation should be counted as normal allocations on the partition")
	assert.Equal(t, 6, partition.getPhAllocationCount(), "placeholder allocations should be counted as placeholders on the partition")

	assertPlaceholderData(t, gangApp, 6, 0)
	partition.removeApplication(appID1)
	assert.Equal(t, 6, partition.GetTotalAllocationCount(), "remove app did not remove allocation from count")
	assert.Equal(t, 6, partition.getPhAllocationCount(), "placeholder allocations changed unexpectedly")

	// add a new app1
	app2, testHandler2 := newApplicationWithHandler(appID3, "default", defQueue)
	err = partition.AddApplication(app2)
	assert.NilError(t, err, "add application to partition should not have failed")

	// required node set on ask
	ask2 := newAllocationAsk(allocID2, appID3, res)
	ask2.SetRequiredNode(nodeID2)
	err = app2.AddAllocationAsk(ask2)
	assert.NilError(t, err, "failed to add ask alloc-2 to app1-1")

	// since node-2 available resource is less than needed, reserve the node
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("allocation attempt should not have returned an allocation")
	}
	// check if updated (must be after allocate call)
	assert.Equal(t, 1, len(app2.GetReservations()), "app reservation should have been updated")
	assert.Equal(t, 1, len(app2.GetAskReservations(allocID2)), "ask should have been reserved")

	// try through reserved scheduling cycle this should trigger preemption
	alloc = partition.tryReservedAllocate()
	if alloc != nil {
		t.Fatal("reserved allocation attempt should not have returned an allocation")
	}

	// check if there is a release event for the expected allocation
	var found bool
	var releasedUUID string
	for _, event := range testHandler2.GetEvents() {
		if allocRelease, ok := event.(*rmevent.RMReleaseAllocationEvent); ok {
			found = allocRelease.ReleasedAllocations[0].AllocationKey == lastPh
			releasedUUID = allocRelease.ReleasedAllocations[0].UUID
			break
		}
	}
	assert.Assert(t, found, "release allocation event not found in list")
	// release allocation: do what the context would do after the shim processing
	release := &si.AllocationRelease{
		PartitionName:   partition.Name,
		ApplicationID:   appID2,
		UUID:            releasedUUID,
		TerminationType: si.TerminationType_PREEMPTED_BY_SCHEDULER,
	}
	releases, confirmed := partition.removeAllocation(release)
	assert.Equal(t, 0, len(releases), "not expecting any released allocations")
	assert.Assert(t, confirmed == nil, "not expecting any confirmed allocations")
	assert.Equal(t, 5, partition.GetTotalAllocationCount(), "preempted placeholder should be removed from allocations")
	assert.Equal(t, 5, partition.getPhAllocationCount(), "preempted placeholder should be removed")
	assertPlaceholderData(t, gangApp, 6, 1)
}

// test node removal effect on placeholder data
// setup:
// queue quota max size: 16GB / 16cpu
// nodes: 2 * 8GB / 8 cpu
// create an application with allocation: 4 GB / 4 cpu
// create an gang application requesting: 7 * 2GB / 2cpu
// Remove the node where placeholders are running
//
// ensure removed placeholders has been accounted under timed out in gang app placeholder data
func TestPlaceholderDataWithNodeRemoval(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app1
	app1, _ := newApplicationWithHandler(appID1, "default", defQueue)
	err = partition.AddApplication(app1)
	assert.NilError(t, err, "add application to partition should not have failed")

	resMap := map[string]string{"mem": "2M", "vcore": "2"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "Unexpected error when creating resource from map")
	appRes := res.Clone()
	phRes := res.Clone()
	newRes := res.Clone()
	appRes.MultiplyTo(2)
	newRes.MultiplyTo(4)
	phRes.MultiplyTo(7)

	// add a node with allocation: must have the correct app1 added already
	ask := newAllocationAskAll("ask-1", appID1, taskGroup, appRes, 0, 1, false)
	alloc := objects.NewAllocation(nodeID1, ask)
	allocs := []*objects.Allocation{alloc}

	node1 := newNodeMaxResource(nodeID1, newRes)
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node1 to partition should not have failed")
	assert.Equal(t, 1, partition.nodes.GetNodeCount(), "node list not correct")

	// get what was allocated
	allocated := node1.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node1")
	assert.Assert(t, resources.Equals(node1.GetAllocatedResource(), appRes), "allocation not added correctly to node1")

	node2 := newNodeMaxResource(nodeID2, newRes)
	err = partition.AddNode(node2, nil)
	assert.NilError(t, err, "test node add failed unexpected")
	assert.Equal(t, 2, partition.nodes.GetNodeCount(), "node list not correct")
	assert.Assert(t, resources.Equals(partition.GetQueue(defQueue).GetAllocatedResource(), appRes), "Queue allocated resource is not correct")

	// add the app1 with 6 placeholder request
	gangApp := newApplicationTGTagsWithPhTimeout(appID2, "default", defQueue, phRes, nil, 0)
	err = partition.AddApplication(gangApp)
	assert.NilError(t, err, "app1-1 should have been added to the partition")

	for i := 1; i <= 6; i++ {
		// add an ask for a placeholder and allocate
		ask = newAllocationAskTG(phID+strconv.Itoa(i+1), appID2, taskGroup, res, true)
		err = gangApp.AddAllocationAsk(ask)
		assert.NilError(t, err, "failed to add placeholder ask ph-1 to app1")
		// try to allocate a placeholder via normal allocate
		ph := partition.tryAllocate()
		if ph == nil {
			t.Fatal("expected placeholder to be allocated")
		}
	}

	// add an ask for a last placeholder and allocate
	lastPh := phID + strconv.Itoa(7)
	ask = newAllocationAskTG(lastPh, appID2, taskGroup, res, true)
	err = gangApp.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add placeholder ask ph-1 to app1")

	// try to allocate a last placeholder via normal allocate
	partition.tryAllocate()

	assertPlaceholderData(t, gangApp, 7, 0)

	// Remove node
	partition.removeNode(nodeID2)
	assertPlaceholderData(t, gangApp, 7, 4)
}

// Test removal of placeholder has been accounted as timed out in app placeholder data
// setup:
// queue quota max size: 16GB / 16cpu
// nodes: 2 * 8GB / 8 cpu
// create an application with allocation: 4 GB / 4 cpu
// create an gang application requesting: 7 * 2GB / 2cpu
// Remove the node where placeholders are running
//
// ensure removed placeholders has been accounted under timed out in gang app placeholder data
func TestPlaceholderDataWithRemoval(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app1
	app1, _ := newApplicationWithHandler(appID1, "default", defQueue)
	err = partition.AddApplication(app1)
	assert.NilError(t, err, "add application to partition should not have failed")

	resMap := map[string]string{"mem": "2M", "vcore": "2"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "Unexpected error when creating resource from map")
	appRes := res.Clone()
	phRes := res.Clone()
	newRes := res.Clone()
	appRes.MultiplyTo(2)
	newRes.MultiplyTo(4)
	phRes.MultiplyTo(7)

	// add a node with allocation: must have the correct app1 added already
	ask := newAllocationAskAll("ask-1", appID1, taskGroup, appRes, 0, 1, false)
	alloc := objects.NewAllocation(nodeID1, ask)
	allocs := []*objects.Allocation{alloc}

	node1 := newNodeMaxResource(nodeID1, newRes)
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node1 to partition should not have failed")
	assert.Equal(t, 1, partition.nodes.GetNodeCount(), "node list not correct")

	// get what was allocated
	allocated := node1.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node1")
	assert.Assert(t, resources.Equals(node1.GetAllocatedResource(), appRes), "allocation not added correctly to node1")

	node2 := newNodeMaxResource(nodeID2, newRes)
	err = partition.AddNode(node2, nil)
	assert.NilError(t, err, "test node add failed unexpected")
	assert.Equal(t, 2, partition.nodes.GetNodeCount(), "node list not correct")
	assert.Assert(t, resources.Equals(partition.GetQueue(defQueue).GetAllocatedResource(), appRes), "Queue allocated resource is not correct")

	// add the app1 with 6 placeholder request
	gangApp := newApplicationTGTagsWithPhTimeout(appID2, "default", defQueue, phRes, nil, 0)
	err = partition.AddApplication(gangApp)
	assert.NilError(t, err, "app1-1 should have been added to the partition")

	var lastPhUUID string
	for i := 1; i <= 6; i++ {
		// add an ask for a placeholder and allocate
		ask = newAllocationAskTG(phID+strconv.Itoa(i+1), appID2, taskGroup, res, true)
		err = gangApp.AddAllocationAsk(ask)
		assert.NilError(t, err, "failed to add placeholder ask ph-1 to app1")
		// try to allocate a placeholder via normal allocate
		ph := partition.tryAllocate()
		if ph == nil {
			t.Fatal("expected placeholder to be allocated")
		}
		lastPhUUID = ph.GetUUID()
	}

	// add an ask for a last placeholder and allocate
	lastPh := phID + strconv.Itoa(7)
	ask = newAllocationAskTG(lastPh, appID2, taskGroup, res, true)
	err = gangApp.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add placeholder ask ph-1 to app1")

	// try to allocate a last placeholder via normal allocate
	partition.tryAllocate()
	assertPlaceholderData(t, gangApp, 7, 0)

	// release allocation: do what the context would do after the shim processing
	release := &si.AllocationRelease{
		PartitionName:   partition.Name,
		ApplicationID:   appID2,
		UUID:            lastPhUUID,
		TerminationType: si.TerminationType_STOPPED_BY_RM,
	}
	releases, _ := partition.removeAllocation(release)
	assert.Equal(t, 1, len(releases), "unexpected number of allocations released")
	assertPlaceholderData(t, gangApp, 7, 1)
}

// check PlaceHolderData
func assertPlaceholderData(t *testing.T, gangApp *objects.Application, count int64, timedout int64) {
	assert.Equal(t, len(gangApp.GetAllPlaceholderData()), 1)
	assert.Equal(t, gangApp.GetAllPlaceholderData()[0].TaskGroupName, taskGroup)
	assert.Equal(t, gangApp.GetAllPlaceholderData()[0].Count, count)
	assert.Equal(t, gangApp.GetAllPlaceholderData()[0].Replaced, int64(0))
	assert.Equal(t, gangApp.GetAllPlaceholderData()[0].TimedOut, timedout)
}

// test with a replacement of a placeholder: placeholder on the removed node, real on the 2nd node
func TestRemoveNodeWithReplacement(t *testing.T) {
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
	ph := objects.NewAllocation(nodeID1, ask)
	allocs := []*objects.Allocation{ph}
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node1 to partition should not have failed")
	// get what was allocated
	allocated := node1.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node1")
	assertLimits(t, getTestUserGroup(), appRes)
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
	alloc := objects.NewAllocation(nodeID2, ask)
	alloc.SetRelease(ph)
	alloc.SetResult(objects.Replaced)
	node2.AddAllocation(alloc)
	allocated = node2.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node2")
	assert.Assert(t, resources.Equals(node2.GetAllocatedResource(), appRes), "allocation not added correctly to node2 (resource count)")
	assertLimits(t, getTestUserGroup(), appRes)

	// double link as if the replacement is ongoing
	ph.SetRelease(alloc)

	allocs = app.GetAllAllocations()
	assert.Equal(t, len(allocs), 1, "expected one allocation for the app (placeholder)")
	assertLimits(t, getTestUserGroup(), appRes)

	// remove the node with the placeholder
	released, confirmed := partition.removeNode(nodeID1)
	assert.Equal(t, 1, partition.GetTotalNodeCount(), "node list was not updated, node was not removed")
	assert.Equal(t, 1, len(node2.GetAllAllocations()), "remaining node should have allocation")
	assert.Equal(t, 1, len(released), "node removal did not release correct allocation")
	assert.Equal(t, 1, len(confirmed), "node removal did not confirm correct allocation")
	assert.Equal(t, ph.GetAllocationID(), released[0].GetAllocationID(), "uuid returned by release not the same as the placeholder")
	assert.Equal(t, alloc.GetAllocationID(), confirmed[0].GetAllocationID(), "uuid returned by confirmed not the same as the real allocation")
	assert.Assert(t, resources.IsZero(app.GetPendingResource()), "app should not have pending resources")
	assert.Assert(t, !app.IsCompleting(), "app should not be COMPLETING after confirming allocation")
	allocs = app.GetAllAllocations()
	assert.Equal(t, 1, len(allocs), "expected one allocation for the app (real)")
	assert.Equal(t, alloc.GetAllocationID(), allocs[0].GetAllocationID(), "uuid for the app is not the same as the real allocation")
	assert.Equal(t, objects.Allocated, allocs[0].GetResult(), "allocation state should be allocated")
	assert.Equal(t, 0, allocs[0].GetReleaseCount(), "real allocation should have no releases linked anymore")
	assertLimits(t, getTestUserGroup(), appRes)
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
	ph := objects.NewAllocation(nodeID1, ask)
	allocs := []*objects.Allocation{ph}
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node1 to partition should not have failed")
	// get what was allocated
	allocated := node1.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node1")
	assert.Assert(t, resources.Equals(node1.GetAllocatedResource(), appRes), "allocation not added correctly to node1")
	assertLimits(t, getTestUserGroup(), appRes)

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
	alloc := objects.NewAllocation(nodeID2, ask)
	alloc.SetRelease(ph)
	alloc.SetResult(objects.Replaced)
	node2.AddAllocation(alloc)
	allocated = node2.GetAllAllocations()
	assert.Equal(t, 1, len(allocated), "allocation not added correctly to node2")
	assert.Assert(t, resources.Equals(node2.GetAllocatedResource(), appRes), "allocation not added correctly to node2 (resource count)")
	assertLimits(t, getTestUserGroup(), appRes)

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
	assert.Equal(t, ph.GetAllocationID(), allocs[0].GetAllocationID(), "uuid for the app is not the same as the real allocation")
	assert.Equal(t, 0, ph.GetReleaseCount(), "no inflight replacements linked")
	assertLimits(t, getTestUserGroup(), appRes)
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

func TestAddAppForced(t *testing.T) {
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	// add a new app to an invalid queue
	app := newApplication(appID1, "default", "root.invalid")
	err = partition.AddApplication(app)
	if err == nil || partition.getApplication(appID1) != nil {
		t.Fatalf("add application to nonexistent queue should have failed but did not")
	}

	// re-add the app, but mark it as forced. this should create the recovery queue and assign the app to it
	app = newApplicationTags(appID1, "default", "root.invalid", map[string]string{siCommon.AppTagCreateForce: "true"})
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app create failed")
	partApp := partition.getApplication(appID1)
	if partApp == nil {
		t.Fatalf("app not found after adding to partition")
	}
	recoveryQueue := partition.GetQueue(common.RecoveryQueueFull)
	if recoveryQueue == nil {
		t.Fatalf("recovery queue not found")
	}
	assert.Equal(t, common.RecoveryQueueFull, partApp.GetQueuePath(), "wrong queue path for app2")
	assert.Check(t, recoveryQueue == partApp.GetQueue(), "wrong queue for app")
	assert.Equal(t, 1, len(recoveryQueue.GetCopyOfApps()), "wrong queue length")

	// add second forced app. this should use the existing recovery queue rather than recreating it
	app2 := newApplicationTags(appID2, "default", "root.invalid2", map[string]string{siCommon.AppTagCreateForce: "true"})
	err = partition.AddApplication(app2)
	assert.NilError(t, err, "app2 create failed")
	partApp2 := partition.getApplication(appID2)
	if partApp2 == nil {
		t.Fatalf("app2 not found after adding to partition")
	}
	assert.Equal(t, common.RecoveryQueueFull, partApp2.GetQueuePath(), "wrong queue path for app2")
	assert.Check(t, recoveryQueue == partApp2.GetQueue(), "wrong queue for app2")
	assert.Equal(t, 2, len(recoveryQueue.GetCopyOfApps()), "wrong queue length")

	// add third app (not forced), but referencing the recovery queue. this should fail.
	app3 := newApplication(appID3, "default", common.RecoveryQueueFull)
	err = partition.AddApplication(app3)
	if err == nil || partition.getApplication(appID3) != nil {
		t.Fatalf("add app3 to recovery queue should have failed but did not")
	}

	// re-add third app, but forced. This should succeed.
	app3 = newApplicationTags(appID3, "default", common.RecoveryQueueFull, map[string]string{siCommon.AppTagCreateForce: "true"})
	err = partition.AddApplication(app3)
	assert.NilError(t, err, "app3 create failed")
	partApp3 := partition.getApplication(appID3)
	if partApp3 == nil {
		t.Fatalf("app3 not found after adding to partition")
	}
	assert.Equal(t, common.RecoveryQueueFull, partApp3.GetQueuePath(), "wrong queue path for app3")
	assert.Check(t, recoveryQueue == partApp3.GetQueue(), "wrong queue for app3")
	assert.Equal(t, 3, len(recoveryQueue.GetCopyOfApps()), "wrong queue length")
}

func TestAddAppForcedWithPlacement(t *testing.T) {
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
				Name:   "tag",
				Value:  "queue",
				Create: true,
			},
		},
		Limits:         nil,
		NodeSortPolicy: configs.NodeSortingPolicy{},
	}
	partition, err := newPartitionContext(confWith, rmID, nil)
	assert.NilError(t, err, "test partition create failed with error")

	// add a new app using tag rule
	app := newApplicationTags(appID1, "default", "", map[string]string{"queue": "root.test"})
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app to tagged queue")
	assert.Equal(t, "root.test", app.GetQueuePath(), "app assigned to wrong queue")

	// add a second app without a tag rule
	app2 := newApplicationTags(appID2, "default", "root.untagged", map[string]string{})
	err = partition.AddApplication(app2)
	if err == nil || partition.getApplication(appID2) != nil {
		t.Fatalf("add app2 to fixed queue should have failed but did not")
	}

	// attempt to add the app again, but with forced addition
	app2 = newApplicationTags(appID2, "default", "root.untagged", map[string]string{siCommon.AppTagCreateForce: "true"})
	err = partition.AddApplication(app2)
	assert.NilError(t, err, "failed to add app2 to tagged queue")
	assert.Equal(t, common.RecoveryQueueFull, app2.GetQueuePath(), "app2 assigned to wrong queue")

	// add a third app, but with the recovery queue tagged
	app3 := newApplicationTags(appID3, "default", common.RecoveryQueueFull, map[string]string{siCommon.AppTagCreateForce: "true"})
	err = partition.AddApplication(app3)
	assert.NilError(t, err, "failed to add app3 to tagged queue")
	assert.Equal(t, common.RecoveryQueueFull, app3.GetQueuePath(), "app2 assigned to wrong queue")
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
	queue.UpdateQueueProperties()
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
	queue.UpdateQueueProperties()
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
	alloc := objects.NewAllocation(nodeID1, ask)
	err = partition.addAllocation(alloc)
	assert.NilError(t, err, "add allocation to partition should not have failed")
	assertLimits(t, getTestUserGroup(), appRes)

	allocs := partition.removeApplication("does_not_exist")
	if allocs != nil {
		t.Errorf("non existing application returned unexpected values: allocs = %v", allocs)
	}

	// add another new app
	app = newApplication(appID1, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")
	assertLimits(t, getTestUserGroup(), appRes)

	// remove the newly added app (no allocations)
	allocs = partition.removeApplication(appID1)
	assert.Equal(t, 0, len(allocs), "existing application without allocations returned allocations %v", allocs)
	assert.Equal(t, 1, len(partition.applications), "existing application was not removed")
	assertLimits(t, getTestUserGroup(), appRes)

	// add the application again and then an allocation
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	ask = newAllocationAsk("alloc-1", appID1, appRes)
	alloc = objects.NewAllocation(nodeID1, ask)
	err = partition.addAllocation(alloc)
	assert.NilError(t, err, "add allocation to partition should not have failed")
	assertLimits(t, getTestUserGroup(), resources.Multiply(appRes, 2))

	// remove the newly added app
	allocs = partition.removeApplication(appID1)
	assert.Equal(t, 1, len(allocs), "existing application with allocations returned unexpected allocations %v", allocs)
	assert.Equal(t, 1, len(partition.applications), "existing application was not removed")
	if partition.GetTotalAllocationCount() != 1 {
		t.Errorf("allocation that should have been left was removed")
	}
	assertLimits(t, getTestUserGroup(), appRes)

	allocs = partition.removeApplication("will_not_remove")
	assert.Equal(t, 1, len(allocs), "existing application with allocations returned unexpected allocations %v", allocs)
	assertLimits(t, getTestUserGroup(), resources.Zero)
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
	alloc := objects.NewAllocation(nodeID1, ask)
	err = partition.addAllocation(alloc)
	assert.NilError(t, err, "add allocation to partition should not have failed")
	assertLimits(t, getTestUserGroup(), appRes)

	ask = newAllocationAsk("alloc-1", appNotRemoved, appRes)
	uuid := "alloc-1-0"
	alloc = objects.NewAllocation(nodeID1, ask)
	err = partition.addAllocation(alloc)
	assert.NilError(t, err, "add allocation to partition should not have failed")
	assertLimits(t, getTestUserGroup(), resources.Multiply(appRes, 2))
	release := &si.AllocationRelease{
		PartitionName:   "default",
		ApplicationID:   "",
		UUID:            "",
		TerminationType: si.TerminationType_STOPPED_BY_RM,
	}

	allocs, _ := partition.removeAllocation(release)
	assert.Equal(t, 0, len(allocs), "empty removal request returned allocations: %v", allocs)
	assertLimits(t, getTestUserGroup(), resources.Multiply(appRes, 2))
	// create a new release without app: should just return
	release.ApplicationID = "does_not_exist"
	allocs, _ = partition.removeAllocation(release)
	assert.Equal(t, 0, len(allocs), "removal request for non existing application returned allocations: %v", allocs)
	assertLimits(t, getTestUserGroup(), resources.Multiply(appRes, 2))
	// create a new release with app, non existing allocation: should just return
	release.ApplicationID = appNotRemoved
	release.UUID = "does_not_exist"
	allocs, _ = partition.removeAllocation(release)
	assert.Equal(t, 0, len(allocs), "removal request for non existing allocation returned allocations: %v", allocs)
	assertLimits(t, getTestUserGroup(), resources.Multiply(appRes, 2))
	// create a new release with app, existing allocation: should return 1 alloc
	assert.Equal(t, 2, partition.GetTotalAllocationCount(), "pre-remove allocation list incorrect: %v", partition.allocations)
	release.UUID = uuid
	allocs, _ = partition.removeAllocation(release)
	assert.Equal(t, 1, len(allocs), "removal request for existing allocation returned wrong allocations: %v", allocs)
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "allocation removal requests removed more than expected: %v", partition.allocations)
	assertLimits(t, getTestUserGroup(), resources.Multiply(appRes, 1))
	// create a new release with app, no uuid: should return last left alloc
	release.UUID = ""
	allocs, _ = partition.removeAllocation(release)
	assert.Equal(t, 1, len(allocs), "removal request for existing allocation returned wrong allocations: %v", allocs)
	assert.Equal(t, 0, partition.GetTotalAllocationCount(), "removal requests did not remove all allocations: %v", partition.allocations)
	assertLimits(t, getTestUserGroup(), resources.Zero)
}

func TestRemoveAllPlaceholderAllocs(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	setupNode(t, nodeID1, partition, resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000000}))

	// add a new app that will just sit around to make sure we remove the right one
	app := newApplication(appID1, "default", defQueue)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "10"})
	assert.NilError(t, err, "failed to create resource")
	phAsk1 := newAllocationAskTG(phID, appID1, taskGroup, res, true)
	phAlloc1 := objects.NewAllocation(nodeID1, phAsk1)
	err = partition.addAllocation(phAlloc1)
	assert.NilError(t, err, "could not add allocation to partition")
	phAsk2 := newAllocationAskTG(phID2, appID1, taskGroup, res, true)
	phAlloc2 := objects.NewAllocation(nodeID1, phAsk2)
	err = partition.addAllocation(phAlloc2)
	assert.NilError(t, err, "could not add allocation to partition")
	partition.removeAllocation(&si.AllocationRelease{
		PartitionName:   "default",
		ApplicationID:   appID1,
		UUID:            "",
		TerminationType: si.TerminationType_STOPPED_BY_RM,
	})
	assert.Equal(t, 0, partition.getPhAllocationCount())
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

func assertUpdateQueues(t *testing.T, resourceType string, resMap map[string]string) {
	var resExpect *resources.Resource
	var err error
	if len(resMap) > 0 {
		resExpect, err = resources.NewResourceFromConf(resMap)
		assert.NilError(t, err, "resource from conf failed")
	} else {
		resExpect = nil
	}

	var res configs.Resources
	switch resourceType {
	case "max":
		res = configs.Resources{Max: resMap}
	case "guaranteed":
		res = configs.Resources{Guaranteed: resMap}
	default:
		res = configs.Resources{Max: resMap, Guaranteed: resMap}
	}

	conf := []configs.QueueConfig{
		{
			Name:      "parent",
			Parent:    true,
			Resources: res,
			Queues: []configs.QueueConfig{
				{
					Name:   "leaf",
					Parent: false,
					Queues: nil,
				},
			},
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
	parent := partition.GetQueue("root.parent")
	if parent == nil {
		t.Fatal("parent queue should still exist")
	}
	switch resourceType {
	case "max":
		assert.Assert(t, resources.Equals(parent.GetMaxResource(), resExpect), "parent queue max resource should have been updated")
		assert.Assert(t, resources.Equals(parent.GetGuaranteedResource(), nil), "parent queue guaranteed resource should have been updated")
	case "guaranteed":
		assert.Assert(t, resources.Equals(parent.GetMaxResource(), nil), "parent queue max resource should have been updated")
		assert.Assert(t, resources.Equals(parent.GetGuaranteedResource(), resExpect), "parent queue guaranteed resource should have been updated")
	default:
		assert.Assert(t, resources.Equals(parent.GetMaxResource(), resExpect), "parent queue max resource should have been updated")
		assert.Assert(t, resources.Equals(parent.GetGuaranteedResource(), resExpect), "parent queue guaranteed resource should have been updated")
	}
	leaf := partition.GetQueue("root.parent.leaf")
	if leaf == nil {
		t.Fatal("leaf queue should have been created")
	}
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

	assertUpdateQueues(t, "max", map[string]string{"vcore": "2"})
	assertUpdateQueues(t, "max", map[string]string{"vcore": "5"})
	assertUpdateQueues(t, "max", map[string]string{"memory": "5"})
	assertUpdateQueues(t, "guaranteed", map[string]string{"vcore": "2", "memory": "5"})
	assertUpdateQueues(t, "guaranteed", map[string]string{"vcore": "4", "memory": "3"})
	assertUpdateQueues(t, "guaranteed", map[string]string{"vcore": "10"})
	assertUpdateQueues(t, "both", map[string]string{"vcore": "2", "memory": "5"})
	assertUpdateQueues(t, "both", map[string]string{"vcore": "5", "memory": "2"})
	assertUpdateQueues(t, "both", map[string]string{"vcore": "5"})
	assertUpdateQueues(t, "both", map[string]string{})
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

	expectedQueuesMaxLimits := make(map[string]map[string]interface{})
	expectedQueuesMaxLimits["root"] = make(map[string]interface{})
	expectedQueuesMaxLimits["root.leaf"] = make(map[string]interface{})
	expectedQueuesMaxLimits["root"][maxresources] = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 10, "vcores": 10})
	expectedQueuesMaxLimits["root.leaf"][maxresources] = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5, "vcores": 5})
	expectedQueuesMaxLimits["root"][maxapplications] = uint64(10)
	expectedQueuesMaxLimits["root.leaf"][maxapplications] = uint64(1)
	assertUserGroupResourceMaxLimits(t, getTestUserGroup(), nil, expectedQueuesMaxLimits)

	// first allocation should be app-1 and alloc-2
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), allocID2, "expected ask alloc-2 to be allocated")
	assertUserGroupResourceMaxLimits(t, getTestUserGroup(), resources.Multiply(res, 1), expectedQueuesMaxLimits)

	// second allocation should be app-2 and alloc-1: higher up in the queue hierarchy
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID2, "expected application app-2 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), allocID, "expected ask alloc-1 to be allocated")
	assertUserGroupResourceMaxLimits(t, getTestUserGroup(), resources.Multiply(res, 2), expectedQueuesMaxLimits)

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
	assertUserGroupResourceMaxLimits(t, getTestUserGroup(), resources.Multiply(res, 3), expectedQueuesMaxLimits)
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
	assertLimits(t, getTestUserGroup(), nil)

	// first allocation should be app-1 and alloc-1
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc.GetApplicationID(), appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), allocID, "expected ask alloc-1 to be allocated")
	assertLimits(t, getTestUserGroup(), res)

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
	assertLimits(t, getTestUserGroup(), res)

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
	assertLimits(t, getTestUserGroup(), res)
}

// allocate ask request with required node having non daemon set reservations
func TestRequiredNodeCancelNonDSReservations(t *testing.T) {
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

	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "7"})
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

	// the second one should be reserved as the 2nd node is not scheduling
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("2nd allocation did not return the correct allocation")
	}
	// check if updated (must be after allocate call)
	assert.Equal(t, 1, len(app.GetReservations()), "ask should have been reserved")
	assert.Equal(t, 1, len(app.GetQueue().GetReservedApps()), "queue reserved apps should be 1")

	res1, err := resources.NewResourceFromConf(map[string]string{"vcore": "1"})
	assert.NilError(t, err, "failed to create resource")

	// only one resource for alloc fits on a node
	app1 := newApplication(appID2, "default", "root.parent.sub-leaf")
	err = partition.AddApplication(app1)
	assert.NilError(t, err, "failed to add app-2 to partition")

	// required node set on ask
	ask2 := newAllocationAsk("alloc-2", appID2, res1)
	ask2.SetRequiredNode(nodeID1)
	err = app1.AddAllocationAsk(ask2)
	assert.NilError(t, err, "failed to add ask alloc-2 to app-1")

	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("1st allocation did not return the correct allocation")
	}
	assert.Equal(t, objects.Allocated, alloc.GetResult(), "allocation result should have been allocated")

	// earlier app (app1) reservation count should be zero
	assert.Equal(t, 0, len(app.GetReservations()), "ask should have been reserved")
	assert.Equal(t, 0, len(app.GetQueue().GetReservedApps()), "queue reserved apps should be 0")
}

// allocate ask request with required node having daemon set reservations
func TestRequiredNodeCancelDSReservations(t *testing.T) {
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

	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "7"})
	assert.NilError(t, err, "failed to create resource")

	// only one resource for alloc fits on a node
	app := newApplication(appID1, "default", "root.parent.sub-leaf")
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app-1 to partition")

	ask := newAllocationAskRepeat("alloc-1", appID1, res, 2)
	ask.SetRequiredNode(nodeID1)
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

	// the second one should be reserved as the 2nd node is not scheduling
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("2nd allocation should not return allocation")
	}
	// check if updated (must be after allocate call)
	assert.Equal(t, 1, len(app.GetReservations()), "ask should have been reserved")
	assert.Equal(t, 1, len(app.GetQueue().GetReservedApps()), "queue reserved apps should be 1")

	res1, err := resources.NewResourceFromConf(map[string]string{"vcore": "1"})
	assert.NilError(t, err, "failed to create resource")

	// only one resource for alloc fits on a node
	app1 := newApplication(appID2, "default", "root.parent.sub-leaf")
	err = partition.AddApplication(app1)
	assert.NilError(t, err, "failed to add app-2 to partition")

	// required node set on ask
	ask2 := newAllocationAsk("alloc-2", appID2, res1)
	ask2.SetRequiredNode(nodeID1)
	err = app1.AddAllocationAsk(ask2)
	assert.NilError(t, err, "failed to add ask alloc-2 to app-1")

	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("3rd allocation should not return allocation")
	}
	// still reservation count is 1
	assert.Equal(t, 1, len(app.GetReservations()), "ask should have been reserved")
	assert.Equal(t, 1, len(app.GetQueue().GetReservedApps()), "queue reserved apps should be 1")
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
	assertLimits(t, getTestUserGroup(), nil)
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
	assertLimits(t, getTestUserGroup(), res)

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
	assertLimits(t, getTestUserGroup(), resources.Multiply(res, 2))
}

func assertPreemptedResource(t *testing.T, appSummary *objects.ApplicationSummary, memorySeconds int64,
	vcoresSecconds int64) {
	detailedResource := appSummary.PreemptedResource.TrackedResourceMap["UNKNOWN"]
	memValue, memPresent := detailedResource["memory"]
	vcoreValue, vcorePresent := detailedResource["vcore"]

	if memorySeconds != -1 {
		assert.Equal(t, memorySeconds, memValue)
	} else {
		assert.Equal(t, memPresent, false)
	}

	if vcoresSecconds != -1 {
		assert.Equal(t, vcoresSecconds, vcoreValue)
	} else {
		assert.Equal(t, vcorePresent, false)
	}
}

func TestPreemption(t *testing.T) {
	setupUGM()
	partition, app1, app2, alloc1, alloc2 := setupPreemption(t)

	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "5"})
	assert.NilError(t, err, "failed to create resource")

	// ask 3
	ask3 := newAllocationAskPreempt(allocID3, appID2, 1, res)
	err = app2.AddAllocationAsk(ask3)
	assert.NilError(t, err, "failed to add ask alloc-3 to app-2")

	// delay so that preemption delay passes
	// also make the delay 1 second to have a minimum non-zero resource*seconds measurement for preempted resources
	time.Sleep(time.Second)

	// third allocation should not succeed, as we are currently above capacity
	alloc := partition.tryAllocate()
	if alloc != nil {
		t.Fatal("unexpected allocation")
	}

	// alloc-2 (as it is newer) should now be marked preempted
	assert.Assert(t, !alloc1.IsPreempted(), "alloc-1 is preempted")
	assert.Assert(t, alloc2.IsPreempted(), "alloc-2 is not preempted")

	// allocation should still not do anything as we have not yet released the preempted allocation
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("unexpected allocation")
	}

	// currently preempting resources in victim queue should be updated
	preemptingRes := partition.GetQueue("root.parent.leaf1").GetPreemptingResource()
	assert.Assert(t, resources.Equals(preemptingRes, res), "incorrect preempting resources")

	// release alloc-2
	partition.removeAllocation(&si.AllocationRelease{
		PartitionName:   "default",
		ApplicationID:   appID1,
		UUID:            alloc2.GetUUID(),
		TerminationType: si.TerminationType_STOPPED_BY_RM,
		Message:         "Preempted",
		AllocationKey:   allocID2,
	})

	// currently preempting resources in victim queue should be zero
	preemptingRes = partition.GetQueue("root.parent.leaf1").GetPreemptingResource()
	assert.Assert(t, resources.IsZero(preemptingRes), "incorrect preempting resources")

	// allocation should now allocate
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("missing allocation")
	}
	assert.Equal(t, 0, len(app2.GetReservations()), "ask should not be reserved")
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result should be allocated")
	assert.Equal(t, alloc.GetAllocationKey(), allocID3, "expected ask alloc-3 to be allocated")
	assertUserGroupResourceMaxLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 10000}), getExpectedQueuesLimitsForPreemption())

	appSummary := app1.GetApplicationSummary("default")
	assertPreemptedResource(t, appSummary, -1, 5000)

	appSummary = app2.GetApplicationSummary("default")
	assertPreemptedResource(t, appSummary, -1, 0)
}

// Preemption followed by a normal allocation
func TestPreemptionForRequiredNodeNormalAlloc(t *testing.T) {
	setupUGM()
	// setup the partition so we can try the real allocation
	partition, app := setupPreemptionForRequiredNode(t)
	// now try the allocation again: the normal path
	alloc := partition.tryAllocate()
	if alloc != nil {
		t.Fatal("allocations should not have returned an allocation")
	}
	// check if updated (must be after allocate call)
	assert.Equal(t, 1, len(app.GetReservations()), "ask should have been reserved")
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
	assertUserGroupResourceMaxLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 8000}), getExpectedQueuesLimitsForPreemption())
}

func TestPreemptionForRequiredNodeMultipleAttemptsAvoided(t *testing.T) {
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

func getExpectedQueuesLimitsForPreemption() map[string]map[string]interface{} {
	expectedQueuesMaxLimits := make(map[string]map[string]interface{})
	expectedQueuesMaxLimits["root"] = make(map[string]interface{})
	expectedQueuesMaxLimits["root.parent"] = make(map[string]interface{})
	expectedQueuesMaxLimits["root.parent.leaf1"] = make(map[string]interface{})
	expectedQueuesMaxLimits["root"][maxresources] = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 10, "vcores": 10})
	expectedQueuesMaxLimits["root.parent"][maxresources] = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5, "vcores": 5})
	expectedQueuesMaxLimits["root.parent.leaf1"][maxresources] = expectedQueuesMaxLimits["root.parent"][maxresources]
	expectedQueuesMaxLimits["root"][maxapplications] = uint64(10)
	expectedQueuesMaxLimits["root.parent"][maxapplications] = uint64(8)
	expectedQueuesMaxLimits["root.parent.leaf1"][maxapplications] = uint64(8)
	return expectedQueuesMaxLimits
}

func getExpectedQueuesLimitsForPreemptionWithRequiredNode() map[string]map[string]interface{} {
	expectedQueuesMaxLimits := make(map[string]map[string]interface{})
	expectedQueuesMaxLimits["root"] = make(map[string]interface{})
	expectedQueuesMaxLimits["root.leaf"] = make(map[string]interface{})
	expectedQueuesMaxLimits["root.parent"] = make(map[string]interface{})
	expectedQueuesMaxLimits["root.parent.sub-leaf"] = make(map[string]interface{})
	expectedQueuesMaxLimits["root"][maxresources] = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 10, "vcores": 10})
	expectedQueuesMaxLimits["root.leaf"][maxresources] = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5, "vcores": 5})
	expectedQueuesMaxLimits["root.parent"][maxresources] = expectedQueuesMaxLimits["root.leaf"][maxresources]
	expectedQueuesMaxLimits["root.parent.sub-leaf"][maxresources] = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 3, "vcores": 3})
	expectedQueuesMaxLimits["root"][maxapplications] = uint64(10)
	expectedQueuesMaxLimits["root.leaf"][maxapplications] = uint64(1)
	expectedQueuesMaxLimits["root.parent"][maxapplications] = uint64(8)
	expectedQueuesMaxLimits["root.parent.sub-leaf"][maxapplications] = uint64(2)
	return expectedQueuesMaxLimits
}

// setup the partition with existing allocations so we can test preemption
func setupPreemption(t *testing.T) (*PartitionContext, *objects.Application, *objects.Application, *objects.Allocation, *objects.Allocation) {
	partition := createPreemptionQueuesNodes(t)
	if partition == nil {
		t.Fatal("partition create failed")
	}
	if alloc := partition.tryAllocate(); alloc != nil {
		t.Fatalf("empty cluster allocate returned allocation: %v", alloc.String())
	}

	app1, _ := newApplicationWithHandler(appID1, "default", "root.parent.leaf1")
	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "5"})
	assert.NilError(t, err, "failed to create resource")

	// add to the partition
	err = partition.AddApplication(app1)
	assert.NilError(t, err, "failed to add app-1 to partition")

	// ask 1
	ask := newAllocationAskPreempt(allocID, appID1, 2, res)
	err = app1.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app-1")

	// first allocation should be app-1 and alloc-1
	alloc1 := partition.tryAllocate()
	if alloc1 == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc1.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc1.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc1.GetApplicationID(), appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc1.GetAllocationKey(), allocID, "expected ask alloc-1 to be allocated")
	assert.Equal(t, alloc1.GetNodeID(), nodeID1, "expected alloc-1 on node-1")

	assertUserGroupResourceMaxLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 5000}), getExpectedQueuesLimitsForPreemption())

	// ask 2
	ask2 := newAllocationAskPreempt(allocID2, appID1, 1, res)
	err = app1.AddAllocationAsk(ask2)
	assert.NilError(t, err, "failed to add ask alloc-2 to app-1")

	// second allocation should be app-1 and alloc-2
	alloc2 := partition.tryAllocate()
	if alloc2 == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc2.GetResult(), objects.Allocated, "result is not the expected allocated")
	assert.Equal(t, alloc2.GetReleaseCount(), 0, "released allocations should have been 0")
	assert.Equal(t, alloc2.GetApplicationID(), appID1, "expected application app-1 to be allocated")
	assert.Equal(t, alloc2.GetAllocationKey(), allocID2, "expected ask alloc-2 to be allocated")
	assert.Equal(t, alloc2.GetNodeID(), nodeID2, "expected alloc-2 on node-2")
	assertUserGroupResourceMaxLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 10000}), getExpectedQueuesLimitsForPreemption())

	app2, _ := newApplicationWithHandler(appID2, "default", "root.parent.leaf2")

	// add to the partition
	err = partition.AddApplication(app2)
	assert.NilError(t, err, "failed to add app-1 to partition")

	return partition, app1, app2, alloc1, alloc2
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
	assertUserGroupResourceMaxLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 8000}), getExpectedQueuesLimitsForPreemptionWithRequiredNode())
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
	assertUserGroupResourceMaxLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 8000}), getExpectedQueuesLimitsForPreemptionWithRequiredNode())

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
	assert.Equal(t, 0, len(releases), "not expecting any released allocations")
	assertUserGroupResourceMaxLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 0}), getExpectedQueuesLimitsForPreemptionWithRequiredNode())
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
	assertLimits(t, getTestUserGroup(), nil)
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
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 8000}))

	// the second one should be reserved as the 2nd node is not scheduling
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("2nd allocation did not return the correct allocation")
	}
	// check if updated (must be after allocate call)
	assert.Equal(t, 1, len(app.GetReservations()), "ask should have been reserved")
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 8000}))

	// turn on 2nd node
	node2.SetSchedulable(true)
	alloc = partition.tryReservedAllocate()
	assert.Equal(t, objects.AllocatedReserved, alloc.GetResult(), "allocation result should have been allocatedReserved")
	assert.Equal(t, "", alloc.GetReservedNodeID(), "reserved node should be reset after processing")
	assert.Equal(t, node2.NodeID, alloc.GetNodeID(), "allocation should be fulfilled on new node")
	// check if all updated
	node1 := partition.GetNode(nodeID1)
	assert.Equal(t, 0, len(node1.GetReservationKeys()), "old node should have no more reservations")
	assert.Equal(t, 0, len(app.GetReservations()), "ask should have been reserved")
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 16000}))
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
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000}))

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
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 2000}))

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
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return correct an allocation")
	}
	assert.Equal(t, objects.AllocatedReserved, alloc.GetResult(), "expected reserved allocation to be returned")
	assert.Equal(t, "", alloc.GetReservedNodeID(), "reserved node should be reset after processing")
	assert.Equal(t, 0, len(node2.GetReservationKeys()), "reservation should have been removed from node")
	assert.Equal(t, false, app.IsReservedOnNode(node2.NodeID), "reservation cleanup for ask on app failed")
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 5000}))

	// node2 is unreserved now so the next one should allocate on the 2nd node (fair sharing)
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return correct allocation")
	}
	assert.Equal(t, objects.Allocated, alloc.GetResult(), "expected allocated allocation to be returned")
	assert.Equal(t, node2.NodeID, alloc.GetNodeID(), "expected allocation on node2 to be returned")
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 10000}))
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
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 16000}))

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
	assert.Equal(t, len(app.GetReservations()), 2, "application reservations should be 2")
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 16000}))

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
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 20000}))

	// before confirming remove the ask: do what the scheduler does when it gets a request from a
	// shim in processAllocationReleaseByAllocationKey()
	// make sure we are counting correctly and leave the other reservation intact
	removeAskID := "alloc-2"
	if alloc.GetAllocationKey() == "alloc-3" {
		removeAskID = "alloc-3"
	}
	released := app.RemoveAllocationAsk(removeAskID)
	assert.Equal(t, released, 1, "expected one reservations to be released")
	assert.Equal(t, len(app.GetReservations()), 1, "application reservations should be 1")
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 20000}))

	// now confirm the allocation: this should not remove the reservation
	rmAlloc := partition.allocate(alloc)
	assert.Equal(t, "", rmAlloc.GetReservedNodeID(), "reserved node should be reset after processing")
	assert.Equal(t, len(app.GetReservations()), 1, "application reservations should be kept at 1")
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 20000}))
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
				Limits: []configs.Limit{
					{
						Limit: "root queue limit",
						Users: []string{
							"testuser",
						},
						Groups: []string{
							"testgroup",
						},
						MaxResources: map[string]string{
							"memory": "10",
							"vcores": "10",
						},
						MaxApplications: 2,
					},
				},
			},
		},
		PlacementRules: nil,
		Limits:         nil,
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

	// add new node, node 3 with 'memory' resource type
	res1, err1 := resources.NewResourceFromConf(map[string]string{"vcore": "20", "memory": "50"})
	assert.NilError(t, err1, "resource creation failed")
	err = partition.AddNode(newNodeMaxResource("node-3", res1), nil)
	assert.NilError(t, err, "test node3 add failed unexpected")

	// root max resource gets updated with 'memory' resource type
	expRes, err1 := resources.NewResourceFromConf(map[string]string{"vcore": "40", "memory": "50"})
	assert.NilError(t, err1, "resource creation failed")
	assert.Assert(t, resources.Equals(expRes, partition.root.GetMaxResource()), "root max resource not set as expected")

	// remove node, node 3. root max resource won't have 'memory' resource type and updated with less 'vcore'
	partition.removeNode("node-3")
	assert.Assert(t, resources.Equals(res, partition.root.GetMaxResource()), "root max resource not set as expected")

	// remove node, node 2. root max resource gets updated with less 'vcores'
	partition.removeNode("node-2")
	expRes1, err1 := resources.NewResourceFromConf(map[string]string{"vcore": "10"})
	assert.NilError(t, err1, "resource creation failed")
	assert.Assert(t, resources.Equals(expRes1, partition.root.GetMaxResource()), "root max resource not set as expected")

	// remove node, node 1. root max resource should set to nil
	partition.removeNode("node-1")
	assert.Assert(t, resources.Equals(nil, partition.root.GetMaxResource()), "root max resource not set as expected")
}

// transition an application to completed state and wait for it to be processed into the completedApplications map
func completeApplicationAndWait(app *objects.Application, pc *PartitionContext) error {
	currentCount := len(pc.GetCompletedApplications())
	err := app.HandleApplicationEvent(objects.CompleteApplication)
	if err != nil {
		return err
	}

	err = common.WaitFor(10*time.Millisecond, time.Duration(1000)*time.Millisecond, func() bool {
		newCount := len(pc.GetCompletedApplications())
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
	assert.Equal(t, 1, len(partition.GetApplications()), "the partition should have 1 app")
	assert.Equal(t, 0, len(partition.GetCompletedApplications()), "the partition should not have any completed apps")
	// complete the application
	err = completeApplicationAndWait(app, partition)
	assert.NilError(t, err, "the completed application should have been processed")
	assert.Equal(t, 0, len(partition.GetApplications()), "the partition should have no active app")
	assert.Equal(t, 1, len(partition.GetCompletedApplications()), "the partition should have 1 completed app")
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

	assert.Equal(t, 2, len(partition.GetApplications()), "the partition should have 2 apps")
	newApp1.SetState(objects.Expired.String())
	assert.Equal(t, 1, len(partition.getAppsByState(objects.Expired.String())), "the partition should have 1 expired apps")

	partition.cleanupExpiredApps()
	assert.Equal(t, 1, len(partition.GetApplications()), "the partition should have 1 app")
	assert.Equal(t, 0, len(partition.getAppsByState(objects.Expired.String())), "the partition should have 0 expired apps")
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

	assert.Equal(t, 2, len(partition.GetApplications()), "the partition should have 2 apps")
	assert.Equal(t, 0, len(partition.GetCompletedApplications()), "the partition should have 0 completed apps")

	// complete the applications using the event system
	completedApp1.SetState(objects.Completing.String())
	err = completeApplicationAndWait(completedApp1, partition)
	assert.NilError(t, err, "the completed application should have been processed")
	completedApp2.SetState(objects.Completing.String())
	err = completeApplicationAndWait(completedApp2, partition)
	assert.NilError(t, err, "the completed application should have been processed")

	assert.Equal(t, 0, len(partition.GetApplications()), "the partition should have 0 apps")
	assert.Equal(t, 2, len(partition.GetCompletedApplications()), "the partition should have 2 completed apps")
	assert.Equal(t, 2, len(partition.getCompletedAppsByState(objects.Completed.String())), "the partition should have 2 completed apps")

	// mark the app for removal
	completedApp1.SetState(objects.Expired.String())
	assert.Equal(t, 1, len(partition.getCompletedAppsByState(objects.Expired.String())), "the partition should have 1 expired apps")
	assert.Equal(t, 1, len(partition.getCompletedAppsByState(objects.Completed.String())), "the partition should have 1 completed apps")

	partition.cleanupExpiredApps()
	assert.Equal(t, 1, len(partition.GetCompletedApplications()), "the partition should have 1 completed app")
	assert.Equal(t, 0, len(partition.getCompletedAppsByState(objects.Expired.String())), "the partition should have 0 expired apps")
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

	assert.Equal(t, 1, len(partition.rejectedApplications), "the rejectedApplications of the partition should have 1 app")
	assert.Equal(t, 1, len(partition.getRejectedAppsByState(objects.Rejected.String())), "the partition should have 1 rejected app")
	rejectedApp.SetState(objects.Expired.String())
	assert.Equal(t, 0, len(partition.getRejectedAppsByState(objects.Rejected.String())), "the partition should have 0 rejected app")

	partition.cleanupExpiredApps()
	assert.Equal(t, 0, len(partition.rejectedApplications), "the partition should not have app")
	assert.Assert(t, partition.getRejectedApplication(rejectedApp.ApplicationID) == nil, "rejected application should have been deleted")
	assert.Equal(t, 0, len(partition.getRejectedAppsByState(objects.Expired.String())), "the partition should have 0 expired app")
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
	assertLimits(t, getTestUserGroup(), phRes)

	// add an ask which is larger than the placeholder
	ask = newAllocationAskTG(allocID, appID1, taskGroup, tgRes, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app with correct TG")
	// allocate should trigger release of placeholder nothing else
	if partition.tryPlaceholderAllocate() != nil {
		t.Fatal("allocation should not have matched placeholder")
	}
	assert.Assert(t, ph.IsReleased(), "placeholder should be released")
	assertLimits(t, getTestUserGroup(), phRes)

	// release placeholder: do what the context would do after the shim processing
	release := &si.AllocationRelease{
		PartitionName:   ph.GetPartitionName(),
		ApplicationID:   appID1,
		UUID:            ph.GetUUID(),
		TerminationType: si.TerminationType_TIMEOUT,
	}
	assert.Equal(t, 1, partition.getPhAllocationCount(), "ph should be registered")

	released, _ := partition.removeAllocation(release)
	assert.Equal(t, 0, partition.getPhAllocationCount(), "ph should not be registered")

	assert.Equal(t, 0, len(released), "expected no releases")
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "nothing should be allocated on node")
	assert.Assert(t, resources.IsZero(app.GetQueue().GetAllocatedResource()), "nothing should be allocated on queue")
	assert.Equal(t, 0, partition.GetTotalAllocationCount(), "no allocation should be registered on the partition")
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0, "second": 0}))
}

// one real allocation should trigger cleanup of all placeholders
func TestPlaceholderSmallerMulti(t *testing.T) {
	setupUGM()
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
	assert.Equal(t, phCount, partition.GetTotalAllocationCount(), "placeholder allocation should be counted as normal allocations on the partition")
	assert.Equal(t, phCount, partition.getPhAllocationCount(), "placeholder allocation should be counted as placeholders on the partition")
	assertLimits(t, getTestUserGroup(), tgRes)

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
	assertLimits(t, getTestUserGroup(), tgRes)

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
	assert.Equal(t, 0, partition.getPhAllocationCount(), "no placeholder allocation should be on the partition")
	assertLimits(t, getTestUserGroup(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0, "second": 0}))
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
	assert.Equal(t, 1, partition.getPhAllocationCount(), "placeholder allocation should be registered")
	assertLimits(t, getTestUserGroup(), phRes)

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
	assertLimits(t, getTestUserGroup(), phRes)

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
	assert.Equal(t, 0, partition.getPhAllocationCount(), "no placeholder allocation should be registered")
	assert.Assert(t, resources.Equals(smallRes, app.GetQueue().GetAllocatedResource()), "real size should be allocated on queue")
	assert.Assert(t, resources.Equals(smallRes, node.GetAllocatedResource()), "real size should be allocated on node")
	assert.Assert(t, !app.IsCompleting(), "application with allocation should not be in COMPLETING state")
	assertLimits(t, getTestUserGroup(), smallRes)
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
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "placeholder allocation should be registered as allocation")
	assert.Equal(t, 1, partition.getPhAllocationCount(), "placeholder allocation should be registered")
	assertLimits(t, getTestUserGroup(), phRes)

	// add a new ask with an unknown task group (should allocate directly)
	ask = newAllocationAskTG(allocID, appID1, "unknown", phRes, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-2 to app")
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("expected ask to be allocated (unmatched task group)")
	}
	assert.Equal(t, 2, partition.GetTotalAllocationCount(), "allocations should be registered: ph + normal")
	assert.Equal(t, 1, partition.getPhAllocationCount(), "placeholder allocation should be registered")
	assert.Equal(t, allocID, alloc.GetAllocationKey(), "expected allocation of alloc-1 to be returned")
	assert.Equal(t, 1, len(app.GetAllPlaceholderData()), "placeholder data should not be updated")
	assert.Equal(t, int64(1), app.GetAllPlaceholderData()[0].Count, "placeholder data should show 1 available placeholder")
	assert.Equal(t, int64(0), app.GetAllPlaceholderData()[0].Replaced, "placeholder data should show no replacements")
	assertLimits(t, getTestUserGroup(), resources.Multiply(phRes, 2))

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
	assertLimits(t, getTestUserGroup(), resources.Multiply(phRes, 2))

	// replace the placeholder should work
	alloc = partition.tryPlaceholderAllocate()
	if alloc == nil {
		t.Fatal("allocation should have matched placeholder")
	}
	assert.Equal(t, 2, partition.GetTotalAllocationCount(), "allocations should be registered: ph + normal")
	assert.Equal(t, 1, partition.getPhAllocationCount(), "placeholder allocation should be registered")
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
	assert.Equal(t, 2, partition.GetTotalAllocationCount(), "two allocations should be registered")
	assert.Equal(t, 0, partition.getPhAllocationCount(), "no placeholder allocation should be registered")
	assert.Equal(t, int64(1), app.GetAllPlaceholderData()[0].Replaced, "placeholder data should show the replacement")
	assertLimits(t, getTestUserGroup(), resources.Multiply(phRes, 2))

	// add a new ask the same task group as the placeholder
	// all placeholders are used so direct allocation is expected
	ask = newAllocationAskTG("no_ph_used", appID1, taskGroup, phRes, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask no_ph_used to app")
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("expected ask to be allocated no placeholders left")
	}
	assertLimits(t, getTestUserGroup(), resources.Multiply(phRes, 3))
	assert.Equal(t, 3, partition.GetTotalAllocationCount(), "three allocations should be registered")
	assert.Equal(t, 0, partition.getPhAllocationCount(), "no placeholder allocation should be registered")
}

func TestPreemptedPlaceholderSkip(t *testing.T) {
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
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "placeholder allocation should be registered as allocation")
	assert.Equal(t, 1, partition.getPhAllocationCount(), "placeholder allocation should be registered")

	// add a new ask the same task group as the placeholder
	ask = newAllocationAskTG(allocID, appID1, taskGroup, phRes, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app")
	alloc := partition.tryAllocate()
	if alloc != nil {
		t.Fatal("expected ask not to be allocated (matched task group)")
	}

	// mark the placeholder as preempted (shortcut not interested in usage accounting etc.)
	ph.MarkPreempted()

	// replace the placeholder should NOT work
	alloc = partition.tryPlaceholderAllocate()
	if alloc != nil {
		t.Fatal("allocation should not have matched placeholder")
	}

	// release placeholder: do what the context would do after the shim processing
	release := &si.AllocationRelease{
		PartitionName:   "test",
		ApplicationID:   appID1,
		UUID:            phUUID,
		TerminationType: si.TerminationType_PREEMPTED_BY_SCHEDULER,
	}
	released, confirmed := partition.removeAllocation(release)
	assert.Equal(t, len(released), 0, "expecting no released allocation")
	if confirmed != nil {
		t.Fatal("confirmed allocation should be nil")
	}
	assert.Equal(t, int64(1), app.GetAllPlaceholderData()[0].TimedOut, "placeholder data should show the preemption")
	assert.Equal(t, 0, partition.GetTotalAllocationCount(), "no allocation should be registered")
	assert.Equal(t, 0, partition.getPhAllocationCount(), "no placeholder allocation should be registered")

	// normal allocate should work as we have no placeholders left
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("expected ask to be allocated (no placeholder left)")
	}
	assert.Equal(t, allocID, alloc.GetAllocationKey(), "expected allocation of alloc-1 to be returned")
	assert.Equal(t, 1, len(app.GetAllPlaceholderData()), "placeholder data should not be updated")
	assert.Equal(t, int64(1), app.GetAllPlaceholderData()[0].Count, "placeholder data should show 1 available placeholder")
	assert.Equal(t, int64(0), app.GetAllPlaceholderData()[0].Replaced, "placeholder data should show no replacements")
	assert.Equal(t, int64(1), app.GetAllPlaceholderData()[0].TimedOut, "placeholder data should show the preemption")
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "allocation should be registered as allocation")
	assert.Equal(t, 0, partition.getPhAllocationCount(), "placeholder allocation should be registered")
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
	assertLimits(t, getTestUserGroup(), nil)
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
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "placeholder allocation should be counted as alloc")
	assert.Equal(t, 1, partition.getPhAllocationCount(), "placeholder allocation should be registered")
	assertLimits(t, getTestUserGroup(), res)
	// add a second ph ask and run it again it should not match the already allocated placeholder
	ask = newAllocationAskTG("ph-2", appID1, taskGroup, res, true)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add placeholder ask ph-2 to app")
	// try to allocate placeholder should just return
	alloc = partition.tryPlaceholderAllocate()
	if alloc != nil {
		t.Fatalf("placeholder ask should not be allocated: %s", alloc)
	}
	assertLimits(t, getTestUserGroup(), res)
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("expected 2nd placeholder to be allocated")
	}
	assert.Equal(t, node.GetAllocation(alloc.GetUUID()), alloc, "placeholder allocation 2 not found on node")
	if !resources.Equals(app.GetPlaceholderResource(), resources.Multiply(res, 2)) {
		t.Fatalf("placeholder allocation not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), resources.Multiply(res, 2))
	}
	assert.Equal(t, 2, partition.GetTotalAllocationCount(), "placeholder allocation should be counted as alloc")
	assert.Equal(t, 2, partition.getPhAllocationCount(), "placeholder allocation should be registered")
	assertLimits(t, getTestUserGroup(), resources.Multiply(res, 2))

	// not mapping to the same taskgroup should not do anything
	ask = newAllocationAskTG(allocID, appID1, "tg-unk", res, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app")
	alloc = partition.tryPlaceholderAllocate()
	if alloc != nil {
		t.Fatalf("allocation should not have matched placeholder: %s", alloc)
	}
	assertLimits(t, getTestUserGroup(), resources.Multiply(res, 2))

	// add an ask with the TG
	ask = newAllocationAskTG(allocID2, appID1, taskGroup, res, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-2 to app with correct TG")
	alloc = partition.tryPlaceholderAllocate()
	if alloc == nil {
		t.Fatal("allocation should have matched placeholder")
	}
	assert.Equal(t, 2, partition.GetTotalAllocationCount(), "placeholder replacement should not be counted as alloc")
	assert.Equal(t, 2, partition.getPhAllocationCount(), "placeholder allocation should be registered")
	assert.Equal(t, alloc.GetResult(), objects.Replaced, "result is not the expected allocated replaced")
	assert.Equal(t, alloc.GetReleaseCount(), 1, "released allocations should have been 1")
	assertLimits(t, getTestUserGroup(), resources.Multiply(res, 2))
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
	assert.Equal(t, 2, partition.GetTotalAllocationCount(), "still should have 2 allocation after 1 placeholder release")
	assert.Equal(t, 1, partition.getPhAllocationCount(), "placeholder allocation should be removed")
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
	assertLimits(t, getTestUserGroup(), resources.Multiply(res, 2))
}

// The failure is triggered by the predicate plugin and is hidden in the alloc handling
func TestFailReplacePlaceholder(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	if alloc := partition.tryPlaceholderAllocate(); alloc != nil {
		t.Fatalf("empty cluster placeholder allocate returned allocation: %s", alloc)
	}
	// plugin to let the pre-check fail on node-1 only, means we cannot replace the placeholder
	plugin := newFakePredicatePlugin(false, map[string]int{nodeID1: -1})
	plugins.RegisterSchedulerPlugin(plugin)
	defer func() {
		passPlugin := newFakePredicatePlugin(false, nil)
		plugins.RegisterSchedulerPlugin(passPlugin)
	}()
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
	assert.Equal(t, partition.getPhAllocationCount(), 1, "placeholder allocation should be counted as placeholder")
	assert.Equal(t, node.GetAllocation(alloc.GetUUID()), alloc, "placeholder allocation not found on node")
	assert.Assert(t, alloc.IsPlaceholder(), "placeholder alloc should return a placeholder allocation")
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "placeholder alloc should return an allocated result")
	assert.Equal(t, alloc.GetNodeID(), nodeID1, "should be allocated on node-1")
	assert.Assert(t, resources.Equals(app.GetPlaceholderResource(), res), "placeholder allocation not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), res)
	assertLimits(t, getTestUserGroup(), res)

	// add 2nd node to allow allocation
	node2 := setupNode(t, nodeID2, partition, tgRes)
	assertLimits(t, getTestUserGroup(), res)
	// add an ask with the TG
	ask = newAllocationAskTG(allocID, appID1, taskGroup, res, false)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask alloc-1 to app with correct TG")
	alloc = partition.tryPlaceholderAllocate()
	if alloc == nil {
		t.Fatal("allocation should have matched placeholder")
	}
	assert.Equal(t, partition.GetTotalAllocationCount(), 1, "placeholder replacement should not be counted as alloc")
	assert.Equal(t, partition.getPhAllocationCount(), 1, "placeholder allocation should not change")
	assert.Equal(t, alloc.GetResult(), objects.Replaced, "result is not the expected allocated replaced")
	assert.Equal(t, alloc.GetReleaseCount(), 1, "released allocations should have been 1")
	// allocation must be added as it is on a different node
	assert.Equal(t, alloc.GetNodeID(), nodeID2, "should be allocated on node-2")
	assert.Assert(t, resources.IsZero(app.GetAllocatedResource()), "allocated resources should be zero")
	assert.Assert(t, resources.Equals(node.GetAllocatedResource(), res), "node-1 allocation not updated as expected: got %s, expected %s", node.GetAllocatedResource(), res)
	assert.Assert(t, resources.Equals(node2.GetAllocatedResource(), res), "node-2 allocation not updated as expected: got %s, expected %s", node2.GetAllocatedResource(), res)

	phUUID := alloc.GetFirstRelease().GetUUID()
	// placeholder is not released until confirmed by the shim
	assert.Assert(t, resources.Equals(app.GetPlaceholderResource(), res), "placeholder allocation not updated as expected: got %s, expected %s", app.GetPlaceholderResource(), resources.Multiply(res, 2))
	assertLimits(t, getTestUserGroup(), res)

	// release placeholder: do what the context would do after the shim processing
	release := &si.AllocationRelease{
		PartitionName:   "test",
		ApplicationID:   appID1,
		UUID:            phUUID,
		TerminationType: si.TerminationType_PLACEHOLDER_REPLACED,
	}
	released, confirmed := partition.removeAllocation(release)
	assert.Equal(t, partition.GetTotalAllocationCount(), 1, "still should have 1 allocation after placeholder release")
	assert.Equal(t, partition.getPhAllocationCount(), 0, "placeholder allocation should be removed")
	assert.Equal(t, len(released), 0, "not expecting any released allocations")
	if confirmed == nil {
		t.Fatal("confirmed allocation should not be nil")
	}
	assert.Equal(t, confirmed.GetUUID(), alloc.GetUUID(), "confirmed allocation has unexpected uuid")
	assert.Assert(t, resources.IsZero(app.GetPlaceholderResource()), "placeholder resources should be zero")
	assert.Assert(t, resources.Equals(app.GetAllocatedResource(), res), "allocations not updated as expected: got %s, expected %s", app.GetAllocatedResource(), res)
	assert.Assert(t, resources.IsZero(node.GetAllocatedResource()), "node-1 allocated resources should be zero")
	assert.Assert(t, resources.Equals(node2.GetAllocatedResource(), res), "node-2 allocations not set as expected: got %s, expected %s", node2.GetAllocatedResource(), res)
	assert.Assert(t, !app.IsCompleting(), "application with allocation should not be in COMPLETING state")
	assertLimits(t, getTestUserGroup(), res)
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
	assertLimits(t, getTestUserGroup(), nil)
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
	assertLimits(t, getTestUserGroup(), nil)
}

func TestUpdatePreemption(t *testing.T) {
	var True = true
	var False = false

	partition, err := newBasePartition()
	assert.NilError(t, err, "Partition creation failed")
	assert.Assert(t, partition.isPreemptionEnabled(), "preeemption should be enabled by default")

	partition.updatePreemption(configs.PartitionConfig{})
	assert.Assert(t, partition.isPreemptionEnabled(), "preeemption should be enabled by empty config")

	partition.updatePreemption(configs.PartitionConfig{Preemption: configs.PartitionPreemptionConfig{}})
	assert.Assert(t, partition.isPreemptionEnabled(), "preeemption should be enabled by empty preemption section")

	partition.updatePreemption(configs.PartitionConfig{Preemption: configs.PartitionPreemptionConfig{Enabled: nil}})
	assert.Assert(t, partition.isPreemptionEnabled(), "preeemption should be enabled by explicit nil")

	partition.updatePreemption(configs.PartitionConfig{Preemption: configs.PartitionPreemptionConfig{Enabled: &True}})
	assert.Assert(t, partition.isPreemptionEnabled(), "preeemption should be enabled by explicit true")

	partition.updatePreemption(configs.PartitionConfig{Preemption: configs.PartitionPreemptionConfig{Enabled: &False}})
	assert.Assert(t, !partition.isPreemptionEnabled(), "preeemption should be disabled by explicit false")
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
				NodeSortPolicy: configs.NodeSortingPolicy{Type: tt.input},
			})

			ans := partition.nodes.GetNodeSortingPolicy().PolicyType().String()
			if ans != tt.want {
				t.Errorf("got %s, want %s", ans, tt.want)
			}
		})
	}
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

func TestNewQueueEvents(t *testing.T) {
	events.Init()
	eventSystem := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	eventSystem.StartServiceWithPublisher(false)

	partition, err := newBasePartition()
	assert.NilError(t, err)
	_, err = partition.createQueue("root.test", security.UserGroup{
		User: "test",
	})
	assert.NilError(t, err)
	noEvents := 0
	err = common.WaitFor(10*time.Millisecond, time.Second, func() bool {
		noEvents = eventSystem.Store.CountStoredEvents()
		return noEvents == 3
	})
	assert.NilError(t, err, "expected 3 events, got %d", noEvents)
	records := eventSystem.Store.CollectEvents()
	assert.Equal(t, si.EventRecord_QUEUE, records[0].Type)
	assert.Equal(t, si.EventRecord_ADD, records[0].EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, records[0].EventChangeDetail)
	assert.Equal(t, "root", records[0].ObjectID)
	assert.Equal(t, si.EventRecord_QUEUE, records[1].Type)
	assert.Equal(t, si.EventRecord_ADD, records[1].EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, records[1].EventChangeDetail)
	assert.Equal(t, "root.default", records[1].ObjectID)
	assert.Equal(t, si.EventRecord_QUEUE, records[2].Type)
	assert.Equal(t, si.EventRecord_ADD, records[2].EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_DYNAMIC, records[2].EventChangeDetail)
	assert.Equal(t, "root.test", records[2].ObjectID)
}

func TestUserHeadroom(t *testing.T) {
	setupUGM()
	partition, err := newConfiguredPartition()
	assert.NilError(t, err, "test partition create failed with error")
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"memory": "10", "vcores": "10"})
	assert.NilError(t, err, "failed to create basic resource")
	err = partition.AddNode(newNodeMaxResource("node-1", res), nil)
	assert.NilError(t, err, "test node1 add failed unexpected")
	err = partition.AddNode(newNodeMaxResource("node-2", res), nil)
	assert.NilError(t, err, "test node2 add failed unexpected")

	app1 := newApplication(appID1, "default", "root.parent.sub-leaf")
	res, err = resources.NewResourceFromConf(map[string]string{"memory": "3", "vcores": "3"})
	assert.NilError(t, err, "failed to create resource")

	err = partition.AddApplication(app1)
	assert.NilError(t, err, "failed to add app-1 to partition")
	err = app1.AddAllocationAsk(newAllocationAsk(allocID, appID1, res))
	assert.NilError(t, err, "failed to add ask alloc-1 to app-1")

	app2 := newApplication(appID2, "default", "root.parent.sub-leaf")
	err = partition.AddApplication(app2)
	assert.NilError(t, err, "failed to add app-2 to partition")
	err = app2.AddAllocationAsk(newAllocationAsk(allocID, appID2, res))
	assert.NilError(t, err, "failed to add ask alloc-1 to app-2")

	// app 1 would be allocated as there is headroom available for the user
	alloc := partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")

	// app 2 allocation won't happen as there is no headroom for the user
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("allocation should not happen")
	}

	res1, err := resources.NewResourceFromConf(map[string]string{"memory": "5", "vcores": "5"})
	assert.NilError(t, err, "failed to create resource")

	app3 := newApplication(appID3, "default", "root.leaf")
	err = partition.AddApplication(app3)
	assert.NilError(t, err, "failed to add app-3 to partition")
	err = app3.AddAllocationAsk(newAllocationAsk(allocID, appID3, res1))
	assert.NilError(t, err, "failed to add ask alloc-1 to app-3")

	// app 3 would be allocated as there is headroom available for the user
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")

	app4 := newApplication("app-4", "default", "root.leaf")
	err = partition.AddApplication(app4)
	assert.NilError(t, err, "failed to add app-4 to partition")
	err = app4.AddAllocationAsk(newAllocationAsk(allocID, "app-4", res1))
	assert.NilError(t, err, "failed to add ask alloc-1 to app-4")

	// app 4 allocation won't happen as there is no headroom for the user
	alloc = partition.tryAllocate()
	if alloc != nil {
		t.Fatal("allocation should not happen")
	}
	partition.removeApplication(appID1)
	partition.removeApplication(appID2)
	partition.removeApplication(appID3)
	partition.removeApplication("app-4")

	// create a reservation and ensure reservation has been allocated because there is enough headroom for the user to run the app
	app5 := newApplication("app-5", "default", "root.parent.sub-leaf")
	err = partition.AddApplication(app5)
	assert.NilError(t, err, "failed to add app-5 to partition")

	res, err = resources.NewResourceFromConf(map[string]string{"memory": "3", "vcores": "3"})
	assert.NilError(t, err, "failed to create resource")
	ask := newAllocationAskRepeat("alloc-1", "app-5", res, 2)
	err = app5.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask to app")

	node2 := partition.GetNode(nodeID2)
	if node2 == nil {
		t.Fatal("expected node-2 to be returned got nil")
	}
	partition.reserve(app5, node2, ask)

	// turn off the second node
	node1 := partition.GetNode(nodeID1)
	node1.SetSchedulable(false)

	alloc = partition.tryReservedAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, objects.AllocatedReserved, alloc.GetResult(), "allocation result should have been allocated")

	// create a reservation and ensure reservation has not been allocated because there is no headroom for the user
	ask = newAllocationAskRepeat("alloc-2", "app-5", res, 2)
	err = app5.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask to app")
	partition.reserve(app5, node2, ask)
	alloc = partition.tryReservedAllocate()
	if alloc != nil {
		t.Fatal("allocation should not happen on other nodes as well")
	}
	partition.removeApplication("app-5")

	app6 := newApplicationWithUser("app-6", "default", "root.parent.sub-leaf", security.UserGroup{
		User:   "testuser1",
		Groups: []string{"testgroup1"},
	})
	res, err = resources.NewResourceFromConf(map[string]string{"memory": "3", "vcores": "3"})
	assert.NilError(t, err, "failed to create resource")

	err = partition.AddApplication(app6)
	assert.NilError(t, err, "failed to add app-6 to partition")
	err = app6.AddAllocationAsk(newAllocationAsk(allocID, "app-6", res))
	assert.NilError(t, err, "failed to add ask alloc-1 to app-6")

	// app 6 would be allocated as headroom is nil because no limits configured for 'testuser1' user an
	alloc = partition.tryAllocate()
	if alloc == nil {
		t.Fatal("allocation did not return any allocation")
	}
	assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
}

func TestPlaceholderAllocationTracking(t *testing.T) {
	const phID3 = "ph-3"
	setupUGM()
	partition := createQueuesNodes(t)
	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "1"})
	assert.NilError(t, err, "failed to create resource")

	// add the app with placeholder request
	app := newApplication(appID1, "default", "root.parent.sub-leaf")
	err = partition.AddApplication(app)
	assert.NilError(t, err, "app-1 should have been added to the partition")
	// add three asks for a placeholder and allocate
	ask1 := newAllocationAskTG(phID, appID1, taskGroup, res, true)
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err, "failed to add placeholder ask to app")
	ask2 := newAllocationAskTG(phID2, appID1, taskGroup, res, true)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "could not add ask")
	ask3 := newAllocationAskTG(phID3, appID1, taskGroup, res, true)
	err = app.AddAllocationAsk(ask3)
	assert.NilError(t, err, "could not add ask")
	assert.Equal(t, 0, partition.getPhAllocationCount())
	// add & allocate real asks
	ask4 := newAllocationAskTG(allocID, appID1, taskGroup, res, false)
	err = app.AddAllocationAsk(ask4)
	assert.NilError(t, err, "failed to add ask to app")

	// allocate first placeholder
	alloc := partition.tryAllocate()
	ph1uuid := alloc.GetUUID()
	assert.Assert(t, alloc != nil, "placeholder ask should have been allocated")
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "placeholder not counted as alloc")
	assert.Equal(t, 1, partition.getPhAllocationCount(), "placeholder not counted as placeholder")
	alloc1 := partition.tryPlaceholderAllocate()
	assert.Assert(t, alloc1 != nil, "ask should have been allocated")
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "should see no change in alloc count")
	assert.Equal(t, 1, partition.getPhAllocationCount(), "should see no change in placeholder count")

	// allocate second placeholder
	alloc = partition.tryAllocate()
	ph2uuid := alloc.GetUUID()
	assert.Assert(t, alloc != nil, "placeholder ask should have been allocated")
	assert.Equal(t, 2, partition.GetTotalAllocationCount(), "placeholder not counted as alloc")
	assert.Equal(t, 2, partition.getPhAllocationCount(), "placeholder not counted as placeholder")
	// allocate third placeholder
	alloc = partition.tryAllocate()
	ph3uuid := alloc.GetUUID()
	assert.Assert(t, alloc != nil, "placeholder ask should have been allocated")
	assert.Equal(t, 3, partition.GetTotalAllocationCount(), "placeholder not counted as alloc")
	assert.Equal(t, 3, partition.getPhAllocationCount(), "placeholder not counted as placeholder")

	partition.removeAllocation(&si.AllocationRelease{
		UUID:            ph1uuid,
		ApplicationID:   appID1,
		TerminationType: si.TerminationType_PLACEHOLDER_REPLACED,
	})
	assert.Equal(t, 3, partition.GetTotalAllocationCount(), "placeholder not counted as alloc")
	assert.Equal(t, 2, partition.getPhAllocationCount(), "placeholder should be removed from count")
	partition.removeAllocation(&si.AllocationRelease{
		UUID:            ph2uuid,
		ApplicationID:   appID1,
		TerminationType: si.TerminationType_STOPPED_BY_RM,
	})
	assert.Equal(t, 2, partition.GetTotalAllocationCount(), "placeholder should be removed from alloc count")
	assert.Equal(t, 1, partition.getPhAllocationCount(), "placeholder should be removed from count")
	partition.removeAllocation(&si.AllocationRelease{
		UUID:            ph3uuid,
		ApplicationID:   appID1,
		TerminationType: si.TerminationType_TIMEOUT,
	})
	assert.Equal(t, 1, partition.GetTotalAllocationCount(), "placeholder should be removed from alloc count")
	assert.Equal(t, 0, partition.getPhAllocationCount(), "placeholder should be removed from count")
}

func TestReservationTracking(t *testing.T) {
	setupUGM()
	partition := createQueuesNodes(t)

	app := newApplication(appID1, "default", "root.parent.sub-leaf")
	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "10"})
	assert.NilError(t, err, "failed to create resource")

	// add to the partition
	err = partition.AddApplication(app)
	assert.NilError(t, err, "failed to add app to partition")
	ask1 := newAllocationAsk(allocID, appID1, res)
	ask1.SetRequiredNode(nodeID1)
	err = app.AddAllocationAsk(ask1)
	assert.NilError(t, err, "failed to add ask")
	ask2 := newAllocationAsk(allocID2, appID1, res)
	ask2.SetRequiredNode(nodeID1)
	err = app.AddAllocationAsk(ask2)
	assert.NilError(t, err, "failed to add ask")

	alloc := partition.tryAllocate() // ask1 occupies node1
	uuid := alloc.GetUUID()
	assert.Equal(t, objects.Allocated, alloc.GetResult())
	assert.Equal(t, "alloc-1", alloc.GetAllocationKey())
	assert.Equal(t, 0, partition.getReservationCount())
	alloc = partition.tryAllocate() // ask2 gets reserved
	assert.Assert(t, alloc == nil)
	assert.Equal(t, 1, partition.getReservationCount())

	partition.removeAllocation(&si.AllocationRelease{
		UUID:            uuid,
		ApplicationID:   appID1,
		TerminationType: si.TerminationType_STOPPED_BY_RM,
	}) // terminate ask1
	alloc = partition.tryReservedAllocate() // allocate reservation
	assert.Equal(t, 0, partition.getReservationCount())
	assert.Equal(t, "alloc-2", alloc.GetAllocationKey())
}

//nolint:funlen
func TestLimitMaxApplications(t *testing.T) {
	testCases := []struct {
		name   string
		limits []configs.Limit
	}{
		{
			name: "specific user",
			limits: []configs.Limit{
				{
					Limit:           "specific user limit",
					Users:           []string{"testuser"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 1,
				},
			},
		},
		{
			name: "specific group",
			limits: []configs.Limit{
				{
					Limit:           "specific group limit",
					Groups:          []string{"testgroup"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 1,
				},
			},
		},
		{
			name: "wildcard user",
			limits: []configs.Limit{
				{
					Limit:           "wildcard user limit",
					Users:           []string{"*"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 1,
				},
			},
		},
		{
			name: "wildcard group",
			limits: []configs.Limit{
				{
					Limit:           "specific group limit",
					Groups:          []string{"nonexistent-group"},
					MaxResources:    map[string]string{"memory": "500", "vcores": "500"},
					MaxApplications: 100,
				},
				{
					Limit:           "wildcard group limit",
					Groups:          []string{"*"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 1,
				},
			},
		},
		{
			name: "specific user lower than specific group limit",
			limits: []configs.Limit{
				{
					Limit:           "specific user limit",
					Users:           []string{"testuser"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 1,
				},
				{
					Limit:           "specific user limit",
					Groups:          []string{"testgroup"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 100,
				},
			},
		},
		{
			name: "specific group lower than specific user limit",
			limits: []configs.Limit{
				{
					Limit:           "specific user limit",
					Users:           []string{"testuser"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 100,
				},
				{
					Limit:           "specific group limit",
					Groups:          []string{"testgroup"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 1,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setupUGM()
			conf := configs.PartitionConfig{
				Name: "default",
				Queues: []configs.QueueConfig{
					{
						Name:      "root",
						Parent:    true,
						SubmitACL: "*",
						Queues: []configs.QueueConfig{
							{
								Name:   "default",
								Parent: false,
								Limits: tc.limits,
							},
						},
					},
				},
				NodeSortPolicy: configs.NodeSortingPolicy{},
			}

			partition, err := newPartitionContext(conf, rmID, nil)
			assert.NilError(t, err, "partition create failed")

			// add node1
			nodeRes, err := resources.NewResourceFromConf(map[string]string{"memory": "10", "vcores": "10"})
			assert.NilError(t, err, "failed to create basic resource")
			err = partition.AddNode(newNodeMaxResource("node-1", nodeRes), nil)
			assert.NilError(t, err, "test node1 add failed unexpected")

			resMap := map[string]string{"memory": "2", "vcores": "2"}
			res, err := resources.NewResourceFromConf(resMap)
			assert.NilError(t, err, "Unexpected error when creating resource from map")

			// add app1
			app1 := newApplication(appID1, "default", defQueue)
			err = partition.AddApplication(app1)
			assert.NilError(t, err, "add application to partition should not have failed")
			err = app1.AddAllocationAsk(newAllocationAsk(allocID, appID1, res))
			assert.NilError(t, err, "failed to add ask alloc-1 to app-1")

			alloc := partition.tryAllocate()
			if alloc == nil {
				t.Fatal("allocation did not return any allocation")
			}
			assert.Equal(t, alloc.GetResult(), objects.Allocated, "result is not the expected allocated")
			assert.Equal(t, alloc.GetApplicationID(), appID1, "expected application app-1 to be allocated")
			assert.Equal(t, alloc.GetAllocationKey(), allocID, "expected ask alloc-1 to be allocated")

			// add app2
			app2 := newApplication(appID2, "default", defQueue)
			err = partition.AddApplication(app2)
			assert.NilError(t, err, "add application to partition should not have failed")
			err = app2.AddAllocationAsk(newAllocationAsk(allocID2, appID2, res))
			assert.NilError(t, err, "failed to add ask alloc-2 to app-1")
			assert.Equal(t, app2.CurrentState(), objects.Accepted.String(), "application should have moved to accepted state")

			alloc = partition.tryAllocate()
			assert.Equal(t, alloc == nil, true, "allocation should not have happened as max apps reached")
		})
	}
}

//nolint:funlen
func TestLimitMaxApplicationsForReservedAllocation(t *testing.T) {
	testCases := []struct {
		name   string
		limits []configs.Limit
	}{
		{
			name: "specific user",
			limits: []configs.Limit{
				{
					Limit:           "specific user limit",
					Users:           []string{"testuser"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 1,
				},
			},
		},
		{
			name: "specific group",
			limits: []configs.Limit{
				{
					Limit:           "specific group limit",
					Groups:          []string{"testgroup"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 1,
				},
			},
		},
		{
			name: "wildcard user",
			limits: []configs.Limit{
				{
					Limit:           "wildcard user limit",
					Users:           []string{"*"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 1,
				},
			},
		},
		{
			name: "wildcard group",
			limits: []configs.Limit{
				{
					Limit:           "specific group limit",
					Groups:          []string{"nonexistent-group"},
					MaxResources:    map[string]string{"memory": "500", "vcores": "500"},
					MaxApplications: 100,
				},
				{
					Limit:           "wildcard group limit",
					Groups:          []string{"*"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 1,
				},
			},
		},
		{
			name: "specific user lower than specific group limit",
			limits: []configs.Limit{
				{
					Limit:           "specific user limit",
					Users:           []string{"testuser"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 1,
				},
				{
					Limit:           "specific user limit",
					Groups:          []string{"testgroup"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 100,
				},
			},
		},
		{
			name: "specific group lower than specific user limit",
			limits: []configs.Limit{
				{
					Limit:           "specific user limit",
					Users:           []string{"testuser"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 100,
				},
				{
					Limit:           "specific group limit",
					Groups:          []string{"testgroup"},
					MaxResources:    map[string]string{"memory": "5", "vcores": "5"},
					MaxApplications: 1,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setupUGM()
			conf := configs.PartitionConfig{
				Name: "default",
				Queues: []configs.QueueConfig{
					{
						Name:      "root",
						Parent:    true,
						SubmitACL: "*",
						Queues: []configs.QueueConfig{
							{
								Name:   "default",
								Parent: false,
								Limits: tc.limits,
							},
						},
					},
				},
				NodeSortPolicy: configs.NodeSortingPolicy{},
			}

			partition, err := newPartitionContext(conf, rmID, nil)
			assert.NilError(t, err, "partition create failed")

			// add node1
			nodeRes, err := resources.NewResourceFromConf(map[string]string{"memory": "10", "vcores": "10"})
			assert.NilError(t, err, "failed to create basic resource")
			node := newNodeMaxResource("node-1", nodeRes)
			err = partition.AddNode(node, nil)
			assert.NilError(t, err, "test node1 add failed unexpected")

			resMap := map[string]string{"memory": "2", "vcores": "2"}
			res, err := resources.NewResourceFromConf(resMap)
			assert.NilError(t, err, "Unexpected error when creating resource from map")

			// add app1
			app1 := newApplication(appID1, "default", defQueue)
			err = partition.AddApplication(app1)
			assert.NilError(t, err, "add application to partition should not have failed")
			app1AllocAsk := newAllocationAsk(allocID, appID1, res)
			err = app1.AddAllocationAsk(app1AllocAsk)
			assert.NilError(t, err, "failed to add ask alloc-1 to app-1")

			partition.reserve(app1, node, app1AllocAsk)
			alloc := partition.tryReservedAllocate()
			if alloc == nil {
				t.Fatal("allocation did not return any allocation")
			}
			assert.Equal(t, alloc.GetResult(), objects.AllocatedReserved, "result is not the expected allocated reserved")
			assert.Equal(t, alloc.GetApplicationID(), appID1, "expected application app-1 to be allocated reserved")
			assert.Equal(t, alloc.GetAllocationKey(), allocID, "expected ask alloc-1 to be allocated reserved")

			// add app2
			app2 := newApplication(appID2, "default", defQueue)
			err = partition.AddApplication(app2)
			assert.NilError(t, err, "add application to partition should not have failed")
			app2AllocAsk := newAllocationAsk(allocID2, appID2, res)
			err = app2.AddAllocationAsk(app2AllocAsk)
			assert.NilError(t, err, "failed to add ask alloc-2 to app-1")
			assert.Equal(t, app2.CurrentState(), objects.Accepted.String(), "application should have moved to accepted state")

			partition.reserve(app2, node, app2AllocAsk)
			alloc = partition.tryReservedAllocate()
			assert.Equal(t, alloc == nil, true, "allocation should not have happened as max apps reached")
		})
	}
}
