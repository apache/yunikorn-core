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
	"reflect"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/commonevents"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

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

func waitForPartitionState(t *testing.T, partition *PartitionInfo, state string, timeoutMs int) {
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

	partition, err := CreatePartitionInfo([]byte(data))
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

	partition, err := CreatePartitionInfo([]byte(data))
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
	partition, err := CreatePartitionInfo([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")
	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := NewNodeForTest(nodeID, resources.NewResourceFromMap(
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
	node2 := NewNodeForTest(nodeID, resources.NewResourceFromMap(
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
	node3 := NewNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))
	// add a third node to the test
	err = partition.addNewNode(node3, nil)
	if err == nil || partition.GetNode(nodeID) != nil {
		t.Errorf("add new node to removed partition should have failed")
	}
}

func TestRemoveNode(t *testing.T) {
	partition, err := CreatePartitionInfo([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")
	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := NewNodeForTest(nodeID, resources.NewResourceFromMap(
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
	partition, err := CreatePartitionInfo([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")
	// add a new app
	appID := "app-1"
	queueName := "root.default"
	appInfo := newApplicationInfo(appID, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	if err != nil {
		t.Errorf("add application to partition should not have failed: %v", err)
	}

	// add a node with allocations: must have the correct app added already
	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := NewNodeForTest(nodeID, resources.NewResourceFromMap(
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
	allocUUID := allocated[0].AllocationProto.UUID

	// add broken allocations
	res := resources.NewResourceFromMap(map[string]resources.Quantity{resources.MEMORY: 1})
	node1.allocations["notanapp"] = CreateMockAllocationInfo("notanapp", res, "noanapp", "root.default", nodeID)
	node1.allocations["notanalloc"] = CreateMockAllocationInfo(appID, res, "notanalloc", "root.default", nodeID)

	// remove the node this cannot fail
	released := partition.RemoveNode(nodeID)
	if partition.GetTotalNodeCount() != 0 {
		t.Errorf("node list was not updated, node was not removed expected 0 got %d", partition.GetTotalNodeCount())
	}
	if len(released) != 1 {
		t.Errorf("node did not release correct allocation expected 1 got %d", len(released))
	}
	assert.Equal(t, released[0].AllocationProto.UUID, allocUUID, "UUID returned by release not the same as on allocation")
}

func TestAddNewApplication(t *testing.T) {
	partition, err := CreatePartitionInfo([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")

	// add a new app
	appInfo := newApplicationInfo("app-1", "default", "root.default")
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

	appInfo = newApplicationInfo("app-2", "default", "root.default")
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
	appInfo = newApplicationInfo("app-3", "default", "root.default")
	err = partition.addNewApplication(appInfo, true)
	if err == nil || partition.getApplication("app-3") != nil {
		t.Errorf("add application on draining partition should have failed but did not")
	}
}

func TestAddNodeWithAllocations(t *testing.T) {
	partition, err := CreatePartitionInfo([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")

	// add a new app
	appID := "app-1"
	queueName := "root.default"
	appInfo := newApplicationInfo(appID, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	if err != nil {
		t.Errorf("add application to partition should not have failed: %v", err)
	}

	// add a node with allocations: must have the correct app added already
	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := NewNodeForTest(nodeID, resources.NewResourceFromMap(
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
			qi.GetQueuePath(), qi.allocatedResource.Resources[resources.MEMORY])
	}
	// check the root queue usage
	qi = partition.getQueue("root")
	if qi.allocatedResource.Resources[resources.MEMORY] != 1 {
		t.Errorf("add node to partition did not add queue %s allocation expected 1 got %d",
			qi.GetQueuePath(), qi.allocatedResource.Resources[resources.MEMORY])
	}

	nodeID = "node-partial-fail"
	node2 := NewNodeForTest(nodeID, resources.NewResourceFromMap(
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
	partition, err := CreatePartitionInfo([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")

	// add a new app
	appID := "app-1"
	queueName := "root.default"
	appInfo := newApplicationInfo(appID, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	if err != nil {
		t.Errorf("add application to partition should not have failed: %v", err)
	}

	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := NewNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))
	// add a node this must work
	err = partition.addNewNode(node1, nil)
	if err != nil || partition.GetNode(nodeID) == nil {
		t.Fatalf("add node to partition should not have failed: %v", err)
	}

	alloc, err := partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-1", appID))
	if err != nil {
		t.Errorf("adding allocation failed and should not have failed: %v", err)
	}
	if partition.allocations[alloc.AllocationProto.UUID] == nil {
		t.Errorf("add allocation to partition not found in the allocation list")
	}
	// check the leaf queue usage
	qi := partition.getQueue(queueName)
	if qi.allocatedResource.Resources[resources.MEMORY] != 1 {
		t.Errorf("add allocation to partition did not add queue %s allocation expected 1 got %d",
			qi.GetQueuePath(), qi.allocatedResource.Resources[resources.MEMORY])
	}

	alloc, err = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-2", appID))
	if err != nil || partition.allocations[alloc.AllocationProto.UUID] == nil {
		t.Errorf("adding allocation failed and should not have failed: %v", err)
	}
	// check the root queue usage
	qi = partition.getQueue(queueName)
	if qi.allocatedResource.Resources[resources.MEMORY] != 2 {
		t.Errorf("add allocation to partition did not add queue %s allocation expected 2 got %d",
			qi.GetQueuePath(), qi.allocatedResource.Resources[resources.MEMORY])
	}

	alloc, err = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-3", "does-not-exist"))
	if err == nil || alloc != nil || len(partition.allocations) != 2 {
		t.Errorf("adding allocation worked and should have failed: %v", alloc)
	}

	// mark the node as not schedulable: allocation must fail
	node1.SetSchedulable(false)
	alloc, err = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-4", appID))
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
	alloc, err = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-4", appID))
	if err == nil || alloc != nil || len(partition.allocations) != 2 {
		t.Errorf("adding allocation worked and should have failed: %v", alloc)
	}
	// mark partition for removal, no new application can be added
	partition.stateMachine.SetState(Active.String())
	if err = partition.handlePartitionEvent(Remove); err != nil {
		t.Fatalf("partition state change failed: %v", err)
	}
	waitForPartitionState(t, partition, Draining.String(), 1000)
	alloc, err = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-4", appID))
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

	partition, err := CreatePartitionInfo([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")

	// add a new app
	appInfo := newApplicationInfo(appID, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	if err != nil {
		t.Errorf("add application to partition should not have failed: %v", err)
	}

	// add a node with 2 existing allocations
	alloc1Res := &si.Resource{Resources: map[string]*si.Quantity{resources.MEMORY: {Value: 200}}}
	alloc2Res := &si.Resource{Resources: map[string]*si.Quantity{resources.MEMORY: {Value: 300}}}
	nodeTotal := &si.Resource{Resources: map[string]*si.Quantity{resources.MEMORY: {Value: 1000}}}
	node1 := NewNodeForTest(nodeID, resources.NewResourceFromProto(nodeTotal))
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
	assert.Equal(t, alloc1.AllocationProto.UUID, "alloc-UUID-1")
	assert.Equal(t, alloc2.AllocationProto.UUID, "alloc-UUID-2")
	assert.Assert(t, resources.Equals(alloc1.AllocatedResource, resources.NewResourceFromProto(alloc1Res)))
	assert.Assert(t, resources.Equals(alloc2.AllocatedResource, resources.NewResourceFromProto(alloc2Res)))
	assert.Equal(t, alloc1.ApplicationID, "app-1")
	assert.Equal(t, alloc2.ApplicationID, "app-1")

	// verify node resource
	assert.Assert(t, resources.Equals(node1.GetCapacity(), resources.NewResourceFromProto(nodeTotal)))
	assert.Assert(t, resources.Equals(node1.GetAllocatedResource(),
		resources.Add(resources.NewResourceFromProto(alloc1Res), resources.NewResourceFromProto(alloc2Res))))

	// add a node with 1 existing allocation that does not have a UUID, must fail
	node2 := NewNodeForTest(nodeID2, resources.NewResourceFromProto(nodeTotal))
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
	partition, err := CreatePartitionInfo([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")
	// add a new app that will just sit around to make sure we remove the right one
	appNotRemoved := "will_not_remove"
	queueName := "root.default"
	appInfo := newApplicationInfo(appNotRemoved, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	assert.NilError(t, err, "add application to partition should not have failed")
	// add a node to allow adding an allocation
	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := NewNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))
	// add a node this must work
	err = partition.addNewNode(node1, nil)
	if err != nil || partition.GetNode(nodeID) == nil {
		t.Fatalf("add node to partition should not have failed: %v", err)
	}
	alloc, err := partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc_not_removed", appNotRemoved))
	assert.NilError(t, err, "add allocation to partition should not have failed")
	uuid := alloc.AllocationProto.UUID

	app, allocs := partition.RemoveApplication("does_not_exist")
	if app != nil && len(allocs) != 0 {
		t.Errorf("non existing application returned unexpected values: application info %v (allocs = %v)", app, allocs)
	}

	// add another new app
	appID := "app-1"
	appInfo = newApplicationInfo(appID, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	if err != nil {
		t.Errorf("add application to partition should not have failed: %v", err)
	}

	// remove the newly added app (no allocations)
	app, allocs = partition.RemoveApplication(appID)
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
	alloc, err = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-1", appID))
	if err != nil || alloc == nil {
		t.Fatalf("add allocation to partition should not have failed: %v", err)
	}

	// remove the newly added app
	app, allocs = partition.RemoveApplication(appID)
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
	partition, err := CreatePartitionInfo([]byte(configDefault))
	assert.NilError(t, err, "partition create failed")
	// add a new app that will just sit around to make sure we remove the right one
	appNotRemoved := "will_not_remove"
	queueName := "root.default"
	appInfo := newApplicationInfo(appNotRemoved, "default", queueName)
	err = partition.addNewApplication(appInfo, true)
	assert.NilError(t, err, "add application to partition should not have failed")
	// add a node to allow adding an allocation
	memVal := resources.Quantity(1000)
	nodeID := "node-1"
	node1 := NewNodeForTest(nodeID, resources.NewResourceFromMap(
		map[string]resources.Quantity{resources.MEMORY: memVal}))
	// add a node this must work
	err = partition.addNewNode(node1, nil)
	if err != nil || partition.GetNode(nodeID) == nil {
		t.Fatalf("add node to partition should not have failed: %v", err)
	}
	var alloc *AllocationInfo
	alloc, err = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc_not_removed", appNotRemoved))
	if err != nil || alloc == nil {
		t.Fatalf("add allocation to partition should not have failed: %v", err)
	}
	alloc, err = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-1", appNotRemoved))
	assert.NilError(t, err, "add allocation to partition should not have failed")
	uuid := alloc.AllocationProto.UUID

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
	partition, err := CreatePartitionInfo([]byte(configDefault))
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
	if queue != nil && !queue.isLeaf && !queue.isManaged {
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
	if !queue.isLeaf && !queue.isManaged {
		t.Errorf("'root.parent.test' queue not created with correct settings: %v", queue)
	}
	queue = queue.Parent
	if queue == nil {
		t.Errorf("'root.parent' queue creation failed: parent is not set correctly")
	}
	if queue != nil && queue.isLeaf && !queue.isManaged {
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
	if queue != nil && !queue.isLeaf && !queue.isManaged {
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

	partition, err := CreatePartitionInfo([]byte(data))
	assert.NilError(t, err, "partition create failed")

	n1 := NewNodeForTest("host1", resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 100, "vcore": 100}))
	n2 := NewNodeForTest("host2", resources.NewResourceFromMap(
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

	n3 := NewNodeForTest("host3", resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 100, "vcore": 100}))
	if err = partition.addNewNode(n3, nil); err != nil {
		t.Error(err)
	}

	m = partition.CalculateNodesResourceUsage()
	assert.Equal(t, len(m), 2)
	assert.Assert(t, reflect.DeepEqual(m["memory"], []int{1, 1, 0, 0, 0, 0, 0, 0, 1, 0}))
}
