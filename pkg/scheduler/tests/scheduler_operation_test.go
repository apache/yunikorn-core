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

package tests

import (
	"testing"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
	"gotest.tools/assert"
)

// this test simulates the scenario the cluster starts up with 0 nodes
// then we submit an app, the app tasks will be pending; then we add a
// node to the cluster, then we see the app gets the allocation it needed.
func TestSchedulerWithoutNodes(t *testing.T) {
	// Register RM
	configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
              max:
                memory: 150
                vcore: 20
`
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Check queues of cache and scheduler.
	partitionInfo := ms.clusterInfo.GetPartition("[rm:123]default")
	assert.Assert(t, partitionInfo.Root.GetMaxResource() == nil, "partition info max resource nil")

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")
	assert.Assert(t, schedulerQueueRoot.QueueInfo.GetMaxResource() == nil)

	// Check scheduling queue a
	schedulerQueueA := ms.getSchedulingQueue("root.a")
	assert.Assert(t, 150 == schedulerQueueA.QueueInfo.GetMaxResource().Resources[resources.MEMORY])

	// Add one application
	err = ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-1": "root.a"}),
		RmID:            "rm:123",
	})

	// Application should be accepted
	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)

	// Check scheduling app
	schedulingApp := ms.getSchedulingApplication("app-1")
	assert.Equal(t, schedulingApp.ApplicationInfo.ApplicationID, "app-1")
	assert.Equal(t, len(schedulingApp.ApplicationInfo.GetAllAllocations()), 0)

	// App asks for 2 allocations
	err = ms.proxy.Update(&si.UpdateRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 1},
					},
				},
				MaxAllocations: 2,
				ApplicationID:  "app-1",
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	waitForPendingQueueResource(t, schedulerQueueA, 20, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 20, 1000)
	waitForPendingAppResource(t, schedulingApp, 20, 1000)

	// no nodes available, no allocation can be made
	ms.scheduler.MultiStepSchedule(16)

	// pending resources should not change
	waitForPendingQueueResource(t, schedulerQueueA, 20, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 20, 1000)
	waitForPendingAppResource(t, schedulingApp, 20, 1000)

	// Register a node
	err = ms.proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeID: "node-1:1234",
				Attributes: map[string]string{
					"si.io/hostname": "node-1",
					"si.io/rackname": "rack-1",
				},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
			},
		},
		NewApplications: newAddAppRequest(map[string]string{"app-1": "root.a"}),
		RmID:            "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	// Wait until node is registered
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)

	// Run scheduling
	ms.scheduler.MultiStepSchedule(16)

	// Wait for allocating resources
	waitForPendingQueueResource(t, schedulerQueueA, 0, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 0, 1000)
	waitForPendingAppResource(t, schedulingApp, 0, 1000)

	// Make sure we get correct allocations
	ms.mockRM.waitForAllocations(t, 2, 3000)
}

func TestAddRemoveNodes(t *testing.T) {
	// Register RM
	configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
              max:
                memory: 150
                vcore: 20
`
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Check queues of cache and scheduler.
	partitionInfo := ms.clusterInfo.GetPartition("[rm:123]default")
	assert.Assert(t, partitionInfo.Root.GetMaxResource() == nil, "partition info max resource nil")

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")
	assert.Assert(t, schedulerQueueRoot.QueueInfo.GetMaxResource() == nil)

	// Check scheduling queue a
	schedulerQueueA := ms.getSchedulingQueue("root.a")
	assert.Assert(t, 150 == schedulerQueueA.QueueInfo.GetMaxResource().Resources[resources.MEMORY])

	// Add one application
	err = ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-1": "root.a"}),
		RmID:            "rm:123",
	})

	// Application should be accepted
	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)

	// Check scheduling app
	schedulingApp := ms.getSchedulingApplication("app-1")
	assert.Equal(t, schedulingApp.ApplicationInfo.ApplicationID, "app-1")
	assert.Equal(t, len(schedulingApp.ApplicationInfo.GetAllAllocations()), 0)

	// App asks for 2 allocations
	err = ms.proxy.Update(&si.UpdateRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 1},
					},
				},
				MaxAllocations: 2,
				ApplicationID:  "app-1",
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	waitForPendingQueueResource(t, schedulerQueueA, 20, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 20, 1000)
	waitForPendingAppResource(t, schedulingApp, 20, 1000)

	// no nodes available, no allocation can be made
	ms.scheduler.MultiStepSchedule(16)

	// pending resources should not change
	waitForPendingQueueResource(t, schedulerQueueA, 20, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 20, 1000)
	waitForPendingAppResource(t, schedulingApp, 20, 1000)

	// Register a node
	err = ms.proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeID: "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
			},
		},
		NewApplications: newAddAppRequest(map[string]string{"app-1": "root.a"}),
		RmID:            "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	// Wait until node is registered
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)

	// Run scheduling
	ms.scheduler.MultiStepSchedule(16)

	// Wait for allocating resources
	waitForPendingQueueResource(t, schedulerQueueA, 0, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 0, 1000)
	waitForPendingAppResource(t, schedulingApp, 0, 1000)
	waitForAllocatedQueueResource(t, schedulerQueueA, 20, 1000)
	waitForAllocatedAppResource(t, schedulingApp, 20, 1000)
	waitForNodesAllocatedResource(t, ms.clusterInfo, "[rm:123]default",
		[]string{"node-1:1234"}, 20, 1000)

	// Make sure we get correct allocations
	ms.mockRM.waitForAllocations(t, 2, 3000)

	// now remove the node
	err = ms.proxy.Update(&si.UpdateRequest{
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID:     "node-1:1234",
				Action:     si.UpdateNodeInfo_DECOMISSION,
				Attributes: map[string]string{},
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	// make sure the resources are released from queue/app
	waitForAllocatedQueueResource(t, schedulerQueueA, 0, 1000)
	waitForAllocatedAppResource(t, schedulingApp, 0, 1000)
	// make sure the node is removed from partition
	assert.Equal(t, len(partitionInfo.GetNodes()), 0)
}

func TestUpdateNodeCapacity(t *testing.T) {
	// Register RM
	configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              max:
                memory: 150
                vcore: 20
`
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Check queues of cache and scheduler.
	partitionInfo := ms.clusterInfo.GetPartition("[rm:123]default")
	assert.Assert(t, partitionInfo.Root.GetMaxResource() == nil, "partition info max resource nil")

	// Register a node
	err = ms.proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeID: "node-1:1234",
				Attributes: map[string]string{
					"si.io/hostname": "node-1",
					"si.io/rackname": "rack-1",
				},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
			},
		},
		RmID:            "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	// Wait until node is registered
	context := ms.scheduler.GetClusterSchedulingContext()
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	waitForNewSchedulerNode(t, context, "node-1:1234", "[rm:123]default", 1000)

	// verify node capacity
	assert.Equal(t, len(partitionInfo.GetNodes()), 1)
	node1 := partitionInfo.GetNode("node-1:1234")
	assert.Equal(t, int64(node1.GetCapacity().Resources[resources.MEMORY]), int64(100))
	schedulingNode1 := ms.scheduler.GetClusterSchedulingContext().
		GetSchedulingNode("node-1:1234", "[rm:123]default")
	assert.Equal(t, int64(schedulingNode1.GetAllocatedResource().Resources[resources.MEMORY]), int64(0))
	assert.Equal(t, int64(schedulingNode1.GetAvailableResource().Resources[resources.MEMORY]), int64(100))

	// update node capacity
	err = ms.proxy.Update(&si.UpdateRequest{
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID: "node-1:1234",
				Attributes: map[string]string{
					"si.io/hostname": "node-1",
					"si.io/rackname": "rack-1",
				},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 300},
						"vcore":  {Value: 10},
					},
				},
				Action: si.UpdateNodeInfo_UPDATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	waitForNodesAvailableResource(t, ms.clusterInfo, "[rm:123]default",
		[]string{"node-1:1234"}, 300, 1000)
	assert.Equal(t, int64(node1.GetCapacity().Resources[resources.MEMORY]), int64(300))
	assert.Equal(t, int64(node1.GetCapacity().Resources[resources.VCORE]), int64(10))
	assert.Equal(t, int64(schedulingNode1.GetAllocatedResource().Resources[resources.MEMORY]), int64(0))
	assert.Equal(t, int64(schedulingNode1.GetAvailableResource().Resources[resources.MEMORY]), int64(300))
}

func TestUpdateNodeOccupiedResources(t *testing.T) {
	// Register RM
	configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              max:
                memory: 150
                vcore: 20
`
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Check queues of cache and scheduler.
	partitionInfo := ms.clusterInfo.GetPartition("[rm:123]default")
	assert.Assert(t, partitionInfo.Root.GetMaxResource() == nil, "partition info max resource nil")

	// Register a node
	err = ms.proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeID: "node-1:1234",
				Attributes: map[string]string{
					"si.io/hostname": "node-1",
					"si.io/rackname": "rack-1",
				},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 10},
					},
				},
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	// Wait until node is registered
	context := ms.scheduler.GetClusterSchedulingContext()
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	waitForNewSchedulerNode(t, context, "node-1:1234", "[rm:123]default", 1000)

	// verify node capacity
	assert.Equal(t, len(partitionInfo.GetNodes()), 1)
	node1 := partitionInfo.GetNode("node-1:1234")
	assert.Equal(t, int64(node1.GetCapacity().Resources[resources.MEMORY]), int64(100))
	schedulingNode1 := ms.scheduler.GetClusterSchedulingContext().
		GetSchedulingNode("node-1:1234", "[rm:123]default")
	assert.Equal(t, int64(schedulingNode1.GetAllocatedResource().Resources[resources.MEMORY]), int64(0))
	assert.Equal(t, int64(schedulingNode1.GetAvailableResource().Resources[resources.MEMORY]), int64(100))

	// update node capacity
	err = ms.proxy.Update(&si.UpdateRequest{
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID: "node-1:1234",
				Attributes: map[string]string{
					"si.io/hostname": "node-1",
					"si.io/rackname": "rack-1",
				},
				OccupiedResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 80},
						"vcore":  {Value: 5},
					},
				},
				Action: si.UpdateNodeInfo_UPDATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	waitForNodesAvailableResource(t, ms.clusterInfo, "[rm:123]default",
		[]string{"node-1:1234"}, 20, 1000)
	assert.Equal(t, int64(node1.GetCapacity().Resources[resources.MEMORY]), int64(100))
	assert.Equal(t, int64(node1.GetCapacity().Resources[resources.VCORE]), int64(10))
	assert.Equal(t, int64(node1.GetOccupiedResource().Resources[resources.MEMORY]), int64(80))
	assert.Equal(t, int64(node1.GetOccupiedResource().Resources[resources.VCORE]), int64(5))
	assert.Equal(t, int64(schedulingNode1.GetAllocatedResource().Resources[resources.MEMORY]), int64(0))
	assert.Equal(t, int64(schedulingNode1.GetAvailableResource().Resources[resources.MEMORY]), int64(20))
}
