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

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
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
	part := ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
	assert.Assert(t, part.GetTotalPartitionResource() == nil, "partition info max resource nil")

	// Check scheduling queue root
	rootQ := part.GetQueue("root")
	assert.Assert(t, rootQ.GetMaxResource() == nil)

	// Check scheduling queue a
	queueA := part.GetQueue("root.a")
	assert.Assert(t, 150 == queueA.GetMaxResource().Resources[resources.MEMORY])

	// Add one application
	err = ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{appID1: "root.a"}),
		RmID:            "rm:123",
	})

	// Application should be accepted
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)

	// Check scheduling app
	app := ms.getApplication(appID1)
	assert.Equal(t, app.ApplicationID, appID1)
	assert.Equal(t, len(app.GetAllAllocations()), 0)

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
				ApplicationID:  appID1,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	waitForPendingQueueResource(t, queueA, 20, 1000)
	waitForPendingQueueResource(t, rootQ, 20, 1000)
	waitForPendingAppResource(t, app, 20, 1000)

	// no nodes available, no allocation can be made
	ms.scheduler.MultiStepSchedule(5)

	// pending resources should not change
	waitForPendingQueueResource(t, queueA, 20, 1000)
	waitForPendingQueueResource(t, rootQ, 20, 1000)
	waitForPendingAppResource(t, app, 20, 1000)

	// Register a node
	err = ms.proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
			},
		},
		NewApplications: newAddAppRequest(map[string]string{appID1: "root.a"}),
		RmID:            "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	// Wait until node is registered
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)

	// Run scheduling
	ms.scheduler.MultiStepSchedule(5)

	// Wait for allocating resources
	waitForPendingQueueResource(t, queueA, 0, 1000)
	waitForPendingQueueResource(t, rootQ, 0, 1000)
	waitForPendingAppResource(t, app, 0, 1000)

	// Make sure we get correct allocations
	ms.mockRM.waitForAllocations(t, 2, 1000)
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
	part := ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
	assert.Assert(t, part.GetTotalPartitionResource() == nil, "partition info max resource nil")

	// Check scheduling queue root
	rootQ := part.GetQueue("root")
	assert.Assert(t, rootQ.GetMaxResource() == nil)

	// Check scheduling queue a
	queueA := part.GetQueue("root.a")
	assert.Assert(t, 150 == queueA.GetMaxResource().Resources[resources.MEMORY])

	// Add one application
	err = ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{appID1: "root.a"}),
		RmID:            "rm:123",
	})

	// Application should be accepted
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)

	// Check scheduling app
	app := ms.getApplication(appID1)
	assert.Equal(t, app.ApplicationID, appID1)
	assert.Equal(t, len(app.GetAllAllocations()), 0)

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
				ApplicationID:  appID1,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	waitForPendingQueueResource(t, queueA, 20, 1000)
	waitForPendingQueueResource(t, rootQ, 20, 1000)
	waitForPendingAppResource(t, app, 20, 1000)

	// no nodes available, no allocation can be made
	ms.scheduler.MultiStepSchedule(5)

	// pending resources should not change
	waitForPendingQueueResource(t, queueA, 20, 1000)
	waitForPendingQueueResource(t, rootQ, 20, 1000)
	waitForPendingAppResource(t, app, 20, 1000)

	// Register a node
	err = ms.proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
			},
		},
		NewApplications: newAddAppRequest(map[string]string{appID1: "root.a"}),
		RmID:            "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	// Wait until node is registered
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)

	// Run scheduling
	ms.scheduler.MultiStepSchedule(5)

	// Wait for allocating resources
	waitForPendingQueueResource(t, queueA, 0, 1000)
	waitForPendingQueueResource(t, rootQ, 0, 1000)
	waitForPendingAppResource(t, app, 0, 1000)
	waitForAllocatedQueueResource(t, queueA, 20, 1000)
	waitForAllocatedAppResource(t, app, 20, 1000)
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), "[rm:123]default",
		[]string{"node-1:1234"}, 20, 1000)

	// Make sure we get correct allocations
	ms.mockRM.waitForAllocations(t, 2, 1000)

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
	waitForAllocatedQueueResource(t, queueA, 0, 1000)
	waitForAllocatedAppResource(t, app, 0, 1000)
	// make sure the node is removed from partition
	assert.Equal(t, len(part.GetNodes()), 0)
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
	partitionInfo := ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
	assert.Assert(t, partitionInfo.GetTotalPartitionResource() == nil, "partition info max resource nil")

	// Register a node
	err = ms.proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	// Wait until node is registered
	context := ms.scheduler.GetClusterContext()
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	waitForNewNode(t, context, "node-1:1234", "[rm:123]default", 1000)

	// verify node capacity
	assert.Equal(t, len(partitionInfo.GetNodes()), 1)
	node1 := partitionInfo.GetNode("node-1:1234")
	assert.Equal(t, int64(node1.GetCapacity().Resources[resources.MEMORY]), int64(100))
	schedulingNode1 := ms.scheduler.GetClusterContext().
		GetNode("node-1:1234", "[rm:123]default")
	assert.Equal(t, int64(schedulingNode1.GetAllocatedResource().Resources[resources.MEMORY]), int64(0))
	assert.Equal(t, int64(schedulingNode1.GetAvailableResource().Resources[resources.MEMORY]), int64(100))

	// update node capacity
	err = ms.proxy.Update(&si.UpdateRequest{
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
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

	waitForAvailableNodeResource(t, ms.scheduler.GetClusterContext(), "[rm:123]default",
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
	partitionInfo := ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
	assert.Assert(t, partitionInfo.GetTotalPartitionResource() == nil, "partition info max resource nil")

	// Register a node
	err = ms.proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
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
	context := ms.scheduler.GetClusterContext()
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	waitForNewNode(t, context, "node-1:1234", "[rm:123]default", 1000)

	// verify node capacity
	assert.Equal(t, len(partitionInfo.GetNodes()), 1)
	node1 := partitionInfo.GetNode("node-1:1234")
	assert.Equal(t, int64(node1.GetCapacity().Resources[resources.MEMORY]), int64(100))
	schedulingNode1 := ms.scheduler.GetClusterContext().
		GetNode("node-1:1234", "[rm:123]default")
	assert.Equal(t, int64(schedulingNode1.GetAllocatedResource().Resources[resources.MEMORY]), int64(0))
	assert.Equal(t, int64(schedulingNode1.GetAvailableResource().Resources[resources.MEMORY]), int64(100))

	// update node capacity
	err = ms.proxy.Update(&si.UpdateRequest{
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
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

	waitForAvailableNodeResource(t, ms.scheduler.GetClusterContext(), "[rm:123]default",
		[]string{"node-1:1234"}, 20, 1000)
	assert.Equal(t, int64(node1.GetCapacity().Resources[resources.MEMORY]), int64(100))
	assert.Equal(t, int64(node1.GetCapacity().Resources[resources.VCORE]), int64(10))
	assert.Equal(t, int64(node1.GetOccupiedResource().Resources[resources.MEMORY]), int64(80))
	assert.Equal(t, int64(node1.GetOccupiedResource().Resources[resources.VCORE]), int64(5))
	assert.Equal(t, int64(schedulingNode1.GetAllocatedResource().Resources[resources.MEMORY]), int64(0))
	assert.Equal(t, int64(schedulingNode1.GetAvailableResource().Resources[resources.MEMORY]), int64(20))
}
