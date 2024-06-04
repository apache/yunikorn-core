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
	"fmt"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
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
                memory: 100M
                vcore: 10
              max:
                memory: 150M
                vcore: 20
`
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Check queues of cache and scheduler.
	part := ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
	assert.Assert(t, part.GetTotalPartitionResource() == nil, "partition info max resource nil")

	// Check scheduling queue root
	rootQ := part.GetQueue("root")
	assert.Assert(t, rootQ.GetMaxResource() == nil)

	// Check scheduling queue a
	queueA := part.GetQueue("root.a")
	assert.Equal(t, resources.Quantity(150000000), queueA.GetMaxResource().Resources[common.Memory])

	// Add one application
	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID1: "root.a"}),
		RmID: "rm:123",
	})
	assert.NilError(t, err, "ApplicationRequest failed")

	// Application should be accepted
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)

	// Check scheduling app
	app := ms.getApplication(appID1)
	assert.Equal(t, app.ApplicationID, appID1)
	assert.Equal(t, len(app.GetAllAllocations()), 0)

	// App asks for 2 allocations
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10000000},
						"vcore":  {Value: 1000},
					},
				},
				ApplicationID: appID1,
			},
			{
				AllocationKey: "alloc-2",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10000000},
						"vcore":  {Value: 1000},
					},
				},
				ApplicationID: appID1,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "AllocationRequest failed")

	waitForPendingQueueResource(t, queueA, 20000000, 1000)
	waitForPendingQueueResource(t, rootQ, 20000000, 1000)
	waitForPendingAppResource(t, app, 20000000, 1000)

	// no nodes available, no allocation can be made
	ms.scheduler.MultiStepSchedule(5)

	// pending resources should not change
	waitForPendingQueueResource(t, queueA, 20000000, 1000)
	waitForPendingQueueResource(t, rootQ, 20000000, 1000)
	waitForPendingAppResource(t, app, 20000000, 1000)

	// Register a node
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100000000},
						"vcore":  {Value: 20000},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")

	// Add one application
	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID1: "root.a"}),
		RmID: "rm:123",
	})

	assert.NilError(t, err, "ApplicationRequest failed")

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
                memory: 100M
                vcore: 10
              max:
                memory: 150M
                vcore: 20
`
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Check queues of cache and scheduler.
	part := ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
	assert.Assert(t, part.GetTotalPartitionResource() == nil, "partition info max resource nil")

	// Check scheduling queue root
	rootQ := part.GetQueue("root")
	assert.Assert(t, rootQ.GetMaxResource() == nil)

	// Check scheduling queue a
	queueA := part.GetQueue("root.a")
	assert.Equal(t, resources.Quantity(150000000), queueA.GetMaxResource().Resources[common.Memory])

	// Add one application
	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID1: "root.a"}),
		RmID: "rm:123",
	})
	assert.NilError(t, err, "ApplicationRequest failed")

	// Application should be accepted
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)

	// Check scheduling app
	app := ms.getApplication(appID1)
	assert.Equal(t, app.ApplicationID, appID1)
	assert.Equal(t, len(app.GetAllAllocations()), 0)

	// App asks for 2 allocations
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10000000},
						"vcore":  {Value: 1000},
					},
				},
				ApplicationID: appID1,
			},
			{
				AllocationKey: "alloc-2",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10000000},
						"vcore":  {Value: 1000},
					},
				},
				ApplicationID: appID1,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "AllocationRequest failed")

	waitForPendingQueueResource(t, queueA, 20000000, 1000)
	waitForPendingQueueResource(t, rootQ, 20000000, 1000)
	waitForPendingAppResource(t, app, 20000000, 1000)

	// no nodes available, no allocation can be made
	ms.scheduler.MultiStepSchedule(5)

	// pending resources should not change
	waitForPendingQueueResource(t, queueA, 20000000, 1000)
	waitForPendingQueueResource(t, rootQ, 20000000, 1000)
	waitForPendingAppResource(t, app, 20000000, 1000)

	// Register a node
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100000000},
						"vcore":  {Value: 20000},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")

	// Add one application
	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID1: "root.a"}),
		RmID: "rm:123",
	})

	assert.NilError(t, err, "ApplicationRequest failed")

	// Wait until node is registered
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)

	// Run scheduling
	ms.scheduler.MultiStepSchedule(5)

	// Wait for allocating resources
	waitForPendingQueueResource(t, queueA, 0, 1000)
	waitForPendingQueueResource(t, rootQ, 0, 1000)
	waitForPendingAppResource(t, app, 0, 1000)
	waitForAllocatedQueueResource(t, queueA, 20000000, 1000)
	waitForAllocatedAppResource(t, app, 20000000, 1000)
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), "[rm:123]default",
		[]string{"node-1:1234"}, 20000000, 1000)

	// Make sure we get correct allocations
	ms.mockRM.waitForAllocations(t, 2, 1000)

	// now remove the node
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID: "node-1:1234",
				Action: si.NodeInfo_DECOMISSION,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")

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
                memory: 150M
                vcore: 20
`
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Check queues of cache and scheduler.
	partitionInfo := ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
	assert.Assert(t, partitionInfo.GetTotalPartitionResource() == nil, "partition info max resource nil")

	// Register a node
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100000000},
						"vcore":  {Value: 20000},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "NodeRequest failed")

	// Wait until node is registered
	context := ms.scheduler.GetClusterContext()
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	waitForNewNode(t, context, "node-1:1234", "[rm:123]default", 1000)

	// verify node capacity
	assert.Equal(t, len(partitionInfo.GetNodes()), 1)
	node1 := partitionInfo.GetNode("node-1:1234")
	assert.Equal(t, int64(node1.GetCapacity().Resources[common.Memory]), int64(100000000))
	schedulingNode1 := ms.scheduler.GetClusterContext().GetNode("node-1:1234", "[rm:123]default")
	assert.Equal(t, int64(schedulingNode1.GetAllocatedResource().Resources[common.Memory]), int64(0))
	assert.Equal(t, int64(schedulingNode1.GetAvailableResource().Resources[common.Memory]), int64(100000000))

	// update node capacity with more mem and less vcores
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 300000000},
						"vcore":  {Value: 10000},
					},
				},
				Action: si.NodeInfo_UPDATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")
	waitForAvailableNodeResource(t, ms.scheduler.GetClusterContext(), "[rm:123]default", []string{"node-1:1234"}, 300000000, 1000)
	waitForUpdatePartitionResource(t, partitionInfo, common.Memory, 300000000, 1000)
	waitForUpdatePartitionResource(t, partitionInfo, common.CPU, 10000, 1000)
	assert.Equal(t, int64(node1.GetCapacity().Resources[common.Memory]), int64(300000000))
	assert.Equal(t, int64(node1.GetCapacity().Resources[common.CPU]), int64(10000))
	assert.Equal(t, int64(schedulingNode1.GetAllocatedResource().Resources[common.Memory]), int64(0))
	assert.Equal(t, int64(schedulingNode1.GetAvailableResource().Resources[common.Memory]), int64(300000000))
	newRes, err := resources.NewResourceFromConf(map[string]string{"memory": "300M", "vcore": "10"})
	assert.NilError(t, err, "failed to create resource")
	if !resources.Equals(newRes, partitionInfo.GetQueue("root").GetMaxResource()) {
		t.Errorf("Expected partition resource %s, doesn't match with actual partition resource %s", newRes, partitionInfo.GetQueue("root").GetMaxResource())
	}

	// update node capacity with more mem and same vcores
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100000000},
						"vcore":  {Value: 20000},
					},
				},
				Action: si.NodeInfo_UPDATE,
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "NodeRequest failed")
	waitForAvailableNodeResource(t, ms.scheduler.GetClusterContext(), "[rm:123]default", []string{"node-1:1234"}, 100000000, 1000)
	waitForUpdatePartitionResource(t, partitionInfo, common.Memory, 100000000, 1000)
	waitForUpdatePartitionResource(t, partitionInfo, common.CPU, 20000, 1000)
	newRes, err = resources.NewResourceFromConf(map[string]string{"memory": "100M", "vcore": "20"})
	assert.NilError(t, err, "failed to create resource")
	if !resources.Equals(newRes, partitionInfo.GetQueue("root").GetMaxResource()) {
		t.Errorf("Expected partition resource %s, doesn't match with actual partition resource %s", newRes, partitionInfo.GetQueue("root").GetMaxResource())
	}
}

func TestUpdateNodeCapacityWithMultipleNodes(t *testing.T) {
	// Register RM
	configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        queues:
          - name: a
`
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Check queues of cache and scheduler.
	partitionInfo := ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
	assert.Assert(t, partitionInfo.GetTotalPartitionResource() == nil, "partition info max resource nil")

	for i := 1; i < 3; i++ {
		var nodeName = fmt.Sprint("node-", i, ":1234")
		// Register a node
		err = ms.proxy.UpdateNode(&si.NodeRequest{
			Nodes: []*si.NodeInfo{
				{
					NodeID:     nodeName,
					Attributes: map[string]string{},
					SchedulableResource: &si.Resource{
						Resources: map[string]*si.Quantity{
							"memory": {Value: 100000000},
							"vcore":  {Value: 20000},
						},
					},
					Action: si.NodeInfo_CREATE,
				},
			},
			RmID: "rm:123",
		})
		assert.NilError(t, err, "NodeRequest failed")

		// Wait until node is registered
		context := ms.scheduler.GetClusterContext()
		ms.mockRM.waitForAcceptedNode(t, nodeName, 1000)
		waitForNewNode(t, context, nodeName, "[rm:123]default", 1000)
	}

	// update 2nd node capacity with less mem and vcores
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 50000000},
						"vcore":  {Value: 10000},
					},
				},
				Action: si.NodeInfo_UPDATE,
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "NodeRequest failed")

	waitForAvailableNodeResource(t, ms.scheduler.GetClusterContext(), "[rm:123]default",
		[]string{"node-2:1234"}, 50000000, 1000)
	waitForUpdatePartitionResource(t, partitionInfo, common.Memory, 150000000, 1000)
	waitForUpdatePartitionResource(t, partitionInfo, common.CPU, 30000, 1000)

	newRes, err := resources.NewResourceFromConf(map[string]string{"memory": "150M", "vcore": "30"})
	assert.NilError(t, err, "failed to create resource")

	if !resources.Equals(newRes, partitionInfo.GetQueue("root").GetMaxResource()) {
		t.Errorf("Expected partition resource %s, doesn't match with actual partition resource %s", newRes, partitionInfo.GetQueue("root").GetMaxResource())
	}

	// update 2nd node capacity with same mem and vcores
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 50000000},
						"vcore":  {Value: 10000},
					},
				},
				Action: si.NodeInfo_UPDATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")

	waitForAvailableNodeResource(t, ms.scheduler.GetClusterContext(), "[rm:123]default",
		[]string{"node-2:1234"}, 50000000, 1000)
	waitForUpdatePartitionResource(t, partitionInfo, common.Memory, 150000000, 1000)
	waitForUpdatePartitionResource(t, partitionInfo, common.CPU, 30000, 1000)

	newRes, err = resources.NewResourceFromConf(map[string]string{"memory": "150M", "vcore": "30"})
	assert.NilError(t, err, "failed to create resource")

	if !resources.Equals(newRes, partitionInfo.GetQueue("root").GetMaxResource()) {
		t.Errorf("Expected partition resource %s, doesn't match with actual partition resource %s", newRes, partitionInfo.GetQueue("root").GetMaxResource())
	}
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

	err := ms.Init(configData, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Check queues of cache and scheduler.
	partitionInfo := ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
	assert.Assert(t, partitionInfo.GetTotalPartitionResource() == nil, "partition info max resource nil")

	// Register a node
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 10},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")

	// Wait until node is registered
	context := ms.scheduler.GetClusterContext()
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	waitForNewNode(t, context, "node-1:1234", "[rm:123]default", 1000)

	// verify node capacity
	assert.Equal(t, len(partitionInfo.GetNodes()), 1)
	node1 := partitionInfo.GetNode("node-1:1234")
	assert.Equal(t, int64(node1.GetCapacity().Resources[common.Memory]), int64(100))
	schedulingNode1 := ms.scheduler.GetClusterContext().
		GetNode("node-1:1234", "[rm:123]default")
	assert.Equal(t, int64(schedulingNode1.GetAllocatedResource().Resources[common.Memory]), int64(0))
	assert.Equal(t, int64(schedulingNode1.GetAvailableResource().Resources[common.Memory]), int64(100))

	// update node capacity
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				OccupiedResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 80},
						"vcore":  {Value: 5},
					},
				},
				Action: si.NodeInfo_UPDATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")

	waitForAvailableNodeResource(t, ms.scheduler.GetClusterContext(), "[rm:123]default",
		[]string{"node-1:1234"}, 20, 1000)
	assert.Equal(t, int64(node1.GetCapacity().Resources[common.Memory]), int64(100))
	assert.Equal(t, int64(node1.GetCapacity().Resources[common.CPU]), int64(10))
	assert.Equal(t, int64(node1.GetOccupiedResource().Resources[common.Memory]), int64(80))
	assert.Equal(t, int64(node1.GetOccupiedResource().Resources[common.CPU]), int64(5))
	assert.Equal(t, int64(schedulingNode1.GetAllocatedResource().Resources[common.Memory]), int64(0))
	assert.Equal(t, int64(schedulingNode1.GetAvailableResource().Resources[common.Memory]), int64(20))
}
