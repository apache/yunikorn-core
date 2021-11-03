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
	"strconv"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

const configDataSmokeTest = `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: singleleaf
            resources:
              max:
                memory: 150
                vcore: 20
`

// Test scheduler reconfiguration
func TestConfigScheduler(t *testing.T) {
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: base
            resources:
              guaranteed: {memory: 100, vcore: 10}
              max: {memory: 150, vcore: 20}
          - name: tobedeleted
`
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// memorize the checksum of current configs
	configChecksum := configs.ConfigContext.Get("policygroup").Checksum

	// Check queues of cache and scheduler.
	part := ms.scheduler.GetClusterContext().GetPartition(partition)
	assert.Assert(t, part.GetTotalPartitionResource() == nil, "partition info max resource not nil, first load")

	// Check the queue root
	rootQ := part.GetQueue("root")
	assert.Assert(t, rootQ.GetMaxResource() == nil, "root queue max resource not nil, first load")

	// Check the queue root.base
	queue := part.GetQueue("root.base")
	assert.Equal(t, int(queue.GetMaxResource().Resources[resources.MEMORY]), 150, "max resource on leaf not set correctly")

	// Check the queue which will be removed
	queue = part.GetQueue("root.tobedeleted")
	assert.Assert(t, queue.IsRunning(), "child queue root.tobedeleted is not in running state")

	configData = `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: base
            resources:
              guaranteed: {memory: 500, vcore: 250}
              max: {memory: 1000, vcore: 500}
          - name: tobeadded
            properties:
              something: withAvalue
  - name: gpu
    queues:
      - name: production
      - name: test
        properties:
          gpu: test queue property
`
	configs.MockSchedulerConfigByData([]byte(configData))
	err = ms.proxy.UpdateConfiguration("rm:123")

	assert.NilError(t, err, "configuration reload failed")

	// wait until configuration is reloaded
	if err = common.WaitFor(time.Second, 5*time.Second, func() bool {
		return configs.ConfigContext.Get("policygroup").Checksum != configChecksum
	}); err != nil {
		t.Errorf("timeout waiting for configuration to be reloaded: %v", err)
	}

	// Check queues of cache and scheduler.
	assert.Assert(t, part.GetTotalPartitionResource() == nil, "partition info max resource not nil, reload")

	// Check the queue root
	rootQ = part.GetQueue("root")
	assert.Assert(t, rootQ.GetMaxResource() == nil, "root queue max resource not nil, reload")

	// Check the queue root.base
	queue = part.GetQueue("root.base")
	assert.Equal(t, int(queue.GetMaxResource().Resources[resources.MEMORY]), 1000, "max resource on leaf not set correctly")
	assert.Equal(t, int(queue.GetGuaranteedResource().Resources[resources.VCORE]), 250, "guaranteed resource on leaf not set correctly")

	queue = part.GetQueue("root.tobeadded")
	assert.Assert(t, queue != nil, "queue root.tobeadded is not found")
	// check the removed queue state
	queue = part.GetQueue("root.tobedeleted")
	assert.Assert(t, queue.IsDraining(), "child queue root.tobedeleted is not in draining state")

	// Check queues of cache and scheduler for the newly added partition
	part = ms.scheduler.GetClusterContext().GetPartition("[rm:123]gpu")
	assert.Assert(t, part != nil, "gpu partition not found")
	assert.Assert(t, part.GetTotalPartitionResource() == nil, "GPU partition info max resource not nil")

	// Check queue root
	rootQ = ms.getPartitionQueue("root", "[rm:123]gpu")
	assert.Assert(t, rootQ.GetMaxResource() == nil, "max resource on root set")

	// Check queue root.production
	queue = ms.getPartitionQueue("root.production", "[rm:123]gpu")
	assert.Assert(t, queue != nil, "New partition: queue root.production is not found")
}

// Test basic interactions from rm proxy to cache and to scheduler.
func TestBasicScheduler(t *testing.T) {
	// Register RM
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configDataSmokeTest, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	leafName := "root.singleleaf"
	// Check queues of cache and scheduler.
	part := ms.scheduler.GetClusterContext().GetPartition(partition)
	assert.Assert(t, part.GetTotalPartitionResource() == nil, "partition info max resource nil")

	// Check the queue root
	root := part.GetQueue("root")
	assert.Assert(t, root.GetMaxResource() == nil, "root queue max resource should be nil")

	// Check the queue singleleaf
	leaf := part.GetQueue(leafName)
	assert.Equal(t, int(leaf.GetMaxResource().Resources[resources.MEMORY]), 150, "%s config not set correct", leafName)

	// Register a node, and add apps
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "NodeRequest failed")

	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID1: leafName}),
		RmID: "rm:123",
	})

	assert.NilError(t, err, "ApplicationRequest failed")

	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	// Get the app
	app := ms.getApplication(appID1)

	// Verify app initial state
	var app01 *objects.Application
	app01, err = getApplication(part, appID1)
	assert.NilError(t, err, "application not found")

	assert.Equal(t, app01.CurrentState(), objects.New.String())

	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
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
	assert.NilError(t, err, "AllocationRequest 2 failed")

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 10 * 2 = 20;
	waitForPendingQueueResource(t, leaf, 20, 1000)
	waitForPendingQueueResource(t, root, 20, 1000)
	waitForPendingAppResource(t, app, 20, 1000)
	assert.Equal(t, app01.CurrentState(), objects.Accepted.String())

	ms.scheduler.MultiStepSchedule(5)

	ms.mockRM.waitForAllocations(t, 2, 1000)

	// Make sure pending resource updated to 0
	waitForPendingQueueResource(t, leaf, 0, 1000)
	waitForPendingQueueResource(t, root, 0, 1000)
	waitForPendingAppResource(t, app, 0, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, int(leaf.GetAllocatedResource().Resources[resources.MEMORY]), 20, "leaf allocated memory incorrect")
	assert.Equal(t, int(root.GetAllocatedResource().Resources[resources.MEMORY]), 20, "root allocated memory incorrect")
	assert.Equal(t, int(app.GetAllocatedResource().Resources[resources.MEMORY]), 20, "app allocated memory incorrect")

	// once we start to process allocation asks from this app, verify the state again
	assert.Equal(t, app01.CurrentState(), objects.Running.String())

	// Check allocated resources of nodes
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), ms.partitionName, []string{"node-1:1234", "node-2:1234"}, 20, 1000)

	// Ask for two more resources
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-2",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 50},
						"vcore":  {Value: 5},
					},
				},
				MaxAllocations: 2,
				ApplicationID:  appID1,
			},
			{
				AllocationKey: "alloc-3",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 5},
					},
				},
				MaxAllocations: 2,
				ApplicationID:  appID1,
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "AllocationRequest 3 failed")

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 50 * 2 + 100 * 2 = 300;
	waitForPendingQueueResource(t, leaf, 300, 1000)
	waitForPendingQueueResource(t, root, 300, 1000)
	waitForPendingAppResource(t, app, 300, 1000)

	// Now app-1 uses 20 resource, and queue-a's max = 150, so it can get two 50 container allocated.
	ms.scheduler.MultiStepSchedule(6)

	ms.mockRM.waitForAllocations(t, 4, 1000)

	// Check pending resource, should be 200 now.
	waitForPendingQueueResource(t, leaf, 200, 1000)
	waitForPendingQueueResource(t, root, 200, 1000)
	waitForPendingAppResource(t, app, 200, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, int(leaf.GetAllocatedResource().Resources[resources.MEMORY]), 120, "leaf allocated memory incorrect")
	assert.Equal(t, int(root.GetAllocatedResource().Resources[resources.MEMORY]), 120, "root allocated memory incorrect")
	assert.Equal(t, int(app.GetAllocatedResource().Resources[resources.MEMORY]), 120, "app allocated memory incorrect")

	// Check allocated resources of nodes
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), partition, []string{"node-1:1234", "node-2:1234"}, 120, 1000)

	updateRequest := &si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: make([]*si.AllocationRelease, 0),
		},
		RmID: "rm:123",
	}

	// Release all allocations
	for _, v := range ms.mockRM.getAllocations() {
		updateRequest.Releases.AllocationsToRelease = append(updateRequest.Releases.AllocationsToRelease, &si.AllocationRelease{
			UUID:          v.UUID,
			ApplicationID: v.ApplicationID,
			PartitionName: v.PartitionName,
		})
	}

	// Release Allocations.
	err = ms.proxy.UpdateAllocation(updateRequest)
	assert.NilError(t, err, "AllocationRequest 4 failed")

	ms.mockRM.waitForAllocations(t, 0, 1000)

	// Check pending resource, should be 200 (same)
	waitForPendingQueueResource(t, leaf, 200, 1000)
	waitForPendingQueueResource(t, root, 200, 1000)
	waitForPendingAppResource(t, app, 200, 1000)

	// Check allocated resources of queues, apps should be 0 now
	assert.Equal(t, int(leaf.GetAllocatedResource().Resources[resources.MEMORY]), 0, "leaf allocated memory incorrect")
	assert.Equal(t, int(root.GetAllocatedResource().Resources[resources.MEMORY]), 0, "root allocated memory incorrect")
	assert.Equal(t, int(app.GetAllocatedResource().Resources[resources.MEMORY]), 0, "app allocated memory incorrect")

	// Release asks
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationAsksToRelease: []*si.AllocationAskRelease{
				{
					ApplicationID: appID1,
					PartitionName: "default",
				},
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "AllocationRequest 5 failed")

	// Check pending resource
	waitForPendingQueueResource(t, leaf, 0, 1000)
	waitForPendingQueueResource(t, root, 0, 1000)
	waitForPendingAppResource(t, app, 0, 1000)
}

func TestBasicSchedulerAutoAllocation(t *testing.T) {
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configDataSmokeTest, true)
	assert.NilError(t, err, "RegisterResourceManager failed")

	leafName := "root.singleleaf"
	appID := appID1

	// Register a node, and add apps
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")

	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID: leafName}),
		RmID: "rm:123",
	})

	assert.NilError(t, err, "ApplicationRequest failed")

	ms.mockRM.waitForAcceptedApplication(t, appID, 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 1},
					},
				},
				MaxAllocations: 20,
				ApplicationID:  appID,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "AllocationRequest 2 failed")

	// wait until we have maxed out the leaf queue
	ms.mockRM.waitForAllocations(t, 15, 1000)

	// Check  queue root
	root := ms.getQueue("root")

	// Check the leaf queue
	leaf := ms.getQueue(leafName)

	// Get app
	app := ms.getApplication(appID)

	// Make sure pending resource decreased to 50
	waitForPendingQueueResource(t, leaf, 50, 1000)
	waitForPendingQueueResource(t, root, 50, 1000)
	waitForPendingAppResource(t, app, 50, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, int(leaf.GetAllocatedResource().Resources[resources.MEMORY]), 150, "leaf allocated memory incorrect")
	assert.Equal(t, int(root.GetAllocatedResource().Resources[resources.MEMORY]), 150, "root allocated memory incorrect")
	assert.Equal(t, int(app.GetAllocatedResource().Resources[resources.MEMORY]), 150, "app allocated memory incorrect")

	// Check allocated resources of nodes
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), partition, []string{"node-1:1234", "node-2:1234"}, 150, 1000)
}

func TestFairnessAllocationForQueues(t *testing.T) {
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: leaf1
          - name: leaf2
`
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	leafApp1 := "root.leaf1"
	app1ID := appID1
	leafApp2 := "root.leaf2"
	app2ID := appID2

	// Register a node, and add apps
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")

	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{app1ID: leafApp1, app2ID: leafApp2}),
		RmID: "rm:123",
	})

	assert.NilError(t, err, "ApplicationRequest failed")

	ms.mockRM.waitForAcceptedApplication(t, app1ID, 1000)
	ms.mockRM.waitForAcceptedApplication(t, app2ID, 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 1},
					},
				},
				MaxAllocations: 20,
				ApplicationID:  app1ID,
			},
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 1},
					},
				},
				MaxAllocations: 20,
				ApplicationID:  app2ID,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "AllocationRequest 2 failed")

	// Check the queue root
	root := ms.getQueue("root")

	// Check the leaf queue app1
	leaf1 := ms.getQueue(leafApp1)
	app1 := ms.getApplication(app1ID)

	// Check the leaf queue app2
	leaf2 := ms.getQueue(leafApp2)
	app2 := ms.getApplication(app2ID)

	// Check pending resource, should be 100 (same)
	waitForPendingQueueResource(t, leaf1, 200, 1000)
	waitForPendingQueueResource(t, leaf2, 200, 1000)
	waitForPendingQueueResource(t, root, 400, 1000)

	ms.scheduler.MultiStepSchedule(25)
	ms.mockRM.waitForAllocations(t, 20, 1500)

	waitForAllocatedAppResource(t, app1, 100, 1000)
	waitForAllocatedAppResource(t, app2, 100, 1000)
	// Make sure pending resource updated to 0
	waitForPendingQueueResource(t, leaf1, 100, 1000)
	waitForPendingQueueResource(t, leaf2, 100, 1000)
	waitForPendingQueueResource(t, root, 200, 1000)
}

func TestFairnessAllocationForApplications(t *testing.T) {
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: leaf
            properties:
              application.sort.policy: fair
`
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	leafName := "root.leaf"
	app1ID := appID1
	app2ID := appID2

	// Register a node, and add applications
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")

	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{app1ID: leafName, app2ID: leafName}),
		RmID: "rm:123",
	})

	assert.NilError(t, err, "ApplicationRequest failed")

	ms.mockRM.waitForAcceptedApplication(t, app1ID, 1000)
	ms.mockRM.waitForAcceptedApplication(t, app2ID, 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 1},
					},
				},
				MaxAllocations: 20,
				ApplicationID:  app1ID,
			},
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 1},
					},
				},
				MaxAllocations: 20,
				ApplicationID:  app2ID,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "AllocationRequest 2 failed")

	// Check the queue root
	root := ms.getQueue("root")

	// Check the queue a
	leaf := ms.getQueue(leafName)

	// Get the app
	app1 := ms.getApplication(appID1)
	app2 := ms.getApplication(appID2)

	waitForPendingQueueResource(t, leaf, 400, 1000)
	waitForPendingQueueResource(t, root, 400, 1000)
	waitForPendingAppResource(t, app1, 200, 1000)
	waitForPendingAppResource(t, app2, 200, 1000)

	ms.scheduler.MultiStepSchedule(25)

	ms.mockRM.waitForAllocations(t, 20, 1000)

	// Make sure pending resource updated to 100, which means
	waitForPendingQueueResource(t, leaf, 200, 1000)
	waitForPendingQueueResource(t, root, 200, 1000)
	waitForPendingAppResource(t, app1, 100, 1000)
	waitForPendingAppResource(t, app2, 100, 1000)

	// Both apps got 100 resources,
	assert.Equal(t, int(app1.GetAllocatedResource().Resources[resources.MEMORY]), 100, "app1 allocated resource incorrect")
	assert.Equal(t, int(app2.GetAllocatedResource().Resources[resources.MEMORY]), 100, "app2 allocated resource incorrect")
}

func TestRejectApplications(t *testing.T) {
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: leaf
            properties:
              application.sort.policy: fair
`
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Register a node, and add applications
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")

	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)

	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{"app-reject-1": "root.non-exist-queue"}),
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest 2 failed")

	ms.mockRM.waitForRejectedApplication(t, "app-reject-1", 1000)

	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{"app-added-2": "root.leaf"}),
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest 3 failed")

	ms.mockRM.waitForAcceptedApplication(t, "app-added-2", 1000)
}

func TestSchedulingOverMaxCapacity(t *testing.T) {
	var parameters = []struct {
		name       string
		configData string
		leafQueue  string
		askMemory  int64
		askCPU     int64
		numOfAsk   int32
	}{
		{"scheduleOverParentQueueCapacity",
			`
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: parent
            resources:
              max:
                memory: 100
                vcore: 10
            queues:
              - name: child-1
              - name: child-2
`, "root.parent.child-1", 10, 1, 12},
		{"scheduleOverLeafQueueCapacity",
			`
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: default
            resources:
              max:
                memory: 100
                vcore: 10
`, "root.default", 10, 1, 12},
	}

	for _, param := range parameters {
		t.Run(param.name, func(t *testing.T) {
			ms := &mockScheduler{}
			defer ms.Stop()

			err := ms.Init(param.configData, false)
			assert.NilError(t, err, "RegisterResourceManager failed in run %s", param.name)

			// Register a node, and add applications
			err = ms.proxy.UpdateNode(&si.NodeRequest{
				Nodes: []*si.NodeInfo{
					{
						NodeID:     "node-1:1234",
						Attributes: map[string]string{},
						SchedulableResource: &si.Resource{
							Resources: map[string]*si.Quantity{
								"memory": {Value: 150},
								"vcore":  {Value: 15},
							},
						},
						Action: si.NodeInfo_CREATE,
					},
				},
				RmID: "rm:123",
			})

			assert.NilError(t, err, "NodeRequest failed in run %s", param.name)

			err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
				New:  newAddAppRequest(map[string]string{appID1: param.leafQueue}),
				RmID: "rm:123",
			})

			assert.NilError(t, err, "ApplicationRequest failed in run %s", param.name)

			ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)

			err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
				Asks: []*si.AllocationAsk{
					{
						AllocationKey: "alloc-1",
						ResourceAsk: &si.Resource{
							Resources: map[string]*si.Quantity{
								"memory": {Value: param.askMemory},
								"vcore":  {Value: param.askCPU},
							},
						},
						MaxAllocations: param.numOfAsk,
						ApplicationID:  appID1,
					},
				},
				RmID: "rm:123",
			})

			assert.NilError(t, err, "AllocationRequest 2 failed in run %s", param.name)

			leaf := ms.getQueue(param.leafQueue)

			waitForPendingQueueResource(t, leaf, 120, 1000)

			ms.scheduler.MultiStepSchedule(20)

			// 100 memory gets allocated, 20 pending because the capacity is 100
			waitForPendingQueueResource(t, leaf, 20, 1000)
			app1 := ms.getApplication(appID1)
			if app1 == nil {
				t.Fatal("application 'app-1' not found in cache")
			}
			waitForAllocatedAppResource(t, app1, 100, 1000)

			assert.Equal(t, len(app1.GetAllAllocations()), 10, "number of app allocations incorrect")
			assert.Equal(t, int(app1.GetAllocatedResource().Resources[resources.MEMORY]), 100, "app allocated resource incorrect")
			assert.Equal(t, len(ms.mockRM.getAllocations()), 10, "number of RM allocations incorrect")

			// release all allocated allocations
			allocReleases := make([]*si.AllocationRelease, 0)
			for _, alloc := range ms.mockRM.getAllocations() {
				allocReleases = append(allocReleases, &si.AllocationRelease{
					PartitionName: "default",
					ApplicationID: appID1,
					UUID:          alloc.UUID,
					Message:       "",
				})
			}

			err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
				Releases: &si.AllocationReleasesRequest{
					AllocationsToRelease: allocReleases,
				},
				RmID: "rm:123",
			})

			assert.NilError(t, err, "AllocationRequest 3 failed in run %s", param.name)
			waitForAllocatedQueueResource(t, leaf, 0, 1000)

			// schedule again, pending requests should be satisfied now
			ms.scheduler.MultiStepSchedule(5)

			waitForPendingQueueResource(t, leaf, 0, 1000)
			ms.mockRM.waitForAllocations(t, 2, 1000)
			assert.Equal(t, len(ms.mockRM.getAllocations()), 2)
		})
	}
}

// Test basic interactions from rm proxy to cache and to scheduler.
func TestRMNodeActions(t *testing.T) {
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
`
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	node1ID := "node-1:1234"
	node2ID := "node-2:1234"

	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     node1ID,
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
			{
				NodeID:     node2ID,
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")

	ms.mockRM.waitForAcceptedNode(t, node1ID, 1000)
	ms.mockRM.waitForAcceptedNode(t, node2ID, 1000)

	// verify the nodes
	context := ms.scheduler.GetClusterContext()
	waitForNewNode(t, context, node1ID, partition, 1000)
	waitForNewNode(t, context, node2ID, partition, 1000)

	// verify all nodes are schedule-able
	assert.Equal(t, ms.scheduler.GetClusterContext().GetPartition(partition).GetNode(node1ID).IsSchedulable(), true)
	assert.Equal(t, ms.scheduler.GetClusterContext().GetPartition(partition).GetNode(node2ID).IsSchedulable(), true)

	// send RM node action DRAIN_NODE (move to unschedulable)
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     node1ID,
				Action:     si.NodeInfo_DRAIN_NODE,
				Attributes: make(map[string]string),
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest 2 failed")

	err = common.WaitFor(10*time.Millisecond, 10*time.Second, func() bool {
		return !ms.scheduler.GetClusterContext().GetPartition(partition).GetNode(node1ID).IsSchedulable() &&
			ms.scheduler.GetClusterContext().GetPartition(partition).GetNode(node2ID).IsSchedulable()
	})

	assert.NilError(t, err, "timed out waiting for node in cache")

	// send RM node action: DRAIN_TO_SCHEDULABLE (make it schedulable again)
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     node1ID,
				Action:     si.NodeInfo_DRAIN_TO_SCHEDULABLE,
				Attributes: make(map[string]string),
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest 3 failed")

	err = common.WaitFor(10*time.Millisecond, 10*time.Second, func() bool {
		return ms.scheduler.GetClusterContext().GetPartition(partition).GetNode(node1ID).IsSchedulable() &&
			ms.scheduler.GetClusterContext().GetPartition(partition).GetNode(node2ID).IsSchedulable()
	})

	assert.NilError(t, err, "timed out waiting for node in cache")

	// send RM node action: DECOMMISSION (make it unschedulable and tell partition to delete)
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     node2ID,
				Action:     si.NodeInfo_DECOMISSION,
				Attributes: make(map[string]string),
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest 3 failed")

	// node removal can be really quick: cannot test for unschedulable state (nil pointer)
	// verify that the node (node-2) was removed
	waitForRemovedNode(t, context, node2ID, partition, 10000)
}

func TestBinPackingAllocationForApplications(t *testing.T) {
	configData := `
partitions:
  -
    name: default
    nodesortpolicy:
        type: binpacking
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: leaf
`
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	leafName := "root.leaf"
	app1ID := appID1
	app2ID := appID2

	// Register a node, and add applications
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")

	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{app1ID: leafName, app2ID: leafName}),
		RmID: "rm:123",
	})
	assert.NilError(t, err, "ApplicationRequest failed")
	ms.mockRM.waitForAcceptedApplication(t, app1ID, 1000)
	ms.mockRM.waitForAcceptedApplication(t, app2ID, 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 1},
					},
				},
				MaxAllocations: 20,
				ApplicationID:  app1ID,
			},
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 1},
					},
				},
				MaxAllocations: 20,
				ApplicationID:  app2ID,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest 2 failed")

	// Check the queue root
	root := ms.getQueue("root")

	// Check the queue a
	leaf := ms.getQueue(leafName)

	// Get the app
	app1 := ms.getApplication(app1ID)
	app2 := ms.getApplication(app2ID)

	waitForPendingQueueResource(t, leaf, 400, 1000)
	waitForPendingQueueResource(t, root, 400, 1000)
	waitForPendingAppResource(t, app1, 200, 1000)
	waitForPendingAppResource(t, app2, 200, 1000)

	ms.scheduler.MultiStepSchedule(9)
	ms.mockRM.waitForAllocations(t, 9, 1000)

	node1Alloc := ms.scheduler.GetClusterContext().GetPartition(partition).GetNode("node-1:1234").GetAllocatedResource().Resources[resources.MEMORY]
	node2Alloc := ms.scheduler.GetClusterContext().GetPartition(partition).GetNode("node-2:1234").GetAllocatedResource().Resources[resources.MEMORY]
	// we do not know which node was chosen so we need to check:
	// node1 == 90 && node2 == 0  || node1 == 0 && node2 == 90
	if !(node1Alloc == 90 && node2Alloc == 0) && !(node1Alloc == 0 && node2Alloc == 90) {
		t.Errorf("allocation not contained on one: node1 = %d, node2 = %d", node1Alloc, node2Alloc)
	}
}

func TestFairnessAllocationForNodes(t *testing.T) {
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: leaf
`
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Register 10 nodes, and add applications
	nodes := make([]*si.NodeInfo, 0)
	for i := 0; i < 10; i++ {
		nodeID := "node-" + strconv.Itoa(i)
		nodes = append(nodes, &si.NodeInfo{
			NodeID:     nodeID + ":1234",
			Attributes: map[string]string{},
			SchedulableResource: &si.Resource{
				Resources: map[string]*si.Quantity{
					"memory": {Value: 100},
					"vcore":  {Value: 20},
				},
			},
			Action: si.NodeInfo_CREATE,
		})
	}

	leafName := "root.leaf"
	appID := appID1

	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: nodes,
		RmID:  "rm:123",
	})

	assert.NilError(t, err, "NodeRequest failed")

	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID: leafName}),
		RmID: "rm:123",
	})

	assert.NilError(t, err, "ApplicationRequest failed")

	// verify app and all nodes are accepted
	ms.mockRM.waitForAcceptedApplication(t, appID, 1000)
	for _, node := range nodes {
		ms.mockRM.waitForAcceptedNode(t, node.NodeID, 1000)
	}

	context := ms.scheduler.GetClusterContext()
	for _, node := range nodes {
		waitForNewNode(t, context, node.NodeID, partition, 1000)
	}

	// Request ask with 20 allocations
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 1},
					},
				},
				MaxAllocations: 20,
				ApplicationID:  appID,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "AllocationRequest failed")

	leaf := ms.getQueue(leafName)
	app := ms.getApplication(appID)

	waitForPendingQueueResource(t, leaf, 200, 1000)
	waitForPendingAppResource(t, app, 200, 1000)

	ms.scheduler.MultiStepSchedule(20)

	// Verify all requests are satisfied
	ms.mockRM.waitForAllocations(t, 20, 1000)
	waitForPendingQueueResource(t, leaf, 0, 1000)
	waitForPendingAppResource(t, app, 0, 1000)
	assert.Equal(t, int(app.GetAllocatedResource().Resources[resources.MEMORY]), 200)

	// Verify 2 allocations for every node
	for _, node := range nodes {
		node := ms.scheduler.GetClusterContext().GetNode(node.NodeID, partition)
		assert.Equal(t, int(node.GetAllocatedResource().Resources[resources.MEMORY]), 20, "node %s did not get 2 allocated", node.NodeID)
	}
}

// this test cases covers a basic gang scheduling scenario, where an app has
// 1 member in the gang and 1 actual request, it verifies each step of the
// placeholder replacement works as expected.
// it does an extra verification at the end, by simulating a dup release allocation
// request, ensure this won't cause any NPE issue.
// nolint: funlen
func TestDupReleasesInGangScheduling(t *testing.T) {
	// Register RM
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configDataSmokeTest, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	leafName := "root.singleleaf"
	part := ms.scheduler.GetClusterContext().GetPartition(partition)
	root := part.GetQueue("root")
	leaf := part.GetQueue(leafName)

	// Register a node, and add apps
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err)

	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID1: leafName}),
		RmID: "rm:123",
	})
	assert.NilError(t, err)

	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	// Get the app
	app := ms.getApplication(appID1)

	// Verify app initial state
	var app01 *objects.Application
	app01, err = getApplication(part, appID1)
	assert.NilError(t, err, "application not found")
	assert.Equal(t, app01.CurrentState(), objects.New.String())

	// shim side creates a placeholder, and send the UpdateRequest
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 1},
					},
				},
				TaskGroupName:  "tg",
				Placeholder:    true,
				MaxAllocations: 1,
				ApplicationID:  appID1,
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "AllocationRequest failed")

	// schedule and make sure the placeholder gets allocated
	ms.scheduler.MultiStepSchedule(5)
	ms.mockRM.waitForAllocations(t, 1, 1000)

	// verify the placeholder allocation
	assert.Equal(t, len(app01.GetAllAllocations()), 1)
	assert.Equal(t, int(app.GetPlaceholderResource().Resources[resources.MEMORY]), 10,
		"app allocated memory incorrect")
	placeholderAlloc := app01.GetAllAllocations()[0]

	// Check allocated resources of nodes
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), ms.partitionName,
		[]string{"node-1:1234", "node-2:1234"}, 10, 1000)

	// shim submits the actual pod for scheduling
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-2",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 1},
					},
				},
				MaxAllocations: 1,
				ApplicationID:  appID1,
				Placeholder:    false,
				TaskGroupName:  "tg",
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "AllocationRequest 3 failed")

	// schedule and this triggers a placeholder REPLACE
	ms.scheduler.MultiStepSchedule(10)
	// the core releases the placeholder allocation,
	// and it waits for the shim's confirmation
	ms.mockRM.waitForAllocations(t, 0, 1000)

	// shim responses the allocation has been released
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: []*si.AllocationRelease{
				{
					PartitionName:   "default",
					ApplicationID:   appID1,
					UUID:            placeholderAlloc.UUID,
					TerminationType: si.TerminationType_PLACEHOLDER_REPLACED,
				},
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err)

	// schedule and verify the actual request gets allocated
	ms.scheduler.MultiStepSchedule(5)
	ms.mockRM.waitForAllocations(t, 1, 1000)

	// actual request gets allocated
	// placeholder requests have been all released
	// and queue has no pending resources
	assert.Equal(t, int(app.GetAllocatedResource().Resources[resources.MEMORY]), 10)
	assert.Equal(t, int(app.GetPlaceholderResource().Resources[resources.MEMORY]), 0,
		"app allocated memory incorrect")
	waitForPendingQueueResource(t, leaf, 0, 1000)
	waitForPendingQueueResource(t, root, 0, 1000)

	// simulate the shim sends the release request again
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: []*si.AllocationRelease{
				{
					PartitionName:   "default",
					ApplicationID:   appID1,
					UUID:            placeholderAlloc.UUID,
					TerminationType: si.TerminationType_PLACEHOLDER_REPLACED,
				},
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err)

	ms.scheduler.MultiStepSchedule(5)
	ms.mockRM.waitForAllocations(t, 1, 1000)
}
