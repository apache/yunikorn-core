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
	"bytes"
	"strconv"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

const partition = "[rm:123]default"

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
	partitionInfo := ms.clusterInfo.GetPartition(partition)
	assert.Assert(t, partitionInfo.Root.GetMaxResource() == nil, "partition info max resource not nil, first load")

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")
	assert.Assert(t, schedulerQueueRoot.QueueInfo.GetMaxResource() == nil, "root scheduling queue max resource not nil, first load")

	// Check scheduling queue root.base
	schedulerQueue := ms.getSchedulingQueue("root.base")
	assert.Equal(t, int(schedulerQueue.QueueInfo.GetMaxResource().Resources[resources.MEMORY]), 150, "max resource on leaf not set correctly")

	// Check the queue which will be removed
	schedulerQueue = ms.getSchedulingQueue("root.tobedeleted")
	assert.Assert(t, schedulerQueue.QueueInfo.IsRunning(), "child queue root.tobedeleted is not in running state")

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
	err = ms.proxy.ReloadConfiguration("rm:123")

	assert.NilError(t, err, "configuration reload failed")

	// wait until configuration is reloaded
	if err = common.WaitFor(time.Second, 5*time.Second, func() bool {
		return !bytes.Equal(configs.ConfigContext.Get("policygroup").Checksum, configChecksum)
	}); err != nil {
		t.Errorf("timeout waiting for configuration to be reloaded: %v", err)
	}

	// Check queues of cache and scheduler.
	partitionInfo = ms.clusterInfo.GetPartition(partition)
	assert.Assert(t, partitionInfo.Root.GetMaxResource() == nil, "partition info max resource not nil, reload")

	// Check scheduling queue root
	schedulerQueueRoot = ms.getSchedulingQueue("root")
	assert.Assert(t, schedulerQueueRoot.QueueInfo.GetMaxResource() == nil, "root scheduling queue max resource not nil, reload")

	// Check scheduling queue root.base
	schedulerQueue = ms.getSchedulingQueue("root.base")
	assert.Equal(t, int(schedulerQueue.QueueInfo.GetMaxResource().Resources[resources.MEMORY]), 1000, "max resource on leaf not set correctly")
	assert.Equal(t, int(schedulerQueue.QueueInfo.GetGuaranteedResource().Resources[resources.VCORE]), 250, "guaranteed resource on leaf not set correctly")

	schedulerQueue = ms.getSchedulingQueue("root.tobeadded")
	assert.Assert(t, schedulerQueue != nil, "scheduling queue root.tobeadded is not found")
	// check the removed queue state
	schedulerQueue = ms.getSchedulingQueue("root.tobedeleted")
	assert.Assert(t, schedulerQueue.QueueInfo.IsDraining(), "child queue root.tobedeleted is not in draining state")

	// Check queues of cache and scheduler for the newly added partition
	partitionInfo = ms.clusterInfo.GetPartition("[rm:123]gpu")
	assert.Assert(t, partitionInfo != nil, "gpu partition not found")
	assert.Assert(t, partitionInfo.Root.GetMaxResource() == nil, "GPU partition info max resource not nil")

	// Check scheduling queue root
	schedulerQueueRoot = ms.getSchedulingQueuePartition("root", "[rm:123]gpu")
	assert.Assert(t, schedulerQueueRoot.QueueInfo.GetMaxResource() == nil, "max resource on root set")

	// Check scheduling queue root.production
	schedulerQueue = ms.getSchedulingQueuePartition("root.production", "[rm:123]gpu")
	assert.Assert(t, schedulerQueue != nil, "New partition: scheduling queue root.production is not found")
}

// Test basic interactions from rm proxy to cache and to scheduler.
func TestBasicScheduler(t *testing.T) {
	// Register RM
	configData := `
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
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	leafName := "root.singleleaf"
	appID := "app-1"
	// Check queues of cache and scheduler.
	partitionInfo := ms.clusterInfo.GetPartition(partition)
	assert.Assert(t, partitionInfo.Root.GetMaxResource() == nil, "partition info max resource nil")

	// Check scheduling queue root
	root := ms.getSchedulingQueue("root")
	assert.Assert(t, root.QueueInfo.GetMaxResource() == nil, "root queue max resource should be nil")

	// Check scheduling queue singleleaf
	leaf := ms.getSchedulingQueue(leafName)
	assert.Equal(t, int(leaf.QueueInfo.GetMaxResource().Resources[resources.MEMORY]), 150, "%s config not set correct", leafName)

	// Register a node, and add apps
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
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
			},
		},
		NewApplications: newAddAppRequest(map[string]string{appID: leafName}),
		RmID:            "rm:123",
	})
	assert.NilError(t, err, "UpdateRequest failed")

	ms.mockRM.waitForAcceptedApplication(t, appID, 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	// Get scheduling app
	schedulingApp := ms.getSchedulingApplication(appID)

	// Verify app initial state
	var app01 *cache.ApplicationInfo
	app01, err = getApplicationInfoFromPartition(partitionInfo, appID)
	assert.NilError(t, err, "application not found")

	assert.Equal(t, app01.GetApplicationState(), cache.Accepted.String())

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
				ApplicationID:  appID,
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "UpdateRequest 2 failed")

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 10 * 2 = 20;
	waitForPendingQueueResource(t, leaf, 20, 1000)
	waitForPendingQueueResource(t, root, 20, 1000)
	waitForPendingAppResource(t, schedulingApp, 20, 1000)

	ms.scheduler.MultiStepSchedule(16)

	ms.mockRM.waitForAllocations(t, 2, 1000)

	// Make sure pending resource updated to 0
	waitForPendingQueueResource(t, leaf, 0, 1000)
	waitForPendingQueueResource(t, root, 0, 1000)
	waitForPendingAppResource(t, schedulingApp, 0, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, int(leaf.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY]), 20, "leaf allocated memory incorrect")
	assert.Equal(t, int(root.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY]), 20, "root allocated memory incorrect")
	assert.Equal(t, int(schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY]), 20, "app allocated memory incorrect")

	// once we start to process allocation asks from this app, verify the state again
	assert.Equal(t, app01.GetApplicationState(), cache.Running.String())

	// Check allocated resources of nodes
	waitForNodesAllocatedResource(t, ms.clusterInfo, ms.partitionName, []string{"node-1:1234", "node-2:1234"}, 20, 1000)

	// Ask for two more resources
	err = ms.proxy.Update(&si.UpdateRequest{
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
				ApplicationID:  appID,
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
				ApplicationID:  appID,
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "UpdateRequest 3 failed")

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 50 * 2 + 100 * 2 = 300;
	waitForPendingQueueResource(t, leaf, 300, 1000)
	waitForPendingQueueResource(t, root, 300, 1000)
	waitForPendingAppResource(t, schedulingApp, 300, 1000)

	// Now app-1 uses 20 resource, and queue-a's max = 150, so it can get two 50 container allocated.
	ms.scheduler.MultiStepSchedule(16)

	ms.mockRM.waitForAllocations(t, 4, 1000)

	// Check pending resource, should be 200 now.
	waitForPendingQueueResource(t, leaf, 200, 1000)
	waitForPendingQueueResource(t, root, 200, 1000)
	waitForPendingAppResource(t, schedulingApp, 200, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, int(leaf.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY]), 120, "leaf allocated memory incorrect")
	assert.Equal(t, int(root.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY]), 120, "root allocated memory incorrect")
	assert.Equal(t, int(schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY]), 120, "app allocated memory incorrect")

	// Check allocated resources of nodes
	waitForNodesAllocatedResource(t, ms.clusterInfo, partition, []string{"node-1:1234", "node-2:1234"}, 120, 1000)

	updateRequest := &si.UpdateRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: make([]*si.AllocationReleaseRequest, 0),
		},
		RmID: "rm:123",
	}

	// Release all allocations
	for _, v := range ms.mockRM.getAllocations() {
		updateRequest.Releases.AllocationsToRelease = append(updateRequest.Releases.AllocationsToRelease, &si.AllocationReleaseRequest{
			UUID:          v.UUID,
			ApplicationID: v.ApplicationID,
			PartitionName: v.PartitionName,
		})
	}

	// Release Allocations.
	err = ms.proxy.Update(updateRequest)
	assert.NilError(t, err, "UpdateRequest 4 failed")

	ms.mockRM.waitForAllocations(t, 0, 1000)

	// Check pending resource, should be 200 (same)
	waitForPendingQueueResource(t, leaf, 200, 1000)
	waitForPendingQueueResource(t, root, 200, 1000)
	waitForPendingAppResource(t, schedulingApp, 200, 1000)

	// Check allocated resources of queues, apps should be 0 now
	assert.Equal(t, int(leaf.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY]), 0, "leaf allocated memory incorrect")
	assert.Equal(t, int(root.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY]), 0, "root allocated memory incorrect")
	assert.Equal(t, int(schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY]), 0, "app allocated memory incorrect")

	// Release asks
	err = ms.proxy.Update(&si.UpdateRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationAsksToRelease: []*si.AllocationAskReleaseRequest{
				{
					ApplicationID: appID,
					PartitionName: "default",
				},
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "UpdateRequest 5 failed")

	// Check pending resource
	waitForPendingQueueResource(t, leaf, 0, 1000)
	waitForPendingQueueResource(t, root, 0, 1000)
	waitForPendingAppResource(t, schedulingApp, 0, 1000)
}

func TestBasicSchedulerAutoAllocation(t *testing.T) {
	configData := `
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
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, true)
	assert.NilError(t, err, "RegisterResourceManager failed")

	leafName := "root.singleleaf"
	appID := "app-1"

	// Register a node, and add apps
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
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
			},
		},
		NewApplications: newAddAppRequest(map[string]string{appID: leafName}),
		RmID:            "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	ms.mockRM.waitForAcceptedApplication(t, appID, 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

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
				MaxAllocations: 20,
				ApplicationID:  appID,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest 2 failed")

	// wait until we have maxed out the leaf queue
	ms.mockRM.waitForAllocations(t, 15, 1000)

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")

	// Check scheduling leaf queue
	schedulerQueue := ms.getSchedulingQueue(leafName)

	// Get scheduling app
	schedulingApp := ms.getSchedulingApplication(appID)

	// Make sure pending resource decreased to 50
	waitForPendingQueueResource(t, schedulerQueue, 50, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 50, 1000)
	waitForPendingAppResource(t, schedulingApp, 50, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, int(schedulerQueue.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY]), 150, "leaf allocated memory incorrect")
	assert.Equal(t, int(schedulerQueueRoot.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY]), 150, "root allocated memory incorrect")
	assert.Equal(t, int(schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY]), 150, "app allocated memory incorrect")

	// Check allocated resources of nodes
	waitForNodesAllocatedResource(t, ms.clusterInfo, partition, []string{"node-1:1234", "node-2:1234"}, 150, 1000)
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
	app1ID := "app-1"
	leafApp2 := "root.leaf2"
	app2ID := "app-2"

	// Register a node, and add apps
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
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
			},
		},
		NewApplications: newAddAppRequest(map[string]string{app1ID: leafApp1, app2ID: leafApp2}),
		RmID:            "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	ms.mockRM.waitForAcceptedApplication(t, app1ID, 1000)
	ms.mockRM.waitForAcceptedApplication(t, app2ID, 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

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

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")

	// Check scheduling leaf queue app1
	schedulerQueue1 := ms.getSchedulingQueue(leafApp1)
	app1 := ms.getSchedulingApplication(app1ID)

	// Check scheduling leaf queue app2
	schedulerQueue2 := ms.getSchedulingQueue(leafApp2)
	app2 := ms.getSchedulingApplication(app2ID)

	// Check pending resource, should be 100 (same)
	waitForPendingQueueResource(t, schedulerQueue1, 200, 1000)
	waitForPendingQueueResource(t, schedulerQueue2, 200, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 400, 1000)

	ms.scheduler.MultiStepSchedule(25)
	ms.mockRM.waitForAllocations(t, 20, 1500)

	waitForAllocatedAppResource(t, app1, 100, 1000)
	waitForAllocatedAppResource(t, app2, 100, 1000)
	// Make sure pending resource updated to 0
	waitForPendingQueueResource(t, schedulerQueue1, 100, 1000)
	waitForPendingQueueResource(t, schedulerQueue2, 100, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 200, 1000)
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
	app1ID := "app-1"
	app2ID := "app-2"

	// Register a node, and add applications
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
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
			},
		},
		NewApplications: newAddAppRequest(map[string]string{app1ID: leafName, app2ID: leafName}),
		RmID:            "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	ms.mockRM.waitForAcceptedApplication(t, app1ID, 1000)
	ms.mockRM.waitForAcceptedApplication(t, app2ID, 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

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

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")

	// Check scheduling queue a
	schedulerQueue := ms.getSchedulingQueue(leafName)

	// Get scheduling app
	schedulingApp1 := ms.getSchedulingApplication("app-1")
	schedulingApp2 := ms.getSchedulingApplication("app-2")

	waitForPendingQueueResource(t, schedulerQueue, 400, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 400, 1000)
	waitForPendingAppResource(t, schedulingApp1, 200, 1000)
	waitForPendingAppResource(t, schedulingApp2, 200, 1000)

	ms.scheduler.MultiStepSchedule(25)

	ms.mockRM.waitForAllocations(t, 20, 1000)

	// Make sure pending resource updated to 100, which means
	waitForPendingQueueResource(t, schedulerQueue, 200, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 200, 1000)
	waitForPendingAppResource(t, schedulingApp1, 100, 1000)
	waitForPendingAppResource(t, schedulingApp2, 100, 1000)

	// Both apps got 100 resources,
	assert.Equal(t, int(schedulingApp1.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY]), 100, "app1 allocated resource incorrect")
	assert.Equal(t, int(schedulingApp2.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY]), 100, "app2 allocated resource incorrect")
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

	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)

	err = ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-reject-1": "root.non-exist-queue"}),
		RmID:            "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest 2 failed")

	ms.mockRM.waitForRejectedApplication(t, "app-reject-1", 1000)

	err = ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-added-2": "root.leaf"}),
		RmID:            "rm:123",
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
			err = ms.proxy.Update(&si.UpdateRequest{
				NewSchedulableNodes: []*si.NewNodeInfo{
					{
						NodeID:     "node-1:1234",
						Attributes: map[string]string{},
						SchedulableResource: &si.Resource{
							Resources: map[string]*si.Quantity{
								"memory": {Value: 150},
								"vcore":  {Value: 15},
							},
						},
					},
				},
				NewApplications: newAddAppRequest(map[string]string{"app-1": param.leafQueue}),
				RmID:            "rm:123",
			})

			assert.NilError(t, err, "UpdateRequest failed in run %s", param.name)

			ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)

			err = ms.proxy.Update(&si.UpdateRequest{
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
						ApplicationID:  "app-1",
					},
				},
				RmID: "rm:123",
			})

			assert.NilError(t, err, "UpdateRequest 2 failed in run %s", param.name)

			schedulingQueue := ms.getSchedulingQueue(param.leafQueue)

			waitForPendingQueueResource(t, schedulingQueue, 120, 1000)

			ms.scheduler.MultiStepSchedule(20)

			// 100 memory gets allocated, 20 pending because the capacity is 100
			waitForPendingQueueResource(t, schedulingQueue, 20, 1000)

			app1 := ms.getSchedulingApplication("app-1")
			if app1 == nil {
				t.Fatal("application 'app-1' not found in cache")
			}
			waitForAllocatedAppResource(t, app1, 100, 1000)

			assert.Equal(t, len(app1.ApplicationInfo.GetAllAllocations()), 10, "number of app allocations incorrect")
			assert.Equal(t, int(app1.GetAllocatedResource().Resources[resources.MEMORY]), 100, "app allocated resource incorrect")
			assert.Equal(t, len(ms.mockRM.getAllocations()), 10, "number of RM allocations incorrect")

			// release all allocated allocations
			allocReleases := make([]*si.AllocationReleaseRequest, 0)
			for _, alloc := range ms.mockRM.getAllocations() {
				allocReleases = append(allocReleases, &si.AllocationReleaseRequest{
					PartitionName: "default",
					ApplicationID: "app-1",
					UUID:          alloc.UUID,
					Message:       "",
				})
			}

			err = ms.proxy.Update(&si.UpdateRequest{
				Releases: &si.AllocationReleasesRequest{
					AllocationsToRelease: allocReleases,
				},
				RmID: "rm:123",
			})

			assert.NilError(t, err, "UpdateRequest 3 failed in run %s", param.name)
			waitForAllocatedQueueResource(t, schedulingQueue, 0, 1000)

			// schedule again, pending requests should be satisfied now
			ms.scheduler.MultiStepSchedule(10)

			waitForPendingQueueResource(t, schedulingQueue, 0, 1000)
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

	err = ms.proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeID:     node1ID,
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
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
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	ms.mockRM.waitForAcceptedNode(t, node1ID, 1000)
	ms.mockRM.waitForAcceptedNode(t, node2ID, 1000)

	// verify scheduling nodes
	context := ms.scheduler.GetClusterSchedulingContext()
	waitForNewSchedulerNode(t, context, node1ID, partition, 1000)
	waitForNewSchedulerNode(t, context, node2ID, partition, 1000)

	// verify all nodes are schedule-able
	assert.Equal(t, ms.clusterInfo.GetPartition(partition).GetNode(node1ID).IsSchedulable(), true)
	assert.Equal(t, ms.clusterInfo.GetPartition(partition).GetNode(node2ID).IsSchedulable(), true)

	// send RM node action DRAIN_NODE (move to unschedulable)
	err = ms.proxy.Update(&si.UpdateRequest{
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID:     node1ID,
				Action:     si.UpdateNodeInfo_DRAIN_NODE,
				Attributes: make(map[string]string),
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest 2 failed")

	err = common.WaitFor(10*time.Millisecond, 10*time.Second, func() bool {
		return !ms.clusterInfo.GetPartition(partition).GetNode(node1ID).IsSchedulable() &&
			ms.clusterInfo.GetPartition(partition).GetNode(node2ID).IsSchedulable()
	})

	assert.NilError(t, err, "timed out waiting for node in cache")

	// send RM node action: DRAIN_TO_SCHEDULABLE (make it schedulable again)
	err = ms.proxy.Update(&si.UpdateRequest{
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID:     node1ID,
				Action:     si.UpdateNodeInfo_DRAIN_TO_SCHEDULABLE,
				Attributes: make(map[string]string),
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest 3 failed")

	err = common.WaitFor(10*time.Millisecond, 10*time.Second, func() bool {
		return ms.clusterInfo.GetPartition(partition).GetNode(node1ID).IsSchedulable() &&
			ms.clusterInfo.GetPartition(partition).GetNode(node2ID).IsSchedulable()
	})

	assert.NilError(t, err, "timed out waiting for node in cache")

	// send RM node action: DECOMMISSION (make it unschedulable and tell partition to delete)
	err = ms.proxy.Update(&si.UpdateRequest{
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID:     node2ID,
				Action:     si.UpdateNodeInfo_DECOMISSION,
				Attributes: make(map[string]string),
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest 3 failed")

	// node removal can be really quick: cannot test for unschedulable state (nil pointer)
	// verify that scheduling node (node-2) was removed
	waitForRemovedSchedulerNode(t, context, node2ID, partition, 10000)
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
	app1ID := "app-1"
	app2ID := "app-2"

	// Register a node, and add applications
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
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
			},
		},
		NewApplications: newAddAppRequest(map[string]string{app1ID: leafName, app2ID: leafName}),
		RmID:            "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	ms.mockRM.waitForAcceptedApplication(t, app1ID, 1000)
	ms.mockRM.waitForAcceptedApplication(t, app2ID, 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

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

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")

	// Check scheduling queue a
	schedulingQueue := ms.getSchedulingQueue(leafName)

	// Get scheduling app
	schedulingApp1 := ms.getSchedulingApplication(app1ID)
	schedulingApp2 := ms.getSchedulingApplication(app2ID)

	waitForPendingQueueResource(t, schedulingQueue, 400, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 400, 1000)
	waitForPendingAppResource(t, schedulingApp1, 200, 1000)
	waitForPendingAppResource(t, schedulingApp2, 200, 1000)

	ms.scheduler.MultiStepSchedule(9)
	ms.mockRM.waitForAllocations(t, 9, 1000)

	node1Alloc := ms.clusterInfo.GetPartition(partition).GetNode("node-1:1234").GetAllocatedResource().Resources[resources.MEMORY]
	node2Alloc := ms.clusterInfo.GetPartition(partition).GetNode("node-2:1234").GetAllocatedResource().Resources[resources.MEMORY]
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
	nodes := make([]*si.NewNodeInfo, 0)
	for i := 0; i < 10; i++ {
		nodeID := "node-" + strconv.Itoa(i)
		nodes = append(nodes, &si.NewNodeInfo{
			NodeID:     nodeID + ":1234",
			Attributes: map[string]string{},
			SchedulableResource: &si.Resource{
				Resources: map[string]*si.Quantity{
					"memory": {Value: 100},
					"vcore":  {Value: 20},
				},
			},
		})
	}

	leafName := "root.leaf"
	appID := "app-1"

	err = ms.proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: nodes,
		NewApplications:     newAddAppRequest(map[string]string{appID: leafName}),
		RmID:                "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	// verify app and all nodes are accepted
	ms.mockRM.waitForAcceptedApplication(t, appID, 1000)
	for _, node := range nodes {
		ms.mockRM.waitForAcceptedNode(t, node.NodeID, 1000)
	}

	context := ms.scheduler.GetClusterSchedulingContext()
	for _, node := range nodes {
		waitForNewSchedulerNode(t, context, node.NodeID, partition, 1000)
	}

	// Request ask with 20 allocations
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
				MaxAllocations: 20,
				ApplicationID:  appID,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	schedulerQueue := ms.getSchedulingQueue(leafName)
	schedulingApp := ms.getSchedulingApplication(appID)

	waitForPendingQueueResource(t, schedulerQueue, 200, 1000)
	waitForPendingAppResource(t, schedulingApp, 200, 1000)

	ms.scheduler.MultiStepSchedule(20)

	// Verify all requests are satisfied
	ms.mockRM.waitForAllocations(t, 20, 1000)
	waitForPendingQueueResource(t, schedulerQueue, 0, 1000)
	waitForPendingAppResource(t, schedulingApp, 0, 1000)
	assert.Equal(t, int(schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY]), 200)

	// Verify 2 allocations for every node
	for _, node := range nodes {
		schedulingNode := ms.scheduler.GetClusterSchedulingContext().GetSchedulingNode(node.NodeID, partition)
		assert.Equal(t, int(schedulingNode.GetAllocatedResource().Resources[resources.MEMORY]), 20, "node %s did not get 2 allocated", schedulingNode.NodeID)
	}
}
