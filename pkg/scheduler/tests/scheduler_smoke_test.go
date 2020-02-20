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
	if err != nil {
		t.Fatalf("RegisterResourceManager failed: %v", err)
	}

	// memorize the checksum of current configs
	configChecksum := configs.ConfigContext.Get("policygroup").Checksum

	// Check queues of cache and scheduler.
	partitionInfo := ms.clusterInfo.GetPartition("[rm:123]default")
	assert.Assert(t, nil == partitionInfo.Root.GetMaxResource(), "partition info max resource nil, first load")

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")
	assert.Assert(t, nil == schedulerQueueRoot.QueueInfo.GetMaxResource(), "root scheduling queue max resource nil, first load")

	// Check scheduling queue root.base
	schedulerQueue := ms.getSchedulingQueue("root.base")
	assert.Assert(t, 150 == schedulerQueue.QueueInfo.GetMaxResource().Resources[resources.MEMORY])

	// Check the queue which will be removed
	schedulerQueue = ms.getSchedulingQueue("root.tobedeleted")
	if !schedulerQueue.QueueInfo.IsRunning() {
		t.Errorf("child queue root.tobedeleted is not in running state")
	}

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

	if err != nil {
		t.Fatalf("configuration reload failed: %v", err)
	}

	// wait until configuration is reloaded
	if err = common.WaitFor(time.Second, 5*time.Second, func() bool {
		return !bytes.Equal(configs.ConfigContext.Get("policygroup").Checksum, configChecksum)
	}); err != nil {
		t.Errorf("timeout waiting for configuration to be reloaded: %v", err)
	}

	// Check queues of cache and scheduler.
	partitionInfo = ms.clusterInfo.GetPartition("[rm:123]default")
	assert.Assert(t, nil == partitionInfo.Root.GetMaxResource(), "partition info max resource nil, reload")

	// Check scheduling queue root
	schedulerQueueRoot = ms.getSchedulingQueue("root")
	assert.Assert(t, nil == schedulerQueueRoot.QueueInfo.GetMaxResource(), "root scheduling queue max resource nil, reload")

	// Check scheduling queue root.base
	schedulerQueue = ms.getSchedulingQueue("root.base")
	assert.Assert(t, 1000 == schedulerQueue.QueueInfo.GetMaxResource().Resources[resources.MEMORY])
	assert.Assert(t, 250 == schedulerQueue.QueueInfo.GuaranteedResource.Resources[resources.VCORE])

	// check the removed queue state
	schedulerQueue = ms.getSchedulingQueue("root.tobedeleted")
	if !schedulerQueue.QueueInfo.IsDraining() {
		t.Errorf("child queue root.tobedeleted is not in draining state")
	}
	// check the newly added queue
	schedulerQueue = ms.getSchedulingQueue("root.tobeadded")
	if schedulerQueue == nil {
		t.Errorf("scheduling queue root.tobeadded is not found")
	}

	// Check queues of cache and scheduler for the newly added partition
	partitionInfo = ms.clusterInfo.GetPartition("[rm:123]gpu")
	if partitionInfo == nil {
		t.Fatal("gpu partition not found")
	}
	assert.Assert(t, nil == partitionInfo.Root.GetMaxResource(), "GPU partition info max resource nil")

	// Check scheduling queue root
	schedulerQueueRoot = ms.getSchedulingQueuePartition("root", "[rm:123]gpu")
	assert.Assert(t, nil == schedulerQueueRoot.QueueInfo.GetMaxResource())

	// Check scheduling queue root.production
	schedulerQueue = ms.getSchedulingQueuePartition("root.production", "[rm:123]gpu")
	if schedulerQueue == nil {
		t.Errorf("New partition: scheduling queue root.production is not found")
	}
}

// Test basic interactions from rm proxy to cache and to scheduler.
func TestBasicScheduler(t *testing.T) {
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
	if err != nil {
		t.Fatalf("RegisterResourceManager failed: %v", err)
	}

	// Check queues of cache and scheduler.
	partitionInfo := ms.clusterInfo.GetPartition("[rm:123]default")
	assert.Assert(t, nil == partitionInfo.Root.GetMaxResource(), "partition info max resource nil")

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")
	assert.Assert(t, nil == schedulerQueueRoot.QueueInfo.GetMaxResource())

	// Check scheduling queue a
	schedulerQueueA := ms.getSchedulingQueue("root.a")
	assert.Assert(t, 150 == schedulerQueueA.QueueInfo.GetMaxResource().Resources[resources.MEMORY])

	// Register a node, and add apps
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
			{
				NodeID: "node-2:1234",
				Attributes: map[string]string{
					"si.io/hostname": "node-2",
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

	if err != nil {
		t.Fatalf("UpdateRequest failed: %v", err)
	}

	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	// Get scheduling app
	schedulingApp := ms.getSchedulingApplication("app-1")

	// Verify app initial state
	var app01 *cache.ApplicationInfo
	app01, err = getApplicationInfoFromPartition(partitionInfo, "app-1")
	if err != nil {
		t.Fatalf("application not found: %v", err)
	}
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
				ApplicationID:  "app-1",
			},
		},
		RmID: "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest 2 failed: %v", err)
	}

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 10 * 2 = 20;
	waitForPendingQueueResource(t, schedulerQueueA, 20, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 20, 1000)
	waitForPendingAppResource(t, schedulingApp, 20, 1000)

	ms.scheduler.MultiStepSchedule(16)

	ms.mockRM.waitForAllocations(t, 2, 1000)

	// Make sure pending resource updated to 0
	waitForPendingQueueResource(t, schedulerQueueA, 0, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 0, 1000)
	waitForPendingAppResource(t, schedulingApp, 0, 1000)

	// Check allocated resources of queues, apps
	assert.Assert(t, schedulerQueueA.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 20)
	assert.Assert(t, schedulerQueueRoot.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 20)
	assert.Assert(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 20)

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
				ApplicationID:  "app-1",
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
				ApplicationID:  "app-1",
			},
		},
		RmID: "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest 3 failed: %v", err)
	}

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 50 * 2 + 100 * 2 = 300;
	waitForPendingQueueResource(t, schedulerQueueA, 300, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 300, 1000)
	waitForPendingAppResource(t, schedulingApp, 300, 1000)

	// Now app-1 uses 20 resource, and queue-a's max = 150, so it can get two 50 container allocated.
	ms.scheduler.MultiStepSchedule(16)

	ms.mockRM.waitForAllocations(t, 4, 1000)

	// Check pending resource, should be 200 now.
	waitForPendingQueueResource(t, schedulerQueueA, 200, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 200, 1000)
	waitForPendingAppResource(t, schedulingApp, 200, 1000)

	// Check allocated resources of queues, apps
	assert.Assert(t, schedulerQueueA.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 120)
	assert.Assert(t, schedulerQueueRoot.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 120)
	assert.Assert(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 120)

	// Check allocated resources of nodes
	waitForNodesAllocatedResource(t, ms.clusterInfo, "[rm:123]default", []string{"node-1:1234", "node-2:1234"}, 120, 1000)

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

	if err != nil {
		t.Fatalf("UpdateRequest 4 failed: %v", err)
	}

	ms.mockRM.waitForAllocations(t, 0, 1000)

	// Check pending resource, should be 200 (same)
	waitForPendingQueueResource(t, schedulerQueueA, 200, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 200, 1000)
	waitForPendingAppResource(t, schedulingApp, 200, 1000)

	// Check allocated resources of queues, apps should be 0 now
	assert.Assert(t, schedulerQueueA.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 0)
	assert.Assert(t, schedulerQueueRoot.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 0)
	assert.Assert(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 0)

	// Release asks
	err = ms.proxy.Update(&si.UpdateRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationAsksToRelease: []*si.AllocationAskReleaseRequest{
				{
					ApplicationID: "app-1",
					PartitionName: "default",
				},
			},
		},
		RmID: "rm:123",
	})
	if err != nil {
		t.Fatalf("UpdateRequest 5 failed: %v", err)
	}

	// Release Allocations.
	err = ms.proxy.Update(updateRequest)
	if err != nil {
		t.Fatalf("UpdateRequest 6 failed: %v", err)
	}

	// Check pending resource
	waitForPendingQueueResource(t, schedulerQueueA, 0, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 0, 1000)
	waitForPendingAppResource(t, schedulingApp, 0, 1000)
}

func TestBasicSchedulerAutoAllocation(t *testing.T) {
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
	ms := &mockScheduler{}
	err := ms.Init(configData, true)
	if err != nil {
		t.Fatalf("RegisterResourceManager failed: %v", err)
	}

	// Register a node, and add apps
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
			{
				NodeID: "node-2:1234",
				Attributes: map[string]string{
					"si.io/hostname": "node-2",
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

	if err != nil {
		t.Fatalf("UpdateRequest failed: %v", err)
	}

	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)
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
				ApplicationID:  "app-1",
			},
		},
		RmID: "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest 2 failed: %v", err)
	}

	ms.mockRM.waitForAllocations(t, 15, 1000)

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")

	// Check scheduling queue a
	schedulerQueueA := ms.getSchedulingQueue("root.a")

	// Get scheduling app
	schedulingApp := ms.getSchedulingApplication("app-1")

	// Make sure pending resource updated to 0
	waitForPendingQueueResource(t, schedulerQueueA, 50, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 50, 1000)
	waitForPendingAppResource(t, schedulingApp, 50, 1000)

	// Check allocated resources of queues, apps
	assert.Assert(t, schedulerQueueA.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 150)
	assert.Assert(t, schedulerQueueRoot.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 150)
	assert.Assert(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 150)

	// Check allocated resources of nodes
	waitForNodesAllocatedResource(t, ms.clusterInfo, "[rm:123]default", []string{"node-1:1234", "node-2:1234"}, 150, 1000)
}

func TestFairnessAllocationForQueues(t *testing.T) {
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
          - name: b
            resources:
              guaranteed:
                memory: 100
                vcore: 10
`
	ms := &mockScheduler{}
	err := ms.Init(configData, false)
	if err != nil {
		t.Fatalf("RegisterResourceManager failed: %v", err)
	}

	// Register a node, and add apps
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
			{
				NodeID: "node-2:1234",
				Attributes: map[string]string{
					"si.io/hostname": "node-2",
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
		NewApplications: newAddAppRequest(map[string]string{"app-1": "root.a", "app-2": "root.b"}),
		RmID:            "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest failed: %v", err)
	}

	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)
	ms.mockRM.waitForAcceptedApplication(t, "app-2", 1000)
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
				ApplicationID:  "app-1",
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
				ApplicationID:  "app-2",
			},
		},
		RmID: "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest 2 failed: %v", err)
	}

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")

	// Check scheduling queue a
	schedulerQueueA := ms.getSchedulingQueue("root.a")

	// Check scheduling queue b
	schedulerQueueB := ms.getSchedulingQueue("root.b")

	// Check pending resource, should be 100 (same)
	waitForPendingQueueResource(t, schedulerQueueA, 200, 1000)
	waitForPendingQueueResource(t, schedulerQueueB, 200, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 400, 1000)

	for i := 0; i < 20; i++ {
		ms.scheduler.MultiStepSchedule(1)
		time.Sleep(100 * time.Millisecond)
	}

	ms.mockRM.waitForAllocations(t, 20, 1000)

	// Make sure pending resource updated to 0
	waitForPendingQueueResource(t, schedulerQueueA, 100, 1000)
	waitForPendingQueueResource(t, schedulerQueueB, 100, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 200, 1000)
}

func TestFairnessAllocationForApplications(t *testing.T) {
	configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            properties:
              application.sort.policy: fair
            resources:
              guaranteed:
                memory: 100
                vcore: 10
          - name: b
            resources:
              guaranteed:
                memory: 100
                vcore: 10
`
	ms := &mockScheduler{}
	err := ms.Init(configData, false)
	if err != nil {
		t.Fatalf("RegisterResourceManager failed: %v", err)
	}

	// Register a node, and add applications
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
			{
				NodeID: "node-2:1234",
				Attributes: map[string]string{
					"si.io/hostname": "node-2",
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
		NewApplications: newAddAppRequest(map[string]string{"app-1": "root.a", "app-2": "root.a"}),
		RmID:            "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest failed: %v", err)
	}

	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)
	ms.mockRM.waitForAcceptedApplication(t, "app-2", 1000)
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
				ApplicationID:  "app-1",
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
				ApplicationID:  "app-2",
			},
		},
		RmID: "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest 2 failed: %v", err)
	}

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")

	// Check scheduling queue a
	schedulerQueueA := ms.getSchedulingQueue("root.a")

	// Check scheduling queue b
	schedulerQueueB := ms.getSchedulingQueue("root.b")

	// Get scheduling app
	schedulingApp1 := ms.getSchedulingApplication("app-1")
	schedulingApp2 := ms.getSchedulingApplication("app-2")

	waitForPendingQueueResource(t, schedulerQueueA, 400, 1000)
	waitForPendingQueueResource(t, schedulerQueueB, 0, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 400, 1000)
	waitForPendingAppResource(t, schedulingApp1, 200, 1000)
	waitForPendingAppResource(t, schedulingApp2, 200, 1000)

	for i := 0; i < 20; i++ {
		ms.scheduler.MultiStepSchedule(1)
		time.Sleep(100 * time.Millisecond)
	}

	ms.mockRM.waitForAllocations(t, 20, 1000)

	// Make sure pending resource updated to 100, which means
	waitForPendingQueueResource(t, schedulerQueueA, 200, 1000)
	waitForPendingQueueResource(t, schedulerQueueB, 0, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 200, 1000)
	waitForPendingAppResource(t, schedulingApp1, 100, 1000)
	waitForPendingAppResource(t, schedulingApp2, 100, 1000)

	// Both apps got 100 resources,
	assert.Assert(t, schedulingApp1.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 100)
	assert.Assert(t, schedulingApp2.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 100)
}

func TestRejectApplications(t *testing.T) {
	configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            properties:
              application.sort.policy: fair
            resources:
              guaranteed:
                memory: 100
                vcore: 10
          - name: b
            resources:
              guaranteed:
                memory: 100
                vcore: 10
`
	ms := &mockScheduler{}
	err := ms.Init(configData, false)
	if err != nil {
		t.Fatalf("RegisterResourceManager failed: %v", err)
	}

	// Register a node, and add applications
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
			{
				NodeID: "node-2:1234",
				Attributes: map[string]string{
					"si.io/hostname": "node-2",
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
		RmID: "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest failed: %v", err)
	}

	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	err = ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-reject-1": "root.non-exist-queue"}),
		RmID:            "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest 2 failed: %v", err)
	}

	ms.mockRM.waitForRejectedApplication(t, "app-reject-1", 1000)

	err = ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-added-2": "root.a"}),
		RmID:            "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest 3 failed: %v", err)
	}

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
  -
    name: default
    queues:
      -
        name: root
        submitacl: "*"
        queues:
          -
            name: parent
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
  -
    name: default
    queues:
      -
        name: root
        submitacl: "*"
        queues:
          -
            name: default
            resources:
              max:
                memory: 100
                vcore: 10
`, "root.default", 10, 1, 12},
	}

	for _, param := range parameters {
		t.Run(param.name, func(t *testing.T) {
			ms := &mockScheduler{}
			err := ms.Init(param.configData, false)

			if err != nil {
				t.Fatalf("RegisterResourceManager failed in run %s: %v", param.name, err)
			}

			// Register a node, and add applications
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
								"memory": {Value: 150},
								"vcore":  {Value: 15},
							},
						},
					},
				},
				NewApplications: newAddAppRequest(map[string]string{"app-1": param.leafQueue}),
				RmID:            "rm:123",
			})

			if err != nil {
				t.Fatalf("UpdateRequest failed in run %s: %v", param.name, err)
			}

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

			if err != nil {
				t.Fatalf("UpdateRequest 2 failed in run %s: %v", param.name, err)
			}

			schedulingQueue := ms.getSchedulingQueue(param.leafQueue)

			waitForPendingQueueResource(t, schedulingQueue, 120, 1000)

			for i := 0; i < 20; i++ {
				ms.scheduler.MultiStepSchedule(1)
				time.Sleep(100 * time.Millisecond)
			}

			// 100 memory gets allocated, 20 pending because the capacity is 100
			waitForPendingQueueResource(t, schedulingQueue, 20, 1000)
			var app1 *cache.ApplicationInfo
			app1, err = getApplicationInfoFromPartition(ms.clusterInfo.GetPartition("[rm:123]default"), "app-1")
			if err != nil || app1 == nil {
				t.Fatal("application 'app-1' not found in cache")
			}

			assert.Equal(t, len(app1.GetAllAllocations()), 10)
			assert.Assert(t, app1.GetAllocatedResource().Resources[resources.MEMORY] == 100)
			assert.Equal(t, len(ms.mockRM.getAllocations()), 10)

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

			if err != nil {
				t.Fatalf("UpdateRequest 3 failed in run %s: %v", param.name, err)
			}

			// schedule again, pending requests should be satisfied now
			for i := 0; i < 20; i++ {
				ms.scheduler.MultiStepSchedule(1)
				time.Sleep(100 * time.Millisecond)
			}

			waitForPendingQueueResource(t, schedulingQueue, 0, 1000)
			assert.Equal(t, len(ms.mockRM.getAllocations()), 2)
		})
	}
}

// Test basic interactions from rm proxy to cache and to scheduler.
func TestRMNodeActions(t *testing.T) {
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
	ms := &mockScheduler{}
	err := ms.Init(configData, false)
	if err != nil {
		t.Fatalf("RegisterResourceManager failed: %v", err)
	}

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
			{
				NodeID: "node-2:1234",
				Attributes: map[string]string{
					"si.io/hostname": "node-2",
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

	if err != nil {
		t.Fatalf("UpdateRequest failed: %v", err)
	}

	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	// verify scheduling nodes
	context := ms.scheduler.GetClusterSchedulingContext()
	waitForNewSchedulerNode(t, context, "node-1:1234", "[rm:123]default", 1000)
	waitForNewSchedulerNode(t, context, "node-2:1234", "[rm:123]default", 1000)

	// verify all nodes are schedule-able
	assert.Equal(t, ms.clusterInfo.GetPartition("[rm:123]default").GetNode("node-1:1234").IsSchedulable(), true)
	assert.Equal(t, ms.clusterInfo.GetPartition("[rm:123]default").GetNode("node-2:1234").IsSchedulable(), true)

	// send RM node action DRAIN_NODE (move to unschedulable)
	err = ms.proxy.Update(&si.UpdateRequest{
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID:     "node-1:1234",
				Action:     si.UpdateNodeInfo_DRAIN_NODE,
				Attributes: make(map[string]string),
			},
		},
		RmID: "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest 2 failed: %v", err)
	}

	err = common.WaitFor(10*time.Millisecond, 10*time.Second, func() bool {
		return !ms.clusterInfo.GetPartition("[rm:123]default").GetNode("node-1:1234").IsSchedulable() &&
			ms.clusterInfo.GetPartition("[rm:123]default").GetNode("node-2:1234").IsSchedulable()
	})

	if nil != err {
		t.Errorf("timedout waiting for node in cache: %v", err)
	}

	// send RM node action: DRAIN_TO_SCHEDULABLE (make it schedulable again)
	err = ms.proxy.Update(&si.UpdateRequest{
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID:     "node-1:1234",
				Action:     si.UpdateNodeInfo_DRAIN_TO_SCHEDULABLE,
				Attributes: make(map[string]string),
			},
		},
		RmID: "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest 3 failed: %v", err)
	}

	err = common.WaitFor(10*time.Millisecond, 10*time.Second, func() bool {
		return ms.clusterInfo.GetPartition("[rm:123]default").GetNode("node-1:1234").IsSchedulable() &&
			ms.clusterInfo.GetPartition("[rm:123]default").GetNode("node-2:1234").IsSchedulable()
	})

	if nil != err {
		t.Errorf("timedout waiting for node in cache: %v", err)
	}

	// send RM node action: DECOMMISSION (make it unschedulable and tell partition to delete)
	err = ms.proxy.Update(&si.UpdateRequest{
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID:     "node-2:1234",
				Action:     si.UpdateNodeInfo_DECOMISSION,
				Attributes: make(map[string]string),
			},
		},
		RmID: "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest 3 failed: %v", err)
	}

	// node removal can be really quick: cannot test for unschedulable state (nil pointer)
	// verify that scheduling node (node-2) was removed
	waitForRemovedSchedulerNode(t, context, "node-2:1234", "[rm:123]default", 10000)
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
          - name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
`
	ms := &mockScheduler{}
	err := ms.Init(configData, false)
	if err != nil {
		t.Fatalf("RegisterResourceManager failed: %v", err)
	}

	// Register a node, and add applications
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
			{
				NodeID: "node-2:1234",
				Attributes: map[string]string{
					"si.io/hostname": "node-2",
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
		NewApplications: newAddAppRequest(map[string]string{"app-1": "root.a", "app-2": "root.a"}),
		RmID:            "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest failed: %v", err)
	}

	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)
	ms.mockRM.waitForAcceptedApplication(t, "app-2", 1000)
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
				ApplicationID:  "app-1",
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
				ApplicationID:  "app-2",
			},
		},
		RmID: "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest 2 failed: %v", err)
	}

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")

	// Check scheduling queue a
	schedulerQueueA := ms.getSchedulingQueue("root.a")

	// Get scheduling app
	schedulingApp1 := ms.getSchedulingApplication("app-1")
	schedulingApp2 := ms.getSchedulingApplication("app-2")

	waitForPendingQueueResource(t, schedulerQueueA, 400, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 400, 1000)
	waitForPendingAppResource(t, schedulingApp1, 200, 1000)
	waitForPendingAppResource(t, schedulingApp2, 200, 1000)

	for i := 1; i < 10; i++ {
		ms.scheduler.MultiStepSchedule(1)
		ms.mockRM.waitForAllocations(t, i, 1000)
	}

	node1Alloc := ms.clusterInfo.GetPartition("[rm:123]default").GetNode("node-1:1234").GetAllocatedResource().Resources[resources.MEMORY]
	node2Alloc := ms.clusterInfo.GetPartition("[rm:123]default").GetNode("node-2:1234").GetAllocatedResource().Resources[resources.MEMORY]
	// we do not know which node was chosen so we need to check:
	// node1 == 90 && node2 == 0  || node1 == 0 && node2 == 90
	if !(node1Alloc == 90 && node2Alloc == 0) && !(node1Alloc == 0 && node2Alloc == 90) {
		t.Errorf("allocation not contained on one: node1 = %d, node2 = %d", node1Alloc, node2Alloc)
	}
}

func TestFairnessAllocationForNodes(t *testing.T) {
	const partition = "[rm:123]default"
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
`
	ms := &mockScheduler{}
	err := ms.Init(configData, false)

	if err != nil {
		t.Fatalf("RegisterResourceManager failed: %v", err)
	}

	// Register 10 nodes, and add applications
	nodes := make([]*si.NewNodeInfo, 0)
	for i := 0; i < 10; i++ {
		nodeID := "node-" + strconv.Itoa(i)
		nodes = append(nodes, &si.NewNodeInfo{
			NodeID: nodeID + ":1234",
			Attributes: map[string]string{
				"si.io/hostname":  nodeID,
				"si.io/rackname":  "rack-1",
				"si.io/partition": "default",
			},
			SchedulableResource: &si.Resource{
				Resources: map[string]*si.Quantity{
					"memory": {Value: 100},
					"vcore":  {Value: 20},
				},
			},
		})
	}

	err = ms.proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: nodes,
		NewApplications:     newAddAppRequest(map[string]string{"app-1": "root.a"}),
		RmID:                "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest failed: %v", err)
	}

	// verify app and all nodes are accepted
	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)
	for _, node := range nodes {
		ms.mockRM.waitForAcceptedNode(t, node.NodeID, 1000)
	}

	for _, node := range nodes {
		waitForNewSchedulerNode(t, ms.scheduler.GetClusterSchedulingContext(), node.NodeID, partition, 1000)
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
				ApplicationID:  "app-1",
			},
		},
		RmID: "rm:123",
	})

	if err != nil {
		t.Fatalf("UpdateRequest failed: %v", err)
	}

	schedulerQueueA := ms.getSchedulingQueue("root.a")
	schedulingApp1 := ms.getSchedulingApplication("app-1")

	waitForPendingQueueResource(t, schedulerQueueA, 200, 1000)
	waitForPendingAppResource(t, schedulingApp1, 200, 1000)

	for i := 0; i < 20; i++ {
		ms.scheduler.MultiStepSchedule(1)
	}

	// Verify all requests are satisfied
	ms.mockRM.waitForAllocations(t, 20, 1000)
	waitForPendingQueueResource(t, schedulerQueueA, 0, 1000)
	waitForPendingAppResource(t, schedulingApp1, 0, 1000)
	assert.Assert(t, schedulingApp1.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 200)

	// Verify 2 allocations for every node
	for _, node := range nodes {
		schedulingNode := ms.scheduler.GetClusterSchedulingContext().GetSchedulingNode(node.NodeID, partition)
		assert.Assert(t, schedulingNode.GetAllocatedResource().Resources[resources.MEMORY] == 20)
	}
}
