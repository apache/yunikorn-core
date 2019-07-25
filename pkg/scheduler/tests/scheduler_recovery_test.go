/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tests

import (
	cacheInfo "github.com/cloudera/yunikorn-core/pkg/cache"
	"github.com/cloudera/yunikorn-core/pkg/common/configs"
	"github.com/cloudera/yunikorn-core/pkg/common/resources"
	"github.com/cloudera/yunikorn-core/pkg/entrypoint"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
	"gotest.tools/assert"
	"testing"
)

// Test basic interactions from rm proxy to cache and to scheduler.
func TestSchedulerRecovery(t *testing.T) {
	// --------------------------------------------------
	// Phase 1) Fresh start
	// --------------------------------------------------
	serviceContext := entrypoint.StartAllServicesWithManualScheduler()
	proxy := serviceContext.RMProxy
	cache := serviceContext.Cache
	scheduler := serviceContext.Scheduler

	// Register RM
	configData := `
partitions:
  -
    name: default
    queues:
      - name: root
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
	configs.MockSchedulerConfigByData([]byte(configData))
	mockRM := NewMockRMCallbackHandler(t)

	_, err := proxy.RegisterResourceManager(
		&si.RegisterResourceManagerRequest{
			RmId:        "rm:123",
			PolicyGroup: "policygroup",
			Version:     "0.0.2",
		}, mockRM)

	if err != nil {
		t.Error(err.Error())
	}

	// Check queues of cache and scheduler.
	partitionInfo := cache.GetPartition("[rm:123]default")
	assert.Assert(t, nil == partitionInfo.Root.MaxResource)

	// Check scheduling queue root
	schedulerQueueRoot := scheduler.GetClusterSchedulingContext().
		GetSchedulingQueue("root", "[rm:123]default")
	assert.Assert(t, nil == schedulerQueueRoot.CachedQueueInfo.MaxResource)

	// Check scheduling queue a
	schedulerQueueA := scheduler.GetClusterSchedulingContext().
		GetSchedulingQueue("root.a", "[rm:123]default")
	assert.Assert(t, 150 == schedulerQueueA.CachedQueueInfo.MaxResource.Resources[resources.MEMORY])

	// Register nodes, and add apps
	err = proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeId: "node-1:1234",
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
				NodeId: "node-2:1234",
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
		NewApplications: []*si.AddApplicationRequest{
			{
				ApplicationId:         "app-1",
				QueueName:     "root.a",
				PartitionName: "",
			},
		},
		RmId: "rm:123",
	})

	if nil != err {
		t.Error(err.Error())
	}

	waitForAcceptedApplications(mockRM, "app-1", 1000)
	waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
	waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

	// Get scheduling app
	schedulingApp := scheduler.GetClusterSchedulingContext().
		GetSchedulingApplication("app-1", "[rm:123]default")

	// Verify app initial state
	app01, err := getApplicationInfoFromPartition(partitionInfo, "app-1")
	assert.Assert(t, err == nil)
	assert.Equal(t, app01.GetApplicationState(), cacheInfo.Accepted.String())

	err = proxy.Update(&si.UpdateRequest{
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
				QueueName:      "root.a",
				ApplicationId:  "app-1",
			},
		},
		RmId: "rm:123",
	})

	if nil != err {
		t.Error(err.Error())
	}

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 10 * 2 = 20;
	waitForPendingResource(t, schedulerQueueA, 20, 1000)
	waitForPendingResource(t, schedulerQueueRoot, 20, 1000)
	waitForPendingResourceForApplication(t, schedulingApp, 20, 1000)

	scheduler.SingleStepScheduleAllocTest(16)

	waitForAllocations(mockRM, 2, 1000)

	// Make sure pending resource updated to 0
	waitForPendingResource(t, schedulerQueueA, 0, 1000)
	waitForPendingResource(t, schedulerQueueRoot, 0, 1000)
	waitForPendingResourceForApplication(t, schedulingApp, 0, 1000)

	// Check allocated resources of queues, apps
	assert.Assert(t, schedulerQueueA.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 20)
	assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 20)
	assert.Assert(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 20)

	// once we start to process allocation asks from this app, verify the state again
	assert.Equal(t, app01.GetApplicationState(), cacheInfo.Running.String())

	// Check allocated resources of nodes
	waitForNodesAllocatedResource(t, cache, "[rm:123]default",
		[]string{"node-1:1234", "node-2:1234"}, 20, 1000)

	// Ask for two more resources
	err = proxy.Update(&si.UpdateRequest{
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
				QueueName:      "root.a",
				ApplicationId:  "app-1",
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
				QueueName:      "root.a",
				ApplicationId:  "app-1",
			},
		},
		RmId: "rm:123",
	})

	if nil != err {
		t.Error(err.Error())
	}

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 50 * 2 + 100 * 2 = 300;
	waitForPendingResource(t, schedulerQueueA, 300, 1000)
	waitForPendingResource(t, schedulerQueueRoot, 300, 1000)
	waitForPendingResourceForApplication(t, schedulingApp, 300, 1000)

	// Now app-1 uses 20 resource, and queue-a's max = 150, so it can get two 50 container allocated.
	scheduler.SingleStepScheduleAllocTest(16)

	waitForAllocations(mockRM, 4, 1000)

	// Check pending resource, should be 200 now.
	waitForPendingResource(t, schedulerQueueA, 200, 1000)
	waitForPendingResource(t, schedulerQueueRoot, 200, 1000)
	waitForPendingResourceForApplication(t, schedulingApp, 200, 1000)

	// Check allocated resources of queues, apps
	assert.Assert(t, schedulerQueueA.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 120)
	assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 120)
	assert.Assert(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 120)

	// Check allocated resources of nodes
	waitForNodesAllocatedResource(t, cache, "[rm:123]default",
		[]string{"node-1:1234", "node-2:1234"}, 120, 1000)

	// --------------------------------------------------
	// Phase 2) Restart the scheduler, test recovery
	// --------------------------------------------------
	serviceContext.StopAll()
	// restart
	serviceContext = entrypoint.StartAllServicesWithManualScheduler()
	proxy = serviceContext.RMProxy
	cache = serviceContext.Cache
	scheduler = serviceContext.Scheduler

	// same RM gets register first
	newMockRM := NewMockRMCallbackHandler(t)
	_, err = proxy.RegisterResourceManager(
		&si.RegisterResourceManagerRequest{
			RmId:        "rm:123",
			PolicyGroup: "policygroup",
			Version:     "0.0.2",
		}, newMockRM)

	if err != nil {
		t.Error(err.Error())
	}

	// Register nodes, and add apps
	err = proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeId: "node-1:1234",
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
				ExistingAllocations: mockRM.nodeAllocations["node-1:1234"],
			},
			{
				NodeId: "node-2:1234",
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
				ExistingAllocations: mockRM.nodeAllocations["node-2:1234"],
			},
		},
		NewApplications: []*si.AddApplicationRequest{
			{
				ApplicationId:  "app-1",
				QueueName:      "root.a",
				PartitionName:  "",
			},
		},
		RmId: "rm:123",
	})

	if nil != err {
		t.Error(err.Error())
	}

	waitForAcceptedApplications(newMockRM, "app-1", 1000)
	waitForAcceptedNodes(newMockRM, "node-1:1234", 1000)
	waitForAcceptedNodes(newMockRM, "node-2:1234", 1000)

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 10 * 2 = 20;
	schedulerQueueA = scheduler.GetClusterSchedulingContext().
		GetSchedulingQueue("root.a", "[rm:123]default")
	schedulerQueueRoot = scheduler.GetClusterSchedulingContext().
		GetSchedulingQueue("root", "[rm:123]default")
	schedulingApp = scheduler.GetClusterSchedulingContext().
		GetSchedulingApplication("app-1", "[rm:123]default")
	waitForPendingResource(t, schedulerQueueA, 20, 1000)
	waitForPendingResource(t, schedulerQueueRoot, 20, 1000)
	waitForPendingResourceForApplication(t, schedulingApp, 20, 1000)
}