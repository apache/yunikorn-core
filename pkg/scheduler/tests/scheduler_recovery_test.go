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
		NewApplications: newAddAppRequest(map[string]string{"app-1":"root.a"}),
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
	assert.Equal(t, schedulerQueueA.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(20))
	assert.Equal(t, schedulerQueueRoot.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(20))
	assert.Equal(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(20))

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

	waitForAllocations(mockRM, 4, 3000)

	// Check pending resource, should be 200 now.
	waitForPendingResource(t, schedulerQueueA, 200, 1000)
	waitForPendingResource(t, schedulerQueueRoot, 200, 1000)
	waitForPendingResourceForApplication(t, schedulingApp, 200, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, schedulerQueueA.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120))
	assert.Equal(t, schedulerQueueRoot.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120))
	assert.Equal(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120))

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
	configs.MockSchedulerConfigByData([]byte(configData))
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
		NewApplications: newAddAppRequest(map[string]string{"app-1":"root.a"}),
		RmId: "rm:123",
	})

	if nil != err {
		t.Error(err.Error())
	}

	// waiting for recovery
	waitForAcceptedApplications(newMockRM, "app-1", 1000)
	waitForAcceptedNodes(newMockRM, "node-1:1234", 1000)
	waitForAcceptedNodes(newMockRM, "node-2:1234", 1000)

	// verify partition info
	t.Log("verifying partition info")
	partition := cache.GetPartition("[rm:123]default")
	// verify apps in this partition
	assert.Equal(t, 1, len(partition.GetApplications()))
	assert.Equal(t, "app-1", partition.GetApplications()[0].ApplicationId)
	assert.Equal(t, len(partition.GetApplications()[0].GetAllAllocations()), 4)
	assert.Equal(t, partition.GetApplications()[0].GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120))
	assert.Equal(t, partition.GetApplications()[0].GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(12))

	// verify nodes
	t.Logf("verifying recovered nodes, counts %d", partition.GetTotalNodeCount())
	assert.Equal(t, 2, partition.GetTotalNodeCount())
	node1Allocations := mockRM.nodeAllocations["node-1:1234"]
	node2Allocations := mockRM.nodeAllocations["node-2:1234"]

	t.Logf("verifying allocations on node-1, expected %d, actual %d",
		len(node1Allocations), len(partition.GetNode("node-1:1234").GetAllAllocations()))
	t.Logf("verifying allocations on node-1, expected %d, actual %d",
		len(node2Allocations), len(partition.GetNode("node-2:1234").GetAllAllocations()))

	assert.Equal(t, len(node1Allocations), len(partition.GetNode("node-1:1234").GetAllAllocations()))
	assert.Equal(t, len(node2Allocations), len(partition.GetNode("node-2:1234").GetAllAllocations()))

	node1AllocatedMemory := partition.GetNode("node-1:1234").GetAllocatedResource().Resources[resources.MEMORY]
	node2AllocatedMemory := partition.GetNode("node-2:1234").GetAllocatedResource().Resources[resources.MEMORY]
	node1AllocatedCpu := partition.GetNode("node-1:1234").GetAllocatedResource().Resources[resources.VCORE]
	node2AllocatedCpu := partition.GetNode("node-2:1234").GetAllocatedResource().Resources[resources.VCORE]
	assert.Equal(t, node1AllocatedMemory+node2AllocatedMemory, resources.Quantity(120))
	assert.Equal(t, node1AllocatedCpu+node2AllocatedCpu, resources.Quantity(12))

	// verify queues
	//  - verify root queue
	t.Log("verifying root queue")
	assert.Equal(t, partition.Root.GuaranteedResource.Resources[resources.MEMORY], resources.Quantity(100))
	assert.Equal(t, partition.Root.GuaranteedResource.Resources[resources.VCORE], resources.Quantity(10))
	assert.Equal(t, partition.Root.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120))
	assert.Equal(t, partition.Root.GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(12))
	//  - verify root.a queue
	t.Log("verifying root.a queue")
	childQueues := partition.Root.GetCopyOfChildren()
	if queueA, ok := childQueues["a"]; !ok {
		t.Fatal("root.a doesn't exist in partition")
	} else {
		assert.Equal(t, queueA.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120))
		assert.Equal(t, partition.Root.GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(12))
	}

	// verify scheduler cache
	t.Log("verifying scheduling app")
	waitForAcceptedApplications(newMockRM, "app-1", 1000)
	recoveredApp := scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", "[rm:123]default")
	assert.Assert(t, recoveredApp != nil)
	assert.Equal(t, recoveredApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120))
	assert.Equal(t, recoveredApp.ApplicationInfo.GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(12))

	// there should be no pending resources
	assert.Equal(t, recoveredApp.Requests.GetPendingResource().Resources[resources.MEMORY], resources.Quantity(0))
	assert.Equal(t, recoveredApp.Requests.GetPendingResource().Resources[resources.VCORE], resources.Quantity(0))
	for _, existingAllocation := range mockRM.Allocations {
		schedulingAllocation := recoveredApp.Requests.GetSchedulingAllocationAsk(existingAllocation.AllocationKey)
		assert.Assert(t, schedulingAllocation != nil)
	}

	t.Log("verifying scheduling queues")
	recoveredQueueRoot := scheduler.GetClusterSchedulingContext().
		GetSchedulingQueue("root", "[rm:123]default")
	recoveredQueue := scheduler.GetClusterSchedulingContext().
		GetSchedulingQueue("root.a", "[rm:123]default")
	assert.Equal(t, recoveredQueue.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120))
	assert.Equal(t, recoveredQueue.CachedQueueInfo.GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(12))
	assert.Equal(t, recoveredQueueRoot.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120))
	assert.Equal(t, recoveredQueueRoot.CachedQueueInfo.GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(12))

	// verify app state
	assert.Equal(t, recoveredApp.ApplicationInfo.GetApplicationState(), "Running")
}

// test scheduler recovery when shim doesn't report existing application
// but only include existing allocations of this app.
func TestSchedulerRecoveryWithoutAppInfo(t *testing.T) {
	serviceContext := entrypoint.StartAllServicesWithManualScheduler()
	proxy := serviceContext.RMProxy
	cache := serviceContext.Cache

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

	// Register nodes, and add apps
	// here we only report back existing allocations, without registering applications
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
				ExistingAllocations: []*si.Allocation{
					{
						AllocationKey: "allocation-key-01",
						Uuid: "UUID01",
						ApplicationId: "app-01",
						PartitionName: "default",
						QueueName: "root.a",
						NodeId: "node-1:1234",
						ResourcePerAlloc: &si.Resource{
							Resources: map[string]*si.Quantity{
								resources.MEMORY: {
									Value: 1024,
								},
								resources.VCORE: {
									Value: 1,
								},
							},
						},

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
		RmId: "rm:123",
	})

	if nil != err {
		t.Error(err.Error())
	}

	// waiting for recovery
	// node-1 should be rejected as some of allocations cannot be recovered
	waitForRejectedNodes(mockRM, "node-1:1234", 1000)
	waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

	// verify partition resources
	partition := cache.GetPartition("[rm:123]default")
	assert.Equal(t, partition.GetTotalNodeCount(), 1)
	assert.Equal(t, partition.GetTotalApplicationCount(), 0)
	assert.Equal(t, partition.GetTotalAllocationCount(), 0)
	assert.Equal(t, partition.GetNode("node-2:1234").GetAllocatedResource().Resources[resources.MEMORY],
		resources.Quantity(0))

	// register the node again, with application info attached
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
				ExistingAllocations: []*si.Allocation{
					{
						AllocationKey: "allocation-key-01",
						Uuid: "UUID01",
						ApplicationId: "app-01",
						PartitionName: "default",
						QueueName: "root.a",
						NodeId: "node-1:1234",
						ResourcePerAlloc: &si.Resource{
							Resources: map[string]*si.Quantity{
								resources.MEMORY: {
									Value: 100,
								},
								resources.VCORE: {
									Value: 1,
								},
							},
						},

					},
				},
			},
		},
		NewApplications: newAddAppRequest(map[string]string{"app-01":"root.a"}),
		RmId: "rm:123",
	})

	if nil != err {
		t.Error(err.Error())
	}

	waitForAcceptedNodes(mockRM, "node-1:1234", 1000)

	assert.Equal(t, partition.GetTotalNodeCount(), 2)
	assert.Equal(t, partition.GetTotalApplicationCount(), 1)
	assert.Equal(t, partition.GetTotalAllocationCount(), 1)
	assert.Equal(t, partition.GetNode("node-1:1234").GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(100))
	assert.Equal(t, partition.GetNode("node-1:1234").GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(1))
	assert.Equal(t, partition.GetNode("node-2:1234").GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(0))
	assert.Equal(t, partition.GetNode("node-2:1234").GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(0))

	t.Log("verifying scheduling queues")
	recoveredQueueRoot := serviceContext.Scheduler.GetClusterSchedulingContext().
		GetSchedulingQueue("root", "[rm:123]default")
	recoveredQueue := serviceContext.Scheduler.GetClusterSchedulingContext().
		GetSchedulingQueue("root.a", "[rm:123]default")
	assert.Equal(t, recoveredQueue.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(100))
	assert.Equal(t, recoveredQueue.CachedQueueInfo.GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(1))
	assert.Equal(t, recoveredQueueRoot.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(100))
	assert.Equal(t, recoveredQueueRoot.CachedQueueInfo.GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(1))
}

// test scheduler recovery that only registers nodes and apps
func TestAppRecovery(t *testing.T) {
	serviceContext := entrypoint.StartAllServicesWithManualScheduler()
	proxy := serviceContext.RMProxy

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
		NewApplications: newAddAppRequest(map[string]string{"app-1":"root.a"}),
		RmId:            "rm:123",
	})

	if nil != err {
		t.Error(err.Error())
	}

	// waiting for recovery
	waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
	waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

	app01 := serviceContext.Scheduler.GetClusterSchedulingContext().
		GetSchedulingApplication("app-1", "[rm:123]default")
	assert.Assert(t, app01 != nil)
	assert.Equal(t, app01.ApplicationInfo.ApplicationId, "app-1")
	assert.Equal(t, app01.ApplicationInfo.QueueName, "root.a")
}

// test scheduler recovery that only registers apps
func TestAppRecoveryAlone(t *testing.T) {
	serviceContext := entrypoint.StartAllServicesWithManualScheduler()
	proxy := serviceContext.RMProxy

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

	// Register apps alone
	err = proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-1":"root.a", "app-2":"root.a"}),
		RmId: "rm:123",
	})

	if nil != err {
		t.Error(err.Error())
	}

	waitForAcceptedApplications(mockRM, "app-1", 1000)
	waitForAcceptedApplications(mockRM, "app-2", 1000)

	// verify app state
	apps := serviceContext.Cache.GetPartition("[rm:123]default").GetApplications()
	var app1 *cacheInfo.ApplicationInfo
	var app2 *cacheInfo.ApplicationInfo
	for _, app := range apps {
		if app.ApplicationId == "app-1" {
			app1 = app
		}

		if app.ApplicationId == "app-2" {
			app2 = app
		}
	}

	assert.Assert(t, app1 != nil)
	assert.Assert(t, app2 != nil)
	assert.Equal(t, app1.GetApplicationState(), "Accepted")
	assert.Equal(t, app2.GetApplicationState(), "Accepted")
}