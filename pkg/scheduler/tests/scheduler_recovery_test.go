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

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/entrypoint"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func TestSchedulerRecovery(t *testing.T) {
	// --------------------------------------------------
	// Phase 1) Fresh start
	// --------------------------------------------------
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
              max:
                memory: 150
                vcore: 20
`
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Check queues of clusterInfo and scheduler.
	partitionInfo := ms.clusterInfo.GetPartition("[rm:123]default")
	assert.Assert(t, nil == partitionInfo.Root.GetMaxResource())

	// Check scheduling queue root
	schedulerQueueRoot := ms.getSchedulingQueue("root")
	assert.Assert(t, nil == schedulerQueueRoot.QueueInfo.GetMaxResource())

	// Check scheduling queue a
	schedulerQueueA := ms.getSchedulingQueue("root.a")
	assert.Assert(t, 150 == schedulerQueueA.QueueInfo.GetMaxResource().Resources[resources.MEMORY])

	// Register nodes, and add apps
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

	if nil != err {
		t.Fatalf("UpdateRequest nodes and app failed: %v", err)
	}

	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	// Get scheduling app
	schedulingApp := ms.getSchedulingApplication("app-1")

	// Verify app initial state
	app01, err := getApplicationInfoFromPartition(partitionInfo, "app-1")
	assert.Assert(t, err == nil)
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

	if nil != err {
		t.Fatalf("UpdateRequest add resuorces failed: %v", err)
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
	assert.Equal(t, schedulerQueueA.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(20))
	assert.Equal(t, schedulerQueueRoot.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(20))
	assert.Equal(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(20))

	// once we start to process allocation asks from this app, verify the state again
	assert.Equal(t, app01.GetApplicationState(), cache.Running.String())

	// Check allocated resources of nodes
	waitForNodesAllocatedResource(t, ms.clusterInfo, "[rm:123]default",
		[]string{"node-1:1234", "node-2:1234"}, 20, 1000)

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

	if nil != err {
		t.Fatalf("UpdateRequest further alloc on existing app failed: %v", err)
	}

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 50 * 2 + 100 * 2 = 300;
	waitForPendingQueueResource(t, schedulerQueueA, 300, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 300, 1000)
	waitForPendingAppResource(t, schedulingApp, 300, 1000)

	// Now app-1 uses 20 resource, and queue-a's max = 150, so it can get two 50 container allocated.
	ms.scheduler.MultiStepSchedule(16)

	ms.mockRM.waitForAllocations(t, 4, 3000)

	// Check pending resource, should be 200 now.
	waitForPendingQueueResource(t, schedulerQueueA, 200, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 200, 1000)
	waitForPendingAppResource(t, schedulingApp, 200, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, schedulerQueueA.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120))
	assert.Equal(t, schedulerQueueRoot.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120))
	assert.Equal(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120))

	// Check allocated resources of nodes
	waitForNodesAllocatedResource(t, ms.clusterInfo, "[rm:123]default",
		[]string{"node-1:1234", "node-2:1234"}, 120, 1000)

	// --------------------------------------------------
	// Phase 2) Restart the scheduler, test recovery
	// --------------------------------------------------
	// keep the existing mockRM
	mockRM := ms.mockRM
	ms.serviceContext.StopAll()
	// restart
	err = ms.Init(configData, false)
	assert.NilError(t, err, "2nd RegisterResourceManager failed")

	// Register nodes, and add apps
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
				ExistingAllocations: mockRM.nodeAllocations["node-1:1234"],
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
				ExistingAllocations: mockRM.nodeAllocations["node-2:1234"],
			},
		},
		NewApplications: newAddAppRequest(map[string]string{"app-1": "root.a"}),
		RmID:            "rm:123",
	})

	if nil != err {
		t.Fatalf("UpdateRequest nodes and app for recovery failed: %v", err)
	}

	// waiting for recovery
	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	// verify partition info
	t.Log("verifying partition info")
	partition := ms.clusterInfo.GetPartition("[rm:123]default")
	// verify apps in this partition
	assert.Equal(t, 1, len(partition.GetApplications()))
	assert.Equal(t, "app-1", partition.GetApplications()[0].ApplicationID)
	assert.Equal(t, len(partition.GetApplications()[0].GetAllAllocations()), 4)
	assert.Equal(t, partition.GetApplications()[0].GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120))
	assert.Equal(t, partition.GetApplications()[0].GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(12))

	// verify nodes
	assert.Equal(t, 2, partition.GetTotalNodeCount(), "incorrect recovered node count")
	node1Allocations := mockRM.nodeAllocations["node-1:1234"]
	node2Allocations := mockRM.nodeAllocations["node-2:1234"]

	assert.Equal(t, len(node1Allocations), len(partition.GetNode("node-1:1234").GetAllAllocations()), "allocations on node-1 not as expected")
	assert.Equal(t, len(node2Allocations), len(partition.GetNode("node-2:1234").GetAllAllocations()), "allocations on node-1 not as expected")

	node1AllocatedMemory := partition.GetNode("node-1:1234").GetAllocatedResource().Resources[resources.MEMORY]
	node2AllocatedMemory := partition.GetNode("node-2:1234").GetAllocatedResource().Resources[resources.MEMORY]
	node1AllocatedCPU := partition.GetNode("node-1:1234").GetAllocatedResource().Resources[resources.VCORE]
	node2AllocatedCPU := partition.GetNode("node-2:1234").GetAllocatedResource().Resources[resources.VCORE]
	assert.Equal(t, node1AllocatedMemory+node2AllocatedMemory, resources.Quantity(120))
	assert.Equal(t, node1AllocatedCPU+node2AllocatedCPU, resources.Quantity(12))

	// verify queues
	//  - verify root queue
	assert.Equal(t, partition.Root.GetGuaranteedResource().Resources[resources.MEMORY], resources.Quantity(100), "guaranteed memory on root queue not as expected")
	assert.Equal(t, partition.Root.GetGuaranteedResource().Resources[resources.VCORE], resources.Quantity(10), "guaranteed vcore on root queue not as expected")
	assert.Equal(t, partition.Root.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120), "allocated memory on root queue not as expected")
	assert.Equal(t, partition.Root.GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(12), "allocated vcore on root queue not as expected")
	//  - verify root.a queue
	childQueues := partition.Root.GetCopyOfChildren()
	queueA := childQueues["a"]
	assert.Assert(t, queueA != nil, "root.a doesn't exist in partition")
	assert.Equal(t, queueA.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120), "allocated memory on root.a queue not as expected")
	assert.Equal(t, queueA.GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(12), "allocated vcore on root.a queue not as expected")

	// verify scheduler clusterInfo
	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)
	recoveredApp := ms.getSchedulingApplication("app-1")
	assert.Assert(t, recoveredApp != nil)
	assert.Equal(t, recoveredApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(120), "allocated memory on app not as expected")
	assert.Equal(t, recoveredApp.ApplicationInfo.GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(12), "allocated vcore on app not as expected")

	// there should be no pending resources
	assert.Equal(t, recoveredApp.GetPendingResource().Resources[resources.MEMORY], resources.Quantity(0), "pending memory on app not as expected")
	assert.Equal(t, recoveredApp.GetPendingResource().Resources[resources.VCORE], resources.Quantity(0), "pending vcore on app not as expected")
	for _, existingAllocation := range mockRM.Allocations {
		schedulingAllocation := recoveredApp.GetSchedulingAllocationAsk(existingAllocation.AllocationKey)
		assert.Assert(t, schedulingAllocation != nil, "recovered scheduling allocation %s not found on app", existingAllocation.AllocationKey)
	}

	// verify app state
	assert.Equal(t, recoveredApp.ApplicationInfo.GetApplicationState(), "Running")
}

// test scheduler recovery when shim doesn't report existing application
// but only include existing allocations of this app.
func TestSchedulerRecoveryWithoutAppInfo(t *testing.T) {
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
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Register nodes, and add apps
	// here we only report back existing allocations, without registering applications
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
				ExistingAllocations: []*si.Allocation{
					{
						AllocationKey: "allocation-key-01",
						UUID:          "UUID01",
						ApplicationID: "app-01",
						PartitionName: "default",
						QueueName:     "root.a",
						NodeID:        "node-1:1234",
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

	if nil != err {
		t.Fatalf("UpdateRequest nodes and apps failed: %v", err)
	}

	// waiting for recovery
	// node-1 should be rejected as some of allocations cannot be recovered
	ms.mockRM.waitForRejectedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	// verify partition resources
	partition := ms.clusterInfo.GetPartition("[rm:123]default")
	assert.Equal(t, partition.GetTotalNodeCount(), 1)
	assert.Equal(t, partition.GetTotalApplicationCount(), 0)
	assert.Equal(t, partition.GetTotalAllocationCount(), 0)
	assert.Equal(t, partition.GetNode("node-2:1234").GetAllocatedResource().Resources[resources.MEMORY],
		resources.Quantity(0))

	// register the node again, with application info attached
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
				ExistingAllocations: []*si.Allocation{
					{
						AllocationKey: "allocation-key-01",
						UUID:          "UUID01",
						ApplicationID: "app-01",
						PartitionName: "default",
						QueueName:     "root.a",
						NodeID:        "node-1:1234",
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
		NewApplications: newAddAppRequest(map[string]string{"app-01": "root.a"}),
		RmID:            "rm:123",
	})

	if nil != err {
		t.Fatalf("UpdateRequest re-register nodes and app failed: %v", err)
	}

	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)

	assert.Equal(t, partition.GetTotalNodeCount(), 2)
	assert.Equal(t, partition.GetTotalApplicationCount(), 1)
	assert.Equal(t, partition.GetTotalAllocationCount(), 1)
	assert.Equal(t, partition.GetNode("node-1:1234").GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(100))
	assert.Equal(t, partition.GetNode("node-1:1234").GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(1))
	assert.Equal(t, partition.GetNode("node-2:1234").GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(0))
	assert.Equal(t, partition.GetNode("node-2:1234").GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(0))

	t.Log("verifying scheduling queues")
	recoveredQueueRoot := ms.getSchedulingQueue("root")
	recoveredQueue := ms.getSchedulingQueue("root.a")
	assert.Equal(t, recoveredQueue.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(100))
	assert.Equal(t, recoveredQueue.QueueInfo.GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(1))
	assert.Equal(t, recoveredQueueRoot.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(100))
	assert.Equal(t, recoveredQueueRoot.QueueInfo.GetAllocatedResource().Resources[resources.VCORE], resources.Quantity(1))
}

// test scheduler recovery that only registers nodes and apps
func TestAppRecovery(t *testing.T) {
	serviceContext := entrypoint.StartAllServicesWithManualScheduler()
	proxy := serviceContext.RMProxy

	// Register RM
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
              max:
                memory: 150
                vcore: 20
`
	configs.MockSchedulerConfigByData([]byte(configData))
	mockRM := NewMockRMCallbackHandler()

	_, err := proxy.RegisterResourceManager(
		&si.RegisterResourceManagerRequest{
			RmID:        "rm:123",
			PolicyGroup: "policygroup",
			Version:     "0.0.2",
		}, mockRM)

	assert.NilError(t, err, "RegisterResourceManager failed")

	// Register nodes, and add apps
	err = proxy.Update(&si.UpdateRequest{
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

	if nil != err {
		t.Fatalf("UpdateRequest nodes and apps failed: %v", err)
	}

	// waiting for recovery
	mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	app01 := serviceContext.Scheduler.GetClusterSchedulingContext().
		GetSchedulingApplication("app-1", "[rm:123]default")
	assert.Assert(t, app01 != nil)
	assert.Equal(t, app01.ApplicationInfo.ApplicationID, "app-1")
	assert.Equal(t, app01.ApplicationInfo.QueueName, "root.a")
}

// test scheduler recovery that only registers apps
func TestAppRecoveryAlone(t *testing.T) {
	serviceContext := entrypoint.StartAllServicesWithManualScheduler()
	proxy := serviceContext.RMProxy

	// Register RM
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
              max:
                memory: 150
                vcore: 20
`
	configs.MockSchedulerConfigByData([]byte(configData))
	mockRM := NewMockRMCallbackHandler()

	_, err := proxy.RegisterResourceManager(
		&si.RegisterResourceManagerRequest{
			RmID:        "rm:123",
			PolicyGroup: "policygroup",
			Version:     "0.0.2",
		}, mockRM)

	assert.NilError(t, err, "RegisterResourceManager failed")

	// Register apps alone
	err = proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-1": "root.a", "app-2": "root.a"}),
		RmID:            "rm:123",
	})

	if nil != err {
		t.Fatalf("UpdateRequest app failed: %v", err)
	}

	mockRM.waitForAcceptedApplication(t, "app-1", 1000)
	mockRM.waitForAcceptedApplication(t, "app-2", 1000)

	// verify app state
	apps := serviceContext.Cache.GetPartition("[rm:123]default").GetApplications()
	var app1 *cache.ApplicationInfo
	var app2 *cache.ApplicationInfo
	for _, app := range apps {
		if app.ApplicationID == "app-1" {
			app1 = app
		}

		if app.ApplicationID == "app-2" {
			app2 = app
		}
	}

	assert.Assert(t, app1 != nil)
	assert.Assert(t, app2 != nil)
	assert.Equal(t, app1.GetApplicationState(), "Accepted")
	assert.Equal(t, app2.GetApplicationState(), "Accepted")
}

// this case cover the scenario when we have placement rule enabled,
// we do auto queue mapping for incoming applications.
// here we enable auto queue mapping using tag-rule, which maps app to
// a queue with name same as the namespace under root.
// when new allocation requests are coming with queue name: "root.default",
// the app will still be mapped to "root.pod-namespace". this is fine for
// new allocations. But during the recovery, when we recover existing
// allocations on node, we need to ensure the placement rule is still
// enforced.
func TestSchedulerRecoveryWhenPlacementRulesApplied(t *testing.T) {
	// Register RM
	configData := `
partitions:
  - name: default
    placementrules:
      - name: tag
        value: namespace
        create: true
    queues:
      - name: root
        submitacl: "*"
`
	// --------------------------------------------------
	// Phase 1) Fresh start
	// --------------------------------------------------
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// initially there is only 1 root queue exist
	schedulerQueueRoot := ms.getSchedulingQueue("root")
	assert.Equal(t, len(schedulerQueueRoot.GetCopyOfChildren()), 0)

	// Register nodes, and add apps
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
		NewApplications: []*si.AddApplicationRequest{{
			ApplicationID: "app-1",
			QueueName:     "",
			PartitionName: "",
			Tags:          map[string]string{"namespace": "app-1-namespace"},
			Ugi: &si.UserGroupInformation{
				User: "test-user",
			},
		}},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest nodes and apps failed")

	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	// now the queue should have been created under root.app-1-namespace
	assert.Equal(t, len(schedulerQueueRoot.GetCopyOfChildren()), 1)
	appQueue := ms.getSchedulingQueue("root.app-1-namespace")
	assert.Assert(t, appQueue != nil)

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

	assert.NilError(t, err, "UpdateRequest add allocations failed")

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 10 * 2 = 20;
	schedulingApp := ms.getSchedulingApplication("app-1")
	waitForPendingQueueResource(t, appQueue, 20, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 20, 1000)
	waitForPendingAppResource(t, schedulingApp, 20, 1000)

	ms.scheduler.MultiStepSchedule(16)

	ms.mockRM.waitForAllocations(t, 2, 1000)

	// Make sure pending resource updated to 0
	waitForPendingQueueResource(t, appQueue, 0, 1000)
	waitForPendingQueueResource(t, schedulerQueueRoot, 0, 1000)
	waitForPendingAppResource(t, schedulingApp, 0, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, appQueue.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(20))
	assert.Equal(t, schedulerQueueRoot.QueueInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(20))
	assert.Equal(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY], resources.Quantity(20))

	// once we start to process allocation asks from this app, verify the state again
	assert.Equal(t, schedulingApp.ApplicationInfo.GetApplicationState(), cache.Running.String())

	// --------------------------------------------------
	// Phase 2) Restart the scheduler, test recovery
	// --------------------------------------------------
	ms.serviceContext.StopAll()
	// keep the old mockRM
	mockRM := ms.mockRM
	// restart
	err = ms.Init(configData, false)
	assert.NilError(t, err, "2nd RegisterResourceManager failed")

	// first recover apps
	err = ms.proxy.Update(&si.UpdateRequest{
		NewApplications: []*si.AddApplicationRequest{
			{
				ApplicationID: "app-1",
				QueueName:     "",
				PartitionName: "",
				Tags:          map[string]string{"namespace": "app-1-namespace"},
				Ugi: &si.UserGroupInformation{
					User: "test-user",
				},
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest add app failed")

	// waiting for recovery
	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)

	// mock existing allocations
	recoveringAllocations := make(map[string][]*si.Allocation)
	for nodeID, allocations := range mockRM.nodeAllocations {
		existingAllocations := make([]*si.Allocation, 0)
		for _, previousAllocation := range allocations {
			// except for queue name, copy from previous allocation
			// this is to simulate the case, when we have admission-controller auto-fill queue name to
			// "root.default" when there is no queue name found in the pod
			existingAllocations = append(existingAllocations, &si.Allocation{
				AllocationKey:    previousAllocation.AllocationKey,
				AllocationTags:   previousAllocation.AllocationTags,
				UUID:             previousAllocation.UUID,
				ResourcePerAlloc: previousAllocation.ResourcePerAlloc,
				Priority:         previousAllocation.Priority,
				QueueName:        "root.default",
				NodeID:           previousAllocation.NodeID,
				ApplicationID:    previousAllocation.ApplicationID,
				PartitionName:    previousAllocation.PartitionName,
			})
		}
		recoveringAllocations[nodeID] = existingAllocations
	}

	// recover nodes
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
				ExistingAllocations: recoveringAllocations["node-1:1234"],
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
				ExistingAllocations: recoveringAllocations["node-2:1234"],
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest nodes failed")

	// waiting for recovery
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)
}
