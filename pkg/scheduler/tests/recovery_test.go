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

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/entrypoint"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const configData = `
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

//nolint:funlen
func TestSchedulerRecovery(t *testing.T) {
	// --------------------------------------------------
	// Phase 1) Fresh start
	// --------------------------------------------------
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Check queues of scheduler.GetClusterContext() and scheduler.
	part := ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
	assert.Assert(t, nil == part.GetTotalPartitionResource())

	// Check scheduling queue root
	rootQ := part.GetQueue("root")
	if rootQ == nil {
		t.Fatal("root queue not found on partition")
	}
	assert.Assert(t, nil == rootQ.GetMaxResource())

	// Check scheduling queue a
	queue := part.GetQueue("root.a")
	assert.Assert(t, 150 == queue.GetMaxResource().Resources[common.Memory])

	// Register nodes, and add apps
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
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	// Add two apps and wait for them to be accepted
	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID1: "root.a"}),
		RmID: "rm:123",
	})
	assert.NilError(t, err, "ApplicationRequest failed")
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)

	// Get scheduling app
	app := ms.getApplication(appID1)

	// Verify app initial state
	var app01 *objects.Application
	app01, err = getApplication(part, appID1)
	assert.NilError(t, err, "app not found on partition")
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

	assert.NilError(t, err, "UpdateRequest add resources failed")

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 10 * 2 = 20;
	waitForPendingQueueResource(t, queue, 20, 1000)
	waitForPendingQueueResource(t, rootQ, 20, 1000)
	waitForPendingAppResource(t, app, 20, 1000)
	assert.Equal(t, app01.CurrentState(), objects.Accepted.String())

	ms.scheduler.MultiStepSchedule(5)

	ms.mockRM.waitForAllocations(t, 2, 1000)

	// Make sure pending resource updated to 0
	waitForPendingQueueResource(t, queue, 0, 1000)
	waitForPendingQueueResource(t, rootQ, 0, 1000)
	waitForPendingAppResource(t, app, 0, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, queue.GetAllocatedResource().Resources[common.Memory], resources.Quantity(20))
	assert.Equal(t, rootQ.GetAllocatedResource().Resources[common.Memory], resources.Quantity(20))
	assert.Equal(t, app.GetAllocatedResource().Resources[common.Memory], resources.Quantity(20))

	// once we start to process allocation asks from this app, verify the state again
	assert.Equal(t, app01.CurrentState(), objects.Running.String())

	// Check allocated resources of nodes
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), "[rm:123]default",
		[]string{"node-1:1234", "node-2:1234"}, 20, 1000)

	// ask for two more resources
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

	assert.NilError(t, err, "UpdateRequest further alloc on existing app failed")

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 50 * 2 + 100 * 2 = 300;
	waitForPendingQueueResource(t, queue, 300, 1000)
	waitForPendingQueueResource(t, rootQ, 300, 1000)
	waitForPendingAppResource(t, app, 300, 1000)

	// Now app-1 uses 20 resource, and queue-a's max = 150, so it can get two 50 container allocated.
	ms.scheduler.MultiStepSchedule(5)

	ms.mockRM.waitForAllocations(t, 4, 3000)

	// Check pending resource, should be 200 now.
	waitForPendingQueueResource(t, queue, 200, 1000)
	waitForPendingQueueResource(t, rootQ, 200, 1000)
	waitForPendingAppResource(t, app, 200, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, queue.GetAllocatedResource().Resources[common.Memory], resources.Quantity(120))
	assert.Equal(t, rootQ.GetAllocatedResource().Resources[common.Memory], resources.Quantity(120))
	assert.Equal(t, app.GetAllocatedResource().Resources[common.Memory], resources.Quantity(120))

	// Check allocated resources of nodes
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), "[rm:123]default",
		[]string{"node-1:1234", "node-2:1234"}, 120, 1000)

	// --------------------------------------------------
	// Phase 2) Restart the scheduler, test recovery
	// --------------------------------------------------
	// keep the existing mockRM
	mockRM := ms.mockRM
	ms.serviceContext.StopAll()
	// restart
	err = ms.Init(configData, false, false)
	assert.NilError(t, err, "2nd RegisterResourceManager failed")

	// Register nodes, and add apps
	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID1: "root.a"}),
		RmID: "rm:123",
	})
	assert.NilError(t, err, "ApplicationRequest failed")
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)

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
				Action:              si.NodeInfo_CREATE,
				ExistingAllocations: mockRM.nodeAllocations["node-1:1234"],
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
				Action:              si.NodeInfo_CREATE,
				ExistingAllocations: mockRM.nodeAllocations["node-2:1234"],
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "NodeRequest nodes and app for recovery failed")
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	// verify partition info
	part = ms.scheduler.GetClusterContext().GetPartition(ms.partitionName)
	// verify apps in this partition
	assert.Equal(t, 1, len(part.GetApplications()))
	assert.Equal(t, appID1, part.GetApplications()[0].ApplicationID)
	assert.Equal(t, len(part.GetApplications()[0].GetAllAllocations()), 4)
	assert.Equal(t, part.GetApplications()[0].GetAllocatedResource().Resources[common.Memory], resources.Quantity(120))
	assert.Equal(t, part.GetApplications()[0].GetAllocatedResource().Resources[common.CPU], resources.Quantity(12))

	// verify nodes
	assert.Equal(t, 2, part.GetTotalNodeCount(), "incorrect recovered node count")
	node1Allocations := mockRM.nodeAllocations["node-1:1234"]
	node2Allocations := mockRM.nodeAllocations["node-2:1234"]

	assert.Equal(t, len(node1Allocations), len(part.GetNode("node-1:1234").GetAllAllocations()), "allocations on node-1 not as expected")
	assert.Equal(t, len(node2Allocations), len(part.GetNode("node-2:1234").GetAllAllocations()), "allocations on node-1 not as expected")

	node1AllocatedMemory := part.GetNode("node-1:1234").GetAllocatedResource().Resources[common.Memory]
	node2AllocatedMemory := part.GetNode("node-2:1234").GetAllocatedResource().Resources[common.Memory]
	node1AllocatedCPU := part.GetNode("node-1:1234").GetAllocatedResource().Resources[common.CPU]
	node2AllocatedCPU := part.GetNode("node-2:1234").GetAllocatedResource().Resources[common.CPU]
	assert.Equal(t, node1AllocatedMemory+node2AllocatedMemory, resources.Quantity(120))
	assert.Equal(t, node1AllocatedCPU+node2AllocatedCPU, resources.Quantity(12))

	// verify queues
	//  - verify root queue
	rootQ = part.GetQueue("root")
	if rootQ == nil {
		t.Fatal("root queue not found on partition")
	}
	assert.Equal(t, rootQ.GetAllocatedResource().Resources[common.Memory], resources.Quantity(120), "allocated memory on root queue not as expected")
	assert.Equal(t, rootQ.GetAllocatedResource().Resources[common.CPU], resources.Quantity(12), "allocated vcore on root queue not as expected")
	//  - verify root.a queue
	childQueues := rootQ.GetCopyOfChildren()
	queueA := childQueues["a"]
	assert.Assert(t, queueA != nil, "root.a doesn't exist in partition")
	assert.Equal(t, queueA.GetAllocatedResource().Resources[common.Memory], resources.Quantity(120), "allocated memory on root.a queue not as expected")
	assert.Equal(t, queueA.GetAllocatedResource().Resources[common.CPU], resources.Quantity(12), "allocated vcore on root.a queue not as expected")

	// verify scheduler scheduler.GetClusterContext()
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)
	recoveredApp := ms.getApplication(appID1)
	assert.Assert(t, recoveredApp != nil)
	assert.Equal(t, recoveredApp.GetAllocatedResource().Resources[common.Memory], resources.Quantity(120), "allocated memory on app not as expected")
	assert.Equal(t, recoveredApp.GetAllocatedResource().Resources[common.CPU], resources.Quantity(12), "allocated vcore on app not as expected")

	// there should be no pending resources
	assert.Equal(t, recoveredApp.GetPendingResource().Resources[common.Memory], resources.Quantity(0), "pending memory on app not as expected")
	assert.Equal(t, recoveredApp.GetPendingResource().Resources[common.CPU], resources.Quantity(0), "pending vcore on app not as expected")
	for _, existingAllocation := range mockRM.Allocations {
		schedulingAllocation := recoveredApp.GetAllocationAsk(existingAllocation.AllocationKey)
		assert.Assert(t, schedulingAllocation != nil, "recovered scheduling allocation %s not found on app", existingAllocation.AllocationKey)
	}

	// verify app state
	assert.Equal(t, recoveredApp.CurrentState(), objects.Running.String())
}

// Test case for YUNIKORN-513
func TestSchedulerRecovery2Allocations(t *testing.T) {
	// --------------------------------------------------
	// Phase 1) Fresh start
	// --------------------------------------------------
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Register node, and add app
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
		New:  newAddAppRequest(map[string]string{appID1: "root.a"}),
		RmID: "rm:123",
	})
	assert.NilError(t, err, "ApplicationRequest failed")
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)

	// Verify app initial state
	part := ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
	var app01 *objects.Application
	app01, err = getApplication(part, appID1)
	assert.NilError(t, err, "app not found on partition")
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

	assert.NilError(t, err, "AllocationRequest add resources failed")
	ms.scheduler.MultiStepSchedule(3)
	ms.mockRM.waitForAllocations(t, 2, 1000)
	// once we start to process allocation asks from this app, verify the state again
	assert.Equal(t, app01.CurrentState(), objects.Running.String())

	// --------------------------------------------------
	// Phase 2) Restart the scheduler, test recovery
	// --------------------------------------------------
	// keep the existing mockRM
	mockRM := ms.mockRM
	ms.serviceContext.StopAll()
	// restart
	err = ms.Init(configData, false, false)
	assert.NilError(t, err, "2nd RegisterResourceManager failed")

	// Register nodes, and add apps
	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID1: "root.a"}),
		RmID: "rm:123",
	})
	assert.NilError(t, err, "ApplicationRequest failed")
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)

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
				Action:              si.NodeInfo_CREATE,
				ExistingAllocations: mockRM.nodeAllocations["node-1:1234"],
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "NodeRequest nodes and app for recovery failed")
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	recoveredApp := ms.getApplication(appID1)
	// verify app state
	assert.Equal(t, recoveredApp.CurrentState(), objects.Running.String())
}

// test scheduler recovery when shim doesn't report existing application
// but only include existing allocations of this app.
func TestSchedulerRecoveryWithoutAppInfo(t *testing.T) {
	// Register RM
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Register nodes, and add apps
	// here we only report back existing allocations, without registering applications
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
				ExistingAllocations: []*si.Allocation{
					{
						AllocationKey: "allocation-key-01",
						UUID:          "UUID01",
						ApplicationID: "app-01",
						PartitionName: "default",
						NodeID:        "node-1:1234",
						ResourcePerAlloc: &si.Resource{
							Resources: map[string]*si.Quantity{
								common.Memory: {
									Value: 1024,
								},
								common.CPU: {
									Value: 1,
								},
							},
						},
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
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "NodeRequest nodes and apps failed")

	// waiting for recovery
	// node-1 should be rejected as some of allocations cannot be recovered
	ms.mockRM.waitForRejectedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	// verify partition resources
	part := ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
	assert.Equal(t, part.GetTotalNodeCount(), 1)
	assert.Equal(t, part.GetTotalAllocationCount(), 0)
	assert.Equal(t, part.GetNode("node-2:1234").GetAllocatedResource().Resources[common.Memory],
		resources.Quantity(0))

	// register the node again, with application info attached
	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{"app-01": "root.a"}),
		RmID: "rm:123",
	})
	assert.NilError(t, err, "ApplicationRequest re-register nodes and app failed")
	ms.mockRM.waitForAcceptedApplication(t, "app-01", 1000)

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
				ExistingAllocations: []*si.Allocation{
					{
						AllocationKey: "allocation-key-01",
						UUID:          "UUID01",
						ApplicationID: "app-01",
						PartitionName: "default",
						NodeID:        "node-1:1234",
						ResourcePerAlloc: &si.Resource{
							Resources: map[string]*si.Quantity{
								common.Memory: {
									Value: 100,
								},
								common.CPU: {
									Value: 1,
								},
							},
						},
					},
				},
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "NodeRequest re-register nodes and app failed")
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)

	assert.Equal(t, part.GetTotalNodeCount(), 2)
	assert.Equal(t, part.GetTotalAllocationCount(), 1)
	assert.Equal(t, part.GetNode("node-1:1234").GetAllocatedResource().Resources[common.Memory], resources.Quantity(100))
	assert.Equal(t, part.GetNode("node-1:1234").GetAllocatedResource().Resources[common.CPU], resources.Quantity(1))
	assert.Equal(t, part.GetNode("node-2:1234").GetAllocatedResource().Resources[common.Memory], resources.Quantity(0))
	assert.Equal(t, part.GetNode("node-2:1234").GetAllocatedResource().Resources[common.CPU], resources.Quantity(0))

	t.Log("verifying scheduling queues")
	rootQ := part.GetQueue("root")
	queueA := part.GetQueue("root.a")
	assert.Equal(t, queueA.GetAllocatedResource().Resources[common.Memory], resources.Quantity(100))
	assert.Equal(t, queueA.GetAllocatedResource().Resources[common.CPU], resources.Quantity(1))
	assert.Equal(t, rootQ.GetAllocatedResource().Resources[common.Memory], resources.Quantity(100))
	assert.Equal(t, rootQ.GetAllocatedResource().Resources[common.CPU], resources.Quantity(1))
}

// test scheduler recovery that only registers nodes and apps
func TestAppRecovery(t *testing.T) {
	serviceContext := entrypoint.StartAllServicesWithManualScheduler()
	proxy := serviceContext.RMProxy

	BuildInfoMap := make(map[string]string)
	BuildInfoMap["k"] = "v"

	// Register RM
	mockRM := newMockRMCallbackHandler()

	_, err := proxy.RegisterResourceManager(
		&si.RegisterResourceManagerRequest{
			RmID:        "rm:123",
			PolicyGroup: "policygroup",
			Version:     "0.0.2",
			BuildInfo:   BuildInfoMap,
			Config:      configData,
		}, mockRM)

	assert.NilError(t, err, "RegisterResourceManager failed")

	// Register nodes, and add apps
	err = proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID1: "root.a"}),
		RmID: "rm:123",
	})
	assert.NilError(t, err, "ApplicationRequest nodes and apps failed")
	mockRM.waitForAcceptedApplication(t, appID1, 1000)

	err = proxy.UpdateNode(&si.NodeRequest{
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
	assert.NilError(t, err, "NodeRequest nodes and apps failed")

	// waiting for recovery
	mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	app := serviceContext.Scheduler.GetClusterContext().GetApplication(appID1, "[rm:123]default")
	if app == nil {
		t.Fatal("application not found after recovery")
	}
	assert.Equal(t, app.ApplicationID, appID1)
	assert.Equal(t, app.GetQueuePath(), "root.a")
}

// test scheduler recovery that only registers apps
func TestAppRecoveryAlone(t *testing.T) {
	serviceContext := entrypoint.StartAllServicesWithManualScheduler()
	proxy := serviceContext.RMProxy

	BuildInfoMap := make(map[string]string)
	BuildInfoMap["k"] = "v"

	// Register RM
	mockRM := newMockRMCallbackHandler()

	_, err := proxy.RegisterResourceManager(
		&si.RegisterResourceManagerRequest{
			RmID:        "rm:123",
			PolicyGroup: "policygroup",
			Version:     "0.0.2",
			BuildInfo:   BuildInfoMap,
			Config:      configData,
		}, mockRM)

	assert.NilError(t, err, "RegisterResourceManager failed")

	// Register apps alone
	err = proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID1: "root.a", appID2: "root.a"}),
		RmID: "rm:123",
	})

	assert.NilError(t, err, "ApplicationRequest app failed")

	mockRM.waitForAcceptedApplication(t, appID1, 1000)
	mockRM.waitForAcceptedApplication(t, appID2, 1000)

	// verify app state
	apps := serviceContext.Scheduler.GetClusterContext().GetPartition("[rm:123]default").GetApplications()
	found := 0
	for _, app := range apps {
		if app.ApplicationID == appID1 || app.ApplicationID == appID2 {
			assert.Equal(t, app.CurrentState(), objects.New.String())
			found++
		}
	}

	assert.Equal(t, found, 2, "did not find expected number of apps after recovery")
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
func TestAppRecoveryPlacement(t *testing.T) {
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

	err := ms.Init(configData, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// initially there is only 1 root queue exist
	part := ms.scheduler.GetClusterContext().GetPartition(ms.partitionName)
	rootQ := part.GetQueue("root")
	assert.Equal(t, len(rootQ.GetCopyOfChildren()), 0, "unexpected child queue(s) found")

	// Register nodes, and add apps
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

	assert.NilError(t, err, "NodeRequest nodes and apps failed")

	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New: []*si.AddApplicationRequest{{
			ApplicationID: appID1,
			QueueName:     "",
			PartitionName: "",
			Tags:          map[string]string{"namespace": "app-1-namespace"},
			Ugi: &si.UserGroupInformation{
				User: "test-user",
			},
		}},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "ApplicationRequest nodes and apps failed")

	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	// now the queue should have been created under root.app-1-namespace
	assert.Equal(t, len(rootQ.GetCopyOfChildren()), 1)
	appQueue := part.GetQueue("root.app-1-namespace")
	assert.Assert(t, appQueue != nil, "application queue was not created")

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

	assert.NilError(t, err, "AllocationRequest add allocations failed")
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 10 * 2 = 20;
	app := ms.getApplication(appID1)
	waitForPendingQueueResource(t, appQueue, 20, 1000)
	waitForPendingQueueResource(t, rootQ, 20, 1000)
	waitForPendingAppResource(t, app, 20, 1000)

	ms.scheduler.MultiStepSchedule(5)

	ms.mockRM.waitForAllocations(t, 2, 1000)

	// Make sure pending resource updated to 0
	waitForPendingQueueResource(t, appQueue, 0, 1000)
	waitForPendingQueueResource(t, rootQ, 0, 1000)
	waitForPendingAppResource(t, app, 0, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, appQueue.GetAllocatedResource().Resources[common.Memory], resources.Quantity(20))
	assert.Equal(t, rootQ.GetAllocatedResource().Resources[common.Memory], resources.Quantity(20))
	assert.Equal(t, app.GetAllocatedResource().Resources[common.Memory], resources.Quantity(20))

	// once we start to process allocation asks from this app, verify the state again
	assert.Equal(t, app.CurrentState(), objects.Running.String())

	// mock existing allocations
	toRecover := make(map[string][]*si.Allocation)
	for nodeID, allocations := range ms.mockRM.nodeAllocations {
		existingAllocations := make([]*si.Allocation, 0)
		for _, alloc := range allocations {
			// except for queue name, copy from previous allocation
			// this is to simulate the case, when we have admission-controller auto-fill queue name to
			// "root.default" when there is no queue name found in the pod
			existingAllocations = append(existingAllocations, &si.Allocation{
				AllocationKey:    alloc.AllocationKey,
				AllocationTags:   alloc.AllocationTags,
				UUID:             alloc.UUID,
				ResourcePerAlloc: alloc.ResourcePerAlloc,
				Priority:         alloc.Priority,
				NodeID:           alloc.NodeID,
				ApplicationID:    alloc.ApplicationID,
				PartitionName:    alloc.PartitionName,
			})
		}
		toRecover[nodeID] = existingAllocations
	}

	// --------------------------------------------------
	// Phase 2) Restart the scheduler, test recovery
	// --------------------------------------------------
	ms.serviceContext.StopAll()

	// restart
	err = ms.Init(configData, false, false)
	assert.NilError(t, err, "2nd RegisterResourceManager failed")
	part = ms.scheduler.GetClusterContext().GetPartition(ms.partitionName)
	rootQ = part.GetQueue("root")
	assert.Equal(t, len(rootQ.GetCopyOfChildren()), 0)

	// first recover apps
	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New: []*si.AddApplicationRequest{
			{
				ApplicationID: appID1,
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

	assert.NilError(t, err, "ApplicationRequest add app failed")

	// waiting for recovery
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)

	// recover nodes
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
				Action:              si.NodeInfo_CREATE,
				ExistingAllocations: toRecover["node-1:1234"],
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
				Action:              si.NodeInfo_CREATE,
				ExistingAllocations: toRecover["node-2:1234"],
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "NodeRequest nodes failed")

	// waiting for recovery
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)
	// now the queue should have been created under root.app-1-namespace
	assert.Equal(t, len(rootQ.GetCopyOfChildren()), 1)
	appQueue = part.GetQueue("root.app-1-namespace")
	assert.Assert(t, appQueue != nil, "application queue was not created after recovery")
}
