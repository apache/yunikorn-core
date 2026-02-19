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
	"strings"
	"testing"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/scheduler"
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

func TestSchedulerRecovery(t *testing.T) {
	// --------------------------------------------------
	// Phase 1) Fresh start
	// --------------------------------------------------
	ms := &mockScheduler{}
	defer ms.Stop()

	_, rootQ, queueA := doRecoverySetup(t, configData, ms, true, false, []string{"node-1:1234", "node-2:1234"}, true, []string{appID1}, nil)
	assert.Equal(t, resources.Quantity(150), queueA.GetMaxResource().Resources[common.Memory])

	// Get scheduling app
	app := ms.getApplication(appID1)

	err := ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			createAllocation("alloc-0", "", appID1, 10, 1, "", false),
			createAllocation("alloc-1", "", appID1, 10, 1, "", false),
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest add resources failed")

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 10 * 2 = 20;
	waitForQueuesPendingResource(t, []*objects.Queue{queueA, rootQ}, 0, 1000)
	waitForPendingAppResource(t, app, 20, 1000)

	assert.Equal(t, app.CurrentState(), objects.Accepted.String())

	ms.scheduler.MultiStepSchedule(5)

	ms.mockRM.waitForAllocations(t, 2, 1000)

	// Make sure pending resource updated to 0
	waitForQueuesPendingResource(t, []*objects.Queue{queueA, rootQ}, 0, 1000)
	waitForPendingAppResource(t, app, 0, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, queueA.GetAllocatedResource().Resources[common.Memory], resources.Quantity(20))
	assert.Equal(t, rootQ.GetAllocatedResource().Resources[common.Memory], resources.Quantity(20))
	assert.Equal(t, app.GetAllocatedResource().Resources[common.Memory], resources.Quantity(20))

	// once we start to process allocation asks from this app, verify the state again
	assert.Equal(t, app.CurrentState(), objects.Running.String())

	// Check allocated resources of nodes
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), "[rm:123]default",
		[]string{"node-1:1234", "node-2:1234"}, 20, 1000)

	// ask for 4 more allocations
	asks := make([]*si.Allocation, 4)
	mem := [4]int64{50, 100, 50, 100}
	for i := 0; i < 4; i++ {
		asks[i] = createAllocation(fmt.Sprintf("alloc-%d", i+2), "", appID1, int(mem[i]), 5, "", false)
	}
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: asks,
		RmID:        "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest further alloc on existing app failed")

	// Wait pending resource of queue a and scheduler queue
	// Both pending memory = 50 * 2 + 100 * 2 = 300;
	waitForQueuesPendingResource(t, []*objects.Queue{queueA, rootQ}, 300, 1000)
	waitForPendingAppResource(t, app, 300, 1000)

	// Now app-1 uses 20 resource, and queue-a's max = 150, so it can get two 50 container allocated.
	ms.scheduler.MultiStepSchedule(5)

	ms.mockRM.waitForAllocations(t, 4, 3000)

	// Check pending resource, should be 200 now.
	waitForQueuesPendingResource(t, []*objects.Queue{queueA, rootQ}, 200, 1000)
	waitForPendingAppResource(t, app, 200, 1000)

	// Check allocated resources of queues, apps
	assert.Equal(t, queueA.GetAllocatedResource().Resources[common.Memory], resources.Quantity(120))
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

	part, rootQ, queueA := doRecoverySetup(t, configData, ms, true, false, []string{"node-1:1234", "node-2:1234"}, true, []string{appID1}, nil)

	// add allocs to partition
	node1Allocations := mockRM.nodeAllocations["node-1:1234"]
	err = registerAllocations(part, node1Allocations)
	assert.NilError(t, err)
	node2Allocations := mockRM.nodeAllocations["node-2:1234"]
	err = registerAllocations(part, node2Allocations)
	assert.NilError(t, err)

	// verify apps in this partition
	assert.Equal(t, 1, len(part.GetApplications()))
	assert.Equal(t, appID1, part.GetApplications()[0].ApplicationID)
	assert.Equal(t, len(part.GetApplications()[0].GetAllAllocations()), 4)
	assert.Equal(t, part.GetApplications()[0].GetAllocatedResource().Resources[common.Memory], resources.Quantity(120))
	assert.Equal(t, part.GetApplications()[0].GetAllocatedResource().Resources[common.CPU], resources.Quantity(12))

	// verify nodes
	assert.Equal(t, 2, part.GetTotalNodeCount(), "incorrect recovered node count")

	assert.Equal(t, len(node1Allocations), len(part.GetNode("node-1:1234").GetYunikornAllocations()), "allocations on node-1 not as expected")
	assert.Equal(t, len(node2Allocations), len(part.GetNode("node-2:1234").GetYunikornAllocations()), "allocations on node-1 not as expected")

	node1Allocated := part.GetNode("node-1:1234").GetAllocatedResource()
	node2Allocated := part.GetNode("node-2:1234").GetAllocatedResource()
	assert.Equal(t, node1Allocated.Resources[common.Memory]+node2Allocated.Resources[common.Memory], resources.Quantity(120))
	assert.Equal(t, node1Allocated.Resources[common.CPU]+node2Allocated.Resources[common.CPU], resources.Quantity(12))

	// verify queues
	//  - verify root queue
	if rootQ == nil {
		t.Fatal("root queue not found on partition")
	}
	assert.Equal(t, rootQ.GetAllocatedResource().Resources[common.Memory], resources.Quantity(120), "allocated memory on root queue not as expected")
	assert.Equal(t, rootQ.GetAllocatedResource().Resources[common.CPU], resources.Quantity(12), "allocated vcore on root queue not as expected")
	//  - verify root.a queue
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
	doRecoverySetup(t, configData, ms, true, false, []string{"node-1:1234"}, true, []string{appID1}, nil)

	// Verify app initial state
	part := ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
	var app01 *objects.Application
	app01, err := getApplication(part, appID1)
	assert.NilError(t, err, "app not found on partition")
	assert.Equal(t, app01.CurrentState(), objects.New.String())

	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			createAllocation("alloc-1", "", appID1, 10, 1, "", false),
			createAllocation("alloc-2", "", appID1, 10, 1, "", false),
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
	doRecoverySetup(t, configData, ms, true, false, []string{"node-1:1234"}, true, []string{appID1}, nil)
	allocs := mockRM.nodeAllocations["node-1:1234"]
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{Allocations: allocs, RmID: "rm:123"})
	assert.NilError(t, err, "failed to update allocations")
	ms.mockRM.waitForAllocations(t, len(allocs), 1000)
	recoveredApp := ms.getApplication(appID1)
	// verify app state
	assert.Equal(t, recoveredApp.CurrentState(), objects.Running.String())
}

// TestSchedulerRecoveryWithoutAppInfo test scheduler recovery when shim doesn't report existing application
// but only include existing allocations of this app.
func TestSchedulerRecoveryWithoutAppInfo(t *testing.T) {
	// Register RM
	ms := &mockScheduler{}
	defer ms.Stop()

	part, rootQ, queueA := doRecoverySetup(t, configData, ms, true, false, []string{"node-1:1234", "node-2:1234"}, false, []string{appID1}, nil)
	assert.Equal(t, resources.Quantity(150), queueA.GetMaxResource().Resources[common.Memory])

	err := ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			createAllocation("allocation-key-01", "node-1:1234", appID1, 1024, 1, "", false),
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err)

	// verify partition resources
	assert.Equal(t, part.GetTotalNodeCount(), 2)
	assert.Equal(t, part.GetTotalAllocationCount(), 0)
	assert.Equal(t, part.GetNode("node-2:1234").GetAllocatedResource().Resources[common.Memory],
		resources.Quantity(0))

	doRecoverySetup(t, configData, ms, false, false, []string{"node-1:1234"}, true, []string{appID1}, nil)
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			createAllocation("allocation-key-01", "node-1:1234", appID1, 100, 1, "", false),
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err)
	ms.mockRM.waitForAllocations(t, 1, 1000)

	assert.Equal(t, part.GetTotalNodeCount(), 2)
	assert.Equal(t, part.GetTotalAllocationCount(), 1)
	assert.Equal(t, part.GetNode("node-1:1234").GetAllocatedResource().Resources[common.Memory], resources.Quantity(100))
	assert.Equal(t, part.GetNode("node-1:1234").GetAllocatedResource().Resources[common.CPU], resources.Quantity(1))
	assert.Equal(t, part.GetNode("node-2:1234").GetAllocatedResource().Resources[common.Memory], resources.Quantity(0))
	assert.Equal(t, part.GetNode("node-2:1234").GetAllocatedResource().Resources[common.CPU], resources.Quantity(0))

	t.Log("verifying scheduling queues")
	assert.Equal(t, queueA.GetAllocatedResource().Resources[common.Memory], resources.Quantity(100))
	assert.Equal(t, queueA.GetAllocatedResource().Resources[common.CPU], resources.Quantity(1))
	assert.Equal(t, rootQ.GetAllocatedResource().Resources[common.Memory], resources.Quantity(100))
	assert.Equal(t, rootQ.GetAllocatedResource().Resources[common.CPU], resources.Quantity(1))
}

// test scheduler recovery that only registers nodes and apps
func TestAppRecovery(t *testing.T) {
	ms := &mockScheduler{}
	defer ms.Stop()

	part, _, _ := doRecoverySetup(t, configData, ms, true, false, []string{"node-1:1234", "node-2:1234"}, true, []string{appID1}, nil)

	app := part.GetApplication(appID1)
	assert.Assert(t, app != nil, "application not found after recovery")
	assert.Equal(t, app.ApplicationID, appID1)
	assert.Equal(t, app.GetQueuePath(), "root.a")
}

// test scheduler recovery that only registers apps
func TestAppRecoveryAlone(t *testing.T) {
	ms := &mockScheduler{}
	defer ms.Stop()
	part, _, _ := doRecoverySetup(t, configData, ms, true, false, []string{"node-1:1234", "node-2:1234"}, true, []string{appID1, appID2}, nil)

	// verify app state
	apps := part.GetApplications()
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

	part, rootQ, _ := doRecoverySetup(t, configData, ms, true, false, []string{"node-1:1234", "node-2:1234"}, true, []string{appID1}, map[string]string{"namespace": "app-1-namespace"})

	// now the queue should have been created under root.app-1-namespace
	assert.Equal(t, len(rootQ.GetCopyOfChildren()), 1)
	appQueue := part.GetQueue("root.app-1-namespace")
	assert.Assert(t, appQueue != nil, "application queue was not created")

	err := ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			createAllocation("alloc-1", "", appID1, 10, 1, "", false),
			createAllocation("alloc-2", "", appID1, 10, 1, "", false),
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
	part, rootQ, _ = doRecoverySetup(t, configData, ms, true, false, []string{"node-1:1234", "node-2:1234"}, true, []string{appID1}, map[string]string{"namespace": "app-1-namespace"})

	err = registerAllocations(part, toRecover["node-1:1234"])
	assert.NilError(t, err)
	err = registerAllocations(part, toRecover["node-2:1234"])
	assert.NilError(t, err)

	// now the queue should have been created under root.app-1-namespace
	assert.Equal(t, len(rootQ.GetCopyOfChildren()), 1)
	appQueue = part.GetQueue("root.app-1-namespace")
	assert.Assert(t, appQueue != nil, "application queue was not created after recovery")
}

func TestPlaceholderRecovery(t *testing.T) {
	// create an existing allocation
	existingAllocations := make([]*si.Allocation, 1)
	existingAllocations[0] = createAllocation("ph-alloc-1", "node-1:1234", appID1, 10, 1, "tg-1", true)

	config := `partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a`
	ms := &mockScheduler{}
	defer ms.Stop()

	doRecoverySetup(t, config, ms, true, true, []string{"node-1:1234"}, true, []string{appID1}, nil)

	// Add existing allocations
	err := ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: existingAllocations,
		RmID:        "rm:123",
	})
	assert.NilError(t, err, "AllocationRequest failed for existing allocations")
	ms.mockRM.waitForAllocations(t, len(existingAllocations), 1000)

	// Add a new placeholder ask with a different task group
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			createAllocation("ph-alloc-2", "", appID1, 10, 1, "tg-2", true),
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "AllocationRequest failed for placeholder ask")
	ms.mockRM.waitForAllocations(t, len(existingAllocations)+1, 1000)

	// Add two real asks
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			createAllocation("real-alloc-1", "", appID1, 10, 1, "tg-1", false),
			createAllocation("real-alloc-2", "", appID1, 10, 1, "tg-2", false),
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "AllocationRequest failed for real asks")
	ms.mockRM.waitForReleasedPlaceholders(t, 2, 1000)

	// remove placeholder allocations
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: []*si.AllocationRelease{
				createAllocationRelease(appID1, "default", "ph-alloc-1", si.TerminationType_PLACEHOLDER_REPLACED),
				createAllocationRelease(appID1, "default", "ph-alloc-2", si.TerminationType_PLACEHOLDER_REPLACED),
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "AllocationReleasesRequest failed for placeholders")

	// remove real allocations
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: []*si.AllocationRelease{
				createAllocationRelease(appID1, "default", "real-alloc-1", si.TerminationType_STOPPED_BY_RM),
				createAllocationRelease(appID1, "default", "real-alloc-2", si.TerminationType_STOPPED_BY_RM),
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "AllocationReleasesRequest failed for real allocations")

	ms.mockRM.waitForApplicationState(t, appID1, "Completing", 1000)
}

// TestSchedulerRecoveryQuotaPreemption Test scheduler recovery with quota preemption when shim doesn't report existing application
// but only include existing allocations of this app.
func TestSchedulerRecoveryQuotaPreemption(t *testing.T) {
	config := `
partitions:
  - name: default
    preemption:
      ENABLED_STR 
    queues:
      - name: root
        properties:
          PROPERTIES_PARENT_STR
        submitacl: "*"
        queues:
          - name: a
            properties:
              PROPERTIES_STR
            resources:
              guaranteed:
                memory: 5
                vcore: 2
              max:
                memory: 5
                vcore: 2
`
	configPreemptionDefault := strings.ReplaceAll(config, "ENABLED_STR", "")
	configPreemptionDefault = strings.ReplaceAll(configPreemptionDefault, "PROPERTIES_PARENT_STR", "")
	configPreemptionDefault = strings.ReplaceAll(configPreemptionDefault, "PROPERTIES_STR", configs.QuotaPreemptionDelay+": 15m")

	configPreemptionDisabled := strings.ReplaceAll(config, "ENABLED_STR", "quotapreemptionenabled: false")
	configPreemptionDisabled = strings.ReplaceAll(configPreemptionDisabled, "PROPERTIES_PARENT_STR", "")
	configPreemptionDisabled = strings.ReplaceAll(configPreemptionDisabled, "PROPERTIES_STR", configs.QuotaPreemptionDelay+": 15m")
	configPreemptionEnabled := strings.ReplaceAll(config, "ENABLED_STR", "quotapreemptionenabled: true")
	configPreemptionEnabled = strings.ReplaceAll(configPreemptionEnabled, "PROPERTIES_PARENT_STR", "")
	configPreemptionEnabled = strings.ReplaceAll(configPreemptionEnabled, "PROPERTIES_STR", "")
	configPreemptionEnabledAndDelaySet := strings.ReplaceAll(config, "ENABLED_STR", "quotapreemptionenabled: true")
	configPreemptionEnabledAndDelaySet = strings.ReplaceAll(configPreemptionEnabledAndDelaySet, "PROPERTIES_PARENT_STR", "")
	configPreemptionEnabledAndDelaySet = strings.ReplaceAll(configPreemptionEnabledAndDelaySet, "PROPERTIES_STR", configs.QuotaPreemptionDelay+": 15m")
	configPreemptionEnabledAndDelaySetAtParent := strings.ReplaceAll(config, "ENABLED_STR", "quotapreemptionenabled: true")
	configPreemptionEnabledAndDelaySetAtParent = strings.ReplaceAll(configPreemptionEnabledAndDelaySetAtParent, "PROPERTIES_PARENT_STR", configs.QuotaPreemptionDelay+": 15m")
	configPreemptionEnabledAndDelaySetAtParent = strings.ReplaceAll(configPreemptionEnabledAndDelaySetAtParent, "PROPERTIES_STR", "")

	tests := []struct {
		name             string
		config           string
		allocated        *resources.Resource
		addedAllocations int
	}{
		{"preemption not configured explicitly at partition level", configPreemptionDefault, resources.NewResourceFromMap(map[string]resources.Quantity{common.Memory: 6}), 1},
		{"preemption disabled at partition level", configPreemptionDisabled, resources.NewResourceFromMap(map[string]resources.Quantity{common.Memory: 6}), 1},
		{"preemption enabled at partition level, but delay not set at queue level", configPreemptionEnabled, resources.NewResourceFromMap(map[string]resources.Quantity{common.Memory: 6}), 1},
		{"preemption enabled, delay set but usage is lower than max resources", configPreemptionEnabledAndDelaySet, resources.NewResourceFromMap(map[string]resources.Quantity{common.Memory: 4}), 1},
		{"preemption enabled, delay set, usage is higher than max resources", configPreemptionEnabledAndDelaySet, resources.NewResourceFromMap(map[string]resources.Quantity{common.Memory: 6}), 0},
		{"preemption enabled, delay set at parent queue but inherited and usage is lower than max resources in leaf queue.", configPreemptionEnabledAndDelaySetAtParent, resources.NewResourceFromMap(map[string]resources.Quantity{common.Memory: 6}), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Register RM
			ms := &mockScheduler{}
			defer ms.Stop()

			part, _, queueA := doRecoverySetup(t, tt.config, ms, true, false, []string{"node-1:1234", "node-2:1234"}, true, []string{appID1}, nil)
			assert.Equal(t, resources.Quantity(5), queueA.GetMaxResource().Resources[common.Memory])

			err := ms.proxy.UpdateAllocation(&si.AllocationRequest{
				Allocations: []*si.Allocation{
					createAllocation("allocation-key-01", "node-1:1234", appID1, 1024, 1, "", false),
				},
				RmID: "rm:123",
			})

			assert.NilError(t, err)

			// verify partition resources
			assert.Equal(t, part.GetTotalNodeCount(), 2)
			assert.Equal(t, part.GetTotalAllocationCount(), 0)
			assert.Equal(t, part.GetNode("node-2:1234").GetAllocatedResource().Resources[common.Memory],
				resources.Quantity(0))

			// register the node again, with application info attached
			err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
				New:  newAddAppRequest(map[string]string{appID1: "root.a"}),
				RmID: "rm:123",
			})
			assert.NilError(t, err, "ApplicationRequest re-register nodes and app failed")
			ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)

			err = ms.proxy.UpdateNode(&si.NodeRequest{
				Nodes: []*si.NodeInfo{
					createNodeInfo("node-1:1234"),
				},
				RmID: "rm:123",
			})
			assert.NilError(t, err, "NodeRequest re-register nodes and app failed")
			ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
			// Set allocated resource to exceed max quota
			queueA.IncAllocatedResource(tt.allocated)
			err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
				Allocations: []*si.Allocation{
					createAllocation("allocation-key-01", "node-1:1234", appID1, 1, 4, "", false),
				},
				RmID: "rm:123",
			})
			assert.NilError(t, err)
			ms.mockRM.waitForAllocations(t, tt.addedAllocations, 1000)
		})
	}
}

func registerAllocations(partition *scheduler.PartitionContext, allocs []*si.Allocation) error {
	for _, alloc := range allocs {
		_, allocCreated, err := partition.UpdateAllocation(objects.NewAllocationFromSI(alloc))
		if err != nil {
			return err
		}
		if !allocCreated {
			return fmt.Errorf("no alloc created")
		}
	}
	return nil
}

func doRecoverySetup(t *testing.T, config string, ms *mockScheduler, init bool, autoSchedule bool, nodes []string, addApp bool, apps []string, tags map[string]string) (*scheduler.PartitionContext, *objects.Queue, *objects.Queue) {
	var part *scheduler.PartitionContext
	var rootQ *objects.Queue
	var queue *objects.Queue
	if init {
		err := ms.Init(config, autoSchedule, false)
		assert.NilError(t, err, "RegisterResourceManager failed")

		// Check queues of scheduler.GetClusterContext() and scheduler.
		part = ms.scheduler.GetClusterContext().GetPartition("[rm:123]default")
		assert.Assert(t, nil == part.GetTotalPartitionResource())

		// Check scheduling queue root
		rootQ = part.GetQueue("root")
		if rootQ == nil {
			t.Fatal("root queue not found on partition")
		}
		assert.Assert(t, nil == rootQ.GetMaxResource())

		if tags != nil {
			assert.Equal(t, len(rootQ.GetCopyOfChildren()), 0, "unexpected child queue(s) found")
		}
		// Check scheduling queue a
		queue = part.GetQueue("root.a")
	}

	var nodesArr []*si.NodeInfo
	for _, node := range nodes {
		nodesArr = append(nodesArr, createNodeInfo(node))
	}

	// Register node, and add app
	err := ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: nodesArr,
		RmID:  "rm:123",
	})
	assert.NilError(t, err, "NodeRequest failed")
	for _, node := range nodesArr {
		ms.mockRM.waitForAcceptedNode(t, node.NodeID, 1000)
	}

	if addApp {
		appsMap := make(map[string]string)
		for _, app := range apps {
			appsMap[app] = "root.a"
		}
		err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
			New:  newAddAppRequestWithTags(appsMap, tags),
			RmID: "rm:123",
		})
		assert.NilError(t, err, "ApplicationRequest failed")
		for _, app := range apps {
			ms.mockRM.waitForAcceptedApplication(t, app, 1000)
			assert.Equal(t, ms.getApplication(app).CurrentState(), objects.New.String())
		}
	}
	return part, rootQ, queue
}

func createNodeInfo(node string) *si.NodeInfo {
	return &si.NodeInfo{
		NodeID:     node,
		Attributes: map[string]string{},
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{
				"memory": {Value: 100},
				"vcore":  {Value: 20},
			},
		},
		Action: si.NodeInfo_CREATE,
	}
}

func createAllocation(name string, nodeId string, app string, mem int, cpu int, tg string, ph bool) *si.Allocation {
	return &si.Allocation{
		AllocationKey: name,
		ApplicationID: app,
		PartitionName: "default",
		NodeID:        nodeId,
		ResourcePerAlloc: &si.Resource{
			Resources: map[string]*si.Quantity{
				common.Memory: {
					Value: int64(mem),
				},
				common.CPU: {
					Value: int64(cpu),
				},
			},
		},
		TaskGroupName: tg,
		Placeholder:   ph,
	}
}

func createAllocationRelease(app string, part string, allocKey string, terminationType si.TerminationType) *si.AllocationRelease {
	return &si.AllocationRelease{
		ApplicationID:   app,
		PartitionName:   part,
		AllocationKey:   allocKey,
		TerminationType: terminationType,
	}
}
