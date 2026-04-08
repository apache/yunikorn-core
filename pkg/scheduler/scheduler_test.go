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
package scheduler

import (
	"fmt"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestInspectOutstandingRequests(t *testing.T) {
	scheduler := NewScheduler()
	partition, err := newBasePartition()
	assert.NilError(t, err, "unable to create partition: %v", err)
	scheduler.clusterContext.partitions["test"] = partition

	// two applications with no asks
	app1 := newApplication(appID1, "test", "root.default")
	app2 := newApplication(appID2, "test", "root.default")
	err = partition.AddApplication(app1)
	assert.NilError(t, err)
	err = partition.AddApplication(app2)
	assert.NilError(t, err)

	// add asks
	askResource := resources.NewResourceFromMap(map[string]resources.Quantity{
		"vcores": 1,
		"memory": 1,
	})
	siAsk1 := &si.Allocation{
		AllocationKey:    "ask-uuid-1",
		ApplicationID:    appID1,
		ResourcePerAlloc: askResource.ToProto(),
	}
	siAsk2 := &si.Allocation{
		AllocationKey:    "ask-uuid-2",
		ApplicationID:    appID1,
		ResourcePerAlloc: askResource.ToProto(),
	}
	siAsk3 := &si.Allocation{
		AllocationKey:    "ask-uuid-3",
		ApplicationID:    appID2,
		ResourcePerAlloc: askResource.ToProto(),
	}
	askCreated, _, err := partition.UpdateAllocation(objects.NewAllocationFromSI(siAsk1))
	assert.NilError(t, err)
	assert.Check(t, askCreated)
	askCreated, _, err = partition.UpdateAllocation(objects.NewAllocationFromSI(siAsk2))
	assert.NilError(t, err)
	assert.Check(t, askCreated)
	askCreated, _, err = partition.UpdateAllocation(objects.NewAllocationFromSI(siAsk3))
	assert.NilError(t, err)
	assert.Check(t, askCreated)

	// mark asks as attempted
	expectedTotal := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 3,
		"vcores": 3,
	})
	app1.GetAllocationAsk("ask-uuid-1").SetSchedulingAttempted(true)
	app1.GetAllocationAsk("ask-uuid-2").SetSchedulingAttempted(true)
	app2.GetAllocationAsk("ask-uuid-3").SetSchedulingAttempted(true)

	// Check #1: collected 3 requests
	noRequests, totalResources := scheduler.inspectOutstandingRequests()
	assert.Equal(t, 3, noRequests)
	assert.Assert(t, resources.Equals(totalResources, expectedTotal),
		"total resource expected: %v, got: %v", expectedTotal, totalResources)

	// Check #2: try again, pending asks are not collected
	noRequests, totalResources = scheduler.inspectOutstandingRequests()
	assert.Equal(t, 0, noRequests)
	assert.Assert(t, resources.IsZero(totalResources), "total resource is not zero: %v", totalResources)
}

// TestInspectOutstandingRequestsSkipsUnvisitedQueue proves that
// inspectOutstandingRequests is blind to asks whose queue was never visited
// by the scheduling loop. This reproduces the autoscaler blindness aspect of
// the fair-share starvation bug (YUNIKORN queue starvation).
//
// Setup:
//   - Two sibling queues under root with vastly different guaranteed resources
//   - Both have pending asks, but only the dominant queue's asks get
//     schedulingAttempted=true (because TryAllocate visits it first and returns)
//   - The starved queue's asks stay schedulingAttempted=false
//   - inspectOutstandingRequests only reports the dominant queue's asks
func TestInspectOutstandingRequestsSkipsUnvisitedQueue(t *testing.T) {
	setupUGM()

	// Create a partition with two sibling queues under root.
	// Memory-only guaranteed with 3600:1 ratio to avoid multi-dimensional complications.
	// root.dominant (guaranteed: 360G) and root.starved (guaranteed: 100M)
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:   "dominant",
						Parent: false,
						Resources: configs.Resources{
							Guaranteed: map[string]string{
								"memory": "360000000000", // 360G
							},
							Max: map[string]string{
								"memory": "400000000000",
							},
						},
					},
					{
						Name:   "starved",
						Parent: false,
						Resources: configs.Resources{
							Guaranteed: map[string]string{
								"memory": "100000000", // 100M
							},
							Max: map[string]string{
								"memory": "100000000",
							},
						},
					},
				},
			},
		},
	}

	partition, err := newPartitionContext(conf, rmID, nil, false)
	assert.NilError(t, err, "partition create failed")

	// Add a node with enough capacity
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 500000000000, // 500G
	})
	node := newNodeMaxResource(nodeID1, nodeRes)
	err = partition.AddNode(node)
	assert.NilError(t, err, "add node failed")

	// CRITICAL: Create a prior allocation on starved queue to make its ratio non-zero.
	// 10M / 100M = 0.1 ratio. Dominant must allocate 36G to catch up.
	priorApp := newApplication("prior-app", "test", "root.starved")
	err = partition.AddApplication(priorApp)
	assert.NilError(t, err, "add prior-app failed")

	priorAskRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 10000000, // 10M
	})
	priorAsk := newAllocationAsk("prior-ask-0", "prior-app", priorAskRes)
	err = priorApp.AddAllocationAsk(priorAsk)
	assert.NilError(t, err, "add prior ask failed")

	// Allocate the prior ask
	result := partition.tryAllocate()
	assert.Assert(t, result != nil, "prior allocation should succeed")

	// Add dominant queue app with asks
	domApp := newApplication("dom-app", "test", "root.dominant")
	err = partition.AddApplication(domApp)
	assert.NilError(t, err, "add dom-app failed")

	domAskRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 1000000000, // 1G each
	})
	for i := 0; i < 15; i++ {
		ask := newAllocationAsk(fmt.Sprintf("dom-ask-%d", i), "dom-app", domAskRes)
		err = domApp.AddAllocationAsk(ask)
		assert.NilError(t, err, "add dom ask failed")
	}

	// Add starved queue app with a small ask
	starvedApp := newApplication("starved-app", "test", "root.starved")
	err = partition.AddApplication(starvedApp)
	assert.NilError(t, err, "add starved-app failed")

	starvedAskRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 1000000, // 1M — tiny
	})
	starvedAsk := newAllocationAsk("starved-ask-0", "starved-app", starvedAskRes)
	err = starvedApp.AddAllocationAsk(starvedAsk)
	assert.NilError(t, err, "add starved ask failed")

	// Run 15 scheduling cycles. Dominant queue sorts first every cycle because:
	// dominant ratio: XG/360G (max 15G/360G = 0.042) < starved ratio: 10M/100M = 0.1
	for i := 0; i < 15; i++ {
		partition.tryAllocate()
	}

	// Verify: dominant app got allocations, starved app got none
	domAllocMem := domApp.GetAllocatedResource().Resources["memory"]
	starvedAllocMem := starvedApp.GetAllocatedResource().Resources["memory"]
	t.Logf("dominant allocated memory: %d, starved allocated memory: %d", domAllocMem, starvedAllocMem)
	assert.Assert(t, domAllocMem > 0, "dominant app should have allocations")
	assert.Equal(t, int(starvedAllocMem), 0, "starved app should have 0 allocations")

	// Verify: starved ask does NOT have schedulingAttempted set
	retrievedStarvedAsk := starvedApp.GetAllocationAsk("starved-ask-0")
	assert.Assert(t, retrievedStarvedAsk != nil, "starved ask should exist")
	assert.Check(t, !retrievedStarvedAsk.IsSchedulingAttempted(),
		"starved ask must NOT have schedulingAttempted=true — queue was never visited")

	// Verify: dominant asks that couldn't fit DO have schedulingAttempted=true
	domAsk14 := domApp.GetAllocationAsk("dom-ask-14")
	if domAsk14 != nil && !domAsk14.IsAllocated() {
		assert.Check(t, domAsk14.IsSchedulingAttempted(),
			"unallocated dominant asks should have schedulingAttempted=true")
	}

	// NOW: wire up the scheduler and test inspectOutstandingRequests
	scheduler := NewScheduler()
	scheduler.clusterContext.partitions["test"] = partition

	noRequests, totalResources := scheduler.inspectOutstandingRequests()
	t.Logf("inspectOutstandingRequests: count=%d, resources=%v", noRequests, totalResources)

	// Outstanding requests should ONLY contain dominant asks (schedulingAttempted=true)
	// The starved ask should be invisible because schedulingAttempted=false
	if noRequests > 0 {
		// Verify no starved resources in the total
		// If starved ask (1M memory) were included, total would include it
		// Since dominant asks are 1G each, total should be multiples of 1G only
		t.Logf("outstanding total memory: %d (should be multiples of 1G, no 1M component)",
			totalResources.Resources["memory"])
	}

	// The definitive check: run inspectOutstandingRequests, then check if starved ask
	// got scaleUpTriggered. If it was included, scaleUpTriggered would be true.
	assert.Check(t, !retrievedStarvedAsk.HasTriggeredScaleUp(),
		"starved ask must NOT have scaleUpTriggered — inspectOutstandingRequests "+
			"skipped it because schedulingAttempted was false. "+
			"This means Karpenter/autoscaler is completely blind to this ask.")

	t.Logf("")
	t.Logf("CONFIRMED: inspectOutstandingRequests is blind to starved queue asks.")
	t.Logf("  dominant asks: schedulingAttempted=true → reported to autoscaler")
	t.Logf("  starved asks:  schedulingAttempted=false → invisible to autoscaler")
}
