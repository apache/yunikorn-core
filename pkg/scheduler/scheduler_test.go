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
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
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

// TestTriggerQuotaPreemption verifies the behavior of triggerQuotaPreemption in two scenarios:
// disabled (no-op) and enabled (releases fired after the preemption delay elapses).
// The only difference between the two cases is the PartitionPreemptionConfig.QuotaPreemptionEnabled
// flag; all other setup is identical.
func TestTriggerQuotaPreemption(t *testing.T) {
	testCases := []struct {
		name                      string
		quotaPreemptionEnabled    bool
		expectedReleaseCount      int // 0 means none; >0 means at-least-one
	}{
		{"quota preemption disabled", false, 0},
		{"quota preemption enabled", true, 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheduler := NewScheduler()
			partition := createQuotaPreemptionQueuesNodes(t)
			// override the partition-level flag to match the test case
			partition.quotaPreemptionEnabled = tc.quotaPreemptionEnabled
			scheduler.clusterContext.partitions["test"] = partition

			app, testHandler := newApplicationWithHandler(appID1, "default", "root.leaf")
			err := partition.AddApplication(app)
			assert.NilError(t, err)

			// add allocations so queue usage is above its limit
			for i := 1; i <= 5; i++ {
				res, resErr := resources.NewResourceFromConf(map[string]string{"vcore": "2"})
				assert.NilError(t, resErr)
				alloc := si.Allocation{
					AllocationKey:    "ask-key-" + strconv.Itoa(i),
					ApplicationID:    appID1,
					NodeID:           nodeID1,
					ResourcePerAlloc: res.ToProto(),
				}
				_, allocCreated, allocErr := partition.UpdateAllocation(objects.NewAllocationFromSI(&alloc))
				assert.NilError(t, allocErr)
				assert.Check(t, allocCreated, "alloc should have been created")
			}

			// lower the max so the queue is now over quota; delay is always set
			root := partition.GetQueue("root")
			assert.Assert(t, root != nil, "root queue not found")
			newLeafConf := []configs.QueueConfig{
				createLeafQueueConfig(
					map[string]string{"memory": "10", "vcore": "5"},
					map[string]string{configs.QuotaPreemptionDelay: "1s"},
				),
			}
			err = partition.updateQueues(newLeafConf, root)
			assert.NilError(t, err, "failed to update queue config")

			// wait for the preemption delay to elapse (no-op for disabled case)
			time.Sleep(1100 * time.Millisecond)

			scheduler.triggerQuotaPreemption()

			// wait for any async preemption goroutine to complete
			time.Sleep(300 * time.Millisecond)

			releaseEventCount := 0
			for _, event := range testHandler.GetEvents() {
				if _, ok := event.(*rmevent.RMReleaseAllocationEvent); ok {
					releaseEventCount++
				}
			}
			assert.Equal(t, releaseEventCount, tc.expectedReleaseCount, "expected release event does not match actual count")
		})
	}
}
