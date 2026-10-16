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
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestInspectOutstandingRequestsCrossAppAdvertisementStability(t *testing.T) {
	const (
		askAKey = "ask-A"
		askYKey = "ask-Y"
	)
	callback := setupOutstandingRequestTest(t)
	disabled := false
	partition, err := newPartitionContext(configs.PartitionConfig{
		Name: "test",
		Preemption: configs.PartitionPreemptionConfig{
			Enabled: &disabled, QuotaPreemptionEnabled: &disabled,
		},
		Queues: []configs.QueueConfig{{
			Name: "root", Parent: true, SubmitACL: "*",
			Queues: []configs.QueueConfig{{
				Name:      "default",
				Resources: configs.Resources{Max: map[string]string{"memory": "20"}},
				Properties: map[string]string{
					configs.ApplicationSortPolicy:   "fifo",
					configs.ApplicationSortPriority: configs.ApplicationSortPriorityEnabled,
				},
			}},
		}},
	}, rmID, nil, false)
	assert.NilError(t, err)
	t.Cleanup(partition.userGroupCache.Stop)
	scheduler := NewScheduler()
	scheduler.clusterContext.partitions["test"] = partition
	node1 := setupNode(t, "node-1", partition, outstandingRequestResource(5))
	node2 := setupNode(t, "node-2", partition, outstandingRequestResource(5))

	// Establish A's advertisement through a real unsuccessful scheduling attempt.
	appA := newApplication(appID1, "test", "root.default")
	assert.NilError(t, partition.AddApplication(appA))
	askA := submitOutstandingRequest(t, partition, appA, askAKey, 12)
	assert.Assert(t, partition.tryAllocate() == nil)
	count, total := scheduler.inspectOutstandingRequests()
	assert.Equal(t, count, 1)
	assert.Assert(t, resources.Equals(total, outstandingRequestResource(12)))
	checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED)
	assert.Assert(t, askA.HasTriggeredScaleUp())

	// The existing submission helper hardcodes appID1, so submit Y through the same API.
	appY := newApplication("app-Y", "test", "root.default")
	assert.NilError(t, partition.AddApplication(appY))
	created, allocated, err := partition.UpdateAllocation(objects.NewAllocationFromSI(&si.Allocation{
		ApplicationID:    appY.ApplicationID,
		AllocationKey:    askYKey,
		ResourcePerAlloc: outstandingRequestResource(12).ToProto(),
		Priority:         1,
	}))
	assert.NilError(t, err)
	assert.Assert(t, created && !allocated)
	askY := appY.GetAllocationAsk(askYKey)
	assert.Assert(t, askY != nil)
	assert.Assert(t, partition.tryAllocate() == nil)
	assert.Assert(t, askA.HasTriggeredScaleUp() && !askY.HasTriggeredScaleUp())
	checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED)

	queue := appA.GetQueue()
	assert.Assert(t, queue == appY.GetQueue())
	assert.Assert(t, queue.IsPrioritySortEnabled())
	assert.Equal(t, appA.GetAskMaxPriority(), int32(0))
	assert.Equal(t, appY.GetAskMaxPriority(), int32(1))
	// FIFO with priority enabled compares priority before submission time. These
	// distinct priorities force Y before A independently of map order or timing.
	t.Logf("application order while both asks are pending: %s (priority 1) -> %s (priority 0)", appY.ApplicationID, appA.ApplicationID)
	stateA, stateY := appA.CurrentState(), appY.CurrentState()
	record := func(activeY bool) {
		t.Helper()
		queueAllocated := queue.GetAllocatedResource()
		allocatedA, allocatedY := appA.GetAllocatedResource(), appY.GetAllocatedResource()
		queueMax := queue.GetMaxQueueSet()
		// This leaf is directly below root, so its configured max minus allocated
		// usage is the effective queue policy headroom. Root physical capacity
		// is excluded from autoscaling policy headroom.
		headroom := resources.Sub(queueMax, queueAllocated)

		assert.Assert(t, resources.IsZero(queueAllocated) && resources.IsZero(allocatedA) && resources.IsZero(allocatedY))
		assert.Assert(t, resources.IsZero(node1.GetAllocatedResource()) && resources.IsZero(node2.GetAllocatedResource()))
		assert.Assert(t, resources.Equals(queueMax, outstandingRequestResource(20)))
		assert.Assert(t, resources.Equals(headroom, outstandingRequestResource(20)))
		pending := resources.Quantity(12)
		if activeY {
			pending = 24
		}
		assert.Assert(t, resources.Equals(queue.GetPendingResource(), outstandingRequestResource(pending)))
		assert.Assert(t, resources.Equals(partition.root.GetMaxResource(), outstandingRequestResource(10)))
		assert.Assert(t, !partition.root.GetMaxResource().FitInMaxUndef(askA.GetAllocatedResource()), "root physical capacity must not limit demand collection")
		assert.Assert(t, !node1.FitInNode(askA.GetAllocatedResource()) && !node2.FitInNode(askA.GetAllocatedResource()))
		assert.Assert(t, appA.GetAllocationAsk(askAKey) == askA && !askA.IsAllocated() && askA.IsSchedulingAttempted())
		if activeY {
			assert.Assert(t, appY.GetAllocationAsk(askYKey) == askY && !askY.IsAllocated() && askY.IsSchedulingAttempted())
			assert.Equal(t, appY.CurrentState(), stateY)
		}
		assert.Equal(t, appA.CurrentState(), stateA)
	}

	failedA := outstandingRequestStateUpdate{applicationID: appA.ApplicationID, allocationKey: askA.GetAllocationKey(), state: si.UpdateContainerSchedulingStateRequest_FAILED}
	skippedA := outstandingRequestStateUpdate{applicationID: appA.ApplicationID, allocationKey: askA.GetAllocationKey(), state: si.UpdateContainerSchedulingStateRequest_SKIPPED}
	failedY := outstandingRequestStateUpdate{applicationID: appY.ApplicationID, allocationKey: askY.GetAllocationKey(), state: si.UpdateContainerSchedulingStateRequest_FAILED}
	checkInspection := newSelectedAdvertisementInspector(t, scheduler, callback, appA, appY, askA, askY, record)
	checkInspection("inspection 1", true, 1, false, skippedA, failedY)
	checkInspection("inspection 2", true, 0, false)
	checkInspection("inspection 3", true, 0, false)

	// Cancel Y through the production RM release path; A must re-enter selection.
	released, confirmed := partition.removeAllocation(&si.AllocationRelease{
		ApplicationID: appY.ApplicationID, AllocationKey: askY.GetAllocationKey(),
		TerminationType: si.TerminationType_STOPPED_BY_RM,
	})
	assert.Assert(t, len(released) == 0 && confirmed == nil)
	assert.Assert(t, appY.GetAllocationAsk(askY.GetAllocationKey()) == nil)
	checkInspection("after cancelling Y", false, 1, true, failedA)
	checkInspection("quiet after re-arm", false, 0, true)
}
