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
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// newSelectedAdvertisementInspector checks one inspection and its invariants.
// Test bodies own the lifecycle order and all changes to pending demand.
func newSelectedAdvertisementInspector(t *testing.T, scheduler *Scheduler,
	callback *outstandingRequestStateRecorder, appA, appY *objects.Application, askA, askY *objects.Allocation,
	checkPolicy func(bool)) func(string, bool, int, bool, ...outstandingRequestStateUpdate) {
	t.Helper()
	return func(phase string, activeY bool, wantCount int, wantA bool, want ...outstandingRequestStateUpdate) {
		t.Helper()
		checkPolicy(activeY)
		before := len(callback.updates)
		count, total := scheduler.inspectOutstandingRequests()
		checkPolicy(activeY)
		updates := callback.updates[before:]
		states := make([]string, 0, len(updates))
		for _, update := range updates {
			states = append(states, fmt.Sprintf("%s(%s)", update.state, update.allocationKey))
		}
		activeTotal := resources.NewResource()
		if askA.HasTriggeredScaleUp() {
			activeTotal.AddTo(askA.GetAllocatedResource())
		}
		if activeY && askY.HasTriggeredScaleUp() {
			activeTotal.AddTo(askY.GetAllocatedResource())
		}
		t.Logf("%s: new count=%d resource=%v, A=%t Y present=%t advertised=%t, active=%v, callbacks=%v", phase, count, total,
			askA.HasTriggeredScaleUp(), activeY, activeY && askY.HasTriggeredScaleUp(), activeTotal, states)
		assert.Check(t, count == wantCount, "%s: new demand count=%d, want %d", phase, count, wantCount)
		assert.Check(t, resources.Equals(total, outstandingRequestResource(resources.Quantity(12*wantCount))), "%s: unexpected new resource %v", phase, total)
		assert.Check(t, resources.Equals(activeTotal, outstandingRequestResource(12)), "%s: active advertised resource=%v, want memory=12", phase, activeTotal)
		assert.Check(t, askA.HasTriggeredScaleUp() == wantA, "%s: A selection mismatch", phase)
		if activeY {
			assert.Check(t, askY.HasTriggeredScaleUp(), "%s: Y must remain selected", phase)
		}
		assert.Check(t, len(updates) == len(want), "%s: callbacks=%+v, want %+v", phase, updates, want)
		if len(updates) == len(want) {
			for i, expected := range want {
				assert.Check(t, updates[i].applicationID == expected.applicationID && updates[i].allocationKey == expected.allocationKey && updates[i].state == expected.state,
					"%s: callback %d=%+v, want %+v", phase, i, updates[i], expected)
			}
		}
		assert.Assert(t, appA.GetAllocationAsk(askA.GetAllocationKey()) == askA && askA.IsSchedulingAttempted() && !askA.IsAllocated())
		for _, app := range []*objects.Application{appA, appY} {
			assert.Assert(t, resources.IsZero(ugm.GetUserManager().GetUserResources(app.GetUser().User)))
			for _, group := range app.GetUser().Groups {
				assert.Assert(t, resources.IsZero(ugm.GetUserManager().GetGroupResources(group)))
			}
			assert.Equal(t, len(app.GetReservations()), 0)
		}
	}
}

func TestInspectOutstandingRequestsCrossChildQueueSelectedSet(t *testing.T) {
	callback := setupOutstandingRequestTest(t)
	disabled := false
	priorityProperties := map[string]string{
		configs.ApplicationSortPriority: configs.ApplicationSortPriorityEnabled,
	}
	partition, err := newPartitionContext(configs.PartitionConfig{
		Name:       "test",
		Preemption: configs.PartitionPreemptionConfig{Enabled: &disabled, QuotaPreemptionEnabled: &disabled},
		Queues: []configs.QueueConfig{{
			Name: "root", Parent: true, SubmitACL: "*",
			Queues: []configs.QueueConfig{{
				Name: "parent", Parent: true,
				Resources:  configs.Resources{Max: map[string]string{"memory": "20"}},
				Properties: priorityProperties,
				Queues: []configs.QueueConfig{
					{Name: "b", Properties: priorityProperties},
					{Name: "c", Properties: priorityProperties},
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
	appA := newApplication(appID1, "test", "root.parent.c")
	assert.NilError(t, partition.AddApplication(appA))
	askA := submitOutstandingRequest(t, partition, appA, "ask-A", 12)
	assert.Assert(t, partition.tryAllocate() == nil)
	count, total := scheduler.inspectOutstandingRequests()
	assert.Equal(t, count, 1)
	assert.Assert(t, resources.Equals(total, outstandingRequestResource(12)))
	checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED)
	assert.Assert(t, askA.HasTriggeredScaleUp())
	t.Log("initial: FAILED(ask-A), new count=1 resource=map[memory:12], A=true")

	appY := newApplication("app-Y", "test", "root.parent.b")
	assert.NilError(t, partition.AddApplication(appY))
	created, allocated, err := partition.UpdateAllocation(objects.NewAllocationFromSI(&si.Allocation{
		ApplicationID: appY.ApplicationID, AllocationKey: "ask-Y", Priority: 1,
		ResourcePerAlloc: outstandingRequestResource(12).ToProto(),
	}))
	assert.NilError(t, err)
	assert.Assert(t, created && !allocated)
	askY := appY.GetAllocationAsk("ask-Y")
	assert.Assert(t, askY != nil && !askY.HasTriggeredScaleUp())
	assert.Assert(t, partition.tryAllocate() == nil)
	assert.Assert(t, !partition.preemptionEnabled && !partition.quotaPreemptionEnabled)
	parent := partition.GetQueue("root.parent")
	queueB, queueC := appY.GetQueue(), appA.GetQueue()
	assert.Assert(t, parent.IsPrioritySortEnabled() && queueB.IsPrioritySortEnabled() && queueC.IsPrioritySortEnabled())
	assert.Equal(t, queueB.GetCurrentPriority(), int32(1))
	assert.Equal(t, queueC.GetCurrentPriority(), int32(0))
	// sortQueues uses priority before fairness when priority sorting is enabled.
	// Distinct propagated ask priorities force B before C, independent of map order.
	t.Log("child traversal: B (priority 1) before C (priority 0), priority before fairness")
	guaranteed := parent.GetGuaranteedResource().Clone()
	checkPolicy := func(activeY bool) {
		t.Helper()
		assert.Assert(t, resources.Equals(parent.GetMaxQueueSet(), outstandingRequestResource(20)))
		assert.Assert(t, resources.Equals(parent.GetGuaranteedResource(), guaranteed))
		policyHeadroom := resources.Sub(parent.GetMaxQueueSet(), parent.GetAllocatedResource())
		assert.Assert(t, policyHeadroom.FitInMaxUndef(askA.GetAllocatedResource()), "A remains individually policy-eligible even when displaced")
		for _, app := range []*objects.Application{appA, appY} {
			assert.Assert(t, ugm.GetUserManager().Headroom(app.GetQueuePath(), app.ApplicationID, app.GetUser()) == nil, "this fixture has no UGM limits")
		}
		for _, queue := range []*objects.Queue{partition.root, parent, queueB, queueC} {
			assert.Assert(t, resources.IsZero(queue.GetAllocatedResource()))
			assert.Equal(t, len(queue.GetReservedApps()), 0)
		}
		assert.Assert(t, resources.IsZero(appA.GetAllocatedResource()) && resources.IsZero(appY.GetAllocatedResource()))
		assert.Assert(t, resources.IsZero(node1.GetAllocatedResource()) && resources.IsZero(node2.GetAllocatedResource()))
		for _, node := range []*objects.Node{node1, node2} {
			assert.Assert(t, !node.FitInNode(askA.GetAllocatedResource()) && !node.FitInNode(askY.GetAllocatedResource()))
			assert.Equal(t, len(node.GetReservations()), 0)
		}
		assert.Assert(t, resources.Equals(partition.root.GetMaxResource(), outstandingRequestResource(10)))
		assert.Assert(t, !partition.root.GetMaxResource().FitInMaxUndef(askA.GetAllocatedResource()), "root physical capacity must not limit selection")
		pending := resources.Quantity(12)
		if activeY {
			pending = 24
			assert.Assert(t, askY.IsSchedulingAttempted() && !askY.IsAllocated())
		}
		assert.Assert(t, resources.Equals(parent.GetPendingResource(), outstandingRequestResource(pending)))
	}
	failedA := outstandingRequestStateUpdate{applicationID: appA.ApplicationID, allocationKey: askA.GetAllocationKey(), state: si.UpdateContainerSchedulingStateRequest_FAILED}
	skippedA := outstandingRequestStateUpdate{applicationID: appA.ApplicationID, allocationKey: askA.GetAllocationKey(), state: si.UpdateContainerSchedulingStateRequest_SKIPPED}
	failedY := outstandingRequestStateUpdate{applicationID: appY.ApplicationID, allocationKey: askY.GetAllocationKey(), state: si.UpdateContainerSchedulingStateRequest_FAILED}
	checkInspection := newSelectedAdvertisementInspector(t, scheduler, callback, appA, appY, askA, askY, checkPolicy)
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
