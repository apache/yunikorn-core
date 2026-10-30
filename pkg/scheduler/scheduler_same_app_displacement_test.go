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
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestInspectOutstandingRequestsSameAppAdvertisementStability(t *testing.T) {
	const (
		askAKey = "ask-A"
		peerKey = "ask-peer"
	)
	for _, tc := range []struct {
		name     string
		queueMax string
		limits   []configs.Limit
	}{
		{"queue", "20", nil},
		{"user", "40", []configs.Limit{{Users: []string{"testuser"}, MaxResources: map[string]string{"memory": "20"}}}},
		{"group", "40", []configs.Limit{{Groups: []string{"testgroup"}, MaxResources: map[string]string{"memory": "20"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
						Name: "default", Limits: tc.limits,
						Resources: configs.Resources{Max: map[string]string{"memory": tc.queueMax}},
					}},
				}},
			}, rmID, nil, false)
			assert.NilError(t, err)
			t.Cleanup(partition.userGroupCache.Stop)
			scheduler := NewScheduler()
			scheduler.clusterContext.partitions["test"] = partition
			node1 := setupNode(t, "node-1", partition, outstandingRequestResource(10))
			node2 := setupNode(t, "node-2", partition, outstandingRequestResource(10))
			app := newApplication(appID1, "test", "root.default")
			assert.NilError(t, partition.AddApplication(app))
			askA := submitOutstandingRequest(t, partition, app, askAKey, 12)
			assert.Assert(t, partition.tryAllocate() == nil)
			count, total := scheduler.inspectOutstandingRequests()
			assert.Equal(t, count, 1)
			assert.Assert(t, resources.Equals(total, outstandingRequestResource(12)))
			checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED)
			assert.Assert(t, askA.HasTriggeredScaleUp())

			// Normal ask priority ordering puts the new peer before advertised A.
			created, allocated, err := partition.UpdateAllocation(objects.NewAllocationFromSI(&si.Allocation{
				ApplicationID: appID1, AllocationKey: peerKey, Priority: 1,
				ResourcePerAlloc: outstandingRequestResource(12).ToProto(),
			}))
			assert.NilError(t, err)
			assert.Assert(t, created && !allocated)
			peer := app.GetAllocationAsk(peerKey)
			assert.Assert(t, peer != nil && peer.GetPriority() > askA.GetPriority())
			assert.Assert(t, partition.tryAllocate() == nil)
			assert.Assert(t, askA.IsSchedulingAttempted() && peer.IsSchedulingAttempted() && !peer.HasTriggeredScaleUp())
			queue := app.GetQueue()
			queuePolicy := queue.GetMaxQueueSet()
			userPolicy := ugm.GetUserManager().Headroom(app.GetQueuePath(), appID1, app.GetUser())
			assert.Assert(t, queuePolicy.FitInMaxUndef(outstandingRequestResource(12)), "one ask must fit queue policy")
			if len(tc.limits) > 0 {
				assert.Assert(t, userPolicy != nil, "%s policy must be active", tc.name)
				assert.Assert(t, resources.Equals(userPolicy, outstandingRequestResource(20)),
					"%s policy headroom must be memory=20, got %v", tc.name, userPolicy)
				assert.Assert(t, userPolicy.FitInMaxUndef(outstandingRequestResource(12)), "one ask must fit user/group policy")
				assert.Assert(t, !userPolicy.FitInMaxUndef(outstandingRequestResource(24)), "two asks must exceed user/group policy")
				assert.Assert(t, resources.Equals(queuePolicy, outstandingRequestResource(40)))
				assert.Assert(t, queuePolicy.FitInMaxUndef(outstandingRequestResource(24)),
					"queue policy must allow both asks so only user/group policy displaces A")
			} else {
				assert.Assert(t, userPolicy == nil, "queue-only case must have no user/group policy limit")
				assert.Assert(t, resources.Equals(queuePolicy, outstandingRequestResource(20)))
				assert.Assert(t, !queuePolicy.FitInMaxUndef(outstandingRequestResource(24)), "two asks must exceed queue policy")
			}
			state := app.CurrentState()
			checkPolicy := func(activeY bool) {
				t.Helper()
				assert.Assert(t, resources.IsZero(queue.GetAllocatedResource()) && resources.IsZero(app.GetAllocatedResource()))
				assert.Assert(t, resources.IsZero(node1.GetAllocatedResource()) && resources.IsZero(node2.GetAllocatedResource()))
				assert.Assert(t, resources.Equals(queue.GetMaxQueueSet(), queuePolicy))
				assert.Assert(t, resources.Equals(ugm.GetUserManager().Headroom(app.GetQueuePath(), appID1, app.GetUser()), userPolicy))
				pending := resources.Quantity(12)
				if activeY {
					pending = 24
				}
				assert.Assert(t, resources.Equals(app.GetPendingResource(), outstandingRequestResource(pending)))
				assert.Assert(t, !askA.IsAllocated() && !peer.IsAllocated())
				assert.Equal(t, app.CurrentState(), state)
			}

			failedA := outstandingRequestStateUpdate{applicationID: app.ApplicationID, allocationKey: askA.GetAllocationKey(), state: si.UpdateContainerSchedulingStateRequest_FAILED}
			skippedA := outstandingRequestStateUpdate{applicationID: app.ApplicationID, allocationKey: askA.GetAllocationKey(), state: si.UpdateContainerSchedulingStateRequest_SKIPPED}
			failedY := outstandingRequestStateUpdate{applicationID: app.ApplicationID, allocationKey: peer.GetAllocationKey(), state: si.UpdateContainerSchedulingStateRequest_FAILED}
			checkInspection := newSelectedAdvertisementInspector(t, scheduler, callback, app, app, askA, peer, checkPolicy)
			checkInspection("inspection 1", true, 1, false, skippedA, failedY)
			checkInspection("inspection 2", true, 0, false)
			checkInspection("inspection 3", true, 0, false)

			// Cancel Y through the production RM release path; A must re-enter selection.
			released, confirmed := partition.removeAllocation(&si.AllocationRelease{
				ApplicationID: app.ApplicationID, AllocationKey: peer.GetAllocationKey(),
				TerminationType: si.TerminationType_STOPPED_BY_RM,
			})
			assert.Assert(t, len(released) == 0 && confirmed == nil)
			assert.Assert(t, app.GetAllocationAsk(peer.GetAllocationKey()) == nil)
			checkInspection("after cancelling Y", false, 1, true, failedA)
			checkInspection("quiet after re-arm", false, 0, true)
		})
	}
}

func TestInspectOutstandingRequestsSameAppPolicyLossWithPendingPeer(t *testing.T) {
	const (
		askAKey = "ask-A"
		peerKey = "ask-peer"
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
			}},
		}},
	}, rmID, nil, false)
	assert.NilError(t, err)
	t.Cleanup(partition.userGroupCache.Stop)
	scheduler := NewScheduler()
	scheduler.clusterContext.partitions["test"] = partition
	node1 := setupNode(t, "node-1", partition, outstandingRequestResource(10))
	node2 := setupNode(t, "node-2", partition, outstandingRequestResource(10))
	app := newApplication(appID1, "test", "root.default")
	assert.NilError(t, partition.AddApplication(app))

	askA := submitOutstandingRequest(t, partition, app, askAKey, 12)
	assert.Assert(t, partition.tryAllocate() == nil)
	count, total := scheduler.inspectOutstandingRequests()
	assert.Equal(t, count, 1)
	assert.Assert(t, resources.Equals(total, outstandingRequestResource(12)))
	checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED)
	assert.Assert(t, askA.HasTriggeredScaleUp())

	// Establish the higher-priority peer's scheduling attempt while quota still permits it.
	created, allocated, err := partition.UpdateAllocation(objects.NewAllocationFromSI(&si.Allocation{
		ApplicationID: appID1, AllocationKey: peerKey, Priority: 1,
		ResourcePerAlloc: outstandingRequestResource(12).ToProto(),
	}))
	assert.NilError(t, err)
	assert.Assert(t, created && !allocated)
	peer := app.GetAllocationAsk(peerKey)
	assert.Assert(t, peer != nil && peer.GetPriority() > askA.GetPriority())
	assert.Assert(t, partition.tryAllocate() == nil)
	assert.Assert(t, askA.IsSchedulingAttempted() && peer.IsSchedulingAttempted())
	assert.Assert(t, !peer.HasTriggeredScaleUp())

	// Real allocation accounting leaves only 11; the pending peer cannot hide A's policy loss.
	askB := submitOutstandingRequest(t, partition, app, "ask-B", 9)
	allocation := partition.tryAllocate()
	assert.Assert(t, allocation != nil)
	assert.Equal(t, allocation.ResultType, objects.Allocated)
	assert.Assert(t, allocation.Request == askB && askB.IsAllocated())
	assert.Assert(t, askA.HasTriggeredScaleUp() && !peer.HasTriggeredScaleUp())
	checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED)
	queue := app.GetQueue()
	state := app.CurrentState()
	checkPolicyLoss := func() {
		t.Helper()
		assert.Assert(t, resources.Equals(queue.GetMaxQueueSet(), outstandingRequestResource(20)))
		assert.Assert(t, resources.Equals(queue.GetAllocatedResource(), outstandingRequestResource(9)))
		assert.Assert(t, resources.Equals(app.GetAllocatedResource(), outstandingRequestResource(9)))
		assert.Assert(t, resources.Equals(resources.Add(node1.GetAllocatedResource(), node2.GetAllocatedResource()), outstandingRequestResource(9)))
		// The leaf is directly below root; physical root capacity is not policy headroom.
		headroom := resources.Sub(queue.GetMaxQueueSet(), queue.GetAllocatedResource())
		assert.Assert(t, resources.Equals(headroom, outstandingRequestResource(11)))
		assert.Assert(t, !headroom.FitInMaxUndef(askA.GetAllocatedResource()))
		assert.Assert(t, !headroom.FitInMaxUndef(peer.GetAllocatedResource()))
		assert.Assert(t, ugm.GetUserManager().Headroom(app.GetQueuePath(), appID1, app.GetUser()) == nil)
		assert.Assert(t, resources.Equals(app.GetPendingResource(), outstandingRequestResource(24)))
		assert.Assert(t, resources.Equals(queue.GetPendingResource(), outstandingRequestResource(24)))
		assert.Assert(t, askB.IsAllocated() && !askA.IsAllocated() && !peer.IsAllocated())
		assert.Equal(t, app.CurrentState(), state)
	}

	for inspection := 1; inspection <= 2; inspection++ {
		checkPolicyLoss()
		before := len(callback.updates)
		count, total = scheduler.inspectOutstandingRequests()
		checkPolicyLoss()
		assert.Equal(t, count, 0)
		assert.Assert(t, resources.IsZero(total))
		assert.Assert(t, !askA.HasTriggeredScaleUp(), "A must remain withdrawn after policy loss")
		assert.Assert(t, !peer.HasTriggeredScaleUp(), "policy-ineligible peer must remain unadvertised")
		updates := callback.updates[before:]
		t.Logf("inspection %d: new callback count=%d", inspection, len(updates))
		for _, update := range updates {
			t.Logf("inspection %d callback: %s(%s), application=%s", inspection, update.state, update.allocationKey, update.applicationID)
		}
		if inspection == 1 {
			assert.Equal(t, len(updates), 1, "first inspection must emit only SKIPPED(A)")
			assert.Equal(t, updates[0].applicationID, appID1)
			assert.Equal(t, updates[0].allocationKey, askAKey)
			assert.Equal(t, updates[0].state, si.UpdateContainerSchedulingStateRequest_SKIPPED)
		} else {
			assert.Equal(t, len(updates), 0, "second inspection must not emit callbacks")
		}
	}
}
