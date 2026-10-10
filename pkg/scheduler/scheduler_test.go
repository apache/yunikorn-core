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
	"github.com/apache/yunikorn-core/pkg/mock"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestInspectOutstandingRequests(t *testing.T) {
	callback := setupOutstandingRequestTest(t)
	scheduler := NewScheduler()
	partition, err := newBasePartition()
	assert.NilError(t, err, "unable to create partition: %v", err)
	defer partition.userGroupCache.Stop()
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

	assert.Equal(t, len(callback.updates), 3)
	for _, update := range callback.updates {
		assert.Equal(t, update.state, si.UpdateContainerSchedulingStateRequest_FAILED)
	}

	// Check #2: try again, pending asks are not collected
	noRequests, totalResources = scheduler.inspectOutstandingRequests()
	assert.Equal(t, 0, noRequests)
	assert.Assert(t, resources.IsZero(totalResources), "total resource is not zero: %v", totalResources)
	assert.Equal(t, len(callback.updates), 3, "repeated inspection must not dispatch duplicate advertisements")
}

type outstandingRequestStateUpdate struct {
	applicationID string
	allocationKey string
	state         si.UpdateContainerSchedulingStateRequest_SchedulingState
	reason        string
}

type outstandingRequestStateRecorder struct {
	mock.ResourceManagerCallback
	updates []outstandingRequestStateUpdate
}

func (r *outstandingRequestStateRecorder) UpdateContainerSchedulingState(request *si.UpdateContainerSchedulingStateRequest) {
	// Copy the observable fields; slice order records callback order.
	r.updates = append(r.updates, outstandingRequestStateUpdate{
		applicationID: request.ApplicationID,
		allocationKey: request.AllocationKey,
		state:         request.State,
		reason:        request.Reason,
	})
}

func setupOutstandingRequestTest(t *testing.T) *outstandingRequestStateRecorder {
	t.Helper()
	setupUGM()
	t.Cleanup(setupUGM)
	plugins.UnregisterSchedulerPlugins()
	t.Cleanup(plugins.UnregisterSchedulerPlugins)
	callback := &outstandingRequestStateRecorder{}
	plugins.RegisterSchedulerPlugin(callback)
	return callback
}

func outstandingRequestResource(amount resources.Quantity) *resources.Resource {
	return resources.NewResourceFromMap(map[string]resources.Quantity{"memory": amount})
}

func submitOutstandingRequest(t *testing.T, partition *PartitionContext, app *objects.Application, key string, amount resources.Quantity) *objects.Allocation {
	t.Helper()
	created, allocated, err := partition.UpdateAllocation(objects.NewAllocationFromSI(&si.Allocation{
		ApplicationID:    appID1,
		AllocationKey:    key,
		ResourcePerAlloc: outstandingRequestResource(amount).ToProto(),
	}))
	assert.NilError(t, err)
	assert.Assert(t, created && !allocated)
	ask := app.GetAllocationAsk(key)
	assert.Assert(t, ask != nil)
	return ask
}

func checkOutstandingRequestUpdate(t *testing.T, callback *outstandingRequestStateRecorder, phase string, before int, state si.UpdateContainerSchedulingStateRequest_SchedulingState) {
	t.Helper()
	// Keep lifecycle failures nonfatal so withdrawal and re-arm are independently observed.
	if len(callback.updates) != before+1 {
		t.Errorf("%s: expected exactly one new %s callback for ask A; got %d new callbacks: %+v",
			phase, state, len(callback.updates)-before, callback.updates)
		return
	}
	update := callback.updates[before]
	assert.Check(t, update.applicationID == appID1 && update.allocationKey == "ask-A" && update.state == state,
		"%s: unexpected callback: %+v", phase, update)
}

func checkOutstandingRequestUpdates(t *testing.T, callback *outstandingRequestStateRecorder, states ...si.UpdateContainerSchedulingStateRequest_SchedulingState) {
	t.Helper()
	assert.Equal(t, len(callback.updates), len(states))
	for i, state := range states {
		assert.Equal(t, callback.updates[i].applicationID, appID1)
		assert.Equal(t, callback.updates[i].allocationKey, "ask-A")
		assert.Equal(t, callback.updates[i].state, state)
	}
}

func TestInspectOutstandingRequestsNilUpdaterLifecycle(t *testing.T) {
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
	askA := submitOutstandingRequest(t, partition, app, "ask-A", 12)
	assert.Assert(t, !node1.FitInNode(askA.GetAllocatedResource()) && !node2.FitInNode(askA.GetAllocatedResource()))
	assert.Assert(t, partition.tryAllocate() == nil)
	assert.Assert(t, askA.IsSchedulingAttempted() && !askA.IsAllocated() && !askA.HasTriggeredScaleUp())

	// Control callback availability in this test; this does not model a production outage.
	plugins.UnregisterSchedulerPlugins()
	assert.Assert(t, plugins.GetResourceManagerCallbackPlugin() == nil)
	count, total := scheduler.inspectOutstandingRequests()
	assert.Equal(t, count, 0)
	assert.Assert(t, resources.IsZero(total))
	assert.Assert(t, !askA.HasTriggeredScaleUp(), "an undispatched advertisement must remain pending")
	checkOutstandingRequestUpdates(t, callback)

	// Register the callback without changing A or its resource eligibility.
	plugins.RegisterSchedulerPlugin(callback)
	count, total = scheduler.inspectOutstandingRequests()
	assert.Equal(t, count, 1)
	assert.Assert(t, resources.Equals(total, outstandingRequestResource(12)))
	assert.Assert(t, askA.HasTriggeredScaleUp())
	checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED)
	count, total = scheduler.inspectOutstandingRequests()
	assert.Equal(t, count, 0)
	assert.Assert(t, resources.IsZero(total))
	checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED)

	// Actual allocation leaves A policy-ineligible, with its advertisement still outstanding.
	askB := submitOutstandingRequest(t, partition, app, "ask-B", 9)
	allocation := partition.tryAllocate()
	assert.Assert(t, allocation != nil && allocation.Request == askB && askB.IsAllocated())
	assert.Assert(t, resources.Equals(app.GetQueue().GetAllocatedResource(), outstandingRequestResource(9)))
	plugins.UnregisterSchedulerPlugins()
	assert.Assert(t, plugins.GetResourceManagerCallbackPlugin() == nil)
	count, total = scheduler.inspectOutstandingRequests()
	assert.Equal(t, count, 0)
	assert.Assert(t, resources.IsZero(total))
	assert.Assert(t, askA.HasTriggeredScaleUp(), "an undispatched withdrawal must remain pending")
	checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED)

	plugins.RegisterSchedulerPlugin(callback)
	count, total = scheduler.inspectOutstandingRequests()
	assert.Equal(t, count, 0)
	assert.Assert(t, resources.IsZero(total))
	assert.Assert(t, !askA.HasTriggeredScaleUp())
	checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED, si.UpdateContainerSchedulingStateRequest_SKIPPED)
	count, total = scheduler.inspectOutstandingRequests()
	assert.Equal(t, count, 0)
	assert.Assert(t, resources.IsZero(total))
	checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED, si.UpdateContainerSchedulingStateRequest_SKIPPED)
	assert.Assert(t, app.GetAllocationAsk("ask-A") == askA && !askA.IsAllocated() && askA.IsSchedulingAttempted())
}

func TestInspectOutstandingRequestsAutoscalingDemandLifecycle(t *testing.T) {
	callback := setupOutstandingRequestTest(t)

	disabled := false
	partition, err := newPartitionContext(configs.PartitionConfig{
		Name: "test",
		Preemption: configs.PartitionPreemptionConfig{
			Enabled:                &disabled,
			QuotaPreemptionEnabled: &disabled,
		},
		Queues: []configs.QueueConfig{{
			Name:      "root",
			Parent:    true,
			SubmitACL: "*",
			Queues: []configs.QueueConfig{{
				Name:      "default",
				Resources: configs.Resources{Max: map[string]string{"memory": "20"}},
			}},
		}},
	}, rmID, nil, false)
	assert.NilError(t, err)
	t.Cleanup(partition.userGroupCache.Stop)
	assert.Assert(t, !partition.IsPreemptionEnabled())
	scheduler := NewScheduler()
	scheduler.clusterContext.partitions["test"] = partition

	node1 := setupNode(t, "node-1", partition, outstandingRequestResource(10))
	node2 := setupNode(t, "node-2", partition, outstandingRequestResource(10))
	assert.Assert(t, node1.IsSchedulable() && node2.IsSchedulable())
	app := newApplication(appID1, "test", "root.default")
	assert.NilError(t, partition.AddApplication(app))
	queue := app.GetQueue()
	headroom := func() *resources.Resource {
		return resources.Sub(queue.GetMaxResource(), queue.GetAllocatedResource())
	}

	// A fits quota but cannot fit either node. A real allocation attempt establishes eligibility.
	askA := submitOutstandingRequest(t, partition, app, "ask-A", 12)
	assert.Assert(t, !askA.HasTriggeredScaleUp())
	assert.Assert(t, headroom().FitInMaxUndef(outstandingRequestResource(12)))
	assert.Assert(t, ugm.GetUserManager().Headroom(app.GetQueuePath(), appID1, app.GetUser()).FitInMaxUndef(outstandingRequestResource(12)))
	assert.Assert(t, !node1.FitInNode(outstandingRequestResource(12)) && !node2.FitInNode(outstandingRequestResource(12)))
	assert.Assert(t, partition.tryAllocate() == nil)
	assert.Assert(t, !askA.IsAllocated() && askA.IsSchedulingAttempted())
	count, total := scheduler.inspectOutstandingRequests()
	assert.Equal(t, count, 1)
	assert.Assert(t, resources.Equals(total, outstandingRequestResource(12)))
	assert.Equal(t, len(callback.updates), 1)
	checkOutstandingRequestUpdate(t, callback, "initial advertisement", 0, si.UpdateContainerSchedulingStateRequest_FAILED)
	assert.Assert(t, askA.HasTriggeredScaleUp())

	count, total = scheduler.inspectOutstandingRequests()
	assert.Equal(t, count, 0)
	assert.Assert(t, resources.IsZero(total))
	assert.Equal(t, len(callback.updates), 1, "stable eligibility must not advertise twice")

	// Allocate B through production accounting, reducing A's leaf quota headroom to 11.
	askB := submitOutstandingRequest(t, partition, app, "ask-B", 9)
	allocation := partition.tryAllocate()
	assert.Assert(t, allocation != nil)
	assert.Equal(t, allocation.ResultType, objects.Allocated)
	assert.Assert(t, allocation.Request == askB && askB.IsAllocated())
	assert.Assert(t, resources.Equals(queue.GetAllocatedResource(), outstandingRequestResource(9)))
	assert.Assert(t, resources.Equals(app.GetAllocatedResource(), outstandingRequestResource(9)))
	assert.Assert(t, resources.Equals(headroom(), outstandingRequestResource(11)))
	assert.Assert(t, !headroom().FitInMaxUndef(outstandingRequestResource(12)))
	assert.Assert(t, app.GetAllocationAsk("ask-A") == askA && !askA.IsAllocated())

	beforeWithdrawal := len(callback.updates)
	count, total = scheduler.inspectOutstandingRequests()
	assert.Check(t, count == 0 && resources.IsZero(total), "withdrawal must not count as new capacity demand")
	checkOutstandingRequestUpdate(t, callback, "withdrawal after headroom loss", beforeWithdrawal, si.UpdateContainerSchedulingStateRequest_SKIPPED)
	blockedCount := len(callback.updates)
	count, total = scheduler.inspectOutstandingRequests()
	assert.Check(t, count == 0 && resources.IsZero(total))
	assert.Check(t, len(callback.updates) == blockedCount, "stable ineligibility must not withdraw twice")

	// Release B normally. The original pending A and both node capacities survive unchanged.
	released, confirmed := partition.removeAllocation(&si.AllocationRelease{
		ApplicationID:   appID1,
		AllocationKey:   "ask-B",
		TerminationType: si.TerminationType_STOPPED_BY_RM,
	})
	assert.Equal(t, len(released), 1)
	assert.Assert(t, released[0] == askB && confirmed == nil)
	assert.Assert(t, app.GetAllocationAsk("ask-B") == nil)
	assert.Assert(t, resources.IsZero(queue.GetAllocatedResource()))
	assert.Assert(t, resources.IsZero(app.GetAllocatedResource()))
	assert.Assert(t, resources.IsZero(node1.GetAllocatedResource()) && resources.IsZero(node2.GetAllocatedResource()))
	assert.Assert(t, resources.Equals(headroom(), outstandingRequestResource(20)))
	assert.Assert(t, resources.Equals(queue.GetPendingResource(), outstandingRequestResource(12)))
	assert.Assert(t, app.GetAllocationAsk("ask-A") == askA && !askA.IsAllocated() && askA.IsSchedulingAttempted())
	assert.Assert(t, !node1.FitInNode(outstandingRequestResource(12)) && !node2.FitInNode(outstandingRequestResource(12)))

	beforeRearm := len(callback.updates)
	count, total = scheduler.inspectOutstandingRequests()
	assert.Check(t, count == 1 && resources.Equals(total, outstandingRequestResource(12)), "re-arm must count A's renewed demand exactly once")
	checkOutstandingRequestUpdate(t, callback, "re-arm after headroom restoration", beforeRearm, si.UpdateContainerSchedulingStateRequest_FAILED)
	rearmedCount := len(callback.updates)
	count, total = scheduler.inspectOutstandingRequests()
	assert.Check(t, count == 0 && resources.IsZero(total))
	assert.Check(t, len(callback.updates) == rearmedCount, "stable restored eligibility must not advertise twice")

	states := make([]si.UpdateContainerSchedulingStateRequest_SchedulingState, 0, len(callback.updates))
	for _, update := range callback.updates {
		states = append(states, update.state)
	}
	assert.Check(t, app.GetAllocationAsk("ask-A") == askA && !askA.IsAllocated(), "A must remain the original pending object")
	t.Logf("observed container-state callback sequence: %v", states)
	assert.Check(t, len(states) == 3, "expected FAILED -> SKIPPED -> FAILED, got %v", states)
}

func TestInspectOutstandingRequestsPolicyHeadroom(t *testing.T) {
	for _, tc := range []struct {
		name         string
		parentMax    map[string]string
		limits       []configs.Limit
		wantWithdraw bool
	}{
		{"parent quota", map[string]string{"memory": "20"}, nil, true},
		{"user quota", nil, []configs.Limit{{
			Limit: "user quota", Users: []string{"testuser"},
			MaxResources: map[string]string{"memory": "20"},
		}}, true},
		{"group quota", nil, []configs.Limit{{
			Limit: "group quota", Groups: []string{"testgroup"},
			MaxResources: map[string]string{"memory": "20"},
		}}, true},
		{"root capacity only", nil, nil, false},
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
						Name: "parent", Parent: true,
						Resources: configs.Resources{Max: tc.parentMax},
						Limits:    tc.limits,
						Queues:    []configs.QueueConfig{{Name: "default"}},
					}},
				}},
			}, rmID, nil, false)
			assert.NilError(t, err)
			t.Cleanup(partition.userGroupCache.Stop)
			scheduler := NewScheduler()
			scheduler.clusterContext.partitions["test"] = partition
			setupNode(t, "node-1", partition, outstandingRequestResource(10))
			setupNode(t, "node-2", partition, outstandingRequestResource(10))
			app := newApplication(appID1, "test", "root.parent.default")
			assert.NilError(t, partition.AddApplication(app))

			askA := submitOutstandingRequest(t, partition, app, "ask-A", 12)
			assert.Assert(t, partition.tryAllocate() == nil)
			count, total := scheduler.inspectOutstandingRequests()
			assert.Equal(t, count, 1)
			assert.Assert(t, resources.Equals(total, outstandingRequestResource(12)))
			assert.Assert(t, askA.HasTriggeredScaleUp())
			checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED)

			askB := submitOutstandingRequest(t, partition, app, "ask-B", 9)
			result := partition.tryAllocate()
			assert.Assert(t, result != nil && result.Request == askB && askB.IsAllocated())
			// Every case now lacks root capacity for A. Only policy quota loss may withdraw it.
			rootHeadroom := resources.Sub(partition.root.GetMaxResource(), partition.root.GetAllocatedResource())
			assert.Assert(t, resources.Equals(rootHeadroom, outstandingRequestResource(11)))
			assert.Assert(t, !rootHeadroom.FitInMaxUndef(outstandingRequestResource(12)))
			userHeadroom := ugm.GetUserManager().Headroom(app.GetQueuePath(), appID1, app.GetUser())
			assert.Equal(t, userHeadroom.FitInMaxUndef(outstandingRequestResource(12)), len(tc.limits) == 0)

			for i := 0; i < 2; i++ {
				count, total = scheduler.inspectOutstandingRequests()
				assert.Equal(t, count, 0)
				assert.Assert(t, resources.IsZero(total))
				assert.Equal(t, askA.HasTriggeredScaleUp(), !tc.wantWithdraw)
				if tc.wantWithdraw {
					checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED, si.UpdateContainerSchedulingStateRequest_SKIPPED)
				} else {
					checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED)
				}
			}

			released, confirmed := partition.removeAllocation(&si.AllocationRelease{
				ApplicationID: appID1, AllocationKey: "ask-B",
				TerminationType: si.TerminationType_STOPPED_BY_RM,
			})
			assert.Assert(t, len(released) == 1 && released[0] == askB && confirmed == nil)
			count, total = scheduler.inspectOutstandingRequests()
			if tc.wantWithdraw {
				assert.Equal(t, count, 1)
				assert.Assert(t, resources.Equals(total, outstandingRequestResource(12)))
				checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED, si.UpdateContainerSchedulingStateRequest_SKIPPED, si.UpdateContainerSchedulingStateRequest_FAILED)
			} else {
				assert.Equal(t, count, 0)
				assert.Assert(t, resources.IsZero(total))
				checkOutstandingRequestUpdates(t, callback, si.UpdateContainerSchedulingStateRequest_FAILED)
			}
			assert.Assert(t, app.GetAllocationAsk("ask-A") == askA && !askA.IsAllocated() && askA.HasTriggeredScaleUp())
			count, total = scheduler.inspectOutstandingRequests()
			assert.Equal(t, count, 0)
			assert.Assert(t, resources.IsZero(total))
			wantUpdates := 1
			if tc.wantWithdraw {
				wantUpdates = 3
			}
			assert.Equal(t, len(callback.updates), wantUpdates)
		})
	}
}

// TestTriggerQuotaPreemption verifies the behavior of triggerQuotaPreemption in two scenarios:
// disabled (no-op) and enabled (releases fired after the preemption delay elapses).
// The only difference between the two cases is the PartitionPreemptionConfig.QuotaPreemptionEnabled
// flag; all other setup is identical.
func TestTriggerQuotaPreemption(t *testing.T) {
	testCases := []struct {
		name                   string
		quotaPreemptionEnabled bool
		expectedReleaseCount   int // 0 means none; >0 means at-least-one
	}{
		{"quota preemption disabled", false, 0},
		{"quota preemption enabled", true, 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheduler := NewScheduler()
			partition := createQuotaPreemptionQueuesNodes(t)
			defer partition.userGroupCache.Stop()

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

// TestHandleEventRouting verifies that HandleEvent routes allocation and application
// events to pendingAllocEvents, and all other events to pendingInfraEvents.
func TestHandleEventRouting(t *testing.T) {
	scheduler := NewScheduler()

	allocCases := []interface{}{
		&rmevent.RMUpdateAllocationEvent{Request: &si.AllocationRequest{}},
		&rmevent.RMUpdateApplicationEvent{Request: &si.ApplicationRequest{}},
	}
	for _, ev := range allocCases {
		scheduler.HandleEvent(ev)
	}
	assert.Equal(t, len(scheduler.pendingAllocEvents), 2, "expected 2 events on pendingAllocEvents")
	assert.Equal(t, len(scheduler.pendingInfraEvents), 0, "expected 0 events on pendingInfraEvents")
	assert.Equal(t, len(scheduler.pendingNodeEvents), 0, "expected 0 events on pendingNodeEvents")

	infraCases := []interface{}{
		&rmevent.RMRegistrationEvent{Channel: make(chan *rmevent.Result, 1)},
		&rmevent.RMConfigUpdateEvent{Channel: make(chan *rmevent.Result, 1)},
		&rmevent.RMPartitionsRemoveEvent{Channel: make(chan *rmevent.Result, 1)},
	}
	for _, ev := range infraCases {
		scheduler.HandleEvent(ev)
	}
	assert.Equal(t, len(scheduler.pendingAllocEvents), 2, "alloc channel should remain unchanged")
	assert.Equal(t, len(scheduler.pendingInfraEvents), 3, "expected 3 events on pendingInfraEvents")

	nodeCase := &rmevent.RMUpdateNodeEvent{Request: &si.NodeRequest{}}
	scheduler.HandleEvent(nodeCase)
	assert.Equal(t, len(scheduler.pendingAllocEvents), 2, "alloc channel should remain unchanged")
	assert.Equal(t, len(scheduler.pendingInfraEvents), 3, "infra channel should remain unchanged")
	assert.Equal(t, len(scheduler.pendingNodeEvents), 1, "expected 1 event on pendingNodeEvents")
}

// TestHandleAllocEventGoroutine verifies that the handleAllocEvent goroutine drains
// pendingAllocEvents and calls registerActivity and stops when signaled.
func TestHandleAllocEventGoroutine(t *testing.T) {
	scheduler := NewScheduler()

	done := make(chan struct{})
	go func() {
		defer close(done)
		scheduler.handleAllocEvent()
	}()

	// send an allocation update event; an empty request processes cleanly
	scheduler.pendingAllocEvents <- &rmevent.RMUpdateAllocationEvent{
		Request: &si.AllocationRequest{},
	}

	// wait for activity signal which proves the event was dequeued and processed
	select {
	case <-scheduler.activityPending:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for activity from handleAllocEvent")
	}

	close(scheduler.stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleAllocEvent goroutine did not stop")
	}
}

// TestHandleInfraEventGoroutine verifies that the handleInfraEvent goroutine drains
// pendingInfraEvents and calls registerActivity and stops when signaled.
func TestHandleInfraEventGoroutine(t *testing.T) {
	scheduler := NewScheduler()

	resultCh := make(chan *rmevent.Result, 1)
	scheduler.pendingInfraEvents <- &rmevent.RMRegistrationEvent{
		Registration: &si.RegisterResourceManagerRequest{
			RmID:        rmID,
			PolicyGroup: "default-policy-group",
			Version:     "0.0.2",
		},
		Channel: resultCh,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		scheduler.handleInfraEvent()
	}()

	// wait for activity signal which proves the event was processed
	select {
	case <-scheduler.activityPending:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for activity from handleInfraEvent")
	}

	close(scheduler.stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleInfraEvent goroutine did not stop")
	}
}

// TestHandleNodeEventGoroutine verifies that the handleNodeEvent goroutine drains
// pendingNodeEvents and calls registerActivity.
func TestHandleNodeEventGoroutine(t *testing.T) {
	scheduler := NewScheduler()

	scheduler.pendingNodeEvents <- &rmevent.RMUpdateNodeEvent{
		Request: &si.NodeRequest{},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		scheduler.handleNodeEvent()
	}()

	// wait for activity signal which proves the event was processed
	select {
	case <-scheduler.activityPending:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for activity from handleNodeEvent")
	}

	close(scheduler.stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleNodeEvent goroutine did not stop")
	}
}

// TestNodeEventsNotBlockedByAllocEvents verifies that a full pendingAllocEvents channel
// does not prevent node events from being enqueued or processed.
func TestNodeEventsNotBlockedByAllocEvents(t *testing.T) {
	scheduler := NewScheduler()
	// fill the alloc channel to capacity so it cannot accept more events
	for i := 0; i < cap(scheduler.pendingAllocEvents); i++ {
		scheduler.pendingAllocEvents <- &rmevent.RMUpdateAllocationEvent{Request: &si.AllocationRequest{}}
	}

	// a node event must still be enqueued without blocking
	nodeEv := &rmevent.RMUpdateNodeEvent{Request: &si.NodeRequest{}}
	scheduler.HandleEvent(nodeEv)
	assert.Equal(t, len(scheduler.pendingNodeEvents), 1, "node event should be queued even when alloc channel is full")
}
