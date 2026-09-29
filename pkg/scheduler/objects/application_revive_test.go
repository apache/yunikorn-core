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

package objects

import (
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/metrics"
)

// completedApp returns an application that has genuinely run through the state machine into
// Completed, so the enter_Completed teardown (cleanupAsks, timers, metrics) has really happened.
// A non-empty rmID also runs LogAppSummary, which is what nils the tracked resources.
func completedApp(t *testing.T, rmID string) *Application {
	t.Helper()
	app := newApplication(appID1, "default", "root.a")
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 10})
	assert.NilError(t, app.AddAllocationAsk(newAllocationAsk("seed-ask", appID1, res)), "seed ask add failed")
	assert.Equal(t, app.CurrentState(), Accepted.String())

	// dropping the only ask leaves no pending and no allocations, which moves the app to Completing
	app.RemoveAllocationAsk("seed-ask")
	assert.Equal(t, app.CurrentState(), Completing.String())

	app.Lock()
	err = app.HandleApplicationEvent(CompleteApplication)
	app.Unlock()
	assert.NilError(t, err, "move to Completed failed")
	assert.Equal(t, app.CurrentState(), Completed.String())

	if rmID != "" {
		app.LogAppSummary(rmID)
	}
	return app
}

func TestAddAllocationAskRevivesCompletedApp(t *testing.T) {
	setupUGM()
	app := completedApp(t, "rm-revive")
	assert.Assert(t, !app.IsRevived(), "app should not be flagged revived while completed")

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5})
	assert.NilError(t, app.AddAllocationAsk(newAllocationAsk("late-ask", appID1, res)), "late ask should revive the app")

	assert.Equal(t, app.CurrentState(), Running.String(), "completed app should be revived to Running")
	assert.Assert(t, app.IsRevived(), "revived flag should be set")
	assert.Assert(t, app.GetAllocationAsk("late-ask") != nil, "ask should be tracked after revival")
	assert.Assert(t, resources.Equals(app.GetPendingResource(), res), "pending should hold the late ask")
	// the terminatedTimeout -> ExpireApplication timer must not survive the revival
	app.RLock()
	timer := app.stateTimer
	app.RUnlock()
	assert.Assert(t, timer == nil, "expiry timer should be cleared on revival")
}

func TestRecoverAllocationAskRevivesCompletedApp(t *testing.T) {
	setupUGM()
	app := completedApp(t, "rm-revive")

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5})
	alloc := newAllocation(appID1, nodeID1, res)
	assert.NilError(t, app.RecoverAllocationAsk(alloc), "recover should revive the app")

	assert.Equal(t, app.CurrentState(), Running.String())
	assert.Assert(t, app.GetAllocationAsk(alloc.GetAllocationKey()) != nil, "recovered ask should be tracked")
}

func TestAddAllocationRevivesCompletedApp(t *testing.T) {
	setupUGM()
	app := completedApp(t, "rm-revive")

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5})
	assert.NilError(t, app.AddAllocation(newAllocation(appID1, nodeID1, res)), "add allocation should revive the app")

	assert.Equal(t, app.CurrentState(), Running.String())
	assert.Assert(t, resources.Equals(app.GetAllocatedResource(), res), "allocated resource should be tracked")
}

// A revived app has to aggregate tracked resources again: LogAppSummary nils them on completion and
// AggregateTrackedResource does not guard a nil receiver.
func TestRevivedAppTracksCompletedResourceWithoutPanic(t *testing.T) {
	setupUGM()
	app := completedApp(t, "rm-revive")
	app.RLock()
	nilled := app.usedResource == nil
	app.RUnlock()
	assert.Assert(t, nilled, "LogAppSummary should have nilled the tracker")

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5})
	alloc := newAllocation(appID1, nodeID1, res)
	assert.NilError(t, app.AddAllocation(alloc))

	app.RLock()
	restored := app.usedResource != nil && app.preemptedResource != nil && app.placeholderResource != nil
	app.RUnlock()
	assert.Assert(t, restored, "tracked resources should be rebuilt on revival")

	// would panic on a nil TrackedResource without the revival restore
	assert.Assert(t, app.RemoveAllocation(alloc.GetAllocationKey(), 0) != nil, "allocation should be removed")
}

func TestRestoreQueueClearsFinishedTime(t *testing.T) {
	setupUGM()
	app := completedApp(t, "")
	app.UnSetQueue()
	assert.Assert(t, !app.FinishedTime().IsZero(), "finished time should be set after completion")

	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.RestoreQueue(queue)
	assert.Assert(t, app.FinishedTime().IsZero(), "finished time should be cleared when the queue is restored")
	assert.Assert(t, app.GetQueue() != nil, "queue should be re-attached")
}

func TestAddPathsRejectTerminalApps(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5})

	for _, state := range []applicationState{Failing, Failed, Rejected, Expired} {
		t.Run(state.String(), func(t *testing.T) {
			setupUGM()
			app := newApplication(appID1, "default", "root.a")
			queue, err := createRootQueue(nil)
			assert.NilError(t, err, "queue create failed")
			app.queue = queue
			app.SetState(state.String())

			assert.ErrorContains(t, app.AddAllocationAsk(newAllocationAsk("ask", appID1, res)), "cannot accept new work")
			assert.Assert(t, app.GetAllocationAsk("ask") == nil, "ask must not be tracked on a terminal app")

			alloc := newAllocation(appID1, nodeID1, res)
			assert.ErrorContains(t, app.RecoverAllocationAsk(alloc), "cannot accept new work")
			assert.ErrorContains(t, app.AddAllocation(alloc), "cannot accept new work")
			assert.Assert(t, resources.IsZero(app.GetAllocatedResource()), "nothing should be allocated")
			assert.Equal(t, app.CurrentState(), state.String(), "state must be unchanged")
		})
	}
}

// Completing is the pre-existing grace period: it must still revive, but it is not a completed-app
// revival and must not be counted as one.
func TestCompletingStillRevivesWithoutReviveFlag(t *testing.T) {
	setupUGM()
	app := newApplication(appID1, "default", "root.a")
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 10})
	assert.NilError(t, app.AddAllocationAsk(newAllocationAsk("ask-1", appID1, res)))
	app.RemoveAllocationAsk("ask-1")
	assert.Equal(t, app.CurrentState(), Completing.String())

	assert.NilError(t, app.AddAllocationAsk(newAllocationAsk("ask-2", appID1, res)))
	assert.Equal(t, app.CurrentState(), Running.String())
	assert.Assert(t, !app.IsRevived(), "Completing is not a completed-app revival")
}

// The completed gauges come back down on revival and the revived gauge unwinds when the application
// completes again. The monotonic queue_app_total counter is deliberately left alone.
func TestReviveMetricsBalance(t *testing.T) {
	setupUGM()
	metrics.Reset()
	app := completedApp(t, "")

	completed, err := metrics.GetSchedulerMetrics().GetTotalApplicationsCompleted()
	assert.NilError(t, err)
	assert.Equal(t, completed, 1, "completed gauge should count the finished app")

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5})
	assert.NilError(t, app.AddAllocationAsk(newAllocationAsk("late-ask", appID1, res)))

	completed, err = metrics.GetSchedulerMetrics().GetTotalApplicationsCompleted()
	assert.NilError(t, err)
	assert.Equal(t, completed, 0, "completed gauge should be given back on revival")
	revived, err := metrics.GetSchedulerMetrics().GetTotalApplicationsRevived()
	assert.NilError(t, err)
	assert.Equal(t, revived, 1, "revived gauge should count the revival")

	// complete the app a second time: the revived gauge unwinds, completed goes back up
	app.RemoveAllocationAsk("late-ask")
	app.Lock()
	err = app.HandleApplicationEvent(CompleteApplication)
	app.Unlock()
	assert.NilError(t, err)
	assert.Equal(t, app.CurrentState(), Completed.String())

	revived, err = metrics.GetSchedulerMetrics().GetTotalApplicationsRevived()
	assert.NilError(t, err)
	assert.Equal(t, revived, 0, "revived gauge should unwind when the app completes again")
	completed, err = metrics.GetSchedulerMetrics().GetTotalApplicationsCompleted()
	assert.NilError(t, err)
	assert.Equal(t, completed, 1, "completed gauge should count the app again")
	assert.Assert(t, !app.IsRevived(), "revived flag should be cleared on re-completion")
}

// Expiry is still terminal: leaving Completed for Expired must not hand the completed gauge back.
func TestExpireDoesNotCountAsRevival(t *testing.T) {
	setupUGM()
	metrics.Reset()
	app := completedApp(t, "")

	app.Lock()
	err := app.HandleApplicationEvent(ExpireApplication)
	app.Unlock()
	assert.NilError(t, err)
	assert.Equal(t, app.CurrentState(), Expired.String())

	completed, err := metrics.GetSchedulerMetrics().GetTotalApplicationsCompleted()
	assert.NilError(t, err)
	assert.Equal(t, completed, 1, "expiry must not return the completed gauge")
	revived, err := metrics.GetSchedulerMetrics().GetTotalApplicationsRevived()
	assert.NilError(t, err)
	assert.Equal(t, revived, 0, "expiry is not a revival")
}
