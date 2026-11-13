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

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
)

// completedPartitionApp adds an application and drives it through moveTerminatedApp, so it is off
// pc.applications and has no queue attached, exactly as enter_Completed leaves it.
func completedPartitionApp(t *testing.T, partition *PartitionContext) *objects.Application {
	t.Helper()
	app := newApplication(appID1, "default", defQueue)
	assert.NilError(t, partition.AddApplication(app), "add application failed")
	app.SetState(objects.Completed.String())
	// this is what enter_Completed dispatches on its own goroutine
	partition.moveTerminatedApp(appID1)

	assert.Assert(t, partition.getApplication(appID1) == nil, "app should be off the active list")
	assert.Assert(t, app.GetQueue() == nil, "app should have lost its queue")
	assert.Equal(t, len(partition.GetCompletedApplications()), 1)
	return app
}

func TestUpdateAllocationRejectsInvalidWorkWithoutReviving(t *testing.T) {
	positive := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1})
	negative := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": -1, "vcores": 1})
	for _, tc := range []struct {
		name   string
		nodeID string
		res    *resources.Resource
		err    string
	}{
		{"unknown node", "missing", positive, "failed to find node missing"},
		{"zero pending", "", resources.NewResource(), "allocation contains no resources"},
		{"zero assigned", nodeID1, resources.NewResource(), "allocation contains no resources"},
		{"negative pending", "", negative, "allocation contains negative resources"},
		{"negative assigned", nodeID1, negative, "allocation contains negative resources"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupUGM()
			partition, err := newBasePartition()
			assert.NilError(t, err)
			defer partition.userGroupCache.Stop()
			node := newNodeMaxResource(nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 10, "vcores": 10}))
			assert.NilError(t, partition.AddNode(node))

			// Use real terminal cleanup synchronously, without starting an expiry timer.
			app := completedPartitionApp(t, partition)
			queue := partition.GetQueue(defQueue)
			assert.Assert(t, queue != nil)
			assert.Assert(t, app.IsCompleted())
			assert.Assert(t, queue.GetCopyOfApps()[appID1] == nil, "completed app must be off the queue")
			assert.Assert(t, !app.IsRevived(), "completed app must not be marked revived")
			finishedTime := app.FinishedTime()
			assert.Assert(t, !finishedTime.IsZero(), "cleanup must record the finished time")
			var completedKey string
			for key, completed := range partition.completedApplications {
				assert.Assert(t, completed == app)
				completedKey = key
			}
			errorsBefore, err := metrics.GetSchedulerMetrics().GetSchedulingErrors()
			assert.NilError(t, err)

			// The SI constructor treats an empty NodeID as a pending ask.
			askCreated, allocCreated, err := partition.UpdateAllocation(newAllocation("invalid-late-work", appID1, tc.nodeID, tc.res))
			assert.Error(t, err, tc.err)
			assert.Check(t, !askCreated && !allocCreated, "rejected work must not be created")
			assert.Check(t, partition.getApplication(appID1) == nil, "rejected work must not restore active ownership")
			assert.Check(t, partition.completedApplications[completedKey] == app, "rejected work must retain the original historical entry")
			assert.Check(t, len(partition.GetCompletedApplications()) == 1, "historical membership must be unchanged")
			assert.Check(t, app.IsCompleted(), "rejected work must leave the app Completed")
			assert.Check(t, app.GetQueue() == nil, "rejected work must leave the app detached")
			assert.Check(t, queue.GetCopyOfApps()[appID1] == nil, "rejected work must not restore queue membership")
			assert.Check(t, app.FinishedTime().Equal(finishedTime), "rejected work must preserve the finished time")
			assert.Check(t, !app.IsRevived(), "rejected work must not set the revived flag")
			assert.Check(t, len(app.GetAllRequests()) == 0, "rejected work must not add an ask")
			assert.Check(t, len(app.GetAllAllocations()) == 0, "rejected work must not add an allocation")
			assert.Check(t, resources.IsZero(app.GetPendingResource()), "application pending resources must stay zero")
			assert.Check(t, resources.IsZero(app.GetAllocatedResource()), "application allocated resources must stay zero")
			assert.Check(t, resources.IsZero(queue.GetPendingResource()), "queue pending resources must stay zero")
			assert.Check(t, resources.IsZero(queue.GetAllocatedResource()), "queue allocated resources must stay zero")
			assert.Check(t, resources.IsZero(node.GetAllocatedResource()), "node allocated resources must stay zero")
			assert.Check(t, node.GetAllocation("invalid-late-work") == nil, "node must not track rejected work")
			assert.Check(t, partition.GetTotalAllocationCount() == 0, "partition allocation count must stay zero")
			assertLimits(t, getTestUserGroup(), nil)
			errorsAfter, err := metrics.GetSchedulerMetrics().GetSchedulingErrors()
			assert.NilError(t, err)
			assert.Equal(t, errorsAfter, errorsBefore+1, "rejection must count one scheduling error")

			fresh := newApplication(appID1, "default", defQueue)
			assert.Assert(t, fresh != app, "same-ID admission must use a distinct application")
			err = partition.AddApplication(fresh)
			assert.Check(t, err == nil, "fresh same-ID admission must succeed: %v", err)
			assert.Check(t, partition.getApplication(appID1) == fresh, "active ownership must belong to the fresh app")
			assert.Check(t, fresh.GetQueue() == queue, "fresh app must be attached to its queue")
			assert.Check(t, queue.GetCopyOfApps()[appID1] == fresh, "queue ownership must belong to the fresh app")
			assert.Check(t, partition.completedApplications[completedKey] == app, "old app must remain historical after same-ID admission")
			assert.Check(t, app.GetQueue() == nil, "old app must remain detached after same-ID admission")
		})
	}
}

func TestUpdateAllocationRevivesCompletedApplication(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	completedPartitionApp(t, partition)

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5, "vcores": 5})
	askCreated, _, err := partition.UpdateAllocation(newAllocationAsk("late-ask", appID1, res))
	assert.NilError(t, err, "late ask should revive the completed application")
	assert.Assert(t, askCreated, "a new request should have been created")

	revived := partition.getApplication(appID1)
	assert.Assert(t, revived != nil, "app should be back on the active list")
	assert.Equal(t, revived.CurrentState(), objects.Running.String())
	assert.Equal(t, len(partition.GetCompletedApplications()), 0, "app should be off the completed list")

	queue := revived.GetQueue()
	assert.Assert(t, queue != nil, "queue should be re-attached")
	assert.Equal(t, queue.QueuePath, defQueue)
	assert.Assert(t, resources.Equals(queue.GetPendingResource(), res), "queue must track the new ask")
}

func TestUpdateAllocationRevivesCompletedApplicationWithNode(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	completedPartitionApp(t, partition)

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5, "vcores": 5})
	node := newNodeMaxResource(nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100, "vcores": 100}))
	assert.NilError(t, partition.AddNode(node), "add node failed")

	_, allocCreated, err := partition.UpdateAllocation(newAllocation("late-alloc", appID1, nodeID1, res))
	assert.NilError(t, err, "allocation should revive the completed application")
	assert.Assert(t, allocCreated, "an allocation should have been created")

	revived := partition.getApplication(appID1)
	assert.Assert(t, revived != nil, "app should be back on the active list")
	assert.Equal(t, revived.CurrentState(), objects.Running.String())
	assert.Equal(t, partition.GetTotalAllocationCount(), 1)
	assert.Assert(t, resources.Equals(revived.GetQueue().GetAllocatedResource(), res), "queue must track the allocation")
	assert.Assert(t, node.GetAllocation("late-alloc") != nil, "node must hold the allocation")
}

// The app is still on the active list but moveTerminatedApp already unset its queue: the revival has
// to re-attach one rather than charge usage to a nil queue.
func TestUpdateAllocationRestoresQueueForUnqueuedApp(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	app := newApplication(appID1, "default", defQueue)
	assert.NilError(t, partition.AddApplication(app), "add application failed")
	app.SetState(objects.Completed.String())
	app.UnSetQueue()
	assert.Assert(t, app.GetQueue() == nil, "app should have lost its queue")
	assert.Assert(t, partition.getApplication(appID1) != nil, "app is still on the active list")

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5, "vcores": 5})
	_, _, err = partition.UpdateAllocation(newAllocationAsk("late-ask", appID1, res))
	assert.NilError(t, err, "late ask should be accepted")

	assert.Assert(t, app.GetQueue() != nil, "queue should be re-attached")
	assert.Equal(t, app.CurrentState(), objects.Running.String())
	assert.Assert(t, resources.Equals(app.GetQueue().GetPendingResource(), res), "queue must track the new ask")
}

func TestUpdateAllocationUnknownAppStillFails(t *testing.T) {
	positive := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1})
	negative := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": -1})
	for _, tc := range []struct {
		name   string
		nodeID string
		res    *resources.Resource
		err    string
	}{
		{"valid pending", "", positive, "failed to find application no-such-app"},
		{"valid assigned", nodeID1, positive, "failed to find application no-such-app"},
		{"unknown node", "missing", positive, "failed to find node missing"},
		{"unknown node and zero resources", "missing", resources.NewResource(), "failed to find node missing"},
		{"zero pending", "", resources.NewResource(), "allocation contains no resources"},
		{"zero assigned", nodeID1, resources.NewResource(), "allocation contains no resources"},
		{"negative pending", "", negative, "allocation contains negative resources"},
		{"negative assigned", nodeID1, negative, "allocation contains negative resources"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupUGM()
			partition, err := newBasePartition()
			assert.NilError(t, err)
			defer partition.userGroupCache.Stop()
			assert.NilError(t, partition.AddNode(newNodeMaxResource(nodeID1, positive)))
			errorsBefore, err := metrics.GetSchedulerMetrics().GetSchedulingErrors()
			assert.NilError(t, err)

			// Malformed work fails validation before the application lookup; valid work still fails lookup.
			askCreated, allocCreated, err := partition.UpdateAllocation(newAllocation("ask", "no-such-app", tc.nodeID, tc.res))
			assert.Error(t, err, tc.err)
			assert.Assert(t, !askCreated && !allocCreated)
			assert.Equal(t, len(partition.GetApplications()), 0)
			errorsAfter, err := metrics.GetSchedulerMetrics().GetSchedulingErrors()
			assert.NilError(t, err)
			assert.Equal(t, errorsAfter, errorsBefore+1)
		})
	}
}

func TestUpdateAllocationValidWorkRecoversRunningHistoricalApplication(t *testing.T) {
	for _, nodeID := range []string{"", nodeID1} {
		t.Run("node="+nodeID, func(t *testing.T) {
			setupUGM()
			partition, err := newBasePartition()
			assert.NilError(t, err)
			defer partition.userGroupCache.Stop()
			res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1})
			assert.NilError(t, partition.AddNode(newNodeMaxResource(nodeID1, res)))
			app := completedPartitionApp(t, partition)

			// A retained pointer can leave Completed after terminal cleanup has filed the app.
			app.Lock()
			err = app.HandleApplicationEvent(objects.RunApplication)
			app.Unlock()
			assert.NilError(t, err)
			assert.Assert(t, app.IsRunning())
			assert.Assert(t, app.GetQueue() == nil)
			assert.Assert(t, partition.getApplication(appID1) == nil)
			assert.Assert(t, partition.GetCompletedApplications()[0] == app)

			askCreated, allocCreated, err := partition.UpdateAllocation(newAllocation("late-work", appID1, nodeID, res))
			assert.NilError(t, err)
			assert.Equal(t, askCreated, nodeID == "")
			assert.Equal(t, allocCreated, nodeID != "")
			assert.Assert(t, partition.getApplication(appID1) == app)
			assert.Equal(t, len(partition.GetCompletedApplications()), 0)
			queue := partition.GetQueue(defQueue)
			assert.Assert(t, app.GetQueue() == queue)
			assert.Assert(t, queue.GetCopyOfApps()[appID1] == app)
			assert.Assert(t, app.IsRunning() && app.IsRevived())
			assert.Assert(t, app.GetAllocationAsk("late-work") != nil)
		})
	}
}

// An expired application is past the point of no return and must not be revived.
func TestUpdateAllocationDoesNotReviveExpiredApplication(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	app := newApplication(appID1, "default", defQueue)
	assert.NilError(t, partition.AddApplication(app), "add application failed")
	app.SetState(objects.Expired.String())
	partition.moveTerminatedApp(appID1)

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5, "vcores": 5})
	_, _, err = partition.UpdateAllocation(newAllocationAsk("late-ask", appID1, res))
	assert.ErrorContains(t, err, "failed to find application", "expired apps must not be revived")
	assert.Equal(t, len(partition.GetCompletedApplications()), 1, "expired app should stay on the completed list")
}

// enter_Completed dispatches moveTerminatedApp on its own goroutine. If the app is revived before
// that goroutine runs, the cleanup must not strip the queue off the now-running application.
func TestMoveTerminatedAppSkipsRevivedApplication(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	app := newApplication(appID1, "default", defQueue)
	assert.NilError(t, partition.AddApplication(app), "add application failed")
	app.SetState(objects.Completed.String())

	// the shim wins the race and revives the app before the cleanup callback runs
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5, "vcores": 5})
	_, _, err = partition.UpdateAllocation(newAllocationAsk("late-ask", appID1, res))
	assert.NilError(t, err, "late ask should revive the app")
	assert.Equal(t, app.CurrentState(), objects.Running.String())

	// the in-flight cleanup now lands
	partition.moveTerminatedApp(appID1)

	assert.Assert(t, partition.getApplication(appID1) == app, "revived app must stay on the active list")
	assert.Assert(t, app.GetQueue() != nil, "revived app must keep its queue")
	assert.Equal(t, len(partition.GetCompletedApplications()), 0, "revived app must not be filed as completed")
}

// Only the most recent completed generation is revived. The completed list is keyed by
// appID + a negative unix timestamp, so it is seeded directly here: moveTerminatedApp would give
// two completions in the same second the same key and one would overwrite the other.
func TestReviveTakesLatestCompletedGeneration(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	older := newApplication(appID1, "default", defQueue)
	older.SetState(objects.Completed.String())
	newer := newApplication(appID1, "default", defQueue)
	newer.SetState(objects.Completed.String())

	partition.Lock()
	partition.completedApplications[appID1+"-100"] = older
	partition.completedApplications[appID1+"-200"] = newer
	partition.Unlock()

	taken := partition.takeCompletedApplication(appID1)
	assert.Assert(t, taken == newer, "the most recent generation should be taken")
	assert.Equal(t, len(partition.GetCompletedApplications()), 1, "the older generation should be left alone")
	assert.Assert(t, partition.getApplication(appID1) == newer, "the taken generation should be on the active list")
}

// moveTerminatedApp must not tear down an application that a revival already re-attached a queue to,
// even while the application still shows a terminal state. RestoreQueue raises the revived flag
// together with the queue: this is exactly the narrow window between the cleanup goroutine passing
// its terminal-state check and actually ripping the queue off.
func TestMoveTerminatedAppSkipsAppRevivedMidFlight(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	app := newApplication(appID1, "default", defQueue)
	assert.NilError(t, partition.AddApplication(app), "add application failed")
	queue := app.GetQueue()
	app.SetState(objects.Completed.String())

	// a revival re-attaches the queue and raises the revived flag; the app has not left Completed yet
	app.RestoreQueue(queue)
	assert.Assert(t, app.IsRevived(), "revived flag should be set")

	// the in-flight cleanup now lands
	partition.moveTerminatedApp(appID1)

	assert.Assert(t, partition.getApplication(appID1) == app, "revived app must stay on the active list")
	assert.Assert(t, app.GetQueue() != nil, "revived app must keep its queue")
	assert.Equal(t, len(partition.GetCompletedApplications()), 0, "revived app must not be filed as completed")
}

// The cleanup can win the race and file the app as completed before the revival restores its queue.
// restoreAppQueue has to reclaim it: put it back on the active list and drop the completed generation.
func TestRestoreAppQueueReclaimsCompletedApp(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	app := newApplication(appID1, "default", defQueue)
	assert.NilError(t, partition.AddApplication(app), "add application failed")
	app.SetState(objects.Completed.String())

	// cleanup wins first: the app is filed as completed and loses its queue
	partition.moveTerminatedApp(appID1)
	assert.Assert(t, partition.getApplication(appID1) == nil, "app should be off the active list")
	assert.Assert(t, app.GetQueue() == nil, "app should have lost its queue")
	assert.Equal(t, len(partition.GetCompletedApplications()), 1)

	// the revival that still holds the app pointer now restores its queue
	assert.Assert(t, partition.restoreAppQueue(app), "queue should be restored")

	assert.Assert(t, partition.getApplication(appID1) == app, "app must be back on the active list")
	assert.Assert(t, app.GetQueue() != nil, "queue must be re-attached")
	assert.Assert(t, app.IsRevived(), "revived flag should be set")
	assert.Equal(t, len(partition.GetCompletedApplications()), 0, "the completed generation must be dropped")
}

// The leaf queue can be cleaned up while the app sits on the completed list. resolveRevivedQueue must
// recreate it dynamically so the revived app has somewhere to run.
func TestResolveRevivedQueueRecreatesDynamicQueue(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	app := newApplication(appID1, "default", "root.recreated")

	queue := partition.resolveRevivedQueue(app)
	assert.Assert(t, queue != nil, "a dynamic leaf queue should be recreated")
	assert.Equal(t, queue.QueuePath, "root.recreated")
	assert.Assert(t, queue.IsLeafQueue(), "recreated queue must be a leaf")
}

// The cleaner can remove an empty queue as soon as the partition lock is released: resolveRevivedQueue
// must have attached the app and added it to the queue before it returns.
func TestResolveRevivedQueueAddsApplication(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	app := newApplication(appID1, "default", "root.recreated")

	queue := partition.resolveRevivedQueue(app)
	assert.Assert(t, queue != nil, "a dynamic leaf queue should be recreated")
	assert.Assert(t, app.GetQueue() == queue, "app must be attached to the queue")
	assert.Assert(t, queue.GetApplication(appID1) == app, "app must be added to the queue")
}

// A revived application that ran in the recovery queue must have it recreated through the recovery
// queue path rather than the regular dynamic queue path.
func TestResolveRevivedQueueRecreatesRecoveryQueue(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	app := newApplication(appID1, "default", common.RecoveryQueueFull)

	queue := partition.resolveRevivedQueue(app)
	assert.Assert(t, queue != nil, "the recovery queue should be recreated")
	assert.Assert(t, queue.IsLeafQueue(), "recovery queue must be a leaf")
}

// The queue can no longer be recreated (its parent is now a leaf): resolveRevivedQueue must give up.
func TestResolveRevivedQueueCreateFails(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	// root.default is a leaf, so a child under it cannot be created
	app := newApplication(appID1, "default", "root.default.sub")

	queue := partition.resolveRevivedQueue(app)
	assert.Assert(t, queue == nil, "queue under a leaf parent must not be created")
}

// The queue path now resolves to a non-leaf queue: the app cannot run there so revival must fail.
func TestResolveRevivedQueueRejectsNonLeaf(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	app := newApplication(appID1, "default", "root")

	queue := partition.resolveRevivedQueue(app)
	assert.Assert(t, queue == nil, "a non-leaf queue must be rejected")
}

// restoreAppQueue returns false when no leaf queue can be resolved.
func TestRestoreAppQueueFailsWithoutLeaf(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	app := newApplication(appID1, "default", "root")

	assert.Assert(t, !partition.restoreAppQueue(app), "restore must fail when no leaf queue resolves")
}

// getOrReviveApplication returns nil when an active app lost its queue and it cannot be restored.
func TestGetOrReviveApplicationRestoreFails(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	// non-leaf queue path means the queue cannot be restored
	app := newApplication(appID1, "default", "root")
	partition.Lock()
	partition.applications[appID1] = app
	partition.Unlock()
	assert.Assert(t, app.GetQueue() == nil, "app must have no queue attached")

	assert.Assert(t, partition.getOrReviveApplication(appID1) == nil, "revival must fail when the queue cannot be restored")
}

// reviveCompletedApplication hands the app back to the completed list when its queue cannot be restored.
func TestReviveCompletedApplicationQueueGoneReturnsToCompleted(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	// non-leaf queue path means the queue cannot be restored
	app := newApplication(appID1, "default", "root")
	app.SetState(objects.Completed.String())
	partition.Lock()
	partition.completedApplications[appID1+"-100"] = app
	partition.Unlock()

	assert.Assert(t, partition.reviveCompletedApplication(appID1) == nil, "revival must fail when the queue cannot be restored")
	assert.Assert(t, partition.getApplication(appID1) == nil, "app must not stay on the active list")
	assert.Equal(t, len(partition.GetCompletedApplications()), 1, "app must be handed back to the completed list")
}

// Completed entries whose key suffix is not a timestamp are ignored rather than crashing the scan.
func TestTakeCompletedApplicationSkipsUnparsableKey(t *testing.T) {
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")
	app := newApplication(appID1, "default", defQueue)
	app.SetState(objects.Completed.String())
	partition.Lock()
	partition.completedApplications[appID1+"notanumber"] = app
	partition.Unlock()

	assert.Assert(t, partition.takeCompletedApplication(appID1) == nil, "an unparsable key must be skipped")
	assert.Equal(t, len(partition.GetCompletedApplications()), 1, "the skipped entry must be left alone")
}
