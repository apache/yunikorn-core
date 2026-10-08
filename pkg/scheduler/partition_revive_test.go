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
	setupUGM()
	partition, err := newBasePartition()
	assert.NilError(t, err, "partition create failed")

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5, "vcores": 5})
	_, _, err = partition.UpdateAllocation(newAllocationAsk("ask", "no-such-app", res))
	assert.ErrorContains(t, err, "failed to find application", "an unknown app must still be rejected")
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
