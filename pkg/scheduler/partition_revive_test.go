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
