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
	"fmt"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestAcceptStateTransition(t *testing.T) {
	// Accept only from new
	app := newApplication("app-00001", "default", "root.a")
	assert.Equal(t, app.CurrentState(), New.String())

	// new to accepted
	err := app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted")
	assert.Equal(t, app.CurrentState(), Accepted.String())

	// accepted to rejected: error expected
	err = app.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err != nil, "error expected accepted to rejected")
	assert.Equal(t, app.CurrentState(), Accepted.String())

	// accepted to failed
	err = app.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected accepted to failing")
	err = common.WaitFor(10*time.Microsecond, time.Millisecond*100, app.IsFailing)
	assert.NilError(t, err, "App should be in Failing state")
}

func TestRejectStateTransition(t *testing.T) {
	// Reject only from new
	app := newApplication("app-00001", "default", "root.a")
	assert.Equal(t, app.CurrentState(), New.String())

	// new to rejected
	err := app.HandleApplicationEvent(RejectApplication)
	assert.NilError(t, err, "no error expected new to rejected")
	assert.Equal(t, app.CurrentState(), Rejected.String())

	// app already rejected: error expected
	err = app.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err != nil, "error expected rejected to rejected")
	assert.Equal(t, app.CurrentState(), Rejected.String())

	// rejected to failing: error expected
	err = app.HandleApplicationEvent(FailApplication)
	assert.Assert(t, err != nil, "error expected rejected to failing")
	assert.Equal(t, app.CurrentState(), Rejected.String())
}

func TestStartStateTransition(t *testing.T) {
	// starting only from accepted
	appInfo := newApplication("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())
	err := appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (start test)")
	assert.Equal(t, appInfo.CurrentState(), Accepted.String())

	// start app
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.Assert(t, err, "no error expected new to starting")
	assert.Equal(t, appInfo.CurrentState(), Starting.String())

	// starting to rejected: error expected
	err = appInfo.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err != nil, "error expected starting to rejected")
	assert.Equal(t, appInfo.CurrentState(), Starting.String())

	// start to failing
	err = appInfo.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected starting to failing")
	err = common.WaitFor(10*time.Microsecond, time.Millisecond*100, appInfo.IsFailing)
	assert.NilError(t, err, "App should be in Failing state")
}

func TestRunStateTransition(t *testing.T) {
	// run only from starting
	appInfo := newApplication("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())
	err := appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (run test)")
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected accepted to starting (run test)")
	assert.Equal(t, appInfo.CurrentState(), Starting.String())

	// run app
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected starting to running")
	assert.Equal(t, appInfo.CurrentState(), Running.String())

	// run app: same state is allowed for running
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected running to running")
	assert.Equal(t, appInfo.CurrentState(), Running.String())

	// running to rejected: error expected
	err = appInfo.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err != nil, "error expected running to rejected")
	assert.Equal(t, appInfo.CurrentState(), Running.String())

	// run to failing
	err = appInfo.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected running to failing")
	err = common.WaitFor(10*time.Microsecond, time.Millisecond*100, appInfo.IsFailing)
	assert.NilError(t, err, "App should be in Failing state")

	// run fails from failing
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.Assert(t, err != nil, "error expected failing to running")
	assert.Equal(t, appInfo.CurrentState(), Failing.String())
}

func TestCompletedStateTransition(t *testing.T) {
	// complete only from completing
	appInfo1 := newApplication("app-00001", "default", "root.a")
	assert.Equal(t, appInfo1.CurrentState(), New.String())
	err := appInfo1.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completed test)")
	err = appInfo1.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected accepted to starting (completed test)")
	err = appInfo1.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected starting to running (completed test)")
	assert.Equal(t, appInfo1.CurrentState(), Running.String())
	// completed from run through completing
	err = appInfo1.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected running to completing")
	assert.Equal(t, appInfo1.CurrentState(), Completing.String())
	err = appInfo1.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected completing to completed")
	assert.Equal(t, appInfo1.CurrentState(), Completed.String())

	// complete from run through completing
	appInfo2 := newApplication("app-00002", "default", "root.a")
	assert.Equal(t, appInfo2.CurrentState(), New.String())
	err = appInfo2.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completed test)")
	err = appInfo2.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected accepted to completing (completed test)")
	assert.Equal(t, appInfo2.CurrentState(), Completing.String())
	// completed from completing
	err = appInfo2.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected completing to completed")
	assert.Equal(t, appInfo2.CurrentState(), Completed.String())

	// completed to rejected: error expected
	err = appInfo2.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err != nil, "error expected completed to rejected")
	assert.Equal(t, appInfo2.CurrentState(), Completed.String())

	// completed to failing: error expected
	err = appInfo2.HandleApplicationEvent(FailApplication)
	assert.Assert(t, err != nil, "error expected completed to failing")
	assert.Equal(t, appInfo2.CurrentState(), Completed.String())

	// completed fails from all completing
	appInfo3 := newApplication("app-00003", "default", "root.a")
	assert.Equal(t, appInfo3.CurrentState(), New.String())
	err = appInfo3.HandleApplicationEvent(CompleteApplication)
	assert.Assert(t, err != nil, "error expected new to completed")
	assert.Equal(t, appInfo3.CurrentState(), New.String())
}

func TestCompletingStateTransition(t *testing.T) {
	appInfo1 := newApplication("app-00001", "default", "root.a")
	assert.Equal(t, appInfo1.CurrentState(), New.String())
	err := appInfo1.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completing test)")

	// accepted to completing and back again
	err = appInfo1.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected accepted to completing")
	assert.Equal(t, appInfo1.CurrentState(), Completing.String())

	// starting to completing
	appInfo2 := newApplication("app-00002", "default", "root.a")
	assert.Equal(t, appInfo2.CurrentState(), New.String())
	err = appInfo2.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completing test)")
	err = appInfo2.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected accepted to starting")
	assert.Equal(t, appInfo2.CurrentState(), Starting.String())
	err = appInfo2.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected starting to completing")
	assert.Equal(t, appInfo2.CurrentState(), Completing.String())

	// completing to run and back again
	err = appInfo2.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected starting to running (completing test)")
	err = appInfo2.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected running to completing")
	assert.Equal(t, appInfo2.CurrentState(), Completing.String())
}

func TestFailedStateTransition(t *testing.T) {
	// failing from all but rejected & completed
	appInfo := newApplication("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())

	// new to failing
	err := appInfo.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected new to failing")
	err = common.WaitFor(10*time.Microsecond, time.Millisecond*100, appInfo.IsFailing)
	assert.NilError(t, err, "App should be in Failing state")

	// failing to failed
	err = appInfo.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected failing to failed")
	assert.Assert(t, appInfo.IsFailed(), "App should be in Failed state")

	// failed to rejected: error expected
	err = appInfo.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err != nil, "error expected failing to rejected")
	err = common.WaitFor(10*time.Microsecond, time.Millisecond*100, appInfo.IsFailed)
	assert.NilError(t, err, "App should be in Failed state")
}

func TestAppStateTransitionEvents(t *testing.T) {
	events.Init()
	eventSystem := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	eventSystem.StartServiceWithPublisher(false)
	// Accept only from new
	appInfo := newApplication("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())

	// new to accepted
	err := appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted")
	assert.Equal(t, appInfo.CurrentState(), Accepted.String())

	// accepted to completing
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected accepted to completing")
	assert.Equal(t, appInfo.CurrentState(), Completing.String())

	// completing to run
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected starting to running (completing test)")

	// run to failing
	err = appInfo.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected new to failing")
	err = common.WaitFor(10*time.Microsecond, time.Millisecond*100, appInfo.IsFailing)
	assert.NilError(t, err, "App should be in Failing state")

	// failing to failed
	err = appInfo.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected failing to failed")
	assert.Assert(t, appInfo.IsFailed(), "App should be in Failed state")

	// failed to expired
	err = appInfo.HandleApplicationEvent(ExpireApplication)
	assert.NilError(t, err, "no error expected failed to expired")
	assert.Assert(t, appInfo.IsExpired(), "App should be in Expired state")

	// accepted to resuming
	appInfo.stateMachine.SetState(Accepted.String())
	err = appInfo.HandleApplicationEvent(ResumeApplication)
	assert.NilError(t, err, "no error expected accepted to resuming")
	assert.Assert(t, appInfo.IsResuming(), "App should be in Resuming state")

	// Verify application events
	err = common.WaitFor(10*time.Millisecond, time.Second, func() bool {
		fmt.Printf("checking event length: %d\n", eventSystem.Store.CountStoredEvents())
		return eventSystem.Store.CountStoredEvents() == 8
	})
	assert.NilError(t, err, "the event should have been processed")
	records := eventSystem.Store.CollectEvents()
	if records == nil {
		t.Fatal("collecting eventChannel should return something")
	}
	assert.Equal(t, 8, len(records), "expecting 8 events")
	isNewApplicationEvent(t, appInfo, records[0])
	isStateChangeEvent(t, appInfo, si.EventRecord_APP_ACCEPTED, records[1])
	isStateChangeEvent(t, appInfo, si.EventRecord_APP_COMPLETING, records[2])
	isStateChangeEvent(t, appInfo, si.EventRecord_APP_RUNNING, records[3])
	isStateChangeEvent(t, appInfo, si.EventRecord_APP_FAILING, records[4])
	isStateChangeEvent(t, appInfo, si.EventRecord_APP_FAILED, records[5])
	isStateChangeEvent(t, appInfo, si.EventRecord_APP_EXPIRED, records[6])
	isStateChangeEvent(t, appInfo, si.EventRecord_APP_RESUMING, records[7])
}

func TestAppStateTransitionMetrics(t *testing.T) {
	// Test New -> Accepted -> Starting -> Running -> Completing-> Completed
	metrics.GetSchedulerMetrics().Reset()
	app := newApplication("app-00001", "default", "root.a")
	totalAppsRunning := getTotalAppsRunning(t)
	assert.Equal(t, totalAppsRunning, 0)
	// New -> Accepted
	err := app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected New to Accepted")
	// Accepted -> Starting
	err = app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected Accepted to Starting")
	totalAppsRunning = getTotalAppsRunning(t)
	assert.Equal(t, app.CurrentState(), Starting.String())
	assert.Equal(t, totalAppsRunning, 1)
	// Starting -> Running
	err = app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected Starting to Running")
	totalAppsRunning = getTotalAppsRunning(t)
	assert.Equal(t, app.CurrentState(), Running.String())
	assert.Equal(t, totalAppsRunning, 1)
	// Running -> Completing
	err = app.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected Running to Completing")
	totalAppsRunning = getTotalAppsRunning(t)
	assert.Equal(t, app.CurrentState(), Completing.String())
	assert.Equal(t, totalAppsRunning, 0)
	// Completing -> Completed
	err = app.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected Completing to Completed")
	totalAppsRunning = getTotalAppsRunning(t)
	assert.Equal(t, app.CurrentState(), Completed.String())
	assert.Equal(t, totalAppsRunning, 0)

	// Test New -> Accepted -> Starting -> Completing -> Running -> Completing-> Completed
	metrics.GetSchedulerMetrics().Reset()
	app = newApplication("app-00002", "default", "root.a")
	totalAppsRunning = getTotalAppsRunning(t)
	assert.Equal(t, totalAppsRunning, 0)
	// New -> Accepted
	err = app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected New to Accepted")
	// Accepted -> Starting
	err = app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected Accepted to Starting")
	totalAppsRunning = getTotalAppsRunning(t)
	assert.Equal(t, app.CurrentState(), Starting.String())
	assert.Equal(t, totalAppsRunning, 1)
	// Starting -> Completing
	err = app.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected Starting to Completing")
	totalAppsRunning = getTotalAppsRunning(t)
	assert.Equal(t, app.CurrentState(), Completing.String())
	assert.Equal(t, totalAppsRunning, 0)
	// Completing -> Running
	err = app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected Completing to Running")
	totalAppsRunning = getTotalAppsRunning(t)
	assert.Equal(t, app.CurrentState(), Running.String())
	assert.Equal(t, totalAppsRunning, 1)
	// Running -> Completing
	err = app.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected Running to Completing")
	totalAppsRunning = getTotalAppsRunning(t)
	assert.Equal(t, app.CurrentState(), Completing.String())
	assert.Equal(t, totalAppsRunning, 0)
	// Completing -> Completed
	err = app.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected Completing to Completed")
	totalAppsRunning = getTotalAppsRunning(t)
	assert.Equal(t, app.CurrentState(), Completed.String())
	assert.Equal(t, totalAppsRunning, 0)

	// Test New -> Accepted -> Starting -> Failing -> Failed
	metrics.GetSchedulerMetrics().Reset()
	app = newApplication("app-00003", "default", "root.a")
	// New -> Accepted
	err = app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected New to Accepted")
	// Accepted -> Starting
	err = app.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected Accepted to Starting")
	totalAppsRunning = getTotalAppsRunning(t)
	assert.Equal(t, app.CurrentState(), Starting.String())
	assert.Equal(t, totalAppsRunning, 1)
	// Starting -> Failing
	err = app.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected Starting to Failing")
	totalAppsRunning = getTotalAppsRunning(t)
	assert.Equal(t, app.CurrentState(), Failing.String())
	assert.Equal(t, totalAppsRunning, 0)
	// Failing -> Failed
	err = app.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected Failing to Failed")
	totalAppsRunning = getTotalAppsRunning(t)
	assert.Equal(t, app.CurrentState(), Failed.String())
	assert.Equal(t, totalAppsRunning, 0)
}

func getTotalAppsRunning(t *testing.T) int {
	totalAppsRunning, err := metrics.GetSchedulerMetrics().GetTotalApplicationsRunning()
	assert.NilError(t, err, "no error expected when getting total running application count.")
	return totalAppsRunning
}
