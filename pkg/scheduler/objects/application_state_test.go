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
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
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
	appInfo := newApplication("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())
	err := appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completed test)")
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected accepted to starting (completed test)")
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected starting to running (completed test)")
	assert.Equal(t, appInfo.CurrentState(), Running.String())
	// completed from run through completing
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected running to completing")
	assert.Equal(t, appInfo.CurrentState(), Completing.String())
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected completing to completed")
	assert.Equal(t, appInfo.CurrentState(), Completed.String())

	// complete from run through completing
	appInfo = newApplication("app-00002", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completed test)")
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected accepted to completing (completed test)")
	assert.Equal(t, appInfo.CurrentState(), Completing.String())
	// completed from completing
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected completing to completed")
	assert.Equal(t, appInfo.CurrentState(), Completed.String())

	// completed to rejected: error expected
	err = appInfo.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err != nil, "error expected completed to rejected")
	assert.Equal(t, appInfo.CurrentState(), Completed.String())

	// completed to failing: error expected
	err = appInfo.HandleApplicationEvent(FailApplication)
	assert.Assert(t, err != nil, "error expected completed to failing")
	assert.Equal(t, appInfo.CurrentState(), Completed.String())

	// completed fails from all completing
	appInfo = newApplication("app-00003", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.Assert(t, err != nil, "error expected new to completed")
	assert.Equal(t, appInfo.CurrentState(), New.String())
}

func TestCompletingStateTransition(t *testing.T) {
	appInfo := newApplication("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())
	err := appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completing test)")

	// accepted to completing and back again
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected accepted to completing")
	assert.Equal(t, appInfo.CurrentState(), Completing.String())

	// starting to completing
	appInfo = newApplication("app-00002", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completing test)")
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected accepted to starting")
	assert.Equal(t, appInfo.CurrentState(), Starting.String())
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected starting to completing")
	assert.Equal(t, appInfo.CurrentState(), Completing.String())

	// completing to run and back again
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected starting to running (completing test)")
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected running to completing")
	assert.Equal(t, appInfo.CurrentState(), Completing.String())
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
