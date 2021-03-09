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

	"gotest.tools/assert"
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

	// accepted to killed
	err = app.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected accepted to killed")
	assert.Equal(t, app.CurrentState(), Failed.String())
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

	// rejected to killed: error expected
	err = app.HandleApplicationEvent(FailApplication)
	assert.Assert(t, err != nil, "error expected rejected to killed")
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

	// start to killed
	err = appInfo.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected starting to killed")
	assert.Equal(t, appInfo.CurrentState(), Failed.String())
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

	// run to killed
	err = appInfo.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected running to killed")
	assert.Equal(t, appInfo.CurrentState(), Failed.String())

	// run fails from killing
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.Assert(t, err != nil, "error expected killed to running")
	assert.Equal(t, appInfo.CurrentState(), Failed.String())
}

func TestCompletedStateTransition(t *testing.T) {
	// complete only from run or waiting
	appInfo := newApplication("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())
	err := appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completed test)")
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected accepted to starting (completed test)")
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected starting to running (completed test)")
	assert.Equal(t, appInfo.CurrentState(), Running.String())
	// completed from run
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected running to completed")
	assert.Equal(t, appInfo.CurrentState(), Completed.String())

	// complete only from run or waiting
	appInfo = newApplication("app-00002", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (completed test)")
	err = appInfo.HandleApplicationEvent(WaitApplication)
	assert.NilError(t, err, "no error expected accepted to waiting (completed test)")
	assert.Equal(t, appInfo.CurrentState(), Waiting.String())
	// completed from waiting
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.NilError(t, err, "no error expected waiting to completed")
	assert.Equal(t, appInfo.CurrentState(), Completed.String())

	// completed to rejected: error expected
	err = appInfo.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err != nil, "error expected completed to rejected")
	assert.Equal(t, appInfo.CurrentState(), Completed.String())

	// completed to killed: error expected
	err = appInfo.HandleApplicationEvent(FailApplication)
	assert.Assert(t, err != nil, "error expected completed to killed")
	assert.Equal(t, appInfo.CurrentState(), Completed.String())

	// completed fails from all but running and waiting
	appInfo = newApplication("app-00003", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.Assert(t, err != nil, "error expected new to completed")
	assert.Equal(t, appInfo.CurrentState(), New.String())
}

func TestWaitStateTransition(t *testing.T) {
	appInfo := newApplication("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())
	err := appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (wait test)")

	// accepted to wait and back again
	err = appInfo.HandleApplicationEvent(WaitApplication)
	assert.NilError(t, err, "no error expected accepted to waiting")
	assert.Equal(t, appInfo.CurrentState(), Waiting.String())

	// starting to wait
	appInfo = newApplication("app-00002", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (wait test)")
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected accepted to starting")
	assert.Equal(t, appInfo.CurrentState(), Starting.String())
	err = appInfo.HandleApplicationEvent(WaitApplication)
	assert.NilError(t, err, "no error expected starting to waiting")
	assert.Equal(t, appInfo.CurrentState(), Waiting.String())

	// wait to run and back again
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected starting to running (wait test)")
	err = appInfo.HandleApplicationEvent(WaitApplication)
	assert.NilError(t, err, "no error expected running to waiting")
	assert.Equal(t, appInfo.CurrentState(), Waiting.String())

	// wait to killed
	err = appInfo.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected wait to killed")
	assert.Equal(t, appInfo.CurrentState(), Failed.String())
}

func TestKilledStateTransition(t *testing.T) {
	// killed from all but rejected & completed
	appInfo := newApplication("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.CurrentState(), New.String())

	// new to killed
	err := appInfo.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected new to killed")
	assert.Equal(t, appInfo.CurrentState(), Failed.String())

	// killed to killed
	err = appInfo.HandleApplicationEvent(FailApplication)
	assert.NilError(t, err, "no error expected killed to killed")
	assert.Equal(t, appInfo.CurrentState(), Failed.String())

	// killed to rejected: error expected
	err = appInfo.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err != nil, "error expected killed to rejected")
	assert.Equal(t, appInfo.CurrentState(), Failed.String())
}
