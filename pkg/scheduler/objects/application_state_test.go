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
	appInfo := NewApplication("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())

	// new to accepted
	err := appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected new to accepted")
	assert.Equal(t, appInfo.GetApplicationState(), Accepted.String())

	// accepted to rejected: error expected
	err = appInfo.HandleApplicationEvent(rejectApplication)
	assert.Assert(t, err != nil, "error expected accepted to rejected")
	assert.Equal(t, appInfo.GetApplicationState(), Accepted.String())

	// accepted to killed
	err = appInfo.HandleApplicationEvent(KillApplication)
	assert.NilError(t, err, "no error expected accepted to killed")
	assert.Equal(t, appInfo.GetApplicationState(), killed.String())
}

func TestRejectStateTransition(t *testing.T) {
	// Reject only from new
	appInfo := cache.newApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())

	// new to rejected
	err := appInfo.HandleApplicationEvent(rejectApplication)
	assert.NilError(t, err, "no error expected new to rejected")
	assert.Equal(t, appInfo.GetApplicationState(), Rejected.String())

	// app already rejected: error expected
	err = appInfo.HandleApplicationEvent(rejectApplication)
	assert.Assert(t, err != nil, "error expected rejected to rejected")
	assert.Equal(t, appInfo.GetApplicationState(), Rejected.String())

	// rejected to killed: error expected
	err = appInfo.HandleApplicationEvent(KillApplication)
	assert.Assert(t, err != nil, "error expected rejected to killed")
	assert.Equal(t, appInfo.GetApplicationState(), Rejected.String())
}

func TestStartStateTransition(t *testing.T) {
	// starting only from accepted
	appInfo := cache.newApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
	err := appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected new to accepted (start test)")
	assert.Equal(t, appInfo.GetApplicationState(), Accepted.String())

	// start app
	err = appInfo.HandleApplicationEvent(runApplication)
	assert.Assert(t, err, "no error expected new to starting")
	assert.Equal(t, appInfo.GetApplicationState(), Starting.String())

	// starting to rejected: error expected
	err = appInfo.HandleApplicationEvent(rejectApplication)
	assert.Assert(t, err != nil, "error expected starting to rejected")
	assert.Equal(t, appInfo.GetApplicationState(), Starting.String())

	// start to killed
	err = appInfo.HandleApplicationEvent(KillApplication)
	assert.NilError(t, err, "no error expected starting to killed")
	assert.Equal(t, appInfo.GetApplicationState(), killed.String())
}

func TestRunStateTransition(t *testing.T) {
	// run only from starting
	appInfo := cache.newApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
	err := appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected new to accepted (run test)")
	err = appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected accepted to starting (run test)")
	assert.Equal(t, appInfo.GetApplicationState(), Starting.String())

	// run app
	err = appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected starting to running")
	assert.Equal(t, appInfo.GetApplicationState(), Running.String())

	// run app: same state is allowed for running
	err = appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected running to running")
	assert.Equal(t, appInfo.GetApplicationState(), Running.String())

	// running to rejected: error expected
	err = appInfo.HandleApplicationEvent(rejectApplication)
	assert.Assert(t, err != nil, "error expected running to rejected")
	assert.Equal(t, appInfo.GetApplicationState(), Running.String())

	// run to killed
	err = appInfo.HandleApplicationEvent(KillApplication)
	assert.NilError(t, err, "no error expected running to killed")
	assert.Equal(t, appInfo.GetApplicationState(), killed.String())

	// run fails from killing
	err = appInfo.HandleApplicationEvent(runApplication)
	assert.Assert(t, err != nil, "error expected killed to running")
	assert.Equal(t, appInfo.GetApplicationState(), killed.String())
}

func TestCompletedStateTransition(t *testing.T) {
	// complete only from run or waiting
	appInfo := cache.newApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
	err := appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected new to accepted (completed test)")
	err = appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected accepted to starting (completed test)")
	err = appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected starting to running (completed test)")
	assert.Equal(t, appInfo.GetApplicationState(), Running.String())
	// completed from run
	err = appInfo.HandleApplicationEvent(completeApplication)
	assert.NilError(t, err, "no error expected running to completed")
	assert.Equal(t, appInfo.GetApplicationState(), Completed.String())

	// complete only from run or waiting
	appInfo = cache.newApplicationInfo("app-00002", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
	err = appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected new to accepted (completed test)")
	err = appInfo.HandleApplicationEvent(waitApplication)
	assert.NilError(t, err, "no error expected accepted to waiting (completed test)")
	assert.Equal(t, appInfo.GetApplicationState(), Waiting.String())
	// completed from waiting
	err = appInfo.HandleApplicationEvent(completeApplication)
	assert.NilError(t, err, "no error expected waiting to completed")
	assert.Equal(t, appInfo.GetApplicationState(), Completed.String())

	// completed to rejected: error expected
	err = appInfo.HandleApplicationEvent(rejectApplication)
	assert.Assert(t, err != nil, "error expected completed to rejected")
	assert.Equal(t, appInfo.GetApplicationState(), Completed.String())

	// completed to killed: error expected
	err = appInfo.HandleApplicationEvent(KillApplication)
	assert.Assert(t, err != nil, "error expected completed to killed")
	assert.Equal(t, appInfo.GetApplicationState(), Completed.String())

	// completed fails from all but running and waiting
	appInfo = cache.newApplicationInfo("app-00003", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
	err = appInfo.HandleApplicationEvent(completeApplication)
	assert.Assert(t, err != nil, "error expected new to completed")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
}

func TestWaitStateTransition(t *testing.T) {
	appInfo := cache.newApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
	err := appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected new to accepted (wait test)")

	// accepted to wait and back again
	err = appInfo.HandleApplicationEvent(waitApplication)
	assert.NilError(t, err, "no error expected accepted to waiting")
	assert.Equal(t, appInfo.GetApplicationState(), Waiting.String())

	// starting to wait
	appInfo = cache.newApplicationInfo("app-00002", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
	err = appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected new to accepted (wait test)")
	err = appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected accepted to starting")
	assert.Equal(t, appInfo.GetApplicationState(), Starting.String())
	err = appInfo.HandleApplicationEvent(waitApplication)
	assert.NilError(t, err, "no error expected starting to waiting")
	assert.Equal(t, appInfo.GetApplicationState(), Waiting.String())

	// wait to run and back again
	err = appInfo.HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "no error expected starting to running (wait test)")
	err = appInfo.HandleApplicationEvent(waitApplication)
	assert.NilError(t, err, "no error expected running to waiting")
	assert.Equal(t, appInfo.GetApplicationState(), Waiting.String())

	// wait to killed
	err = appInfo.HandleApplicationEvent(KillApplication)
	assert.NilError(t, err, "no error expected wait to killed")
	assert.Equal(t, appInfo.GetApplicationState(), killed.String())
}

func TestKilledStateTransition(t *testing.T) {
	// killed from all but rejected & completed
	appInfo := cache.newApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())

	// new to killed
	err := appInfo.HandleApplicationEvent(KillApplication)
	assert.NilError(t, err, "no error expected new to killed")
	assert.Equal(t, appInfo.GetApplicationState(), killed.String())

	// killed to killed
	err = appInfo.HandleApplicationEvent(KillApplication)
	assert.NilError(t, err, "no error expected killed to killed")
	assert.Equal(t, appInfo.GetApplicationState(), killed.String())

	// killed to rejected: error expected
	err = appInfo.HandleApplicationEvent(rejectApplication)
	assert.Assert(t, err != nil, "error expected killed to rejected")
	assert.Equal(t, appInfo.GetApplicationState(), killed.String())
}
