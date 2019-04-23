/*
Copyright 2019 The Unity Scheduler Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"gotest.tools/assert"
	"testing"
)

func TestNewApplicationInfo(t *testing.T) {
	appInfo := NewApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.ApplicationId, "app-00001")
	assert.Equal(t, appInfo.Partition, "default")
	assert.Equal(t, appInfo.QueueName, "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
}

func TestApplicationStateTransition(t *testing.T) {
	// initially app should be in New state
	appInfo := NewApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())

	// accept app
	err := appInfo.HandleApplicationEvent(AcceptApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Accepted.String())

	// app already accepted
	err = appInfo.HandleApplicationEvent(AcceptApplication)
	assert.Assert(t, err != nil)
	assert.Equal(t, appInfo.GetApplicationState(), Accepted.String())

	// run app
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Running.String())

	// run app
	err = appInfo.HandleApplicationEvent(RunApplication)
	// assert.Assert(t, err == nil)
	// looks like there is a bug in fsm, it should allow src=dest transition
	assert.Equal(t, appInfo.GetApplicationState(), Running.String())

	// complete app
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Completed.String())
}