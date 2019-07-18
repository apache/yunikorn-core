/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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

package entrypoint

import (
	"github.com/cloudera/yunikorn-core/pkg/statemachine"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSchedulerStartOptions(t *testing.T) {
	testCases := []struct {
		recoveryFlag bool
		expectState  statemachine.FsmStateType
		name         string
	}{
		{false, statemachine.Running, "normal start"},
		{true, statemachine.Recovering, "recovery mode"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			context := startAllServicesWithParameters(StartupOptions{
				manualScheduleFlag: false,
				startWebAppFlag:    false,
				recoveryFlag:       tc.recoveryFlag,
			})

			assert.NoError(t, context.StateMachine.WaitForState(
				time.Duration(3)*time.Second,
				tc.expectState))

			context.StopAll()
		})
	}
}