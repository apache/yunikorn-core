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

package statemachine

import (
	"testing"
	"time"
)

func TestStartScheduler(t *testing.T) {
	sm := NewSchedulerStateMachine()
	go sm.handleFsmStateEvent()
	defer sm.Stop()

	sm.HandleEvent(FsmStateEvent{EventType: StartScheduler})
	if err := sm.WaitForState(time.Duration(3) * time.Second, Running); err != nil {
		t.Errorf(err.Error())
	}
}

func TestStopScheduler(t *testing.T) {
	sm := NewSchedulerStateMachine()
	go sm.handleFsmStateEvent()
	sm.Stop()

	if err := sm.WaitForState(time.Duration(3) * time.Second, Stopped); err != nil {
		t.Errorf(err.Error())
	}
}

func TestSchedulerRecoverySuccess(t *testing.T) {
	sm := NewSchedulerStateMachine()
	go sm.handleFsmStateEvent()
	defer sm.Stop()

	// trigger recovery
	sm.HandleEvent(FsmStateEvent{EventType: RecoverScheduler})
	if err := sm.WaitForState(time.Duration(3) * time.Second, Recovering); err != nil {
		t.Errorf(err.Error())
	}

	// recover succeed
	sm.HandleEvent(FsmStateEvent{EventType: RecoverSchedulerSuccess})
	if err := sm.WaitForState(time.Duration(3) * time.Second, Running); err != nil {
		t.Errorf(err.Error())
	}
}

func TestSchedulerRecoveryFail(t *testing.T) {
	sm := NewSchedulerStateMachine()
	go sm.handleFsmStateEvent()
	defer sm.Stop()

	// trigger recovery
	sm.HandleEvent(FsmStateEvent{EventType: RecoverScheduler})
	if err := sm.WaitForState(time.Duration(3) * time.Second, Recovering); err != nil {
		t.Errorf(err.Error())
	}

	// recover succeed
	sm.HandleEvent(FsmStateEvent{EventType: RecoverySchedulerFail})
	if err := sm.WaitForState(time.Duration(3) * time.Second, RecoverFailed); err != nil {
		t.Errorf(err.Error())
	}
}

func TestBlockUntilStarted(t *testing.T) {
	sm := NewSchedulerStateMachine()
	sm.StartService(false)

	if err := sm.WaitForState(time.Duration(3) * time.Second, Running); err != nil {
		t.Errorf(err.Error())
	}

	sm.Stop()
	if err := sm.WaitForState(time.Duration(3) * time.Second, Stopped); err != nil {
		t.Errorf(err.Error())
	}
}