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

package fsm

import (
	"testing"
	"time"
)

func TestStartScheduler(t *testing.T) {
	sm := NewSchedulerStateMachine()
	go sm.startEventHandling()
	defer sm.Stop()

	if err := sm.EnqueueSchedulerStateEvent(SchedulerStateEvent{
		EventType: StartScheduler,
	}); err != nil {
		t.Errorf(err.Error())
	}

	if err := sm.waitForState(time.Duration(3) * time.Second, Running); err != nil {
		t.Errorf(err.Error())
	}
}

func TestStopScheduler(t *testing.T) {
	sm := NewSchedulerStateMachine()
	go sm.startEventHandling()
	sm.Stop()

	if err := sm.waitForState(time.Duration(3) * time.Second, Stopped); err != nil {
		t.Errorf(err.Error())
	}
}

func TestSchedulerRecoverySuccess(t *testing.T) {
	sm := NewSchedulerStateMachine()
	go sm.startEventHandling()
	defer sm.Stop()

	// trigger recovery
	if err := sm.EnqueueSchedulerStateEvent(SchedulerStateEvent{
		EventType: RecoverScheduler,
	}); err != nil {
		t.Errorf(err.Error())
	}

	if err := sm.waitForState(time.Duration(3) * time.Second, Recovering); err != nil {
		t.Errorf(err.Error())
	}

	// recover succeed
	if err := sm.EnqueueSchedulerStateEvent(SchedulerStateEvent{
		EventType: RecoverSchedulerSuccess,
	}); err != nil {
		t.Errorf(err.Error())
	}

	if err := sm.waitForState(time.Duration(3) * time.Second, Running); err != nil {
		t.Errorf(err.Error())
	}
}

func TestSchedulerRecoveryFail(t *testing.T) {
	sm := NewSchedulerStateMachine()
	go sm.startEventHandling()
	defer sm.Stop()

	// trigger recovery
	if err := sm.EnqueueSchedulerStateEvent(SchedulerStateEvent{
		EventType: RecoverScheduler,
	}); err != nil {
		t.Errorf(err.Error())
	}

	if err := sm.waitForState(time.Duration(3) * time.Second, Recovering); err != nil {
		t.Errorf(err.Error())
	}

	// recover succeed
	if err := sm.EnqueueSchedulerStateEvent(SchedulerStateEvent{
		EventType: RecoverySchedulerFail,
	}); err != nil {
		t.Errorf(err.Error())
	}

	if err := sm.waitForState(time.Duration(3) * time.Second, RecoverFailed); err != nil {
		t.Errorf(err.Error())
	}
}

func TestBlockUntilStarted(t *testing.T) {
	sm := NewSchedulerStateMachine()
	sm.BlockUntilStarted(false)

	if err := sm.waitForState(time.Duration(3) * time.Second, Running); err != nil {
		t.Errorf(err.Error())
	}

	sm.Stop()
	if err := sm.waitForState(time.Duration(3) * time.Second, Stopped); err != nil {
		t.Errorf(err.Error())
	}
}