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
	"fmt"
	"github.com/cloudera/yunikorn-core/pkg/log"
	"github.com/looplab/fsm"
	"go.uber.org/zap"
	"time"
)

type SchedulerStateMachine struct {
	stateMachine *fsm.FSM
	pendingEvents chan SchedulerStateEvent
	stopChan chan interface{}
}

func NewSchedulerStateMachine() *SchedulerStateMachine{
	sm := &SchedulerStateMachine{}
	sm.pendingEvents = make(chan SchedulerStateEvent, 1024)
	sm.stopChan = make(chan interface{})
	sm.stateMachine = fsm.NewFSM(string(New),
		fsm.Events{
			{
				Name: string(StartScheduler),
				Src:  []string{string(New)},
				Dst:  string(Running),
			},
			{
				Name: string(RecoverScheduler),
				Src:  []string{string(New)},
				Dst:  string(Recovering),
			},
			{
				Name: string(RecoverSchedulerSuccess),
				Src:  []string{string(Recovering)},
				Dst:  string(Running),
			},
			{
				Name: string(RecoverySchedulerFail),
				Src:  []string{string(Recovering)},
				Dst:  string(RecoverFailed),
			},
		},
		fsm.Callbacks{

		},
	)
	return sm
}

func (sm *SchedulerStateMachine) EnqueueSchedulerStateEvent(event SchedulerStateEvent) error {
	select {
	case sm.pendingEvents <- event:
		return nil
	default:
		return fmt.Errorf("failed to enqueue event")
	}
}

func (sm *SchedulerStateMachine) startEventHandling() {
	for {
		select {
		case event := <- sm.pendingEvents:
			log.Logger.Debug("scheduler-core state transition",
				zap.String("preState", sm.stateMachine.Current()),
				zap.String("pendingEvent", string(event.EventType)))
			if err := sm.stateMachine.Event(string(event.EventType), event.Args...); err != nil {
				log.Logger.Error("state machine", zap.Error(err))
			}
			log.Logger.Debug("scheduler-core state transition",
				zap.String("postState", sm.stateMachine.Current()),
				zap.String("handledEvent", string(event.EventType)))
		case <-sm.stopChan:
			log.Logger.Debug("stopping")
			close(sm.pendingEvents)
			sm.stateMachine.SetState(string(Stopped))
			return
		}
	}
}

func (sm *SchedulerStateMachine) BlockUntilStarted(recoveryMode bool) {
	// start to handling events
	go sm.startEventHandling()

	// trigger start or recovery based on start-up options
	if recoveryMode {
		if err := sm.EnqueueSchedulerStateEvent(SchedulerStateEvent{
			EventType: RecoverScheduler,
		}); err != nil {
			log.Logger.Fatal("unable to run scheduler recovery", zap.Error(err))
		}
	} else {
		if err := sm.EnqueueSchedulerStateEvent(SchedulerStateEvent{
			EventType: StartScheduler,
		}); err != nil {
			log.Logger.Fatal("unable to start scheduler", zap.Error(err))
		}
	}

	// scheduler can only serve requests under Running state
	// this is a block call until it reaches that state
	if err := sm.waitForState(time.Duration(10) * time.Minute, Running); err != nil {
		log.Logger.Fatal("scheduler failed to reach Running state after 10 minutes, failing ...")
	}
}

func (sm *SchedulerStateMachine) Stop() {
	sm.stopChan <- 0
}

// only used in test
func (sm *SchedulerStateMachine) waitForState(timeout time.Duration, expectedState SchedulerStateType) error{
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for reaching state %s", expectedState)
		}

		if string(expectedState) == sm.stateMachine.Current() {
			return nil
		}

		log.Logger.Debug("state mismatch",
			zap.String("expected", string(expectedState)),
			zap.String("actual", sm.stateMachine.Current()))
		time.Sleep(time.Duration(1) * time.Second)
	}
}