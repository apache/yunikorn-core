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
	"time"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"

	"github.com/looplab/fsm"
)

const noTransition = "no transition"

// ----------------------------------
// application events
// ----------------------------------
type applicationEvent int

const (
	RunApplication applicationEvent = iota
	RejectApplication
	CompleteApplication
	FailApplication
	ExpireApplication
	ResumeApplication
)

func (ae applicationEvent) String() string {
	return [...]string{"runApplication", "rejectApplication", "completeApplication", "failApplication", "expireApplication", "resumeApplication"}[ae]
}

// ----------------------------------
// application states
// ----------------------------------
type applicationState int

const (
	New applicationState = iota
	Accepted
	Starting
	Running
	Rejected
	Completing
	Completed
	Failing
	Failed
	Expired
	Resuming
)

func (as applicationState) String() string {
	return [...]string{"New", "Accepted", "Starting", "Running", "Rejected", "Completing", "Completed", "Failing", "Failed", "Expired", "Resuming"}[as]
}

func NewAppState() *fsm.FSM {
	return fsm.NewFSM(
		New.String(), fsm.Events{
			{
				Name: RejectApplication.String(),
				Src:  []string{New.String()},
				Dst:  Rejected.String(),
			}, {
				Name: RunApplication.String(),
				Src:  []string{New.String(), Resuming.String()},
				Dst:  Accepted.String(),
			}, {
				Name: RunApplication.String(),
				Src:  []string{Accepted.String()},
				Dst:  Starting.String(),
			}, {
				Name: RunApplication.String(),
				Src:  []string{Running.String(), Starting.String(), Completing.String()},
				Dst:  Running.String(),
			}, {
				Name: CompleteApplication.String(),
				Src:  []string{Accepted.String(), Running.String(), Starting.String()},
				Dst:  Completing.String(),
			}, {
				Name: CompleteApplication.String(),
				Src:  []string{Completing.String()},
				Dst:  Completed.String(),
			}, {
				Name: FailApplication.String(),
				Src:  []string{New.String(), Accepted.String(), Starting.String(), Running.String()},
				Dst:  Failing.String(),
			}, {
				Name: FailApplication.String(),
				Src:  []string{Failing.String()},
				Dst:  Failed.String(),
			}, {
				Name: ResumeApplication.String(),
				Src:  []string{New.String(), Accepted.String()},
				Dst:  Resuming.String(),
			}, {
				Name: ExpireApplication.String(),
				Src:  []string{Completed.String(), Failed.String()},
				Dst:  Expired.String(),
			},
		},
		fsm.Callbacks{
			"enter_state": func(event *fsm.Event) {
				app, ok := event.Args[0].(*Application)
				if !ok {
					log.Logger().Warn("The first argument is not an Application")
					return
				}
				log.Logger().Info("Application state transition",
					zap.String("appID", app.ApplicationID),
					zap.String("source", event.Src),
					zap.String("destination", event.Dst),
					zap.String("event", event.Event))
				if len(event.Args) == 2 {
					eventInfo, ok := event.Args[1].(string)
					if ok {
						app.OnStateChange(event, eventInfo)
					} else {
						log.Logger().Warn("unable to get additional event info")
						app.OnStateChange(event, "")
					}
				} else {
					app.OnStateChange(event, "")
				}
			},
			"leave_state": func(event *fsm.Event) {
				event.Args[0].(*Application).clearStateTimer()
			},
			fmt.Sprintf("enter_%s", Starting.String()): func(event *fsm.Event) {
				app, ok := event.Args[0].(*Application)
				if !ok {
					log.Logger().Warn("The first argument is not an Application")
					return
				}
				setTimer(app.startTimeout, event, RunApplication)
			},
			fmt.Sprintf("enter_%s", Completing.String()): func(event *fsm.Event) {
				setTimer(completingTimeout, event, CompleteApplication)
			},
			fmt.Sprintf("leave_%s", New.String()): func(event *fsm.Event) {
				metrics.GetSchedulerMetrics().IncTotalApplicationsAccepted()
			},
			fmt.Sprintf("enter_%s", Rejected.String()): func(event *fsm.Event) {
				metrics.GetSchedulerMetrics().IncTotalApplicationsRejected()
			},
			fmt.Sprintf("enter_%s", Running.String()): func(event *fsm.Event) {
				metrics.GetSchedulerMetrics().IncTotalApplicationsRunning()
			},
			fmt.Sprintf("leave_%s", Running.String()): func(event *fsm.Event) {
				metrics.GetSchedulerMetrics().DecTotalApplicationsRunning()
			},
			fmt.Sprintf("enter_%s", Completed.String()): func(event *fsm.Event) {
				metrics.GetSchedulerMetrics().IncTotalApplicationsCompleted()
				app := setTimer(terminatedTimeout, event, ExpireApplication)
				app.executeTerminatedCallback()
				app.clearPlaceholderTimer()
			},
			fmt.Sprintf("enter_%s", Failed.String()): func(event *fsm.Event) {
				app := setTimer(terminatedTimeout, event, ExpireApplication)
				app.executeTerminatedCallback()
			},
		},
	)
}

func setTimer(timeout time.Duration, event *fsm.Event, eventToTrigger applicationEvent) *Application {
	app, ok := event.Args[0].(*Application)
	if ok {
		app.setStateTimer(timeout, app.stateMachine.Current(), eventToTrigger)
	}
	return app
}
