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

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"

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
				Src:  []string{Completed.String(), Failed.String(), Rejected.String()},
				Dst:  Expired.String(),
			},
		},
		fsm.Callbacks{
			// The state machine is tightly tied to the Application object.
			//
			// The first argument must always be an Application and if there is a second,
			// that must be a string. If this precondition is not met, a runtime panic
			// will occur.
			"enter_state": func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				log.Logger().Info("Application state transition",
					zap.String("appID", app.ApplicationID),
					zap.String("source", event.Src),
					zap.String("destination", event.Dst),
					zap.String("event", event.Event))
				if len(event.Args) == 2 {
					eventInfo := event.Args[1].(string) //nolint:errcheck
					app.OnStateChange(event, eventInfo)
				} else {
					app.OnStateChange(event, "")
				}
			},
			"leave_state": func(event *fsm.Event) {
				event.Args[0].(*Application).clearStateTimer() //nolint:errcheck
			},
			fmt.Sprintf("enter_%s", Starting.String()): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				app.queue.incRunningApps()
				app.queue.decAllocatingAcceptedApps(app)
				metrics.GetQueueMetrics(app.queuePath).IncQueueApplicationsRunning()
				metrics.GetSchedulerMetrics().IncTotalApplicationsRunning()
				app.setStateTimer(app.startTimeout, app.stateMachine.Current(), RunApplication)
			},
			fmt.Sprintf("enter_%s", Completing.String()): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				app.setStateTimer(completingTimeout, app.stateMachine.Current(), CompleteApplication)
			},
			fmt.Sprintf("leave_%s", New.String()): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				metrics.GetQueueMetrics(app.queuePath).IncQueueApplicationsAccepted()
				metrics.GetSchedulerMetrics().IncTotalApplicationsAccepted()
			},
			fmt.Sprintf("enter_%s", Rejected.String()): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				metrics.GetQueueMetrics(app.queuePath).IncQueueApplicationsRejected()
				metrics.GetSchedulerMetrics().IncTotalApplicationsRejected()
				app.setStateTimer(terminatedTimeout, app.stateMachine.Current(), ExpireApplication)
				app.finishedTime = time.Now()
				// No rejected message when use app.HandleApplicationEvent(RejectApplication)
				if len(event.Args) == 2 {
					app.rejectedMessage = event.Args[1].(string) //nolint:errcheck
				}
			},
			fmt.Sprintf("enter_%s", Running.String()): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				metrics.GetQueueMetrics(app.queuePath).IncQueueApplicationsRunning()
				metrics.GetSchedulerMetrics().IncTotalApplicationsRunning()
			},
			fmt.Sprintf("leave_%s", Running.String()): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				app.queue.decRunningApps()
				metrics.GetQueueMetrics(app.queuePath).DecQueueApplicationsRunning()
				metrics.GetSchedulerMetrics().DecTotalApplicationsRunning()
			},
			fmt.Sprintf("enter_%s", Completed.String()): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				metrics.GetSchedulerMetrics().IncTotalApplicationsCompleted()
				metrics.GetQueueMetrics(app.queuePath).IncQueueApplicationsCompleted()
				app.setStateTimer(terminatedTimeout, app.stateMachine.Current(), ExpireApplication)
				app.executeTerminatedCallback()
				app.clearPlaceholderTimer()
			},
			fmt.Sprintf("enter_%s", Failed.String()): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				metrics.GetQueueMetrics(app.queuePath).DecQueueApplicationsRunning()
				metrics.GetQueueMetrics(app.queuePath).IncQueueApplicationsFailed()
				metrics.GetSchedulerMetrics().DecTotalApplicationsRunning()
				metrics.GetSchedulerMetrics().IncTotalApplicationsFailed()
				app.setStateTimer(terminatedTimeout, app.stateMachine.Current(), ExpireApplication)
				app.executeTerminatedCallback()
			},
		},
	)
}
