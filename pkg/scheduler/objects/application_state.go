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
	"context"
	"fmt"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
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

const (
	EventNotNeeded = -1
)

func (ae applicationEvent) String() string {
	return [...]string{"runApplication", "rejectApplication", "completeApplication", "failApplication", "expireApplication", "resumeApplication"}[ae]
}

// ----------------------------------
// application states
// ----------------------------------
type applicationState int

// Application states are used for filtering in the webservice handlers. Please check&update the logic as needed if the state machine is modified
const (
	New applicationState = iota
	Accepted
	Running
	Rejected
	Completing
	Completed
	Failing
	Failed
	Expired
	Resuming
)

var stateEvents = map[string]si.EventRecord_ChangeDetail{
	Accepted.String():   si.EventRecord_APP_ACCEPTED,
	Running.String():    si.EventRecord_APP_RUNNING,
	Rejected.String():   si.EventRecord_APP_REJECT,
	Completing.String(): si.EventRecord_APP_COMPLETING,
	Completed.String():  si.EventRecord_APP_COMPLETED,
	Failing.String():    si.EventRecord_APP_FAILING,
	Failed.String():     si.EventRecord_APP_FAILED,
	Resuming.String():   si.EventRecord_APP_RESUMING,
	Expired.String():    si.EventRecord_APP_EXPIRED,
}

func (as applicationState) String() string {
	return [...]string{"New", "Accepted", "Running", "Rejected", "Completing", "Completed", "Failing", "Failed", "Expired", "Resuming"}[as]
}

func eventDesc() fsm.Events {
	return fsm.Events{
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
			Src:  []string{Accepted.String(), Running.String(), Completing.String()},
			Dst:  Running.String(),
		}, {
			Name: CompleteApplication.String(),
			Src:  []string{Accepted.String(), Running.String()},
			Dst:  Completing.String(),
		}, {
			Name: CompleteApplication.String(),
			Src:  []string{Completing.String()},
			Dst:  Completed.String(),
		}, {
			Name: FailApplication.String(),
			Src:  []string{New.String(), Accepted.String(), Running.String()},
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
	}
}

// The state machine is tightly tied to the Application object.
//
// The first argument must always be an Application and if there is a second,
// that must be a string. If this precondition is not met, a runtime panic
// will occur.
func callbacks() fsm.Callbacks {
	return fsm.Callbacks{
		"enter_state": func(_ context.Context, event *fsm.Event) {
			app := event.Args[0].(*Application) //nolint:errcheck
			log.Log(log.SchedFSM).Info("Application state transition",
				zap.String("appID", app.ApplicationID),
				zap.String("source", event.Src),
				zap.String("destination", event.Dst),
				zap.String("event", event.Event))

			eventInfo := ""
			if len(event.Args) == 2 {
				eventInfo = event.Args[1].(string) //nolint:errcheck
				app.OnStateChange(event, eventInfo)
			} else {
				app.OnStateChange(event, "")
			}
			eventDetails, ok := stateEvents[event.Dst]
			if !ok {
				log.Log(log.SchedFSM).Error("event details not found",
					zap.String("state", event.Dst))
				return
			}
			if app.sendStateChangeEvents {
				app.appEvents.SendStateChangeEvent(app.ApplicationID, eventDetails, eventInfo)
			}
		},
		"leave_state": func(_ context.Context, event *fsm.Event) {
			event.Args[0].(*Application).clearStateTimer() //nolint:errcheck
		},
		fmt.Sprintf("enter_%s", Completing.String()): func(_ context.Context, event *fsm.Event) {
			app := event.Args[0].(*Application) //nolint:errcheck
			app.setStateTimer(completingTimeout, app.stateMachine.Current(), CompleteApplication)
		},
		fmt.Sprintf("leave_%s", New.String()): func(_ context.Context, event *fsm.Event) {
			if event.Dst != Rejected.String() {
				app := event.Args[0].(*Application) //nolint:errcheck
				metrics.GetQueueMetrics(app.queuePath).IncQueueApplicationsAccepted()
				metrics.GetSchedulerMetrics().IncTotalApplicationsAccepted()
			}
		},
		fmt.Sprintf("enter_%s", Rejected.String()): func(_ context.Context, event *fsm.Event) {
			app := event.Args[0].(*Application) //nolint:errcheck
			metrics.GetQueueMetrics(app.queuePath).IncQueueApplicationsRejected()
			metrics.GetSchedulerMetrics().IncTotalApplicationsRejected()
			app.setStateTimer(terminatedTimeout, app.stateMachine.Current(), ExpireApplication)
			app.finishedTime = time.Now()
			app.cleanupTrackedResource()
			// No rejected message when use app.HandleApplicationEvent(RejectApplication)
			if len(event.Args) == 2 {
				app.rejectedMessage = event.Args[1].(string) //nolint:errcheck
			}
		},
		fmt.Sprintf("enter_%s", Running.String()): func(_ context.Context, event *fsm.Event) {
			if event.Src != Running.String() {
				app := event.Args[0].(*Application) //nolint:errcheck
				app.startTime = time.Now()
				app.queue.incRunningApps(app.ApplicationID)
				metrics.GetQueueMetrics(app.queuePath).IncQueueApplicationsRunning()
				metrics.GetSchedulerMetrics().IncTotalApplicationsRunning()
			}
		},
		fmt.Sprintf("leave_%s", Running.String()): func(_ context.Context, event *fsm.Event) {
			if event.Dst != Running.String() {
				app := event.Args[0].(*Application) //nolint:errcheck
				app.queue.decRunningApps()
				metrics.GetQueueMetrics(app.queuePath).DecQueueApplicationsRunning()
				metrics.GetSchedulerMetrics().DecTotalApplicationsRunning()
			}
		},
		fmt.Sprintf("enter_%s", Completed.String()): func(_ context.Context, event *fsm.Event) {
			app := event.Args[0].(*Application) //nolint:errcheck
			metrics.GetSchedulerMetrics().IncTotalApplicationsCompleted()
			metrics.GetQueueMetrics(app.queuePath).IncQueueApplicationsCompleted()
			app.setStateTimer(terminatedTimeout, app.stateMachine.Current(), ExpireApplication)
			app.executeTerminatedCallback()
			app.clearPlaceholderTimer()
			app.cleanupAsks()
		},
		fmt.Sprintf("enter_%s", Failing.String()): func(_ context.Context, event *fsm.Event) {
			app := event.Args[0].(*Application) //nolint:errcheck
			metrics.GetQueueMetrics(app.queuePath).IncQueueApplicationsFailed()
			metrics.GetSchedulerMetrics().IncTotalApplicationsFailed()
		},
		fmt.Sprintf("enter_%s", Failed.String()): func(_ context.Context, event *fsm.Event) {
			app := event.Args[0].(*Application) //nolint:errcheck
			app.setStateTimer(terminatedTimeout, app.stateMachine.Current(), ExpireApplication)
			app.executeTerminatedCallback()
			app.cleanupAsks()
		},
	}
}

func NewAppState() *fsm.FSM {
	return fsm.NewFSM(New.String(), eventDesc(), callbacks())
}
