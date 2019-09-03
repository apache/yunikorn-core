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

package cache

import (
	"github.com/cloudera/yunikorn-core/pkg/log"
	"github.com/looplab/fsm"
	"go.uber.org/zap"
)

// ----------------------------------
// application events
// ----------------------------------
type ApplicationEvent int

const (
	AcceptApplication ApplicationEvent = iota
	RejectApplication
	RunApplication
	CompleteApplication
	KillApplication
)

func (ae ApplicationEvent) String() string {
	return [...]string{"AcceptApplication", "RejectApplication", "RunApplication", "CompleteApplication", "KillApplication"}[ae]
}

// ----------------------------------
// application states
// ----------------------------------
type ApplicationState int

const (
	New ApplicationState = iota
	Accepted
	Rejected
	Running
	Completed
	Killed
)

func (as ApplicationState) String() string {
	return [...]string{"New", "Accepted", "Rejected", "Running", "Completed", "Killed"}[as]
}

func newAppState() *fsm.FSM {
	return fsm.NewFSM(
		New.String(), fsm.Events{
			{
				Name: AcceptApplication.String(),
				Src: []string{New.String()},
				Dst: Accepted.String(),
			},{
				Name: RejectApplication.String(),
				Src: []string{New.String()},
				Dst: Rejected.String(),
			},{
				Name: RunApplication.String(),
				Src: []string{Accepted.String(), Running.String()},
				Dst: Running.String(),
			},{
				Name: CompleteApplication.String(),
				Src: []string{Running.String()},
				Dst: Completed.String(),
			},{
				Name: KillApplication.String(),
				Src: []string{New.String(), Accepted.String(), Running.String(), Killed.String()},
				Dst: Killed.String(),
			},
		},
		fsm.Callbacks{
			"enter_state": func(event *fsm.Event) {
				log.Logger().Debug("app state transition",
					zap.Any("app", event.Args[0]),
					zap.String("source", event.Src),
					zap.String("destination", event.Dst),
					zap.String("event", event.Event))
			},
		},
	)
}
