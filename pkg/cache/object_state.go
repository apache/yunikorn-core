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
    "github.com/golang/glog"
    "github.com/looplab/fsm"
)

// ----------------------------------
// object events
// these events are used for: partitions and managed queues
// ----------------------------------
type SchedulingObjectEvent int

const (
    Remove SchedulingObjectEvent = iota
    Start
    Stop
)

func (soe SchedulingObjectEvent) String() string {
    return [...]string{"Remove", "Start", "Stop"}[soe]
}

// ----------------------------------
// object states
// these states are used by: partitions and managed queues
// ----------------------------------
type SchedulingObjectState int

const (
    Active SchedulingObjectState = iota
    Draining
    Stopped
)

func (sos SchedulingObjectState) String() string {
    return [...]string{"Active", "Draining", "Stopped"}[sos]
}

func newObjectState() *fsm.FSM {
    return fsm.NewFSM(
        Active.String(), fsm.Events{
            {
                Name: Remove.String(),
                Src: []string{Active.String(), Draining.String()},
                Dst: Draining.String(),
            },{
                Name: Start.String(),
                Src: []string{Active.String(), Stopped.String()},
                Dst: Active.String(),
            },{
                Name: Stop.String(),
                Src: []string{Active.String(), Stopped.String()},
                Dst: Stopped.String(),
            },
        },
        fsm.Callbacks{
            "enter_state": func(event *fsm.Event) {
                glog.V(0).Infof(
                    "Object %s transitioned to state %s from %s, on event %s",
                    event.Args[0], event.Dst, event.Src, event.Event)
            },
        },
    )
}
