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

	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
)

// ----------------------------------
// object events
// these events are used for: partitions and managed queues
// ----------------------------------
type ObjectEvent int

const (
	Remove ObjectEvent = iota
	Start
	Stop
)

func (oe ObjectEvent) String() string {
	return [...]string{"Remove", "Start", "Stop"}[oe]
}

// ----------------------------------
// object states
// these states are used by: partitions and managed queues
// ----------------------------------
type ObjectState int

const (
	Active ObjectState = iota
	Draining
	Stopped
)

func (os ObjectState) String() string {
	return [...]string{"Active", "Draining", "Stopped"}[os]
}

func NewObjectState() *fsm.FSM {
	return fsm.NewFSM(
		Active.String(), fsm.Events{
			{
				Name: Remove.String(),
				Src:  []string{Active.String(), Draining.String()},
				Dst:  Draining.String(),
			}, {
				Name: Start.String(),
				Src:  []string{Active.String(), Stopped.String(), Draining.String()},
				Dst:  Active.String(),
			}, {
				Name: Stop.String(),
				Src:  []string{Active.String(), Stopped.String()},
				Dst:  Stopped.String(),
			},
		},
		fsm.Callbacks{
			"enter_state": func(_ context.Context, event *fsm.Event) {
				log.Log(log.SchedFSM).Info("object transition",
					zap.Any("object", event.Args[0]),
					zap.String("source", event.Src),
					zap.String("destination", event.Dst),
					zap.String("event", event.Event))
			},
		},
	)
}
