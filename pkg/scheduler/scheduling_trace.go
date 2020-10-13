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

package scheduler

import (
	"fmt"
	"github.com/opentracing/opentracing-go"

	"github.com/apache/incubator-yunikorn-core/pkg/trace"
)

const (
	RootLevel      = "root"
	PartitionLevel = "partition"
	QueueLevel     = "queue"
	AppLevel       = "app"
	RequestLevel   = "request"
	NodesLevel     = "nodes"
	NodeLevel      = "node"

	ReservedAllocatePhase = "reservedAllocate"
	TryAllocatePhase      = "tryAllocate"
	AllocatePhase         = "allocate"
	SortQueuesPhase       = "sortQueues"
	SortAppsPhase         = "sortApps"
	SortRequestsPhase     = "sortRequests"

	LevelKey = "level"
	PhaseKey = "phase"
	NameKey  = "name"
	StateKey = "state"
	InfoKey  = "info"

	SkipState = "skip"

	NoMaxResourceInfo              = "max resource is nil"
	NoPendingRequestInfo           = "no pending request left"
	BeyondQueueHeadroomInfo        = "beyond queue headroom: headroom=%v, req=%v"
	RequestBeyondTotalResourceInfo = "request resource beyond total resource of node: req=%v"
	NodeAlreadyReservedInfo        = "node has already been reserved"
)

func startSpanWrapper(ctx trace.SchedulerTraceContext, level, phase, name string) (opentracing.Span, error) {
	if len(level) == 0 {
		return nil, fmt.Errorf("level field cannot be empty")
	}
	span, err := ctx.StartSpan(fmt.Sprintf("[%v]%v", level, phase))
	if err == nil {
		span.SetTag(LevelKey, level)
		if len(phase) != 0 {
			span.SetTag(PhaseKey, phase)
		}
		if len(name) != 0 {
			span.SetTag(NameKey, name)
		}
	}
	return span, err
}

func finishActiveSpanWrapper(ctx trace.SchedulerTraceContext, state, info string) error {
	span, err := ctx.ActiveSpan()
	if err == nil {
		if len(state) != 0 {
			span.SetTag(StateKey, state)
		}
		if len(info) != 0 {
			span.SetTag(InfoKey, info)
		}
		ctx.FinishActiveSpan()
	}
	return err
}
