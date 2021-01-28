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

package trace

import (
	"fmt"

	"github.com/opentracing/opentracing-go"
)

const (
	LevelKey = "level"
	PhaseKey = "phase"
	NameKey  = "name"
	StateKey = "state"
	InfoKey  = "info"

	RootLevel      = "root"
	PartitionLevel = "partition"
	QueueLevel     = "queue"
	AppLevel       = "app"
	RequestLevel   = "request"
	NodesLevel     = "nodes"
	NodeLevel      = "node"

	TryReservedAllocatePhase = "tryReservedAllocate"
	TryPlaceholderAllocatePhase = "tryPlaceholderAllocate"
	TryAllocatePhase      = "tryAllocate"
	//AllocatePhase         = "allocate"
	SortQueuesPhase       = "sortQueues"
	SortAppsPhase         = "sortApps"
	SortRequestsPhase     = "sortRequests"

	SkipState = "skip"

	NoMaxResourceInfo              = "max resource is nil"
	StoppedInfo                    = "resource is stopped"
	NoPendingRequestInfo           = "no pending request left"
	BeyondQueueHeadroomInfo        = "beyond queue headroom: headroom=%v, req=%v"
	RequestBeyondTotalResourceInfo = "request resource beyond total resource of node: req=%v"
	NodeAlreadyReservedInfo        = "node has already been reserved"
)

// startSpanWrapper simplifies span starting process by integrating general tags' setting.
// The level tag is required, nonempty and logs span's scheduling level. (root, partition, queue, ...)
// The phase tag is optional and logs span's calling phase. (reservedAllocate, tryAllocate, allocate, ...)
// The name tag is optional and logs span's related object's identity. (resources' name or ID)
// These tags can be decided when starting the span because they don't depend on the calling result.
// Logs or special tags can be set with the returned span object.
// It shares the restriction on trace.SchedulerTraceContext that we should start and finish span in pairs, like this:
//  span, _ := startSpanWrapper(ctx, "root", "", "")
//  defer finishActiveSpanWrapper(ctx)
//  ...
//  span.SetTag("foo", "bar") // if we have irregular tags to set
//  ...
func StartSpanWrapper(ctx SchedulerTraceContext, level, phase, name string) (opentracing.Span, error) {
	if ctx == nil {
		return opentracing.NoopTracer{}.StartSpan(""), nil
	}
	if level == "" {
		return opentracing.NoopTracer{}.StartSpan(""),
			fmt.Errorf("level field cannot be empty")
	}

	span, err := ctx.StartSpan(fmt.Sprintf("[%v]%v", level, phase))
	if err == nil {
		span.SetTag(LevelKey, level)
		if phase != "" {
			span.SetTag(PhaseKey, phase)
		}
		if name != "" {
			span.SetTag(NameKey, name)
		}
	}
	return span, err
}

// finishActiveSpanWrapper simplifies span finishing process by integrating result tags' setting.
// The state tag is optional and logs span's calling result. (skip, allocated, reserved, ...)
// The info tag is optional and logs span's result message. (errors or hints for the state)
// These general tags depend on the calling result so they can be integrated with the finishing process
func FinishActiveSpanWrapper(ctx SchedulerTraceContext, state, info string) error {
	if ctx == nil {
		return nil
	}

	span, err := ctx.ActiveSpan()
	if err == nil {
		if state != "" {
			span.SetTag(StateKey, state)
		}
		if info != "" {
			span.SetTag(InfoKey, info)
		}
		return ctx.FinishActiveSpan()
	}
	return err
}
