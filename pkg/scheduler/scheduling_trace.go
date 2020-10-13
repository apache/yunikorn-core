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

func startSpanWrapper(ctx trace.SchedulerTraceContext, level, phase, name string) {
	span, _ := ctx.StartSpan(fmt.Sprintf("[%v]%v", level, phase))
	span.SetTag(LevelKey, level).
		SetTag(PhaseKey, phase).
		SetTag(NameKey, name)
}

var _ trace.TagsBuilder = &TagsBuilder{}

type TagsBuilder struct {
	//tags map[string]interface{}
	level string
	phase string
	name  string
	info  string
	state string
}

func NewTagsBuilder() *TagsBuilder {
	return &TagsBuilder{}
}

//func (tb *TagsBuilder) with(key string, value string) *TagsBuilder {
//	tb.tags[key] = value
//	return tb
//}

func (tb *TagsBuilder) withLevel(level string) *TagsBuilder {
	tb.level = level
	return tb
}

func (tb *TagsBuilder) withPhase(phase string) *TagsBuilder {
	tb.phase = phase
	return tb
}

func (tb *TagsBuilder) withName(name string) *TagsBuilder {
	tb.name = name
	return tb
}

func (tb *TagsBuilder) withInfo(info string) *TagsBuilder {
	tb.info = info
	return tb
}

func (tb *TagsBuilder) withError(err error) *TagsBuilder {
	if err != nil {
		tb.info = err.Error()
	}
	return tb
}

func (tb *TagsBuilder) withState(state string) *TagsBuilder {
	tb.state = state
	return tb
}

func (tb *TagsBuilder) Build() map[string]interface{} {
	tags := make(map[string]interface{})
	if tb.level != "" {
		tags[LevelKey] = tb.level
	}
	if tb.phase != "" {
		tags[PhaseKey] = tb.phase
	}
	if tb.name != "" {
		tags[NameKey] = tb.name
	}
	if tb.info != "" {
		tags[InfoKey] = tb.info
	}
	if tb.state != "" {
		tags[StateKey] = tb.state
	}
	return tags
}
