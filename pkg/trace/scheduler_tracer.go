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
	"io"
	"sync"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
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

	TryReservedAllocatePhase    = "tryReservedAllocate"
	TryPlaceholderAllocatePhase = "tryPlaceholderAllocate"
	TryAllocatePhase            = "tryAllocate"
	SortQueuesPhase             = "sortQueues"
	SortAppsPhase               = "sortApps"
	SortRequestsPhase           = "sortRequests"
	AllocatePhase               = "allocate"
	ReservePhase                = "reserve"
	UnReservePhase              = "unReserve"

	SkipState = "skip"

	NoMaxResourceInfo              = "max resource is nil"
	StoppedInfo                    = "resource is stopped"
	NoPendingRequestInfo           = "no pending request left"
	BeyondQueueHeadroomInfo        = "beyond queue headroom: headroom=%v, req=%v"
	RequestBeyondTotalResourceInfo = "request resource beyond total resource of node: req=%v"
	NodeAlreadyReservedInfo        = "node has already been reserved"
)

type SchedulerTracerBase struct {
	context Context
	sync.Mutex
}

func (s *SchedulerTracerBase) Context() Context {
	return s.context
}

func (s *SchedulerTracerBase) ActiveSpan() opentracing.Span {
	span, err := s.context.ActiveSpan()
	if err != nil {
		log.Logger().Error("getting active span fail", zap.Error(err))
		return noopSpan
	}
	return span
}

// startSpan simplifies span starting process by integrating general tags' setting.
// The level tag is required, nonempty and logs span's scheduling level. (root, partition, queue, ...)
// The phase tag is optional and logs span's calling phase. (reservedAllocate, tryAllocate, allocate, ...)
// The name tag is optional and logs span's related object's identity. (resources' name or ID)
// These tags can be decided when starting the span because they don't depend on the calling result.
// Logs or special tags can be set with the returned span object.
// It shares the restriction on trace.Context that we should start and finish span in pairs, like this:
//  span, _ := startSpanWrapper(ctx, "root", "", "")
//  defer finishActiveSpanWrapper(ctx)
//  ...
//  span.SetTag("foo", "bar") // if we have irregular tags to set
//  ...
func (s *SchedulerTracerBase) StartSpan(level, phase, name string) opentracing.Span {
	if level == "" {
		log.Logger().Error("level field cannot be empty")
		return noopSpan
	}
	span, err := s.context.StartSpan(fmt.Sprintf("[%v]%v", level, phase))
	if err != nil {
		log.Logger().Error("starting span fail", zap.Error(err))
		return noopSpan
	}
	span.SetTag(LevelKey, level)
	if phase != "" {
		span.SetTag(PhaseKey, phase)
	}
	if name != "" {
		span.SetTag(NameKey, name)
	}
	return span
}

// finishActiveSpan simplifies span finishing process.
func (s *SchedulerTracerBase) FinishActiveSpan() {
	err := s.context.FinishActiveSpan()
	if err != nil {
		log.Logger().Error("finishing active span fail", zap.Error(err))
	}
}

// FinishActiveSpanWithState simplifies span finishing process by integrating result tags' setting.
// The state tag is optional and logs span's calling result. (skip, allocated, reserved, ...)
// The info tag is optional and logs span's result message. (errors or hints for the state)
// These general tags depend on the calling result so they can be integrated with the finishing process
func (s *SchedulerTracerBase) FinishActiveSpanWithState(state, info string) {
	span, err := s.context.ActiveSpan()
	if err != nil {
		log.Logger().Error("getting active span fail when finish active span", zap.Error(err))
		return
	}
	if state != "" {
		span.SetTag(StateKey, state)
	}
	if info != "" {
		span.SetTag(InfoKey, info)
	}
	err = s.context.FinishActiveSpan()
	if err != nil {
		log.Logger().Error("finishing active span fail", zap.Error(err))
	}
}

// SchedulerTracer defines minimum interface for tracing
type SchedulerTracer interface {
	Close() error
	Lock()
	Unlock()
	Context() Context
	InitContext() error
	ActiveSpan() opentracing.Span
	StartSpan(level, phase, name string) opentracing.Span
	FinishActiveSpan()
	FinishActiveSpanWithState(state, info string)
}

var _ SchedulerTracer = &NoopSchedulerTracerImpl{}

type NoopSchedulerTracerImpl struct {
	*SchedulerTracerBase
}

func (n *NoopSchedulerTracerImpl) Close() error {
	return nil
}

func (n *NoopSchedulerTracerImpl) InitContext() error {
	n.SchedulerTracerBase.context = &NoopContextImpl{}
	return nil
}

var _ SchedulerTracer = &SchedulerTracerImpl{}

type SchedulerTracerImpl struct {
	*SchedulerTracerBase
	Tracer      opentracing.Tracer
	Closer      io.Closer
	paramsMutex sync.RWMutex
	*SchedulerTracerImplParams
}

type SchedulerTracerImplParams struct {
	Mode       string
	FilterTags map[string]interface{}
}

const (
	Sampling        = "Sampling"
	Debug           = "Debug"
	DebugWithFilter = "DebugWithFilter"
)

var DefaultSchedulerTracerImplParams = &SchedulerTracerImplParams{
	Mode:       Sampling,
	FilterTags: nil,
}

// SetParams set runtime parameter for tracer
func (s *SchedulerTracerImpl) SetParams(params *SchedulerTracerImplParams) {
	if params == nil {
		return
	}
	if params.Mode == DebugWithFilter && len(params.FilterTags) == 0 {
		log.Logger().Warn("FilterTags is empty while trying to run in DebugWithFilter mode." +
			" Please use Debug mode instead.")
	}
	s.paramsMutex.Lock()
	defer s.paramsMutex.Unlock()
	s.SchedulerTracerImplParams = params
}

// Close calls tracer's closer if exists
func (s *SchedulerTracerImpl) Close() error {
	return s.Closer.Close()
}

// InitTraceContext create Context based on parameter settings
func (s *SchedulerTracerImpl) InitContext() error {
	s.paramsMutex.RLock()
	defer s.paramsMutex.RUnlock()
	switch s.Mode {
	case Sampling:
		s.context = &ContextImpl{
			Tracer:       s.Tracer,
			SpanStack:    []opentracing.Span{},
			OnDemandFlag: false,
		}
	case Debug:
		s.context = &ContextImpl{
			Tracer:       s.Tracer,
			SpanStack:    []opentracing.Span{},
			OnDemandFlag: true,
		}
	case DebugWithFilter:
		s.context = &DelayContextImpl{
			Tracer:     s.Tracer,
			Spans:      []*DelaySpan{},
			FilterTags: s.FilterTags,
		}
	default:
		s.context = &NoopContextImpl{}
		return fmt.Errorf("error mode code")
	}
	return nil
}

// NewSchedulerTracer creates new tracer instance with params
// params is set to default sampling mode if it is nil
func NewSchedulerTracer(params *SchedulerTracerImplParams) (SchedulerTracer, error) {
	if params == nil {
		params = DefaultSchedulerTracerImplParams
	}

	tracer, closer, err := NewTracerFromEnv("yunikorn-core-scheduler")
	if err != nil {
		return nil, err
	}

	return &SchedulerTracerImpl{
		SchedulerTracerBase: &SchedulerTracerBase{
			context: &NoopContextImpl{},
		},
		Tracer:                    tracer,
		Closer:                    closer,
		SchedulerTracerImplParams: params,
	}, nil
}

var globalSchedulerTracer SchedulerTracer
var once sync.Once

func GlobalSchedulerTracer() SchedulerTracer {
	once.Do(func() {
		if globalSchedulerTracer == nil {
			var err error
			globalSchedulerTracer, err = NewSchedulerTracer(nil)
			if err != nil {
				log.Logger().Error("Tracing disabled, tracer init failed", zap.Error(err))
				globalSchedulerTracer = &NoopSchedulerTracerImpl{}
			}
		}
	})
	return globalSchedulerTracer
}
