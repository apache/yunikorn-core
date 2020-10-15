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
	"io"
	"sync"

	"github.com/opentracing/opentracing-go"
)

type SchedulerTracer interface {
	NewTraceContext() SchedulerTraceContext
	Close()
}

var _ SchedulerTracer = &SchedulerTracerImpl{}

const (
	Sampling = "Sampling"
	OnDemand = "OnDemand"
)

type SchedulerTracerImplParams struct {
	Mode       string
	FilterTags map[string]interface{}
}

var DefaultSchedulerTracerImplParams = &SchedulerTracerImplParams{
	Mode:       Sampling,
	FilterTags: nil,
}

type SchedulerTracerImpl struct {
	Tracer opentracing.Tracer
	Closer io.Closer
	sync.RWMutex
	*SchedulerTracerImplParams
}

func (s *SchedulerTracerImpl) NewTraceContext() SchedulerTraceContext {
	s.RLock()
	defer s.RUnlock()
	switch s.Mode {
	case Sampling:
		return &SchedulerTraceContextImpl{
			Tracer:       s.Tracer,
			SpanStack:    []opentracing.Span{},
			OnDemandFlag: false,
		}
	case OnDemand:
		if len(s.FilterTags) == 0 {
			return &SchedulerTraceContextImpl{
				Tracer:       s.Tracer,
				SpanStack:    []opentracing.Span{},
				OnDemandFlag: true,
			}
		} else {
			return &DelaySchedulerTraceContextImpl{
				Tracer:     s.Tracer,
				SpanStack:  []*DelaySpan{},
				Spans:      []*DelaySpan{},
				FilterTags: s.FilterTags,
			}
		}
	default:
		return nil
	}
}

func (s *SchedulerTracerImpl) SetParams(params *SchedulerTracerImplParams) {
	if params == nil {
		return
	}
	s.Lock()
	defer s.Unlock()
	s.SchedulerTracerImplParams = params
}

func (s *SchedulerTracerImpl) Close() {
	if s.Closer != nil {
		s.Closer.Close()
	}
}

func NewSchedulerTracer(params *SchedulerTracerImplParams) (SchedulerTracer, error) {
	if params == nil {
		params = DefaultSchedulerTracerImplParams
	}

	tracer, closer, err := NewTracerFromEnv("yunikorn-core-scheduler")
	if err != nil {
		return nil, err
	}

	return &SchedulerTracerImpl{
		Tracer:                    tracer,
		Closer:                    closer,
		SchedulerTracerImplParams: params,
	}, nil
}
