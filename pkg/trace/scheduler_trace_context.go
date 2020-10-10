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
	"github.com/opentracing/opentracing-go/ext"
)

type SchedulerTraceContext interface {
	StartSpan(name string) (opentracing.Span, error)
	ActiveSpan() (opentracing.Span, error)
	FinishActiveSpan() error
}

var _ SchedulerTraceContext = &SchedulerTraceContextImpl{}

type SchedulerTraceContextImpl struct {
	Tracer       opentracing.Tracer
	Spans        []opentracing.Span
	OnDemandFlag bool
}

func (s *SchedulerTraceContextImpl) ActiveSpan() (opentracing.Span, error) {
	if len(s.Spans) == 0 {
		return nil, fmt.Errorf("not found")
	} else {
		return s.Spans[len(s.Spans)-1], nil
	}
}

func (s *SchedulerTraceContextImpl) StartSpan(operationName string) (opentracing.Span, error) {
	var newSpan opentracing.Span
	if span, err := s.ActiveSpan(); err != nil {
		newSpan = s.Tracer.StartSpan(operationName)
		if s.OnDemandFlag {
			ext.SamplingPriority.Set(newSpan, 1)
		}
	} else {
		newSpan = s.Tracer.StartSpan(operationName, opentracing.ChildOf(span.Context()))
	}
	s.Spans = append(s.Spans, newSpan)
	return newSpan, nil
}

func (s *SchedulerTraceContextImpl) FinishActiveSpan() error {
	if span, err := s.ActiveSpan(); err != nil {
		return err
	} else {
		span.Finish()
		s.Spans = s.Spans[:len(s.Spans)-1]
		return nil
	}
}
