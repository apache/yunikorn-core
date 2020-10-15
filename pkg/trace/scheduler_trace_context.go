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
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/uber/jaeger-client-go"
)

// SchedulerTraceContext manages spans for one trace.
// it only designs for the scheduling process so we keeps the interface simple
type SchedulerTraceContext interface {
	// ActiveSpan returns current active (latest unfinished) span in this context object.
	// Error returns if there doesn't exist an unfinished span.
	ActiveSpan() (opentracing.Span, error)

	// StartSpan creates and starts a new span based on the context state with the operationName parameter.
	// The new span is the child of current active span if it exists.
	// Or the new span will become the root span of this trace.
	StartSpan(operationName string) (opentracing.Span, error)

	// FinishActiveSpan finishes current active span and set its parent as active if exists.
	// Error returns if there doesn't exist an unfinished span.
	FinishActiveSpan() error
}

var _ SchedulerTraceContext = &SchedulerTraceContextImpl{}

// SchedulerTraceContextImpl reports the spans to tracer once they are finished.
// Root span's "sampling.priority" tag will be set to 1 to force reporting all spans if OnDemandFlag is true.
type SchedulerTraceContextImpl struct {
	Tracer       opentracing.Tracer
	SpanStack    []opentracing.Span
	OnDemandFlag bool
}

func (s *SchedulerTraceContextImpl) ActiveSpan() (opentracing.Span, error) {
	if len(s.SpanStack) == 0 {
		return nil, fmt.Errorf("not found")
	} else {
		return s.SpanStack[len(s.SpanStack)-1], nil
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
	s.SpanStack = append(s.SpanStack, newSpan)
	return newSpan, nil
}

func (s *SchedulerTraceContextImpl) FinishActiveSpan() error {
	if span, err := s.ActiveSpan(); err != nil {
		return err
	} else {
		span.Finish()
		s.SpanStack = s.SpanStack[:len(s.SpanStack)-1]
		return nil
	}
}

var _ opentracing.Span = &DelaySpan{}

// DelaySpan implements the opentracing.Span interface.
// It will set the FinishTime field and delay reporting when finished.
type DelaySpan struct {
	opentracing.Span
	FinishTime time.Time
}

// Finish implements the opentracing.Span interface and panics when calling.
func (d *DelaySpan) Finish() {
	panic("should not call it")
}

// FinishWithOptions implements the opentracing.Span interface and panics when calling.
func (d *DelaySpan) FinishWithOptions(opentracing.FinishOptions) {
	panic("should not call it")
}

var _ SchedulerTraceContext = &DelaySchedulerTraceContextImpl{}

// DelaySchedulerTraceContextImpl delays reporting spans
// and chooses whether to report based on FilterTags when the entire trace is collected.
type DelaySchedulerTraceContextImpl struct {
	Tracer     opentracing.Tracer
	SpanStack  []*DelaySpan
	Spans      []*DelaySpan
	FilterTags map[string]interface{}
}

func (d *DelaySchedulerTraceContextImpl) ActiveSpan() (opentracing.Span, error) {
	if len(d.SpanStack) == 0 {
		return nil, fmt.Errorf("not found")
	} else {
		return d.SpanStack[len(d.SpanStack)-1], nil
	}
}

func (d *DelaySchedulerTraceContextImpl) StartSpan(operationName string) (opentracing.Span, error) {
	var newSpan *DelaySpan
	if span, err := d.ActiveSpan(); err != nil {
		newSpan = &DelaySpan{
			Span: d.Tracer.StartSpan(operationName),
		}
		ext.SamplingPriority.Set(newSpan, 1)
	} else {
		newSpan = &DelaySpan{
			Span: d.Tracer.StartSpan(operationName, opentracing.ChildOf(span.Context())),
		}
	}
	d.SpanStack = append(d.SpanStack, newSpan)
	d.Spans = append(d.Spans, newSpan)
	return newSpan, nil
}

// FinishActiveSpan finishes current active span by setting its FinishTime
// and pop it from the unfinished span stack.
func (d *DelaySchedulerTraceContextImpl) FinishActiveSpan() error {
	if _, err := d.ActiveSpan(); err != nil {
		return err
	}
	span := d.SpanStack[len(d.SpanStack)-1]
	span.FinishTime = time.Now()
	d.SpanStack = d.SpanStack[:len(d.SpanStack)-1]

	if len(d.SpanStack) == 0 {
		if d.isMatch() {
			for _, span := range d.Spans {
				span.Span.FinishWithOptions(opentracing.FinishOptions{
					FinishTime: span.FinishTime,
				})
			}
		}
		d.Spans = []*DelaySpan{}
	}

	return nil
}

// isMatch checks whether there is a span in the trace that matches the FilterTags.
func (d *DelaySchedulerTraceContextImpl) isMatch() bool {
	if len(d.FilterTags) == 0 {
		return false
	}
	for _, span := range d.Spans {
		tags := span.Span.(*jaeger.Span).Tags()
		MatchFlag := true
		for k, v := range d.FilterTags {
			if tag, ok := tags[k]; !ok || tag != v {
				MatchFlag = false
				break
			}
		}
		if MatchFlag {
			return true
		}
	}
	return false
}
