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
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"gotest.tools/v3/assert"
)

func TestSchedulerTraceContextImpl(t *testing.T) {
	closeTracer, closer1, err := NewConstTracer("close-tracer", false)
	assert.NilError(t, err)
	defer closer1.Close()
	openTracer, closer2, err := NewConstTracer("open-tracer", true)
	assert.NilError(t, err)
	defer closer2.Close()

	type fields struct {
		Tracer       opentracing.Tracer
		SpanStack    []opentracing.Span
		OnDemandFlag bool
	}

	tests := []struct {
		name           string
		fields         fields
		wantDebugFlag  bool
		wantSampleFlag bool
	}{
		{
			name: "Sampling_Open",
			fields: fields{
				Tracer:       openTracer,
				SpanStack:    []opentracing.Span{},
				OnDemandFlag: false,
			},
			wantDebugFlag:  false,
			wantSampleFlag: true,
		},
		{
			name: "Sampling_Close",
			fields: fields{
				Tracer:       closeTracer,
				SpanStack:    []opentracing.Span{},
				OnDemandFlag: false,
			},
			wantDebugFlag:  false,
			wantSampleFlag: false,
		},
		{
			name: "OnDemand_Open",
			fields: fields{
				Tracer:       openTracer,
				SpanStack:    []opentracing.Span{},
				OnDemandFlag: true,
			},
			wantDebugFlag:  true,
			wantSampleFlag: true,
		},
		{
			name: "OnDemand_Close",
			fields: fields{
				Tracer:       closeTracer,
				SpanStack:    []opentracing.Span{},
				OnDemandFlag: true,
			},
			wantDebugFlag:  true,
			wantSampleFlag: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SchedulerTraceContextImpl{
				Tracer:       tt.fields.Tracer,
				SpanStack:    tt.fields.SpanStack,
				OnDemandFlag: tt.fields.OnDemandFlag,
			}
			rootSpan, err := s.StartSpan("root")
			assert.Equal(t, rootSpan.(*jaeger.Span).SpanContext().IsDebug(), tt.wantDebugFlag)
			assert.NilError(t, err)
			_, err = s.StartSpan("child")
			assert.NilError(t, err)
			assert.Equal(t, len(s.SpanStack), 2)
			span, err := s.ActiveSpan()
			assert.NilError(t, err)
			assert.Equal(t, span.(*jaeger.Span).OperationName(), "child")
			err = s.FinishActiveSpan()
			assert.NilError(t, err)
			span, err = s.ActiveSpan()
			assert.NilError(t, err)
			assert.Equal(t, span.(*jaeger.Span).OperationName(), "root")
			err = s.FinishActiveSpan()
			assert.NilError(t, err)
			err = s.FinishActiveSpan()
			assert.Assert(t, err != nil)
		})
	}
}

func TestDelaySpan_ForbiddenFunctions(t *testing.T) {
	type fields struct {
		Span       opentracing.Span
		FinishTime time.Time
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "panic",
			fields: struct {
				Span       opentracing.Span
				FinishTime time.Time
			}{
				Span:       nil,
				FinishTime: time.Time{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("don't cause a panic")
				}
			}()
			d := &DelaySpan{
				Span:       tt.fields.Span,
				FinishTime: tt.fields.FinishTime,
			}
			d.Finish()
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("don't cause a panic")
				}
			}()
			d := &DelaySpan{
				Span:       tt.fields.Span,
				FinishTime: tt.fields.FinishTime,
			}
			d.FinishWithOptions(opentracing.FinishOptions{})
		})
	}
}

func TestDelaySchedulerTraceContextImpl(t *testing.T) {
	tracer, closer, err := NewConstTracer("test-tracer", true)
	assert.NilError(t, err)
	defer closer.Close()

	type fields struct {
		Tracer     opentracing.Tracer
		Spans      []*DelaySpan
		FilterTags map[string]interface{}
	}

	tests := []struct {
		name           string
		fields         fields
		wantDebugFlag  bool
		wantSampleFlag bool
	}{
		{
			name: "Match",
			fields: fields{
				Tracer: tracer,
				Spans:  []*DelaySpan{},
				FilterTags: map[string]interface{}{
					"organ": "stem",
				},
			},
		},
		{
			name: "NotMatch",
			fields: fields{
				Tracer: tracer,
				Spans:  []*DelaySpan{},
				FilterTags: map[string]interface{}{
					"organ": "root",
					"color": "green",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DelaySchedulerTraceContextImpl{
				Tracer:     tt.fields.Tracer,
				Spans:      tt.fields.Spans,
				FilterTags: tt.fields.FilterTags,
			}
			// foobar root in
			span, err := d.StartSpan("foobar")
			assert.NilError(t, err)
			span.SetTag("organ", "root").
				SetTag("color", "black").
				SetTag("depth", 0)
			assert.Equal(t, d.StackLen, 1)
			assert.Equal(t, len(d.Spans), 1)

			// foo stem in
			span, err = d.StartSpan("foo")
			assert.NilError(t, err)
			span.SetTag("organ", "stem").
				SetTag("color", "green").
				SetTag("depth", 1)
			assert.Equal(t, d.StackLen, 2)
			assert.Equal(t, len(d.Spans), 2)

			// foo stem out
			err = d.FinishActiveSpan()
			assert.NilError(t, err)
			assert.Equal(t, d.StackLen, 1)
			assert.Equal(t, len(d.Spans), 2)

			// bar stem in
			span, err = d.StartSpan("bar")
			assert.NilError(t, err)
			span.SetTag("organ", "stem").
				SetTag("color", "brown").
				SetTag("depth", 1)
			assert.Equal(t, d.StackLen, 2)
			assert.Equal(t, len(d.Spans), 3)

			// b leaf in
			span, err = d.StartSpan("b")
			assert.NilError(t, err)
			span.SetTag("organ", "leaf").
				SetTag("color", "green").
				SetTag("depth", 2)
			assert.Equal(t, d.StackLen, 3)
			assert.Equal(t, len(d.Spans), 4)

			// b leaf out
			err = d.FinishActiveSpan()
			assert.NilError(t, err)
			assert.Equal(t, d.StackLen, 2)
			assert.Equal(t, len(d.Spans), 4)

			// bar stem out
			err = d.FinishActiveSpan()
			assert.NilError(t, err)
			assert.Equal(t, d.StackLen, 1)
			assert.Equal(t, len(d.Spans), 4)

			// foobar root out
			err = d.FinishActiveSpan()
			assert.NilError(t, err)
			assert.Equal(t, d.StackLen, 0)
			assert.Equal(t, len(d.Spans), 0)

			// err finish
			err = d.FinishActiveSpan()
			assert.Assert(t, err != nil)
		})
	}
}

func TestDelaySchedulerTraceContextImpl_isMatch(t *testing.T) {
	closeTracer, closer1, err := NewConstTracer("close-tracer", false)
	assert.NilError(t, err)
	defer closer1.Close()
	openTracer, closer2, err := NewConstTracer("open-tracer", true)
	assert.NilError(t, err)
	defer closer2.Close()

	closeSpans := []*DelaySpan{
		{Span: closeTracer.StartSpan("foobar",
			opentracing.Tag{Key: "organ", Value: "root"},
			opentracing.Tag{Key: "color", Value: "black"},
			opentracing.Tag{Key: "depth", Value: "0"})},
		{Span: closeTracer.StartSpan("foo",
			opentracing.Tag{Key: "organ", Value: "stem"},
			opentracing.Tag{Key: "color", Value: "green"},
			opentracing.Tag{Key: "depth", Value: "1"})},
		{Span: closeTracer.StartSpan("bar",
			opentracing.Tag{Key: "organ", Value: "stem"},
			opentracing.Tag{Key: "color", Value: "brown"},
			opentracing.Tag{Key: "depth", Value: "1"})},
		{Span: closeTracer.StartSpan("b",
			opentracing.Tag{Key: "organ", Value: "leaf"},
			opentracing.Tag{Key: "color", Value: "green"},
			opentracing.Tag{Key: "depth", Value: "2"})},
		{Span: closeTracer.StartSpan("a",
			opentracing.Tag{Key: "organ", Value: "leaf"},
			opentracing.Tag{Key: "color", Value: "yellow"},
			opentracing.Tag{Key: "depth", Value: "2"})},
		{Span: closeTracer.StartSpan("r",
			opentracing.Tag{Key: "organ", Value: "leaf"},
			opentracing.Tag{Key: "color", Value: "green"},
			opentracing.Tag{Key: "depth", Value: "2"})},
	}
	openSpans := []*DelaySpan{
		{Span: openTracer.StartSpan("foobar",
			opentracing.Tag{Key: "organ", Value: "root"},
			opentracing.Tag{Key: "color", Value: "black"})},
		{Span: openTracer.StartSpan("foo",
			opentracing.Tag{Key: "organ", Value: "stem"},
			opentracing.Tag{Key: "color", Value: "green"})},
		{Span: openTracer.StartSpan("bar",
			opentracing.Tag{Key: "organ", Value: "stem"},
			opentracing.Tag{Key: "color", Value: "brown"})},
		{Span: openTracer.StartSpan("b",
			opentracing.Tag{Key: "organ", Value: "leaf"},
			opentracing.Tag{Key: "color", Value: "green"})},
		{Span: openTracer.StartSpan("a",
			opentracing.Tag{Key: "organ", Value: "leaf"},
			opentracing.Tag{Key: "color", Value: "yellow"})},
		{Span: openTracer.StartSpan("r",
			opentracing.Tag{Key: "organ", Value: "leaf"},
			opentracing.Tag{Key: "color", Value: "green"})},
	}

	type fields struct {
		Tracer     opentracing.Tracer
		Spans      []*DelaySpan
		FilterTags map[string]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "EmptyFilterTags",
			fields: fields{
				Spans:      openSpans,
				FilterTags: map[string]interface{}{},
			},
			want: true,
		},
		{
			name: "NotWritable",
			fields: fields{
				Spans: closeSpans,
				FilterTags: map[string]interface{}{
					"color": "yellow",
				},
			},
			want: false,
		},
		{
			name: "Match",
			fields: fields{
				Spans: openSpans,
				FilterTags: map[string]interface{}{
					"color": "yellow",
				},
			},
			want: true,
		},
		{
			name: "NotMatch",
			fields: fields{
				Spans: openSpans,
				FilterTags: map[string]interface{}{
					"color": "yellow",
					"organ": "stem",
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DelaySchedulerTraceContextImpl{
				Tracer:     tt.fields.Tracer,
				Spans:      tt.fields.Spans,
				FilterTags: tt.fields.FilterTags,
			}
			if got := d.isMatch(); got != tt.want {
				t.Errorf("isMatch() = %v, want %v", got, tt.want)
			}
		})
	}
}
