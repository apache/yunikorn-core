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
	"testing"

	"github.com/opentracing/opentracing-go"
	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/trace"
)

func Test_startSpanWrapper(t *testing.T) {
	tracer, closer, err := trace.NewConstTracer("open-tracer", true)
	assert.NilError(t, err)
	defer closer.Close()

	type args struct {
		ctx   trace.SchedulerTraceContext
		level string
		phase string
		name  string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "NilContext",
			args: args{
				ctx:   nil,
				level: "foobar",
				phase: "",
				name:  "",
			},
			wantErr: false,
		},
		{
			name: "EmptyLevel",
			args: args{
				ctx: &trace.SchedulerTraceContextImpl{
					Tracer:    tracer,
					SpanStack: []opentracing.Span{},
				},
				level: "",
				phase: "",
				name:  "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := startSpanWrapper(tt.args.ctx, tt.args.level, tt.args.phase, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("startSpanWrapper() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_finishActiveSpanWrapper(t *testing.T) {
	tracer, closer, err := trace.NewConstTracer("open-tracer", true)
	assert.NilError(t, err)
	defer closer.Close()

	type args struct {
		ctx   trace.SchedulerTraceContext
		state string
		info  string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "NilContext",
			args: args{
				ctx:   nil,
				state: "",
				info:  "",
			},
			wantErr: false,
		},
		{
			name: "EmptyContext",
			args: args{
				ctx: &trace.SchedulerTraceContextImpl{
					Tracer:    tracer,
					SpanStack: []opentracing.Span{},
				},
				state: "",
				info:  "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := finishActiveSpanWrapper(tt.args.ctx, tt.args.state, tt.args.info); (err != nil) != tt.wantErr {
				t.Errorf("finishActiveSpanWrapper() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
