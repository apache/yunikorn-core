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

	"gotest.tools/assert"
)

// TestSchedulerTracerImpl tests SetParams and NewTraceContext
func TestSchedulerTracerImpl(t *testing.T) {
	type fields struct {
		SchedulerTracerImplParams *SchedulerTracerImplParams
	}
	tests := []struct {
		name         string
		fields       fields
		wantType     int
		wantOnDemand bool
	}{
		{
			name: "Default",
			fields: fields{
				SchedulerTracerImplParams: nil,
			},
			wantType:     0,
			wantOnDemand: false,
		},
		{
			name: "FromEnv",
			fields: fields{
				SchedulerTracerImplParams: &SchedulerTracerImplParams{
					Mode:       Sampling,
					FilterTags: nil,
				},
			},
			wantType:     0,
			wantOnDemand: false,
		},
		{
			name: "Debug",
			fields: fields{
				SchedulerTracerImplParams: &SchedulerTracerImplParams{
					Mode:       Debug,
					FilterTags: nil,
				},
			},
			wantType:     0,
			wantOnDemand: true,
		},
		{
			name: "DebugWithFilter",
			fields: fields{
				SchedulerTracerImplParams: &SchedulerTracerImplParams{
					Mode: DebugWithFilter,
					FilterTags: map[string]interface{}{
						"foo": "bar",
					},
				},
			},
			wantType:     1,
			wantOnDemand: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracer, err := NewSchedulerTracer(nil)
			assert.NilError(t, err)
			defer tracer.Close()
			tracer.(*SchedulerTracerImpl).SetParams(tt.fields.SchedulerTracerImplParams)
			ctx := tracer.NewTraceContext()
			switch typeInfo := ctx.(type) {
			case nil:
				t.Errorf("Nil context object, type: %T", typeInfo)
			case *SchedulerTraceContextImpl:
				assert.Equal(t, tt.wantType, 0)
				assert.Equal(t, tt.wantOnDemand, ctx.(*SchedulerTraceContextImpl).OnDemandFlag)
			case *DelaySchedulerTraceContextImpl:
				assert.Equal(t, tt.wantType, 1)
			default:
				t.Errorf("Unknown type: %T", typeInfo)
			}
		})
	}
}
