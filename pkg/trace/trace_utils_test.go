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
)

func Test_startSpanWrapper(t *testing.T) {
	type args struct {
		ctx   SchedulerTraceContext
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
			name: "EmptyLevel",
			args: args{
				level: "",
				phase: "",
				name:  "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			InitGlobalSchedulerTraceContext()
			_, err := StartSpanWrapper(tt.args.level, tt.args.phase, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("startSpanWrapper() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_finishActiveSpanWrapper(t *testing.T) {
	type args struct {
		ctx   SchedulerTraceContext
		state string
		info  string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "EmptyContext",
			args: args{
				state: "",
				info:  "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			InitGlobalSchedulerTraceContext()
			if err := FinishActiveSpanWrapper(tt.args.state, tt.args.info); (err != nil) != tt.wantErr {
				t.Errorf("finishActiveSpanWrapper() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
