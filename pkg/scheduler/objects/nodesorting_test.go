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

package objects

import (
	"testing"

	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

func TestNewNodeSortingPolicy(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want policies.SortingPolicy
	}{
		{"EmptyString", "", policies.FairnessPolicy},
		{"FairString", "fair", policies.FairnessPolicy},
		{"BinString", "binpacking", policies.BinPackingPolicy},
		{"UnknownString", "unknown", policies.FairnessPolicy},
	}
	for _, tt := range tests {
		got := NewNodeSortingPolicy(tt.arg)
		if got == nil || got.PolicyType() != tt.want {
			t.Errorf("%s unexpected policy returned, expected = '%s', got '%v'", tt.name, tt.want, got)
		}
	}
}
