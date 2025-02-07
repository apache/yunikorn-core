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

	"gotest.tools/v3/assert"
)

func TestSolutionScoring(t *testing.T) {
	singleAlloc := scoreMap(nodeID1, []bool{false}, []bool{true})
	singleOriginator := scoreMap(nodeID1, []bool{true}, []bool{true})
	singleNoPreempt := scoreMap(nodeID1, []bool{false}, []bool{false})
	dual := scoreMap(nodeID1, []bool{true, false}, []bool{true, false})

	var none *predicateCheckResult = nil
	assert.Equal(t, scoreUnfit, none.getSolutionScore(singleAlloc), "wrong score for nil")

	missing := &predicateCheckResult{nodeID: "missing", success: true, index: 0}
	assert.Equal(t, scoreUnfit, missing.getSolutionScore(singleAlloc), "wrong score for missing")

	failure := &predicateCheckResult{nodeID: nodeID1, success: false, index: -1}
	assert.Equal(t, scoreUnfit, failure.getSolutionScore(singleAlloc), "wrong score for failure")

	overrun := &predicateCheckResult{nodeID: nodeID1, success: true, index: 1}
	assert.Equal(t, scoreUnfit, overrun.getSolutionScore(singleAlloc), "wrong score for overrun")

	noOp := &predicateCheckResult{nodeID: nodeID1, success: true, index: -1}
	assert.Equal(t, uint64(0), noOp.getSolutionScore(singleAlloc), "wrong score for noop")

	ideal := &predicateCheckResult{nodeID: nodeID1, success: true, index: 0}
	assert.Equal(t, uint64(1), ideal.getSolutionScore(singleAlloc), "wrong score for ideal")

	originator := &predicateCheckResult{nodeID: nodeID1, success: true, index: 0}
	assert.Equal(t, scoreOriginator+1, originator.getSolutionScore(singleOriginator), "wrong score for originator")

	noPreempt := &predicateCheckResult{nodeID: nodeID1, success: true, index: 0}
	assert.Equal(t, scoreNoPreempt+1, noPreempt.getSolutionScore(singleNoPreempt), "wrong score for no-preempt")

	both := &predicateCheckResult{nodeID: nodeID1, success: true, index: 1}
	assert.Equal(t, scoreOriginator+scoreNoPreempt+2, both.getSolutionScore(dual), "wrong score for both")

	assert.Check(t, noOp.betterThan(none, singleAlloc), "noop should be better than nil")
	assert.Check(t, noOp.betterThan(ideal, singleAlloc), "noop should be better than ideal")
}

func TestPredicateCheckResult_String(t *testing.T) {
	tests := []struct {
		name   string
		result *predicateCheckResult
		want   string
	}{
		{
			name: "empty nodeID",
			result: &predicateCheckResult{
				nodeID: "",
			},
			want: "",
		},
		{
			name: "basic result without victims",
			result: &predicateCheckResult{
				nodeID:        "node-1",
				allocationKey: "alloc-1",
				success:       true,
				index:         0,
			},
			want: "node: node-1, alloc: alloc-1, success: true, index: 0",
		},
		{
			name: "result with single victim",
			result: &predicateCheckResult{
				nodeID:        "node-1",
				allocationKey: "alloc-1",
				success:       true,
				index:         1,
				victims: []*Allocation{
					newAllocationAll("alloc-key-1", "app-1", "node-1", "", nil, false, 0),
				},
			},
			want: "node: node-1, alloc: alloc-1, success: true, index: 1, victims: [allocationKey alloc-key-1, applicationID app-1, Resource map[], Allocated true]",
		},
		{
			name: "result with multiple victims",
			result: &predicateCheckResult{
				nodeID:        "node-1",
				allocationKey: "alloc-1",
				success:       true,
				index:         2,
				victims: []*Allocation{
					newAllocationAll("alloc-key-1", "app-1", "node-1", "", nil, false, 0),
					newAllocationAll("alloc-key-2", "app-2", "node-2", "", nil, false, 0),
				},
			},
			want: "node: node-1, alloc: alloc-1, success: true, index: 2, victims: [allocationKey alloc-key-1, applicationID app-1, Resource map[], Allocated true, allocationKey alloc-key-2, applicationID app-2, Resource map[], Allocated true]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.result.String()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPredicateCheckResult_PopulateVictims(t *testing.T) {
	tests := []struct {
		name          string
		pcr           *predicateCheckResult
		victimsByNode map[string][]*Allocation
		wantVictims   []*Allocation
		wantSuccess   bool
		wantIndex     int
	}{
		{
			name: "nil predicateCheckResult",
			pcr:  nil,
			victimsByNode: map[string][]*Allocation{
				"node-1": {newAllocation("app-1", "node-1", nil)},
			},
			wantVictims: nil,
			wantSuccess: false,
			wantIndex:   0,
		},
		{
			name: "unsuccessful result",
			pcr: &predicateCheckResult{
				nodeID:  "node-1",
				success: false,
				index:   1,
			},
			victimsByNode: map[string][]*Allocation{
				"node-1": {newAllocation("app-1", "node-1", nil)},
			},
			wantVictims: nil,
			wantSuccess: false,
			wantIndex:   1,
		},
		{
			name: "node not found",
			pcr: &predicateCheckResult{
				nodeID:  "node-missing",
				success: true,
				index:   0,
			},
			victimsByNode: map[string][]*Allocation{
				"node-1": {newAllocation("app-1", "node-1", nil)},
			},
			wantVictims: nil,
			wantSuccess: false,
			wantIndex:   -1,
		},
		{
			name: "index too large",
			pcr: &predicateCheckResult{
				nodeID:  "node-1",
				success: true,
				index:   2,
			},
			victimsByNode: map[string][]*Allocation{
				"node-1": {newAllocation("app-1", "node-1", nil)},
			},
			wantVictims: nil,
			wantSuccess: false,
			wantIndex:   -1,
		},
		{
			name: "successful population single victim",
			pcr: &predicateCheckResult{
				nodeID:  "node-1",
				success: true,
				index:   0,
			},
			victimsByNode: map[string][]*Allocation{
				"node-1": {newAllocation("app-1", "node-1", nil)},
			},
			wantVictims: []*Allocation{newAllocation("app-1", "node-1", nil)},
			wantSuccess: true,
			wantIndex:   0,
		},
		{
			name: "successful population multiple victims",
			pcr: &predicateCheckResult{
				nodeID:  "node-1",
				success: true,
				index:   1,
			},
			victimsByNode: map[string][]*Allocation{
				"node-1": {
					newAllocation("app-1", "node-1", nil),
					newAllocation("app-2", "node-1", nil),
				},
			},
			wantVictims: []*Allocation{
				newAllocation("app-1", "node-1", nil),
				newAllocation("app-2", "node-1", nil),
			},
			wantSuccess: true,
			wantIndex:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.pcr.populateVictims(tt.victimsByNode)

			if tt.pcr != nil {
				assert.Equal(t, tt.wantSuccess, tt.pcr.success, "success mismatch")
				assert.Equal(t, tt.wantIndex, tt.pcr.index, "index mismatch")
				if tt.wantVictims == nil {
					assert.Assert(t, tt.pcr.victims == nil || len(tt.pcr.victims) == 0, "expected no victims")
				} else {
					assert.Equal(t, len(tt.wantVictims), len(tt.pcr.victims), "victims length mismatch")
				}
			}
		})
	}
}

func scoreMap(nodeID string, orig, self []bool) map[string][]*Allocation {
	alloc := make([]*Allocation, 0)
	for i := range orig {
		alloc = append(alloc, allocForScore(orig[i], self[i]))
	}
	return map[string][]*Allocation{nodeID: alloc}
}
