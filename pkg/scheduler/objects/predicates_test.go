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

func scoreMap(nodeID string, orig, self []bool) map[string][]*Allocation {
	alloc := make([]*Allocation, 0)
	for i := range orig {
		alloc = append(alloc, allocForScore(orig[i], self[i]))
	}
	return map[string][]*Allocation{nodeID: alloc}
}
