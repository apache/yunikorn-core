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
	"time"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
)

// Test adding/updating/removing requests with different priorities,
// to verify the correctness of all/pending priority groups and requests.
func TestSortedRequests(t *testing.T) {
	// init
	appID := "app-1"
	res := resources.NewResource()
	sr := NewSortedRequests()
	assert.Equal(t, len(sr.requests), 0)
	assert.Equal(t, sr.sortedPriorityGroups.Size(), 0)

	// ignore nil request
	sr.AddRequest(nil)
	assert.Equal(t, sr.Size(), 0)
	assert.Equal(t, sr.sortedPriorityGroups.Size(), 0)

	// add request-1 with priority 1
	reqKey1 := "req-1"
	req1 := newAllocationAskWithPriority(reqKey1, appID, res, 1)
	sr.AddRequest(req1)
	checkSortedRequests(t, sr, []int32{1}, [][]string{{reqKey1}}, []int32{1}, [][]string{{reqKey1}})

	// update request-1 to non-pending state
	req1.pendingRepeatAsk = 0
	sr.AddRequest(req1)
	checkSortedRequests(t, sr, []int32{1}, [][]string{{reqKey1}}, []int32{}, [][]string{})

	// update request-1 to pending state
	req1.pendingRepeatAsk = 2
	sr.AddRequest(req1)
	checkSortedRequests(t, sr, []int32{1}, [][]string{{reqKey1}}, []int32{1}, [][]string{{reqKey1}})

	// add request-2 with priority 1
	reqKey2 := "req-2"
	req2 := newAllocationAskWithPriority(reqKey2, appID, res, 1)
	sr.AddRequest(req2)
	checkSortedRequests(t, sr, []int32{1}, [][]string{{reqKey1, reqKey2}}, []int32{1}, [][]string{{reqKey1, reqKey2}})

	// update request-1 to non-pending state
	req2.pendingRepeatAsk = 0
	sr.AddRequest(req2)
	checkSortedRequests(t, sr, []int32{1}, [][]string{{reqKey1, reqKey2}}, []int32{1}, [][]string{{reqKey1}})

	// add request-3 with priority 1
	reqKey3 := "req-3"
	req3 := newAllocationAskWithPriority(reqKey3, appID, res, 1)
	sr.AddRequest(req3)
	checkSortedRequests(t, sr, []int32{1}, [][]string{{reqKey1, reqKey2, reqKey3}},
		[]int32{1}, [][]string{{reqKey1, reqKey3}})

	// add request-4 with priority 3
	reqKey4 := "req-4"
	req4 := newAllocationAskWithPriority(reqKey4, appID, res, 3)
	sr.AddRequest(req4)
	checkSortedRequests(t, sr, []int32{3, 1}, [][]string{{reqKey4}, {reqKey1, reqKey2, reqKey3}},
		[]int32{3, 1}, [][]string{{reqKey4}, {reqKey1, reqKey3}})

	// update request-4 to non-pending state
	req4.pendingRepeatAsk = 0
	sr.AddRequest(req4)
	checkSortedRequests(t, sr, []int32{3, 1}, [][]string{{reqKey4}, {reqKey1, reqKey2, reqKey3}},
		[]int32{1}, [][]string{{reqKey1, reqKey3}})

	// update request-4 to pending state
	req4.pendingRepeatAsk = 2
	sr.AddRequest(req4)
	checkSortedRequests(t, sr, []int32{3, 1}, [][]string{{reqKey4}, {reqKey1, reqKey2, reqKey3}},
		[]int32{3, 1}, [][]string{{reqKey4}, {reqKey1, reqKey3}})

	// add request-5 with priority 2
	reqKey5 := "req-5"
	req5 := newAllocationAskWithPriority(reqKey5, appID, res, 2)
	sr.AddRequest(req5)
	checkSortedRequests(t, sr, []int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2, reqKey3}},
		[]int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey3}})

	// add request-0 with priority 1 and create time is less than request-1
	// it should be placed before request-1
	reqKey0 := "req-0"
	req0 := newAllocationAskWithPriority(reqKey0, appID, res, 1)
	req0.createTime = req1.createTime.Add(-1 * time.Millisecond)
	sr.AddRequest(req0)
	checkSortedRequests(t, sr, []int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey0, reqKey1, reqKey2, reqKey3}},
		[]int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey0, reqKey1, reqKey3}})

	// remove request-0
	sr.RemoveRequest(reqKey0)
	checkSortedRequests(t, sr, []int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2, reqKey3}},
		[]int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey3}})

	// update request-5 to non-pending state
	req5.pendingRepeatAsk = 0
	sr.AddRequest(req5)
	checkSortedRequests(t, sr, []int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2, reqKey3}},
		[]int32{3, 1}, [][]string{{reqKey4}, {reqKey1, reqKey3}})

	// update request-1 to non-pending state
	req1.pendingRepeatAsk = 0
	sr.AddRequest(req1)
	checkSortedRequests(t, sr, []int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2, reqKey3}},
		[]int32{3, 1}, [][]string{{reqKey4}, {reqKey3}})

	// update request-3 to non-pending state
	req3.pendingRepeatAsk = 0
	sr.AddRequest(req3)
	checkSortedRequests(t, sr, []int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2, reqKey3}},
		[]int32{3}, [][]string{{reqKey4}})

	// update request-4 to non-pending state
	req4.pendingRepeatAsk = 0
	sr.AddRequest(req4)
	checkSortedRequests(t, sr, []int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2, reqKey3}},
		[]int32{}, [][]string{})

	// update request-5 to pending state
	req5.pendingRepeatAsk = 1
	sr.AddRequest(req5)
	checkSortedRequests(t, sr, []int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2, reqKey3}},
		[]int32{2}, [][]string{{reqKey5}})

	// update request-4 to pending state
	req4.pendingRepeatAsk = 1
	sr.AddRequest(req4)
	checkSortedRequests(t, sr, []int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2, reqKey3}},
		[]int32{3, 2}, [][]string{{reqKey4}, {reqKey5}})

	// update request-3 to pending state
	req3.pendingRepeatAsk = 1
	sr.AddRequest(req3)
	checkSortedRequests(t, sr, []int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2, reqKey3}},
		[]int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey3}})

	// update request-2 to pending state
	req2.pendingRepeatAsk = 1
	sr.AddRequest(req2)
	checkSortedRequests(t, sr, []int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2, reqKey3}},
		[]int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey2, reqKey3}})

	// update request-1 to pending state
	req1.pendingRepeatAsk = 1
	sr.AddRequest(req1)
	checkSortedRequests(t, sr, []int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2, reqKey3}},
		[]int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2, reqKey3}})

	// remove request-4
	sr.RemoveRequest(reqKey4)
	checkSortedRequests(t, sr, []int32{2, 1}, [][]string{{reqKey5}, {reqKey1, reqKey2, reqKey3}},
		[]int32{2, 1}, [][]string{{reqKey5}, {reqKey1, reqKey2, reqKey3}})

	// remove request-3
	sr.RemoveRequest(reqKey3)
	checkSortedRequests(t, sr, []int32{2, 1}, [][]string{{reqKey5}, {reqKey1, reqKey2}},
		[]int32{2, 1}, [][]string{{reqKey5}, {reqKey1, reqKey2}})

	// remove request-5
	sr.RemoveRequest(reqKey5)
	checkSortedRequests(t, sr, []int32{1}, [][]string{{reqKey1, reqKey2}},
		[]int32{1}, [][]string{{reqKey1, reqKey2}})

	// remove request-2
	sr.RemoveRequest(reqKey2)
	checkSortedRequests(t, sr, []int32{1}, [][]string{{reqKey1}}, []int32{1}, [][]string{{reqKey1}})

	// remove request-1
	sr.RemoveRequest(reqKey1)
	checkSortedRequests(t, sr, []int32{}, [][]string{}, []int32{}, [][]string{})

	// add request-1/request-2/request-4/request-5
	sr.AddRequest(req1)
	sr.AddRequest(req2)
	sr.AddRequest(req4)
	sr.AddRequest(req5)
	checkSortedRequests(t, sr, []int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2}},
		[]int32{3, 2, 1}, [][]string{{reqKey4}, {reqKey5}, {reqKey1, reqKey2}})

	// reset
	sr.Reset()
	checkSortedRequests(t, sr, []int32{}, [][]string{}, []int32{}, [][]string{})
}

// Create request with specified priority
func newAllocationAskWithPriority(allocKey, appID string, res *resources.Resource, priority int32) *schedulingAllocationAsk {
	ask := newAllocationAsk(allocKey, appID, res)
	ask.priority = priority
	ask.createTime = time.Now()
	return ask
}

// Check the correctness of all/pending priority groups and requests
func checkSortedRequests(t *testing.T, sortedRequests *SortedRequests, expectedPriorities []int32,
	expectedRequestKeys [][]string, expectedPendingPriorities []int32, expectedPendingRequestKeys [][]string) {
	// check priority groups and requests
	t.Logf("Check sorted requests: expectedPriorities=%v, expectedRequestKeys=%v, expectedPendingPriorities=%v,"+
		" expectedPendingRequestKeys=%v", expectedPriorities, expectedRequestKeys, expectedPendingPriorities,
		expectedPendingRequestKeys)
	pgIt := sortedRequests.sortedPriorityGroups.GetIterator()
	pgIndex := 0
	size := 0
	for pgIt.HasNext() {
		_, priorityGroup := pgIt.Next()
		sortedPriorityGroup, ok := priorityGroup.(*SortedPriorityGroup)
		if !ok {
			t.Fatal("cast failed unexpected priority group")
		}
		assert.Equal(t, sortedPriorityGroup.priority, expectedPriorities[pgIndex])
		reqIt := sortedPriorityGroup.GetRequestIterator()
		reqIndex := 0
		for reqIt.HasNext() {
			ask := reqIt.Next()
			assert.Equal(t, ask.AskProto.AllocationKey, expectedRequestKeys[pgIndex][reqIndex])
			reqIndex++
		}
		assert.Equal(t, reqIt.HasNext(), false)
		assert.Equal(t, reqIndex, len(expectedRequestKeys[pgIndex]))
		size += len(expectedRequestKeys[pgIndex])
		pgIndex++
	}
	assert.Equal(t, sortedRequests.Size(), size)
	assert.Equal(t, pgIt.HasNext(), false)
	assert.Equal(t, pgIndex, len(expectedPriorities))
	// check pending priority groups and pending requests
	ppgIt := sortedRequests.sortedPriorityGroups.GetMatchedIterator()
	ppgIndex := 0
	for ppgIt.HasNext() {
		_, pendingPriorityGroup := ppgIt.Next()
		sortedPendingPriorityGroup, ok := pendingPriorityGroup.(*SortedPriorityGroup)
		if !ok {
			t.Fatal("cast failed unexpected priority group")
		}
		assert.Equal(t, sortedPendingPriorityGroup.priority, expectedPendingPriorities[ppgIndex])
		reqIt := sortedPendingPriorityGroup.GetPendingRequestIterator()
		reqIndex := 0
		for reqIt.HasNext() {
			ask := reqIt.Next()
			assert.Equal(t, ask.AskProto.AllocationKey, expectedPendingRequestKeys[ppgIndex][reqIndex])
			reqIndex++
		}
		assert.Equal(t, reqIt.HasNext(), false)
		assert.Equal(t, reqIndex, len(expectedPendingRequestKeys[ppgIndex]))
		ppgIndex++
	}
	assert.Equal(t, ppgIt.HasNext(), false)
	assert.Equal(t, ppgIndex, len(expectedPendingPriorities))
}
