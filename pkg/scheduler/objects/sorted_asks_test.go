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
	"math"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestInsertRemove(t *testing.T) {
	sorted := sortedRequests{}
	// alloc-3 > alloc-2 > alloc-1
	sorted.insert(&Allocation{
		createTime:    time.Unix(10, 0),
		priority:      10,
		allocationKey: "alloc-1",
	})
	sorted.insert(&Allocation{
		createTime:    time.Unix(10, 0),
		priority:      25,
		allocationKey: "alloc-2",
	})
	sorted.insert(&Allocation{
		createTime:    time.Unix(5, 0),
		priority:      35,
		allocationKey: "alloc-3",
	})
	assert.Equal(t, 3, len(sorted))
	assert.Equal(t, "alloc-3", sorted[0].allocationKey)
	assert.Equal(t, "alloc-2", sorted[1].allocationKey)
	assert.Equal(t, "alloc-1", sorted[2].allocationKey)

	sorted = sortedRequests{}
	for i := 0; i < 100; i++ {
		j := i * 3
		ask := getAllocationAsk(j)
		sorted.insert(ask)
	}
	assert.Assert(t, checkSorted(sorted), "asks are not sorted")

	// insert element between 48 and 51
	ask50 := getAllocationAsk(50)
	sorted.insert(ask50)
	assert.Assert(t, checkSorted(sorted), "asks are not sorted")
	assert.Assert(t, askPresent(ask50, sorted), "ask has not been added")
	assert.Equal(t, 101, len(sorted))

	// insert element between 90 and 93
	ask91 := getAllocationAsk(91)
	sorted.insert(ask91)
	assert.Assert(t, checkSorted(sorted), "asks are not sorted")
	assert.Assert(t, askPresent(ask91, sorted), "ask has not been added")
	assert.Equal(t, 102, len(sorted))

	// remove first element
	sorted.removeAt(0)
	assert.Assert(t, checkSorted(sorted), "asks are not sorted")
	assert.Equal(t, "alloc-3", sorted[0].allocationKey, "unexpected first element")
	assert.Equal(t, 101, len(sorted))

	// remove last element
	sorted.removeAt(len(sorted) - 1)
	last := sorted[len(sorted)-1]
	assert.Equal(t, "alloc-294", last.allocationKey, "unexpected last element")
	assert.Assert(t, checkSorted(sorted), "asks are not sorted")
	assert.Equal(t, 100, len(sorted))

	// remove ask50
	sorted.remove(ask50)
	assert.Assert(t, !askPresent(ask50, sorted), "ask was not removed")
	assert.Assert(t, checkSorted(sorted), "asks are not sorted")
	assert.Equal(t, 99, len(sorted))

	// remove non-existent
	sorted.remove(&Allocation{
		allocationKey: "non-existing",
	})
	assert.Equal(t, 99, len(sorted))
}

func TestRemoveWithSamePriorityAndTime(t *testing.T) {
	// Create a sorted requests list
	sorted := sortedRequests{}

	// Create allocations with same priority and creation time
	// but different allocation keys
	baseTime := time.Now()
	alloc1 := &Allocation{
		createTime:    baseTime,
		priority:      10,
		allocationKey: "alloc-1",
	}
	alloc2 := &Allocation{
		createTime:    baseTime,
		priority:      10,
		allocationKey: "alloc-2",
	}
	alloc3 := &Allocation{
		createTime:    baseTime,
		priority:      10,
		allocationKey: "alloc-3",
	}

	// Insert the allocations
	sorted.insert(alloc1)
	sorted.insert(alloc2)
	sorted.insert(alloc3)

	// Verify all allocations are in the list
	assert.Equal(t, 3, len(sorted))
	assert.Assert(t, askPresent(alloc1, sorted), "alloc1 should be present")
	assert.Assert(t, askPresent(alloc2, sorted), "alloc2 should be present")
	assert.Assert(t, askPresent(alloc3, sorted), "alloc3 should be present")

	// Try to remove alloc2
	sorted.remove(alloc2)

	// Verify alloc2 is removed but alloc1 and alloc3 are still there
	assert.Equal(t, 2, len(sorted))
	assert.Assert(t, askPresent(alloc1, sorted), "alloc1 should still be present")
	assert.Assert(t, !askPresent(alloc2, sorted), "alloc2 should be removed")
	assert.Assert(t, askPresent(alloc3, sorted), "alloc3 should still be present")

	// Try to remove alloc1
	sorted.remove(alloc1)

	// Verify alloc1 is removed and only alloc3 remains
	assert.Equal(t, 1, len(sorted))
	assert.Assert(t, !askPresent(alloc1, sorted), "alloc1 should be removed")
	assert.Assert(t, askPresent(alloc3, sorted), "alloc3 should still be present")

	// Try to remove alloc3
	sorted.remove(alloc3)

	// Verify all allocations are removed
	assert.Equal(t, 0, len(sorted))
	assert.Assert(t, !askPresent(alloc3, sorted), "alloc3 should be removed")
}

func askPresent(ask *Allocation, asks []*Allocation) bool {
	for _, a := range asks {
		if a.allocationKey == ask.allocationKey {
			return true
		}
	}
	return false
}

func checkSorted(asks []*Allocation) bool {
	prev := int64(math.MinInt64)
	for _, ask := range asks {
		if ask.createTime.UnixNano() < prev {
			return false
		}
		prev = ask.createTime.UnixNano()
	}
	return true
}

func getAllocationAsk(n int) *Allocation {
	return &Allocation{
		allocationKey: "alloc-" + strconv.Itoa(n),
		priority:      1,
		createTime:    time.Unix(int64(n), 0),
	}
}
