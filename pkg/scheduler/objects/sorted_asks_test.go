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
	sorted.insert(&AllocationAsk{
		createTime:    time.Unix(10, 0),
		priority:      10,
		allocationKey: "alloc-1",
	})
	sorted.insert(&AllocationAsk{
		createTime:    time.Unix(10, 0),
		priority:      25,
		allocationKey: "alloc-2",
	})
	sorted.insert(&AllocationAsk{
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
	sorted.remove(&AllocationAsk{
		allocationKey: "non-existing",
	})
	assert.Equal(t, 99, len(sorted))
}

func askPresent(ask *AllocationAsk, asks []*AllocationAsk) bool {
	for _, a := range asks {
		if a.allocationKey == ask.allocationKey {
			return true
		}
	}
	return false
}

func checkSorted(asks []*AllocationAsk) bool {
	prev := int64(math.MinInt64)
	for _, ask := range asks {
		if ask.createTime.UnixNano() < prev {
			return false
		}
		prev = ask.createTime.UnixNano()
	}
	return true
}

func getAllocationAsk(n int) *AllocationAsk {
	return &AllocationAsk{
		allocationKey: "alloc-" + strconv.Itoa(n),
		priority:      1,
		createTime:    time.Unix(int64(n), 0),
	}
}
