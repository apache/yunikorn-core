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

package events

import (
	"math"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestRingBuffer_New(t *testing.T) {
	buffer := newEventRingBuffer(10)

	assert.Equal(t, 10, buffer.capacity)
	assert.Equal(t, int64(math.MinInt64), buffer.latest)
}

func TestRingBuffer_Add(t *testing.T) {
	buffer := newEventRingBuffer(10)
	populate(buffer, 4)

	assert.Equal(t, 4, buffer.idx)
	assert.Equal(t, false, buffer.full)
	assert.Equal(t, int64(3), buffer.latest)
}

func TestRingBuffer_Add_WhenFull(t *testing.T) {
	buffer := newEventRingBuffer(10)
	populate(buffer, 13)

	assert.Equal(t, 3, buffer.idx)
	assert.Equal(t, true, buffer.full)
	assert.Equal(t, int64(12), buffer.latest)
}

func TestRingBuffer_GetLatestEntries(t *testing.T) {
	buffer := newEventRingBuffer(10)
	now = func() time.Time {
		return time.Unix(0, 10)
	}

	populate(buffer, 8)
	records := buffer.GetLatestEntries(5)

	// with current time being 10 and interval being 5, we should start from 5
	assert.Equal(t, 3, len(records))
	assert.Equal(t, int64(5), records[0].TimestampNano)
	assert.Equal(t, int64(6), records[1].TimestampNano)
	assert.Equal(t, int64(7), records[2].TimestampNano)
}

func TestRingBuffer_GetLatestEntries_WhenFull(t *testing.T) {
	buffer := newEventRingBuffer(10)
	now = func() time.Time {
		return time.Unix(0, 15)
	}
	populate(buffer, 13)

	records := buffer.GetLatestEntries(5)
	assert.Equal(t, 3, len(records))
	assert.Equal(t, int64(10), records[0].TimestampNano)
	assert.Equal(t, int64(11), records[1].TimestampNano)
	assert.Equal(t, int64(12), records[2].TimestampNano)
}

func TestRingBuffer_GetLatestEntries_WhenEmpty(t *testing.T) {
	buffer := newEventRingBuffer(10)
	now = func() time.Time {
		return time.Unix(0, 10)
	}

	records := buffer.GetLatestEntries(5)
	assert.Equal(t, 0, len(records))
}

func TestRingBuffer_GetLatestEntries_WhenIntervalTooNarrow(t *testing.T) {
	buffer := newEventRingBuffer(10)
	now = func() time.Time {
		return time.Unix(0, 100)
	}
	defer func() {
		now = time.Now
	}()

	populate(buffer, 8)
	records := buffer.GetLatestEntries(10)

	assert.Equal(t, 0, len(records)) // we should start from 90, which we don't have
}

func TestRingBuffer_GetLatestEntriesCount(t *testing.T) {
	buffer := newEventRingBuffer(10)

	populate(buffer, 8)
	records := buffer.GetLatestEntriesCount(3)

	assert.Equal(t, 3, len(records))
	assert.Equal(t, int64(5), records[0].TimestampNano)
	assert.Equal(t, int64(6), records[1].TimestampNano)
	assert.Equal(t, int64(7), records[2].TimestampNano)
}

func TestRingBuffer_GetLatestEntriesCount_WhenEmpty(t *testing.T) {
	buffer := newEventRingBuffer(10)

	records := buffer.GetLatestEntriesCount(3)

	assert.Equal(t, 0, len(records))
}

func TestRingBuffer_GetLatestEntriesCount_WhenFull(t *testing.T) {
	buffer := newEventRingBuffer(10)
	populate(buffer, 13)

	records := buffer.GetLatestEntriesCount(5)

	assert.Equal(t, 5, len(records))
	assert.Equal(t, int64(8), records[0].TimestampNano)
	assert.Equal(t, int64(9), records[1].TimestampNano)
	assert.Equal(t, int64(10), records[2].TimestampNano)
	assert.Equal(t, int64(11), records[3].TimestampNano)
	assert.Equal(t, int64(12), records[4].TimestampNano)
}

func TestRingBuffer_GetEventsFromPosition(t *testing.T) {
	buffer := newEventRingBuffer(10)
	populate(buffer, 8)

	records := buffer.GetEventsFromPosition(5)
	assert.Equal(t, 3, len(records))
	assert.Equal(t, int64(5), records[0].TimestampNano)
	assert.Equal(t, int64(6), records[1].TimestampNano)
	assert.Equal(t, int64(7), records[2].TimestampNano)
}

func TestRingBuffer_GetEventsFromPosition_WhenFull(t *testing.T) {
	buffer := newEventRingBuffer(10)
	populate(buffer, 12)

	records := buffer.GetEventsFromPosition(5)
	assert.Equal(t, 7, len(records))
}

func TestRingBuffer_GetEventsFromPosition_WhenEmpty(t *testing.T) {
	buffer := newEventRingBuffer(10)

	records := buffer.GetEventsFromPosition(5)

	assert.Equal(t, 0, len(records))
}

func TestRingBuffer_GetEventsFromPosition_InvalidPos(t *testing.T) {
	buffer := newEventRingBuffer(10)
	populate(buffer, 5)

	// pos > idx
	records := buffer.GetEventsFromPosition(8)
	assert.Equal(t, 0, len(records))
}

func populate(buffer *eventRingBuffer, count int) {
	for i := 0; i < count; i++ {
		buffer.Add(&si.EventRecord{
			TimestampNano: int64(i),
		})
	}
}
