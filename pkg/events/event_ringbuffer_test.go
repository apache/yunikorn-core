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
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestRingBuffer_New(t *testing.T) {
	buffer := NewEventRingBuffer(10, time.Minute)

	assert.Equal(t, 10, buffer.capacity)
	assert.Equal(t, 0, buffer.noElements)
	assert.Equal(t, int64(-1<<63), buffer.latest)
	assert.Equal(t, int64(time.Minute), buffer.lifetimeNanos)
}

func TestRingBuffer_Add(t *testing.T) {
	buffer := NewEventRingBuffer(10, time.Minute)
	populate(buffer, 4)

	assert.Equal(t, 0, buffer.head)
	assert.Equal(t, 4, buffer.tail)
	assert.Equal(t, 4, buffer.noElements)
	assert.Equal(t, int64(3), buffer.latest)
}

func TestRingBuffer_Add_WhenFull(t *testing.T) {
	buffer := NewEventRingBuffer(10, time.Minute)
	populate(buffer, 13)

	assert.Equal(t, 3, buffer.head)
	assert.Equal(t, 3, buffer.tail)
	assert.Equal(t, 10, buffer.noElements)
	assert.Equal(t, int64(12), buffer.latest)
}

func TestRingBuffer_GetLatestEntries(t *testing.T) {
	buffer := NewEventRingBuffer(10, time.Minute)
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
	buffer := NewEventRingBuffer(10, time.Minute)
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
	buffer := NewEventRingBuffer(10, time.Minute)
	now = func() time.Time {
		return time.Unix(0, 10)
	}

	records := buffer.GetLatestEntries(5)
	assert.Equal(t, 0, len(records))
}

func TestRingBuffer_GetLatestEntries_WhenIntervalTooNarrow(t *testing.T) {
	buffer := NewEventRingBuffer(10, time.Minute)
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
	buffer := NewEventRingBuffer(10, time.Minute)

	populate(buffer, 8)
	records := buffer.GetLatestEntriesCount(3)

	assert.Equal(t, 3, len(records))
	assert.Equal(t, int64(5), records[0].TimestampNano)
	assert.Equal(t, int64(6), records[1].TimestampNano)
	assert.Equal(t, int64(7), records[2].TimestampNano)
}

func TestRingBuffer_GetLatestEntriesCount_WhenEmpty(t *testing.T) {
	buffer := NewEventRingBuffer(10, time.Minute)

	records := buffer.GetLatestEntriesCount(3)

	assert.Equal(t, 0, len(records))
}

func TestRingBuffer_GetLatestEntriesCount_WhenFull(t *testing.T) {
	buffer := NewEventRingBuffer(10, time.Minute)
	populate(buffer, 13)

	records := buffer.GetLatestEntriesCount(3)

	assert.Equal(t, 3, len(records))
	assert.Equal(t, int64(10), records[0].TimestampNano)
	assert.Equal(t, int64(11), records[1].TimestampNano)
	assert.Equal(t, int64(12), records[2].TimestampNano)
}

func TestRingBuffer_RemoveExpiredEntries(t *testing.T) {
	buffer := NewEventRingBuffer(20, 10)
	now = func() time.Time {
		return time.Unix(0, 20)
	}
	defer func() {
		now = time.Now
	}()
	populate(buffer, 20)

	count := buffer.RemoveExpiredEntries()
	assert.Equal(t, 11, count)
	assert.Equal(t, 9, buffer.noElements)
	assert.Equal(t, 11, buffer.head)
	assert.Equal(t, 0, buffer.tail)
}

func TestRingBuffer_RemoveExpiredEntries_WhenAllEntriesExpired(t *testing.T) {
	buffer := NewEventRingBuffer(20, 10)
	now = func() time.Time {
		return time.Unix(0, 100)
	}
	defer func() {
		now = time.Now
	}()
	populate(buffer, 20)

	count := buffer.RemoveExpiredEntries()
	assert.Equal(t, 20, count)
	assert.Equal(t, 0, buffer.noElements)
	assert.Equal(t, 0, buffer.head)
	assert.Equal(t, 0, buffer.tail)
	assert.Equal(t, int64(-1<<63), buffer.latest)
}

func TestRingBuffer_RemoveExpiredEntries_Empty(t *testing.T) {
	buffer := NewEventRingBuffer(20, 10)
	now = func() time.Time {
		return time.Unix(0, 100)
	}
	defer func() {
		now = time.Now
	}()

	removed := buffer.RemoveExpiredEntries()
	assert.Equal(t, 0, removed)
}

func populate(buffer *EventRingBuffer, count int) {
	for i := 0; i < count; i++ {
		buffer.Add(&si.EventRecord{
			TimestampNano: int64(i),
		})
	}
}
