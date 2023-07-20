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

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestRingBuffer_New(t *testing.T) {
	buffer := newEventRingBuffer(10)

	assert.Equal(t, uint64(10), buffer.capacity)
	assert.Equal(t, false, buffer.full)
}

func TestRingBuffer_Add(t *testing.T) {
	buffer := newEventRingBuffer(10)
	populate(buffer, 4)

	assert.Equal(t, uint64(4), buffer.head)
	assert.Equal(t, false, buffer.full)
	assert.Equal(t, uint64(4), buffer.id)
}

func TestRingBuffer_Add_WhenFull(t *testing.T) {
	buffer := newEventRingBuffer(10)
	populate(buffer, 13)

	assert.Equal(t, uint64(3), buffer.head)
	assert.Equal(t, true, buffer.full)
	assert.Equal(t, uint64(13), buffer.id)
}

func TestGetEventsFromId(t *testing.T) {
	// single element
	buffer := newEventRingBuffer(10)
	populate(buffer, 1)
	records, lowest, highest := buffer.GetEventsFromID(0, math.MaxUint64)
	assert.Equal(t, 1, len(records))
	assert.Equal(t, uint64(0), lowest)
	assert.Equal(t, uint64(0), highest)
	verifyRecords(t, 0, 0, records)

	// half filled
	buffer = newEventRingBuffer(20)
	populate(buffer, 10)
	records, lowest, highest = buffer.GetEventsFromID(5, math.MaxUint64)
	assert.Equal(t, 5, len(records))
	assert.Equal(t, uint64(0), lowest)
	assert.Equal(t, uint64(9), highest)
	verifyRecords(t, 5, 9, records)

	// half filled - reduced count
	buffer = newEventRingBuffer(20)
	populate(buffer, 10)
	records, lowest, highest = buffer.GetEventsFromID(5, 3)
	assert.Equal(t, 3, len(records))
	assert.Equal(t, uint64(0), lowest)
	assert.Equal(t, uint64(9), highest)
	verifyRecords(t, 5, 7, records)
}

func TestGetEventsFromId_WhenFull(t *testing.T) {
	// wrapped, have to count back
	buffer := newEventRingBuffer(20)
	populate(buffer, 25)
	records, lowest, highest := buffer.GetEventsFromID(18, math.MaxUint64)
	assert.Equal(t, 7, len(records))
	assert.Equal(t, uint64(5), lowest)
	assert.Equal(t, uint64(24), highest)
	verifyRecords(t, 18, 24, records)

	// exactly 100% filled
	buffer = newEventRingBuffer(10)
	populate(buffer, 10)
	records, lowest, highest = buffer.GetEventsFromID(0, math.MaxUint64)
	assert.Equal(t, 10, len(records))
	assert.Equal(t, uint64(0), lowest)
	assert.Equal(t, uint64(9), highest)
	verifyRecords(t, 0, 9, records)

	// wrapped twice
	buffer = newEventRingBuffer(20)
	populate(buffer, 50)
	records, lowest, highest = buffer.GetEventsFromID(38, math.MaxUint64)
	assert.Equal(t, 12, len(records))
	assert.Equal(t, uint64(30), lowest)
	assert.Equal(t, uint64(49), highest)
	verifyRecords(t, 38, 49, records)

	// wrapped twice pos < e.head
	buffer = newEventRingBuffer(20)
	populate(buffer, 50)
	records, lowest, highest = buffer.GetEventsFromID(42, math.MaxUint64)
	assert.Equal(t, 8, len(records))
	assert.Equal(t, uint64(30), lowest)
	assert.Equal(t, uint64(49), highest)
	verifyRecords(t, 42, 49, records)

	// wrapped, have to count back - limited count 1st range
	buffer = newEventRingBuffer(20)
	populate(buffer, 25)
	records, lowest, highest = buffer.GetEventsFromID(18, 2)
	assert.Equal(t, 2, len(records))
	assert.Equal(t, uint64(5), lowest)
	assert.Equal(t, uint64(24), highest)
	verifyRecords(t, 18, 19, records)

	// wrapped, have to count back - limited count 2nd range
	buffer = newEventRingBuffer(20)
	populate(buffer, 25)
	records, lowest, highest = buffer.GetEventsFromID(18, 5)
	assert.Equal(t, 5, len(records))
	assert.Equal(t, uint64(5), lowest)
	assert.Equal(t, uint64(24), highest)
	verifyRecords(t, 18, 22, records)
}

func TestGetEventsFromId_WhenEmpty(t *testing.T) {
	buffer := newEventRingBuffer(20)
	records, lowest, highest := buffer.GetEventsFromID(18, math.MaxUint64)

	assert.Equal(t, uint64(0), lowest)
	assert.Equal(t, 0, len(records))
	assert.Equal(t, uint64(0), highest)
}

func TestGetEventsFromId_IdNotFound(t *testing.T) {
	// wrapped, id is too high
	buffer := newEventRingBuffer(20)
	populate(buffer, 25)
	records, lowest, highest := buffer.GetEventsFromID(311, math.MaxUint64)
	assert.Equal(t, uint64(5), lowest)
	assert.Equal(t, 0, len(records))
	assert.Equal(t, uint64(24), highest)

	// half filled, id is too high
	buffer = newEventRingBuffer(20)
	populate(buffer, 10)
	records, lowest, highest = buffer.GetEventsFromID(311, math.MaxUint64)
	assert.Equal(t, uint64(0), lowest)
	assert.Equal(t, 0, len(records))
	assert.Equal(t, uint64(9), highest)

	// wrapped twice, id is too low, we no longer have it
	buffer = newEventRingBuffer(20)
	populate(buffer, 50)
	records, lowest, highest = buffer.GetEventsFromID(18, math.MaxUint64)
	assert.Equal(t, uint64(30), lowest)
	assert.Equal(t, 0, len(records))
	assert.Equal(t, uint64(49), highest)
}

func populate(buffer *eventRingBuffer, count int) {
	for i := 0; i < count; i++ {
		buffer.Add(&si.EventRecord{
			TimestampNano: int64(i),
		})
	}
}

func verifyRecords(t *testing.T, start, stop int64, records []*si.EventRecord) {
	for i, j := start, int64(0); i != stop; {
		assert.Equal(t, i, records[j].TimestampNano)
		i++
		j++
	}
}
