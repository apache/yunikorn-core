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
}

func TestRingBuffer_Add_WhenFull(t *testing.T) {
	buffer := newEventRingBuffer(10)
	populate(buffer, 13)

	assert.Equal(t, uint64(3), buffer.head)
	assert.Equal(t, true, buffer.full)
}

func TestGetRecentEntries(t *testing.T) {
	// single element
	buffer := newEventRingBuffer(20)
	populate(buffer, 1)
	records, id, state := buffer.GetRecentEntries(1)
	assert.Equal(t, resultOK, state)
	assert.Equal(t, uint64(0), id)
	assert.Equal(t, 1, len(records))
	verifyRecords(t, 0, 0, records)

	// half filled
	buffer = newEventRingBuffer(20)
	populate(buffer, 10)
	records, id, state = buffer.GetRecentEntries(3)
	assert.Equal(t, resultOK, state)
	assert.Equal(t, uint64(7), id)
	assert.Equal(t, 3, len(records))
	verifyRecords(t, 7, 9, records)

	// "count" too high
	buffer = newEventRingBuffer(20)
	populate(buffer, 10)
	records, id, state = buffer.GetRecentEntries(15)
	assert.Equal(t, resultOK, state)
	assert.Equal(t, uint64(0), id)
	assert.Equal(t, 10, len(records))
	verifyRecords(t, 0, 9, records)
}

func TestGetRecentEntries_WhenFull(t *testing.T) {
	// exactly 100% full
	buffer := newEventRingBuffer(20)
	populate(buffer, 20)
	records, id, state := buffer.GetRecentEntries(3)
	assert.Equal(t, resultOK, state)
	assert.Equal(t, uint64(17), id)
	assert.Equal(t, 3, len(records))
	verifyRecords(t, 17, 19, records)

	// wrapped - count > head
	buffer = newEventRingBuffer(20)
	populate(buffer, 23)
	records, id, state = buffer.GetRecentEntries(5)
	assert.Equal(t, resultOK, state)
	assert.Equal(t, uint64(18), id)
	assert.Equal(t, 5, len(records))
	verifyRecords(t, 18, 22, records)

	// wrapped - count < head
	buffer = newEventRingBuffer(20)
	populate(buffer, 25)
	records, id, state = buffer.GetRecentEntries(3)
	assert.Equal(t, resultOK, state)
	assert.Equal(t, uint64(22), id)
	assert.Equal(t, 3, len(records))
	verifyRecords(t, 22, 24, records)

	// "count" too high
	buffer = newEventRingBuffer(10)
	populate(buffer, 15)
	records, id, state = buffer.GetRecentEntries(20)
	assert.Equal(t, resultOK, state)
	assert.Equal(t, uint64(15), id)
	assert.Equal(t, 10, len(records))
	verifyRecords(t, 5, 14, records)
}

func TestGetRecentEntries_WhenEmpty(t *testing.T) {
	buffer := newEventRingBuffer(10)
	records, _, state := buffer.GetRecentEntries(3)
	assert.Equal(t, bufferEmpty, state)
	assert.Equal(t, 0, len(records))
}

func TestGetEventsFromId(t *testing.T) {
	// single element
	buffer := newEventRingBuffer(10)
	populate(buffer, 1)
	records, state := buffer.GetEventsFromID(0)
	assert.Equal(t, 1, len(records))
	assert.Equal(t, resultOK, state)
	verifyRecords(t, 0, 0, records)

	// half filled
	buffer = newEventRingBuffer(20)
	populate(buffer, 10)
	records, state = buffer.GetEventsFromID(5)
	assert.Equal(t, 5, len(records))
	assert.Equal(t, resultOK, state)
	verifyRecords(t, 5, 9, records)
}

func TestGetEventsFromId_WhenFull(t *testing.T) {
	// wrapped, have to count back
	buffer := newEventRingBuffer(20)
	populate(buffer, 25)
	records, state := buffer.GetEventsFromID(18)
	assert.Equal(t, 7, len(records))
	assert.Equal(t, resultOK, state)
	verifyRecords(t, 18, 24, records)

	// exactly 100% filled
	buffer = newEventRingBuffer(10)
	populate(buffer, 10)
	records, state = buffer.GetEventsFromID(0)
	assert.Equal(t, 10, len(records))
	assert.Equal(t, resultOK, state)
	verifyRecords(t, 0, 9, records)

	// wrapped twice
	buffer = newEventRingBuffer(20)
	populate(buffer, 50)
	records, state = buffer.GetEventsFromID(38)
	assert.Equal(t, 12, len(records))
	assert.Equal(t, resultOK, state)
	verifyRecords(t, 38, 49, records)

	// wrapped twice pos < e.head
	buffer = newEventRingBuffer(20)
	populate(buffer, 50)
	records, state = buffer.GetEventsFromID(42)
	assert.Equal(t, 8, len(records))
	assert.Equal(t, resultOK, state)
	verifyRecords(t, 42, 49, records)
}

func TestGetEventsFromId_WhenEmpty(t *testing.T) {
	buffer := newEventRingBuffer(20)
	records, state := buffer.GetEventsFromID(18)

	assert.Equal(t, 0, len(records))
	assert.Equal(t, bufferEmpty, state)
}

func TestGetEventsFromId_IdNotFound(t *testing.T) {
	// wrapped, id is too high
	buffer := newEventRingBuffer(20)
	populate(buffer, 25)
	records, state := buffer.GetEventsFromID(311)
	assert.Equal(t, 0, len(records))
	assert.Equal(t, idNotFound, state)

	// half filled, id is too high
	buffer = newEventRingBuffer(20)
	populate(buffer, 10)
	records, state = buffer.GetEventsFromID(311)
	assert.Equal(t, 0, len(records))
	assert.Equal(t, idNotFound, state)

	// wrapped twice, id is too low, we no longer have it
	buffer = newEventRingBuffer(20)
	populate(buffer, 50)
	records, state = buffer.GetEventsFromID(18)
	assert.Equal(t, 0, len(records))
	assert.Equal(t, idNotFound, state)
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
