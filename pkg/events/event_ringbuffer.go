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
	"sync"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type eventRange struct {
	start uint64
	end   uint64
}

// eventRingBuffer A specialized circular buffer to store event objects.
//
// Unlike to regular circular buffers, existing entries are never directly removed and new entries can be added if the buffer is full.
// In this case, the oldest entry is overwritten and can be collected by the GC.
// Each event has an ID, however, this mapping is not stored directly. If needed, we calculate the id
// of the event based on slice positions.
//
// Retrieving the records can be achieved with GetEventsFromID and GetRecentEntries.
type eventRingBuffer struct {
	events   []*si.EventRecord
	capacity uint64 // capacity of the buffer, never changes
	head     uint64 // position of the next element (no tail since we don't remove elements)
	full     bool   // indicates whether the buffer if full - once it is, it stays full
	id       uint64 // unique id of an event record

	sync.RWMutex
}

// Add adds an event to the ring buffer. If the buffer is full, the oldest element is overwritten.
// This method never fails.
func (e *eventRingBuffer) Add(event *si.EventRecord) {
	e.Lock()
	defer e.Unlock()

	e.events[e.head] = event
	if !e.full {
		e.full = e.head == e.capacity-1
	}
	e.head = (e.head + 1) % e.capacity
	e.id++
}

// GetEventsFromID returns "count" number of event records from id if possible. The id can be determined from
// the first call of the method - if it returns nothing because the id is not in the buffer, the lowest valid
// identifier is returned which can be used to get the first batch.
// If the caller does not want to pose limit on the number of events returned, "count" must be set to a high
// value, e.g. math.MaxUint64.
func (e *eventRingBuffer) GetEventsFromID(id uint64, count uint64) ([]*si.EventRecord, uint64) {
	e.RLock()
	defer e.RUnlock()
	lowest := e.getLowestId()

	pos, idFound := e.id2pos(id)
	if !idFound {
		return nil, lowest
	}
	// limit count to the capacity
	count = min(count, e.capacity)
	// possible wrap case full buffer and pos after or on the current head
	if e.full && pos >= e.head {
		// first range never passes the end of the slice
		end := min(pos+count, e.capacity)
		r1 := &eventRange{
			start: pos,
			end:   end,
		}
		// second range only if still events left to fetch
		var r2 *eventRange
		end = pos + count - e.capacity
		if end > 0 {
			// never fetch pass the current head
			end = min(end, e.head)
			r2 = &eventRange{
				start: 0,
				end:   end,
			}
		}
		return e.getEntriesFromRanges(r1, r2), lowest
	}

	// no wrapping: either limited by head position or by the count
	end := min(pos+count, e.head)
	return e.getEntriesFromRanges(&eventRange{
		start: pos,
		end:   end,
	}, nil), lowest
}

// min a utility function to return the smallest value of two unsigned int
func min(a, b uint64) uint64 {
	m := a
	if b < a {
		m = b
	}
	return m
}

// GetLastEventID returns the value of the unique id counter
func (e *eventRingBuffer) GetLastEventID() uint64 {
	return e.id
}

// getEntriesFromRanges retrieves the event records based on pre-calculated ranges. We have two
// ranges if the buffer is full and the requested start position is behind the current head.
// Example: a buffer of capacity 20 is wrapped, head is at 10, and we want events from position 15. This means
// two ranges (15->20, 0->9).
func (e *eventRingBuffer) getEntriesFromRanges(r1, r2 *eventRange) []*si.EventRecord {
	if r2 == nil {
		dst := make([]*si.EventRecord, r1.end-r1.start)
		copy(dst, e.events[r1.start:])
		return dst
	}

	total := (r1.end - r1.start) + (r2.end - r2.start)
	dst := make([]*si.EventRecord, total)
	copy(dst, e.events[r1.start:])
	nextIdx := r1.end - r1.start
	copy(dst[nextIdx:], e.events[r2.start:])
	return dst
}

// id2pos translates the unique event ID to an index in the event slice.
// If the event is present the position will be returned and the found flag will be true.
// In the case that the event ID is not present the position returned is 0 and the flag false.
func (e *eventRingBuffer) id2pos(id uint64) (uint64, bool) {
	pos := id % e.capacity
	var calculatedID uint64 // calculated ID based on index values
	if pos > e.head {
		diff := pos - e.head
		calculatedID = e.getLowestId() + diff
	} else {
		pId := e.id - 1
		idAtZero := pId - (pId % e.capacity) // unique id at slice position 0
		calculatedID = idAtZero + pos
	}

	if !e.full {
		if e.head == 0 {
			// empty
			return 0, false
		}
		if pos >= e.head {
			// "pos" is not in the [0..head-1] range
			return 0, false
		}
	}

	if calculatedID != id {
		return calculatedID, false
	}

	return pos, true
}

// getLowestId returns the current lowest available id in the buffer.
func (e *eventRingBuffer) getLowestId() uint64 {
	if !e.full {
		return 0
	}

	return e.id - e.capacity
}

func newEventRingBuffer(capacity uint64) *eventRingBuffer {
	return &eventRingBuffer{
		capacity: capacity,
		events:   make([]*si.EventRecord, capacity),
	}
}
