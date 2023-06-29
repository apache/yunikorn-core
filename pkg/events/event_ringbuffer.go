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

func (e *eventRingBuffer) GetEventsFromID(id uint64, count uint64) ([]*si.EventRecord, uint64) {
	e.RLock()
	defer e.RUnlock()
	lowest := e.getLowestId()

	pos, idFound := e.id2pos(id)
	if !idFound {
		return nil, lowest
	}

	if e.full && pos > e.head {
		r1 := &eventRange{
			start: pos,
			end:   e.capacity,
		}
		r2 := &eventRange{
			start: 0,
			end:   e.head,
		}
		return e.getEntriesFromRanges(r1, r2, count), lowest
	}

	end := e.head
	if e.full && e.head == 0 {
		// Special case: buffer is full and head is pointing at the beginning.
		// Need to set explicitly to get [0..capacity] range because e.head
		// never points to e.capacity.
		end = e.capacity
	}
	return e.getEntriesFromRanges(&eventRange{
		start: pos,
		end:   end,
	}, nil, count), lowest
}

func (e *eventRingBuffer) GetLastEventID() uint64 {
	return e.id
}

func (e *eventRingBuffer) getEntriesFromRanges(r1, r2 *eventRange, count uint64) []*si.EventRecord {
	r1total := r1.end - r1.start

	if r2 == nil || count <= r1total {
		total := r1.end - r1.start
		end := r1.end
		if count < total {
			total = count
			end = r1.start + count
		}
		dst := make([]*si.EventRecord, total)
		copy(dst, e.events[r1.start:end])
		return dst
	}

	r2total := r2.end - r2.start
	total := r1total + r2total
	if count < total {
		r2.end -= total - count
		total = count
	}
	dst := make([]*si.EventRecord, total)
	copy(dst, e.events[r1.start:])
	nextIdx := r1.end - r1.start
	copy(dst[nextIdx:], e.events[r2.start:r2.end])
	return dst
}

// translates slice position to unique id
func (e *eventRingBuffer) pos2id(pos uint64) uint64 {
	if e.full && pos > e.head {
		return e.id - e.head - e.capacity + pos
	}

	return e.id - e.head + pos
}

// translates unique id to a slice position (index)
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
