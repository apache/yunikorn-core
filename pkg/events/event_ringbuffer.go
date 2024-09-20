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
	"strconv"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type eventRange struct {
	start uint64
	end   uint64
}

// eventRingBuffer A specialized circular buffer to store event objects.
//
// Unlike regular circular buffers, existing entries are never directly removed
// and new entries can be added if the buffer is full.
// In this case, the oldest entry is overwritten and can be collected by the GC.
// Each event has an ID; however, this mapping is not stored directly.
// If needed, we calculate the id of the event based on slice positions.
//
// Retrieving the records can be achieved with GetEventsFromID.
type eventRingBuffer struct {
	events       []*si.EventRecord
	capacity     uint64 // capacity of the buffer
	head         uint64 // position of the next element (no tail since we don't remove elements)
	full         bool   // indicates whether the buffer if full - once it is, it stays full unless the buffer is resized
	id           uint64 // unique id of an event record
	lowestId     uint64 // lowest id of an event record available in the buffer at any given time
	resizeOffset uint64 // used to aid the calculation of id->pos after resize (see id2pos)

	locking.RWMutex
}

// Add adds an event to the ring buffer. If the buffer is full, the oldest element is overwritten.
// This method never fails.
func (e *eventRingBuffer) Add(event *si.EventRecord) {
	e.Lock()
	defer e.Unlock()

	e.events[e.head] = event
	if !e.full {
		e.full = e.head == e.capacity-1
	} else {
		e.lowestId++
	}
	e.head = (e.head + 1) % e.capacity
	e.id++
}

// GetRecentEvents returns the most recent "count" elements from the ring buffer.
// It is allowed for "count" to be larger than the number of elements.
func (e *eventRingBuffer) GetRecentEvents(count uint64) []*si.EventRecord {
	e.RLock()
	defer e.RUnlock()

	lastID := e.getLastEventID()
	var startID uint64
	if lastID < count {
		startID = 0
	} else {
		startID = lastID - count + 1
	}

	history, _, _ := e.getEventsFromID(startID, count)
	return history
}

// GetEventsFromID returns "count" number of event records from id if possible. The id can be determined from
// the first call of the method - if it returns nothing because the id is not in the buffer, the lowest valid
// identifier is returned which can be used to get the first batch.
// If the caller does not want to pose limit on the number of events returned, "count" must be set to a high
// value, e.g. math.MaxUint64.
func (e *eventRingBuffer) GetEventsFromID(id uint64, count uint64) ([]*si.EventRecord, uint64, uint64) {
	e.RLock()
	defer e.RUnlock()

	return e.getEventsFromID(id, count)
}

// getEventsFromID unlocked version of GetEventsFromID
func (e *eventRingBuffer) getEventsFromID(id uint64, count uint64) ([]*si.EventRecord, uint64, uint64) {
	lowest := e.getLowestID()

	pos, idFound := e.id2pos(id)
	if !idFound {
		return nil, lowest, e.getLastEventID()
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
		return e.getEntriesFromRanges(r1, r2), lowest, e.getLastEventID()
	}

	// no wrapping: either limited by head position or by the count
	end := min(pos+count, e.head)
	return e.getEntriesFromRanges(&eventRange{
		start: pos,
		end:   end,
	}, nil), lowest, e.getLastEventID()
}

// GetLastEventID returns the value of the unique id counter.
// If the buffer is empty, it returns 0.
func (e *eventRingBuffer) GetLastEventID() uint64 {
	e.RLock()
	defer e.RUnlock()
	return e.getLastEventID()
}

// unlocked version of GetLastEventID()
func (e *eventRingBuffer) getLastEventID() uint64 {
	if e.id == 0 {
		return 0
	}
	return e.id - 1 // e.id is the next ID to be used
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
// If the event is present, the position will be returned and the found flag will be true.
// If the event ID is not present, the position returned is 0 and the flag is false.
func (e *eventRingBuffer) id2pos(id uint64) (uint64, bool) {
	// id out of range?
	if id < e.lowestId || id >= e.id {
		return 0, false
	}

	// resizeOffset tells how many elements were "shifted out" after resizing the buffer
	// eg a buffer with 10 elements is full, then gets resized to 6
	// the first element at index 0 is no longer 0 or the multiples of 10, but 4, 16, 22, etc.
	return (id - e.resizeOffset) % e.capacity, true
}

// getLowestID returns the current lowest available id in the buffer.
func (e *eventRingBuffer) getLowestID() uint64 {
	return e.lowestId
}

func newEventRingBuffer(capacity uint64) *eventRingBuffer {
	return &eventRingBuffer{
		capacity: capacity,
		events:   make([]*si.EventRecord, capacity),
	}
}

// called from Resize(), this function updates the lowest event id available in the buffer
func (e *eventRingBuffer) updateLowestID(beginSize, endSize uint64) {
	// if buffer size is increasing, lowestId stays the same
	if beginSize < endSize {
		return
	}

	// bufferSize is shrinking
	// if the number of events is < newSize, then no change
	if (e.id - e.getLowestID()) <= endSize {
		return
	}

	// number of events > newSize => get last 'newSize' events
	e.lowestId = e.id - endSize
}

// Resize resizes the existing ring buffer
// this method will be called upon configuration reload
func (e *eventRingBuffer) Resize(newSize uint64) {
	e.Lock()
	defer e.Unlock()

	if newSize == e.capacity {
		return
	}

	initialSize := e.capacity
	newEvents := make([]*si.EventRecord, newSize)
	numEventsToCopy := min(e.id-e.getLowestID(), newSize)

	// Calculate the index from where to start copying (the oldest event)
	startIndex := (e.head + e.capacity - numEventsToCopy) % e.capacity
	endIndex := (startIndex + numEventsToCopy - 1) % e.capacity

	prevLowestId := e.getLowestID()
	e.updateLowestID(initialSize, newSize)
	newLowestId := e.getLowestID()

	if prevLowestId != newLowestId {
		log.Log(log.Events).Info("event buffer resized, some events were lost",
			zap.String("previous lowest event id", strconv.FormatUint(prevLowestId, 10)),
			zap.String("new lowest event id", strconv.FormatUint(newLowestId, 10)))
	}

	// Copy existing events to the new buffer
	// We determine the range of elements to copy based on the relative positions of the head and tail in the circular buffer.
	// If the tail is ahead of the head (wrap-around scenario), we copy in two steps to ensure the correct order.
	if endIndex >= startIndex {
		copy(newEvents, e.events[startIndex:endIndex+1])
	} else {
		copy(newEvents, e.events[startIndex:])
		copy(newEvents[e.capacity-startIndex:], e.events[:endIndex+1])
	}

	e.capacity = newSize
	e.events = newEvents
	e.head = numEventsToCopy % newSize
	e.resizeOffset = e.lowestId
	e.full = numEventsToCopy == e.capacity
}
