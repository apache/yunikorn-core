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

type resultState int

const (
	resultOK resultState = iota
	bufferEmpty
	idNotFound
)

type eventRange struct {
	start uint64
	end   uint64
}

// eventRingBuffer A specialized circular buffer to store event objects.
//
// Unlike to regular circular buffers, existing entries are never directly removed and new entries can be added if the buffer is full.
// In this case, the oldest entry is overwritten and can be collected by the GC.
//
// Retrieving the records can be achieved with GetEventsFromID and GetRecentEntries.
type eventRingBuffer struct {
	events   []*si.EventRecord
	capacity uint64
	idx      uint64
	full     bool

	id       uint64 // increasing unique id
	startId  uint64 // id of the message at index 0
	lowestId uint64 // the lowest available id in the buffer (pointed at by e.idx when full)

	sync.RWMutex
}

// Add adds an event to the ring buffer. If the buffer is full, the oldest element is overwritten.
// This method never fails.
// Each event has an ID, however, this mapping is not stored directly. Instead, we store the starting ID at
// index 0 and the lowest available ID.
func (e *eventRingBuffer) Add(event *si.EventRecord) {
	e.Lock()
	defer e.Unlock()

	e.events[e.idx] = event
	if e.idx == 0 {
		e.startId = e.id
	}
	if e.idx == e.capacity-1 {
		e.full = true
	}
	e.idx = (e.idx + 1) % e.capacity
	if e.full {
		// once the buffer becomes full, we keep incrementing this value
		// this is the id of the element which is about to be overwritten in the next Add() call
		e.lowestId++
	}

	e.id++
}

func (e *eventRingBuffer) GetEventsFromID(id uint64) ([]*si.EventRecord, resultState) {
	e.RLock()
	defer e.RUnlock()

	storedId, state := e.id2pos(id)
	if state != resultOK {
		return nil, state
	}

	if e.full {
		r1 := &eventRange{
			start: storedId,
			end:   e.capacity,
		}
		r2 := &eventRange{
			start: 0,
			end:   e.idx,
		}
		return e.getEntriesFromRanges(r1, r2), resultOK
	}

	return e.getEntriesFromRanges(&eventRange{
		start: storedId,
		end:   e.idx,
	}, nil), resultOK
}

func (e *eventRingBuffer) GetRecentEntries(count uint64) ([]*si.EventRecord, uint64, resultState) {
	e.RLock()
	defer e.RUnlock()

	if !e.full && e.idx == 0 {
		return nil, 0, bufferEmpty
	}

	if e.full {
		if count > e.capacity {
			count = e.capacity
		}
		if count > e.idx {
			startPos := e.capacity - count + e.idx
			r1 := &eventRange{
				start: startPos,
				end:   e.capacity,
			}
			r2 := &eventRange{
				start: 0,
				end:   e.idx,
			}

			return e.getEntriesFromRanges(r1, r2), e.pos2id(startPos), resultOK
		}

		startIdx := e.idx - count
		return e.getEntriesFromRanges(&eventRange{
			start: startIdx,
			end:   e.idx,
		}, nil), e.pos2id(startIdx), resultOK
	}

	if count > e.idx {
		count = e.idx
	}
	startIdx := e.idx - count
	return e.getEntriesFromRanges(&eventRange{
		start: startIdx,
		end:   e.idx,
	}, nil), startIdx, resultOK
}

func (e *eventRingBuffer) getEntriesFromRanges(r1, r2 *eventRange) []*si.EventRecord {
	var total uint64
	var src []*si.EventRecord
	if r2 == nil {
		total = r1.end - r1.start
		src = e.events[r1.start:r1.end]
	} else {
		total = (r1.end - r1.start) + (r2.end - r2.start)
		src = e.events[r1.start:r1.end]
		src = append(src, e.events[r2.start:r2.end]...)
	}

	dst := make([]*si.EventRecord, total)
	copy(dst, src)
	return dst
}

// translates slice position to unique id
func (e *eventRingBuffer) pos2id(pos uint64) uint64 {
	if e.full && pos > e.idx {
		return e.id - e.idx - e.capacity + pos
	}

	return e.id - e.idx + pos
}

// translates unique id to a slice position (index)
func (e *eventRingBuffer) id2pos(id uint64) (uint64, resultState) {
	pos := id % e.capacity
	var calculatedID uint64 // calculated ID based on index values
	if pos > e.idx {
		diff := pos - e.idx
		calculatedID = e.lowestId - 1 + diff
	} else {
		calculatedID = e.startId + pos
	}

	if !e.full {
		if e.idx == 0 && e.events[0] == nil {
			return 0, bufferEmpty
		}
		if pos >= e.idx {
			// "pos" is not in the [0..idx-1] range
			return 0, idNotFound
		}
	}

	if calculatedID != id {
		return calculatedID, idNotFound
	}

	return pos, resultOK
}

func newEventRingBuffer(capacity uint64) *eventRingBuffer {
	return &eventRingBuffer{
		capacity: capacity,
		events:   make([]*si.EventRecord, capacity),
	}
}
