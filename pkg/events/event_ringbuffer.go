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
	"sync"
	"time"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// default value of "latest" if there are no elements, helps in GetLatestEntries()
const latestUnset int64 = math.MinInt64

var now = time.Now

// eventRingBuffer A specialized circular buffer to store event objects.
//
// Unlike to regular circular buffers, existing entries are never directly removed and new entries can be added if the buffer is full.
// In this case, the oldest entry is overwritten and can be collected by the GC.
//
// Retrieving the records can be achieved with GetLatestEntriesCount, GetLatestEntries and GetEventsFromPosition.
type eventRingBuffer struct {
	events   []*si.EventRecord
	capacity int
	idx      int
	latest   int64
	full     bool
	sync.RWMutex
}

// Add adds an event to the ring buffer. If the buffer is full, the oldest element is overwritten.
// This method never fails.
func (e *eventRingBuffer) Add(event *si.EventRecord) {
	e.Lock()
	defer e.Unlock()

	e.events[e.idx] = event
	e.idx = e.next(e.idx)
	if e.idx == 0 {
		// wrapped
		e.full = true
	}
	e.latest = event.TimestampNano
}

// GetLatestEntriesCount returns most recent items. The amount is defined by "count".
func (e *eventRingBuffer) GetLatestEntriesCount(count int) []*si.EventRecord {
	e.RLock()
	defer e.RUnlock()

	if !e.full && e.idx == 0 {
		return nil
	}

	records := make([]*si.EventRecord, 0, count)
	var stop int
	if e.full {
		stop = e.idx
	} else {
		stop = 0
	}
	for i := e.prev(e.idx); ; {
		records = append(records, e.events[i])
		if len(records) == count || i == stop {
			reverse(records)
			return records
		}

		i = e.prev(i)
	}
}

// GetLatestEntries returns the most recent items whose age is younger than the current time minus interval.
func (e *eventRingBuffer) GetLatestEntries(interval time.Duration) []*si.EventRecord {
	e.RLock()
	defer e.RUnlock()

	unixNow := now().UnixNano()
	startTime := unixNow - interval.Nanoseconds()

	if (!e.full && e.idx == 0) || (startTime > e.latest) {
		return nil
	}

	var start, count int
	if e.full {
		count = e.capacity
		start = e.idx
	} else {
		count = e.idx
		start = 0
	}

	records := make([]*si.EventRecord, 0)
	for i, c := start, 0; c != count; c++ {
		if e.events[i].TimestampNano >= startTime {
			records = append(records, e.events[i])
		}

		next := e.next(i)
		i = next
	}

	return records
}

func (e *eventRingBuffer) GetEventsFromPosition(pos int) []*si.EventRecord {
	e.RLock()
	defer e.RUnlock()

	if !e.full && pos >= e.idx {
		// invalid position, "pos" is not in the [0..idx] range
		return nil
	}

	records := make([]*si.EventRecord, 0)
	for i := pos; i != e.idx; i = e.next(i) {
		records = append(records, e.events[i])
	}

	return records
}

func (e *eventRingBuffer) prev(i int) int {
	i--
	if i == -1 {
		i = e.capacity - 1
	}

	return i
}

func (e *eventRingBuffer) next(i int) int {
	return (i + 1) % e.capacity
}

func reverse(r []*si.EventRecord) {
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
}

func newEventRingBuffer(capacity int) *eventRingBuffer {
	return &eventRingBuffer{
		capacity: capacity,
		events:   make([]*si.EventRecord, capacity),
		latest:   latestUnset,
	}
}
