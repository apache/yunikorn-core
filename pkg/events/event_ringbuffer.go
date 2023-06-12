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
	"time"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// default value of "latest" if there are no elements, helps in GetLatestEntries()
const latestUnset int64 = math.MinInt64

var now = time.Now

// eventRingBuffer A specialized circular buffer to store event objects.
//
// Unlike to regular circular buffers, new entries can be added if the buffer is full. In this case,
// the oldest entry is overwritten. This is not a classic enqueue operation, so it's named differently.
//
// Retrieving the records can be achieved with GetLatestEntriesCount and GetLatestEntries. Since these do not
// remove the elements, they are not regular dequeue operations either.
//
// Entries have a maximum lifespan defined in nanoseconds. Cleanup of expired objects occurs when a call to
// RemoveExpiredEntries is made.
type eventRingBuffer struct {
	events        []*si.EventRecord
	capacity      int
	noElements    int
	head          int
	tail          int
	latest        int64
	lifetimeNanos int64
}

// Add adds an event to the ring buffer. If the buffer is full, the oldest element is overwritten.
// This method never fails.
func (e *eventRingBuffer) Add(event *si.EventRecord) {
	full := false
	if e.noElements == e.capacity {
		full = true
	}
	e.events[e.tail] = event
	e.tail = e.next(e.tail)
	e.latest = event.TimestampNano

	if full {
		e.head = e.tail
		return
	}

	e.noElements++
}

// GetLatestEntriesCount returns most recent items. The amount is defined by "count".
func (e *eventRingBuffer) GetLatestEntriesCount(count int) []*si.EventRecord {
	if e.noElements == 0 {
		return nil
	}

	records := make([]*si.EventRecord, 0)
	for i := e.prev(e.tail); ; {
		records = append(records, e.events[i])
		if len(records) == count || i == e.head {
			reverse(records)
			return records
		}

		i = e.prev(i)
	}
}

// GetLatestEntries returns the most recent items whose age is younger than the current time minus interval.
func (e *eventRingBuffer) GetLatestEntries(interval time.Duration) []*si.EventRecord {
	unixNow := now().UnixNano()
	startTime := unixNow - interval.Nanoseconds()

	if e.noElements == 0 || startTime > e.latest {
		return nil
	}

	records := make([]*si.EventRecord, 0)
	for i := e.head; ; {
		if e.events[i].TimestampNano >= startTime {
			records = append(records, e.events[i])
		}

		next := e.next(i)
		if next == e.tail {
			break
		}
		i = next
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

func newEventRingBuffer(capacity int, eventLifeTime time.Duration) *eventRingBuffer {
	return &eventRingBuffer{
		capacity:      capacity,
		events:        make([]*si.EventRecord, capacity),
		lifetimeNanos: eventLifeTime.Nanoseconds(),
		latest:        latestUnset,
	}
}
