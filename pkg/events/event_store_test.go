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
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp/cmpopts"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// the fields of an event should match after stored and retrieved
func TestStoreAndRetrieve(t *testing.T) {
	store := newEventStore(1000)
	event1 := &si.EventRecord{
		Type:              si.EventRecord_REQUEST,
		EventChangeDetail: si.EventRecord_DETAILS_NONE,
		EventChangeType:   si.EventRecord_NONE,
		ObjectID:          "alloc1",
		ReferenceID:       "app1",
		Message:           "message",
	}
	event2 := &si.EventRecord{
		Type:              si.EventRecord_REQUEST,
		EventChangeDetail: si.EventRecord_DETAILS_NONE,
		EventChangeType:   si.EventRecord_NONE,
		ObjectID:          "alloc2",
		ReferenceID:       "app1",
		Message:           "message",
	}
	store.Store(event1)
	store.Store(event2)

	records := store.CollectEvents()
	assert.Equal(t, len(records), 2)
	assert.DeepEqual(t, records[0], event1, cmpopts.IgnoreUnexported(si.EventRecord{}))
	assert.DeepEqual(t, records[1], event2, cmpopts.IgnoreUnexported(si.EventRecord{}))

	// ensure that the underlying array of the return slice of CollectEvents() isn't the same as the one in EventStore.events
	newSliceData := unsafe.SliceData(records)           // pointer to underlying array of the return slice of EventStore.CollectEvents()
	internalSliceData := unsafe.SliceData(store.events) // pointer to underlying array of EventStore.events
	assert.Check(t, newSliceData != internalSliceData)

	// ensure modify EventStore.events won't affect the return slice of store.CollectEvents()
	store.events[0] = event2
	assert.DeepEqual(t, records[0], event1, cmpopts.IgnoreUnexported(si.EventRecord{}))

	// calling CollectEvents erases the eventChannel map
	records = store.CollectEvents()
	assert.Equal(t, len(records), 0)
}

// if we push more events to the EventStore than its
// allowed maximum, those that couldn't fit will be omitted
func TestStoreWithLimitedSize(t *testing.T) {
	store := newEventStore(3)

	for i := 0; i < 5; i++ {
		event := &si.EventRecord{
			Type:        si.EventRecord_REQUEST,
			ObjectID:    "alloc-" + strconv.Itoa(i),
			ReferenceID: "app",
			Message:     "message",
		}
		store.Store(event)
	}
	records := store.CollectEvents()
	assert.Equal(t, len(records), 3)
}

func TestSetStoreSize(t *testing.T) {
	store := newEventStore(5)

	// validate that store.CollectEvents() doesn't create a new slice for store.events if the store size remain unchanged after EventStore is initialized.
	oldSliceData := unsafe.SliceData(store.events)
	store.CollectEvents()
	newSliceData := unsafe.SliceData(store.events)
	assert.Check(t, oldSliceData == newSliceData)

	// store 5 events
	for i := 0; i < 5; i++ {
		store.Store(&si.EventRecord{
			Type:              si.EventRecord_REQUEST,
			EventChangeDetail: si.EventRecord_DETAILS_NONE,
			EventChangeType:   si.EventRecord_NONE,
			ObjectID:          "alloc" + strconv.Itoa(i),
			ReferenceID:       "app1",
		})
	}

	// set smaller size
	store.SetStoreSize(3)
	assert.Equal(t, uint64(3), store.size)

	// collect events & make sure events are not lost
	events := store.CollectEvents()
	assert.Equal(t, 5, len(events))
	assert.Equal(t, 3, len(store.events))

	// validate that store.CollectEvents() create a new slice for store.events after the store size has changed
	newSliceData = unsafe.SliceData(store.events)
	assert.Check(t, oldSliceData != newSliceData)
}
