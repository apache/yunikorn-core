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

	"gotest.tools/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// the fields of an event should match after stored and retrieved
func TestStoreAndRetrieve(t *testing.T) {
	store := newEventStoreImpl()
	event := si.EventRecord{
		Type:     si.EventRecord_REQUEST,
		ObjectID: "alloc1",
		GroupID:  "app1",
		Reason:   "reason",
		Message:  "message",
	}
	store.Store(&event)

	records := store.CollectEvents()
	assert.Equal(t, len(records), 1)
	record := records[0]
	assert.Equal(t, record.Type, si.EventRecord_REQUEST)
	assert.Equal(t, record.ObjectID, "alloc1")
	assert.Equal(t, record.GroupID, "app1")
	assert.Equal(t, record.Message, "message")
	assert.Equal(t, record.Reason, "reason")

	// calling CollectEvents erases the eventChannel map
	records = store.CollectEvents()
	assert.Equal(t, len(records), 0)
}

// the store should store only one event per ObjectID and
// that should be the least recently added
func TestMultiEventsSameObjectID(t *testing.T) {
	store := newEventStoreImpl()
	for i := 0; i < 3; i++ {
		event := &si.EventRecord{
			Type:     si.EventRecord_REQUEST,
			ObjectID: "alloc" + strconv.Itoa(i/2),
			GroupID:  "app" + strconv.Itoa(i/2),
			Reason:   "reason" + strconv.Itoa(i),
			Message:  "message" + strconv.Itoa(i),
		}
		store.Store(event)
	}
	records := store.CollectEvents()
	assert.Equal(t, len(records), 2)
	for _, record := range records {
		assert.Equal(t, record.Type, si.EventRecord_REQUEST)
		switch {
		case record.ObjectID == "alloc0":
			assert.Equal(t, record.GroupID, "app0")
			assert.Equal(t, record.Reason, "reason1")
			assert.Equal(t, record.Message, "message1")
		case record.ObjectID == "alloc1":
			assert.Equal(t, record.GroupID, "app1")
			assert.Equal(t, record.Reason, "reason2")
			assert.Equal(t, record.Message, "message2")
		default:
			t.Fatalf("Unexpected allocation found")
		}
	}
}

// if we push more events to the EventStore than its
// allowed maximum, those that couldn't fit will be omitted
func TestStoreWithLimitedSize(t *testing.T) {
	maxEventStoreSize = 3

	store := newEventStoreImpl()
	for i := 0; i < 5; i++ {
		event := &si.EventRecord{
			Type:     si.EventRecord_REQUEST,
			ObjectID: "alloc-" + strconv.Itoa(i),
			GroupID:  "app",
			Reason:   "reason",
			Message:  "message",
		}
		store.Store(event)
	}
	records := store.CollectEvents()
	assert.Equal(t, len(records), 3)
}
