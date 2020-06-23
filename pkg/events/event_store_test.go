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

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func TestStoreAndRetrieveAllocationAsk(t *testing.T) {
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

func TestStoreAndRetrieveMultipleAllocationAsks(t *testing.T) {
	store := newEventStoreImpl()

	event1 := si.EventRecord{
		Type:     si.EventRecord_REQUEST,
		ObjectID: "alloc1",
		GroupID:  "app1",
		Reason:   "reason1",
		Message:  "message1",
	}
	event2 := si.EventRecord{
		Type:     si.EventRecord_REQUEST,
		ObjectID: "alloc1",
		GroupID:  "app1",
		Reason:   "reason2",
		Message:  "message2",
	}
	event3 := si.EventRecord{
		Type:     si.EventRecord_REQUEST,
		ObjectID: "alloc2",
		GroupID:  "app2",
		Reason:   "reason3",
		Message:  "message3",
	}
	store.Store(&event1)
	store.Store(&event2)
	store.Store(&event3)
	records := store.CollectEvents()
	assert.Equal(t, len(records), 2)
	for _, record := range records {
		assert.Equal(t, record.Type, si.EventRecord_REQUEST)
		switch {
		case record.ObjectID == "alloc1":
			assert.Equal(t, record.GroupID, "app1")
			assert.Equal(t, record.Message, "message2")
			assert.Equal(t, record.Reason, "reason2")
		case record.ObjectID == "alloc2":
			assert.Equal(t, record.GroupID, "app2")
			assert.Equal(t, record.Message, "message3")
			assert.Equal(t, record.Reason, "reason3")
		default:
			t.Fatalf("Unexpected allocation found")
		}
	}
}

func TestStoreWithLimitedSize(t *testing.T) {
	maxEventStoreSize = 2

	store := newEventStoreImpl()
	event1 := si.EventRecord{
		Type:     si.EventRecord_REQUEST,
		ObjectID: "alloc1",
		GroupID:  "app1",
		Reason:   "reason1",
		Message:  "message1",
	}
	event2 := si.EventRecord{
		Type:     si.EventRecord_REQUEST,
		ObjectID: "alloc2",
		GroupID:  "app2",
		Reason:   "reason2",
		Message:  "message2",
	}
	event3 := si.EventRecord{
		Type:     si.EventRecord_REQUEST,
		ObjectID: "alloc3",
		GroupID:  "app3",
		Reason:   "reason3",
		Message:  "message3",
	}
	store.Store(&event1)
	store.Store(&event2)
	store.Store(&event3)
	records := store.CollectEvents()
	assert.Equal(t, len(records), 2)
}
