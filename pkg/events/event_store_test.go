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
	ask := si.AllocationAsk{
		AllocationKey: "alloc1",
	}
	event := baseEvent{
		source:  &ask,
		group:   "app1",
		reason:  "reason",
		message: "message",
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

	// calling CollectEvents erases the events map
	records = store.CollectEvents()
	assert.Equal(t, len(records), 0)
}

func TestStoreAndRetrieveMultipleAllocationAsks(t *testing.T) {
	store := newEventStoreImpl()
	ask1 := si.AllocationAsk{
		AllocationKey: "alloc1",
	}
	ask2 := si.AllocationAsk{
		AllocationKey: "alloc2",
	}
	event1 := baseEvent{
		source:  &ask1,
		group:   "app1",
		reason:  "reason1",
		message: "message1",
	}
	event2 := baseEvent{
		source:  &ask1,
		group:   "app1",
		reason:  "reason2",
		message: "message2",
	}
	event3 := baseEvent{
		source:  &ask2,
		group:   "app2",
		reason:  "reason3",
		message: "message3",
	}
	store.Store(&event1)
	store.Store(&event2)
	store.Store(&event3)
	records := store.CollectEvents()
	assert.Equal(t, len(records), 2)
	for _, record := range records {
		assert.Equal(t, record.Type, si.EventRecord_REQUEST)
		if record.ObjectID == "alloc1" {
			assert.Equal(t, record.GroupID, "app1")
			assert.Equal(t, record.Message, "message2")
			assert.Equal(t, record.Reason, "reason2")
		} else if record.ObjectID == "alloc2" {
			assert.Equal(t, record.GroupID, "app2")
			assert.Equal(t, record.Message, "message3")
			assert.Equal(t, record.Reason, "reason3")
		} else {
			t.Fatalf("Unexpected allocation found")
		}
	}
}