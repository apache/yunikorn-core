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

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// the fields of an event should match after stored and retrieved
func TestStoreAndRetrieve(t *testing.T) {
	store := newEventStore()
	event := &si.EventRecord{
		Type:              si.EventRecord_REQUEST,
		EventChangeDetail: si.EventRecord_DETAILS_NONE,
		EventChangeType:   si.EventRecord_NONE,
		ObjectID:          "alloc1",
		ReferenceID:       "app1",
		Message:           "message",
	}
	store.Store(event)

	records := store.CollectEvents()
	assert.Equal(t, len(records), 1)
	record := records[0]
	assert.DeepEqual(t, record, event, cmpopts.IgnoreUnexported(si.EventRecord{}))

	// calling CollectEvents erases the eventChannel map
	records = store.CollectEvents()
	assert.Equal(t, len(records), 0)
}

// if we push more events to the EventStore than its
// allowed maximum, those that couldn't fit will be omitted
func TestStoreWithLimitedSize(t *testing.T) {
	defaultEventStoreSize = 3

	store := newEventStore()
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
