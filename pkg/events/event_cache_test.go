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
	"testing"
	"time"

	"go.uber.org/zap"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func TestGetEventCache(t *testing.T) {
	cache := GetEventCache()
	assert.Assert(t, cache != nil, "the cache should not be nil")
}

func TestStartStop(t *testing.T) {
	cache := createEventStoreAndCache()
	assert.Equal(t, cache.IsStarted(), false, "EventCache should not be started when constructed")
	// adding event to stopped cache does not cause panic
	cache.AddEvent(nil)
	cache.StartService()
	// add an event
	cache.AddEvent(nil)
	assert.Equal(t, cache.IsStarted(), true, "EventCache should have been started")
	cache.Stop()
	// adding event to stopped cache does not cause panic
	cache.AddEvent(nil)
	assert.Equal(t, cache.IsStarted(), false, "EventCache should have been stopped")
}

func TestSingleEvent(t *testing.T) {
	cache := createEventStoreAndCache()
	store := cache.GetEventStore()

	cache.StartService()

	event := si.EventRecord{
		Type:     si.EventRecord_REQUEST,
		ObjectID: "alloc1",
		GroupID:  "app1",
		Reason:   "reason",
		Message:  "message",
	}
	cache.AddEvent(&event)

	// wait for events to be processed
	err := common.WaitFor(1*time.Millisecond, 10*time.Millisecond, func() bool {
		return store.CountStoredEvents() == 1
	})
	assert.NilError(t, err, "the event should have been processed")

	records := store.CollectEvents()
	if records == nil {
		t.Fatal("collecting eventChannel should return something")
	}
	assert.Equal(t, len(records), 1)
	record := records[0]
	assert.Equal(t, record.Type, si.EventRecord_REQUEST)
	assert.Equal(t, record.ObjectID, "alloc1")
	assert.Equal(t, record.GroupID, "app1")
	assert.Equal(t, record.Message, "message")
	assert.Equal(t, record.Reason, "reason")
}

func TestMultipleEvents(t *testing.T) {
	cache := createEventStoreAndCache()
	store := cache.GetEventStore()

	cache.StartService()

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
	cache.AddEvent(&event1)
	cache.AddEvent(&event2)
	cache.AddEvent(&event3)

	// wait for events to be processed
	err := common.WaitFor(1*time.Millisecond, 100*time.Millisecond, func() bool {
		return store.CountStoredEvents() >= 2
	})
	assert.NilError(t, err, "the event should have been processed")

	records := store.CollectEvents()
	if records == nil {
		t.Fatal("collecting eventChannel should return something")
	}
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

type slowEventStore struct {
	busy bool

	sync.Mutex
}

func (ses *slowEventStore) setBusy(b bool) {
	ses.Lock()
	defer ses.Unlock()

	log.Logger().Info("setting busy to ", zap.Bool("bool", b))

	ses.busy = b
}

func (ses *slowEventStore) getBusy() bool {
	ses.Lock()
	defer ses.Unlock()

	log.Logger().Info("getting busy ", zap.Bool("bool", ses.busy))

	return ses.busy
}

func (ses *slowEventStore) Store(*si.EventRecord) {
	ses.setBusy(true)
	defer ses.setBusy(false)

	time.Sleep(50 * time.Millisecond)
}

func (ses *slowEventStore) CollectEvents() []*si.EventRecord {
	return nil
}

func (ses *slowEventStore) CountStoredEvents() int {
	return 0
}

func TestLimitedChannel(t *testing.T) {
	defaultEventChannelSize = 2

	store := slowEventStore{}
	cache := createEventCacheInternal(&store)
	assert.Equal(t, cache.IsStarted(), false, "EventCache should not be started when constructed")
	cache.StartService()
	assert.Equal(t, cache.IsStarted(), true, "EventCache should have been started")

	event := si.EventRecord{
		Type:     si.EventRecord_REQUEST,
		ObjectID: "alloc1",
		GroupID:  "app1",
		Reason:   "reason",
		Message:  "message",
	}

	cache.AddEvent(&event)

	// wait for events to be occupy the slow store
	err := common.WaitFor(1*time.Millisecond, 100*time.Millisecond, store.getBusy)
	assert.NilError(t, err, "the event should have been processed")
	assert.Equal(t, len(cache.channel), 0, "the number of queued elements should be zero")

	cache.AddEvent(&event)
	assert.Equal(t, len(cache.channel), 1, "the number of queued elements should be two")
	cache.AddEvent(&event)
	assert.Equal(t, len(cache.channel), 2, "the number of queued elements should be two")
	cache.AddEvent(&event)
	assert.Equal(t, len(cache.channel), 2, "the number of queued elements should be two")
}
