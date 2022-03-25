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
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"gotest.tools/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// the EventCache should be nil by default, until not set by CreateAndSetEventCache()
func TestGetEventCache(t *testing.T) {
	cache := GetEventCache()
	assert.Assert(t, cache == nil, "the cache should be nil by default")
	CreateAndSetEventCache()
	cache = GetEventCache()
	assert.Assert(t, cache != nil, "the cache should not be nil")
}

// StartService() and Stop() must not cause panic
func TestSimpleStartAndStop(t *testing.T) {
	CreateAndSetEventCache()
	cache := GetEventCache()
	// adding event to stopped cache does not cause panic
	cache.AddEvent(nil)
	cache.StartService()
	// add an event
	cache.AddEvent(nil)
	cache.Stop()
	// adding event to stopped cache does not cause panic
	cache.AddEvent(nil)
}

// if an EventRecord is added to the EventCache, the same record
// should be retrieved from the EventStore
func TestSingleEventStoredCorrectly(t *testing.T) {
	CreateAndSetEventCache()
	cache := GetEventCache()
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
		return cache.Store.CountStoredEvents() == 1
	})
	assert.NilError(t, err, "the event should have been processed")

	records := cache.Store.CollectEvents()
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

// this test checks that if we have multiple events
// stored in the EventStore, for a given ObjectID we
// only store one event, and it is always the
// least recently added one
func TestEventWithMatchingObjectID(t *testing.T) {
	CreateAndSetEventCache()
	cache := GetEventCache()
	cache.StartService()

	for i := 0; i < 3; i++ {
		event := &si.EventRecord{
			Type:     si.EventRecord_REQUEST,
			ObjectID: "alloc" + strconv.Itoa(i/2),
			GroupID:  "app" + strconv.Itoa(i/2),
			Reason:   "reason" + strconv.Itoa(i),
			Message:  "message" + strconv.Itoa(i),
		}
		cache.AddEvent(event)
	}

	// wait for events to be processed
	err := common.WaitFor(1*time.Millisecond, 100*time.Millisecond, func() bool {
		return cache.Store.CountStoredEvents() >= 2
	})
	assert.NilError(t, err, "the event should have been processed")

	records := cache.Store.CollectEvents()
	if records == nil {
		t.Fatal("collecting eventChannel should return something")
	}
	assert.Equal(t, len(records), 2)
	for _, record := range records {
		assert.Equal(t, record.Type, si.EventRecord_REQUEST)
		switch {
		case record.ObjectID == "alloc0":
			assert.Equal(t, record.GroupID, "app0")
			assert.Equal(t, record.Message, "message1")
			assert.Equal(t, record.Reason, "reason1")
		case record.ObjectID == "alloc1":
			assert.Equal(t, record.GroupID, "app1")
			assert.Equal(t, record.Message, "message2")
			assert.Equal(t, record.Reason, "reason2")
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

// this test checks that if storing events is much slower
// than the rate the events are generated, it doesn't cause
// panic by filling up the EventChannel
func TestSlowChannelFillingUpEventChannel(t *testing.T) {
	defaultEventChannelSize = 2

	store := slowEventStore{}
	cache := createEventCacheInternal(&store)

	cache.StartService()

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
