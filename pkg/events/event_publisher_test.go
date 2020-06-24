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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
	"gotest.tools/assert"
)

type mockEventPlugin struct {
	records chan *si.EventRecord
	fail    bool

	sync.Mutex
}

// create and register mocked event plugin
func createEventPluginForTest(fail bool) (*mockEventPlugin, error) {
	eventPlugin := mockEventPlugin{
		records: make(chan *si.EventRecord, 3),
		fail:    fail,
	}
	plugins.RegisterSchedulerPlugin(&eventPlugin)
	if plugins.GetEventPlugin() == nil {
		return nil, fmt.Errorf("event plugin is not registered")
	}
	return &eventPlugin, nil
}

func (ep *mockEventPlugin) SendEvent(events []*si.EventRecord) error {
	ep.Lock()
	defer ep.Unlock()

	if ep.fail {
		// we only fail once, next time we succeed
		ep.fail = false
		return fmt.Errorf("could not send event")
	}
	for _, event := range events {
		ep.records <- event
	}
	return nil
}

func (ep *mockEventPlugin) getNextEventRecord() *si.EventRecord {
	ep.Lock()
	defer ep.Unlock()

	select {
	case record := <-ep.records:
		return record
	default:
		return nil
	}
}

func TestCreateShimPublisher(t *testing.T) {
	publisher := CreateShimPublisher(nil)
	assert.Assert(t, publisher != nil, "publisher should not be nil")
}

func TestServiceStartStopInternal(t *testing.T) {
	store := newEventStoreImpl()
	publisher := createShimPublisherInternal(store)
	publisher.StartService()
	assert.Equal(t, publisher.getEventStore(), store)
	publisher.Stop()
}

func TestPublisher(t *testing.T) {
	pushEventInterval := 2 * time.Millisecond

	eventPlugin, err := createEventPluginForTest(false)
	assert.NilError(t, err, "could not create event plugin for test")

	store := newEventStoreImpl()
	publisher := createShimPublisherWithParameters(store, pushEventInterval)
	publisher.StartService()

	event := &si.EventRecord{
		Type:          si.EventRecord_REQUEST,
		ObjectID:      "ask",
		GroupID:       "app",
		Reason:        "reason",
		Message:       "message",
		TimestampNano: 123456,
	}
	store.Store(event)
	time.Sleep(2 * pushEventInterval)

	eventFromPlugin := eventPlugin.getNextEventRecord()
	if eventFromPlugin == nil {
		t.Fatal("EventRecord should not be nil!")
	}
	assert.Equal(t, eventFromPlugin.ObjectID, "ask")
	assert.Equal(t, eventFromPlugin.GroupID, "app")
	assert.Equal(t, eventFromPlugin.Reason, "reason")
	assert.Equal(t, eventFromPlugin.Message, "message")
	assert.Equal(t, eventFromPlugin.TimestampNano, int64(123456))

	publisher.Stop()
}

func TestPublisherHandleError(t *testing.T) {
	pushEventInterval := 2 * time.Millisecond

	eventPlugin, err := createEventPluginForTest(true)
	assert.NilError(t, err, "could not create event plugin for test")

	store := newEventStoreImpl()
	publisher := createShimPublisherWithParameters(store, pushEventInterval)
	publisher.StartService()

	event1 := &si.EventRecord{
		Type:          si.EventRecord_REQUEST,
		ObjectID:      "ask1",
		GroupID:       "app1",
		Reason:        "reason1",
		Message:       "message1",
		TimestampNano: 123456,
	}
	store.Store(event1)
	time.Sleep(2 * pushEventInterval)

	// was not able to send event, plugin should be empty
	eventFromPlugin1 := eventPlugin.getNextEventRecord()
	if eventFromPlugin1 != nil {
		t.Fatal("event should be nil due to failure")
	}

	event2 := &si.EventRecord{
		Type:          si.EventRecord_REQUEST,
		ObjectID:      "ask2",
		GroupID:       "app2",
		Reason:        "reason2",
		Message:       "message2",
		TimestampNano: 123457,
	}
	store.Store(event2)
	time.Sleep(2 * pushEventInterval)
	eventFromPlugin2 := eventPlugin.getNextEventRecord()
	if eventFromPlugin2 == nil {
		t.Fatal("EventRecord should not be nil!")
	}
	assert.Equal(t, eventFromPlugin2.ObjectID, "ask2")
	assert.Equal(t, eventFromPlugin2.GroupID, "app2")
	assert.Equal(t, eventFromPlugin2.Reason, "reason2")
	assert.Equal(t, eventFromPlugin2.Message, "message2")
	assert.Equal(t, eventFromPlugin2.TimestampNano, int64(123457))

	publisher.Stop()
}
