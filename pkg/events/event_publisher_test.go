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
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
	"gotest.tools/assert"
)

type eventStoreForTest struct {
	events chan Event
}

func eventToRecord(event Event) *si.EventRecord {
	source, ok := event.GetSource().(string)
	if !ok {
		panic("Expected string in tests")
	}
	return &si.EventRecord{
		ObjectID: source,
		GroupID: event.GetGroup(),
		Reason: event.GetReason(),
		Message: event.GetMessage(),
	}
}

func (es *eventStoreForTest) Store(e Event) {
	es.events <- e
}

func (es *eventStoreForTest) CollectEvents() []*si.EventRecord {
	records := make([]*si.EventRecord, 0)
	for {
		select {
			case event := <-es.events:
				records = append(records, eventToRecord(event))
			default:
				return records
		}
	}
}

type eventPluginForTest struct {
	records chan *si.EventRecord
}

// create and register mocked event plugin
func createEventPluginForTest(t *testing.T) eventPluginForTest {
	eventPlugin := eventPluginForTest{
		records: make(chan *si.EventRecord, 3),
	}
	plugins.RegisterSchedulerPlugin(&eventPlugin)
	if plugins.GetEventPlugin() == nil {
		t.Fatalf("Event plugin should have been registered!")
	}
	return eventPlugin
}

func (ep *eventPluginForTest) SendEvent(events []*si.EventRecord) error {
	for _, event := range events {
		ep.records <- event
	}
	return nil
}

func (ep *eventPluginForTest) getNextEventRecord() *si.EventRecord {
	select {
		case record := <- ep.records:
			return record
		default:
			return nil
	}
}

func TestServiceStartStopWithoutEventPlugin(t *testing.T) {
	store := &eventStoreForTest{
		events: make(chan Event, 2),
	}
	publisher := NewShimPublisher(store)
	publisher.StartService()
	assert.Equal(t, publisher.GetEventStore(), store)
	time.Sleep(100 * time.Millisecond)
	publisher.Stop()
}

func TestServiceStartStopWithEventPlugin(t *testing.T) {
	createEventPluginForTest(t)

	store := &eventStoreForTest{
		events: make(chan Event, 2),
	}
	publisher := NewShimPublisher(store)
	publisher.StartService()
	assert.Equal(t, publisher.GetEventStore(), store)
	time.Sleep(100 * time.Millisecond)
	publisher.Stop()
}

func TestPublisher(t *testing.T) {
	eventPlugin := createEventPluginForTest(t)
	store := &eventStoreForTest{
		events: make(chan Event, 2),
	}
	publisher := NewShimPublisher(store)
	publisher.StartService()

	event := baseEvent{
		source:  "source",
		group:   "group",
		reason:  "reason",
		message: "message",
	}
	store.Store(&event)
	time.Sleep(2 * pushEventInterval)

	eventFromPlugin := eventPlugin.getNextEventRecord()
	if eventFromPlugin == nil {
		t.Fatal("EventRecord should not be nil!")
	}
	assert.Equal(t, eventFromPlugin.ObjectID, "source")
	assert.Equal(t, eventFromPlugin.GroupID, "group")
	assert.Equal(t, eventFromPlugin.Reason, "reason")
	assert.Equal(t, eventFromPlugin.Message, "message")
}
