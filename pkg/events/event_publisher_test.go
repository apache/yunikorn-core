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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type eventStoreForTest struct {
	events chan Event
}

func eventToRecord(event Event) (*si.EventRecord, error) {
	source, ok := event.GetSource().(string)
	if !ok {
		return nil, fmt.Errorf("expected string in tests")
	}
	return &si.EventRecord{
		ObjectID: source,
		GroupID: event.GetGroup(),
		Reason: event.GetReason(),
		Message: event.GetMessage(),
	}, nil
}

func (es *eventStoreForTest) Store(e Event) {
	es.events <- e
}

func (es *eventStoreForTest) CollectEvents() ([]*si.EventRecord, error) {
	records := make([]*si.EventRecord, 0)
	errorList := make([]string, 0)
	for {
		select {
			case event := <-es.events:
				record, err := eventToRecord(event)
				if err != nil {
					log.Logger().Warn("error during converting eventChannel to records", zap.Error(err))
					errorList = append(errorList, err.Error())
					continue
				}
				records = append(records, record)
			default:
				var errorsToReturn error
				if len(errorList) > 0 {
					errorsToReturn = errors.New(strings.Join(errorList, ","))
				}
				return records, errorsToReturn
		}
	}
}

type eventPluginForTest struct {
	records chan *si.EventRecord
}

// create and register mocked event plugin
func createEventPluginForTest() (*eventPluginForTest, error) {
	eventPlugin := eventPluginForTest{
		records: make(chan *si.EventRecord, 3),
	}
	plugins.RegisterSchedulerPlugin(&eventPlugin)
	if plugins.GetEventPlugin() == nil {
		return nil, fmt.Errorf("event plugin is not registered")
	}
	return &eventPlugin, nil
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

func createPublisherForTest(store EventStore) *shimPublisher {
	return &shimPublisher{
		store: store,
		stop:  false,
	}
}

func TestServiceStartStopWithoutEventPlugin(t *testing.T) {
	store := &eventStoreForTest{
		events: make(chan Event, 2),
	}
	publisher := createPublisherForTest(store)
	publisher.StartService()
	assert.Equal(t, publisher.getEventStore(), store)
	time.Sleep(100 * time.Millisecond)
	publisher.Stop()
}

func TestServiceStartStopWithEventPlugin(t *testing.T) {
	store := &eventStoreForTest{
		events: make(chan Event, 2),
	}
	publisher := createPublisherForTest(store)
	publisher.StartService()
	assert.Equal(t, publisher.getEventStore(), store)
	time.Sleep(100 * time.Millisecond)
	publisher.Stop()
}

func TestPublisher(t *testing.T) {
	eventPlugin, err := createEventPluginForTest()
	assert.NilError(t, err, "unable to create event plugin for test")
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
