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

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-core/pkg/scheduler/tests"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type mockEventPlugin struct {
	tests.MockResourceManagerCallback
	records chan *si.EventRecord

	sync.Mutex
}

// create and register mocked event plugin
func createEventPluginForTest() (*mockEventPlugin, error) {
	eventPlugin := mockEventPlugin{
		records: make(chan *si.EventRecord, 3),
	}
	plugins.RegisterSchedulerPlugin(&eventPlugin)
	if plugins.GetResourceManagerCallbackPlugin() == nil {
		return nil, fmt.Errorf("event plugin is not registered")
	}
	return &eventPlugin, nil
}

func (ep *mockEventPlugin) SendEvent(events []*si.EventRecord) {
	ep.Lock()
	defer ep.Unlock()

	for _, event := range events {
		ep.records <- event
	}
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

// creating a Publisher with nil store should still provide a non-nil object
func TestCreateShimPublisher(t *testing.T) {
	publisher := CreateShimPublisher(nil)
	assert.Assert(t, publisher != nil, "publisher should not be nil")
}

// StartService() and Stop() functions should not cause panic
func TestServiceStartStopInternal(t *testing.T) {
	store := newEventStore()
	publisher := CreateShimPublisher(store)
	publisher.StartService()
	defer publisher.Stop()
	assert.Equal(t, publisher.getEventStore(), store)
}

func TestNoFillWithoutEventPluginRegistered(t *testing.T) {
	pushEventInterval := 2 * time.Millisecond

	store := newEventStore()
	publisher := CreateShimPublisher(store)
	publisher.pushEventInterval = pushEventInterval
	publisher.StartService()
	defer publisher.Stop()

	event := &si.EventRecord{
		Type:          si.EventRecord_REQUEST,
		ObjectID:      "ask",
		ReferenceID:   "app",
		Message:       "message",
		TimestampNano: 123456,
	}
	store.Store(event)
	time.Sleep(2 * pushEventInterval)
	assert.Equal(t, store.CountStoredEvents(), 0,
		"the Publisher should erase the store even if no EventPlugin registered")
}

// we push an event to the publisher, and check that the same event
// is published by observing the mocked EventPlugin
func TestPublisherSendsEvent(t *testing.T) {
	eventPlugin, err := createEventPluginForTest()
	assert.NilError(t, err, "could not create event plugin for test")

	store := newEventStore()
	publisher := CreateShimPublisher(store)
	publisher.pushEventInterval = time.Millisecond
	publisher.StartService()
	defer publisher.Stop()

	event := &si.EventRecord{
		Type:          si.EventRecord_REQUEST,
		ObjectID:      "ask",
		ReferenceID:   "app",
		Message:       "message",
		TimestampNano: 123456,
	}
	store.Store(event)

	var eventFromPlugin *si.EventRecord
	err = common.WaitForCondition(func() bool {
		eventFromPlugin = eventPlugin.getNextEventRecord()
		return eventFromPlugin != nil
	}, time.Millisecond, time.Second)
	assert.NilError(t, err, "event was not received in time: %v", err)
	assert.Equal(t, eventFromPlugin.ObjectID, "ask")
	assert.Equal(t, eventFromPlugin.ReferenceID, "app")
	assert.Equal(t, eventFromPlugin.Message, "message")
	assert.Equal(t, eventFromPlugin.TimestampNano, int64(123456))
}
