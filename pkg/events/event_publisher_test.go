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

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type mockEventPlugin struct {
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

func (f *mockEventPlugin) UpdateAllocation(response *si.AllocationResponse) error {
	// do nothing
	return nil
}

func (f *mockEventPlugin) UpdateApplication(response *si.ApplicationResponse) error {
	// do nothing
	return nil
}

func (f *mockEventPlugin) UpdateNode(response *si.NodeResponse) error {
	// do nothing
	return nil
}

func (f *mockEventPlugin) Predicates(args *si.PredicatesArgs) error {
	// do nothing
	return nil
}

func (f *mockEventPlugin) ReSyncSchedulerCache(args *si.ReSyncSchedulerCacheArgs) error {
	// do nothing
	return nil
}

func (f *mockEventPlugin) UpdateContainerSchedulingState(request *si.UpdateContainerSchedulingStateRequest) {
	// do nothing
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
	store := newEventStoreImpl()
	publisher := createShimPublisherInternal(store)
	publisher.StartService()
	assert.Equal(t, publisher.getEventStore(), store)
	publisher.Stop()
}

func TestNoFillWithoutEventPluginRegistered(t *testing.T) {
	pushEventInterval := 2 * time.Millisecond

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
	assert.Equal(t, store.CountStoredEvents(), 0,
		"the Publisher should erase the store even if no EventPlugin registered")
}

// we push an event to the publisher, and check that the same event
// is published by observing the mocked EventPlugin
func TestPublisherSendsEvent(t *testing.T) {
	pushEventInterval := 2 * time.Millisecond

	eventPlugin, err := createEventPluginForTest()
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
