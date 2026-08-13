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
	"runtime"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/mock"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// creating a Publisher with nil store should still provide a non-nil object
func TestCreateShimPublisher(t *testing.T) {
	publisher := createShimPublisher(nil)
	assert.Assert(t, publisher != nil, "publisher should not be nil")
}

// StartService() and stop() functions should not cause panic
func TestServiceStartStopInternal(t *testing.T) {
	countPublisherGoroutines := func() int {
		buf := make([]byte, 2*1024)
		n := runtime.Stack(buf, true)
		return strings.Count(string(buf[:n]), "(*eventPublisher).start.func1")
	}

	// wait for any background publisher goroutine to finish exiting
	err := common.WaitForCondition(time.Millisecond, time.Second, func() bool {
		return countPublisherGoroutines() == 0
	})
	assert.NilError(t, err, "expected initial publisher goroutines count to be 0")

	store := newEventStore(1000)
	publisher := createShimPublisher(store)
	defer publisher.stop()
	assert.Equal(t, publisher.getEventStore(), store)

	// start and stop simulate a restart
	publisher.start()
	publisher.stop()
	err = common.WaitForCondition(time.Millisecond, time.Second, func() bool {
		return countPublisherGoroutines() == 0
	})
	assert.NilError(t, err, "expected no new go routine after start and stop")

	// start should not fail or panic
	publisher.start()
	err = common.WaitForCondition(time.Millisecond, time.Second, func() bool {
		return countPublisherGoroutines() == 1
	})
	assert.NilError(t, err, "expected 1 new go routine")

	publisher.start()
	assert.Equal(t, 1, countPublisherGoroutines(), "Already started should not create new go routine")
}

func TestNoFillWithoutEventPluginRegistered(t *testing.T) {
	store := newEventStore(1000)
	publisher := createShimPublisher(store)
	publisher.pushEventInterval = time.Millisecond
	publisher.start()
	defer publisher.stop()

	event := &si.EventRecord{
		Type:          si.EventRecord_REQUEST,
		ObjectID:      "ask",
		ReferenceID:   "app",
		Message:       "message",
		TimestampNano: 123456,
	}
	store.Store(event)

	err := common.WaitForCondition(time.Millisecond,
		time.Second,
		func() bool {
			return store.CountStoredEvents() == 0
		},
	)
	assert.NilError(t, err, "the Publisher should erase the store even if no EventPlugin registered")
}

// we push an event to the publisher, and check that the same event
// is published by observing the mocked EventPlugin
func TestPublisherSendsEvent(t *testing.T) {
	eventPlugin := mock.NewEventPlugin()
	plugins.RegisterSchedulerPlugin(eventPlugin)
	if plugins.GetResourceManagerCallbackPlugin() == nil {
		t.Fatal("could not register event plugin for test")
	}

	store := newEventStore(1000)
	publisher := createShimPublisher(store)
	publisher.pushEventInterval = time.Millisecond
	publisher.start()
	defer publisher.stop()

	event := &si.EventRecord{
		Type:          si.EventRecord_REQUEST,
		ObjectID:      "ask",
		ReferenceID:   "app",
		Message:       "message",
		TimestampNano: 123456,
	}
	store.Store(event)

	var eventFromPlugin *si.EventRecord
	err := common.WaitForCondition(time.Millisecond,
		time.Second,
		func() bool {
			eventFromPlugin = eventPlugin.GetNextEventRecord()
			return eventFromPlugin != nil
		},
	)
	assert.NilError(t, err, "event was not received in time: %v", err)
	assert.Equal(t, eventFromPlugin.ObjectID, "ask")
	assert.Equal(t, eventFromPlugin.ReferenceID, "app")
	assert.Equal(t, eventFromPlugin.Message, "message")
	assert.Equal(t, eventFromPlugin.TimestampNano, int64(123456))
}
