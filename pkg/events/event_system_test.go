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
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// the EventSystem should be nil by default, until not set by CreateAndSetEventSystem()
func TestGetEventSystem(t *testing.T) {
	eventSystem := GetEventSystem()
	assert.Assert(t, eventSystem == nil, "the eventSystem should be nil by default")
	CreateAndSetEventSystem()
	eventSystem = GetEventSystem()
	assert.Assert(t, eventSystem != nil, "the eventSystem should not be nil")
}

// StartService() and Stop() must not cause panic
func TestSimpleStartAndStop(t *testing.T) {
	CreateAndSetEventSystem()
	eventSystem := GetEventSystem()
	// adding event to stopped eventSystem does not cause panic
	eventSystem.AddEvent(nil)
	eventSystem.StartService()
	defer eventSystem.Stop()
	// add an event
	eventSystem.AddEvent(nil)
	eventSystem.Stop()
	// adding event to stopped eventSystem does not cause panic
	eventSystem.AddEvent(nil)
}

// if an EventRecord is added to the EventSystem, the same record
// should be retrieved from the EventStore
func TestSingleEventStoredCorrectly(t *testing.T) {
	CreateAndSetEventSystem()
	eventSystem := GetEventSystem().(*EventSystemImpl) //nolint:errcheck
	// don't run publisher, because it can collect the event while we're waiting
	eventSystem.StartServiceWithPublisher(false)
	defer eventSystem.Stop()

	event := si.EventRecord{
		Type:        si.EventRecord_REQUEST,
		ObjectID:    "alloc1",
		ReferenceID: "app1",
		Message:     "message",
	}
	eventSystem.AddEvent(&event)

	// wait for events to be processed
	err := common.WaitFor(time.Millisecond, time.Second, func() bool {
		return eventSystem.Store.CountStoredEvents() == 1
	})
	assert.NilError(t, err, "the event should have been processed")

	records := eventSystem.Store.CollectEvents()
	if records == nil {
		t.Fatal("collecting eventChannel should return something")
	}
	assert.Equal(t, len(records), 1)
	record := records[0]
	assert.Equal(t, record.Type, si.EventRecord_REQUEST)
	assert.Equal(t, record.ObjectID, "alloc1")
	assert.Equal(t, record.ReferenceID, "app1")
	assert.Equal(t, record.Message, "message")
}

func TestGetEvents(t *testing.T) {
	CreateAndSetEventSystem()
	eventSystem := GetEventSystem().(*EventSystemImpl) //nolint:errcheck
	eventSystem.StartServiceWithPublisher(false)
	defer eventSystem.Stop()

	for i := 0; i < 10; i++ {
		event := &si.EventRecord{
			Type:        si.EventRecord_REQUEST,
			ObjectID:    "alloc1",
			ReferenceID: "app1",
			Message:     strconv.Itoa(i),
		}
		eventSystem.AddEvent(event)
	}
	err := common.WaitFor(time.Millisecond, time.Second, func() bool {
		return eventSystem.Store.CountStoredEvents() == 10
	})
	assert.NilError(t, err, "the events should have been processed")

	records, lowest, highest := eventSystem.GetEventsFromID(3, 3)
	assert.Equal(t, uint64(0), lowest)
	assert.Equal(t, uint64(9), highest)
	assert.Equal(t, 3, len(records))
	assert.Equal(t, "3", records[0].Message)
	assert.Equal(t, "4", records[1].Message)
	assert.Equal(t, "5", records[2].Message)
}

func TestConfigUpdate(t *testing.T) {
	configs.SetConfigMap(map[string]string{configs.CMEventTrackingEnabled: "true"})
	configs.SetConfigMap(map[string]string{})
	defer configs.SetConfigMap(map[string]string{})

	CreateAndSetEventSystem()
	eventSystem := GetEventSystem().(*EventSystemImpl) //nolint:errcheck
	eventSystem.StartService()
	defer eventSystem.Stop()

	assert.Assert(t, eventSystem.IsEventTrackingEnabled(), "Event tracking should be enabled by default")
	assert.Assert(t, eventSystem.GetRingBufferCapacity() == configs.DefaultEventRingBufferCapacity, "Invalid event buffer capacity")
	assert.Assert(t, eventSystem.GetRequestCapacity() == configs.DefaultEventRequestCapacity, "Invalid request capacity")
	assert.Assert(t, eventSystem.eventBuffer.capacity == configs.DefaultEventRingBufferCapacity, "Event buffer capacity mismatch")

	// update config and wait for refresh
	var newRingBufferCapacity uint64 = 123
	newRequestCapacity := 555

	configs.SetConfigMap(
		map[string]string{configs.CMEventTrackingEnabled: "false",
			configs.CMEventRingBufferCapacity: strconv.FormatUint(newRingBufferCapacity, 10),
			configs.CMEventRequestCapacity:    strconv.Itoa(int(newRequestCapacity)),
		})
	err := common.WaitForCondition(func() bool {
		return eventSystem.IsEventTrackingEnabled() == false
	}, 10*time.Millisecond, 5*time.Second)
	assert.NilError(t, err, "timed out waiting for config refresh")

	assert.Assert(t, eventSystem.GetRingBufferCapacity() == newRingBufferCapacity, "Invalid event buffer capacity")
	assert.Assert(t, eventSystem.GetRequestCapacity() == newRequestCapacity, "Invalid request capacity")
	assert.Assert(t, eventSystem.eventBuffer.capacity == newRingBufferCapacity, "Event buffer not resized")
}
