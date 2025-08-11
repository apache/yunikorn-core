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

// StartService() and Stop() must not cause panic
func TestSimpleStartAndStop(t *testing.T) {
	Init()
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
	Init()
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
	err := common.WaitForCondition(time.Millisecond, time.Second, func() bool {
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
	Init()
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
	err := common.WaitForCondition(time.Millisecond, time.Second, func() bool {
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
	configs.SetConfigMap(map[string]string{})
	defer configs.SetConfigMap(map[string]string{})

	Init()
	eventSystem := GetEventSystem().(*EventSystemImpl) //nolint:errcheck
	eventSystem.StartService()
	defer eventSystem.Stop()

	assert.Assert(t, eventSystem.IsEventTrackingEnabled())
	assert.Equal(t, eventSystem.GetRingBufferCapacity(), uint64(configs.DefaultEventRingBufferCapacity))
	assert.Equal(t, eventSystem.GetRequestCapacity(), uint64(configs.DefaultEventRequestCapacity))
	assert.Equal(t, eventSystem.eventBuffer.capacity, uint64(configs.DefaultEventRingBufferCapacity))

	// update config and wait for refresh
	var newRingBufferCapacity uint64 = 123
	newRequestCapacity := uint64(555)

	configs.SetConfigMap(
		map[string]string{configs.CMEventTrackingEnabled: "false",
			configs.CMEventRingBufferCapacity: strconv.FormatUint(newRingBufferCapacity, 10),
			configs.CMEventRequestCapacity:    strconv.FormatUint(newRequestCapacity, 10),
		})
	err := common.WaitForCondition(10*time.Millisecond,
		5*time.Second,
		func() bool {
			return !eventSystem.IsEventTrackingEnabled()
		},
	)
	assert.NilError(t, err, "timed out waiting for config refresh")

	assert.Equal(t, eventSystem.GetRingBufferCapacity(), newRingBufferCapacity)
	assert.Equal(t, eventSystem.GetRequestCapacity(), newRequestCapacity)
	assert.Equal(t, eventSystem.eventBuffer.capacity, newRingBufferCapacity)
}

func TestEventStreaming(t *testing.T) {
	Init()
	eventSystem := GetEventSystem()
	eventSystem.StartService()
	defer eventSystem.Stop()

	eventSystem.CreateEventStream("test", 10)
	streams := eventSystem.GetEventStreams()

	assert.Equal(t, 1, len(streams))
	assert.Equal(t, "test", streams[0].Name)
}

func TestRequestCapacity(t *testing.T) {
	config := map[string]string{
		configs.CMEventRequestCapacity: "123",
	}
	configs.SetConfigMap(config)

	capacity := getRequestCapacity()
	assert.Equal(t, uint64(123), capacity)

	config = map[string]string{
		configs.CMEventRequestCapacity: "0",
	}
	configs.SetConfigMap(config)
	capacity = getRequestCapacity()
	assert.Equal(t, uint64(configs.DefaultEventRequestCapacity), capacity)

	config = map[string]string{
		configs.CMEventRequestCapacity: "xyz",
	}
	configs.SetConfigMap(config)
	capacity = getRequestCapacity()
	assert.Equal(t, uint64(configs.DefaultEventRequestCapacity), capacity)
}

func TestRingBufferCapacity(t *testing.T) {
	config := map[string]string{
		configs.CMEventRingBufferCapacity: "123",
	}
	configs.SetConfigMap(config)

	capacity := getRingBufferCapacity()
	assert.Equal(t, uint64(123), capacity)

	config = map[string]string{
		configs.CMEventRingBufferCapacity: "0",
	}
	configs.SetConfigMap(config)
	capacity = getRingBufferCapacity()
	assert.Equal(t, uint64(configs.DefaultEventRingBufferCapacity), capacity)

	config = map[string]string{
		configs.CMEventRingBufferCapacity: "xyz",
	}
	configs.SetConfigMap(config)
	capacity = getRingBufferCapacity()
	assert.Equal(t, uint64(configs.DefaultEventRingBufferCapacity), capacity)
}

func TestTruncateEventMessage(t *testing.T) {
	testCases := []struct {
		name           string
		message        string
		expectedLength int
	}{
		{
			name:           "message length less than 1024 characters",
			message:        "Lorem ipsum",
			expectedLength: 11,
		},
		{
			name:           "message length exactly 1024 characters",
			message:        "Lorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos. Lorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos. Lorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus d",
			expectedLength: 1024,
		},
		{
			name:           "message length greater than 1024 characters",
			message:        "Lorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos. Lorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos. Lorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.",
			expectedLength: 1024,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			truncated := truncateEventMessage(testCase.message)
			assert.Equal(t, testCase.expectedLength, len(truncated))
		})
	}

}
