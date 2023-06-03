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

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
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
		Type:     si.EventRecord_REQUEST,
		ObjectID: "alloc1",
		GroupID:  "app1",
		Reason:   "reason",
		Message:  "message",
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
	assert.Equal(t, record.GroupID, "app1")
	assert.Equal(t, record.Message, "message")
	assert.Equal(t, record.Reason, "reason")
}
