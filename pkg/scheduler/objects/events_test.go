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

package objects

import (
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type EventSystemMock struct {
	events []*si.EventRecord
}

func (m *EventSystemMock) AddEvent(event *si.EventRecord) {
	m.events = append(m.events, event)
}

func (m *EventSystemMock) StartService() {}

func (m *EventSystemMock) Stop() {}

func TestSendAppDoesNotFitEvent(t *testing.T) {
	app := &Application{
		queuePath: "root.test",
	}

	// not enabled
	evt := newApplicationEvents(app, nil)
	assert.Assert(t, evt.eventSystem == nil, "event system should be nil")
	assert.Assert(t, !evt.enabled, "event system should be disabled")
	evt.sendAppDoesNotFitEvent(&AllocationAsk{})

	// enabled
	mock := newEventSystemMock()
	evt = newApplicationEvents(app, mock)
	assert.Assert(t, evt.eventSystem != nil, "event system should not be nil")
	assert.Assert(t, evt.enabled, "event system should be enabled")
	evt.sendAppDoesNotFitEvent(&AllocationAsk{
		applicationID: appID0,
		allocationKey: aKey,
	})
	assert.Equal(t, 1, len(mock.events), "event was not generated")
}

func TestSendPlaceholderLargerEvent(t *testing.T) {
	app := &Application{
		queuePath: "root.test",
	}

	// not enabled
	evt := newApplicationEvents(app, nil)
	assert.Assert(t, evt.eventSystem == nil, "event system should be nil")
	assert.Assert(t, !evt.enabled, "event system should be disabled")
	evt.sendPlaceholderLargerEvent(&Allocation{}, &AllocationAsk{})

	// enabled
	mock := newEventSystemMock()
	evt = newApplicationEvents(app, mock)
	assert.Assert(t, evt.eventSystem != nil, "event system should not be nil")
	assert.Assert(t, evt.enabled, "event system should be enabled")
	evt.sendPlaceholderLargerEvent(&Allocation{
		allocationKey: aKey,
	}, &AllocationAsk{
		applicationID: appID0,
		allocationKey: aKey,
	})
	assert.Equal(t, 1, len(mock.events), "event was not generated")
}

func newEventSystemMock() *EventSystemMock {
	return &EventSystemMock{events: make([]*si.EventRecord, 0)}
}
