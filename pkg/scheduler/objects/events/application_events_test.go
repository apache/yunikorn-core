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

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	taskGroup = "tg-0"
	appID     = "app-0"
	allocKey  = "alloc-0"
)

func TestSendPlaceholderLargerEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := NewApplicationEvents(eventSystem)
	appEvents.SendPlaceholderLargerEvent(taskGroup, appID, allocKey, resources.NewResource(), resources.NewResource())
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = NewApplicationEvents(eventSystem)
	appEvents.SendPlaceholderLargerEvent(taskGroup, appID, allocKey, resources.NewResource(), resources.NewResource())
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
}

func TestSendNewAllocationEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := NewApplicationEvents(eventSystem)
	appEvents.SendNewAllocationEvent(appID, allocKey, resources.NewResource())
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = NewApplicationEvents(eventSystem)
	assert.Assert(t, appEvents.eventSystem != nil, "event system should not be nil")
	appEvents.SendNewAllocationEvent(appID, allocKey, resources.NewResource())
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	assert.Equal(t, si.EventRecord_APP, eventSystem.Events[0].Type, "event type is not expected")
	assert.Equal(t, si.EventRecord_ADD, eventSystem.Events[0].EventChangeType, "event change type is not expected")
	assert.Equal(t, si.EventRecord_APP_ALLOC, eventSystem.Events[0].EventChangeDetail, "event change detail is not expected")
	assert.Equal(t, appID, eventSystem.Events[0].ObjectID, "event object id is not expected")
	assert.Equal(t, allocKey, eventSystem.Events[0].ReferenceID, "event reference id is not expected")
	assert.Equal(t, common.Empty, eventSystem.Events[0].Message, "message is not expected")
}

func TestSendNewAskEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := NewApplicationEvents(eventSystem)
	appEvents.SendNewAskEvent(appID, allocKey, resources.NewResource())
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = NewApplicationEvents(eventSystem)
	assert.Assert(t, appEvents.eventSystem != nil, "event system should not be nil")
	appEvents.SendNewAskEvent(appID, allocKey, resources.NewResource())
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	assert.Equal(t, si.EventRecord_APP, eventSystem.Events[0].Type, "event type is not expected")
	assert.Equal(t, si.EventRecord_ADD, eventSystem.Events[0].EventChangeType, "event change type is not expected")
	assert.Equal(t, si.EventRecord_APP_REQUEST, eventSystem.Events[0].EventChangeDetail, "event change detail is not expected")
	assert.Equal(t, appID, eventSystem.Events[0].ObjectID, "event object id is not expected")
	assert.Equal(t, allocKey, eventSystem.Events[0].ReferenceID, "event reference id is not expected")
	assert.Equal(t, common.Empty, eventSystem.Events[0].Message, "message is not expected")
}

func TestSendRemoveAllocationEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := NewApplicationEvents(eventSystem)
	appEvents.SendRemoveAllocationEvent(appID, allocKey, resources.NewResource(), si.TerminationType_STOPPED_BY_RM)
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	testCases := []struct {
		name                 string
		eventSystemMock      *mock.EventSystem
		terminationType      si.TerminationType
		expectedEventCnt     int
		expectedType         si.EventRecord_Type
		expectedChangeType   si.EventRecord_ChangeType
		expectedChangeDetail si.EventRecord_ChangeDetail
	}{
		{
			name:                 "remove allocation cause of node removal",
			eventSystemMock:      mock.NewEventSystem(),
			terminationType:      si.TerminationType_UNKNOWN_TERMINATION_TYPE,
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_NODEREMOVED,
		},
		{
			name:                 "remove allocation cause of resource manager cancel",
			eventSystemMock:      mock.NewEventSystem(),
			terminationType:      si.TerminationType_STOPPED_BY_RM,
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_CANCEL,
		},
		{
			name:                 "remove allocation cause of timeout",
			eventSystemMock:      mock.NewEventSystem(),
			terminationType:      si.TerminationType_TIMEOUT,
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_TIMEOUT,
		},
		{
			name:                 "remove allocation cause of preemption",
			eventSystemMock:      mock.NewEventSystem(),
			terminationType:      si.TerminationType_PREEMPTED_BY_SCHEDULER,
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_PREEMPT,
		},
		{
			name:                 "remove allocation cause of replacement",
			eventSystemMock:      mock.NewEventSystem(),
			terminationType:      si.TerminationType_PLACEHOLDER_REPLACED,
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_REPLACED,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.eventSystemMock == nil {
				appEvt := NewApplicationEvents(nil)
				assert.Assert(t, appEvt.eventSystem == nil, "event system should be nil")
				appEvt.SendRemoveAllocationEvent(appID, allocKey, resources.NewResource(), testCase.terminationType)
			} else {
				appEvt := NewApplicationEvents(testCase.eventSystemMock)
				assert.Assert(t, appEvt.eventSystem != nil, "event system should not be nil")
				appEvt.SendRemoveAllocationEvent(appID, allocKey, resources.NewResource(), testCase.terminationType)
				assert.Equal(t, testCase.expectedEventCnt, len(testCase.eventSystemMock.Events), "event was not generated")
				assert.Equal(t, testCase.expectedType, testCase.eventSystemMock.Events[0].Type, "event type is not expected")
				assert.Equal(t, testCase.expectedChangeType, testCase.eventSystemMock.Events[0].EventChangeType, "event change type is not expected")
				assert.Equal(t, testCase.expectedChangeDetail, testCase.eventSystemMock.Events[0].EventChangeDetail, "event change detail is not expected")
				assert.Equal(t, appID, testCase.eventSystemMock.Events[0].ObjectID, "event object id is not expected")
				assert.Equal(t, allocKey, testCase.eventSystemMock.Events[0].ReferenceID, "event reference id is not expected")
				assert.Equal(t, common.Empty, testCase.eventSystemMock.Events[0].Message, "message is not expected")
			}
		})
	}
}

func TestSendRemoveAskEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := NewApplicationEvents(eventSystem)
	appEvents.SendRemoveAskEvent(appID, allocKey, resources.NewResource(), si.EventRecord_REQUEST_CANCEL)
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = NewApplicationEvents(eventSystem)
	appEvents.SendRemoveAskEvent(appID, allocKey, resources.NewResource(), si.EventRecord_REQUEST_CANCEL)
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_REQUEST_CANCEL, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "alloc-0", event.ReferenceID)
	assert.Equal(t, "", event.Message)

	eventSystem.Reset()
	appEvents.SendRemoveAskEvent(appID, allocKey, resources.NewResource(), si.EventRecord_REQUEST_TIMEOUT)
	event = eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_REQUEST_TIMEOUT, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "alloc-0", event.ReferenceID)
	assert.Equal(t, "", event.Message)
}

func TestSendNewApplicationEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := NewApplicationEvents(eventSystem)
	appEvents.SendNewApplicationEvent(appID)
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	mockEvents := mock.NewEventSystem()
	appEvents = NewApplicationEvents(mockEvents)
	appEvents.SendNewApplicationEvent(appID)
	event := mockEvents.Events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_APP_NEW, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "", event.Message)
}

func TestSendRemoveApplicationEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := NewApplicationEvents(eventSystem)
	appEvents.SendRemoveApplicationEvent(appID)
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = NewApplicationEvents(eventSystem)
	appEvents.SendRemoveApplicationEvent(appID)
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "", event.Message)
}

func TestSendStateChangeEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := NewApplicationEvents(eventSystem)
	appEvents.SendStateChangeEvent(appID, si.EventRecord_APP_RUNNING, "")
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = NewApplicationEvents(eventSystem)
	appEvents.SendStateChangeEvent(appID, si.EventRecord_APP_RUNNING, "The application is running")
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_APP_RUNNING, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "The application is running", event.Message)

	eventSystem = mock.NewEventSystemDisabled()
	appEvents = NewApplicationEvents(eventSystem)
	appEvents.SendStateChangeEvent(appID, si.EventRecord_APP_RUNNING, "ResourceReservationTimeout")
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = NewApplicationEvents(eventSystem)
	appEvents.SendStateChangeEvent(appID, si.EventRecord_APP_REJECT, "Failed to add application to partition (placement rejected)")
	event = eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_APP_REJECT, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "Failed to add application to partition (placement rejected)", event.Message)
}

func TestSendAppCannotRunInQueueEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := NewApplicationEvents(eventSystem)
	appEvents.SendAppNotRunnableInQueueEvent(appID)
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = NewApplicationEvents(eventSystem)
	appEvents.SendAppNotRunnableInQueueEvent(appID)
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_APP_CANNOTRUN_QUEUE, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "", event.Message)
}

func TestSendAppCannotRunByQuotaEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := NewApplicationEvents(eventSystem)
	appEvents.SendAppNotRunnableQuotaEvent(appID)
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = NewApplicationEvents(eventSystem)
	appEvents.SendAppNotRunnableQuotaEvent(appID)
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_APP_CANNOTRUN_QUOTA, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "", event.Message)
}

func TestSendAppRunnableInQueueEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := NewApplicationEvents(eventSystem)
	appEvents.SendAppRunnableInQueueEvent(appID)
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = NewApplicationEvents(eventSystem)
	appEvents.SendAppRunnableInQueueEvent(appID)
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_APP_RUNNABLE_QUEUE, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "", event.Message)
}

func TestSendAppRunnableByQuotaEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := NewApplicationEvents(eventSystem)
	appEvents.SendAppRunnableQuotaEvent(appID)
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = NewApplicationEvents(eventSystem)
	appEvents.SendAppRunnableQuotaEvent(appID)
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_APP_RUNNABLE_QUOTA, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "", event.Message)
}
