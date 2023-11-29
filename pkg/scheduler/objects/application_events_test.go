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
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func isNewApplicationEvent(t *testing.T, app *Application, record *si.EventRecord) {
	assert.Equal(t, si.EventRecord_APP, record.Type, "incorrect event type, expect app")
	assert.Equal(t, app.ApplicationID, record.ObjectID, "incorrect object ID, expected application ID")
	assert.Equal(t, si.EventRecord_ADD, record.EventChangeType, "incorrect change type, expected add")
	assert.Equal(t, si.EventRecord_DETAILS_NONE, record.EventChangeDetail, "incorrect change detail, expected none")
}

func isRemoveApplicationEvent(t *testing.T, app *Application, record *si.EventRecord) {
	assert.Equal(t, si.EventRecord_APP, record.Type, "incorrect event type, expect app")
	assert.Equal(t, app.ApplicationID, record.ObjectID, "incorrect object ID, expected application ID")
	assert.Equal(t, si.EventRecord_REMOVE, record.EventChangeType, "incorrect change type, expected remove")
	assert.Equal(t, si.EventRecord_DETAILS_NONE, record.EventChangeDetail, "incorrect change detail, expected none")
}

func isStateChangeEvent(t *testing.T, app *Application, changeDetail si.EventRecord_ChangeDetail, record *si.EventRecord) {
	assert.Equal(t, si.EventRecord_APP, record.Type, "incorrect event type, expect app")
	assert.Equal(t, app.ApplicationID, record.ObjectID, "incorrect object ID, expected application ID")
	assert.Equal(t, si.EventRecord_SET, record.EventChangeType, "incorrect change type, expected set")
	assert.Equal(t, changeDetail, record.EventChangeDetail, "incorrect change detail")
}

func TestSendAppDoesNotFitEvent(t *testing.T) {
	app := &Application{
		queuePath: "root.test",
	}
	mock := newEventSystemMockDisabled()
	appEvents := newApplicationEvents(app, mock)
	appEvents.sendAppDoesNotFitEvent(&AllocationAsk{}, &resources.Resource{})
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	appEvents = newApplicationEvents(app, mock)
	appEvents.sendAppDoesNotFitEvent(&AllocationAsk{
		applicationID: appID0,
		allocationKey: aKey,
	}, &resources.Resource{})
	assert.Equal(t, 1, len(mock.events), "event was not generated")
}

func TestSendAppDoesNotFitEventWithRateLimiter(t *testing.T) {
	app := &Application{
		queuePath: "root.test",
	}
	mock := newEventSystemMock()
	appEvents := newApplicationEvents(app, mock)
	startTime := time.Now()
	for {
		elapsed := time.Since(startTime)
		if elapsed > 500*time.Millisecond {
			break
		}
		appEvents.sendAppDoesNotFitEvent(&AllocationAsk{
			applicationID: appID0,
			allocationKey: aKey,
		}, &resources.Resource{})
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(t, 1, len(mock.events), "event was not generated")
}

func TestSendPlaceholderLargerEvent(t *testing.T) {
	app := &Application{
		queuePath: "root.test",
	}
	mock := newEventSystemMockDisabled()
	appEvents := newApplicationEvents(app, mock)
	appEvents.sendPlaceholderLargerEvent(&Allocation{}, &AllocationAsk{})
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	appEvents = newApplicationEvents(app, mock)
	appEvents.sendPlaceholderLargerEvent(&Allocation{
		allocationKey: aKey,
	}, &AllocationAsk{
		applicationID: appID0,
		allocationKey: aKey,
	})
	assert.Equal(t, 1, len(mock.events), "event was not generated")
}

func TestSendNewAllocationEvent(t *testing.T) {
	app := &Application{
		ApplicationID: appID0,
		queuePath:     "root.test",
	}
	mock := newEventSystemMockDisabled()
	appEvents := newApplicationEvents(app, mock)
	appEvents.sendNewAllocationEvent(&Allocation{})
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	appEvents = newApplicationEvents(app, mock)
	assert.Assert(t, appEvents.eventSystem != nil, "event system should not be nil")
	appEvents.sendNewAllocationEvent(&Allocation{
		applicationID: appID0,
		allocationKey: aKey,
		allocationID:  aUUID,
	})
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	assert.Equal(t, si.EventRecord_APP, mock.events[0].Type, "event type is not expected")
	assert.Equal(t, si.EventRecord_ADD, mock.events[0].EventChangeType, "event change type is not expected")
	assert.Equal(t, si.EventRecord_APP_ALLOC, mock.events[0].EventChangeDetail, "event change detail is not expected")
	assert.Equal(t, appID0, mock.events[0].ObjectID, "event object id is not expected")
	assert.Equal(t, aUUID, mock.events[0].ReferenceID, "event reference id is not expected")
	assert.Equal(t, common.Empty, mock.events[0].Message, "message is not expected")
}

func TestSendNewAskEvent(t *testing.T) {
	app := &Application{
		ApplicationID: appID0,
		queuePath:     "root.test",
	}
	mock := newEventSystemMockDisabled()
	appEvents := newApplicationEvents(app, mock)
	appEvents.sendNewAskEvent(&AllocationAsk{})
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	appEvents = newApplicationEvents(app, mock)
	assert.Assert(t, appEvents.eventSystem != nil, "event system should not be nil")
	appEvents.sendNewAskEvent(&AllocationAsk{
		applicationID: appID0,
		allocationKey: aKey,
	})
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	assert.Equal(t, si.EventRecord_APP, mock.events[0].Type, "event type is not expected")
	assert.Equal(t, si.EventRecord_ADD, mock.events[0].EventChangeType, "event change type is not expected")
	assert.Equal(t, si.EventRecord_APP_REQUEST, mock.events[0].EventChangeDetail, "event change detail is not expected")
	assert.Equal(t, appID0, mock.events[0].ObjectID, "event object id is not expected")
	assert.Equal(t, aKey, mock.events[0].ReferenceID, "event reference id is not expected")
	assert.Equal(t, common.Empty, mock.events[0].Message, "message is not expected")
}

func TestSendRemoveAllocationEvent(t *testing.T) {
	app := &Application{
		ApplicationID: appID0,
		queuePath:     "root.test",
	}
	mock := newEventSystemMockDisabled()
	appEvents := newApplicationEvents(app, mock)
	appEvents.sendRemoveAllocationEvent(&Allocation{}, si.TerminationType_STOPPED_BY_RM)
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	testCases := []struct {
		name                 string
		eventSystemMock      *EventSystemMock
		terminationType      si.TerminationType
		allocation           *Allocation
		expectedEventCnt     int
		expectedType         si.EventRecord_Type
		expectedChangeType   si.EventRecord_ChangeType
		expectedChangeDetail si.EventRecord_ChangeDetail
		expectedObjectID     string
		expectedReferenceID  string
	}{
		{
			name:                 "remove allocation cause of node removal",
			eventSystemMock:      newEventSystemMock(),
			terminationType:      si.TerminationType_UNKNOWN_TERMINATION_TYPE,
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, allocationID: aUUID},
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_NODEREMOVED,
			expectedObjectID:     appID0,
			expectedReferenceID:  aUUID,
		},
		{
			name:                 "remove allocation cause of resource manager cancel",
			eventSystemMock:      newEventSystemMock(),
			terminationType:      si.TerminationType_STOPPED_BY_RM,
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, allocationID: aUUID},
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_CANCEL,
			expectedObjectID:     appID0,
			expectedReferenceID:  aUUID,
		},
		{
			name:                 "remove allocation cause of timeout",
			eventSystemMock:      newEventSystemMock(),
			terminationType:      si.TerminationType_TIMEOUT,
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, allocationID: aUUID},
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_TIMEOUT,
			expectedObjectID:     appID0,
			expectedReferenceID:  aUUID,
		},
		{
			name:                 "remove allocation cause of preemption",
			eventSystemMock:      newEventSystemMock(),
			terminationType:      si.TerminationType_PREEMPTED_BY_SCHEDULER,
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, allocationID: aUUID},
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_PREEMPT,
			expectedObjectID:     appID0,
			expectedReferenceID:  aUUID,
		},
		{
			name:                 "remove allocation cause of replacement",
			eventSystemMock:      newEventSystemMock(),
			terminationType:      si.TerminationType_PLACEHOLDER_REPLACED,
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, allocationID: aUUID},
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_REPLACED,
			expectedObjectID:     appID0,
			expectedReferenceID:  aUUID,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.eventSystemMock == nil {
				appEvents := newApplicationEvents(app, nil)
				assert.Assert(t, appEvents.eventSystem == nil, "event system should be nil")
				appEvents.sendRemoveAllocationEvent(testCase.allocation, testCase.terminationType)
			} else {
				appEvents := newApplicationEvents(app, testCase.eventSystemMock)
				assert.Assert(t, appEvents.eventSystem != nil, "event system should not be nil")
				appEvents.sendRemoveAllocationEvent(testCase.allocation, testCase.terminationType)
				assert.Equal(t, testCase.expectedEventCnt, len(testCase.eventSystemMock.events), "event was not generated")
				assert.Equal(t, testCase.expectedType, testCase.eventSystemMock.events[0].Type, "event type is not expected")
				assert.Equal(t, testCase.expectedChangeType, testCase.eventSystemMock.events[0].EventChangeType, "event change type is not expected")
				assert.Equal(t, testCase.expectedChangeDetail, testCase.eventSystemMock.events[0].EventChangeDetail, "event change detail is not expected")
				assert.Equal(t, testCase.expectedObjectID, testCase.eventSystemMock.events[0].ObjectID, "event object id is not expected")
				assert.Equal(t, testCase.expectedReferenceID, testCase.eventSystemMock.events[0].ReferenceID, "event reference id is not expected")
				assert.Equal(t, common.Empty, testCase.eventSystemMock.events[0].Message, "message is not expected")
			}
		})
	}
}

func TestSendRemoveAskEvent(t *testing.T) {
	app := &Application{
		ApplicationID: appID0,
		queuePath:     "root.test",
	}
	mock := newEventSystemMockDisabled()
	appEvents := newApplicationEvents(app, mock)
	appEvents.sendRemoveAskEvent(&AllocationAsk{}, si.EventRecord_REQUEST_CANCEL)
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	ask := &AllocationAsk{
		applicationID: appID0,
		allocationKey: aKey}
	mock = newEventSystemMock()
	appEvents = newApplicationEvents(app, mock)
	appEvents.sendRemoveAskEvent(ask, si.EventRecord_REQUEST_CANCEL)
	event := mock.events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_REQUEST_CANCEL, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "alloc-1", event.ReferenceID)
	assert.Equal(t, "", event.Message)

	mock.Reset()
	appEvents.sendRemoveAskEvent(ask, si.EventRecord_REQUEST_TIMEOUT)
	event = mock.events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_REQUEST_TIMEOUT, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "alloc-1", event.ReferenceID)
	assert.Equal(t, "", event.Message)
}

func TestSendNewApplicationEvent(t *testing.T) {
	app := &Application{
		ApplicationID: appID0,
		queuePath:     "root.test",
	}
	mock := newEventSystemMockDisabled()
	appEvents := newApplicationEvents(app, mock)
	appEvents.sendNewApplicationEvent()
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mockEvents := newEventSystemMock()
	appEvents = newApplicationEvents(app, mockEvents)
	appEvents.sendNewApplicationEvent()
	event := mockEvents.events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "", event.Message)
}

func TestSendRemoveApplicationEvent(t *testing.T) {
	app := &Application{
		ApplicationID: appID0,
		queuePath:     "root.test",
	}
	mock := newEventSystemMockDisabled()
	appEvents := newApplicationEvents(app, mock)
	appEvents.sendRemoveApplicationEvent()
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	appEvents = newApplicationEvents(app, mock)
	appEvents.sendRemoveApplicationEvent()
	event := mock.events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "", event.Message)
}

func TestSendStateChangeEvent(t *testing.T) {
	app := &Application{
		ApplicationID:         appID0,
		queuePath:             "root.test",
		sendStateChangeEvents: true,
	}
	mock := newEventSystemMockDisabled()
	appEvents := newApplicationEvents(app, mock)
	appEvents.sendStateChangeEvent(si.EventRecord_APP_RUNNING, "")
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	appEvents = newApplicationEvents(app, mock)
	appEvents.sendStateChangeEvent(si.EventRecord_APP_RUNNING, "The application is running")
	event := mock.events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_APP_RUNNING, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "The application is running", event.Message)

	mock = newEventSystemMockDisabled()
	appEvents = newApplicationEvents(app, mock)
	appEvents.sendStateChangeEvent(si.EventRecord_APP_RUNNING, "ResourceReservationTimeout")
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	appEvents = newApplicationEvents(app, mock)
	appEvents.sendStateChangeEvent(si.EventRecord_APP_REJECT, "Failed to add application to partition (placement rejected)")
	event = mock.events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_APP_REJECT, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "Failed to add application to partition (placement rejected)", event.Message)
}
