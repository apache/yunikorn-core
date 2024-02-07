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
	"github.com/apache/yunikorn-core/pkg/events/mock"
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
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := newApplicationEvents(app, eventSystem)
	appEvents.sendAppDoesNotFitEvent(&AllocationAsk{}, &resources.Resource{})
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = newApplicationEvents(app, eventSystem)
	appEvents.sendAppDoesNotFitEvent(&AllocationAsk{
		applicationID: appID0,
		allocationKey: aKey,
	}, &resources.Resource{})
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
}

func TestSendAppDoesNotFitEventWithRateLimiter(t *testing.T) {
	app := &Application{
		queuePath: "root.test",
	}
	eventSystem := mock.NewEventSystem()
	appEvents := newApplicationEvents(app, eventSystem)
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
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
}

func TestSendPlaceholderLargerEvent(t *testing.T) {
	app := &Application{
		queuePath: "root.test",
	}
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := newApplicationEvents(app, eventSystem)
	appEvents.sendPlaceholderLargerEvent(&Allocation{}, &AllocationAsk{})
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = newApplicationEvents(app, eventSystem)
	appEvents.sendPlaceholderLargerEvent(&Allocation{
		allocationKey: aKey,
	}, &AllocationAsk{
		applicationID: appID0,
		allocationKey: aKey,
	})
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
}

func TestSendNewAllocationEvent(t *testing.T) {
	app := &Application{
		ApplicationID: appID0,
		queuePath:     "root.test",
	}
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := newApplicationEvents(app, eventSystem)
	appEvents.sendNewAllocationEvent(&Allocation{})
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = newApplicationEvents(app, eventSystem)
	assert.Assert(t, appEvents.eventSystem != nil, "event system should not be nil")
	appEvents.sendNewAllocationEvent(&Allocation{
		applicationID: appID0,
		allocationKey: aKey,
		allocationID:  aAllocationID,
	})
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	assert.Equal(t, si.EventRecord_APP, eventSystem.Events[0].Type, "event type is not expected")
	assert.Equal(t, si.EventRecord_ADD, eventSystem.Events[0].EventChangeType, "event change type is not expected")
	assert.Equal(t, si.EventRecord_APP_ALLOC, eventSystem.Events[0].EventChangeDetail, "event change detail is not expected")
	assert.Equal(t, appID0, eventSystem.Events[0].ObjectID, "event object id is not expected")
	assert.Equal(t, aAllocationID, eventSystem.Events[0].ReferenceID, "event reference id is not expected")
	assert.Equal(t, common.Empty, eventSystem.Events[0].Message, "message is not expected")
}

func TestSendNewAskEvent(t *testing.T) {
	app := &Application{
		ApplicationID: appID0,
		queuePath:     "root.test",
	}
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := newApplicationEvents(app, eventSystem)
	appEvents.sendNewAskEvent(&AllocationAsk{})
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = newApplicationEvents(app, eventSystem)
	assert.Assert(t, appEvents.eventSystem != nil, "event system should not be nil")
	appEvents.sendNewAskEvent(&AllocationAsk{
		applicationID: appID0,
		allocationKey: aKey,
	})
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	assert.Equal(t, si.EventRecord_APP, eventSystem.Events[0].Type, "event type is not expected")
	assert.Equal(t, si.EventRecord_ADD, eventSystem.Events[0].EventChangeType, "event change type is not expected")
	assert.Equal(t, si.EventRecord_APP_REQUEST, eventSystem.Events[0].EventChangeDetail, "event change detail is not expected")
	assert.Equal(t, appID0, eventSystem.Events[0].ObjectID, "event object id is not expected")
	assert.Equal(t, aKey, eventSystem.Events[0].ReferenceID, "event reference id is not expected")
	assert.Equal(t, common.Empty, eventSystem.Events[0].Message, "message is not expected")
}

func TestSendRemoveAllocationEvent(t *testing.T) {
	app := &Application{
		ApplicationID: appID0,
		queuePath:     "root.test",
	}
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := newApplicationEvents(app, eventSystem)
	appEvents.sendRemoveAllocationEvent(&Allocation{}, si.TerminationType_STOPPED_BY_RM)
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	testCases := []struct {
		name                 string
		eventSystemMock *mock.EventSystem
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
			eventSystemMock: mock.NewEventSystem(),
			terminationType:      si.TerminationType_UNKNOWN_TERMINATION_TYPE,
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, allocationID: aAllocationID},
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_NODEREMOVED,
			expectedObjectID:     appID0,
			expectedReferenceID:  aAllocationID,
		},
		{
			name:                 "remove allocation cause of resource manager cancel",
			eventSystemMock: mock.NewEventSystem(),
			terminationType:      si.TerminationType_STOPPED_BY_RM,
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, allocationID: aAllocationID},
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_CANCEL,
			expectedObjectID:     appID0,
			expectedReferenceID:  aAllocationID,
		},
		{
			name:                 "remove allocation cause of timeout",
			eventSystemMock: mock.NewEventSystem(),
			terminationType:      si.TerminationType_TIMEOUT,
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, allocationID: aAllocationID},
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_TIMEOUT,
			expectedObjectID:     appID0,
			expectedReferenceID:  aAllocationID,
		},
		{
			name:                 "remove allocation cause of preemption",
			eventSystemMock: mock.NewEventSystem(),
			terminationType:      si.TerminationType_PREEMPTED_BY_SCHEDULER,
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, allocationID: aAllocationID},
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_PREEMPT,
			expectedObjectID:     appID0,
			expectedReferenceID:  aAllocationID,
		},
		{
			name:                 "remove allocation cause of replacement",
			eventSystemMock: mock.NewEventSystem(),
			terminationType:      si.TerminationType_PLACEHOLDER_REPLACED,
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, allocationID: aAllocationID},
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_ALLOC_REPLACED,
			expectedObjectID:     appID0,
			expectedReferenceID:  aAllocationID,
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
				assert.Equal(t, testCase.expectedEventCnt, len(testCase.eventSystemMock.Events), "event was not generated")
				assert.Equal(t, testCase.expectedType, testCase.eventSystemMock.Events[0].Type, "event type is not expected")
				assert.Equal(t, testCase.expectedChangeType, testCase.eventSystemMock.Events[0].EventChangeType, "event change type is not expected")
				assert.Equal(t, testCase.expectedChangeDetail, testCase.eventSystemMock.Events[0].EventChangeDetail, "event change detail is not expected")
				assert.Equal(t, testCase.expectedObjectID, testCase.eventSystemMock.Events[0].ObjectID, "event object id is not expected")
				assert.Equal(t, testCase.expectedReferenceID, testCase.eventSystemMock.Events[0].ReferenceID, "event reference id is not expected")
				assert.Equal(t, common.Empty, testCase.eventSystemMock.Events[0].Message, "message is not expected")
			}
		})
	}
}

func TestSendRemoveAskEvent(t *testing.T) {
	app := &Application{
		ApplicationID: appID0,
		queuePath:     "root.test",
	}
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := newApplicationEvents(app, eventSystem)
	appEvents.sendRemoveAskEvent(&AllocationAsk{}, si.EventRecord_REQUEST_CANCEL)
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	ask := &AllocationAsk{
		applicationID: appID0,
		allocationKey: aKey}
	eventSystem = mock.NewEventSystem()
	appEvents = newApplicationEvents(app, eventSystem)
	appEvents.sendRemoveAskEvent(ask, si.EventRecord_REQUEST_CANCEL)
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_REQUEST_CANCEL, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "alloc-1", event.ReferenceID)
	assert.Equal(t, "", event.Message)

	eventSystem.Reset()
	appEvents.sendRemoveAskEvent(ask, si.EventRecord_REQUEST_TIMEOUT)
	event = eventSystem.Events[0]
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
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := newApplicationEvents(app, eventSystem)
	appEvents.sendNewApplicationEvent()
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	mockEvents := mock.NewEventSystem()
	appEvents = newApplicationEvents(app, mockEvents)
	appEvents.sendNewApplicationEvent()
	event := mockEvents.Events[0]
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
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := newApplicationEvents(app, eventSystem)
	appEvents.sendRemoveApplicationEvent()
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = newApplicationEvents(app, eventSystem)
	appEvents.sendRemoveApplicationEvent()
	event := eventSystem.Events[0]
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
	eventSystem := mock.NewEventSystemDisabled()
	appEvents := newApplicationEvents(app, eventSystem)
	appEvents.sendStateChangeEvent(si.EventRecord_APP_RUNNING, "")
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = newApplicationEvents(app, eventSystem)
	appEvents.sendStateChangeEvent(si.EventRecord_APP_RUNNING, "The application is running")
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_APP_RUNNING, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "The application is running", event.Message)

	eventSystem = mock.NewEventSystemDisabled()
	appEvents = newApplicationEvents(app, eventSystem)
	appEvents.sendStateChangeEvent(si.EventRecord_APP_RUNNING, "ResourceReservationTimeout")
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	appEvents = newApplicationEvents(app, eventSystem)
	appEvents.sendStateChangeEvent(si.EventRecord_APP_REJECT, "Failed to add application to partition (placement rejected)")
	event = eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_APP, event.Type)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_APP_REJECT, event.EventChangeDetail)
	assert.Equal(t, "app-0", event.ObjectID)
	assert.Equal(t, "", event.ReferenceID)
	assert.Equal(t, "Failed to add application to partition (placement rejected)", event.Message)
}
