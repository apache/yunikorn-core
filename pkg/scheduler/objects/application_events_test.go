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

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

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

func TestSendNewAllocationEvent(t *testing.T) {
	app := &Application{
		ApplicationID: appID0,
		queuePath:     "root.test",
	}

	// not enabled
	evt := newApplicationEvents(app, nil)
	assert.Assert(t, evt.eventSystem == nil, "event system should be nil")
	assert.Assert(t, !evt.enabled, "event system should be disabled")
	evt.sendNewAllocationEvent(&Allocation{})

	// enabled
	mock := newEventSystemMock()
	evt = newApplicationEvents(app, mock)
	assert.Assert(t, evt.eventSystem != nil, "event system should not be nil")
	assert.Assert(t, evt.enabled, "event system should be enabled")
	evt.sendNewAllocationEvent(&Allocation{
		applicationID: appID0,
		allocationKey: aKey,
		uuid:          aUUID,
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

	// not enabled
	evt := newApplicationEvents(app, nil)
	assert.Assert(t, evt.eventSystem == nil, "event system should be nil")
	assert.Assert(t, !evt.enabled, "event system should be disabled")
	evt.sendNewAskEvent(&AllocationAsk{})

	// enabled
	mock := newEventSystemMock()
	evt = newApplicationEvents(app, mock)
	assert.Assert(t, evt.eventSystem != nil, "event system should not be nil")
	assert.Assert(t, evt.enabled, "event system should be enabled")
	evt.sendNewAskEvent(&AllocationAsk{
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
			name:            "disabled event system",
			eventSystemMock: nil,
			terminationType: si.TerminationType_UNKNOWN_TERMINATION_TYPE,
			allocation:      &Allocation{},
		},
		{
			name:                 "remove allocation cause of node removal",
			eventSystemMock:      newEventSystemMock(),
			terminationType:      si.TerminationType_UNKNOWN_TERMINATION_TYPE,
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, uuid: aUUID},
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
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, uuid: aUUID},
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
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, uuid: aUUID},
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
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, uuid: aUUID},
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
			allocation:           &Allocation{applicationID: appID0, allocationKey: aKey, uuid: aUUID},
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
				evt := newApplicationEvents(app, nil)
				assert.Assert(t, evt.eventSystem == nil, "event system should be nil")
				assert.Assert(t, !evt.enabled, "event system should be disabled")
				evt.sendRemoveAllocationEvent(testCase.allocation, testCase.terminationType)
			} else {
				evt := newApplicationEvents(app, testCase.eventSystemMock)
				assert.Assert(t, evt.eventSystem != nil, "event system should not be nil")
				assert.Assert(t, evt.enabled, "event system should be enabled")
				evt.sendRemoveAllocationEvent(testCase.allocation, testCase.terminationType)
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

	testCases := []struct {
		name                 string
		eventSystemMock      *EventSystemMock
		terminationType      si.TerminationType
		appRemoved           bool
		allocationAsk        *AllocationAsk
		expectedEventCnt     int
		expectedType         si.EventRecord_Type
		expectedChangeType   si.EventRecord_ChangeType
		expectedChangeDetail si.EventRecord_ChangeDetail
		expectedObjectID     string
		expectedReferenceID  string
	}{
		{
			name:            "disabled event system",
			eventSystemMock: nil,
			terminationType: si.TerminationType_UNKNOWN_TERMINATION_TYPE,
			allocationAsk:   &AllocationAsk{},
		},
		{
			name:                 "remove allocation ask cause of resource manager cancel",
			eventSystemMock:      newEventSystemMock(),
			terminationType:      si.TerminationType_STOPPED_BY_RM,
			allocationAsk:        &AllocationAsk{applicationID: appID0, allocationKey: aKey},
			appRemoved:           false,
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_APP_REQUEST,
			expectedObjectID:     appID0,
			expectedReferenceID:  aKey,
		},
		{
			name:                 "remove allocation ask cause of timeout",
			eventSystemMock:      newEventSystemMock(),
			terminationType:      si.TerminationType_TIMEOUT,
			allocationAsk:        &AllocationAsk{applicationID: appID0, allocationKey: aKey},
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_REQUEST_TIMEOUT,
			expectedObjectID:     appID0,
			expectedReferenceID:  aKey,
		},
		{
			name:                 "remove allocation cause of application removal",
			eventSystemMock:      newEventSystemMock(),
			terminationType:      si.TerminationType_STOPPED_BY_RM,
			allocationAsk:        &AllocationAsk{applicationID: appID0, allocationKey: aKey},
			appRemoved:           true,
			expectedEventCnt:     1,
			expectedType:         si.EventRecord_APP,
			expectedChangeType:   si.EventRecord_REMOVE,
			expectedChangeDetail: si.EventRecord_REQUEST_CANCEL,
			expectedObjectID:     appID0,
			expectedReferenceID:  aKey,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.eventSystemMock == nil {
				evt := newApplicationEvents(app, nil)
				assert.Assert(t, evt.eventSystem == nil, "event system should be nil")
				assert.Assert(t, !evt.enabled, "event system should be disabled")
				evt.sendRemoveAskEvent(testCase.allocationAsk, testCase.terminationType, testCase.appRemoved)
			} else {
				evt := newApplicationEvents(app, testCase.eventSystemMock)
				assert.Assert(t, evt.eventSystem != nil, "event system should not be nil")
				assert.Assert(t, evt.enabled, "event system should be enabled")
				evt.sendRemoveAskEvent(testCase.allocationAsk, testCase.terminationType, testCase.appRemoved)
				assert.Equal(t, testCase.expectedEventCnt, len(testCase.eventSystemMock.events), "event was not generated")
				assert.Equal(t, testCase.expectedType, testCase.eventSystemMock.events[0].Type, "event type is not expected")
				assert.Equal(t, testCase.expectedChangeType, testCase.eventSystemMock.events[0].EventChangeType, "event change type is not expected")
				assert.Equal(t, testCase.expectedChangeDetail, testCase.eventSystemMock.events[0].EventChangeDetail, "event change detail is not expected")
				assert.Equal(t, testCase.expectedObjectID, testCase.eventSystemMock.events[0].ObjectID, "event object id is not expected")
				assert.Equal(t, testCase.expectedReferenceID, testCase.eventSystemMock.events[0].ReferenceID, "event reference id is not expected")
			}
		})
	}
}
