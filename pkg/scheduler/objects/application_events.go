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
	"fmt"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type applicationEvents struct {
	eventSystem events.EventSystem
	app         *Application
}

func (evt *applicationEvents) sendAppDoesNotFitEvent(request *AllocationAsk) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	message := fmt.Sprintf("Application %s does not fit into %s queue", request.GetApplicationID(), evt.app.queuePath)
	event := events.CreateRequestEventRecord(request.GetAllocationKey(), request.GetApplicationID(), message, request.GetAllocatedResource())
	evt.eventSystem.AddEvent(event)
}

func (evt *applicationEvents) sendPlaceholderLargerEvent(ph *Allocation, request *AllocationAsk) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	message := fmt.Sprintf("Task group '%s' in application '%s': allocation resources '%s' are not matching placeholder '%s' allocation with ID '%s'", ph.GetTaskGroup(), evt.app.ApplicationID, request.GetAllocatedResource().String(), ph.GetAllocatedResource().String(), ph.GetAllocationKey())
	event := events.CreateRequestEventRecord(ph.GetAllocationKey(), evt.app.ApplicationID, message, request.GetAllocatedResource())
	evt.eventSystem.AddEvent(event)
}

func (evt *applicationEvents) sendNewAllocationEvent(alloc *Allocation) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateAppEventRecord(evt.app.ApplicationID, common.Empty, alloc.GetUUID(), si.EventRecord_ADD, si.EventRecord_APP_ALLOC, alloc.GetAllocatedResource())
	evt.eventSystem.AddEvent(event)
}

func (evt *applicationEvents) sendNewAskEvent(request *AllocationAsk) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateAppEventRecord(evt.app.ApplicationID, common.Empty, request.GetAllocationKey(), si.EventRecord_ADD, si.EventRecord_APP_REQUEST, request.GetAllocatedResource())
	evt.eventSystem.AddEvent(event)
}

func (evt *applicationEvents) sendRemoveAllocationEvent(alloc *Allocation, terminationType si.TerminationType) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}

	var eventChangeDetail si.EventRecord_ChangeDetail
	switch terminationType {
	case si.TerminationType_UNKNOWN_TERMINATION_TYPE:
		eventChangeDetail = si.EventRecord_ALLOC_NODEREMOVED
	case si.TerminationType_STOPPED_BY_RM:
		eventChangeDetail = si.EventRecord_ALLOC_CANCEL
	case si.TerminationType_TIMEOUT:
		eventChangeDetail = si.EventRecord_ALLOC_TIMEOUT
	case si.TerminationType_PREEMPTED_BY_SCHEDULER:
		eventChangeDetail = si.EventRecord_ALLOC_PREEMPT
	case si.TerminationType_PLACEHOLDER_REPLACED:
		eventChangeDetail = si.EventRecord_ALLOC_REPLACED
	}

	event := events.CreateAppEventRecord(evt.app.ApplicationID, common.Empty, alloc.GetUUID(), si.EventRecord_REMOVE, eventChangeDetail, alloc.GetAllocatedResource())
	evt.eventSystem.AddEvent(event)
}

func (evt *applicationEvents) sendRemoveAskEvent(request *AllocationAsk, detail si.EventRecord_ChangeDetail) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateAppEventRecord(evt.app.ApplicationID, "", request.GetAllocationKey(), si.EventRecord_REMOVE, detail, request.GetAllocatedResource())
	evt.eventSystem.AddEvent(event)
}

func (evt *applicationEvents) sendNewApplicationEvent() {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateAppEventRecord(evt.app.ApplicationID, "", "", si.EventRecord_ADD, si.EventRecord_DETAILS_NONE, evt.app.allocatedResource)
	evt.eventSystem.AddEvent(event)
}

func (evt *applicationEvents) sendRemoveApplicationEvent() {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateAppEventRecord(evt.app.ApplicationID, "", "", si.EventRecord_REMOVE, si.EventRecord_DETAILS_NONE, evt.app.allocatedResource)
	evt.eventSystem.AddEvent(event)
}

func (evt *applicationEvents) sendRejectApplicationEvent(eventInfo string) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateAppEventRecord(evt.app.ApplicationID, eventInfo, "", si.EventRecord_REMOVE, si.EventRecord_APP_REJECT, evt.app.allocatedResource)
	evt.eventSystem.AddEvent(event)
}

func (evt *applicationEvents) sendStateChangeEvent(changeDetail si.EventRecord_ChangeDetail) {
	if !evt.eventSystem.IsEventTrackingEnabled() || !evt.app.sendStateChangeEvents {
		return
	}
	event := events.CreateAppEventRecord(evt.app.ApplicationID, "", "", si.EventRecord_SET, changeDetail, evt.app.allocatedResource)
	evt.eventSystem.AddEvent(event)
}

func newApplicationEvents(app *Application, evt events.EventSystem) *applicationEvents {
	return &applicationEvents{
		eventSystem: evt,
		app:         app,
	}
}
