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
	"fmt"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	evt "github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type ApplicationEvents struct {
	eventSystem evt.EventSystem
}

func (ae *ApplicationEvents) SendPlaceholderLargerEvent(taskGroup, appID, phAllocKey string, askRes, phRes *resources.Resource) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	message := fmt.Sprintf("Task group '%s' in application '%s': allocation resources '%s' are not matching placeholder '%s' allocation with ID '%s'", taskGroup, appID, askRes, phRes, phAllocKey)
	event := evt.CreateRequestEventRecord(phAllocKey, appID, message, askRes)
	ae.eventSystem.AddEvent(event)
}

func (ae *ApplicationEvents) SendNewAllocationEvent(appID, allocKey string, allocated *resources.Resource) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := evt.CreateAppEventRecord(appID, common.Empty, allocKey, si.EventRecord_ADD, si.EventRecord_APP_ALLOC, allocated)
	ae.eventSystem.AddEvent(event)
}

func (ae *ApplicationEvents) SendNewAskEvent(appID, allocKey string, allocated *resources.Resource) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := evt.CreateAppEventRecord(appID, common.Empty, allocKey, si.EventRecord_ADD, si.EventRecord_APP_REQUEST, allocated)
	ae.eventSystem.AddEvent(event)
}

func (ae *ApplicationEvents) SendRemoveAllocationEvent(appID, allocKey string, allocated *resources.Resource, terminationType si.TerminationType) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
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

	event := evt.CreateAppEventRecord(appID, common.Empty, allocKey, si.EventRecord_REMOVE, eventChangeDetail, allocated)
	ae.eventSystem.AddEvent(event)
}

func (ae *ApplicationEvents) SendRemoveAskEvent(appID, allocKey string, allocated *resources.Resource, detail si.EventRecord_ChangeDetail) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := evt.CreateAppEventRecord(appID, common.Empty, allocKey, si.EventRecord_REMOVE, detail, allocated)
	ae.eventSystem.AddEvent(event)
}

func (ae *ApplicationEvents) SendNewApplicationEvent(appID string) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := evt.CreateAppEventRecord(appID, common.Empty, common.Empty, si.EventRecord_ADD, si.EventRecord_APP_NEW, nil)
	ae.eventSystem.AddEvent(event)
}

func (ae *ApplicationEvents) SendRemoveApplicationEvent(appID string) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := evt.CreateAppEventRecord(appID, common.Empty, common.Empty, si.EventRecord_REMOVE, si.EventRecord_DETAILS_NONE, nil)
	ae.eventSystem.AddEvent(event)
}

func (ae *ApplicationEvents) SendStateChangeEvent(appID string, changeDetail si.EventRecord_ChangeDetail, eventInfo string) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := evt.CreateAppEventRecord(appID, eventInfo, common.Empty, si.EventRecord_SET, changeDetail, nil)
	ae.eventSystem.AddEvent(event)
}

func (ae *ApplicationEvents) SendAppNotRunnableInQueueEvent(appID string) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := evt.CreateAppEventRecord(appID, common.Empty, common.Empty, si.EventRecord_NONE, si.EventRecord_APP_CANNOTRUN_QUEUE, nil)
	ae.eventSystem.AddEvent(event)
}

func (ae *ApplicationEvents) SendAppRunnableInQueueEvent(appID string) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := evt.CreateAppEventRecord(appID, common.Empty, common.Empty, si.EventRecord_NONE, si.EventRecord_APP_RUNNABLE_QUEUE, nil)
	ae.eventSystem.AddEvent(event)
}

func (ae *ApplicationEvents) SendAppNotRunnableQuotaEvent(appID string) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := evt.CreateAppEventRecord(appID, common.Empty, common.Empty, si.EventRecord_NONE, si.EventRecord_APP_CANNOTRUN_QUOTA, nil)
	ae.eventSystem.AddEvent(event)
}

func (ae *ApplicationEvents) SendAppRunnableQuotaEvent(appID string) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := evt.CreateAppEventRecord(appID, common.Empty, common.Empty, si.EventRecord_NONE, si.EventRecord_APP_RUNNABLE_QUOTA, nil)
	ae.eventSystem.AddEvent(event)
}

func NewApplicationEvents(es evt.EventSystem) *ApplicationEvents {
	return &ApplicationEvents{
		eventSystem: es,
	}
}
