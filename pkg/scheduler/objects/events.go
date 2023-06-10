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

	"github.com/apache/yunikorn-core/pkg/events"
)

type applicationEvents struct {
	enabled     bool
	eventSystem events.EventSystem
	app         *Application
}

func (evt *applicationEvents) sendAppDoesNotFitEvent(request *AllocationAsk) {
	if !evt.enabled {
		return
	}

	message := fmt.Sprintf("Application %s does not fit into %s queue", request.GetApplicationID(), evt.app.queuePath)
	event := events.CreateRequestEventRecord(request.GetAllocationKey(), request.GetApplicationID(), "InsufficientQueueResources", message)
	evt.eventSystem.AddEvent(event)
}

func (evt *applicationEvents) sendPlaceholderLargerEvent(ph *Allocation, request *AllocationAsk) {
	if !evt.enabled {
		return
	}

	message := fmt.Sprintf("Task group '%s' in application '%s': allocation resources '%s' are not matching placeholder '%s' allocation with ID '%s'", ph.GetTaskGroup(), evt.app.ApplicationID, request.GetAllocatedResource().String(), ph.GetAllocatedResource().String(), ph.GetAllocationKey())
	event := events.CreateRequestEventRecord(ph.GetAllocationKey(), evt.app.ApplicationID, "releasing placeholder: real allocation is larger than placeholder", message)
	evt.eventSystem.AddEvent(event)
}

func newApplicationEvents(app *Application, evt events.EventSystem) *applicationEvents {
	return &applicationEvents{
		eventSystem: evt,
		enabled:     evt != nil,
		app:         app,
	}
}
