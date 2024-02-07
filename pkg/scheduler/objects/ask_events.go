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
	"time"

	"golang.org/x/time/rate"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events"
)

// Ask-specific events. These events are of REQUEST type, so they are eventually sent to the respective pods in K8s.
type askEvents struct {
	eventSystem events.EventSystem
	ask         *AllocationAsk
	limiter     *rate.Limiter
}

func (ae *askEvents) sendRequestExceedsQueueHeadroom(headroom *resources.Resource, queuePath string) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	message := fmt.Sprintf("Request '%s' does not fit in queue '%s' (requested %s, available %s)", ae.ask.allocationKey, queuePath, ae.ask.GetAllocatedResource(), headroom)
	event := events.CreateRequestEventRecord(ae.ask.allocationKey, ae.ask.applicationID, message, ae.ask.GetAllocatedResource())
	ae.eventSystem.AddEvent(event)
}

func (ae *askEvents) sendRequestFitsInQueue(queuePath string) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	message := fmt.Sprintf("Request '%s' has become schedulable in queue '%s'", ae.ask.allocationKey, queuePath)
	event := events.CreateRequestEventRecord(ae.ask.allocationKey, ae.ask.applicationID, message, ae.ask.GetAllocatedResource())
	ae.eventSystem.AddEvent(event)
}

func (ae *askEvents) sendRequestExceedsUserQuota(headroom *resources.Resource) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	message := fmt.Sprintf("Request '%s' exceeds the available user quota (requested %s, available %s)", ae.ask.allocationKey, ae.ask.GetAllocatedResource(), headroom)
	event := events.CreateRequestEventRecord(ae.ask.allocationKey, ae.ask.applicationID, message, ae.ask.GetAllocatedResource())
	ae.eventSystem.AddEvent(event)
}

func (ae *askEvents) sendRequestFitsInUserQuota() {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	message := fmt.Sprintf("Request '%s' fits in the available user quota", ae.ask.allocationKey)
	event := events.CreateRequestEventRecord(ae.ask.allocationKey, ae.ask.applicationID, message, ae.ask.GetAllocatedResource())
	ae.eventSystem.AddEvent(event)
}

func (ae *askEvents) sendPredicateFailed(predicateMsg string) {
	if !ae.eventSystem.IsEventTrackingEnabled() || !ae.limiter.Allow() {
		return
	}
	message := fmt.Sprintf("Predicate failed for request '%s' with message: '%s'", ae.ask.allocationKey, predicateMsg)
	event := events.CreateRequestEventRecord(ae.ask.allocationKey, ae.ask.applicationID, message, ae.ask.GetAllocatedResource())
	ae.eventSystem.AddEvent(event)
}

func newAskEvents(ask *AllocationAsk, evt events.EventSystem) *askEvents {
	return newAskEventsWithRate(ask, evt, 15*time.Second, 1)
}

func newAskEventsWithRate(ask *AllocationAsk, evt events.EventSystem, interval time.Duration, burst int) *askEvents {
	return &askEvents{
		eventSystem: evt,
		ask:         ask,
		limiter:     rate.NewLimiter(rate.Every(interval), burst),
	}
}
