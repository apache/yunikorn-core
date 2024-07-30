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
	"time"

	"golang.org/x/time/rate"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events"
)

// AskEvents Request-specific events. These events are of REQUEST type, so they are eventually sent to the respective pods in K8s.
type AskEvents struct {
	eventSystem events.EventSystem
	limiter     *rate.Limiter
}

func (ae *AskEvents) SendRequestExceedsQueueHeadroom(allocKey, appID string, headroom, allocatedResource *resources.Resource, queuePath string) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	message := fmt.Sprintf("Request '%s' does not fit in queue '%s' (requested %s, available %s)", allocKey, queuePath, allocatedResource, headroom)
	event := events.CreateRequestEventRecord(allocKey, appID, message, allocatedResource)
	ae.eventSystem.AddEvent(event)
}

func (ae *AskEvents) SendRequestFitsInQueue(allocKey, appID, queuePath string, allocatedResource *resources.Resource) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	message := fmt.Sprintf("Request '%s' has become schedulable in queue '%s'", allocKey, queuePath)
	event := events.CreateRequestEventRecord(allocKey, appID, message, allocatedResource)
	ae.eventSystem.AddEvent(event)
}

func (ae *AskEvents) SendRequestExceedsUserQuota(allocKey, appID string, headroom, allocatedResource *resources.Resource) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	message := fmt.Sprintf("Request '%s' exceeds the available user quota (requested %s, available %s)", allocKey, allocatedResource, headroom)
	event := events.CreateRequestEventRecord(allocKey, appID, message, allocatedResource)
	ae.eventSystem.AddEvent(event)
}

func (ae *AskEvents) SendRequestFitsInUserQuota(allocKey, appID string, allocatedResource *resources.Resource) {
	if !ae.eventSystem.IsEventTrackingEnabled() {
		return
	}
	message := fmt.Sprintf("Request '%s' fits in the available user quota", allocKey)
	event := events.CreateRequestEventRecord(allocKey, appID, message, allocatedResource)
	ae.eventSystem.AddEvent(event)
}

func (ae *AskEvents) SendPredicateFailed(allocKey, appID, predicateMsg string, allocatedResource *resources.Resource) {
	if !ae.eventSystem.IsEventTrackingEnabled() || !ae.limiter.Allow() {
		return
	}
	message := fmt.Sprintf("Predicate failed for request '%s' with message: '%s'", allocKey, predicateMsg)
	event := events.CreateRequestEventRecord(allocKey, appID, message, allocatedResource)
	ae.eventSystem.AddEvent(event)
}

func NewAskEvents(evt events.EventSystem) *AskEvents {
	return newAskEventsWithRate(evt, 15*time.Second, 1)
}

func newAskEventsWithRate(evt events.EventSystem, interval time.Duration, burst int) *AskEvents {
	return &AskEvents{
		eventSystem: evt,
		limiter:     rate.NewLimiter(rate.Every(interval), burst),
	}
}
