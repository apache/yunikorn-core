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
	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type queueEvents struct {
	eventSystem events.EventSystem
}

func (q *queueEvents) sendNewQueueEvent(queuePath string, managed bool) {
	if !q.eventSystem.IsEventTrackingEnabled() {
		return
	}
	detail := si.EventRecord_QUEUE_DYNAMIC
	if managed {
		detail = si.EventRecord_DETAILS_NONE
	}
	event := events.CreateQueueEventRecord(queuePath, common.Empty, common.Empty, si.EventRecord_ADD,
		detail, nil)
	q.eventSystem.AddEvent(event)
}

func (q *queueEvents) sendNewApplicationEvent(queuePath, appID string) {
	if !q.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateQueueEventRecord(queuePath, common.Empty, appID, si.EventRecord_ADD,
		si.EventRecord_QUEUE_APP, nil)
	q.eventSystem.AddEvent(event)
}

func (q *queueEvents) sendRemoveQueueEvent(queuePath string, managed bool) {
	if !q.eventSystem.IsEventTrackingEnabled() {
		return
	}
	detail := si.EventRecord_QUEUE_DYNAMIC
	if managed {
		detail = si.EventRecord_DETAILS_NONE
	}
	event := events.CreateQueueEventRecord(queuePath, common.Empty, common.Empty, si.EventRecord_REMOVE,
		detail, nil)
	q.eventSystem.AddEvent(event)
}

func (q *queueEvents) sendRemoveApplicationEvent(queuePath, appID string) {
	if !q.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateQueueEventRecord(queuePath, common.Empty, appID, si.EventRecord_REMOVE,
		si.EventRecord_QUEUE_APP, nil)
	q.eventSystem.AddEvent(event)
}

func (q *queueEvents) sendMaxResourceChangedEvent(queuePath string, maxResource *resources.Resource) {
	if !q.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateQueueEventRecord(queuePath, common.Empty, common.Empty, si.EventRecord_SET,
		si.EventRecord_QUEUE_MAX, maxResource)
	q.eventSystem.AddEvent(event)
}

func (q *queueEvents) sendGuaranteedResourceChangedEvent(queuePath string, guaranteed *resources.Resource) {
	if !q.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateQueueEventRecord(queuePath, common.Empty, common.Empty, si.EventRecord_SET,
		si.EventRecord_QUEUE_GUARANTEED, guaranteed)
	q.eventSystem.AddEvent(event)
}

func (q *queueEvents) sendTypeChangedEvent(queuePath string, isLeaf bool) {
	if !q.eventSystem.IsEventTrackingEnabled() {
		return
	}
	message := "leaf queue: false"
	if isLeaf {
		message = "leaf queue: true"
	}
	event := events.CreateQueueEventRecord(queuePath, message, common.Empty, si.EventRecord_SET,
		si.EventRecord_QUEUE_TYPE, nil)
	q.eventSystem.AddEvent(event)
}

func newQueueEvents(evt events.EventSystem) *queueEvents {
	return &queueEvents{
		eventSystem: evt,
	}
}
