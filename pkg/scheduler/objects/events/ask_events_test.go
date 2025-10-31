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
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

var requestResource = resources.NewResourceFromMap(map[string]resources.Quantity{
	"memory": 100,
	"cpu":    100,
})

func TestRequestDoesNotFitInQueueEvent(t *testing.T) {
	headroom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	events := NewAskEvents(eventSystem)
	events.SendRequestExceedsQueueHeadroom(allocKey, appID, headroom, requestResource, "root.test")
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = NewAskEvents(eventSystem)
	events.SendRequestExceedsQueueHeadroom(allocKey, appID, headroom, requestResource, "root.test")
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, appID, event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Request 'alloc-0' does not fit in queue 'root.test' (requested map[cpu:100 memory:100], available map[first:1])", event.Message)
}

func TestRequestFitsInQueueEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := NewAskEvents(eventSystem)
	events.SendRequestFitsInQueue(allocKey, appID, "root.test", requestResource)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = NewAskEvents(eventSystem)
	events.SendRequestFitsInQueue(allocKey, appID, "root.test", requestResource)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, appID, event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Request 'alloc-0' has become schedulable in queue 'root.test'", event.Message)
}

func TestRequestExceedsUserQuotaEvent(t *testing.T) {
	headroom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	events := NewAskEvents(eventSystem)
	events.SendRequestExceedsUserQuota(allocKey, appID, headroom, requestResource)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = NewAskEvents(eventSystem)
	events.SendRequestExceedsUserQuota(allocKey, appID, headroom, requestResource)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, appID, event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Request 'alloc-0' exceeds the available user quota (requested map[cpu:100 memory:100], available map[first:1])", event.Message)
}

func TestRequestFitsInUserQuotaEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := NewAskEvents(eventSystem)
	events.SendRequestFitsInUserQuota(allocKey, appID, requestResource)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = NewAskEvents(eventSystem)
	events.SendRequestFitsInUserQuota(allocKey, appID, requestResource)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, appID, event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Request 'alloc-0' fits in the available user quota", event.Message)
}

func TestPredicateFailedEvents(t *testing.T) {
	resource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	events := NewAskEvents(eventSystem)
	events.SendPredicatesFailed("alloc-0", "app-0", map[string]int{}, resource)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newAskEventsWithRate(eventSystem, 50*time.Millisecond, 1)
	errors := map[string]int{
		"error#0": 2,
		"error#1": 123,
		"error#2": 44,
	}
	// only the first event is expected to be emitted due to rate limiting
	for i := 0; i < 200; i++ {
		events.SendPredicatesFailed("alloc-0", "app-0", errors, resource)
	}
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "Unschedulable request 'alloc-0': error#0 (2x); error#1 (123x); error#2 (44x); ", event.Message)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, "app-0", event.ReferenceID)

	eventSystem.Reset()
	// wait a bit, a new event is expected
	time.Sleep(100 * time.Millisecond)
	events.SendPredicatesFailed("alloc-1", "app-0", errors, resource)
	assert.Equal(t, 1, len(eventSystem.Events))
	event = eventSystem.Events[0]
	assert.Equal(t, "Unschedulable request 'alloc-1': error#0 (2x); error#1 (123x); error#2 (44x); ", event.Message)
}

func TestRequiredNodePreemptionFailedEvents(t *testing.T) {
	resource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	events := NewAskEvents(eventSystem)
	events.SendRequiredNodePreemptionFailed("alloc-0", "app-0", nodeID1, resource)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newAskEventsWithRate(eventSystem, 50*time.Millisecond, 1)
	// only the first event is expected to be emitted due to rate limiting
	for i := 0; i < 200; i++ {
		events.SendRequiredNodePreemptionFailed("alloc-0", "app-0", nodeID1, resource)
	}
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "Unschedulable request 'alloc-0' with required node 'node-1', no preemption victim found", event.Message)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, "app-0", event.ReferenceID)
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, resource, protoRes)

	eventSystem.Reset()
	// wait a bit, a new event is expected
	time.Sleep(100 * time.Millisecond)
	events.SendRequiredNodePreemptionFailed("alloc-1", "app-0", nodeID1, resource)
	assert.Equal(t, 1, len(eventSystem.Events))
	event = eventSystem.Events[0]
	assert.Equal(t, "Unschedulable request 'alloc-1' with required node 'node-1', no preemption victim found", event.Message)
}

func TestPreemptedBySchedulerEvents(t *testing.T) {
	resource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	events := NewAskEvents(eventSystem)
	events.SendPreemptedByScheduler("alloc-0", appID, "preemptor-0", "preemptor-app-0", "root.parent.child1", resource)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = NewAskEvents(eventSystem)
	events.SendPreemptedByScheduler("alloc-0", appID, "preemptor-0", "preemptor-app-0", "root.parent.child1", resource)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, appID, event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Preempted by preemptor-0 from application preemptor-app-0 in root.parent.child1", event.Message)
}

func TestSendPreemptedByQuotaChange(t *testing.T) {
	resource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	events := NewAskEvents(eventSystem)
	events.SendPreemptedByQuotaChange("alloc-0", appID, "root.parent.child1", resource)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = NewAskEvents(eventSystem)
	events.SendPreemptedByQuotaChange("alloc-0", appID, "root.parent.child1", resource)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, appID, event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Preempted by Quota change enforcement process in root.parent.child1", event.Message)
}
