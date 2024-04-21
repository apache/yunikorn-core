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
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const allocKey = "alloc-0"

var requestResource = resources.NewResourceFromMap(map[string]resources.Quantity{
	"memory": 100,
	"cpu":    100,
})

func TestRequestDoesNotFitInQueueEvent(t *testing.T) {
	headroom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	events := newAskEvents(eventSystem)
	events.sendRequestExceedsQueueHeadroom(allocKey, appID1, headroom, requestResource, "root.test")
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newAskEvents(eventSystem)
	events.sendRequestExceedsQueueHeadroom(allocKey, appID1, headroom, requestResource, "root.test")
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, appID1, event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Request 'alloc-0' does not fit in queue 'root.test' (requested map[cpu:100 memory:100], available map[first:1])", event.Message)
}

func TestRequestFitsInQueueEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := newAskEvents(eventSystem)
	events.sendRequestFitsInQueue(allocKey, appID1, "root.test", requestResource)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newAskEvents(eventSystem)
	events.sendRequestFitsInQueue(allocKey, appID1, "root.test", requestResource)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, appID1, event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Request 'alloc-0' has become schedulable in queue 'root.test'", event.Message)
}

func TestRequestExceedsUserQuotaEvent(t *testing.T) {
	headroom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	events := newAskEvents(eventSystem)
	events.sendRequestExceedsUserQuota(allocKey, appID1, headroom, requestResource)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newAskEvents(eventSystem)
	events.sendRequestExceedsUserQuota(allocKey, appID1, headroom, requestResource)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, appID1, event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Request 'alloc-0' exceeds the available user quota (requested map[cpu:100 memory:100], available map[first:1])", event.Message)
}

func TestRequestFitsInUserQuotaEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := newAskEvents(eventSystem)
	events.sendRequestFitsInUserQuota(allocKey, appID1, requestResource)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newAskEvents(eventSystem)
	events.sendRequestFitsInUserQuota(allocKey, appID1, requestResource)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, appID1, event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Request 'alloc-0' fits in the available user quota", event.Message)
}

func TestPredicateFailedEvents(t *testing.T) {
	resource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	events := newAskEvents(eventSystem)
	events.sendPredicateFailed("alloc-0", "app-0", "failed", resource)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newAskEventsWithRate(eventSystem, 50*time.Millisecond, 1)
	// only the first event is expected to be emitted due to rate limiting
	for i := 0; i < 200; i++ {
		events.sendPredicateFailed("alloc-0", "app-0", "failure-"+strconv.FormatUint(uint64(i), 10), resource)
	}
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "Predicate failed for request 'alloc-0' with message: 'failure-0'", event.Message)

	eventSystem.Reset()
	// wait a bit, a new event is expected
	time.Sleep(100 * time.Millisecond)
	events.sendPredicateFailed("alloc-0", "app-0", "failed", resource)
	assert.Equal(t, 1, len(eventSystem.Events))
	event = eventSystem.Events[0]
	assert.Equal(t, "Predicate failed for request 'alloc-0' with message: 'failed'", event.Message)
}
