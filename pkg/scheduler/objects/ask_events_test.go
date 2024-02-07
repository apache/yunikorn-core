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
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

var requestResource = resources.NewResourceFromMap(map[string]resources.Quantity{
	"memory": 100,
	"cpu":    100,
})

func TestRequestDoesNotFitInQueueEvent(t *testing.T) {
	ask := &AllocationAsk{
		allocationKey:     "alloc-0",
		applicationID:     "app-0",
		allocatedResource: requestResource,
	}
	eventSystem := newEventSystemMockDisabled()
	events := newAskEvents(ask, eventSystem)
	events.sendRequestExceedsQueueHeadroom(getTestResource(), "root.test")
	assert.Equal(t, 0, len(eventSystem.events))

	eventSystem = newEventSystemMock()
	events = newAskEvents(ask, eventSystem)
	events.sendRequestExceedsQueueHeadroom(getTestResource(), "root.test")
	assert.Equal(t, 1, len(eventSystem.events))
	event := eventSystem.events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, "app-0", event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Request 'alloc-0' does not fit in queue 'root.test' (requested map[cpu:100 memory:100], available map[cpu:1])", event.Message)
}

func TestRequestFitsInQueueEvent(t *testing.T) {
	ask := &AllocationAsk{
		allocationKey:     "alloc-0",
		applicationID:     "app-0",
		allocatedResource: requestResource,
	}
	eventSystem := newEventSystemMockDisabled()
	events := newAskEvents(ask, eventSystem)
	events.sendRequestFitsInQueue("root.test")
	assert.Equal(t, 0, len(eventSystem.events))

	eventSystem = newEventSystemMock()
	events = newAskEvents(ask, eventSystem)
	events.sendRequestFitsInQueue("root.test")
	assert.Equal(t, 1, len(eventSystem.events))
	event := eventSystem.events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, "app-0", event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Request 'alloc-0' has become schedulable in queue 'root.test'", event.Message)
}

func TestRequestExceedsUserQuotaEvent(t *testing.T) {
	ask := &AllocationAsk{
		allocationKey:     "alloc-0",
		applicationID:     "app-0",
		allocatedResource: requestResource,
	}
	eventSystem := newEventSystemMockDisabled()
	events := newAskEvents(ask, eventSystem)
	events.sendRequestExceedsUserQuota(getTestResource())
	assert.Equal(t, 0, len(eventSystem.events))

	eventSystem = newEventSystemMock()
	events = newAskEvents(ask, eventSystem)
	events.sendRequestExceedsUserQuota(getTestResource())
	assert.Equal(t, 1, len(eventSystem.events))
	event := eventSystem.events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, "app-0", event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Request 'alloc-0' exceeds the available user quota (requested map[cpu:100 memory:100], available map[cpu:1])", event.Message)
}

func TestRequestFitsInUserQuotaEvent(t *testing.T) {
	ask := &AllocationAsk{
		allocationKey:     "alloc-0",
		applicationID:     "app-0",
		allocatedResource: requestResource,
	}
	eventSystem := newEventSystemMockDisabled()
	events := newAskEvents(ask, eventSystem)
	events.sendRequestFitsInUserQuota()
	assert.Equal(t, 0, len(eventSystem.events))

	eventSystem = newEventSystemMock()
	events = newAskEvents(ask, eventSystem)
	events.sendRequestFitsInUserQuota()
	assert.Equal(t, 1, len(eventSystem.events))
	event := eventSystem.events[0]
	assert.Equal(t, "alloc-0", event.ObjectID)
	assert.Equal(t, "app-0", event.ReferenceID)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, "Request 'alloc-0' fits in the available user quota", event.Message)
}

func TestPredicateFailedEvents(t *testing.T) {
	ask := &AllocationAsk{
		allocationKey:     "alloc-0",
		applicationID:     "app-0",
		allocatedResource: requestResource,
	}
	eventSystem := newEventSystemMockDisabled()
	events := newAskEvents(ask, eventSystem)
	events.sendPredicateFailed("failed")
	assert.Equal(t, 0, len(eventSystem.events))

	eventSystem = newEventSystemMock()
	events = newAskEventsWithRate(ask, eventSystem, 50*time.Millisecond, 1)
	// only the first event is expected to be emitted due to rate limiting
	for i := 0; i < 200; i++ {
		events.sendPredicateFailed("failure-" + strconv.FormatUint(uint64(i), 10))
	}
	assert.Equal(t, 1, len(eventSystem.events))
	event := eventSystem.events[0]
	assert.Equal(t, "Predicate failed for request 'alloc-0' with message: 'failure-0'", event.Message)

	eventSystem.Reset()
	// wait a bit, a new event is expected
	time.Sleep(100 * time.Millisecond)
	events.sendPredicateFailed("failed")
	assert.Equal(t, 1, len(eventSystem.events))
	event = eventSystem.events[0]
	assert.Equal(t, "Predicate failed for request 'alloc-0' with message: 'failed'", event.Message)
}
