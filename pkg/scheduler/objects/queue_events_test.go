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
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	testQueuePath = "root.test"
)

func TestSendNewQueueEvent(t *testing.T) {
	queue := &Queue{
		QueuePath: testQueuePath,
		isManaged: true,
	}
	eventSystem := mock.NewEventSystemDisabled()
	nq := newQueueEvents(queue, eventSystem)
	nq.sendNewQueueEvent()
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	nq = newQueueEvents(queue, eventSystem)
	nq.sendNewQueueEvent()
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_QUEUE, event.Type)
	assert.Equal(t, testQueuePath, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))
	eventSystem = mock.NewEventSystem()
	nq = newQueueEvents(&Queue{
		QueuePath: testQueuePath,
		isManaged: false,
	}, eventSystem)
	nq.sendNewQueueEvent()
	event = eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_QUEUE_DYNAMIC, event.EventChangeDetail)
}

func TestSendRemoveQueueEvent(t *testing.T) {
	queue := &Queue{
		QueuePath: testQueuePath,
		isManaged: true,
	}
	eventSystem := mock.NewEventSystemDisabled()
	nq := newQueueEvents(queue, eventSystem)
	nq.sendRemoveQueueEvent()
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	nq = newQueueEvents(queue, eventSystem)
	nq.sendRemoveQueueEvent()
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_QUEUE, event.Type)
	assert.Equal(t, testQueuePath, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))
	eventSystem = mock.NewEventSystem()
	nq = newQueueEvents(&Queue{
		QueuePath: testQueuePath,
		isManaged: false,
	}, eventSystem)
	nq.sendRemoveQueueEvent()
	event = eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_QUEUE_DYNAMIC, event.EventChangeDetail)
}

func TestNewApplicationEvent(t *testing.T) {
	queue := &Queue{
		QueuePath: testQueuePath,
	}
	eventSystem := mock.NewEventSystemDisabled()
	nq := newQueueEvents(queue, eventSystem)
	nq.sendNewApplicationEvent(appID0)
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	nq = newQueueEvents(queue, eventSystem)
	nq.sendNewApplicationEvent(appID0)
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_QUEUE, event.Type)
	assert.Equal(t, testQueuePath, event.ObjectID)
	assert.Equal(t, appID0, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_APP, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestRemoveApplicationEvent(t *testing.T) {
	queue := &Queue{
		QueuePath: testQueuePath,
	}
	eventSystem := mock.NewEventSystemDisabled()
	nq := newQueueEvents(queue, eventSystem)
	nq.sendRemoveApplicationEvent(appID0)
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	nq = newQueueEvents(queue, eventSystem)
	nq.sendRemoveApplicationEvent(appID0)
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_QUEUE, event.Type)
	assert.Equal(t, testQueuePath, event.ObjectID)
	assert.Equal(t, appID0, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_APP, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestTypeChangedEvent(t *testing.T) {
	queue := &Queue{
		QueuePath: testQueuePath,
	}
	eventSystem := mock.NewEventSystemDisabled()
	nq := newQueueEvents(queue, eventSystem)
	nq.sendTypeChangedEvent()
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	nq = newQueueEvents(queue, eventSystem)
	nq.sendTypeChangedEvent()
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_QUEUE, event.Type)
	assert.Equal(t, testQueuePath, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, "leaf queue: false", event.Message)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_TYPE, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestSendMaxResourceChangedEvent(t *testing.T) {
	maxRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	queue := &Queue{
		QueuePath:   testQueuePath,
		maxResource: maxRes,
	}
	eventSystem := mock.NewEventSystemDisabled()
	nq := newQueueEvents(queue, eventSystem)
	nq.sendMaxResourceChangedEvent()
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	nq = newQueueEvents(queue, eventSystem)
	nq.sendMaxResourceChangedEvent()
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_QUEUE, event.Type)
	assert.Equal(t, testQueuePath, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_MAX, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, maxRes, protoRes)
}

func TestSendGuaranteedResourceChangedEvent(t *testing.T) {
	guaranteed := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	queue := &Queue{
		QueuePath:          testQueuePath,
		guaranteedResource: guaranteed,
	}
	eventSystem := mock.NewEventSystemDisabled()
	nq := newQueueEvents(queue, eventSystem)
	nq.sendGuaranteedResourceChangedEvent()
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	nq = newQueueEvents(queue, eventSystem)
	nq.sendGuaranteedResourceChangedEvent()
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, si.EventRecord_QUEUE, event.Type)
	assert.Equal(t, testQueuePath, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_GUARANTEED, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, guaranteed, protoRes)
}
