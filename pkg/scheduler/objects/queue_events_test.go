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
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	testQueuePath = "root.test"
)

func TestNewQueueEvents(t *testing.T) {
	queue := &Queue{
		QueuePath: testQueuePath,
	}

	// not enabled
	nq := newQueueEvents(queue, nil)
	assert.Assert(t, nq.eventSystem == nil, "event system should be nil")
	assert.Assert(t, !nq.enabled, "event system should be disabled")

	// enabled
	nq = newQueueEvents(queue, newEventSystemMock())
	assert.Assert(t, nq.eventSystem != nil, "event system should not be nil")
	assert.Assert(t, nq.enabled, "event system should be enabled")
}

func TestSendNewQueueEvent(t *testing.T) {
	queue := &Queue{
		QueuePath: testQueuePath,
		isManaged: true,
	}

	// not enabled
	nq := newQueueEvents(queue, nil)
	nq.sendNewQueueEvent()

	// enabled
	mock := newEventSystemMock()
	nq = newQueueEvents(queue, mock)
	nq.sendNewQueueEvent()
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
	assert.Equal(t, si.EventRecord_QUEUE, event.Type)
	assert.Equal(t, testQueuePath, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))
	mock = newEventSystemMock()
	nq = newQueueEvents(&Queue{
		QueuePath: testQueuePath,
		isManaged: false,
	}, mock)
	nq.sendNewQueueEvent()
	event = mock.events[0]
	assert.Equal(t, si.EventRecord_QUEUE_DYNAMIC, event.EventChangeDetail)
}

func TestSendRemoveQueueEvent(t *testing.T) {
	queue := &Queue{
		QueuePath: testQueuePath,
		isManaged: true,
	}

	// not enabled
	nq := newQueueEvents(queue, nil)
	nq.sendRemoveQueueEvent()

	// enabled
	mock := newEventSystemMock()
	nq = newQueueEvents(queue, mock)
	nq.sendRemoveQueueEvent()
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
	assert.Equal(t, si.EventRecord_QUEUE, event.Type)
	assert.Equal(t, testQueuePath, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))
	mock = newEventSystemMock()
	nq = newQueueEvents(&Queue{
		QueuePath: testQueuePath,
		isManaged: false,
	}, mock)
	nq.sendRemoveQueueEvent()
	event = mock.events[0]
	assert.Equal(t, si.EventRecord_QUEUE_DYNAMIC, event.EventChangeDetail)
}

func TestNewApplicationEvent(t *testing.T) {
	queue := &Queue{
		QueuePath: testQueuePath,
	}

	// not enabled
	nq := newQueueEvents(queue, nil)
	nq.sendNewApplicationEvent(appID0)

	// enabled
	mock := newEventSystemMock()
	nq = newQueueEvents(queue, mock)
	nq.sendNewApplicationEvent(appID0)
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
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

	// not enabled
	nq := newQueueEvents(queue, nil)
	nq.sendRemoveApplicationEvent(appID0)

	// enabled
	mock := newEventSystemMock()
	nq = newQueueEvents(queue, mock)
	nq.sendRemoveApplicationEvent(appID0)
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
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

	// not enabled
	nq := newQueueEvents(queue, nil)
	nq.sendTypeChangedEvent()

	// enabled
	mock := newEventSystemMock()
	nq = newQueueEvents(queue, mock)
	nq.sendTypeChangedEvent()
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
	assert.Equal(t, si.EventRecord_QUEUE, event.Type)
	assert.Equal(t, testQueuePath, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, "leaf queue: false", event.Message)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_TYPE, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestSendMaxResourceChangedEvent(t *testing.T) {
	max := getTestResource()
	queue := &Queue{
		QueuePath:   testQueuePath,
		maxResource: max,
	}

	// not enabled
	nq := newQueueEvents(queue, nil)
	nq.sendMaxResourceChangedEvent()

	// enabled
	mock := newEventSystemMock()
	nq = newQueueEvents(queue, mock)
	nq.sendMaxResourceChangedEvent()
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
	assert.Equal(t, si.EventRecord_QUEUE, event.Type)
	assert.Equal(t, testQueuePath, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_MAX, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, max, protoRes)
}

func TestSendGuaranteedResourceChangedEvent(t *testing.T) {
	guaranteed := getTestResource()
	queue := &Queue{
		QueuePath:          testQueuePath,
		guaranteedResource: guaranteed,
	}

	// not enabled
	nq := newQueueEvents(queue, nil)
	nq.sendGuaranteedResourceChangedEvent()

	// enabled
	mock := newEventSystemMock()
	nq = newQueueEvents(queue, mock)
	nq.sendGuaranteedResourceChangedEvent()
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
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
