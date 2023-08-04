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

func TestSendNodeAddedEvent(t *testing.T) {
	node := &Node{
		NodeID: nodeID1,
	}
	mock := newEventSystemMockDisabled()
	ne := newNodeEvents(node, mock)
	ne.sendNodeAddedEvent()
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	ne = newNodeEvents(node, mock)
	ne.sendNodeAddedEvent()
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestSendNodeRemovedEvent(t *testing.T) {
	node := &Node{
		NodeID: nodeID1,
	}
	mock := newEventSystemMockDisabled()
	ne := newNodeEvents(node, mock)
	ne.sendNodeRemovedEvent()
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	ne = newNodeEvents(node, mock)
	ne.sendNodeRemovedEvent()
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_DECOMISSION, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestSendAllocationAddedEvent(t *testing.T) {
	node := &Node{
		NodeID: nodeID1,
	}
	resource := getTestResource()

	mock := newEventSystemMockDisabled()
	ne := newNodeEvents(node, mock)
	ne.sendAllocationAddedEvent("alloc-0", resource)
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	ne = newNodeEvents(node, mock)
	ne.sendAllocationAddedEvent("alloc-0", resource)
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, "alloc-0", event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_ALLOC, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, protoRes, resource)
}

func TestSendAllocationRemovedEvent(t *testing.T) {
	node := &Node{
		NodeID: nodeID1,
	}
	resource := getTestResource()

	mock := newEventSystemMockDisabled()
	ne := newNodeEvents(node, mock)
	ne.sendAllocationRemovedEvent("alloc-0", resource)
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	ne = newNodeEvents(node, mock)
	ne.sendAllocationRemovedEvent("alloc-0", resource)
	event := mock.events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, "alloc-0", event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_ALLOC, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, protoRes, resource)
}

func TestSendNodeReadyChangedEvent(t *testing.T) {
	node := &Node{
		NodeID: nodeID1,
	}
	mock := newEventSystemMockDisabled()
	ne := newNodeEvents(node, mock)
	ne.sendNodeReadyChangedEvent(true)
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	ne = newNodeEvents(node, mock)
	ne.sendNodeReadyChangedEvent(true)
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	assert.Equal(t, "ready: true", mock.events[0].Message)
	assert.Equal(t, nodeID1, mock.events[0].ObjectID)

	mock.Reset()
	ne.sendNodeReadyChangedEvent(false)
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	assert.Equal(t, "ready: false", mock.events[0].Message)
	assert.Equal(t, nodeID1, mock.events[0].ObjectID)
}

func TestSendOccupiedResourceChangedEvent(t *testing.T) {
	resource := getTestResource()
	node := &Node{
		NodeID:           nodeID1,
		occupiedResource: resource,
	}
	mock := newEventSystemMockDisabled()
	ne := newNodeEvents(node, mock)
	ne.sendNodeOccupiedResourceChangedEvent()
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	ne = newNodeEvents(node, mock)
	ne.sendNodeOccupiedResourceChangedEvent()
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_OCCUPIED, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, protoRes, resource)
}

func TestSendCapacityChangedEvent(t *testing.T) {
	resource := getTestResource()
	node := &Node{
		NodeID:        nodeID1,
		totalResource: resource,
	}
	mock := newEventSystemMockDisabled()
	ne := newNodeEvents(node, mock)
	ne.sendNodeCapacityChangedEvent()
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	ne = newNodeEvents(node, mock)
	ne.sendNodeCapacityChangedEvent()
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_CAPACITY, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, protoRes, resource)
}

func TestNodeSchedulableChangedEvent(t *testing.T) {
	node := &Node{
		NodeID:      nodeID1,
		schedulable: true,
	}
	mock := newEventSystemMockDisabled()
	ne := newNodeEvents(node, mock)
	ne.sendNodeSchedulableChangedEvent(false)
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	ne = newNodeEvents(node, mock)
	ne.sendNodeSchedulableChangedEvent(false)
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, "schedulable: false", event.Message)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_SCHEDULABLE, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))

	mock.Reset()
	ne.sendNodeSchedulableChangedEvent(true)
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event = mock.events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, "schedulable: true", event.Message)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_SCHEDULABLE, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestNodeReservationEvent(t *testing.T) {
	resource := getTestResource()
	node := &Node{
		NodeID:      nodeID1,
		schedulable: true,
	}
	mock := newEventSystemMockDisabled()
	ne := newNodeEvents(node, mock)
	ne.sendReservedEvent(resource, "alloc-0")
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	ne = newNodeEvents(node, mock)
	ne.sendReservedEvent(resource, "alloc-0")
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, "alloc-0", event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_RESERVATION, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, protoRes, resource)
}

func TestNodeUnreservationEvent(t *testing.T) {
	resource := getTestResource()
	node := &Node{
		NodeID:      nodeID1,
		schedulable: true,
	}
	mock := newEventSystemMockDisabled()
	ne := newNodeEvents(node, mock)
	ne.sendUnreservedEvent(resource, "alloc-0")
	assert.Equal(t, 0, len(mock.events), "unexpected event")

	mock = newEventSystemMock()
	ne = newNodeEvents(node, mock)
	ne.sendUnreservedEvent(resource, "alloc-0")
	assert.Equal(t, 1, len(mock.events), "event was not generated")
	event := mock.events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, "alloc-0", event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_RESERVATION, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, protoRes, resource)
}
