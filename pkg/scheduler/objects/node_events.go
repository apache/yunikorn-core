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

type nodeEvents struct {
	eventSystem events.EventSystem
}

func (n *nodeEvents) sendNodeAddedEvent(nodeID string, capacity *resources.Resource) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, "Node added to the scheduler", common.Empty, si.EventRecord_ADD,
		si.EventRecord_DETAILS_NONE, capacity)
	n.eventSystem.AddEvent(event)
}

func (n *nodeEvents) sendNodeRemovedEvent(nodeID string) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, "Node removed from the scheduler", common.Empty, si.EventRecord_REMOVE,
		si.EventRecord_NODE_DECOMISSION, nil)
	n.eventSystem.AddEvent(event)
}

func (n *nodeEvents) sendAllocationAddedEvent(nodeID, allocKey string, res *resources.Resource) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, common.Empty, allocKey, si.EventRecord_ADD,
		si.EventRecord_NODE_ALLOC, res)
	n.eventSystem.AddEvent(event)
}

func (n *nodeEvents) sendAllocationRemovedEvent(nodeID, allocKey string, res *resources.Resource) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, common.Empty, allocKey, si.EventRecord_REMOVE,
		si.EventRecord_NODE_ALLOC, res)
	n.eventSystem.AddEvent(event)
}

func (n *nodeEvents) sendNodeSchedulableChangedEvent(nodeID string, ready bool) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	var reason string
	if ready {
		reason = "schedulable: true"
	} else {
		reason = "schedulable: false"
	}
	event := events.CreateNodeEventRecord(nodeID, reason, common.Empty, si.EventRecord_SET,
		si.EventRecord_NODE_SCHEDULABLE, nil)
	n.eventSystem.AddEvent(event)
}

func (n *nodeEvents) sendNodeCapacityChangedEvent(nodeID string, total *resources.Resource) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, common.Empty, common.Empty, si.EventRecord_SET,
		si.EventRecord_NODE_CAPACITY, total)
	n.eventSystem.AddEvent(event)
}

func (n *nodeEvents) sendNodeOccupiedResourceChangedEvent(nodeID string, occupied *resources.Resource) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, common.Empty, common.Empty, si.EventRecord_SET,
		si.EventRecord_NODE_OCCUPIED, occupied)
	n.eventSystem.AddEvent(event)
}

func (n *nodeEvents) sendReservedEvent(nodeID string, res *resources.Resource, askID string) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, common.Empty, askID, si.EventRecord_ADD,
		si.EventRecord_NODE_RESERVATION, res)
	n.eventSystem.AddEvent(event)
}

func (n *nodeEvents) sendUnreservedEvent(nodeID string, res *resources.Resource, askID string) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, common.Empty, askID, si.EventRecord_REMOVE,
		si.EventRecord_NODE_RESERVATION, res)
	n.eventSystem.AddEvent(event)
}

func newNodeEvents(evt events.EventSystem) *nodeEvents {
	return &nodeEvents{
		eventSystem: evt,
	}
}
