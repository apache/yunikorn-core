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

	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func createEventRecord(recordType si.EventRecord_Type, objectID, groupID, reason, message string) (*si.EventRecord, error) {
	if objectID == "" {
		return nil, fmt.Errorf("objectID should not be nil")
	}
	if reason == "" {
		return nil, fmt.Errorf("reason should not be nil")
	}

	return &si.EventRecord{
		Type:          recordType,
		ObjectID:      objectID,
		GroupID:       groupID,
		Reason:        reason,
		Message:       message,
		TimestampNano: time.Now().UnixNano(),
	}, nil
}

func CreateRequestEventRecord(objectID, groupID, reason, message string) (*si.EventRecord, error) {
	return createEventRecord(si.EventRecord_REQUEST, objectID, groupID, reason, message)
}

func CreateAppEventRecord(objectID, reason, message string) (*si.EventRecord, error) {
	return createEventRecord(si.EventRecord_APP, objectID, "", reason, message)
}

func CreateNodeEventRecord(objectID, reason, message string) (*si.EventRecord, error) {
	return createEventRecord(si.EventRecord_NODE, objectID, "", reason, message)
}

func CreateQueueEventRecord(objectID, groupID, reason, message string) (*si.EventRecord, error) {
	return createEventRecord(si.EventRecord_QUEUE, objectID, groupID, reason, message)
}

// emitting event to the shim for a reservation
// the event will be sent to the node, the application and the allocation involved in the reservation
func EmitReserveEvent(allocKey, appID, nodeID string) error {
	return emitRequestAppAndNodeEvents(allocKey, appID, nodeID,
		"Allocation %s from application %s reserved node %s", "AppReservedNode")
}

// emit an unreserve event for the allocation, the application and the node as well
func EmitUnReserveEvent(allocKey, appID, nodeID string) error {
	return emitRequestAppAndNodeEvents(allocKey, appID, nodeID,
		"Allocation %s from application %s unreserved from node %s", "AppUnreservedNode")
}

// emit an unreserve event only to the node
func EmitUnreserveEventForNode(allocKey, appID, nodeID string) error {
	eventCache := GetEventCache()
	if eventCache == nil {
		// The event cache is disabled.
		// It's not an error, let's return.
		return nil
	}
	msg := fmt.Sprintf("Ask %s from application %s allocated to node %s", allocKey, appID, nodeID)

	event, err := CreateNodeEventRecord(nodeID, "AppAllocatedReservedNode", msg)
	if err != nil {
		return err
	}
	eventCache.AddEvent(event)

	return nil
}

// emit an allocation event to the node through the shim
func EmitAllocatedReservedEvent(allocKey, appID, nodeID string) error {
	return emitRequestAppAndNodeEvents(allocKey, appID, nodeID,
		"Ask %s from application %s allocated to node %s", "AppAllocatedReservedNode")
}

func emitRequestAppAndNodeEvents(allocKey, appID, nodeID, msgFmt, reason string) error {
	eventCache := GetEventCache()
	if eventCache == nil {
		// The event cache is disabled.
		// It's not an error, let's return.
		return nil
	}
	msg := fmt.Sprintf(msgFmt, allocKey, appID, nodeID)

	event, err := CreateNodeEventRecord(nodeID, reason, msg)
	if err != nil {
		return err
	}
	eventCache.AddEvent(event)

	// push event to the app
	event, err = CreateAppEventRecord(appID, reason, msg)
	if err != nil {
		return err
	}
	eventCache.AddEvent(event)

	// push event to the request
	event, err = CreateRequestEventRecord(allocKey, appID, reason, msg)
	if err != nil {
		return err
	}
	eventCache.AddEvent(event)

	return nil
}
