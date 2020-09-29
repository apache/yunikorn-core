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
	"strings"
	"testing"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func TestCreateEventRecord(t *testing.T) {
	record, err := createEventRecord(si.EventRecord_NODE, "ask", "app", "reason", "message")
	assert.NilError(t, err, "the error should be nil")
	assert.Equal(t, record.Type, si.EventRecord_NODE)
	assert.Equal(t, record.ObjectID, "ask")
	assert.Equal(t, record.GroupID, "app")
	assert.Equal(t, record.Reason, "reason")
	assert.Equal(t, record.Message, "message")
	if record.TimestampNano == 0 {
		t.Fatal("the timestamp should have been created")
	}
}

func TestCreateEventRecordTypes(t *testing.T) {
	record, err := CreateRequestEventRecord("ask", "app", "reason", "message")
	assert.NilError(t, err, "the error should be nil")
	assert.Equal(t, record.Type, si.EventRecord_REQUEST)

	record, err = CreateAppEventRecord("ask", "app", "message")
	assert.NilError(t, err, "the error should be nil")
	assert.Equal(t, record.Type, si.EventRecord_APP)

	record, err = CreateNodeEventRecord("ask", "app", "message")
	assert.NilError(t, err, "the error should be nil")
	assert.Equal(t, record.Type, si.EventRecord_NODE)

	record, err = CreateQueueEventRecord("ask", "app", "reason", "message")
	assert.NilError(t, err, "the error should be nil")
	assert.Equal(t, record.Type, si.EventRecord_QUEUE)
}

func TestEmptyFields(t *testing.T) {
	record, err := createEventRecord(si.EventRecord_QUEUE, "obj", "group", "reason", "message")
	assert.NilError(t, err)
	assert.Assert(t, record != nil, "the EventRecord should be created with a non-empty objectID")

	_, err = createEventRecord(si.EventRecord_QUEUE, "", "group", "reason", "message")
	assert.Assert(t, err != nil, "the EventRecord should not be created with empty objectID")

	_, err = createEventRecord(si.EventRecord_QUEUE, "obj", "group", "", "message")
	assert.Assert(t, err != nil, "the EventRecord should not be created with empty reason")
}

func TestEmitReserveEventWithoutEventCache(t *testing.T) {
	cache := GetEventCache()
	assert.Assert(t, cache == nil, "cache should not be initialized")

	allocKey := "test-alloc"
	appID := "app-1234"
	nodeID := "node-6"
	err := EmitReserveEvent(allocKey, appID, nodeID)
	assert.NilError(t, err, "emitting event without event cache should succeed")
}

func TestEmitReserveEvent(t *testing.T) {
	CreateAndSetEventCache()
	defer ResetCache()
	cache := GetEventCache()
	cache.StartService()

	allocKey := "test-alloc"
	appID := "app-1234"
	nodeID := "node-6"
	err := EmitReserveEvent(allocKey, appID, nodeID)
	assert.NilError(t, err, "expected EmitReserveEvent to run without errors")

	assertEmitRequestAppAndNodeEvents(t, allocKey, appID, nodeID, "AppReservedNode")
}

func TestEmitUnReserveEventWithoutEventCache(t *testing.T) {
	evCache := GetEventCache()
	assert.Assert(t, evCache == nil, "cache should not be initialized")

	allocKey := "test-alloc"
	appID := "app-1234"
	nodeID := "node-6"
	err := EmitUnReserveEvent(allocKey, appID, nodeID)
	assert.NilError(t, err, "emitting event without event cache should succeed")
}

func TestEmitAllocatedReservedEvent(t *testing.T) {
	CreateAndSetEventCache()
	defer ResetCache()
	cache := GetEventCache()
	cache.StartService()

	allocKey := "test-alloc"
	appID := "app-1234"
	nodeID := "node-6"
	err := EmitAllocatedReservedEvent(allocKey, appID, nodeID)
	assert.NilError(t, err, "expected EmitReserveEvent to run without errors")

	assertEmitRequestAppAndNodeEvents(t, allocKey, appID, nodeID, "AppAllocatedReservedNode")
}

func TestEmitAllocatedReservedEventWithoutEventCache(t *testing.T) {
	evCache := GetEventCache()
	assert.Assert(t, evCache == nil, "cache should not be initialized")

	allocKey := "test-alloc"
	appID := "app-1234"
	nodeID := "node-6"
	err := EmitAllocatedReservedEvent(allocKey, appID, nodeID)
	assert.NilError(t, err, "emitting event without event cache should succeed")
}

func TestEmitUnReserveEvent(t *testing.T) {
	CreateAndSetEventCache()
	defer ResetCache()
	cache := GetEventCache()
	cache.StartService()

	allocKey := "test-alloc"
	appID := "app-1234"
	nodeID := "node-6"
	err := EmitUnReserveEvent(allocKey, appID, nodeID)
	assert.NilError(t, err, "expected EmitReserveEvent to run without errors")

	assertEmitRequestAppAndNodeEvents(t, allocKey, appID, nodeID, "AppUnreservedNode")
}

func assertEmitRequestAppAndNodeEvents(t *testing.T, allocKey, appID, nodeID, reason string) {
	cache := GetEventCache()

	// wait for events to be processed
	err := common.WaitFor(1*time.Millisecond, 100*time.Millisecond, func() bool {
		return cache.Store.CountStoredEvents() == 3
	})
	assert.NilError(t, err, "the event should have been processed")

	records := cache.Store.CollectEvents()
	assert.Equal(t, len(records), 3)
	var requestFound, appFound, nodeFound bool
	for _, record := range records {
		assert.Equal(t, record.Reason, reason)
		msg := record.Message
		assert.Assert(t, strings.Contains(msg, allocKey), "allocation key not found in event message")
		assert.Assert(t, strings.Contains(msg, appID), "app ID not found in event message")
		assert.Assert(t, strings.Contains(msg, nodeID), "node ID not found in event message")
		switch {
		case record.ObjectID == allocKey:
			requestFound = true
			assert.Equal(t, record.Type, si.EventRecord_REQUEST)
			assert.Equal(t, record.GroupID, appID)
		case record.ObjectID == appID:
			appFound = true
			assert.Equal(t, record.Type, si.EventRecord_APP)
		case record.ObjectID == nodeID:
			nodeFound = true
			assert.Equal(t, record.Type, si.EventRecord_NODE)
		default:
			t.Fatalf("unexpected event found")
		}
	}
	assert.Assert(t, requestFound, "request event not found")
	assert.Assert(t, appFound, "app event not found")
	assert.Assert(t, nodeFound, "node not found")
}
