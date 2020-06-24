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
	assert.Assert(t, record != nil, "the EventRecord should be created with a non-empty objectID")

	_, err = createEventRecord(si.EventRecord_QUEUE, "", "group", "reason", "message")
	assert.Assert(t, err != nil, "the EventRecord should not be created with empty objectID")

	_, err = createEventRecord(si.EventRecord_QUEUE, "obj", "group", "", "message")
	assert.Assert(t, err != nil, "the EventRecord should not be created with empty reason")
}
