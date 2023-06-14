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

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestCreateEventRecord(t *testing.T) {
	record := createEventRecord(si.EventRecord_NODE, "ask", "app", "message",
		si.EventRecord_NONE, si.EventRecord_DETAILS_NONE, resources.NewResourceFromMap(
			map[string]resources.Quantity{
				"cpu": 1,
			},
		))
	assert.Equal(t, record.Type, si.EventRecord_NODE)
	assert.Equal(t, record.ObjectID, "ask")
	assert.Equal(t, record.ReferenceID, "app")
	assert.Equal(t, record.Message, "message")
	assert.Equal(t, int64(1), record.Resource.Resources["cpu"].Value)
	assert.Equal(t, si.EventRecord_NONE, record.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, record.EventChangeDetail)
	if record.TimestampNano == 0 {
		t.Fatal("the timestamp should have been created")
	}
}

func TestCreateEventRecordTypes(t *testing.T) {
	record := CreateRequestEventRecord("ask", "app", "message", nil)
	assert.Equal(t, record.Type, si.EventRecord_REQUEST)

	record = CreateAppEventRecord("app", "message", "ask", si.EventRecord_NONE, si.EventRecord_DETAILS_NONE, nil)
	assert.Equal(t, record.Type, si.EventRecord_APP)

	record = CreateNodeEventRecord("node", "message", "ask", si.EventRecord_NONE, si.EventRecord_DETAILS_NONE, nil)
	assert.Equal(t, record.Type, si.EventRecord_NODE)

	record = CreateQueueEventRecord("queue", "message", "app", si.EventRecord_NONE, si.EventRecord_DETAILS_NONE, nil)
	assert.Equal(t, record.Type, si.EventRecord_QUEUE)
}
