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
	"time"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const Empty = ""

func createEventRecord(recordType si.EventRecord_Type, objectID, referenceID, message string,
	changeType si.EventRecord_ChangeType, changeDetail si.EventRecord_ChangeDetail, resource *resources.Resource) *si.EventRecord {
	return &si.EventRecord{
		Type:              recordType,
		ObjectID:          objectID,
		ReferenceID:       referenceID,
		Message:           message,
		TimestampNano:     time.Now().UnixNano(),
		Resource:          resource.ToProto(),
		EventChangeDetail: changeDetail,
		EventChangeType:   changeType,
	}
}

func CreateRequestEventRecord(objectID, referenceID, message string, resource *resources.Resource) *si.EventRecord {
	return createEventRecord(si.EventRecord_REQUEST, objectID, referenceID, message, si.EventRecord_NONE, si.EventRecord_DETAILS_NONE, resource)
}

func CreateAppEventRecord(objectID, message, referenceID string, changeType si.EventRecord_ChangeType, changeDetail si.EventRecord_ChangeDetail, resource *resources.Resource) *si.EventRecord {
	return createEventRecord(si.EventRecord_APP, objectID, referenceID, message, changeType, changeDetail, resource)
}

func CreateNodeEventRecord(objectID, message, referenceID string, changeType si.EventRecord_ChangeType, changeDetail si.EventRecord_ChangeDetail, resource *resources.Resource) *si.EventRecord {
	return createEventRecord(si.EventRecord_NODE, objectID, referenceID, message, changeType, changeDetail, resource)
}

func CreateQueueEventRecord(objectID, referenceID, message string, changeType si.EventRecord_ChangeType, changeDetail si.EventRecord_ChangeDetail, resource *resources.Resource) *si.EventRecord {
	return createEventRecord(si.EventRecord_QUEUE, objectID, referenceID, message, changeType, changeDetail, resource)
}
