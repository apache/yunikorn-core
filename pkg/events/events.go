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
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func createEventRecord(recordType si.EventRecord_Type, objectID, groupID, reason, message string) (*si.EventRecord, error) {
	return &si.EventRecord{
		Type:     recordType,
		ObjectID: objectID,
		GroupID:  groupID,
		Reason:   reason,
		Message:  message,
	}, nil
}

func CreateRequestEventRecord(objectID, groupID, reason, message string) (*si.EventRecord, error) {
	return createEventRecord(si.EventRecord_REQUEST, objectID, groupID, reason, message)
}
