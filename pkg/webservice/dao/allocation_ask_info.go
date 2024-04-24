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

package dao

type AllocationAskLogDAOInfo struct {
	Message        string `json:"message,omitempty"`
	LastOccurrence int64  `json:"lastOccurrence,omitempty"`
	Count          int32  `json:"count,omitempty"`
}

type AllocationAskDAOInfo struct {
	AllocationKey       string                     `json:"allocationKey"` // no omitempty, allocation key should not be empty
	AllocationTags      map[string]string          `json:"allocationTags,omitempty"`
	RequestTime         int64                      `json:"requestTime,omitempty"`
	ResourcePerAlloc    map[string]int64           `json:"resource,omitempty"`
	Priority            string                     `json:"priority,omitempty"`
	RequiredNodeID      string                     `json:"requiredNodeId,omitempty"`
	ApplicationID       string                     `json:"applicationId,omitempty"`
	Partition           string                     `json:"partition,omitempty"`
	Placeholder         bool                       `json:"placeholder,omitempty"`
	PlaceholderTimeout  int64                      `json:"placeholderTimeout,omitempty"`
	TaskGroupName       string                     `json:"taskGroupName,omitempty"`
	AllocationLog       []*AllocationAskLogDAOInfo `json:"allocationLog,omitempty"`
	TriggeredPreemption bool                       `json:"triggeredPreemption,omitempty"`
	Originator          bool                       `json:"originator,omitempty"`
	SchedulingAttempted bool                       `json:"schedulingAttempted,omitempty"`
	TriggeredScaleUp    bool                       `json:"triggeredScaleUp,omitempty"`
}
