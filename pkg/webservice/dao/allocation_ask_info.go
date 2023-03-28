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
	Message        string `json:"message"`
	LastOccurrence int64  `json:"lastOccurrence"`
	Count          int32  `json:"count"`
}

type AllocationAskDAOInfo struct {
	AllocationKey       string                     `json:"allocationKey"`
	AllocationTags      map[string]string          `json:"allocationTags"`
	RequestTime         int64                      `json:"requestTime"`
	ResourcePerAlloc    map[string]int64           `json:"resource"`
	PendingCount        int32                      `json:"pendingCount"`
	Priority            string                     `json:"priority"`
	RequiredNodeID      string                     `json:"requiredNodeId"`
	ApplicationID       string                     `json:"applicationId"`
	Partition           string                     `json:"partition"`
	Placeholder         bool                       `json:"placeholder"`
	PlaceholderTimeout  int64                      `json:"placeholderTimeout"`
	TaskGroupName       string                     `json:"taskGroupName"`
	AllocationLog       []*AllocationAskLogDAOInfo `json:"allocationLog"`
	TriggeredPreemption bool                       `json:"triggeredPreemption"`
	Originator          bool                       `json:"originator"`
}
