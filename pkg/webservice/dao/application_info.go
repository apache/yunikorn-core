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

type ApplicationsDAOInfo struct {
	Applications []ApplicationDAOInfo `json:"applications,omitempty"`
}

type ApplicationDAOInfo struct {
	ApplicationID      string                  `json:"applicationID"` // no omitempty, application id should not be empty
	UsedResource       map[string]int64        `json:"usedResource,omitempty"`
	MaxUsedResource    map[string]int64        `json:"maxUsedResource,omitempty"`
	PendingResource    map[string]int64        `json:"pendingResource,omitempty"`
	Partition          string                  `json:"partition"` // no omitempty, partition should not be empty
	QueueName          string                  `json:"queueName"` // no omitempty, queue name should not be empty
	SubmissionTime     int64                   `json:"submissionTime,omitempty"`
	FinishedTime       *int64                  `json:"finishedTime,omitempty"`
	Requests           []*AllocationAskDAOInfo `json:"requests,omitempty"`
	Allocations        []*AllocationDAOInfo    `json:"allocations,omitempty"`
	State              string                  `json:"applicationState,omitempty"`
	User               string                  `json:"user,omitempty"`
	Groups             []string                `json:"groups,omitempty"`
	RejectedMessage    string                  `json:"rejectedMessage,omitempty"`
	StateLog           []*StateDAOInfo         `json:"stateLog,omitempty"`
	PlaceholderData    []*PlaceholderDAOInfo   `json:"placeholderData,omitempty"`
	HasReserved        bool                    `json:"hasReserved,omitempty"`
	Reservations       []string                `json:"reservations,omitempty"`
	MaxRequestPriority int32                   `json:"maxRequestPriority,omitempty"`
}

type StateDAOInfo struct {
	Time             int64  `json:"time,omitempty"`
	ApplicationState string `json:"applicationState,omitempty"`
}

type PlaceholderDAOInfo struct {
	TaskGroupName string           `json:"taskGroupName,omitempty"`
	Count         int64            `json:"count,omitempty"`
	MinResource   map[string]int64 `json:"minResource,omitempty"`
	Replaced      int64            `json:"replaced,omitempty"`
	TimedOut      int64            `json:"timedout,omitempty"`
}
