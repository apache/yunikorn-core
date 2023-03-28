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
	Applications []ApplicationDAOInfo `json:"applications"`
}

type ApplicationDAOInfo struct {
	ApplicationID      string                  `json:"applicationID"`
	UsedResource       map[string]int64        `json:"usedResource"`
	MaxUsedResource    map[string]int64        `json:"maxUsedResource"`
	PendingResource    map[string]int64        `json:"pendingResource"`
	Partition          string                  `json:"partition"`
	QueueName          string                  `json:"queueName"`
	SubmissionTime     int64                   `json:"submissionTime"`
	FinishedTime       *int64                  `json:"finishedTime"`
	Requests           []*AllocationAskDAOInfo `json:"requests"`
	Allocations        []*AllocationDAOInfo    `json:"allocations"`
	State              string                  `json:"applicationState"`
	User               string                  `json:"user"`
	Groups             []string                `json:"groups"`
	RejectedMessage    string                  `json:"rejectedMessage"`
	StateLog           []*StateDAOInfo         `json:"stateLog"`
	PlaceholderData    []*PlaceholderDAOInfo   `json:"placeholderData"`
	HasReserved        bool                    `json:"hasReserved"`
	Reservations       []string                `json:"reservations"`
	MaxRequestPriority int32                   `json:"maxRequestPriority"`
}

type StateDAOInfo struct {
	Time             int64  `json:"time"`
	ApplicationState string `json:"applicationState"`
}

type PlaceholderDAOInfo struct {
	TaskGroupName string           `json:"taskGroupName"`
	Count         int64            `json:"count"`
	MinResource   map[string]int64 `json:"minResource"`
	Replaced      int64            `json:"replaced"`
	TimedOut      int64            `json:"timedout"`
}
