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

type QueueDAOInfo struct {
	QueueName   string            `json:"queuename"`
	Status      string            `json:"status"`
	Capacities  QueueCapacity     `json:"capacities"`
	ChildQueues []QueueDAOInfo    `json:"queues"`
	Properties  map[string]string `json:"properties"`
}

type QueueCapacity struct {
	Capacity        map[string]int64 `json:"capacity"`
	MaxCapacity     map[string]int64 `json:"maxCapacity"`
	UsedCapacity    map[string]int64 `json:"usedCapacity"`
	AbsUsedCapacity map[string]int64 `json:"absUsedCapacity"`
}

type TemplateInfo struct {
	MaxResource        map[string]int64  `json:"maxResource"`
	GuaranteedResource map[string]int64  `json:"guaranteedResource"`
	Properties         map[string]string `json:"properties"`
}

type PartitionQueueDAOInfo struct {
	QueueName              string                  `json:"queuename"`
	Status                 string                  `json:"status"`
	Partition              string                  `json:"partition"`
	PendingResource        map[string]int64        `json:"pendingResource"`
	MaxResource            map[string]int64        `json:"maxResource"`
	GuaranteedResource     map[string]int64        `json:"guaranteedResource"`
	AllocatedResource      map[string]int64        `json:"allocatedResource"`
	PreemptingResource     map[string]int64        `json:"preemptingResource"`
	IsLeaf                 bool                    `json:"isLeaf"`
	IsManaged              bool                    `json:"isManaged"`
	Properties             map[string]string       `json:"properties"`
	Parent                 string                  `json:"parent"`
	TemplateInfo           *TemplateInfo           `json:"template"`
	Children               []PartitionQueueDAOInfo `json:"children"`
	AbsUsedCapacity        map[string]int64        `json:"absUsedCapacity"`
	MaxRunningApps         uint64                  `json:"maxRunningApps"`
	RunningApps            uint64                  `json:"runningApps"`
	CurrentPriority        int32                   `json:"currentPriority"`
	AllocatingAcceptedApps []string                `json:"allocatingAcceptedApps"`
}
