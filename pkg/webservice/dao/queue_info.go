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

type TemplateInfo struct {
	MaxResource        map[string]int64  `json:"maxResource,omitempty"`
	GuaranteedResource map[string]int64  `json:"guaranteedResource,omitempty"`
	Properties         map[string]string `json:"properties,omitempty"`
}

type PartitionQueueDAOInfo struct {
	QueueName              string                  `json:"queuename"` // no omitempty, queue name should not be empty
	Status                 string                  `json:"status,omitempty"`
	Partition              string                  `json:"partition"` // no omitempty, queue name should not be empty
	PendingResource        map[string]int64        `json:"pendingResource,omitempty"`
	MaxResource            map[string]int64        `json:"maxResource,omitempty"`
	GuaranteedResource     map[string]int64        `json:"guaranteedResource,omitempty"`
	AllocatedResource      map[string]int64        `json:"allocatedResource,omitempty"`
	PreemptingResource     map[string]int64        `json:"preemptingResource,omitempty"`
	IsLeaf                 bool                    `json:"isLeaf"`    // no omitempty, a false value gives a quick way to understand whether it's leaf.
	IsManaged              bool                    `json:"isManaged"` // no omitempty, a false value gives a quick way to understand whether it's managed.
	Properties             map[string]string       `json:"properties,omitempty"`
	Parent                 string                  `json:"parent,omitempty"`
	TemplateInfo           *TemplateInfo           `json:"template,omitempty"`
	Children               []PartitionQueueDAOInfo `json:"children,omitempty"`
	AbsUsedCapacity        map[string]int64        `json:"absUsedCapacity,omitempty"`
	MaxRunningApps         uint64                  `json:"maxRunningApps,omitempty"`
	RunningApps            uint64                  `json:"runningApps,omitempty"`
	CurrentPriority        int32                   `json:"currentPriority"` // no omitempty, as the current priority value may be 0, which is a valid priority level
	AllocatingAcceptedApps []string                `json:"allocatingAcceptedApps,omitempty"`
}
