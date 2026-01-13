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

type PartitionInfo struct {
	ClusterID               string            `json:"clusterId"`              // no omitempty, cluster id should not be empty
	Name                    string            `json:"name"`                   // no omitempty, name should not be empty
	Capacity                PartitionCapacity `json:"capacity"`               // no omitempty, omitempty doesn't work on a structure value
	NodeSortingPolicy       NodeSortingPolicy `json:"nodeSortingPolicy"`      // no omitempty, omitempty doesn't work on a structure value
	PreemptionEnabled       bool              `json:"preemptionEnabled"`      // no omitempty, false shows preemption status better
	QuotaPreemptionEnabled  bool              `json:"quotaPreemptionEnabled"` // no omitempty, false shows quota preemption status better
	TotalNodes              int               `json:"totalNodes,omitempty"`
	Applications            map[string]int    `json:"applications,omitempty"`
	TotalContainers         int               `json:"totalContainers,omitempty"`
	State                   string            `json:"state,omitempty"`
	LastStateTransitionTime int64             `json:"lastStateTransitionTime,omitempty"`
}

type PartitionCapacity struct {
	Capacity     map[string]int64 `json:"capacity,omitempty"`
	UsedCapacity map[string]int64 `json:"usedCapacity,omitempty"`
	Utilization  map[string]int64 `json:"utilization,omitempty"`
}

type NodeSortingPolicy struct {
	Type            string             `json:"type,omitempty"`
	ResourceWeights map[string]float64 `json:"resourceWeights,omitempty"`
}
