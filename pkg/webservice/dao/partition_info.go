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

type PartitionDAOInfo struct {
	PartitionName string            `json:"partitionName"`
	Capacity      PartitionCapacity `json:"capacity"`
	Nodes         []NodeInfo        `json:"nodes"`
	Queues        QueueDAOInfo      `json:"queues"`
}

type PartitionInfo struct {
	ClusterID               string            `json:"clusterId"`
	Name                    string            `json:"name"`
	Capacity                PartitionCapacity `json:"capacity"`
	NodeSortingPolicy       NodeSortingPolicy `json:"nodeSortingPolicy"`
	Applications            map[string]int    `json:"applications"`
	State                   string            `json:"state"`
	LastStateTransitionTime int64             `json:"lastStateTransitionTime"`
}

type PartitionCapacity struct {
	Capacity     map[string]int64 `json:"capacity"`
	UsedCapacity map[string]int64 `json:"usedCapacity"`
	Utilization  map[string]int64 `json:"utilization"`
}

type NodeInfo struct {
	NodeID     string `json:"nodeId"`
	Capability string `json:"capability"`
}

type NodeSortingPolicy struct {
	Type            string             `json:"type"`
	ResourceWeights map[string]float64 `json:"resourceWeights"`
}
