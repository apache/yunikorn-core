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

type NodesDAOInfo struct {
	PartitionName string         `json:"partitionName"` // no omitempty, partition name shoud not be empty
	Nodes         []*NodeDAOInfo `json:"nodesInfo,omitempty"`
}

type NodeDAOInfo struct {
	NodeID             string                      `json:"nodeID"` // no omitempty, node id should not be empty
	HostName           string                      `json:"hostName,omitempty"`
	RackName           string                      `json:"rackName,omitempty"`
	Attributes         map[string]string           `json:"attributes,omitempty"`
	Capacity           map[string]int64            `json:"capacity,omitempty"`
	Allocated          map[string]int64            `json:"allocated,omitempty"`
	Occupied           map[string]int64            `json:"occupied,omitempty"`
	Available          map[string]int64            `json:"available,omitempty"`
	Utilized           map[string]int64            `json:"utilized,omitempty"`
	Allocations        []*AllocationDAOInfo        `json:"allocations,omitempty"`
	ForeignAllocations []*ForeignAllocationDAOInfo `json:"foreignAllocations,omitempty"`
	Schedulable        bool                        `json:"schedulable"` // no omitempty, a false value gives a quick way to understand whether a node is schedulable.
	IsReserved         bool                        `json:"isReserved"`  // no omitempty, a false value gives a quick way to understand whether a node is reserved.
	Reservations       []string                    `json:"reservations,omitempty"`
}
