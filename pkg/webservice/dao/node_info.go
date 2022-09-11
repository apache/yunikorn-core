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
	PartitionName string         `json:"partitionName"`
	Nodes         []*NodeDAOInfo `json:"nodesInfo"`
}

type NodeDAOInfo struct {
	NodeID       string               `json:"nodeID"`
	HostName     string               `json:"hostName"`
	RackName     string               `json:"rackName"`
	Capacity     map[string]int64     `json:"capacity"`
	Allocated    map[string]int64     `json:"allocated"`
	Occupied     map[string]int64     `json:"occupied"`
	Available    map[string]int64     `json:"available"`
	Utilized     map[string]int64     `json:"utilized"`
	Allocations  []*AllocationDAOInfo `json:"allocations"`
	Schedulable  bool                 `json:"schedulable"`
	IsReserved   bool                 `json:"isReserved"`
	Reservations []string             `json:"Reservations"`
}
