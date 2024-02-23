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

type PartitionNodesUtilDAOInfo struct {
	ClusterID     string              `json:"clusterId"` // no omitempty, cluster id should not be empty
	Partition     string              `json:"partition"` // no omitempty, partition should not be empty
	NodesUtilList []*NodesUtilDAOInfo `json:"utilizations,omitempty"`
}

type NodesUtilDAOInfo struct {
	ResourceType string             `json:"type,omitempty"`
	NodesUtil    []*NodeUtilDAOInfo `json:"utilization,omitempty"`
}

type NodeUtilDAOInfo struct {
	BucketName string   `json:"bucketName,omitempty"`
	NumOfNodes int64    `json:"numOfNodes,omitempty"`
	NodeNames  []string `json:"nodeNames,omitempty"`
}
