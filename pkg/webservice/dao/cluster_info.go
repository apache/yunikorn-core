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

type ClusterDAOInfo struct {
	ScheduleStartDate     string              `json:"scheduleStartDate"`
	RMBuildInformation    []map[string]string `json:"rmBuildInformation"`
	PartitionName         string              `json:"partition"`
	ClusterName           string              `json:"clusterName"`
	TotalApplications     string              `json:"totalApplications"`
	FailedApplications    string              `json:"failedApplications"`
	PendingApplications   string              `json:"pendingApplications"`
	RunningApplications   string              `json:"runningApplications"`
	CompletedApplications string              `json:"completedApplications"`
	TotalContainers       string              `json:"totalContainers"`
	FailedContainers      string              `json:"failedContainers"`
	PendingContainers     string              `json:"pendingContainers"`
	RunningContainers     string              `json:"runningContainers"`
	ActiveNodes           string              `json:"activeNodes"`
	TotalNodes            string              `json:"totalNodes"`
	FailedNodes           string              `json:"failedNodes"`
}

var ScheduleStartDate string
