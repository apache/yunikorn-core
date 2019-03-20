/*
Copyright 2019 The Unity Scheduler Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package dao

type ClusterDAOInfo struct {
	ClusterName       string `json:"clustername"`
	TotalJobs         string `json:"totalJobs"`
	FailedJobs        string `json:"failedJobs"`
	PendingJobs       string `json:"pendingJobs"`
	RunningJobs       string `json:"runningJobs"`
	CompletedJobs     string `json:"completedJobs"`
	TotalContainers   string `json:"totalContainers"`
	FailedContainers  string `json:"failedContainers"`
	PendingContainers string `json:"pendingContainers"`
	RunningContainers string `json:"runningContainers"`
	ActiveNodes       string `json:"activeNodes"`
	TotalNodes        string `json:"totalNodes"`
	FailedNodes       string `json:"failedNodes"`
}