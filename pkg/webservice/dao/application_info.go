/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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

type ApplicationsDAOInfo struct {
	Applications []ApplicationDAOInfo `json:"applications"`
}

type ApplicationDAOInfo struct {
	ApplicationID  string              `json:"applicationID"`
	UsedResource   string              `json:"usedResource"`
	Partition      string              `json:"partition"`
	QueueName      string              `json:"queueName"`
	SubmissionTime int64               `json:"submissionTime"`
	Allocations    []AllocationDAOInfo `json:"allocations"`
	State          string              `json:"applicationState"`
}

type AllocationDAOInfo struct {
	AllocationKey    string            `json:"allocationKey"`
	AllocationTags   map[string]string `json:"allocationTags"`
	UUID             string            `json:"uuid"`
	ResourcePerAlloc string            `json:"resource"`
	Priority         string            `json:"priority"`
	QueueName        string            `json:"queueName"`
	NodeID           string            `json:"nodeID"`
	ApplicationID    string            `json:"applicationID"`
	Partition        string            `json:"partition"`
}
