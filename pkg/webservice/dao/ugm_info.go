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

import "github.com/apache/yunikorn-core/pkg/common/resources"

type UserResourceUsageDAOInfo struct {
	UserName string                `json:"userName"` // no omitempty, user name should not be empty
	Groups   map[string]string     `json:"groups,omitempty"`
	Queues   *ResourceUsageDAOInfo `json:"queues,omitempty"`
}

type GroupResourceUsageDAOInfo struct {
	GroupName    string                `json:"groupName"` // no omitempty, group name should not be empty
	Applications []string              `json:"applications,omitempty"`
	Queues       *ResourceUsageDAOInfo `json:"queues,omitempty"`
}

type ResourceUsageDAOInfo struct {
	QueuePath           string                  `json:"queuePath"` // no omitempty, queue path should not be empty
	ResourceUsage       *resources.Resource     `json:"resourceUsage,omitempty"`
	RunningApplications []string                `json:"runningApplications,omitempty"`
	MaxResources        *resources.Resource     `json:"maxResources,omitempty"`
	MaxApplications     uint64                  `json:"maxApplications,omitempty"`
	Children            []*ResourceUsageDAOInfo `json:"children,omitempty"`
}
