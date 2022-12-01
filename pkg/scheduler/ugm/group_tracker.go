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

package ugm

import (
	"sync"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

type GroupTracker struct {
	groupName    string          // Name of the group for which usage is being tracked upon
	applications map[string]bool // Hold applications currently run by all users belong to this group
	queueTracker *QueueTracker   // Holds the actual resource usage of queue path where application run

	sync.RWMutex
}

func newGroupTracker(group string) *GroupTracker {
	queueTracker := newRootQueueTracker()
	groupTracker := &GroupTracker{
		groupName:    group,
		applications: make(map[string]bool),
		queueTracker: queueTracker,
	}
	return groupTracker
}

func (gt *GroupTracker) increaseTrackedResource(queuePath, applicationID string, usage *resources.Resource) error {
	gt.Lock()
	defer gt.Unlock()
	gt.applications[applicationID] = true
	return gt.queueTracker.increaseTrackedResource(queuePath, applicationID, usage)
}

func (gt *GroupTracker) decreaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, removeApp bool) error {
	gt.Lock()
	defer gt.Unlock()
	if removeApp {
		delete(gt.applications, applicationID)
	}
	return gt.queueTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
}

func (gt *GroupTracker) getTrackedApplications() map[string]bool {
	gt.RLock()
	defer gt.RUnlock()
	return gt.applications
}

func (gt *GroupTracker) GetGroupResourceUsageDAOInfo() *dao.GroupResourceUsageDAOInfo {
	gt.RLock()
	defer gt.RUnlock()
	groupResourceUsage := &dao.GroupResourceUsageDAOInfo{
		Applications: []string{},
	}
	groupResourceUsage.GroupName = gt.groupName
	for app := range gt.applications {
		groupResourceUsage.Applications = append(groupResourceUsage.Applications, app)
	}
	groupResourceUsage.Queues = gt.queueTracker.getResourceUsageDAOInfo("")
	return groupResourceUsage
}
