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

func (gt *GroupTracker) increaseTrackedResource(queuePath, applicationID string, usage *resources.Resource) bool {
	gt.Lock()
	defer gt.Unlock()
	gt.applications[applicationID] = true
	return gt.queueTracker.increaseTrackedResource(queuePath, applicationID, group, usage)
}

func (gt *GroupTracker) decreaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, removeApp bool) (bool, bool) {
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

func (gt *GroupTracker) setLimits(queuePath string, resource *resources.Resource, maxApps uint64) {
	gt.Lock()
	defer gt.Unlock()
	gt.queueTracker.setLimit(queuePath, resource, maxApps)
}

func (gt *GroupTracker) headroom(queuePath string) *resources.Resource {
	gt.Lock()
	defer gt.Unlock()
	return gt.queueTracker.headroom(queuePath)
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

func (gt *GroupTracker) IsQueuePathTrackedCompletely(queuePath string) bool {
	gt.RLock()
	defer gt.RUnlock()
	return gt.queueTracker.IsQueuePathTrackedCompletely(queuePath)
}

func (gt *GroupTracker) IsUnlinkRequired(queuePath string) bool {
	gt.RLock()
	defer gt.RUnlock()
	return gt.queueTracker.IsUnlinkRequired(queuePath)
}

func (gt *GroupTracker) UnlinkQT(queuePath string) bool {
	gt.RLock()
	defer gt.RUnlock()
	return gt.queueTracker.UnlinkQT(queuePath)
}

// canBeRemoved Does "root" queue has any child queue trackers? Is there any running applications in "root" qt?
func (gt *GroupTracker) canBeRemoved() bool {
	gt.RLock()
	defer gt.RUnlock()
	return len(gt.queueTracker.childQueueTrackers) == 0 && len(gt.queueTracker.runningApplications) == 0
}
