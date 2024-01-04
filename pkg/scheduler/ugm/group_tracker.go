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

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

type GroupTracker struct {
	groupName    string            // Name of the group for which usage is being tracked upon
	applications map[string]string // Hold applications currently run by all users belong to this group
	queueTracker *QueueTracker     // Holds the actual resource usage of queue path where application run

	sync.RWMutex
}

func newGroupTracker(groupName string) *GroupTracker {
	queueTracker := newRootQueueTracker(group)
	groupTracker := &GroupTracker{
		groupName:    groupName,
		applications: make(map[string]string),
		queueTracker: queueTracker,
	}
	return groupTracker
}

func (gt *GroupTracker) increaseTrackedResource(hierarchy []string, applicationID string, usage *resources.Resource, user string) bool {
	if gt == nil {
		return true
	}
	gt.Lock()
	defer gt.Unlock()
	gt.applications[applicationID] = user
	return gt.queueTracker.increaseTrackedResource(hierarchy, applicationID, group, usage)
}

func (gt *GroupTracker) decreaseTrackedResource(hierarchy []string, applicationID string, usage *resources.Resource, removeApp bool) (bool, bool) {
	if gt == nil {
		return false, true
	}
	gt.Lock()
	defer gt.Unlock()
	if removeApp {
		delete(gt.applications, applicationID)
	}
	return gt.queueTracker.decreaseTrackedResource(hierarchy, applicationID, usage, removeApp)
}

func (gt *GroupTracker) getTrackedApplications() map[string]string {
	gt.RLock()
	defer gt.RUnlock()
	return gt.applications
}

func (gt *GroupTracker) setLimits(hierarchy []string, resource *resources.Resource, maxApps uint64) {
	gt.Lock()
	defer gt.Unlock()
	gt.queueTracker.setLimit(hierarchy, resource, maxApps, false, group, false)
}

func (gt *GroupTracker) headroom(hierarchy []string) *resources.Resource {
	gt.Lock()
	defer gt.Unlock()
	return gt.queueTracker.headroom(hierarchy, group)
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
	groupResourceUsage.Queues = gt.queueTracker.getResourceUsageDAOInfo(common.Empty)
	return groupResourceUsage
}

func (gt *GroupTracker) IsQueuePathTrackedCompletely(hierarchy []string) bool {
	gt.RLock()
	defer gt.RUnlock()
	return gt.queueTracker.IsQueuePathTrackedCompletely(hierarchy)
}

func (gt *GroupTracker) IsUnlinkRequired(hierarchy []string) bool {
	gt.RLock()
	defer gt.RUnlock()
	return gt.queueTracker.IsUnlinkRequired(hierarchy)
}

func (gt *GroupTracker) UnlinkQT(hierarchy []string) bool {
	gt.RLock()
	defer gt.RUnlock()
	return gt.queueTracker.UnlinkQT(hierarchy)
}

func (gt *GroupTracker) canBeRemoved() bool {
	gt.RLock()
	defer gt.RUnlock()
	return gt.queueTracker.canBeRemoved()
}

func (gt *GroupTracker) getName() string {
	if gt == nil {
		return common.Empty
	}
	return gt.groupName
}

func (gt *GroupTracker) decreaseAllTrackedResourceUsage(hierarchy []string) map[string]string {
	if gt == nil {
		return nil
	}
	gt.Lock()
	defer gt.Unlock()
	applications := gt.queueTracker.decreaseTrackedResourceUsageDownwards(hierarchy)
	removedApplications := make(map[string]string)
	for app := range applications {
		if u, ok := gt.applications[app]; ok {
			removedApplications[app] = u
		}
	}
	return removedApplications
}

func (gt *GroupTracker) canRunApp(hierarchy []string, applicationID string) bool {
	gt.Lock()
	defer gt.Unlock()
	return gt.queueTracker.canRunApp(hierarchy, applicationID, group)
}
