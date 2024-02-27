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
	"strings"
	"sync"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

type GroupTracker struct {
	groupName    string            // Name of the group for which usage is being tracked upon
	applications map[string]string // Hold applications currently run by all users belong to this group
	queueTracker *QueueTracker     // Holds the actual resource usage of queue path where application run
	events       *ugmEvents

	sync.RWMutex
}

func newGroupTracker(groupName string, events *ugmEvents) *GroupTracker {
	queueTracker := newRootQueueTracker(group)
	groupTracker := &GroupTracker{
		groupName:    groupName,
		applications: make(map[string]string),
		queueTracker: queueTracker,
		events:       events,
	}
	return groupTracker
}

func (gt *GroupTracker) increaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, user string) bool {
	if gt == nil {
		return true
	}
	gt.Lock()
	defer gt.Unlock()
	gt.events.sendIncResourceUsageForGroup(gt.groupName, queuePath, usage)
	gt.applications[applicationID] = user
	return gt.queueTracker.increaseTrackedResource(strings.Split(queuePath, configs.DOT), applicationID, group, usage)
}

func (gt *GroupTracker) decreaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, removeApp bool) (bool, bool) {
	if gt == nil {
		return false, true
	}
	gt.Lock()
	defer gt.Unlock()
	gt.events.sendDecResourceUsageForGroup(gt.groupName, queuePath, usage)
	if removeApp {
		delete(gt.applications, applicationID)
	}
	return gt.queueTracker.decreaseTrackedResource(strings.Split(queuePath, configs.DOT), applicationID, usage, removeApp)
}

func (gt *GroupTracker) getTrackedApplications() map[string]string {
	gt.RLock()
	defer gt.RUnlock()
	return gt.applications
}

func (gt *GroupTracker) setLimits(queuePath string, resource *resources.Resource, maxApps uint64) {
	gt.Lock()
	defer gt.Unlock()
	gt.events.sendLimitSetForGroup(gt.groupName, queuePath)
	gt.queueTracker.setLimit(strings.Split(queuePath, configs.DOT), resource, maxApps, false, group, false)
}

func (gt *GroupTracker) clearLimits(queuePath string) {
	gt.Lock()
	defer gt.Unlock()
	gt.events.sendLimitRemoveForGroup(gt.groupName, queuePath)
	gt.queueTracker.setLimit(strings.Split(queuePath, configs.DOT), nil, 0, false, group, false)
}

// Note: headroom of queue tracker is not read-only, it also traverses the queue hierarchy and creates childQueueTracker if it does not exist.
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
	gt.Lock()
	defer gt.Unlock()
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

// Note: canRunApp of queue tracker is not read-only, it also traverses the queue hierarchy and creates a childQueueTracker if it does not exist.
func (gt *GroupTracker) canRunApp(hierarchy []string, applicationID string) bool {
	gt.Lock()
	defer gt.Unlock()
	return gt.queueTracker.canRunApp(hierarchy, applicationID, group)
}
