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
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

type GroupTracker struct {
	groupName    string            // Name of the group for which usage is being tracked upon
	applications map[string]string // Hold applications currently run by all users belong to this group
	queueTracker *QueueTracker     // Holds the actual resource usage of queue path where application run

	sync.RWMutex
}

func newGroupTracker(group string) *GroupTracker {
	queueTracker := newRootQueueTracker()
	groupTracker := &GroupTracker{
		groupName:    group,
		applications: make(map[string]string),
		queueTracker: queueTracker,
	}
	return groupTracker
}

func (gt *GroupTracker) increaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, user string) bool {
	if gt == nil {
		return true
	}
	gt.Lock()
	defer gt.Unlock()
	gt.applications[applicationID] = user
	return gt.queueTracker.increaseTrackedResource(queuePath, applicationID, group, usage)
}

func (gt *GroupTracker) decreaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, removeApp bool) (bool, bool) {
	if gt == nil {
		return false, true
	}
	gt.Lock()
	defer gt.Unlock()
	if removeApp {
		delete(gt.applications, applicationID)
	}
	return gt.queueTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
}

func (gt *GroupTracker) getTrackedApplications() map[string]string {
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
	groupResourceUsage.Queues = gt.queueTracker.getResourceUsageDAOInfo(common.Empty)
	return groupResourceUsage
}

func (gt *GroupTracker) ClearEarlierSetLimits(prefixPath string, wildcardLimits map[string]*LimitConfig, pathLimits map[string]bool, removedApp map[string]bool) {
	gt.Lock()
	defer gt.Unlock()
	gt.internalClearEarlierSetLimits(prefixPath, gt.queueTracker, wildcardLimits, pathLimits, removedApp)
}

func (gt *GroupTracker) internalClearEarlierSetLimits(prefixPath string, queueTracker *QueueTracker, wildcardLimits map[string]*LimitConfig, pathLimits map[string]bool, removedApp map[string]bool) bool {
	queuePath := prefixPath + configs.DOT + queueTracker.queueName
	if prefixPath == common.Empty {
		queuePath = queueTracker.queueName
	}

	allLeafQueueRemoved := true
	for _, childQt := range queueTracker.childQueueTrackers {
		if gt.internalClearEarlierSetLimits(queuePath, childQt, wildcardLimits, pathLimits, removedApp) {
			delete(queueTracker.childQueueTrackers, childQt.queueName)
		} else {
			allLeafQueueRemoved = false
		}
	}

	currentPathHasLimits := wildcardLimits[queuePath] != nil || pathLimits[queuePath]
	if allLeafQueueRemoved && !currentPathHasLimits {
		for app := range queueTracker.runningApplications {
			removedApp[app] = true
		}
		queueTracker.runningApplications = make(map[string]bool)
		queueTracker.resourceUsage = resources.NewResource()
		return true
	} else {
		// if we don't remove current tracker, we have to keep applications in current tracker
		for app := range queueTracker.runningApplications {
			delete(removedApp, app)
		}
	}

	// if only have wildcard limits, then clear the limits
	if wildcardLimits[queuePath] != nil && !pathLimits[queuePath] {
		queueTracker.maxRunningApps = 0
		queueTracker.maxResources = resources.NewResource()
	}
	return false
}

// canBeRemoved Does "root" queue has any child queue trackers? Is there any running applications in "root" qt?
func (gt *GroupTracker) canBeRemoved() bool {
	gt.RLock()
	defer gt.RUnlock()
	return len(gt.queueTracker.childQueueTrackers) == 0 && len(gt.queueTracker.runningApplications) == 0
}

func (gt *GroupTracker) getName() string {
	if gt == nil {
		return common.Empty
	}
	return gt.groupName
}

func (gt *GroupTracker) canRunApp(queuePath, applicationID string) bool {
	gt.Lock()
	defer gt.Unlock()
	return gt.queueTracker.canRunApp(queuePath, applicationID, group)
}
