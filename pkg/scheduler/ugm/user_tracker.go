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

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

type UserTracker struct {
	userName string // Name of the user for which usage is being tracked upon

	// Holds group tracker object for every application user runs.
	// Group is not fixed for user unlike other systems and would be selected based on queue limit config processing flow and may vary for different applications.
	// Hence, group tracker object may vary for same user running different applications linked through this map with key as application id
	// and group tracker object as value.
	appGroupTrackers map[string]*GroupTracker
	queueTracker     *QueueTracker // Holds the actual resource usage of queue path where application runs

	sync.RWMutex
}

func newUserTracker(userName string) *UserTracker {
	queueTracker := newRootQueueTracker(user)
	userTracker := &UserTracker{
		userName:         userName,
		appGroupTrackers: make(map[string]*GroupTracker),
		queueTracker:     queueTracker,
	}
	return userTracker
}

func (ut *UserTracker) increaseTrackedResource(hierarchy []string, applicationID string, usage *resources.Resource) {
	ut.Lock()
	defer ut.Unlock()
	ut.queueTracker.increaseTrackedResource(hierarchy, applicationID, user, usage)
	gt := ut.appGroupTrackers[applicationID]
	log.Log(log.SchedUGM).Debug("Increasing resource usage for group",
		zap.String("group", gt.getName()),
		zap.Strings("queue path", hierarchy),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage))
	gt.increaseTrackedResource(hierarchy, applicationID, usage, ut.userName)
}

func (ut *UserTracker) decreaseTrackedResource(hierarchy []string, applicationID string, usage *resources.Resource, removeApp bool) bool {
	ut.Lock()
	defer ut.Unlock()
	if removeApp {
		delete(ut.appGroupTrackers, applicationID)
	}
	return ut.queueTracker.decreaseTrackedResource(hierarchy, applicationID, usage, removeApp)
}

func (ut *UserTracker) hasGroupForApp(applicationID string) bool {
	ut.RLock()
	defer ut.RUnlock()
	_, ok := ut.appGroupTrackers[applicationID]
	return ok
}

func (ut *UserTracker) setGroupForApp(applicationID string, groupTrack *GroupTracker) {
	ut.Lock()
	defer ut.Unlock()
	ut.appGroupTrackers[applicationID] = groupTrack
}

func (ut *UserTracker) getGroupForApp(applicationID string) string {
	ut.RLock()
	defer ut.RUnlock()
	if ut.appGroupTrackers[applicationID] != nil {
		return ut.appGroupTrackers[applicationID].groupName
	}
	return common.Empty
}

func (ut *UserTracker) getTrackedApplications() map[string]*GroupTracker {
	ut.RLock()
	defer ut.RUnlock()
	return ut.appGroupTrackers
}

func (ut *UserTracker) setLimits(hierarchy []string, resource *resources.Resource, maxApps uint64, useWildCard bool, doWildCardCheck bool) {
	ut.Lock()
	defer ut.Unlock()
	ut.queueTracker.setLimit(hierarchy, resource, maxApps, useWildCard, user, doWildCardCheck)
}

func (ut *UserTracker) headroom(hierarchy []string) *resources.Resource {
	ut.Lock()
	defer ut.Unlock()
	return ut.queueTracker.headroom(hierarchy, user)
}

func (ut *UserTracker) GetUserResourceUsageDAOInfo() *dao.UserResourceUsageDAOInfo {
	ut.RLock()
	defer ut.RUnlock()
	userResourceUsage := &dao.UserResourceUsageDAOInfo{
		Groups: make(map[string]string),
	}
	userResourceUsage.UserName = ut.userName
	for app, gt := range ut.appGroupTrackers {
		if gt != nil {
			userResourceUsage.Groups[app] = gt.groupName
		}
	}
	userResourceUsage.Queues = ut.queueTracker.getResourceUsageDAOInfo(common.Empty)
	return userResourceUsage
}

func (ut *UserTracker) IsQueuePathTrackedCompletely(hierarchy []string) bool {
	ut.RLock()
	defer ut.RUnlock()
	return ut.queueTracker.IsQueuePathTrackedCompletely(hierarchy)
}

func (ut *UserTracker) IsUnlinkRequired(hierarchy []string) bool {
	ut.RLock()
	defer ut.RUnlock()
	return ut.queueTracker.IsUnlinkRequired(hierarchy)
}

func (ut *UserTracker) UnlinkQT(hierarchy []string) bool {
	ut.Lock()
	defer ut.Unlock()
	return ut.queueTracker.UnlinkQT(hierarchy)
}

func (ut *UserTracker) canBeRemoved() bool {
	ut.RLock()
	defer ut.RUnlock()
	return ut.queueTracker.canBeRemoved()
}

func (ut *UserTracker) canRunApp(hierarchy []string, applicationID string) bool {
	ut.Lock()
	defer ut.Unlock()
	return ut.queueTracker.canRunApp(hierarchy, applicationID, user)
}
