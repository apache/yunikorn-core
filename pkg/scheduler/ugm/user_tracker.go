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
	"github.com/apache/yunikorn-core/pkg/common/security"
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

func newUserTracker(user security.UserGroup) *UserTracker {
	queueTracker := newRootQueueTracker()
	userTracker := &UserTracker{
		userName:         user.User,
		appGroupTrackers: make(map[string]*GroupTracker),
		queueTracker:     queueTracker,
	}
	return userTracker
}

func (ut *UserTracker) increaseTrackedResource(queuePath, applicationID string, usage *resources.Resource) error {
	ut.Lock()
	defer ut.Unlock()
	return ut.queueTracker.increaseTrackedResource(queuePath, applicationID, usage)
}

func (ut *UserTracker) decreaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, removeApp bool) error {
	ut.Lock()
	defer ut.Unlock()
	if removeApp {
		delete(ut.appGroupTrackers, applicationID)
	}
	return ut.queueTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
}

func (ut *UserTracker) hasGroupForApp(applicationID string) bool {
	ut.RLock()
	defer ut.RUnlock()
	return ut.appGroupTrackers[applicationID] != nil
}

func (ut *UserTracker) setGroupForApp(applicationID string, groupTrack *GroupTracker) {
	ut.Lock()
	defer ut.Unlock()
	if ut.appGroupTrackers[applicationID] == nil {
		ut.appGroupTrackers[applicationID] = groupTrack
	}
}

func (ut *UserTracker) getTrackedApplications() map[string]*GroupTracker {
	ut.RLock()
	defer ut.RUnlock()
	return ut.appGroupTrackers
}

func (ut *UserTracker) GetUserResourceUsageDAOInfo() *dao.UserResourceUsageDAOInfo {
	ut.RLock()
	defer ut.RUnlock()
	userResourceUsage := &dao.UserResourceUsageDAOInfo{
		Groups: make(map[string]string),
	}
	userResourceUsage.UserName = ut.userName
	for app, gt := range ut.appGroupTrackers {
		userResourceUsage.Groups[app] = gt.groupName
	}
	userResourceUsage.Queues = ut.queueTracker.getResourceUsageDAOInfo("")
	return userResourceUsage
}
