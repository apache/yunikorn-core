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

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/locking"
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
	events           *ugmEvents

	locking.RWMutex
}

func newUserTracker(userName string, ugmEvents *ugmEvents) *UserTracker {
	queueTracker := newRootQueueTracker(user)
	userTracker := &UserTracker{
		userName:         userName,
		appGroupTrackers: make(map[string]*GroupTracker),
		queueTracker:     queueTracker,
		events:           ugmEvents,
	}
	return userTracker
}

func (ut *UserTracker) increaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource) {
	ut.Lock()
	defer ut.Unlock()
	ut.events.sendIncResourceUsageForUser(ut.userName, queuePath, usage)
	hierarchy := strings.Split(queuePath, configs.DOT)
	ut.queueTracker.increaseTrackedResource(hierarchy, applicationID, user, usage)
}

func (ut *UserTracker) decreaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource, removeApp bool) bool {
	ut.Lock()
	defer ut.Unlock()
	ut.events.sendDecResourceUsageForUser(ut.userName, queuePath, usage)
	if removeApp {
		tracker := ut.appGroupTrackers[applicationID]
		if tracker != nil {
			appGroup := tracker.groupName
			ut.events.sendAppGroupUnlinked(appGroup, applicationID)
		}
		delete(ut.appGroupTrackers, applicationID)
	}
	return ut.queueTracker.decreaseTrackedResource(strings.Split(queuePath, configs.DOT), applicationID, usage, removeApp)
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
	if groupTrack != nil {
		ut.events.sendAppGroupLinked(groupTrack.groupName, applicationID)
	}
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

func (ut *UserTracker) setLimits(queuePath string, resource *resources.Resource, maxApps uint64, useWildCard bool, doWildCardCheck bool) {
	ut.Lock()
	defer ut.Unlock()
	ut.events.sendLimitSetForUser(ut.userName, queuePath)
	ut.queueTracker.setLimit(strings.Split(queuePath, configs.DOT), resource, maxApps, useWildCard, user, doWildCardCheck)
}

func (ut *UserTracker) clearLimits(queuePath string, doWildCardCheck bool) {
	ut.Lock()
	defer ut.Unlock()
	ut.events.sendLimitRemoveForUser(ut.userName, queuePath)
	ut.queueTracker.setLimit(strings.Split(queuePath, configs.DOT), nil, 0, false, user, doWildCardCheck)
}

// headroom calculate the resource headroom for the user in the hierarchy defined
// Note: headroom of queue tracker is not read-only.
// It traverses the queue hierarchy and creates a childQueueTracker if it does not exist.
func (ut *UserTracker) headroom(hierarchy []string) *resources.Resource {
	ut.Lock()
	defer ut.Unlock()
	return ut.queueTracker.headroom(hierarchy, user)
}

// GetResourceUsageDAOInfo returns the DAO object used in the REST API for this user tracker
func (ut *UserTracker) GetResourceUsageDAOInfo() *dao.UserResourceUsageDAOInfo {
	ut.RLock()
	defer ut.RUnlock()
	groups := make(map[string]string, len(ut.appGroupTrackers))
	for app, gt := range ut.appGroupTrackers {
		if gt != nil {
			groups[app] = gt.groupName
		}
	}

	return &dao.UserResourceUsageDAOInfo{
		Groups:   groups,
		UserName: ut.userName,
		Queues:   ut.queueTracker.getResourceUsageDAOInfo(),
	}
}

func (ut *UserTracker) isQueuePathTrackedCompletely(hierarchy []string) bool {
	ut.RLock()
	defer ut.RUnlock()
	return ut.queueTracker.isQueuePathTrackedCompletely(hierarchy)
}

func (ut *UserTracker) isUnlinkRequired(hierarchy []string) bool {
	ut.RLock()
	defer ut.RUnlock()
	return ut.queueTracker.isUnlinkRequired(hierarchy)
}

func (ut *UserTracker) unlinkQT(hierarchy []string) bool {
	ut.Lock()
	defer ut.Unlock()
	return ut.queueTracker.unlink(hierarchy)
}

func (ut *UserTracker) canBeRemoved() bool {
	ut.RLock()
	defer ut.RUnlock()
	return ut.queueTracker.canBeRemoved()
}

// canRunApp checks if the user is allowed to run the application in the queue defined in hierarchy.
// Note: canRunApp of queue tracker is not read-only.
// It traverses the queue hierarchy and creates a childQueueTracker if it does not exist.
func (ut *UserTracker) canRunApp(hierarchy []string, applicationID string) bool {
	ut.Lock()
	defer ut.Unlock()
	return ut.queueTracker.canRunApp(hierarchy, applicationID, user)
}

// GetMaxResources returns a map of the maxResources for all queues registered under this user tracker.
// The key into the map is the queue path.
// This should only be used in test
func (ut *UserTracker) GetMaxResources() map[string]*resources.Resource {
	ut.RLock()
	defer ut.RUnlock()
	return ut.queueTracker.getMaxResources()
}

// GetMaxApplications returns a map of the maxRunningApps for all queues registered under this user tracker.
// The key into the map is the queue path.
// This should only be used in test
func (ut *UserTracker) GetMaxApplications() map[string]uint64 {
	ut.RLock()
	defer ut.RUnlock()
	return ut.queueTracker.getMaxApplications()
}

// getUsedResources returns a map of the usedResources for all queues registered under this user tracker.
// The key into the map is the queue path.
// This should only be used in test
func (ut *UserTracker) getUsedResources() map[string]*resources.Resource {
	ut.RLock()
	defer ut.RUnlock()
	return ut.queueTracker.getUsedResources()
}
