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
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/log"
)

var once sync.Once
var m *Manager

// Manager implements tracker. A User Group Manager to track the usage for both user and groups.
// Holds object of both user and group trackers
type Manager struct {
	userTrackers  map[string]*UserTracker
	groupTrackers map[string]*GroupTracker
	lock          sync.RWMutex
}

func newManager() *Manager {
	manager := &Manager{
		userTrackers:  make(map[string]*UserTracker),
		groupTrackers: make(map[string]*GroupTracker),
		lock:          sync.RWMutex{},
	}
	return manager
}

func GetUserManager() *Manager {
	once.Do(func() {
		m = newManager()
	})
	return m
}

// IncreaseTrackedResource Increase the resource usage for the given user group and queue path combination.
// As and when every allocation or asks requests fulfilled on application, corresponding user and group
// resource usage would be increased against specific application.
func (m *Manager) IncreaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource, user security.UserGroup) error {
	log.Logger().Debug("Increasing resource usage", zap.String("user", user.User),
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.String("resource", usage.String()))
	if queuePath == "" || applicationID == "" || usage == nil || user.User == "" {
		log.Logger().Error("Mandatory parameters are missing to increase the resource usage",
			zap.String("user", user.User),
			zap.String("queue path", queuePath),
			zap.String("application", applicationID),
			zap.String("resource", usage.String()))
		return fmt.Errorf("mandatory parameters are missing. queuepath: %s, application id: %s, resource usage: %s, user: %s",
			queuePath, applicationID, usage.String(), user.User)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	var userTracker *UserTracker
	if m.userTrackers[user.User] == nil {
		userTracker = newUserTracker(user)
		m.userTrackers[user.User] = userTracker
	} else {
		userTracker = m.userTrackers[user.User]
	}
	err := userTracker.increaseTrackedResource(queuePath, applicationID, usage)
	if err != nil {
		log.Logger().Error("Problem in increasing the user resource usage",
			zap.String("user", user.User),
			zap.String("queue path", queuePath),
			zap.String("application", applicationID),
			zap.String("resource", usage.String()),
			zap.String("err message", err.Error()))
		return err
	}
	if err = m.ensureGroupTrackerForApp(queuePath, applicationID, user); err != nil {
		return err
	}
	group, err := m.getGroup(user)
	if err != nil {
		return err
	}
	groupTracker := m.groupTrackers[group]
	if groupTracker != nil {
		err = groupTracker.increaseTrackedResource(queuePath, applicationID, usage)
		if err != nil {
			log.Logger().Error("Problem in increasing the group resource usage",
				zap.String("user", user.User),
				zap.String("group", group),
				zap.String("queue path", queuePath),
				zap.String("application", applicationID),
				zap.String("resource", usage.String()),
				zap.String("err message", err.Error()))
			return err
		}
	}
	return nil
}

// DecreaseTrackedResource Decrease the resource usage for the given user group and queue path combination.
// As and when every allocation or asks release happens, corresponding user and group
// resource usage would be decreased against specific application. When the final asks release happens, removeApp should be set to true and
// application itself would be removed from the tracker and no more usage would be tracked further for that specific application.
func (m *Manager) DecreaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource, user security.UserGroup, removeApp bool) error {
	log.Logger().Debug("Decreasing resource usage", zap.String("user", user.User),
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.String("resource", usage.String()),
		zap.Bool("removeApp", removeApp))
	if queuePath == "" || applicationID == "" || usage == nil || user.User == "" {
		log.Logger().Error("Mandatory parameters are missing to decrease the resource usage",
			zap.String("user", user.User),
			zap.String("queue path", queuePath),
			zap.String("application", applicationID),
			zap.String("resource", usage.String()),
			zap.Bool("removeApp", removeApp))
		return fmt.Errorf("mandatory parameters are missing. queuepath: %s, application id: %s, resource usage: %s, user: %s",
			queuePath, applicationID, usage.String(), user.User)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	userTracker := m.userTrackers[user.User]
	if userTracker != nil {
		err := userTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
		if err != nil {
			log.Logger().Error("Problem in decreasing the user resource usage",
				zap.String("user", user.User),
				zap.String("queue path", queuePath),
				zap.String("application", applicationID),
				zap.String("resource", usage.String()),
				zap.Bool("removeApp", removeApp),
				zap.String("err message", err.Error()))
			return err
		}
		if removeApp {
			if m.isUserRemovable(userTracker) {
				delete(m.userTrackers, user.User)
			}
		}
	} else {
		log.Logger().Error("user tracker must be available in userTrackers map",
			zap.String("user", user.User))
		return fmt.Errorf("user tracker for %s is missing in userTrackers map", user.User)
	}

	group, err := m.getGroup(user)
	if err != nil {
		return err
	}
	groupTracker := m.groupTrackers[group]
	if groupTracker != nil {
		err := groupTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
		if err != nil {
			log.Logger().Error("Problem in decreasing the group resource usage",
				zap.String("user", user.User),
				zap.String("group", group),
				zap.String("queue path", queuePath),
				zap.String("application", applicationID),
				zap.String("resource", usage.String()),
				zap.Bool("removeApp", removeApp),
				zap.String("err message", err.Error()))
			return err
		}
		if removeApp {
			if m.isGroupRemovable(groupTracker) {
				delete(m.groupTrackers, group)
			}
		}
	} else {
		log.Logger().Error("appGroupTrackers tracker must be available in groupTrackers map",
			zap.String("appGroupTrackers", group))
		return fmt.Errorf("appGroupTrackers tracker for %s is missing in groupTrackers map", group)
	}
	return nil
}

func (m *Manager) GetUserResources(user security.UserGroup) *resources.Resource {
	return nil
}

func (m *Manager) GetGroupResources(group string) *resources.Resource {
	return nil
}

func (m *Manager) GetUsersResources() []*UserTracker {
	return nil
}

func (m *Manager) GetGroupsResources() []*GroupTracker {
	return nil
}

func (m *Manager) ensureGroupTrackerForApp(queuePath string, applicationID string, user security.UserGroup) error {
	userTracker := m.userTrackers[user.User]
	if !userTracker.hasGroupForApp(applicationID) {
		var groupTracker *GroupTracker
		group, err := m.getGroup(user)
		if err != nil {
			return err
		}
		if m.groupTrackers[group] == nil {
			log.Logger().Debug("Group tracker does not exist. Creating group tracker object and linking the same with application",
				zap.String("application", applicationID),
				zap.String("queue path", queuePath),
				zap.String("user", user.User),
				zap.String("group", group))
			groupTracker = newGroupTracker(group)
			m.groupTrackers[group] = groupTracker
		} else {
			log.Logger().Debug("Group tracker already exists and linking (reusing) the same with application",
				zap.String("application", applicationID),
				zap.String("queue path", queuePath),
				zap.String("user", user.User),
				zap.String("group", group))
			groupTracker = m.groupTrackers[group]
		}
		userTracker.setGroupForApp(applicationID, groupTracker)
	}
	return nil
}

// getGroup Based on the current limitations, username and group name is same. Groups[0] is always set and same as username.
// It would be changed in future based on user group resolution, limit configuration processing etc
func (m *Manager) getGroup(user security.UserGroup) (string, error) {
	if len(user.Groups) > 0 {
		return user.Groups[0], nil
	}
	return "", fmt.Errorf("group is not available in usergroup for user %s", user.User)
}

// cleaner Auto wakeup go routine to remove the user and group trackers based on applications being tracked upon, its root queueTracker usage etc
// nolint:unused
func (m *Manager) cleaner() {
	m.lock.Lock()
	defer m.lock.Unlock()
	for user, ut := range m.userTrackers {
		if m.isUserRemovable(ut) {
			delete(m.userTrackers, user)
		}
	}
	for group, gt := range m.groupTrackers {
		if m.isGroupRemovable(gt) {
			delete(m.groupTrackers, group)
		}
	}
}

func (m *Manager) isUserRemovable(ut *UserTracker) bool {
	if len(ut.getTrackedApplications()) == 0 && resources.IsZero(ut.queueTracker.resourceUsage) {
		return true
	}
	return false
}

func (m *Manager) isGroupRemovable(gt *GroupTracker) bool {
	if len(gt.getTrackedApplications()) == 0 && resources.IsZero(gt.queueTracker.resourceUsage) {
		return true
	}
	return false
}

// getUserTrackers only for tests
func (m *Manager) getUserTrackers() map[string]*UserTracker {
	return m.userTrackers
}

func (m *Manager) getGroupTrackers() map[string]*GroupTracker {
	return m.groupTrackers
}
