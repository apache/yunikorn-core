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

type Manager struct {
	userTrackers  map[string]*UserTracker
	groupTrackers map[string]*GroupTracker
	sync.RWMutex
}

func NewManager() *Manager {
	manager := &Manager{
		userTrackers:  make(map[string]*UserTracker),
		groupTrackers: make(map[string]*GroupTracker),
	}
	return manager
}
func (m *Manager) IncreaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource, user *security.UserGroup) error {
	if queuePath == "" || applicationID == "" || usage == nil || user == nil {
		return fmt.Errorf("mandatory parameters are missing. queuepath: %s, application id: %s, resource usage: %s, user: %s",
			queuePath, applicationID, usage.String(), user.User)
	}
	m.Lock()
	defer m.Unlock()
	var userTracker *UserTracker
	if m.userTrackers[user.User] == nil {
		userTracker = NewUserTracker(user)
		m.userTrackers[user.User] = userTracker
	} else {
		userTracker = m.userTrackers[user.User]
	}
	err := userTracker.increaseTrackedResource(queuePath, applicationID, usage)
	if err != nil {
		return err
	}
	m.ensureGroupTrackerForApp(applicationID, user)
	groupTracker := m.groupTrackers[m.getGroup(user)]
	if groupTracker != nil {
		err = groupTracker.increaseTrackedResource(queuePath, applicationID, usage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) DecreaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource, user *security.UserGroup, removeApp bool) error {
	if queuePath == "" || applicationID == "" || usage == nil || user == nil {
		return fmt.Errorf("mandatory parameters are missing. queuepath: %s, application id: %s, resource usage: %s, user: %s",
			queuePath, applicationID, usage.String(), user.User)
	}
	m.Lock()
	defer m.Unlock()
	userTracker := m.userTrackers[user.User]
	if userTracker != nil {
		err := userTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
		if err != nil {
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

	groupTracker := m.groupTrackers[m.getGroup(user)]
	if groupTracker != nil {
		err := groupTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
		if err != nil {
			return err
		}
		if removeApp {
			if m.isGroupRemovable(groupTracker) {
				delete(m.groupTrackers, m.getGroup(user))
			}
		}
	} else {
		log.Logger().Error("appGroupTrackers tracker must be available in groupTrackers map",
			zap.String("appGroupTrackers", m.getGroup(user)))
		return fmt.Errorf("appGroupTrackers tracker for %s is missing in groupTrackers map", m.getGroup(user))
	}
	return nil
}

func (m *Manager) ensureGroupTrackerForApp(applicationID string, user *security.UserGroup) {
	userTracker := m.userTrackers[user.User]
	if !userTracker.hasGroupForApp(applicationID) {
		var groupTracker *GroupTracker
		group := m.getGroup(user)
		if m.groupTrackers[group] == nil {
			groupTracker = NewGroupTracker(group)
			m.groupTrackers[group] = groupTracker
		} else {
			groupTracker = m.groupTrackers[group]
		}
		userTracker.setGroupForApp(applicationID, groupTracker)
	}
}

// getGroup Based on the current limitations, group name and username is same. hence, using username as group name.
// It would be changed in future based on user group resolution, limit configuration processing etc
func (m *Manager) getGroup(user *security.UserGroup) string {
	return user.User
}

// cleaner Auto wakeup go routine to remove the user and group trackers based on applications being tracked upon, its root queueTracker usage etc
// todo
func (m *Manager) cleaner() {
	m.Lock()
	defer m.Unlock()
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

// only for tests
func (m *Manager) getUserTrackers() map[string]*UserTracker {
	return m.userTrackers
}

func (m *Manager) getGroupTrackers() map[string]*GroupTracker {
	return m.groupTrackers
}
