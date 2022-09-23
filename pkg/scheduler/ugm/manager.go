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
	users  map[string]*UserTracker
	groups map[string]*GroupTracker
	sync.RWMutex
}

func NewManager() *Manager {
	manager := &Manager{
		users:  make(map[string]*UserTracker),
		groups: make(map[string]*GroupTracker),
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
	if m.users[user.User] == nil {
		userTracker = NewUserTracker(user)
		m.users[user.User] = userTracker
	} else {
		userTracker = m.users[user.User]
	}
	err := userTracker.increaseTrackedResource(queuePath, applicationID, usage)
	if err != nil {
		return err
	}
	m.checkAppGroup(applicationID, user)
	groupTracker := m.groups[m.getGroup(user)]
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
	userTracker := m.users[user.User]
	if userTracker != nil {
		err := userTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
		if err != nil {
			return err
		}
		if removeApp {
			if m.isUserRemovable(userTracker) {
				delete(m.users, user.User)
			}
		}
	} else {
		log.Logger().Info("user tracker must be available in users map",
			zap.String("user", user.User))
		return fmt.Errorf("user tracker for %s is missing in users map", user.User)
	}

	groupTracker := m.groups[m.getGroup(user)]
	if groupTracker != nil {
		err := groupTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
		if err != nil {
			return err
		}
		if removeApp {
			if m.isGroupRemovable(groupTracker) {
				delete(m.groups, m.getGroup(user))
			}
		}
	} else {
		log.Logger().Info("group tracker must be available in groups map",
			zap.String("group", m.getGroup(user)))
		return fmt.Errorf("group tracker for %s is missing in groups map", m.getGroup(user))
	}
	return nil
}

func (m *Manager) checkAppGroup(applicationID string, user *security.UserGroup) {
	userTracker := m.users[user.User]
	if !userTracker.hasGroupForApp(applicationID) {
		var groupTracker *GroupTracker
		group := m.getGroup(user)
		if m.groups[group] == nil {
			groupTracker = NewGroupTracker(group)
			m.groups[group] = groupTracker
		} else {
			groupTracker = m.groups[group]
		}
		userTracker.setGroupForApp(applicationID, groupTracker)
	}
}

// getGroup Based on the current limitations, group name and username is same. hence, using username as group name.
// It would be changed in future based on user group resolution, limit configuration processing etc
func (m *Manager) getGroup(user *security.UserGroup) string {
	return user.User
}

// cleaner Auto wakeup go routine to remove the user and group based on applications being tracked upon, its root queue usage etc
// todo
func (m *Manager) cleaner() {
	m.Lock()
	defer m.Unlock()
	for user, ut := range m.users {
		if m.isUserRemovable(ut) {
			delete(m.users, user)
		}
	}
	for group, gt := range m.groups {
		if m.isGroupRemovable(gt) {
			delete(m.groups, group)
		}
	}
}

func (m *Manager) isUserRemovable(ut *UserTracker) bool {
	if len(ut.getTrackedApplications()) == 0 && resources.IsZero(ut.queue.resourceUsage) {
		return true
	}
	return false
}

func (m *Manager) isGroupRemovable(gt *GroupTracker) bool {
	if len(gt.getTrackedApplications()) == 0 && resources.IsZero(gt.queue.resourceUsage) {
		return true
	}
	return false
}

// only for tests
func (m *Manager) getUsers() map[string]*UserTracker {
	return m.users
}

func (m *Manager) getGroups() map[string]*GroupTracker {
	return m.groups
}
