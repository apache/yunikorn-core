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

package objects

import (
	"sync"
)

type UserGroupManager struct {
	queue            *Queue
	users            map[string]*User
	groups           map[string]*Group
	allUsersAllowed  bool
	allGroupsAllowed bool

	sync.RWMutex
}

func userGroupManager(queue *Queue) *UserGroupManager {
	return &UserGroupManager{
		queue:            queue,
		users:            make(map[string]*User),
		groups:           make(map[string]*Group),
		allUsersAllowed:  false,
		allGroupsAllowed: false,
	}
}

func NewUserGroupManager(queue *Queue) *UserGroupManager {
	u := userGroupManager(queue)
	limits := queue.GetConfigLimits()

	// Traverse each limit from queue config
	if len(limits) > 0 {
		for _, limit := range limits {
			// Add user
			for _, user := range limit.Users {
				newUser := NewUser(user)
				newUser.SetMaxApplications(int32(limit.MaxApplications))
				u.AddUserIfAbsent(newUser)
				if user == AllUser {
					u.allUsersAllowed = true
					break
				}
			}

			// Add group
			for _, group := range limit.Groups {
				newGroup := NewGroup(group)
				newGroup.SetMaxApplications(int32(limit.MaxApplications))
				u.AddGroupIfAbsent(newGroup)
				if group == AllGroup {
					u.allGroupsAllowed = true
					break
				}
			}
		}
	}
	return u
}

func (ugm *UserGroupManager) GetUsers() map[string]*User {
	ugm.RLock()
	defer ugm.RUnlock()
	return ugm.users
}

func (ugm *UserGroupManager) GetUser(user string) *User {
	ugm.RLock()
	defer ugm.RUnlock()
	return ugm.users[user]
}

func (ugm *UserGroupManager) AddUserIfAbsent(user *User) {
	ugm.Lock()
	defer ugm.Unlock()
	if _, ok := ugm.users[user.GetName()]; !ok {
		ugm.users[user.GetName()] = user
	}
}

func (ugm *UserGroupManager) GetGroup(group string) *Group {
	ugm.RLock()
	defer ugm.RUnlock()
	return ugm.groups[group]
}

func (ugm *UserGroupManager) GetGroups() map[string]*Group {
	ugm.RLock()
	defer ugm.RUnlock()
	return ugm.groups
}

func (ugm *UserGroupManager) AddGroupIfAbsent(group *Group) {
	ugm.Lock()
	defer ugm.Unlock()
	if _, ok := ugm.groups[group.GetName()]; !ok {
		ugm.groups[group.GetName()] = group
	}
}

func (ugm *UserGroupManager) IsAllUserAllowed() bool {
	return ugm.allUsersAllowed
}

func (ugm *UserGroupManager) IsAllGroupAllowed() bool {
	return ugm.allGroupsAllowed
}
