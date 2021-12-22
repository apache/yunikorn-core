package usergroupmanagement

import (
	"sync"

	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects"
)

type UserGroupManager struct {
	queue	*objects.Queue
	users	map[string]*User
	groups	map[string]*Group

	sync.RWMutex
}

func userGroupManager(queue *objects.Queue) *UserGroupManager {
	return &UserGroupManager{
		queue:	queue,
		users:	make(map[string]*User),
		groups:	make(map[string]*Group),
	}
}

func (ugm *UserGroupManager) NewUsersManager(queue *objects.Queue) *UserGroupManager {
	u  := userGroupManager(queue)
	limits := queue.GetConfigLimits()

	// Traverse each limit from queue config
	if len(limits) > 0 {
		for _, limit := range limits {
			// Add user
			for _, user := range limit.Users {
				newUser := NewUser(user)
				ugm.addUserIfAbsent(newUser)
			}

			// Add group
			for _, group := range limit.Groups {
				newGroup := NewGroup(group)
				ugm.addGroupIfAbsent(newGroup)
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

func (ugm *UserGroupManager) addUserIfAbsent(user *User) {
	ugm.Lock()
	defer ugm.Unlock()
	if _, ok := ugm.users[user.GetName()]; ! ok {
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

func (ugm *UserGroupManager) addGroupIfAbsent(group *Group) {
	ugm.Lock()
	defer ugm.Unlock()
	if _, ok := ugm.groups[group.GetName()]; ! ok {
		ugm.groups[group.GetName()] = group
	}
}