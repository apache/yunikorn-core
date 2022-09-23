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
)

type UserTracker struct {
	userName string
	group    map[string]*GroupTracker
	queue    *QueueTracker

	sync.RWMutex
}

func NewUserTracker(user *security.UserGroup) *UserTracker {
	queueTracker := NewQueueTracker("root")
	userTracker := &UserTracker{
		userName: user.User,
		group:    make(map[string]*GroupTracker),
		queue:    queueTracker,
	}
	return userTracker
}

func (ut *UserTracker) increaseTrackedResource(queuePath, applicationID string, usage *resources.Resource) error {
	return ut.queue.increaseTrackedResource(queuePath, applicationID, usage)
}

func (ut *UserTracker) decreaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, removeApp bool) error {
	if removeApp {
		ut.Lock()
		defer ut.Unlock()
		delete(ut.group, applicationID)
	}
	return ut.queue.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
}

func (ut *UserTracker) hasGroupForApp(applicationID string) bool {
	ut.RLock()
	defer ut.RUnlock()
	return ut.group[applicationID] != nil
}

func (ut *UserTracker) setGroupForApp(applicationID string, groupTrack *GroupTracker) {
	ut.Lock()
	defer ut.Unlock()
	if ut.group[applicationID] == nil {
		ut.group[applicationID] = groupTrack
	}
}

func (ut *UserTracker) getTrackedApplications() map[string]*GroupTracker {
	ut.RLock()
	defer ut.RUnlock()
	return ut.group
}

// GetResource only for tests
func (ut *UserTracker) GetResource() map[string]*resources.Resource {
	resources := make(map[string]*resources.Resource)
	return ut.internalGetResource("root", "root", ut.queue, resources)
}

func (ut *UserTracker) internalGetResource(parentQueuePath string, queueName string, queueTracker *QueueTracker, resources map[string]*resources.Resource) map[string]*resources.Resource {
	fullQueuePath := ""
	if queueName == "root" {
		fullQueuePath = parentQueuePath
	} else {
		fullQueuePath = parentQueuePath + "." + queueTracker.queueName
	}
	resources[fullQueuePath] = queueTracker.resourceUsage
	if len(queueTracker.childQueues) > 0 {
		for childQueueName, childQueueTracker := range queueTracker.childQueues {
			ut.internalGetResource(fullQueuePath, childQueueName, childQueueTracker, resources)
		}
	}
	return resources
}
