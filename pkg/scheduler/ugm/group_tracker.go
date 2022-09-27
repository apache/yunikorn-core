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
)

type GroupTracker struct {
	groupName    string
	applications map[string]bool
	queueTracker *QueueTracker

	sync.RWMutex
}

func NewGroupTracker(group string) *GroupTracker {
	queueTracker := NewQueueTracker("root")
	groupTracker := &GroupTracker{
		groupName:    group,
		applications: make(map[string]bool),
		queueTracker: queueTracker,
	}
	return groupTracker
}

func (gt *GroupTracker) increaseTrackedResource(queuePath, applicationID string, usage *resources.Resource) error {
	gt.Lock()
	defer gt.Unlock()
	gt.applications[applicationID] = true
	return gt.queueTracker.increaseTrackedResource(queuePath, applicationID, usage)
}

func (gt *GroupTracker) decreaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, removeApp bool) error {
	if removeApp {
		gt.Lock()
		defer gt.Unlock()
		delete(gt.applications, applicationID)
	}
	return gt.queueTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
}

func (gt *GroupTracker) getTrackedApplications() map[string]bool {
	gt.RLock()
	defer gt.RUnlock()
	return gt.applications
}

// GetResource only for tests
func (gt *GroupTracker) GetResource() map[string]*resources.Resource {
	resources := make(map[string]*resources.Resource)
	return gt.internalGetResource("root", "root", gt.queueTracker, resources)
}

func (gt *GroupTracker) internalGetResource(parentQueuePath string, queueName string, queueTracker *QueueTracker, resources map[string]*resources.Resource) map[string]*resources.Resource {
	fullQueuePath := ""
	if queueName == "root" {
		fullQueuePath = parentQueuePath
	} else {
		fullQueuePath = parentQueuePath + "." + queueTracker.queueName
	}
	resources[fullQueuePath] = queueTracker.resourceUsage
	if len(queueTracker.childQueueTrackers) > 0 {
		for childQueueName, childQueueTracker := range queueTracker.childQueueTrackers {
			gt.internalGetResource(fullQueuePath, childQueueName, childQueueTracker, resources)
		}
	}
	return resources
}
