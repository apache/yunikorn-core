/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"strings"
	"sync"
	"time"

	"github.com/looplab/fsm"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
)

/* Related to applications */
type ApplicationInfo struct {
	ApplicationID  string
	Partition      string
	QueueName      string
	SubmissionTime int64

	// Private fields need protection
	user              security.UserGroup         // owner of the application
	tags              map[string]string          // application tags used in scheduling
	leafQueue         *QueueInfo                 // link to the leaf queue
	allocatedResource *resources.Resource        // total allocated resources
	allocations       map[string]*AllocationInfo // list of all allocations
	stateMachine      *fsm.FSM                   // application state machine
	lock              sync.RWMutex
}

// Create a new application
func NewApplicationInfo(appID, partition, queueName string, ugi security.UserGroup, tags map[string]string) *ApplicationInfo {
	return &ApplicationInfo{
		ApplicationID:     appID,
		Partition:         partition,
		QueueName:         queueName,
		SubmissionTime:    time.Now().UnixNano(),
		tags:              tags,
		user:              ugi,
		allocatedResource: resources.NewResource(),
		allocations:       make(map[string]*AllocationInfo),
		stateMachine:      newAppState(),
	}
}

// Return the current allocations for the application.
func (ai *ApplicationInfo) GetAllAllocations() []*AllocationInfo {
	ai.lock.RLock()
	defer ai.lock.RUnlock()

	var allocations []*AllocationInfo
	for _, alloc := range ai.allocations {
		allocations = append(allocations, alloc)
	}
	return allocations
}

// Return the current state for the application.
// The state machine handles the locking.
func (ai *ApplicationInfo) GetApplicationState() string {
	return ai.stateMachine.Current()
}

// Handle the state event for the application.
// The state machine handles the locking.
func (ai *ApplicationInfo) HandleApplicationEvent(event ApplicationEvent) error {
	err := ai.stateMachine.Event(event.String(), ai.ApplicationID)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() == "no transition" {
		return nil
	}
	return err
}

// Return the total allocated resources for the application.
func (ai *ApplicationInfo) GetAllocatedResource() *resources.Resource {
	ai.lock.RLock()
	defer ai.lock.RUnlock()

	return ai.allocatedResource
}

// Set the leaf queue the application runs in. Update the queue name also to match as this might be different from the
// queue that was given when submitting the application.
func (ai *ApplicationInfo) SetQueue(leaf *QueueInfo) {
	ai.lock.Lock()
	defer ai.lock.Unlock()

	ai.leafQueue = leaf
	ai.QueueName = leaf.GetQueuePath()
}

// Add a new allocation to the application
func (ai *ApplicationInfo) addAllocation(info *AllocationInfo) {
	ai.lock.Lock()
	defer ai.lock.Unlock()

	ai.allocations[info.AllocationProto.UUID] = info
	ai.allocatedResource = resources.Add(ai.allocatedResource, info.AllocatedResource)
}

// Remove a specific allocation from the application.
// Return the allocation that was removed.
func (ai *ApplicationInfo) removeAllocation(uuid string) *AllocationInfo {
	ai.lock.Lock()
	defer ai.lock.Unlock()

	alloc := ai.allocations[uuid]

	if alloc != nil {
		// When app has the allocation, update map, and update allocated resource of the app
		ai.allocatedResource = resources.Sub(ai.allocatedResource, alloc.AllocatedResource)
		delete(ai.allocations, uuid)
		return alloc
	}

	return nil
}

// Remove all allocations from the application.
// All allocations that have been removed are returned.
func (ai *ApplicationInfo) removeAllAllocations() []*AllocationInfo {
	allocationsToRelease := make([]*AllocationInfo, 0)

	ai.lock.Lock()
	defer ai.lock.Unlock()

	for _, alloc := range ai.allocations {
		allocationsToRelease = append(allocationsToRelease, alloc)
	}
	// cleanup allocated resource for app
	ai.allocatedResource = resources.NewResource()
	ai.allocations = make(map[string]*AllocationInfo)

	return allocationsToRelease
}

// get a copy of the user details for the application
func (ai *ApplicationInfo) GetUser() security.UserGroup {
	return ai.user
}

// Get a tag from the application
// Note: Tags are not case sensitive
func (ai *ApplicationInfo) GetTag(tag string) string {
	tagVal := ""
	for key, val := range ai.tags {
		if strings.EqualFold(key, tag) {
			tagVal = val
			break
		}
	}
	return tagVal
}
