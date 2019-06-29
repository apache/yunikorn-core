/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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
    "github.com/looplab/fsm"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "sync"
    "time"
)

/* Related to applications */
type ApplicationInfo struct {
    ApplicationId  string
    Partition      string
    QueueName      string
    SubmissionTime int64

    // Private fields need protection
    leafQueue         *QueueInfo                 // link to the leaf queue
    allocatedResource *resources.Resource        // total allocated resources
    allocations       map[string]*AllocationInfo // list of all allocations
    stateMachine      *fsm.FSM                   // application state machine
    lock sync.RWMutex
}

// Create a new application
func NewApplicationInfo(appId string, partition, queueName string) *ApplicationInfo {
    ai := &ApplicationInfo{ApplicationId: appId}
    ai.allocatedResource = resources.NewResource()
    ai.allocations = make(map[string]*AllocationInfo)
    ai.Partition = partition
    ai.QueueName = queueName
    ai.SubmissionTime = time.Now().UnixNano()
    ai.stateMachine = newAppState()
    return ai
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
    err := ai.stateMachine.Event(event.String(), ai.ApplicationId);
    // handle the same state transition not nil error (limit of fsm).
    if err != nil  && err.Error() == "no transition" {
        return nil
    }
    return err
}

// Return the total allocated resources for the application.
func (ai * ApplicationInfo) GetAllocatedResource() *resources.Resource {
    ai.lock.RLock()
    defer ai.lock.RUnlock()

    return ai.allocatedResource
}

// Set the leaf queue the application runs in. Update the queue name also to match as this might be different from the
// queue that was given when submitting the application.
func (ai *ApplicationInfo) setQueue(leaf *QueueInfo) {
    ai.lock.Lock()
    defer ai.lock.Unlock()

    ai.leafQueue = leaf
    ai.QueueName = leaf.GetQueuePath()
}

// Add a new allocation to the application
func (ai *ApplicationInfo) addAllocation(info *AllocationInfo) {
    ai.lock.Lock()
    defer ai.lock.Unlock()

    ai.allocations[info.AllocationProto.Uuid] = info
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
