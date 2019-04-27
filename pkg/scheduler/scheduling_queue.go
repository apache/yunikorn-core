/*
Copyright 2019 The Unity Scheduler Authors

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

package scheduler

import (
    "github.com/golang/glog"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/cache"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "sync"
)

// Represents Queue inside Scheduler
type SchedulingQueue struct {
    Name               string              // Fully qualified path for the queue
    CachedQueueInfo    *cache.QueueInfo    // link back to the queue in the cache
    ProposingResource  *resources.Resource // How much resource added for proposing, this is used by queue sort when do candidate selection
    allocatingResource *resources.Resource // Allocating resource
    PartitionResource  *resources.Resource // For fairness calculation
    Parent             *SchedulingQueue    // link back to the parent in the scheduler
    pendingResource    *resources.Resource // Total pending resource

    ApplicationSortType  SortType            // How applications are sorted (leaf queue only)
    QueueSortType        SortType            // How sub queues are sorted (parent queue only)

    childrenQueues map[string]*SchedulingQueue // Only for direct children, parent queue only
    applications   map[string]*SchedulingApplication // only for leaf queue

    lock sync.RWMutex
}

func NewSchedulingQueueInfo(cacheQueueInfo *cache.QueueInfo, parent *SchedulingQueue) *SchedulingQueue {
    sq := &SchedulingQueue{}
    sq.Name = cacheQueueInfo.GetQueuePath()
    sq.CachedQueueInfo = cacheQueueInfo
    sq.Parent = parent
    sq.ProposingResource = resources.NewResource()
    sq.childrenQueues = make(map[string]*SchedulingQueue)
    sq.applications = make(map[string]*SchedulingApplication)
    sq.pendingResource = resources.NewResource()

    // update the properties
    sq.updateSchedulingQueueProperties(cacheQueueInfo.Properties)

    // initialise the child queues based what is in the cached copy
    for childName, childQueue := range cacheQueueInfo.GetCopyOfChildren() {
        newChildQueue := NewSchedulingQueueInfo(childQueue, sq)
        sq.childrenQueues[childName] = newChildQueue
    }

    return sq
}

func (m* SchedulingQueue) GetPendingResource() *resources.Resource{
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.pendingResource
}

// Update the properties for the scheduling queue based on the current cached configuration
func (sq *SchedulingQueue) updateSchedulingQueueProperties(prop map[string]string) {
    // set the defaults, override with what is in the configured properties
    sq.ApplicationSortType = FIFO_SORT_POLICY
    sq.QueueSortType = FAIR_SORT_POLICY
    // walk over all properties and process
    if prop != nil {
        for key, value := range prop {
            if key == cache.ApplicationSortPolicy  && value == "fair" {
                sq.ApplicationSortType = FAIR_SORT_POLICY
            }
            // for now skip the rest just log them
            glog.V(5).Infof("queue property skip: %s, %s", key, value)
        }
    }
}

// Update the queue properties and the child queues for the queue after a configuration update.
// New child queues will be added.
// Child queues that are removed from the configuration have been changed to a draining state and will not be scheduled.
// They are not removed until the queue is really empty, no action must be taken here.
func (sq *SchedulingQueue) updateSchedulingQueueInfo(info map[string]*cache.QueueInfo, parent *SchedulingQueue) {
    sq.lock.Lock()
    defer sq.lock.Unlock()
    // initialise the child queues based on what is in the cached copy
    for childName, childQueue := range info {
        child := sq.childrenQueues[childName]
        // create a new queue if it does not exist
        if child == nil {
            child = NewSchedulingQueueInfo(childQueue, parent)
            parent.childrenQueues[childName] = child
        } else {
            child.updateSchedulingQueueProperties(childQueue.Properties)
        }
        child.updateSchedulingQueueInfo(childQueue.GetCopyOfChildren(), child)
    }
}

// Update pending resource of this queue
func (sq *SchedulingQueue) IncPendingResource(delta *resources.Resource) {
    sq.lock.Lock()
    defer sq.lock.Unlock()

    sq.pendingResource = resources.Add(sq.pendingResource, delta)
}

// Remove pending resource of this queue
func (sq *SchedulingQueue) DecPendingResource(delta *resources.Resource) {
    sq.lock.Lock()
    defer sq.lock.Unlock()

    sq.pendingResource = resources.Sub(sq.GetPendingResource(), delta)
}

func (sq *SchedulingQueue) AddSchedulingApplication(app *SchedulingApplication) {
    sq.lock.Lock()
    defer sq.lock.Unlock()

    sq.applications[app.ApplicationInfo.ApplicationId] = app
}

func (sq *SchedulingQueue) RemoveSchedulingApplication(app *SchedulingApplication) {
    sq.lock.Lock()
    defer sq.lock.Unlock()

    delete(sq.applications, app.ApplicationInfo.ApplicationId)
}

// Create a flat queue structure at this level
func (sq *SchedulingQueue) GetFlatChildrenQueues(allQueues map[string]*SchedulingQueue) {
    sq.lock.RLock()
    defer sq.lock.RUnlock()

    // add self
    allQueues[sq.Name] = sq

    for _, child := range sq.childrenQueues {
        allQueues[child.Name] = child
        child.GetFlatChildrenQueues(allQueues)
    }
}

// Is this queue a leaf or not (i.e parent)
// link back to the underlying queue object to prevent out of sync types
func (sq *SchedulingQueue) isLeafQueue() bool {
    return sq.CachedQueueInfo.IsLeafQueue()
}

// Queue status methods reflecting the underlying queue object state
// link back to the underlying queue object to prevent out of sync states
func (sq *SchedulingQueue) isRunning() bool {
    return sq.CachedQueueInfo.IsRunning()
}

func (sq *SchedulingQueue) isDraining() bool {
    return sq.CachedQueueInfo.IsDraining()
}

func (sq *SchedulingQueue) isStopped() bool {
    return sq.CachedQueueInfo.IsStopped()
}

func (sq *SchedulingQueue) isRoot() bool {
    return sq.Parent == nil
}

func (sq *SchedulingQueue) GetAllocatingResource() *resources.Resource {
    sq.lock.RLock()
    defer sq.lock.RUnlock()

    return sq.allocatingResource
}

func (sq *SchedulingQueue) IncAllocatingResource(newAlloc *resources.Resource) {
    sq.lock.Lock()
    defer sq.lock.Unlock()

    sq.allocatingResource = resources.Add(sq.allocatingResource, newAlloc)
}

func (sq *SchedulingQueue) SetAllocatingResource(newAlloc *resources.Resource) {
    sq.lock.Lock()
    defer sq.lock.Unlock()

    sq.allocatingResource = newAlloc
}