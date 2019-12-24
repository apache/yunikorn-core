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

package scheduler

import (
    "github.com/cloudera/yunikorn-core/pkg/cache"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/common/security"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "go.uber.org/zap"
    "strings"
    "sync"
)

// Represents Queue inside Scheduler
type SchedulingQueue struct {
    Name                string           // Fully qualified path for the queue
    CachedQueueInfo     *cache.QueueInfo // link back to the queue in the cache
    ApplicationSortType SortType         // How applications are sorted (leaf queue only)
    QueueSortType       SortType         // How sub queues are sorted (parent queue only)

    // Private fields need protection

    // How much resource allocated for this queue, this includes:
    // - Already allocated resources which is confirmed.
    // - Allocating resources which are waiting to be confirmed.
    // This will be used by sorting algorithm to sort queues while there're multiple thread
    // allocate resources
    mayAllocatedResource *resources.Resource
    childrenQueues       map[string]*SchedulingQueue       // Only for direct children, parent queue only
    applications         map[string]*SchedulingApplication // only for leaf queue
    parent               *SchedulingQueue                  // link back to the parent in the scheduler
    allocatingResource   *resources.Resource               // Allocating resource
    pendingResource      *resources.Resource               // Total pending resource
    lock                 sync.RWMutex
}

func NewSchedulingQueueInfo(cacheQueueInfo *cache.QueueInfo, parent *SchedulingQueue) *SchedulingQueue {
    sq := &SchedulingQueue{}
    sq.Name = cacheQueueInfo.GetQueuePath()
    sq.CachedQueueInfo = cacheQueueInfo
    sq.parent = parent
    sq.mayAllocatedResource = resources.NewResource()
    sq.childrenQueues = make(map[string]*SchedulingQueue)
    sq.applications = make(map[string]*SchedulingApplication)
    sq.pendingResource = resources.NewResource()

    // we can update the parent as we have a lock on the partition or the cluster when we get here
    if parent != nil {
        name := sq.Name[strings.LastIndex(sq.Name, cache.DOT)+1:]
        parent.childrenQueues[name] = sq
    }

    // update the properties
    sq.updateSchedulingQueueProperties(cacheQueueInfo.Properties)

    // initialise the child queues based what is in the cached copy
    for childName, childQueue := range cacheQueueInfo.GetCopyOfChildren() {
        newChildQueue := NewSchedulingQueueInfo(childQueue, sq)
        sq.childrenQueues[childName] = newChildQueue
    }

    return sq
}

func (queue *SchedulingQueue) tryAllocate(
    partitionTotalResource *resources.Resource,
    partitionContext *PartitionSchedulingContext,
    parentHeadroom *resources.Resource,
    parentQueueMaxLimit *resources.Resource) *SchedulingAllocation {
    queue.lock.Lock()
    defer queue.lock.Unlock()

    // skip stopped queues: running and draining queues are allowed
    if queue.isStopped() {
        log.Logger().Debug("skip non-running queue",
            zap.String("queueName", queue.Name))
        return nil
    }
    // Is it need any resource?
    if !resources.StrictlyGreaterThanZero(queue.pendingResource) {
        log.Logger().Debug("skip queue because it has no pending resource",
            zap.String("queueName", queue.Name))
        return nil
    }

    // Get queue max resource
    queueMaxLimit := queue.getMaxLimit(partitionTotalResource, parentQueueMaxLimit)

    // Get headroom
    newHeadroom := queue.getHeadroom(parentHeadroom, queueMaxLimit)

    var allocation *SchedulingAllocation = nil

    if queue.isLeafQueue() {
        sortedApps := queue.sortApplicationsFromLeafQueue()
        for _, app := range sortedApps {
            if allocation = app.tryAllocate(partitionContext, newHeadroom); allocation != nil {
                break
            }
        }
    } else {
        sortedChildren := queue.sortSubqueuesFromQueue()
        for _, queue := range sortedChildren {
            if allocation = queue.tryAllocate(partitionTotalResource, partitionContext, newHeadroom, queueMaxLimit); allocation != nil {
                break
            }
        }
    }

    if allocation != nil {
        queue.mayAllocatedResource = resources.Add(queue.mayAllocatedResource, allocation.SchedulingAsk.AllocatedResource)

        // Deduct pending resource of the queue
        newPending, err := resources.SubErrorNegative(queue.pendingResource, allocation.SchedulingAsk.AllocatedResource)
        if err != nil {
            log.Logger().Warn("Pending resources went negative",
                zap.Error(err))
        }
        queue.pendingResource = newPending
        return allocation
    }

    return nil
}


// Return a sorted copy of the applications in the queue.
// Only applications with a pending resource request are considered. The applications are sorted using the
// sorting type for the leaf queue they are in.
func (queue *SchedulingQueue) sortApplicationsFromLeafQueue() []*SchedulingApplication {
    // Create a copy of the applications with pending resources
    sortedApps := make([]*SchedulingApplication, 0)
    for _, v := range queue.applications {
        // Only look at app when pending-res > 0
        if resources.StrictlyGreaterThanZero(v.Requests.GetPendingResource()) {
            sortedApps = append(sortedApps, v)
        }
    }

    // Sort the applications
    SortApplications(sortedApps, queue.ApplicationSortType, queue.CachedQueueInfo.GuaranteedResource)

    return sortedApps
}


// Return a sorted copy of the queues for this parent queue.
// Only queues with a pending resource request are considered. The queues are sorted using the
// sorting type for the parent queue.
// Stopped queues will be filtered out at a later stage.
func (queue *SchedulingQueue) sortSubqueuesFromQueue() []*SchedulingQueue {
    // Create a list of the queues with pending resources
    sortedQueues := make([]*SchedulingQueue, 0)
    for _, child := range queue.childrenQueues {
        // Only look at queue when pending-res > 0
        if resources.StrictlyGreaterThanZero(child.GetPendingResource()) {
            sortedQueues = append(sortedQueues, child)
        }
    }

    // Sort the queues
    SortQueue(sortedQueues, queue.QueueSortType)

    return sortedQueues
}

func (sq * SchedulingQueue) GetPendingResource() *resources.Resource{
    sq.lock.RLock()
    defer sq.lock.RUnlock()

    return sq.pendingResource
}

// Update the properties for the scheduling queue based on the current cached configuration
func (sq *SchedulingQueue) updateSchedulingQueueProperties(prop map[string]string) {
    // set the defaults, override with what is in the configured properties
    sq.ApplicationSortType = FifoSortPolicy
    sq.QueueSortType = FairSortPolicy
    // walk over all properties and process
    if prop != nil {
        for key, value := range prop {
            if key == cache.ApplicationSortPolicy  && value == "fair" {
                sq.ApplicationSortType = FairSortPolicy
            }
            // for now skip the rest just log them
            log.Logger().Debug("queue property skipped",
                zap.String("key", key),
                zap.String("value", value))
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
        } else {
            child.updateSchedulingQueueProperties(childQueue.Properties)
        }
        child.updateSchedulingQueueInfo(childQueue.GetCopyOfChildren(), child)
    }
}

// Update pending resource of this queue
func (sq *SchedulingQueue) IncPendingResource(delta *resources.Resource) {
    // update the parent
    if sq.parent != nil {
        sq.parent.IncPendingResource(delta)
    }
    // update this queue
    sq.lock.Lock()
    defer sq.lock.Unlock()
    sq.pendingResource = resources.Add(sq.pendingResource, delta)
}

// Remove pending resource of this queue and its parents.
func (sq *SchedulingQueue) DecPendingResourceHierarchical(delta *resources.Resource) {
    // update the parent
    if sq.parent != nil {
        sq.parent.DecPendingResourceHierarchical(delta)
    }
    // update this queue
    sq.lock.Lock()
    defer sq.lock.Unlock()
    var err error
    sq.pendingResource, err = resources.SubErrorNegative(sq.pendingResource, delta)
    if err != nil {
        log.Logger().Warn("Pending resources went negative",
            zap.Error(err))
    }
}

// Add scheduling app to the queue
func (sq *SchedulingQueue) AddSchedulingApplication(app *SchedulingApplication) {
    sq.lock.Lock()
    defer sq.lock.Unlock()
    sq.applications[app.ApplicationInfo.ApplicationId] = app
}

// Remove scheduling app and pending resource of this queue and update the parent queues
func (sq *SchedulingQueue) RemoveSchedulingApplication(app *SchedulingApplication) {
    // lock without using defer as we want to release the read lock before we start walking up
    // the tree and make updates requiring a write lock.
    sq.lock.RLock()
    // make sure that the app is assigned to this queue and not removed yet, if not found return
    if _, ok := sq.applications[app.ApplicationInfo.ApplicationId]; !ok {
        sq.lock.RUnlock()
        return
    }
    sq.lock.RUnlock()
    // Update pending resource of the parent queues
    totalPending := app.Requests.GetPendingResource()
    if !resources.IsZero(totalPending) {
        sq.parent.DecPendingResourceHierarchical(totalPending)
    }
    sq.lock.Lock()
    defer sq.lock.Unlock()
    var err error
    sq.pendingResource, err = resources.SubErrorNegative(sq.pendingResource, totalPending)
    if err != nil {
        log.Logger().Warn("Removing application made pending resources negative",
            zap.Error(err))
    }
    delete(sq.applications, app.ApplicationInfo.ApplicationId)
}

// Get a copy of the child queues
func (sq *SchedulingQueue) GetCopyOfChildren() map[string]*SchedulingQueue {
    sq.lock.RLock()
    defer sq.lock.RUnlock()

    // add self
    children := make(map[string]*SchedulingQueue)
    for k, v := range sq.childrenQueues {
        children[k] = v
    }
    return children
}

// Remove a child queue from this queue.
// No checks are performed: if the child has been removed already it is a noop.
// This may only be called by the queue removal itself on the registered parent.
// Queue removal is always a bottom up action: leafs first then the parent.
func (sq *SchedulingQueue) removeChildQueue(name string) {
    sq.lock.Lock()
    defer sq.lock.Unlock()

    delete(sq.childrenQueues, name)
}

// Remove the queue from the structure.
// Since nothing is allocated there shouldn't be anything referencing this queue any more.
// The real removal is removing the queue from the parent's child list, use read lock on the queue
func (sq *SchedulingQueue) RemoveQueue() bool {
    sq.lock.RLock()
    defer sq.lock.RUnlock()
    // cannot remove a managed queue that is running
    if sq.isManaged() && sq.isRunning() {
        return false
    }
    // cannot remove a queue that has children or applications assigned
    if len(sq.childrenQueues) > 0 || len(sq.applications) > 0 {
        return false
    }
    // root is always managed and is the only queue with a nil parent: no need to guard
    sq.parent.removeChildQueue(sq.Name)
    return true
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

// Is this queue managed or not.
// link back to the underlying queue object to prevent out of sync types
func (sq *SchedulingQueue) isManaged() bool {
    return sq.CachedQueueInfo.IsManaged()
}

func (sq *SchedulingQueue) isRoot() bool {
    return sq.parent == nil
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

// Check if the user has access to the queue to submit an application.
// This will check the submit ACL and the admin ACL.
// Calls the cache queue which is doing the real work.
func (sq *SchedulingQueue) CheckSubmitAccess(user security.UserGroup) bool {
    return sq.CachedQueueInfo.CheckSubmitAccess(user)
}

// Check if the user has access to the queue for admin actions.
// Calls the cache queue which is doing the real work.
func (sq *SchedulingQueue) CheckAdminAccess(user security.UserGroup) bool {
    return sq.CachedQueueInfo.CheckAdminAccess(user)
}

func (sq* SchedulingQueue) getHeadroom(parentHeadroom *resources.Resource, queueMaxLimit *resources.Resource) *resources.Resource {
    // new headroom for this queue
    if nil != parentHeadroom {
        return resources.ComponentWiseMin(resources.Sub(queueMaxLimit, sq.mayAllocatedResource), parentHeadroom)
    }
    return resources.Sub(queueMaxLimit, sq.mayAllocatedResource)
}

func (sq* SchedulingQueue) getMaxLimit(partitionTotalResource *resources.Resource, parentMaxLimit *resources.Resource) *resources.Resource {
    if sq.isRoot() {
        return partitionTotalResource
    }

    // Get max resource of parent queue
    maxResource := sq.CachedQueueInfo.MaxResource
    if maxResource == nil {
        maxResource = parentMaxLimit
    }
    maxResource = resources.ComponentWiseMin(maxResource, partitionTotalResource)
    return maxResource
}

func (sq* SchedulingQueue) GetMayAllocatedResource() *resources.Resource {
    sq.lock.RLock()
    defer sq.lock.RUnlock()

    return sq.mayAllocatedResource
}

// Update may allocated resource from this queue
func (sq *SchedulingQueue) DecMayAllocatedResource(alloc *resources.Resource) {
    // update the parent
    if sq.parent != nil {
        sq.parent.DecMayAllocatedResource(alloc)
    }
    // update this queue
    sq.lock.Lock()
    defer sq.lock.Unlock()
    sq.mayAllocatedResource = resources.Sub(sq.mayAllocatedResource, alloc)
}