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

package scheduler

import (
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

// Represents Queue inside Scheduler
type SchedulingQueue struct {
	Name                string              // Fully qualified path for the queue
	CachedQueueInfo     *cache.QueueInfo    // link back to the queue in the cache
	QueueSortType       SortType            // How sub queues are sorted (parent queue only)

	// Private fields need protection
	applicationSortType SortType                          // How applications are sorted (leaf queue only)
	childrenQueues      map[string]*SchedulingQueue       // Only for direct children, parent queue only
	applications        map[string]*SchedulingApplication // only for leaf queue
	parent              *SchedulingQueue                  // link back to the parent in the scheduler
	allocating          *resources.Resource               // resource being allocated in the queue but not confirmed
	preempting          *resources.Resource               // resource considered for preemption in the queue
	pending             *resources.Resource               // pending resource for the apps in the queue

	sync.RWMutex
}

func newSchedulingQueueInfo(cacheQueueInfo *cache.QueueInfo, parent *SchedulingQueue) *SchedulingQueue {
	sq := &SchedulingQueue{
		Name:            cacheQueueInfo.GetQueuePath(),
		CachedQueueInfo: cacheQueueInfo,
		parent:          parent,
		childrenQueues:  make(map[string]*SchedulingQueue),
		applications:    make(map[string]*SchedulingApplication),
		allocating:      resources.NewResource(),
		preempting:      resources.NewResource(),
		pending:         resources.NewResource(),
	}

	// we can update the parent as we have a lock on the partition or the cluster when we get here
	if parent != nil {
		name := sq.Name[strings.LastIndex(sq.Name, cache.DOT)+1:]
		parent.childrenQueues[name] = sq
	}

	// update the properties
	sq.updateSchedulingQueueProperties(cacheQueueInfo.Properties)

	// initialise the child queues based what is in the cached copy
	for childName, childQueue := range cacheQueueInfo.GetCopyOfChildren() {
		newChildQueue := newSchedulingQueueInfo(childQueue, sq)
		sq.childrenQueues[childName] = newChildQueue
	}

	return sq
}

// Update the properties for the scheduling queue based on the current cached configuration
func (sq *SchedulingQueue) updateSchedulingQueueProperties(prop map[string]string) {
	// set the defaults, override with what is in the configured properties
	sq.applicationSortType = FifoSortPolicy
	sq.QueueSortType = FairSortPolicy
	// walk over all properties and process
	for key, value := range prop {
		if key == cache.ApplicationSortPolicy && value == "fair" {
			sq.applicationSortType = FairSortPolicy
		}
		// for now skip the rest just log them
		log.Logger().Debug("queue property skipped",
			zap.String("key", key),
			zap.String("value", value))
	}
}

// Update the queue properties and the child queues for the queue after a configuration update.
// New child queues will be added.
// Child queues that are removed from the configuration have been changed to a draining state and will not be scheduled.
// They are not removed until the queue is really empty, no action must be taken here.
func (sq *SchedulingQueue) updateSchedulingQueueInfo(info map[string]*cache.QueueInfo, parent *SchedulingQueue) {
	sq.Lock()
	defer sq.Unlock()
	// initialise the child queues based on what is in the cached copy
	for childName, childQueue := range info {
		child := sq.childrenQueues[childName]
		// create a new queue if it does not exist
		if child == nil {
			child = newSchedulingQueueInfo(childQueue, parent)
		} else {
			child.updateSchedulingQueueProperties(childQueue.Properties)
		}
		child.updateSchedulingQueueInfo(childQueue.GetCopyOfChildren(), child)
	}
}

func (sq *SchedulingQueue) GetPendingResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return sq.pending
}

// Update pending resource of this queue
func (sq *SchedulingQueue) incPendingResource(delta *resources.Resource) {
	// update the parent
	if sq.parent != nil {
		sq.parent.incPendingResource(delta)
	}
	// update this queue
	sq.Lock()
	defer sq.Unlock()
	sq.pending = resources.Add(sq.pending, delta)
}

// Remove pending resource of this queue
func (sq *SchedulingQueue) decPendingResource(delta *resources.Resource) {
	// update the parent
	if sq.parent != nil {
		sq.parent.decPendingResource(delta)
	}
	// update this queue
	sq.Lock()
	defer sq.Unlock()
	var err error
	sq.pending, err = resources.SubErrorNegative(sq.pending, delta)
	if err != nil {
		log.Logger().Warn("Pending resources went negative",
			zap.String("queueName", sq.CachedQueueInfo.Name),
			zap.Error(err))
	}
}

// Add scheduling app to the queue. All checks are assumed to have passed before we get here.
// No update of pending resource is needed as it should not have any requests yet.
// Replaces the existing application without further checks or updates.
func (sq *SchedulingQueue) addSchedulingApplication(app *SchedulingApplication) {
	sq.Lock()
	defer sq.Unlock()
	sq.applications[app.ApplicationInfo.ApplicationID] = app
}

// Remove scheduling app and pending resource of this queue and update the parent queues.
// The cache application is already removed just update what is tracked on the scheduler side.
// This is a lock free call all locks are taken when updating
func (sq *SchedulingQueue) removeSchedulingApplication(app *SchedulingApplication) {
	if !sq.removeSchedulingAppInternal(app.ApplicationInfo.ApplicationID) {
		log.Logger().Debug("Application not found while removing from queue",
			zap.String("queueName", sq.CachedQueueInfo.Name),
			zap.String("applicationID", app.ApplicationInfo.ApplicationID))
		return
	}
	// Update pending resource of the queues
	sq.decPendingResource(app.GetPendingResource())
}

// Remove the scheduling app from the list of tracked applications. Make sure that the app
// is assigned to this queue and not removed yet.
// If not found this call is a noop
func (sq *SchedulingQueue) removeSchedulingAppInternal(appID string) bool {
	sq.Lock()
	defer sq.Unlock()
	_, ok := sq.applications[appID]
	if ok {
		delete(sq.applications, appID)
	}
	return ok
}

// Get a copy of the child queues
func (sq *SchedulingQueue) GetCopyOfChildren() map[string]*SchedulingQueue {
	sq.RLock()
	defer sq.RUnlock()

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
	sq.Lock()
	defer sq.Unlock()

	delete(sq.childrenQueues, name)
}

// Remove the queue from the structure.
// Since nothing is allocated there shouldn't be anything referencing this queue any more.
// The real removal is removing the queue from the parent's child list, use read lock on the queue
func (sq *SchedulingQueue) removeQueue() bool {
	sq.RLock()
	defer sq.RUnlock()
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

// Return the preempting resources for the queue
func (sq *SchedulingQueue) getPreemptingResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return sq.preempting
}

// Increment the number of resource marked for preemption in the queue.
func (sq *SchedulingQueue) incPreemptingResource(newAlloc *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()
	sq.preempting.AddTo(newAlloc)
}

// Decrement the number of resource marked for preemption in the queue.
func (sq *SchedulingQueue) decPreemptingResource(newAlloc *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()
	var err error
	sq.preempting, err = resources.SubErrorNegative(sq.preempting, newAlloc)
	if err != nil {
		log.Logger().Warn("Preempting resources went negative",
			zap.String("queueName", sq.CachedQueueInfo.Name),
			zap.Error(err))
	}
}

// (Re)Set the preempting resources for the queue.
// This could be because they are pre-empted or the preemption was cancelled.
func (sq *SchedulingQueue) setPreemptingResource(newAlloc *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()
	sq.preempting = newAlloc
}

// Check if the user has access to the queue to submit an application.
// This will check the submit ACL and the admin ACL.
// Calls the cache queue which is doing the real work.
func (sq *SchedulingQueue) checkSubmitAccess(user security.UserGroup) bool {
	return sq.CachedQueueInfo.CheckSubmitAccess(user)
}

// Check if the user has access to the queue for admin actions.
// Calls the cache queue which is doing the real work.
func (sq *SchedulingQueue) checkAdminAccess(user security.UserGroup) bool {
	return sq.CachedQueueInfo.CheckAdminAccess(user)
}

// Return the allocated and allocating resources for this queue
func (sq *SchedulingQueue) getUnconfirmedAllocated() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return resources.Add(sq.allocating, sq.CachedQueueInfo.GetAllocatedResource())
}

// Return the allocating resources for this queue
func (sq *SchedulingQueue) getAllocatingResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return sq.allocating
}

// Increment the number of resource proposed for allocation in the queue.
// Decrement will be triggered when the allocation is confirmed in the cache.
func (sq *SchedulingQueue) incAllocatingResource(delta *resources.Resource) {
	if sq.parent != nil {
		sq.parent.incAllocatingResource(delta)
	}
	// update this queue
	sq.Lock()
	defer sq.Unlock()
	sq.allocating = resources.Add(sq.allocating, delta)
}

// Decrement the number of resources proposed for allocation in the queue.
// This is triggered when the cache queue is updated and the allocation is confirmed.
func (sq *SchedulingQueue) decAllocatingResource(delta *resources.Resource) {
	// update the parent
	if sq.parent != nil {
		sq.parent.decAllocatingResource(delta)
	}
	// update this queue
	sq.Lock()
	defer sq.Unlock()
	var err error
	sq.allocating, err = resources.SubErrorNegative(sq.allocating, delta)
	if err != nil {
		log.Logger().Warn("Allocating resources went negative on queue",
			zap.String("queueName", sq.CachedQueueInfo.Name),
			zap.Error(err))
	}
}

// Return a sorted copy of the applications in the queue. Applications are sorted using the
// sorting type of the queue.
// Only applications with a pending resource request are considered.
func (sq *SchedulingQueue) sortApplications() []*SchedulingApplication {
	sq.RLock()
	defer sq.RUnlock()

	if !sq.isLeafQueue() {
		return nil
	}
	// Create a copy of the applications with pending resources
	sortedApps := make([]*SchedulingApplication, 0)
	for _, v := range sq.applications {
		// Only look at app when pending-res > 0
		if resources.StrictlyGreaterThanZero(v.GetPendingResource()) {
			sortedApps = append(sortedApps, v)
		}
	}
	// Sort the applications
	sortApplications(sortedApps, sq.applicationSortType, sq.CachedQueueInfo.GuaranteedResource)

	return sortedApps
}

// Return a sorted copy of the queues for this parent queue.
// Only queues with a pending resource request are considered. The queues are sorted using the
// sorting type for the parent queue.
func (sq *SchedulingQueue) sortQueues() []*SchedulingQueue {
	sq.RLock()
	defer sq.RUnlock()

	if sq.isLeafQueue() {
		return nil
	}
	// Create a list of the queues with pending resources
	sortedQueues := make([]*SchedulingQueue, 0)
	// TODO Stopped queues are filtered out at a later stage should be here
	for _, child := range sq.childrenQueues {
		// Only look at queue when pending-res > 0
		if resources.StrictlyGreaterThanZero(child.GetPendingResource()) {
			sortedQueues = append(sortedQueues, child)
		}
	}
	// Sort the queues
	sortQueue(sortedQueues, sq.QueueSortType)

	return sortedQueues
}
