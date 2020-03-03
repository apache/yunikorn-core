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
	Name      string           // Fully qualified path for the queue
	QueueInfo *cache.QueueInfo // link back to the queue in the cache

	// Private fields need protection
	sortType       SortType                          // How applications (leaf) or queues (parents) are sorted
	childrenQueues map[string]*SchedulingQueue       // Only for direct children, parent queue only
	applications   map[string]*SchedulingApplication // only for leaf queue
	reservedApps   map[string]int                    // applications reserved within this queue, with reservation count
	parent         *SchedulingQueue                  // link back to the parent in the scheduler
	allocating     *resources.Resource               // resource being allocated in the queue but not confirmed
	preempting     *resources.Resource               // resource considered for preemption in the queue
	pending        *resources.Resource               // pending resource for the apps in the queue

	sync.RWMutex
}

func newSchedulingQueueInfo(cacheQueueInfo *cache.QueueInfo, parent *SchedulingQueue) *SchedulingQueue {
	sq := &SchedulingQueue{
		Name:           cacheQueueInfo.GetQueuePath(),
		QueueInfo:      cacheQueueInfo,
		parent:         parent,
		childrenQueues: make(map[string]*SchedulingQueue),
		applications:   make(map[string]*SchedulingApplication),
		reservedApps:   make(map[string]int),
		allocating:     resources.NewResource(),
		preempting:     resources.NewResource(),
		pending:        resources.NewResource(),
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
	if sq.isLeafQueue() {
		sq.sortType = FifoSortPolicy
		// walk over all properties and process
		for key, value := range prop {
			if key == cache.ApplicationSortPolicy && value == "fair" {
				sq.sortType = FairSortPolicy
			}
			// for now skip the rest just log them
			log.Logger().Debug("queue property skipped",
				zap.String("key", key),
				zap.String("value", value))
		}
		return
	}
	// set the sorting type for parent queues
	sq.sortType = FairSortPolicy
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

// Return the allocated resources for this queue
func (sq *SchedulingQueue) GetAllocatedResource() *resources.Resource {
	return sq.QueueInfo.GetAllocatedResource()
}

// Return the pending resources for this queue
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
			zap.String("queueName", sq.QueueInfo.Name),
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

// Remove the scheduling app from the list of tracked applications. Make sure that the app
// is assigned to this queue and not removed yet.
// If not found this call is a noop
func (sq *SchedulingQueue) removeSchedulingApplication(app *SchedulingApplication) {
	// clean up any outstanding pending resources
	appID := app.ApplicationInfo.ApplicationID
	if _, ok := sq.applications[appID]; !ok {
		log.Logger().Debug("Application not found while removing from queue",
			zap.String("queueName", sq.QueueInfo.Name),
			zap.String("applicationID", appID))
		return
	}
	if appPending := app.GetPendingResource(); !resources.IsZero(appPending) {
		sq.decPendingResource(appPending)
	}
	sq.Lock()
	defer sq.Unlock()

	delete(sq.applications, appID)
}

// Get a copy of all apps holding the lock
func (sq *SchedulingQueue) getCopyOfApps() map[string]*SchedulingApplication {
	sq.RLock()
	defer sq.RUnlock()
	appsCopy := make(map[string]*SchedulingApplication, 0)
	for appID, app := range sq.applications {
		appsCopy[appID] = app
	}
	return appsCopy
}

// Get a copy of the child queues
// This is used by the partition manager to find all queues to clean however we can not
// guarantee that there is no new child added while we clean up since there is no overall
// lock on the scheduler. We'll need to test just before to make sure the parent is empty
func (sq *SchedulingQueue) GetCopyOfChildren() map[string]*SchedulingQueue {
	sq.RLock()
	defer sq.RUnlock()
	children := make(map[string]*SchedulingQueue)
	for k, v := range sq.childrenQueues {
		children[k] = v
	}
	return children
}

// Check if the queue is empty
// A parent queue is empty when it has no children left
// A leaf queue is empty when there are no applications left
func (sq *SchedulingQueue) isEmpty() bool {
	sq.RLock()
	defer sq.RUnlock()
	if sq.isLeafQueue() {
		return len(sq.applications) == 0
	}
	return len(sq.childrenQueues) == 0
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
	return sq.QueueInfo.IsLeafQueue()
}

// Queue status methods reflecting the underlying queue object state
// link back to the underlying queue object to prevent out of sync states
func (sq *SchedulingQueue) isRunning() bool {
	return sq.QueueInfo.IsRunning()
}

func (sq *SchedulingQueue) isDraining() bool {
	return sq.QueueInfo.IsDraining()
}

func (sq *SchedulingQueue) isStopped() bool {
	return sq.QueueInfo.IsStopped()
}

// Is this queue managed or not.
// link back to the underlying queue object to prevent out of sync types
func (sq *SchedulingQueue) isManaged() bool {
	return sq.QueueInfo.IsManaged()
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
			zap.String("queueName", sq.QueueInfo.Name),
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
	return sq.QueueInfo.CheckSubmitAccess(user)
}

// Check if the user has access to the queue for admin actions.
// Calls the cache queue which is doing the real work.
func (sq *SchedulingQueue) checkAdminAccess(user security.UserGroup) bool {
	return sq.QueueInfo.CheckAdminAccess(user)
}

// Return the allocated and allocating resources for this queue
func (sq *SchedulingQueue) getAssumeAllocated() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return resources.Add(sq.allocating, sq.QueueInfo.GetAllocatedResource())
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
			zap.String("queueName", sq.QueueInfo.Name),
			zap.Error(err))
	}
}

// Return a sorted copy of the applications in the queue. Applications are sorted using the
// sorting type of the queue.
// Only applications with a pending resource request are considered.
// Lock free call all locks are taken when needed in called functions
func (sq *SchedulingQueue) sortApplications() []*SchedulingApplication {
	if !sq.isLeafQueue() {
		return nil
	}
	// Create a copy of the applications with pending resources
	sortedApps := make([]*SchedulingApplication, 0)
	for _, app := range sq.getCopyOfApps() {
		// Only look at app when pending-res > 0
		if resources.StrictlyGreaterThanZero(app.GetPendingResource()) {
			sortedApps = append(sortedApps, app)
		}
	}
	// Sort the applications
	sortApplications(sortedApps, sq.getSortType(), sq.QueueInfo.GetGuaranteedResource())

	return sortedApps
}

// Return a sorted copy of the queues for this parent queue.
// Only queues with a pending resource request are considered. The queues are sorted using the
// sorting type for the parent queue.
// Lock free call all locks are taken when needed in called functions
func (sq *SchedulingQueue) sortQueues() []*SchedulingQueue {
	if sq.isLeafQueue() {
		return nil
	}
	// Create a list of the queues with pending resources
	sortedQueues := make([]*SchedulingQueue, 0)
	for _, child := range sq.GetCopyOfChildren() {
		// a stopped queue cannot be scheduled
		if child.isStopped() {
			continue
		}
		// queue must have pending resources to be considered for scheduling
		if resources.StrictlyGreaterThanZero(child.GetPendingResource()) {
			sortedQueues = append(sortedQueues, child)
		}
	}
	// Sort the queues
	sorter := sq.getSortType()
	sortQueue(sortedQueues, sorter)

	return sortedQueues
}

// Get the headroom for the queue this should never be more than the headroom for the parent.
// In case there are no nodes in a newly started cluster and no queues have a limit configured this call
// will return nil.
// NOTE: if a resource quantity is missing and a limit is defined the missing quantity will be seen as a limit of 0.
// When defining a limit you therefore should define all resource quantities.
func (sq *SchedulingQueue) getHeadRoom() *resources.Resource {
	var parentHeadRoom *resources.Resource
	if sq.parent != nil {
		parentHeadRoom = sq.parent.getHeadRoom()
	}
	sq.RLock()
	defer sq.RUnlock()
	headRoom := sq.QueueInfo.GetMaxResource()
	// if we have no max set headroom is always the same as the parent
	if headRoom == nil {
		return parentHeadRoom
	}
	// calculate unused
	headRoom.SubFrom(sq.allocating)
	headRoom.SubFrom(sq.QueueInfo.GetAllocatedResource())
	// check the minimum of the two: parentHeadRoom is nil for root
	if parentHeadRoom == nil {
		return headRoom
	}
	return resources.ComponentWiseMin(headRoom, parentHeadRoom)
}

// Get the max resource for the queue this should never be more than the max for the parent.
// The root queue always has its limit set to the total cluster size (dynamic based on node registration)
// In case there are no nodes in a newly started cluster and no queues have a limit configured this call
// will return nil.
// NOTE: if a resource quantity is missing and a limit is defined the missing quantity will be seen as a limit of 0.
// When defining a limit you therefore should define all resource quantities.
func (sq *SchedulingQueue) getMaxResource() *resources.Resource {
	// get the limit for the parent first and check against the queues own
	var limit *resources.Resource
	if sq.parent != nil {
		limit = sq.parent.getMaxResource()
	}
	sq.RLock()
	defer sq.RUnlock()
	max := sq.QueueInfo.GetMaxResource()
	// no queue limit set, not even for root
	if limit == nil {
		return max
	}
	// parent limit set no queue limit return parent
	if max == nil {
		return limit
	}
	// calculate the smallest value for each type
	return resources.ComponentWiseMin(limit, max)
}

// Try allocate pending requests. This only gets called if there is a pending request on this queue or its children.
// This is a depth first algorithm: descend into the depth of the queue tree first. Child queues are sorted based on
// the configured queue sortType. Queues without pending resources are skipped.
// Applications are sorted based on the application sortType. Applications without pending resources are skipped.
// Lock free call this all locks are taken when needed in called functions
func (sq *SchedulingQueue) tryAllocate(ctx *partitionSchedulingContext) *schedulingAllocation {
	if sq.isLeafQueue() {
		// get the headroom
		headRoom := sq.getHeadRoom()
		// process the apps (filters out app without pending requests)
		for _, app := range sq.sortApplications() {
			alloc := app.tryAllocate(headRoom, ctx)
			if alloc != nil {
				log.Logger().Debug("allocation found on queue",
					zap.String("queueName", sq.Name),
					zap.String("appID", app.ApplicationInfo.ApplicationID),
					zap.String("allocation", alloc.String()))
				return alloc
			}
		}
	} else {
		// process the child queues (filters out queues without pending requests)
		for _, child := range sq.sortQueues() {
			alloc := child.tryAllocate(ctx)
			if alloc != nil {
				return alloc
			}
		}
	}
	return nil
}

// Try allocate reserved requests. This only gets called if there is a pending request on this queue or its children.
// This is a depth first algorithm: descend into the depth of the queue tree first. Child queues are sorted based on
// the configured queue sortType. Queues without pending resources are skipped.
// Applications are currently NOT sorted and are iterated over in a random order.
// Lock free call this all locks are taken when needed in called functions
func (sq *SchedulingQueue) tryReservedAllocate(ctx *partitionSchedulingContext) *schedulingAllocation {
	if sq.isLeafQueue() {
		// skip if it has no reservations
		if len(sq.reservedApps) != 0 {
			// get the headroom
			headRoom := sq.getHeadRoom()
			// process the apps
			for appID, numRes := range sq.reservedApps {
				if numRes > 1 {
					log.Logger().Debug("multiple reservations found for application trying to allocate one",
						zap.String("appID", appID),
						zap.Int("reservations", numRes))
				}
				app := sq.getApplication(appID)
				alloc := app.tryReservedAllocate(headRoom, ctx)
				if alloc != nil {
					log.Logger().Debug("reservation found for allocation found on queue",
						zap.String("queueName", sq.Name),
						zap.String("appID", appID),
						zap.String("allocation", alloc.String()))
					return alloc
				}
			}
		}
	} else {
		// process the child queues (filters out queues that have no pending requests)
		for _, child := range sq.sortQueues() {
			alloc := child.tryReservedAllocate(ctx)
			if alloc != nil {
				return alloc
			}
		}
	}
	return nil
}

// Add an reserved app to the list.
// No checks this is only called when a reservation is processed using the app stored in the queue.
func (sq *SchedulingQueue) reserve(appID string) {
	sq.Lock()
	defer sq.Unlock()
	// increase the number of reservations for this app
	sq.reservedApps[appID]++
}

// Add an reserved app to the list.
// No checks this is only called when a reservation is processed using the app stored in the queue.
func (sq *SchedulingQueue) unReserve(appID string) {
	sq.Lock()
	defer sq.Unlock()
	// make sure we cannot go below 0
	if num, ok := sq.reservedApps[appID]; ok {
		// decrease the number of reservations for this app and cleanup
		if num == 1 {
			delete(sq.reservedApps, appID)
		} else {
			sq.reservedApps[appID]--
		}
	}
}

// Get the app based on the ID.
func (sq *SchedulingQueue) getApplication(appID string) *SchedulingApplication {
	sq.RLock()
	defer sq.RUnlock()
	return sq.applications[appID]
}

// get the queue sort type holding a lock
func (sq *SchedulingQueue) getSortType() SortType {
	sq.RLock()
	defer sq.RUnlock()
	return sq.sortType
}
