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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

const (
	DOT        = "."
	DotReplace = "_dot_"
)

const appTagNamespaceResourceQuota = "namespace.resourcequota"

// Represents Queue inside Scheduler
type SchedulingQueue struct {
	QueuePath      string // Fully qualified path for the queue

	// Private fields need protection
	sortType       policies.SortPolicy               // How applications (leaf) or queues (parents) are sorted
	childrenQueues map[string]*SchedulingQueue       // Only for direct children, parent queue only
	applications   map[string]*SchedulingApplication // only for leaf queue
	reservedApps   map[string]int                    // applications reserved within this queue, with reservation count
	parent         *SchedulingQueue                  // link back to the parent in the scheduler
	allocating     *resources.Resource               // resource being allocated in the queue but not confirmed
	preempting     *resources.Resource               // resource considered for preemption in the queue
	pending        *resources.Resource               // pending resource for the apps in the queue

	properties         map[string]string
	adminACL           security.ACL        // admin ACL
	submitACL          security.ACL        // submit ACL
	maxResource        *resources.Resource // When not set, max = nil
	guaranteedResource *resources.Resource // When not set, Guaranteed == 0
	allocatedResource  *resources.Resource // set based on allocation
	isLeaf             bool                // this is a leaf queue or not (i.e. parent)
	managed            bool                // queue is part of the config, not auto created
	stateMachine       *fsm.FSM            // the state of the queue for scheduling
	stateTime          time.Time           // last time the state was updated (needed for cleanup)

	sync.RWMutex
}

// Create a new queue from the configuration object.
// The configuration is validated before we call this: we should not see any errors.
func NewPreConfiguredQueue(conf configs.QueueConfig, parent *SchedulingQueue) (*SchedulingQueue, error) {
	sq := newBlankSchedulingQueue()

	sq.QueuePath = strings.ToLower(conf.Name)
	sq.parent = parent
	sq.managed = true
	sq.isLeaf = !conf.Parent

	err := sq.updateQueueProps(conf)
	if err != nil {
		return nil, fmt.Errorf("queue creation failed: %s", err)
	}

	// add the queue in the structure
	if parent != nil {
		err = parent.addChildQueue(sq)
		if err != nil {
			return nil, fmt.Errorf("queue creation failed: %s", err)
		}
	}

	log.Logger().Debug("queue added",
		zap.String("queueName", sq.QueuePath))
	return sq, nil
}

// Create a new unmanaged queue. An unmanaged queue is created as result of a rule.
// Rule based queues which might not fit in the structure or fail parsing.
func NewDynamicQueue(name string, leaf bool, parent *SchedulingQueue) (*SchedulingQueue, error) {
	// name might not be checked do it here
	if !configs.QueueNameRegExp.MatchString(name) {
		return nil, fmt.Errorf("invalid queue name %s, a name must only have alphanumeric characters,"+
			" - or _, and be no longer than 64 characters", name)
	}
	sq := newBlankSchedulingQueue()

	// create the object
	sq.QueuePath = strings.ToLower(name)
	sq.parent = parent
	sq.isLeaf = leaf


	// TODO set resources and properties on unmanaged queues
	// add the queue in the structure
	if parent != nil {
		err := parent.addChildQueue(sq)
		if err != nil {
			return nil, fmt.Errorf("queue creation failed: %s", err)
		}
		// pull the properties from the parent that should be set on the child
		sq.setTemplateProperties()
	}

	return sq, nil
}

func newBlankSchedulingQueue() *SchedulingQueue {
	sq := &SchedulingQueue{
		childrenQueues:    make(map[string]*SchedulingQueue),
		applications:      make(map[string]*SchedulingApplication),
		reservedApps:      make(map[string]int),
		allocating:        resources.NewResource(),
		preempting:        resources.NewResource(),
		pending:           resources.NewResource(),
		stateMachine:      newObjectState(),
		allocatedResource: resources.NewResource(),
	}

	return sq
}

// Update the properties for the scheduling queue based on the current cached configuration
func (sq *SchedulingQueue) updateSchedulingQueueProperties(prop map[string]string) {
	sq.Lock()
	defer sq.Unlock()
	// set the defaults, override with what is in the configured properties
	if sq.isLeafQueue() {
		// walk over all properties and process
		var err error
		sq.sortType = policies.Undefined
		for key, value := range prop {
			if key == configs.ApplicationSortPolicy {
				sq.sortType, err = policies.SortPolicyFromString(value)
				if err != nil {
					log.Logger().Debug("application sort property configuration error",
						zap.Error(err))
				}
			}
			// for now skip the rest just log them
			log.Logger().Debug("queue property skipped",
				zap.String("key", key),
				zap.String("value", value))
		}
		// if it is not defined default to fifo
		if sq.sortType == policies.Undefined {
			sq.sortType = policies.FifoSortPolicy
		}
		return
	}
	// set the sorting type for parent queues
	sq.sortType = policies.FairSortPolicy
}

// Update the queue properties and the child queues for the queue after a configuration update.
// New child queues will be added.
// Child queues that are removed from the configuration have been changed to a draining state and will not be scheduled.
// They are not removed until the queue is really empty, no action must be taken here.
// FIXME: Properly handle update scheduler config, error during initialization, etc.
func (sq *SchedulingQueue) updateSchedulingQueueInfo(updatedQueues map[string]*SchedulingQueue, parent *SchedulingQueue) {
	// initialise the child queues based on what is in the cached copy
	for childName, childQueue := range updatedQueues {
		child := sq.getChildQueue(childName)
		// create a new queue if it does not exist
		if child == nil {
			parent.addChildQueue(child)
		} else {
			child.updateSchedulingQueueProperties(childQueue.GetProperties())
		}
		child.updateSchedulingQueueInfo(childQueue.GetCopyOfChildren(), child)
	}
}

// Return the allocated resources for this queue
func (sq *SchedulingQueue) GetAllocatedResource() *resources.Resource {
	return sq.GetAllocatedResource()
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
			zap.String("queueName", sq.QueuePath),
			zap.Error(err))
	}
}

// Add scheduling app to the queue. All checks are assumed to have passed before we get here.
// No update of pending resource is needed as it should not have any requests yet.
// Replaces the existing application without further checks.
func (sq *SchedulingQueue) addSchedulingApplication(app *SchedulingApplication) {
	sq.Lock()
	defer sq.Unlock()
	sq.applications[app.ApplicationID] = app
	// YUNIKORN-199: update the quota from the namespace
	// get the tag with the quota
	quota := app.getTag(appTagNamespaceResourceQuota)
	if quota == "" {
		return
	}
	// need to set a quota: convert json string to resource
	res, err := resources.NewResourceFromString(quota)
	if err != nil {
		log.Logger().Error("application resource quota conversion failure",
			zap.String("json quota string", quota),
			zap.Error(err))
		return
	}
	if !resources.StrictlyGreaterThanZero(res) {
		log.Logger().Error("application resource quota has at least one 0 value: cannot set queue limit",
			zap.String("maxResource", res.String()))
		return
	}
	// set the quota
	sq.UpdateUnManagedMaxResource(res)
}

// Remove the scheduling app from the list of tracked applications. Make sure that the app
// is assigned to this queue and not removed yet.
// If not found this call is a noop
func (sq *SchedulingQueue) removeSchedulingApplication(app *SchedulingApplication) {
	// clean up any outstanding pending resources
	appID := app.ApplicationID
	if _, ok := sq.applications[appID]; !ok {
		log.Logger().Error("Application not found while removing from queue",
			zap.String("queueName", sq.QueuePath),
			zap.String("applicationID", appID))
		return
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

// Get a child queue based on the name of the child.
func (sq *SchedulingQueue) getChildQueue(name string) *SchedulingQueue {
	sq.RLock()
	defer sq.RUnlock()

	return sq.childrenQueues[name]
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
	sq.parent.removeChildQueue(sq.QueuePath)
	return true
}

// Is this queue a leaf or not (i.e parent)
// link back to the underlying queue object to prevent out of sync types
func (sq *SchedulingQueue) isLeafQueue() bool {
	return sq.IsLeafQueue()
}

// Queue status methods reflecting the underlying queue object state
// link back to the underlying queue object to prevent out of sync states
func (sq *SchedulingQueue) isRunning() bool {
	return sq.IsRunning()
}

func (sq *SchedulingQueue) isDraining() bool {
	return sq.IsDraining()
}

func (sq *SchedulingQueue) isStopped() bool {
	return sq.IsStopped()
}

// Is this queue managed or not.
// link back to the underlying queue object to prevent out of sync types
func (sq *SchedulingQueue) isManaged() bool {
	return sq.isManaged()
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
			zap.String("queueName", sq.QueuePath),
			zap.Error(err))
	}
}

// (Re)Set the preempting resources for the queue.
// This could be because they are preempted, or the preemption was cancelled.
func (sq *SchedulingQueue) setPreemptingResource(newAlloc *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()
	sq.preempting = newAlloc
}

// Check if the user has access to the queue to submit an application.
// This will check the submit ACL and the admin ACL.
// Calls the cache queue which is doing the real work.
func (sq *SchedulingQueue) checkSubmitAccess(user security.UserGroup) bool {
	return sq.CheckSubmitAccess(user)
}

// Check if the user has access to the queue for admin actions.
// Calls the cache queue which is doing the real work.
func (sq *SchedulingQueue) checkAdminAccess(user security.UserGroup) bool {
	return sq.CheckAdminAccess(user)
}

// Return the allocated and allocating resources for this queue
func (sq *SchedulingQueue) getAssumeAllocated() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return resources.Add(sq.allocating, sq.allocatedResource)
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
			zap.String("queueName", sq.QueuePath),
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
	// Sort the applications
	return sortApplications(sq.getCopyOfApps(), sq.getSortType(), sq.GetGuaranteedResource())
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
	sortQueue(sortedQueues, sq.getSortType())

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
	return sq.internalHeadRoom(parentHeadRoom)
}

// this function returns the max headRoom of a queue
// this doesn't get the partition resources into the consideration
func (sq *SchedulingQueue) getMaxHeadRoom() *resources.Resource {
	var parentHeadRoom *resources.Resource
	if sq.parent != nil {
		parentHeadRoom = sq.parent.getMaxHeadRoom()
	} else {
		return nil
	}
	return sq.internalHeadRoom(parentHeadRoom)
}

func (sq *SchedulingQueue) internalHeadRoom(parentHeadRoom *resources.Resource) *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	headRoom := sq.maxResource.Clone()

	// if we have no max set headroom is always the same as the parent
	if headRoom == nil {
		return parentHeadRoom
	}
	// calculate unused
	headRoom.SubFrom(sq.allocating)
	headRoom.SubFrom(sq.GetAllocatedResource())
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
	max := sq.maxResource.Clone()
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
// the configured queue sortPolicy. Queues without pending resources are skipped.
// Applications are sorted based on the application sortPolicy. Applications without pending resources are skipped.
// Lock free call this all locks are taken when needed in called functions
func (sq *SchedulingQueue) tryAllocate(ctx *PartitionSchedulingContext) *schedulingAllocation {
	if sq.isLeafQueue() {
		// get the headroom
		headRoom := sq.getHeadRoom()
		// process the apps (filters out app without pending requests)
		for _, app := range sq.sortApplications() {
			alloc := app.tryAllocate(headRoom, ctx)
			if alloc != nil {
				log.Logger().Debug("allocation found on queue",
					zap.String("queueName", sq.QueuePath),
					zap.String("appID", app.ApplicationID),
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

func (sq *SchedulingQueue) getQueueOutstandingRequests(total *[]*schedulingAllocationAsk) {
	if sq.isLeafQueue() {
		headRoom := sq.getMaxHeadRoom()
		for _, app := range sq.sortApplications() {
			app.getOutstandingRequests(headRoom, total)
		}
	} else {
		for _, child := range sq.sortQueues() {
			child.getQueueOutstandingRequests(total)
		}
	}
}

// Try allocate reserved requests. This only gets called if there is a pending request on this queue or its children.
// This is a depth first algorithm: descend into the depth of the queue tree first. Child queues are sorted based on
// the configured queue sortPolicy. Queues without pending resources are skipped.
// Applications are currently NOT sorted and are iterated over in a random order.
// Lock free call this all locks are taken when needed in called functions
func (sq *SchedulingQueue) tryReservedAllocate(ctx *PartitionSchedulingContext) *schedulingAllocation {
	if sq.isLeafQueue() {
		// skip if it has no reservations
		reservedCopy := sq.getReservedApps()
		if len(reservedCopy) != 0 {
			// get the headroom
			headRoom := sq.getHeadRoom()
			// process the apps
			for appID, numRes := range reservedCopy {
				if numRes > 1 {
					log.Logger().Debug("multiple reservations found for application trying to allocate one",
						zap.String("appID", appID),
						zap.Int("reservations", numRes))
				}
				app := sq.getApplication(appID)
				alloc := app.tryReservedAllocate(headRoom, ctx)
				if alloc != nil {
					log.Logger().Debug("reservation found for allocation found on queue",
						zap.String("queueName", sq.QueuePath),
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

// Get a copy of the reserved app list
// locked to prevent race conditions from event updates
func (sq *SchedulingQueue) getReservedApps() map[string]int {
	sq.Lock()
	defer sq.Unlock()

	copied := make(map[string]int)
	for appID, numRes := range sq.reservedApps {
		copied[appID] = numRes
	}
	// increase the number of reservations for this app
	return copied
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
func (sq *SchedulingQueue) unReserve(appID string, releases int) {
	sq.Lock()
	defer sq.Unlock()
	// make sure we cannot go below 0
	if num, ok := sq.reservedApps[appID]; ok {
		// decrease the number of reservations for this app and cleanup
		if num <= releases {
			delete(sq.reservedApps, appID)
		} else {
			sq.reservedApps[appID] -= releases
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
func (sq *SchedulingQueue) getSortType() policies.SortPolicy {
	sq.RLock()
	defer sq.RUnlock()
	return sq.sortType
}


// Handle the state event for the queue.
// The state machine handles the locking.
func (sq *SchedulingQueue) HandleQueueEvent(event SchedulingObjectEvent) error {
	err := sq.stateMachine.Event(event.String(), sq.QueuePath)
	// err is nil the state transition was done
	if err == nil {
		sq.stateTime = time.Now()
		return nil
	}
	// handle the same state transition not nil error (limit of fsm).
	if err.Error() == "no transition" {
		return nil
	}
	return err
}

// Return the guaranteed resource for the queue.
func (sq *SchedulingQueue) GetGuaranteedResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return sq.guaranteedResource
}

// Return the max resource for the queue.
// If not set the returned resource will be nil.
func (sq *SchedulingQueue) GetMaxResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	if sq.maxResource == nil {
		return nil
	}
	return sq.maxResource.Clone()
}

// Set the max resource for root the queue.
// Should only happen on the root, all other queues get it from the config via properties.
func (sq *SchedulingQueue) setMaxResource(max *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()

	if sq.parent != nil {
		log.Logger().Warn("Max resources set on a queue that is not the root",
			zap.String("queueName", sq.QueuePath))
		return
	}
	sq.maxResource = max.Clone()
}

// Update the max resource for an unmanaged leaf queue.
func (sq *SchedulingQueue) UpdateUnManagedMaxResource(max *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()

	if sq.managed || !sq.isLeaf {
		log.Logger().Warn("Trying to set max resources set on a queue that is not an unmanaged leaf",
			zap.String("queueName", sq.QueuePath))
		return
	}
	sq.maxResource = max
}

// Return if this is a leaf queue or not
func (sq *SchedulingQueue) IsLeafQueue() bool {
	return sq.isLeaf
}

// Return if this is a leaf queue or not
func (sq *SchedulingQueue) IsManaged() bool {
	return sq.managed
}

// Add a new child queue to this queue
// - can only add to a non leaf queue
// - cannot add when the queue is marked for deletion
// - if this is the first child initialise
func (sq *SchedulingQueue) addChildQueue(child *SchedulingQueue) error {
	sq.Lock()
	defer sq.Unlock()
	if sq.isLeaf {
		return fmt.Errorf("cannot add a child queue to a leaf queue: %s", sq.QueuePath)
	}
	if sq.IsDraining() {
		return fmt.Errorf("cannot add a child queue when queue is marked for deletion: %s", sq.QueuePath)
	}
	// add the child (init if needed)
	if sq.childrenQueues == nil {
		sq.childrenQueues = make(map[string]*SchedulingQueue)
	}
	sq.childrenQueues[child.QueuePath] = child
	return nil
}

func (sq *SchedulingQueue) updateUsedResourceMetrics() {
	// update queue metrics when this is a leaf queue
	if sq.isLeaf {
		for k, v := range sq.allocatedResource.Resources {
			metrics.GetQueueMetrics(sq.QueuePath).SetQueueUsedResourceMetrics(k, float64(v))
		}
	}
}

// Increment the allocated resources for this queue (recursively)
// Guard against going over max resources if set
func (sq *SchedulingQueue) IncAllocatedResource(alloc *resources.Resource, nodeReported bool) error {
	sq.Lock()
	defer sq.Unlock()

	// check this queue: failure stops checks if the allocation is not part of a node addition
	newAllocation := resources.Add(sq.allocatedResource, alloc)
	if !nodeReported {
		if sq.maxResource != nil && !resources.FitIn(sq.maxResource, newAllocation) {
			return fmt.Errorf("allocation (%v) puts queue %s over maximum allocation (%v)",
				alloc, sq.QueuePath, sq.maxResource)
		}
	}
	// check the parent: need to pass before updating
	if sq.parent != nil {
		if err := sq.parent.IncAllocatedResource(alloc, nodeReported); err != nil {
			log.Logger().Error("parent queue exceeds maximum resource",
				zap.Any("allocationId", alloc),
				zap.Any("maxResource", sq.maxResource),
				zap.Error(err))
			return err
		}
	}
	// all OK update this queue
	sq.allocatedResource = newAllocation
	sq.updateUsedResourceMetrics()
	return nil
}

// Decrement the allocated resources for this queue (recursively)
// Guard against going below zero resources.
func (sq *SchedulingQueue) decAllocatedResource(alloc *resources.Resource) error {
	sq.Lock()
	defer sq.Unlock()

	// check this queue: failure stops checks
	if alloc != nil && !resources.FitIn(sq.allocatedResource, alloc) {
		return fmt.Errorf("released allocation (%v) is larger than queue %s allocation (%v)",
			alloc, sq.QueuePath, sq.allocatedResource)
	}
	// check the parent: need to pass before updating
	if sq.parent != nil {
		if err := sq.parent.decAllocatedResource(alloc); err != nil {
			log.Logger().Error("released allocation is larger than parent queue allocated resource",
				zap.Any("allocationId", alloc),
				zap.Any("parent allocatedResource", sq.parent.GetAllocatedResource()),
				zap.Error(err))
			return err
		}
	}
	// all OK update the queue
	sq.allocatedResource = resources.Sub(sq.allocatedResource, alloc)
	sq.updateUsedResourceMetrics()
	return nil
}

// Remove the queue from the structure.
// Since nothing is allocated there shouldn't be anything referencing this queue any more.
// The real removal is removing the queue from the parent's child list
func (sq *SchedulingQueue) RemoveQueue() bool {
	sq.Lock()
	defer sq.Unlock()
	// cannot remove a managed queue that is running
	if sq.managed && sq.IsRunning() {
		return false
	}
	// cannot remove a queue that has children or allocated resources
	if len(sq.childrenQueues) > 0 || !resources.IsZero(sq.allocatedResource) {
		return false
	}

	log.Logger().Info("removing queue", zap.String("queue", sq.QueuePath))
	// root is always managed and is the only queue with a nil parent: no need to guard
	sq.parent.removeChildQueue(sq.QueuePath)
	return true
}

// Mark the managed queue for removal from the system.
// This can be executed multiple times and is only effective the first time.
// This is a noop on an unmanaged queue
func (sq *SchedulingQueue) MarkQueueForRemoval() {
	// need to lock for write as we don't want to add a queue while marking for removal
	sq.Lock()
	defer sq.Unlock()
	// Mark the managed queue for deletion: it is removed from the config let it drain.
	// Also mark all the managed children for deletion.
	if sq.managed {
		log.Logger().Info("marking managed queue for deletion",
			zap.String("queue", sq.QueuePath))
		if err := sq.HandleQueueEvent(Remove); err != nil {
			log.Logger().Info("failed to marking managed queue for deletion",
				zap.String("queue", sq.QueuePath),
				zap.Error(err))
		}
		if len(sq.childrenQueues) > 0 {
			for _, child := range sq.childrenQueues {
				child.MarkQueueForRemoval()
			}
		}
	}
}

// Return a copy of the properties for this queue
func (sq *SchedulingQueue) GetProperties() map[string]string {
	sq.Lock()
	defer sq.Unlock()
	props := make(map[string]string)
	for key, value := range sq.properties {
		props[key] = value
	}
	return props
}

// Set the properties that the unmanaged child queue inherits from the parent
// only called on create of a new unmanaged queue
// This currently only sets the sort policy as it is set on the parent
// Further implementation is part of YUNIKORN-193
func (sq *SchedulingQueue) setTemplateProperties() {
	sq.Lock()
	defer sq.Unlock()
	// for a leaf queue pull out all values from the template and set each of them
	// See YUNIKORN-193: for now just copy one attr from parent
	if sq.isLeaf {
		sq.properties = make(map[string]string)
		parentProp := sq.parent.GetProperties()
		if len(parentProp) != 0 {
			if parentProp[configs.ApplicationSortPolicy] != "" {
				sq.properties[configs.ApplicationSortPolicy] = parentProp[configs.ApplicationSortPolicy]
			}
		}
	}
	// for a parent queue we just copy the template from its parent (no need to be recursive)
	// this stops at the first managed queue
	// See YUNIKORN-193
}

// Update an existing managed queue based on the updated configuration
func (sq *SchedulingQueue) updateQueueProps(conf configs.QueueConfig) error {
	sq.Lock()
	defer sq.Unlock()
	// Set the ACLs
	var err error
	sq.submitACL, err = security.NewACL(conf.SubmitACL)
	if err != nil {
		log.Logger().Error("parsing submit ACL failed this should not happen",
			zap.Error(err))
		return err
	}
	sq.adminACL, err = security.NewACL(conf.AdminACL)
	if err != nil {
		log.Logger().Error("parsing admin ACL failed this should not happen",
			zap.Error(err))
		return err
	}
	// Change from unmanaged to managed
	if !sq.managed {
		log.Logger().Info("changed un-managed queue to managed",
			zap.String("queue", sq.QueuePath))
		sq.managed = true
	}

	// Make sure the parent flag is set correctly: config might expect auto parent type creation
	if len(conf.Queues) > 0 {
		sq.isLeaf = false
	}

	// Load the max resources
	maxResource, err := resources.NewResourceFromConf(conf.Resources.Max)
	if err != nil {
		log.Logger().Error("parsing failed on max resources this should not happen",
			zap.Error(err))
		return err
	}
	if len(maxResource.Resources) != 0 {
		sq.maxResource = maxResource
	}

	// Load the guaranteed resources
	guaranteedResource, err := resources.NewResourceFromConf(conf.Resources.Guaranteed)
	if err != nil {
		log.Logger().Error("parsing failed on max resources this should not happen",
			zap.Error(err))
		return err
	}
	if len(guaranteedResource.Resources) != 0 {
		sq.guaranteedResource = guaranteedResource
	}

	// Update properties
	sq.properties = conf.Properties
	if sq.parent != nil {
		parentProps := sq.parent.GetProperties()
		if len(parentProps) != 0 {
			sq.properties = mergeProperties(parentProps, conf.Properties)
		}
	}

	return nil
}

// Merge the properties for the queue. This is only called when updating the queue from the configuration.
func mergeProperties(parent, child map[string]string) map[string]string {
	merged := make(map[string]string)
	if len(parent) > 0 {
		for key, value := range parent {
			merged[key] = value
		}
	}
	if len(child) > 0 {
		for key, value := range child {
			merged[key] = value
		}
	}
	return merged
}

// Is the queue marked for deletion and can only handle existing application requests.
// No new applications will be accepted.
func (sq *SchedulingQueue) IsDraining() bool {
	return sq.stateMachine.Current() == Draining.String()
}

// Is the queue in a normal active state.
func (sq *SchedulingQueue) IsRunning() bool {
	return sq.stateMachine.Current() == Active.String()
}

// Is the queue stopped, not active in scheduling at all.
func (sq *SchedulingQueue) IsStopped() bool {
	return sq.stateMachine.Current() == Stopped.String()
}

// Return the current state of the queue
func (sq *SchedulingQueue) CurrentState() string {
	return sq.stateMachine.Current()
}

// Check if the user has access to the queue to submit an application recursively.
// This will check the submit ACL and the admin ACL.
func (sq *SchedulingQueue) CheckSubmitAccess(user security.UserGroup) bool {
	sq.RLock()
	allow := sq.submitACL.CheckAccess(user) || sq.adminACL.CheckAccess(user)
	sq.RUnlock()
	if !allow && sq.parent != nil {
		allow = sq.parent.CheckSubmitAccess(user)
	}
	return allow
}

// Check if the user has access to the queue for admin actions recursively.
func (sq *SchedulingQueue) CheckAdminAccess(user security.UserGroup) bool {
	sq.RLock()
	allow := sq.adminACL.CheckAccess(user)
	sq.RUnlock()
	if !allow && sq.parent != nil {
		allow = sq.parent.CheckAdminAccess(user)
	}
	return allow
}

func (sq *SchedulingQueue) GetQueueInfos() dao.QueueDAOInfo {
	sq.RLock()
	defer sq.RUnlock()
	queueInfo := dao.QueueDAOInfo{}
	queueInfo.QueueName = sq.QueuePath
	queueInfo.Status = sq.stateMachine.Current()
	queueInfo.Capacities = dao.QueueCapacity{
		Capacity:     sq.GetGuaranteedResource().DAOString(),
		MaxCapacity:  sq.GetMaxResource().DAOString(),
		UsedCapacity: sq.GetAllocatedResource().DAOString(),
		AbsUsedCapacity: resources.CalculateAbsUsedCapacity(
			sq.GetMaxResource(), sq.GetAllocatedResource()).DAOString(),
	}
	queueInfo.Properties = make(map[string]string)
	for k, v := range sq.properties {
		queueInfo.Properties[k] = v
	}
	for _, child := range sq.childrenQueues {
		queueInfo.ChildQueues = append(queueInfo.ChildQueues, child.GetQueueInfos())
	}
	return queueInfo
}
