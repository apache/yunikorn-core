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

package objects

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects/template"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
)

const AppTagNamespaceResourceQuota = "namespace.resourcequota"

// Represents Queue inside Scheduler
type Queue struct {
	QueuePath string // Fully qualified path for the queue
	Name      string // Queue name as in the config etc.

	// Private fields need protection
	sortType              policies.SortPolicy     // How applications (leaf) or queues (parents) are sorted
	children              map[string]*Queue       // Only for direct children, parent queue only
	applications          map[string]*Application // only for leaf queue
	completedApplications map[string]*Application // completed applications from this leaf queue
	reservedApps          map[string]int          // applications reserved within this queue, with reservation count
	parent                *Queue                  // link back to the parent in the scheduler
	preempting            *resources.Resource     // resource considered for preemption in the queue
	pending               *resources.Resource     // pending resource for the apps in the queue

	// The queue properties should be treated as immutable the value is a merge of the
	// parent properties with the config for this queue only manipulated during creation
	// of the queue or via a queue configuration update.
	properties         map[string]string
	adminACL           security.ACL        // admin ACL
	submitACL          security.ACL        // submit ACL
	maxResource        *resources.Resource // When not set, max = nil
	guaranteedResource *resources.Resource // When not set, Guaranteed == 0
	allocatedResource  *resources.Resource // set based on allocation
	isLeaf             bool                // this is a leaf queue or not (i.e. parent)
	isManaged          bool                // queue is part of the config, not auto created
	stateMachine       *fsm.FSM            // the state of the queue for scheduling
	stateTime          time.Time           // last time the state was updated (needed for cleanup)
	template           *template.Template

	sync.RWMutex
}

func newBlankQueue() *Queue {
	return &Queue{
		children:              make(map[string]*Queue),
		applications:          make(map[string]*Application),
		completedApplications: make(map[string]*Application),
		reservedApps:          make(map[string]int),
		properties:            make(map[string]string),
		stateMachine:          NewObjectState(),
		allocatedResource:     resources.NewResource(),
		preempting:            resources.NewResource(),
		pending:               resources.NewResource(),
	}
}

// Create a new queue from scratch based on the configuration
// lock free as it cannot be referenced yet
func NewConfiguredQueue(conf configs.QueueConfig, parent *Queue) (*Queue, error) {
	sq := newBlankQueue()
	sq.Name = strings.ToLower(conf.Name)
	sq.QueuePath = strings.ToLower(conf.Name)
	sq.parent = parent
	sq.isManaged = true

	// update the properties
	if err := sq.applyConf(conf); err != nil {
		return nil, fmt.Errorf("configured queue creation failed: %s", err)
	}

	// add to the parent, we might have an overall lock already
	// still need to make sure we lock the parent so we do not interfere with scheduling
	if parent != nil {
		sq.QueuePath = parent.QueuePath + configs.DOT + sq.Name
		err := parent.addChildQueue(sq)
		if err != nil {
			return nil, fmt.Errorf("configured queue creation failed: %s", err)
		}
		// pull the properties from the parent that should be set on the child
		sq.mergeProperties(parent.getProperties(), conf.Properties)
	}
	sq.UpdateSortType()

	log.Logger().Debug("configured queue added to scheduler",
		zap.String("queueName", sq.QueuePath))

	return sq, nil
}

// Add a new queue to the system based on the placement rules
// A dynamically added queue can never be the root queue so parent must be set
// lock free as it cannot be referenced yet
func NewDynamicQueue(name string, leaf bool, parent *Queue) (*Queue, error) {
	// fail without a parent
	if parent == nil {
		return nil, fmt.Errorf("dynamic queue can not be added without parent: %s", name)
	}
	// name might not be checked do it here
	if !configs.QueueNameRegExp.MatchString(name) {
		return nil, fmt.Errorf("invalid queue name %s, a name must only have alphanumeric characters,"+
			" - or _, and be no longer than 64 characters", name)
	}
	sq := newBlankQueue()
	sq.Name = strings.ToLower(name)
	sq.QueuePath = parent.QueuePath + configs.DOT + sq.Name
	sq.parent = parent
	sq.isManaged = false
	sq.isLeaf = leaf

	// add to the parent, we might have a partition lock already
	// still need to make sure we lock the parent so we do not interfere with scheduling
	err := parent.addChildQueue(sq)
	if err != nil {
		return nil, fmt.Errorf("dynamic queue creation failed: %s", err)
	}

	sq.UpdateSortType()
	log.Logger().Info("dynamic queue added to scheduler",
		zap.String("queueName", sq.QueuePath))

	return sq, nil
}

// use input template to initialize properties, maxResource, and guaranteedResource
func (sq *Queue) applyTemplate(childTemplate *template.Template) {
	sq.properties = childTemplate.GetProperties()
	// the resources in template are already checked
	sq.guaranteedResource = childTemplate.GetGuaranteedResource()
	sq.maxResource = childTemplate.GetMaxResource()
}

// Return a copy of the properties for this queue
// Will never return a nil, can return an empty map.
func (sq *Queue) getProperties() map[string]string {
	sq.Lock()
	defer sq.Unlock()
	props := make(map[string]string)
	for key, value := range sq.properties {
		props[key] = value
	}
	return props
}

// Merge the properties from the parent queue and the config in the set from new queue
// lock free call
func (sq *Queue) mergeProperties(parent, config map[string]string) {
	// clean out all existing values (handles update case)
	sq.properties = make(map[string]string)
	// Set the parent properties
	if len(parent) != 0 {
		for key, value := range parent {
			sq.properties[key] = value
		}
	}
	// merge the config properties
	if len(config) > 0 {
		for key, value := range config {
			sq.properties[key] = value
		}
	}
}

func (sq *Queue) ApplyConf(conf configs.QueueConfig) error {
	sq.Lock()
	defer sq.Unlock()
	return sq.applyConf(conf)
}

// Apply all the properties to the queue from the config
// lock free call, must be called holding the queue lock or during create only
func (sq *Queue) applyConf(conf configs.QueueConfig) error {
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
	if !sq.isManaged {
		log.Logger().Info("changed dynamic queue to managed",
			zap.String("queue", sq.QueuePath))
		sq.isManaged = true
	}

	sq.isLeaf = !conf.Parent
	// Make sure the parent flag is set correctly: config might expect auto parent type creation
	if len(conf.Queues) > 0 {
		sq.isLeaf = false
	}

	if !sq.isLeaf {
		if err = sq.setTemplate(conf.ChildTemplate); err != nil {
			return nil
		}
	}

	// Load the max & guaranteed resources for all but the root queue
	if sq.Name != configs.RootQueue {
		if err = sq.setResources(conf.Resources); err != nil {
			return err
		}
	}

	sq.properties = conf.Properties
	return nil
}

func (sq *Queue) setResources(resource configs.Resources) error {
	maxResource, err := resources.NewResourceFromConf(resource.Max)
	if err != nil {
		log.Logger().Error("parsing failed on max resources this should not happen",
			zap.Error(err))
		return err
	}

	guaranteedResource, err := resources.NewResourceFromConf(resource.Guaranteed)
	if err != nil {
		log.Logger().Error("parsing failed on guaranteed resources this should not happen",
			zap.Error(err))
		return err
	}

	if resources.StrictlyGreaterThanZero(maxResource) {
		sq.maxResource = maxResource
	} else {
		log.Logger().Debug("max resources setting ignored: cannot set zero max resources")
	}

	if resources.StrictlyGreaterThanZero(guaranteedResource) {
		sq.guaranteedResource = guaranteedResource
	} else {
		log.Logger().Debug("guaranteed resources setting ignored: cannot set zero max resources")
	}

	return nil
}

// lock free call, must be called holding the queue lock or during create only
func (sq *Queue) setTemplate(conf configs.ChildTemplate) error {
	t, err := template.FromConf(&conf)
	if err != nil {
		return err
	}
	sq.template = t
	return nil
}

// Update the sortType for the queue based on the current properties
func (sq *Queue) UpdateSortType() {
	sq.Lock()
	defer sq.Unlock()
	// set the defaults, override with what is in the configured properties
	if sq.isLeaf {
		// walk over all properties and process
		var err error
		sq.sortType = policies.Undefined
		for key, value := range sq.properties {
			if key == configs.ApplicationSortPolicy {
				sq.sortType, err = policies.SortPolicyFromString(value)
				if err != nil {
					log.Logger().Debug("application sort property configuration error",
						zap.Error(err))
				}
			} else {
				// skip unknown properties just log them
				log.Logger().Debug("queue property skipped",
					zap.String("key", key),
					zap.String("value", value))
			}
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

func (sq *Queue) GetQueuePath() string {
	sq.RLock()
	defer sq.RUnlock()
	return sq.QueuePath
}

// Is the queue marked for deletion and can only handle existing application requests.
// No new applications will be accepted.
func (sq *Queue) IsDraining() bool {
	return sq.stateMachine.Is(Draining.String())
}

// Is the queue in a normal active state.
func (sq *Queue) IsRunning() bool {
	return sq.stateMachine.Is(Active.String())
}

// Is the queue stopped, not active in scheduling at all.
func (sq *Queue) IsStopped() bool {
	return sq.stateMachine.Is(Stopped.String())
}

// Return the current state of the queue
func (sq *Queue) CurrentState() string {
	return sq.stateMachine.Current()
}

// Handle the state event for the queue.
// The state machine handles the locking.
func (sq *Queue) handleQueueEvent(event ObjectEvent) error {
	err := sq.stateMachine.Event(event.String(), sq.QueuePath)
	// err is nil the state transition was done
	if err == nil {
		sq.stateTime = time.Now()
		return nil
	}
	// handle the same state transition not nil error (limit of fsm).
	if err.Error() == noTransition {
		return nil
	}
	return err
}

// Return the allocated resources for this queue
func (sq *Queue) GetAllocatedResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return sq.allocatedResource.Clone()
}

// Return the guaranteed resource for the queue.
func (sq *Queue) GetGuaranteedResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return sq.guaranteedResource
}

// Check if the user has access to the queue to submit an application recursively.
// This will check the submit ACL and the admin ACL.
func (sq *Queue) CheckSubmitAccess(user security.UserGroup) bool {
	sq.RLock()
	allow := sq.submitACL.CheckAccess(user) || sq.adminACL.CheckAccess(user)
	sq.RUnlock()
	if !allow && sq.parent != nil {
		allow = sq.parent.CheckSubmitAccess(user)
	}
	return allow
}

// Check if the user has access to the queue for admin actions recursively.
func (sq *Queue) CheckAdminAccess(user security.UserGroup) bool {
	sq.RLock()
	allow := sq.adminACL.CheckAccess(user)
	sq.RUnlock()
	if !allow && sq.parent != nil {
		allow = sq.parent.CheckAdminAccess(user)
	}
	return allow
}

// Convert the queue hierarchy into an object for the webservice

func (sq *Queue) GetQueueInfos() dao.QueueDAOInfo {
	queueInfo := dao.QueueDAOInfo{}
	for _, child := range sq.GetCopyOfChildren() {
		queueInfo.ChildQueues = append(queueInfo.ChildQueues, child.GetQueueInfos())
	}

	// children are done we can now lock just this queue.
	sq.RLock()
	defer sq.RUnlock()
	queueInfo.QueueName = sq.Name
	queueInfo.Status = sq.stateMachine.Current()
	queueInfo.Capacities = dao.QueueCapacity{
		Capacity:     sq.guaranteedResource.DAOString(),
		MaxCapacity:  sq.maxResource.DAOString(),
		UsedCapacity: sq.allocatedResource.DAOString(),
		AbsUsedCapacity: resources.CalculateAbsUsedCapacity(
			sq.maxResource, sq.allocatedResource).DAOString(),
	}
	queueInfo.Properties = make(map[string]string)
	for k, v := range sq.properties {
		queueInfo.Properties[k] = v
	}
	return queueInfo
}

func (sq *Queue) GetPartitionQueueDAOInfo() dao.PartitionQueueDAOInfo {
	queueInfo := dao.PartitionQueueDAOInfo{}
	childes := sq.GetCopyOfChildren()
	queueInfo.Children = make([]dao.PartitionQueueDAOInfo, 0, len(childes))
	for _, child := range childes {
		queueInfo.Children = append(queueInfo.Children, child.GetPartitionQueueDAOInfo())
	}
	// we have held the read lock so following method should not take lock again.
	sq.RLock()
	defer sq.RUnlock()

	queueInfo.QueueName = sq.QueuePath
	queueInfo.Status = sq.stateMachine.Current()
	queueInfo.MaxResource = sq.maxResource.DAOString()
	queueInfo.GuaranteedResource = sq.guaranteedResource.DAOString()
	queueInfo.AllocatedResource = sq.allocatedResource.DAOString()
	queueInfo.IsLeaf = sq.isLeaf
	queueInfo.IsManaged = sq.isManaged
	queueInfo.TemplateInfo = sq.template.GetTemplateInfo()
	queueInfo.Properties = make(map[string]string)
	for k, v := range sq.properties {
		queueInfo.Properties[k] = v
	}
	if sq.parent == nil {
		queueInfo.Parent = ""
	} else {
		queueInfo.Parent = sq.QueuePath[:strings.LastIndex(sq.QueuePath, configs.DOT)]
	}
	return queueInfo
}

// Return the pending resources for this queue
func (sq *Queue) GetPendingResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return sq.pending
}

// Update pending resource of this queue
func (sq *Queue) incPendingResource(delta *resources.Resource) {
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
func (sq *Queue) decPendingResource(delta *resources.Resource) {
	if sq == nil {
		return
	}
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

// Add  app to the queue. All checks are assumed to have passed before we get here.
// No update of pending resource is needed as it should not have any requests yet.
// Replaces the existing application without further checks.
func (sq *Queue) AddApplication(app *Application) {
	sq.Lock()
	defer sq.Unlock()
	sq.applications[app.ApplicationID] = app
	// YUNIKORN-199: update the quota from the namespace
	// get the tag with the quota
	quota := app.GetTag(AppTagNamespaceResourceQuota)
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
	if sq.isManaged {
		log.Logger().Warn("Trying to set max resources set on a queue that is not an unmanaged leaf",
			zap.String("queueName", sq.QueuePath))
		return
	}
	sq.maxResource = res
}

// Remove the app from the list of tracked applications. Make sure that the app
// is assigned to this queue and not removed yet.
// If not found this call is a noop
func (sq *Queue) RemoveApplication(app *Application) {
	// clean up any outstanding pending resources
	appID := app.ApplicationID
	if _, ok := sq.applications[appID]; !ok {
		log.Logger().Debug("Application not found while removing from queue",
			zap.String("queueName", sq.QueuePath),
			zap.String("applicationID", appID))
		return
	}
	if appPending := app.GetPendingResource(); !resources.IsZero(appPending) {
		sq.decPendingResource(appPending)
	}
	// clean up the allocated resource
	if appAllocated := app.GetAllocatedResource(); !resources.IsZero(appAllocated) {
		// failures are logged in the decrement do not do it twice
		//nolint:errcheck
		_ = sq.DecAllocatedResource(appAllocated)
	}
	// clean up the allocated placeholder resource
	if phAllocated := app.GetPlaceholderResource(); !resources.IsZero(phAllocated) {
		// failures are logged in the decrement do not do it twice
		//nolint:errcheck
		_ = sq.DecAllocatedResource(phAllocated)
	}
	sq.Lock()
	defer sq.Unlock()

	delete(sq.applications, appID)
	sq.completedApplications[appID] = app
}

// GetCopyOfApps Get a copy of all non-complated apps holding the lock
func (sq *Queue) GetCopyOfApps() map[string]*Application {
	sq.RLock()
	defer sq.RUnlock()
	appsCopy := make(map[string]*Application, len(sq.applications))
	for appID, app := range sq.applications {
		appsCopy[appID] = app
	}
	return appsCopy
}

// GetCopyOfCompletedApps Get a copy of all completed apps holding the lock
func (sq *Queue) GetCopyOfCompletedApps() map[string]*Application {
	sq.RLock()
	defer sq.RUnlock()
	completedAppsCopy := make(map[string]*Application, len(sq.completedApplications))
	for appID, app := range sq.completedApplications {
		completedAppsCopy[appID] = app
	}
	return completedAppsCopy
}

// Get a copy of the child queues
// This is used by the partition manager to find all queues to clean however we can not
// guarantee that there is no new child added while we clean up since there is no overall
// lock on the scheduler. We'll need to test just before to make sure the parent is empty
func (sq *Queue) GetCopyOfChildren() map[string]*Queue {
	sq.RLock()
	defer sq.RUnlock()
	childCopy := make(map[string]*Queue)
	for k, v := range sq.children {
		childCopy[k] = v
	}
	return childCopy
}

// Check if the queue is empty
// A parent queue is empty when it has no children left
// A leaf queue is empty when there are no applications left
func (sq *Queue) IsEmpty() bool {
	sq.RLock()
	defer sq.RUnlock()
	if sq.isLeaf {
		return len(sq.applications) == 0
	}
	return len(sq.children) == 0
}

// Remove a child queue from this queue.
// No checks are performed: if the child has been removed already it is a noop.
// This may only be called by the queue removal itself on the registered parent.
// Queue removal is always a bottom up action: leaves first then the parent.
func (sq *Queue) removeChildQueue(name string) {
	sq.Lock()
	defer sq.Unlock()

	delete(sq.children, name)
}

// Add a child queue to this queue.
// note: both child.isLeaf and child.isManaged must be already configured
func (sq *Queue) addChildQueue(child *Queue) error {
	sq.Lock()
	defer sq.Unlock()
	if sq.isLeaf {
		return fmt.Errorf("cannot add a child queue to a leaf queue: %s", sq.QueuePath)
	}
	if sq.IsDraining() {
		return fmt.Errorf("cannot add a child queue when queue is marked for deletion: %s", sq.QueuePath)
	}

	// no need to lock child as it is a new queue which cannot be accessed yet
	sq.children[child.Name] = child

	if child.isLeaf {
		if !child.isManaged {
			// this is a story about compatibility. the template is a new feature, and we want to keep old behavior.
			// 1) try to use template if it is not nil
			// 2) otherwise, child leaf copy the configs.ApplicationSortPolicy from parent (old behavior)
			if sq.template != nil {
				child.applyTemplate(sq.template)
			} else {
				policyValue, ok := sq.properties[configs.ApplicationSortPolicy]
				if ok {
					child.properties[configs.ApplicationSortPolicy] = policyValue
				}
			}
		}
		// managed (configured) leaf queue can't use template
	} else {
		child.template = sq.template
	}
	return nil
}

// Mark the managed queue for removal from the system.
// This can be executed multiple times and is only effective the first time.
// This is a noop on an unmanaged queue
func (sq *Queue) MarkQueueForRemoval() {
	// need to lock for write as we don't want to add a queue while marking for removal
	sq.Lock()
	defer sq.Unlock()
	// Mark the managed queue for deletion: it is removed from the config let it drain.
	// Also mark all the managed children for deletion.
	if sq.isManaged {
		log.Logger().Info("marking managed queue for deletion",
			zap.String("queue", sq.QueuePath))
		if err := sq.handleQueueEvent(Remove); err != nil {
			log.Logger().Info("failed to mark managed queue for deletion",
				zap.String("queue", sq.QueuePath),
				zap.Error(err))
		}
		if len(sq.children) > 0 {
			for _, child := range sq.children {
				child.MarkQueueForRemoval()
			}
		}
	}
}

// Get a child queue based on the name of the child.
func (sq *Queue) GetChildQueue(name string) *Queue {
	sq.RLock()
	defer sq.RUnlock()

	return sq.children[name]
}

// Remove the queue from the structure.
// Since nothing is allocated there shouldn't be anything referencing this queue any more.
// The real removal is removing the queue from the parent's child list, use read lock on the queue
func (sq *Queue) RemoveQueue() bool {
	sq.RLock()
	defer sq.RUnlock()
	// cannot remove a managed queue that is running
	if sq.isManaged && sq.IsRunning() {
		return false
	}
	// cannot remove a queue that has children or applications assigned
	if len(sq.children) > 0 || len(sq.applications) > 0 {
		return false
	}
	log.Logger().Info("removing queue", zap.String("queue", sq.QueuePath))
	// root is always managed and is the only queue with a nil parent: no need to guard
	sq.parent.removeChildQueue(sq.Name)
	return true
}

// Is this queue a leaf or not (i.e parent)
func (sq *Queue) IsLeafQueue() bool {
	sq.RLock()
	defer sq.RUnlock()
	return sq.isLeaf
}

// Is this queue managed or not.
func (sq *Queue) IsManaged() bool {
	sq.RLock()
	defer sq.RUnlock()
	return sq.isManaged
}

// test only
func (sq *Queue) isRoot() bool {
	return sq.parent == nil
}

// Return the preempting resources for the queue
func (sq *Queue) GetPreemptingResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return sq.preempting
}

// Increment the number of resource marked for preemption in the queue.
func (sq *Queue) IncPreemptingResource(newAlloc *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()
	sq.preempting.AddTo(newAlloc)
}

// Decrement the number of resource marked for preemption in the queue.
func (sq *Queue) decPreemptingResource(newAlloc *resources.Resource) {
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
func (sq *Queue) setPreemptingResource(newAlloc *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()
	sq.preempting = newAlloc
}

// Increment the allocated resources for this queue (recursively)
// Guard against going over max resources if set
func (sq *Queue) IncAllocatedResource(alloc *resources.Resource, nodeReported bool) error {
	sq.Lock()
	defer sq.Unlock()

	// check this queue: failure stops checks if the allocation is not part of a node addition
	newAllocated := resources.Add(sq.allocatedResource, alloc)
	if !nodeReported {
		if !sq.maxResource.FitInMaxUndef(newAllocated) {
			return fmt.Errorf("allocation (%v) puts queue %s over maximum allocation (%v)",
				alloc, sq.QueuePath, sq.maxResource)
		}
	}
	// check the parent: need to pass before updating
	if sq.parent != nil {
		if err := sq.parent.IncAllocatedResource(alloc, nodeReported); err != nil {
			log.Logger().Error("parent queue exceeds maximum resource",
				zap.Any("allocationId", alloc),
				zap.String("maxResource", sq.maxResource.String()),
				zap.Error(err))
			return err
		}
	}
	// all OK update this queue
	sq.allocatedResource = newAllocated
	sq.updateUsedResourceMetrics()
	return nil
}

// Decrement the allocated resources for this queue (recursively)
// Guard against going below zero resources.
func (sq *Queue) DecAllocatedResource(alloc *resources.Resource) error {
	if sq == nil {
		return fmt.Errorf("queue is nil")
	}
	sq.Lock()
	defer sq.Unlock()

	// check this queue: failure stops checks
	if alloc != nil && !resources.FitIn(sq.allocatedResource, alloc) {
		return fmt.Errorf("released allocation (%v) is larger than '%s' queue allocation (%v)",
			alloc, sq.QueuePath, sq.allocatedResource)
	}
	// check the parent: need to pass before updating
	if sq.parent != nil {
		if err := sq.parent.DecAllocatedResource(alloc); err != nil {
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

// Return a sorted copy of the applications in the queue. Applications are sorted using the
// sorting type of the queue.
// Only applications with a pending resource request are considered.
// Lock free call all locks are taken when needed in called functions
func (sq *Queue) sortApplications(filterApps bool) []*Application {
	if !sq.IsLeafQueue() {
		return nil
	}
	// sort applications based on the sorting policy
	// some apps might be filtered out based on the policy specific conditions.
	// currently, only the stateAware policy does the filtering (based on app state).
	// if the filterApps flag is true, the app filtering is skipped while doing the sorting.
	queueSortType := sq.getSortType()
	if !filterApps && queueSortType == policies.StateAwarePolicy {
		// fallback to FIFO policy if the filterApps flag is false
		// this is to skip the app filtering in the StateAware policy sorting
		queueSortType = policies.FifoSortPolicy
	}
	return sortApplications(sq.GetCopyOfApps(), queueSortType, sq.GetGuaranteedResource())
}

// Return a sorted copy of the queues for this parent queue.
// Only queues with a pending resource request are considered. The queues are sorted using the
// sorting type for the parent queue.
// Lock free call all locks are taken when needed in called functions
func (sq *Queue) sortQueues() []*Queue {
	if sq.IsLeafQueue() {
		return nil
	}
	// Create a list of the queues with pending resources
	sortedQueues := make([]*Queue, 0)
	for _, child := range sq.GetCopyOfChildren() {
		// a stopped queue cannot be scheduled
		if child.IsStopped() {
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
func (sq *Queue) getHeadRoom() *resources.Resource {
	var parentHeadRoom *resources.Resource
	if sq.parent != nil {
		parentHeadRoom = sq.parent.getHeadRoom()
	}
	return sq.internalHeadRoom(parentHeadRoom)
}

// This function returns the max headRoom of a queue.
// It doesn't get the partition resources into the consideration.
// Used during scheduling in an auto scaling cluster.
func (sq *Queue) getMaxHeadRoom() *resources.Resource {
	var parentHeadRoom *resources.Resource
	if sq.parent != nil {
		parentHeadRoom = sq.parent.getMaxHeadRoom()
	} else {
		return nil
	}
	return sq.internalHeadRoom(parentHeadRoom)
}

func (sq *Queue) internalHeadRoom(parentHeadRoom *resources.Resource) *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	headRoom := sq.maxResource.Clone()

	// if we have no max set headroom is always the same as the parent
	if headRoom == nil {
		return parentHeadRoom
	}
	// calculate unused
	headRoom.SubFrom(sq.allocatedResource)
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
func (sq *Queue) GetMaxResource() *resources.Resource {
	// get the limit for the parent first and check against the queue's own
	var limit *resources.Resource
	if sq.parent != nil {
		limit = sq.parent.GetMaxResource()
	}
	return sq.internalGetMax(limit)
}

// This function returns the max size of a queue.
// It doesn't take the partition resources into the consideration.
// Used during scheduling in an auto scaling cluster.
func (sq *Queue) GetMaxQueueSet() *resources.Resource {
	// get the limit for the parent first and check against the queue's own
	var limit *resources.Resource
	if sq.parent == nil {
		return nil
	}
	limit = sq.parent.GetMaxQueueSet()
	return sq.internalGetMax(limit)
}

func (sq *Queue) internalGetMax(parentLimit *resources.Resource) *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	// no parent queue limit set, not even for root
	if parentLimit == nil {
		if sq.maxResource == nil {
			return nil
		}
		return sq.maxResource.Clone()
	}
	// parent limit set, no queue limit return parent
	if sq.maxResource == nil {
		return parentLimit
	}
	// calculate the smallest value for each type
	return resources.ComponentWiseMin(parentLimit, sq.maxResource)
}

// Set the max resource for root the queue.
// Should only happen on the root, all other queues get it from the config via properties.
func (sq *Queue) SetMaxResource(max *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()

	if sq.parent != nil {
		log.Logger().Warn("Max resources set on a queue that is not the root",
			zap.String("queueName", sq.QueuePath))
		return
	}
	sq.maxResource = max.Clone()
}

// Try allocate pending requests. This only gets called if there is a pending request on this queue or its children.
// This is a depth first algorithm: descend into the depth of the queue tree first. Child queues are sorted based on
// the configured queue sortPolicy. Queues without pending resources are skipped.
// Applications are sorted based on the application sortPolicy. Applications without pending resources are skipped.
// Lock free call this all locks are taken when needed in called functions
func (sq *Queue) TryAllocate(iterator func() interfaces.NodeIterator) *Allocation {
	if sq.IsLeafQueue() {
		// get the headroom
		headRoom := sq.getHeadRoom()
		// process the apps (filters out app without pending requests)
		for _, app := range sq.sortApplications(true) {
			alloc := app.tryAllocate(headRoom, iterator)
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
			alloc := child.TryAllocate(iterator)
			if alloc != nil {
				return alloc
			}
		}
	}
	return nil
}

// Try replace placeholder allocations. This only gets called if there is a pending request on this queue or its children.
// This is a depth first algorithm: descend into the depth of the queue tree first. Child queues are sorted based on
// the configured queue sortPolicy. Queues without pending resources are skipped.
// Applications are sorted based on the application sortPolicy. Applications without pending resources are skipped.
// Lock free call this all locks are taken when needed in called functions
func (sq *Queue) TryPlaceholderAllocate(iterator func() interfaces.NodeIterator, getnode func(string) *Node) *Allocation {
	if sq.IsLeafQueue() {
		// process the apps (filters out app without pending requests)
		for _, app := range sq.sortApplications(true) {
			alloc := app.tryPlaceholderAllocate(iterator, getnode)
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
			alloc := child.TryPlaceholderAllocate(iterator, getnode)
			if alloc != nil {
				return alloc
			}
		}
	}
	return nil
}

func (sq *Queue) GetQueueOutstandingRequests(total *[]*AllocationAsk) {
	if sq.IsLeafQueue() {
		headRoom := sq.getMaxHeadRoom()
		// while calculating outstanding requests, we do not need to filter apps.
		// e.g StateAware filters apps by state in order to schedule app one by one.
		// we calculates all the requests that can fit into the queues headroom,
		// all these requests are qualified to trigger the up scaling.
		for _, app := range sq.sortApplications(false) {
			app.getOutstandingRequests(headRoom, total)
		}
	} else {
		for _, child := range sq.sortQueues() {
			child.GetQueueOutstandingRequests(total)
		}
	}
}

// Try allocate reserved requests. This only gets called if there is a pending request on this queue or its children.
// This is a depth first algorithm: descend into the depth of the queue tree first. Child queues are sorted based on
// the configured queue sortPolicy. Queues without pending resources are skipped.
// Applications are currently NOT sorted and are iterated over in a random order.
// Lock free call this all locks are taken when needed in called functions
func (sq *Queue) TryReservedAllocate(iterator func() interfaces.NodeIterator) *Allocation {
	if sq.IsLeafQueue() {
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
				if app == nil {
					log.Logger().Debug("reservation(s) found but application did not exist in queue",
						zap.String("queueName", sq.QueuePath),
						zap.String("appID", appID))
					return nil
				}
				alloc := app.tryReservedAllocate(headRoom, iterator)
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
			alloc := child.TryReservedAllocate(iterator)
			if alloc != nil {
				return alloc
			}
		}
	}
	return nil
}

// Get a copy of the reserved app list
// locked to prevent race conditions from event updates
func (sq *Queue) getReservedApps() map[string]int {
	sq.RLock()
	defer sq.RUnlock()

	copied := make(map[string]int)
	for appID, numRes := range sq.reservedApps {
		copied[appID] = numRes
	}
	// increase the number of reservations for this app
	return copied
}

// Add an reserved app to the list.
// No checks this is only called when a reservation is processed using the app stored in the queue.
func (sq *Queue) Reserve(appID string) {
	sq.Lock()
	defer sq.Unlock()
	// increase the number of reservations for this app
	sq.reservedApps[appID]++
}

// Add an reserved app to the list.
// No checks this is only called when a reservation is processed using the app stored in the queue.
func (sq *Queue) UnReserve(appID string, releases int) {
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
func (sq *Queue) getApplication(appID string) *Application {
	sq.RLock()
	defer sq.RUnlock()
	return sq.applications[appID]
}

// get the queue sort type holding a lock
func (sq *Queue) getSortType() policies.SortPolicy {
	sq.RLock()
	defer sq.RUnlock()
	return sq.sortType
}

// Can the queue support task groups based on the sorting policy
// FIFO and StateAware can support this
// NOTE: this call does not make sense for a parent queue, and always returns false
func (sq *Queue) SupportTaskGroup() bool {
	sq.RLock()
	defer sq.RUnlock()
	if !sq.isLeaf {
		return false
	}
	return sq.sortType == policies.FifoSortPolicy || sq.sortType == policies.StateAwarePolicy
}

// update queue metrics when this is a leaf queue
func (sq *Queue) updateUsedResourceMetrics() {
	if sq.isLeaf {
		for k, v := range sq.allocatedResource.Resources {
			metrics.GetQueueMetrics(sq.QueuePath).SetQueueUsedResourceMetrics(k, float64(v))
		}
	}
}

func (sq *Queue) String() string {
	sq.RLock()
	defer sq.RUnlock()
	return fmt.Sprintf("{QueuePath: %s, State: %s, StateTime: %x, MaxResource: %s}",
		sq.QueuePath, sq.stateMachine.Current(), sq.stateTime, sq.maxResource)
}
