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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cloudera/yunikorn-core/pkg/common/configs"
	"github.com/cloudera/yunikorn-core/pkg/common/resources"
	"github.com/cloudera/yunikorn-core/pkg/common/security"
	"github.com/cloudera/yunikorn-core/pkg/log"
	"github.com/cloudera/yunikorn-core/pkg/metrics"
	"github.com/looplab/fsm"
	"go.uber.org/zap"
)

const (
	DOT        = "."
	DotReplace = "_dot_"
	// How to sort applications, valid options are fair / fifo
	ApplicationSortPolicy = "application.sort.policy"
)

// The queue structure as used throughout the scheduler
type QueueInfo struct {
	Name               string
	MaxResource        *resources.Resource // When not set, max = nil
	GuaranteedResource *resources.Resource // When not set, Guaranteed == 0
	Parent             *QueueInfo          // link to the parent queue

	Properties map[string]string // this should be treated as immutable the value is a merge of parent(s)
	// properties with the config for this queue only manipulated during creation

	// of the queue or via a queue configuration update

	// Private fields need protection
	adminACL          security.ACL          // admin ACL
	submitACL         security.ACL          // submit ACL
	allocatedResource *resources.Resource   // set based on allocation
	isLeaf            bool                  // this is a leaf queue or not (i.e. parent)
	isManaged         bool                  // queue is part of the config, not auto created
	stateMachine      *fsm.FSM              // the state of the queue for scheduling
	stateTime         time.Time             // last time the state was updated (needed for cleanup)
	children          map[string]*QueueInfo // list of direct children
	lock              sync.RWMutex          // lock for updating the queue
}

// Create a new queue from the configuration object.
// The configuration is validated before we call this: we should not see any errors.
func NewManagedQueue(conf configs.QueueConfig, parent *QueueInfo) (*QueueInfo, error) {
	qi := &QueueInfo{Name: strings.ToLower(conf.Name),
		Parent:            parent,
		isManaged:         true,
		isLeaf:            !conf.Parent,
		stateMachine:      newObjectState(),
		allocatedResource: resources.NewResource(),
	}

	err := qi.updateQueueProps(conf)
	if err != nil {
		return nil, fmt.Errorf("queue creation failed: %s", err)
	}

	// add the queue in the structure
	if parent != nil {
		err := parent.AddChildQueue(qi)
		if err != nil {
			return nil, fmt.Errorf("queue creation failed: %s", err)
		}
	}

	log.Logger().Debug("queue added",
		zap.String("queueName", qi.Name),
		zap.String("queuePath", qi.GetQueuePath()))
	return qi, nil
}

// Create a new queue unmanaged queue
// Rule base queue which might not fit in the structure or fail parsing
func NewUnmanagedQueue(name string, leaf bool, parent *QueueInfo) (*QueueInfo, error) {
	// name might not be checked do it here
	if !configs.QueueNameRegExp.MatchString(name) {
		return nil, fmt.Errorf("invalid queue name %s, a name must only have alphanumeric characters,"+
			" - or _, and be no longer than 64 characters", name)
	}
	// create the object
	qi := &QueueInfo{Name: strings.ToLower(name),
		Parent:            parent,
		isLeaf:            leaf,
		stateMachine:      newObjectState(),
		allocatedResource: resources.NewResource(),
	}
	// TODO set resources and properties on unmanaged queues
	// add the queue in the structure
	if parent != nil {
		err := parent.AddChildQueue(qi)
		if err != nil {
			return nil, fmt.Errorf("queue creation failed: %s", err)
		}
	}

	return qi, nil
}

// Handle the state event for the application.
// The state machine handles the locking.
func (qi *QueueInfo) HandleQueueEvent(event SchedulingObjectEvent) error {
	err := qi.stateMachine.Event(event.String(), qi.Name)
	// err is nil the state transition was done
	if err == nil {
		qi.stateTime = time.Now()
		return nil
	}
	// handle the same state transition not nil error (limit of fsm).
	if err.Error() == "no transition" {
		return nil
	}
	return err
}

func (qi *QueueInfo) GetAllocatedResource() *resources.Resource {
	qi.lock.RLock()
	defer qi.lock.RUnlock()

	return qi.allocatedResource
}

// Return if this is a leaf queue or not
func (qi *QueueInfo) IsLeafQueue() bool {
	return qi.isLeaf
}

// Return if this is a leaf queue or not
func (qi *QueueInfo) IsManaged() bool {
	return qi.isManaged
}

// Get the fully qualified path name
func (qi *QueueInfo) GetQueuePath() string {
	if qi.Parent == nil {
		return qi.Name
	} else {
		return qi.Parent.GetQueuePath() + DOT + qi.Name
	}
}

// Add a new child queue to this queue
// - can only add to a non leaf queue
// - cannot add when the queue is marked for deletion
// - if this is the first child initialise
func (qi *QueueInfo) AddChildQueue(child *QueueInfo) error {
	qi.lock.Lock()
	defer qi.lock.Unlock()
	if qi.isLeaf {
		return fmt.Errorf("cannot add a child queue to a leaf queue: %s", qi.Name)
	}
	if qi.IsDraining() {
		return fmt.Errorf("cannot add a child queue when queue is marked for deletion: %s", qi.Name)
	}
	// add the child (init if needed)
	if qi.children == nil {
		qi.children = make(map[string]*QueueInfo)
	}
	qi.children[child.Name] = child
	return nil
}

func (qi *QueueInfo) updateUsedResourceMetrics() {
	// update queue metrics when this is a leaf queue
	if qi.isLeaf {
		for k, v := range qi.allocatedResource.Resources {
			metrics.GetQueueMetrics(qi.GetQueuePath()).SetQueueUsedResourceMetrics(k, float64(v))
		}
	}
}

// Increment the allocated resources for this queue (recursively)
// Guard against going over max resources if the
func (qi *QueueInfo) IncAllocatedResource(alloc *resources.Resource, nodeReported bool) error {
	qi.lock.Lock()
	defer qi.lock.Unlock()

	// check this queue: failure stops checks if the allocation is not part of a node addition
	newAllocation := resources.Add(qi.allocatedResource, alloc)
	if !nodeReported {
		if qi.MaxResource != nil && !resources.FitIn(qi.MaxResource, newAllocation) {
			return fmt.Errorf("allocation (%v) puts queue %s over maximum allocation (%v)",
				alloc, qi.GetQueuePath(), qi.MaxResource)
		}
	}
	// check the parent: need to pass before updating
	if qi.Parent != nil {
		if err := qi.Parent.IncAllocatedResource(alloc, nodeReported); err != nil {
			log.Logger().Error("parent queue exceeds maximum resource",
				zap.Any("allocationId", alloc),
				zap.Any("maxResource", qi.MaxResource),
				zap.Error(err))
			return err
		}
	}
	// all OK update this queue
	qi.allocatedResource = newAllocation
	qi.updateUsedResourceMetrics()
	return nil
}

// Decrement the allocated resources for this queue (recursively)
// Guard against going below zero resources.
func (qi *QueueInfo) DecAllocatedResource(alloc *resources.Resource) error {
	qi.lock.Lock()
	defer qi.lock.Unlock()

	// check this queue: failure stops checks
	if alloc != nil && !resources.FitIn(qi.allocatedResource, alloc) {
		return fmt.Errorf("released allocation (%v) is larger than queue %s allocation (%v)",
			alloc, qi.GetQueuePath(), qi.allocatedResource)
	}
	// check the parent: need to pass before updating
	if qi.Parent != nil {
		if err := qi.Parent.DecAllocatedResource(alloc); err != nil {
			log.Logger().Error("released allocation is larger than parent queue max resource",
				zap.Any("allocationId", alloc),
				zap.Any("maxResource", qi.MaxResource),
				zap.Error(err))
			return err
		}
	}
	// all OK update the queue
	qi.allocatedResource = resources.Sub(qi.allocatedResource, alloc)
	qi.updateUsedResourceMetrics()
	return nil
}

func (qi *QueueInfo) GetCopyOfChildren() map[string]*QueueInfo {
	qi.lock.RLock()
	defer qi.lock.RUnlock()

	children := make(map[string]*QueueInfo)
	for k, v := range qi.children {
		children[k] = v
	}

	return children
}

// Remove a child from the list of children
// No checks are performed: if the child has been removed already it is a noop.
// This may only be called by the queue removal itself on the registered parent.
// Queue removal is always a bottom up action: leafs first then the parent.
func (qi *QueueInfo) removeChildQueue(name string) {
	qi.lock.Lock()
	defer qi.lock.Unlock()
	delete(qi.children, strings.ToLower(name))
}

// Remove the queue from the structure.
// Since nothing is allocated there shouldn't be anything referencing this queue any more.
// The real removal is removing the queue from the parent's child list
func (qi *QueueInfo) RemoveQueue() bool {
	qi.lock.Lock()
	defer qi.lock.Unlock()
	// cannot remove a managed queue that is running
	if qi.isManaged && qi.IsRunning() {
		return false
	}
	// cannot remove a queue that has children or allocated resources
	if len(qi.children) > 0 || !resources.IsZero(qi.allocatedResource) {
		return false
	}

	log.Logger().Info("removing queue", zap.String("queue", qi.Name))
	// root is always managed and is the only queue with a nil parent: no need to guard
	qi.Parent.removeChildQueue(qi.Name)
	return true
}

// Mark the managed queue for removal from the system.
// This can be executed multiple times and is only effective the first time.
// This is a noop on an unmanaged queue
func (qi *QueueInfo) MarkQueueForRemoval() {
	// need to lock for write as we don't want to add a queue while marking for removal
	qi.lock.Lock()
	defer qi.lock.Unlock()
	// Mark the managed queue for deletion: it is removed from the config let it drain.
	// Also mark all the managed children for deletion.
	if qi.isManaged {
		log.Logger().Info("marking managed queue for deletion",
			zap.String("queue", qi.GetQueuePath()))
		if err := qi.HandleQueueEvent(Remove); err != nil {
			log.Logger().Info("failed to marking managed queue for deletion",
				zap.String("queue", qi.GetQueuePath()),
				zap.Error(err))
		}
		if qi.children != nil || len(qi.children) > 0 {
			for _, child := range qi.children {
				child.MarkQueueForRemoval()
			}
		}
	}
}

// Update an existing managed queue based on the updated configuration
func (qi *QueueInfo) updateQueueProps(conf configs.QueueConfig) error {
	// Set the ACLs
	var err error
	qi.submitACL, err = security.NewACL(conf.SubmitACL)
	if err != nil {
		log.Logger().Error("parsing submit ACL failed this should not happen",
			zap.Error(err))
		return err
	}
	qi.adminACL, err = security.NewACL(conf.AdminACL)
	if err != nil {
		log.Logger().Error("parsing admin ACL failed this should not happen",
			zap.Error(err))
		return err
	}
	// Change from unmanaged to managed
	if !qi.isManaged {
		log.Logger().Info("changed un-managed queue to managed",
			zap.String("queue", qi.GetQueuePath()))
		qi.isManaged = true
	}

	// Make sure the parent flag is set correctly: config might expect auto parent type creation
	if len(conf.Queues) > 0 {
		qi.isLeaf = false
	}

	// Load the max resources
	maxResource, err := resources.NewResourceFromConf(conf.Resources.Max)
	if err != nil {
		log.Logger().Error("parsing failed on max resources this should not happen",
			zap.Error(err))
		return err
	}
	if len(maxResource.Resources) != 0 {
		qi.MaxResource = maxResource
	}

	// Load the guaranteed resources
	guaranteedResource, err := resources.NewResourceFromConf(conf.Resources.Guaranteed)
	if err != nil {
		log.Logger().Error("parsing failed on max resources this should not happen",
			zap.Error(err))
		return err
	}
	if len(guaranteedResource.Resources) != 0 {
		qi.GuaranteedResource = guaranteedResource
	}

	// Update Properties
	qi.Properties = conf.Properties
	if qi.Parent != nil && qi.Parent.Properties != nil {
		qi.Properties = mergeProperties(qi.Parent.Properties, conf.Properties)
	}

	return nil
}

// Merge the properties for the queue. This is only called when updating the queue from the configuration.
func mergeProperties(parent map[string]string, child map[string]string) map[string]string {
	merged := make(map[string]string)
	if parent != nil && len(parent) > 0 {
		for key, value := range parent {
			merged[key] = value
		}
	}
	if child != nil && len(child) > 0 {
		for key, value := range child {
			merged[key] = value
		}
	}
	return merged
}

// Is the queue marked for deletion and can only handle existing application requests.
// No new applications will be accepted.
func (qi *QueueInfo) IsDraining() bool {
	return qi.stateMachine.Current() == Draining.String()
}

// Is the queue in a normal active state.
func (qi *QueueInfo) IsRunning() bool {
	return qi.stateMachine.Current() == Active.String()
}

// Is the queue stopped, not active in scheduling at all.
func (qi *QueueInfo) IsStopped() bool {
	return qi.stateMachine.Current() == Stopped.String()
}

// Check if the user has access to the queue to submit an application recursively.
// This will check the submit ACL and the admin ACL.
func (qi *QueueInfo) CheckSubmitAccess(user security.UserGroup) bool {
	qi.lock.RLock()
	allow := qi.submitACL.CheckAccess(user) || qi.adminACL.CheckAccess(user)
	qi.lock.RUnlock()
	if !allow && qi.Parent != nil {
		allow = qi.Parent.CheckSubmitAccess(user)
	}
	return allow
}

// Check if the user has access to the queue for admin actions recursively.
func (qi *QueueInfo) CheckAdminAccess(user security.UserGroup) bool {
	qi.lock.RLock()
	allow := qi.adminACL.CheckAccess(user)
	qi.lock.RUnlock()
	if !allow && qi.Parent != nil {
		allow = qi.Parent.CheckAdminAccess(user)
	}
	return allow
}
