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
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	schedEvt "github.com/apache/yunikorn-core/pkg/scheduler/objects/events"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects/template"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

var (
	maxPreemptionsPerQueue = 10 // maximum number of asks to attempt to preempt for in a single queue
)

// Queue structure inside Scheduler
type Queue struct {
	QueuePath string // Fully qualified path for the queue
	Name      string // Queue name as in the config etc.

	// Private fields need protection
	sortType            policies.SortPolicy       // How applications (leaf) or queues (parents) are sorted
	children            map[string]*Queue         // Only for direct children, parent queue only
	childPriorities     map[string]int32          // cached priorities for child queues
	applications        map[string]*Application   // only for leaf queue
	appPriorities       map[string]int32          // cached priorities for application
	reservedApps        map[string]int            // applications reserved within this queue, with reservation count
	parent              *Queue                    // link back to the parent in the scheduler
	pending             *resources.Resource       // pending resource for the apps in the queue
	allocatedResource   *resources.Resource       // allocated resource for the apps in the queue
	preemptingResource  *resources.Resource       // preempting resource for the apps in the queue
	prioritySortEnabled bool                      // whether priority is used for request sorting
	priorityPolicy      policies.PriorityPolicy   // priority policy
	priorityOffset      int32                     // priority offset for this queue relative to others
	preemptionPolicy    policies.PreemptionPolicy // preemption policy
	preemptionDelay     time.Duration             // time before preemption is considered
	currentPriority     int32                     // the current scheduling priority of this queue

	// The queue properties should be treated as immutable the value is a merge of the
	// parent properties with the config for this queue only manipulated during creation
	// of the queue or via a queue configuration update.
	properties                     map[string]string
	adminACL                       security.ACL        // admin ACL
	submitACL                      security.ACL        // submit ACL
	maxResource                    *resources.Resource // When not set, max = nil
	guaranteedResource             *resources.Resource // When not set, Guaranteed == 0
	isLeaf                         bool                // this is a leaf queue or not (i.e. parent)
	isManaged                      bool                // queue is part of the config, not auto created
	stateMachine                   *fsm.FSM            // the state of the queue for scheduling
	stateTime                      time.Time           // last time the state was updated (needed for cleanup)
	maxRunningApps                 uint64
	runningApps                    uint64
	allocatingAcceptedApps         map[string]bool
	template                       *template.Template
	queueEvents                    *schedEvt.QueueEvents
	appQueueMapping                *AppQueueMapping // appID mapping to queues
	quotaChangePreemptionDelay     uint64
	quotaChangePreemptionStartTime time.Time
	isQuotaChangePreemptionRunning bool
	unschedAskBackoff              uint64
	askBackoffDelay                time.Duration

	locking.RWMutex
}

// newBlankQueue creates a new empty queue objects with all values initialised.
func newBlankQueue() *Queue {
	return &Queue{
		children:                       make(map[string]*Queue),
		childPriorities:                make(map[string]int32),
		applications:                   make(map[string]*Application),
		appPriorities:                  make(map[string]int32),
		reservedApps:                   make(map[string]int),
		allocatingAcceptedApps:         make(map[string]bool),
		properties:                     make(map[string]string),
		stateMachine:                   NewObjectState(),
		allocatedResource:              resources.NewResource(),
		preemptingResource:             resources.NewResource(),
		pending:                        resources.NewResource(),
		currentPriority:                configs.MinPriority,
		prioritySortEnabled:            true,
		preemptionDelay:                configs.DefaultPreemptionDelay,
		preemptionPolicy:               policies.DefaultPreemptionPolicy,
		quotaChangePreemptionDelay:     0,
		quotaChangePreemptionStartTime: time.Time{},
		askBackoffDelay:                configs.DefaultAskBackOffDelay,
	}
}

// NewConfiguredQueue creates a new queue from scratch based on the configuration
// lock free as it cannot be referenced yet.
// If the silence flag is set to true, the function will neither log the queue creation nor send a queue event.
func NewConfiguredQueue(conf configs.QueueConfig, parent *Queue, silence bool, appQueueMapping *AppQueueMapping) (*Queue, error) {
	sq := newBlankQueue()
	sq.Name = strings.ToLower(conf.Name)
	sq.QueuePath = strings.ToLower(conf.Name)
	sq.appQueueMapping = appQueueMapping
	if parent != nil {
		sq.QueuePath = parent.QueuePath + configs.DOT + sq.Name
	}
	sq.parent = parent
	sq.isManaged = true
	sq.maxRunningApps = conf.MaxApplications
	sq.updateMaxRunningAppsMetrics()

	// update the properties
	if err := sq.applyConf(conf, silence); err != nil {
		return nil, errors.Join(errors.New("configured queue creation failed: "), err)
	}

	// add to the parent, we might have an overall lock already
	// still need to make sure we lock the parent so we do not interfere with scheduling
	if parent != nil {
		// pull the properties from the parent that should be set on the child
		sq.mergeProperties(parent.getProperties(), conf.Properties)
		sq.UpdateQueueProperties()
		err := parent.addChildQueue(sq)
		if err != nil {
			return nil, errors.Join(errors.New("configured queue creation failed: "), err)
		}
	} else {
		sq.UpdateQueueProperties()
	}

	if !silence {
		sq.queueEvents = schedEvt.NewQueueEvents(events.GetEventSystem())
		log.Log(log.SchedQueue).Info("configured queue added to scheduler",
			zap.String("queueName", sq.QueuePath))
		sq.queueEvents.SendNewQueueEvent(sq.QueuePath, sq.isManaged)
	}
	return sq, nil
}

// NewRecoveryQueue creates a recovery queue if it does not exist. The recovery queue
// is a dynamic queue, but has an invalid name so that it cannot be directly referenced.
func NewRecoveryQueue(parent *Queue, appQueueMapping *AppQueueMapping) (*Queue, error) {
	if parent == nil {
		return nil, errors.New("recovery queue cannot be created with nil parent")
	}
	if parent.GetQueuePath() != configs.RootQueue {
		return nil, fmt.Errorf("recovery queue cannot be created with non-root parent: %s", parent.GetQueuePath())
	}
	queue, err := newDynamicQueueInternal(common.RecoveryQueue, true, parent, appQueueMapping)
	if err == nil {
		queue.Lock()
		defer queue.Unlock()
		queue.submitACL = security.ACL{}
		queue.sortType = policies.FifoSortPolicy
	}
	return queue, err
}

// NewDynamicQueue creates a new queue to be added to the system based on the placement rules
// A dynamically added queue can never be the root queue so parent must be set
// lock free as it cannot be referenced yet
func NewDynamicQueue(name string, leaf bool, parent *Queue, appQueueMapping *AppQueueMapping) (*Queue, error) {
	// fail without a parent
	if parent == nil {
		return nil, fmt.Errorf("dynamic queue can not be added without parent: %s", name)
	}
	// name might not be checked do it here
	if err := configs.IsQueueNameValid(name); err != nil {
		return nil, err
	}
	if name == common.RecoveryQueue {
		return nil, fmt.Errorf("dynamic queue cannot be root.@recovery@")
	}
	return newDynamicQueueInternal(name, leaf, parent, appQueueMapping)
}

func newDynamicQueueInternal(name string, leaf bool, parent *Queue, appQueueMapping *AppQueueMapping) (*Queue, error) {
	sq := newBlankQueue()
	sq.Name = strings.ToLower(name)
	sq.QueuePath = parent.QueuePath + configs.DOT + sq.Name
	sq.parent = parent
	sq.isManaged = false
	sq.isLeaf = leaf
	sq.appQueueMapping = appQueueMapping

	// add to the parent, we might have a partition lock already
	// still need to make sure we lock the parent so we do not interfere with scheduling
	err := parent.addChildQueue(sq)
	if err != nil {
		return nil, errors.Join(errors.New("dynamic queue creation failed: "), err)
	}

	sq.UpdateQueueProperties()
	sq.queueEvents = schedEvt.NewQueueEvents(events.GetEventSystem())
	log.Log(log.SchedQueue).Info("dynamic queue added to scheduler",
		zap.String("queueName", sq.QueuePath))
	sq.queueEvents.SendNewQueueEvent(sq.QueuePath, sq.isManaged)

	return sq, nil
}

// applyTemplate uses input template to initialize properties, maxResource, and guaranteedResource
func (sq *Queue) applyTemplate(childTemplate *template.Template) {
	sq.maxRunningApps = childTemplate.GetMaxApplications()
	sq.properties = childTemplate.GetProperties()
	// the resources in template are already checked
	sq.guaranteedResource = childTemplate.GetGuaranteedResource()
	sq.maxResource = childTemplate.GetMaxResource()
	// update metrics for guaranteed and max resource
	sq.updateGuaranteedResourceMetrics()
	sq.updateMaxResourceMetrics()
	sq.updateMaxRunningAppsMetrics()
}

// getProperties returns a copy of the properties for this queue
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

// mergeProperties merges the properties from the parent queue and the config in the set from new queue
// lock free call
func (sq *Queue) mergeProperties(parent, config map[string]string) {
	// clean out all existing values (handles update case)
	sq.properties = make(map[string]string)
	// Set the parent properties
	if len(parent) != 0 {
		for key, value := range parent {
			sq.properties[key] = filterParentProperty(key, value)
		}
	}
	// merge the config properties
	if len(config) > 0 {
		for key, value := range config {
			sq.properties[key] = value
		}
	}
}

func preemptionDelay(value string) (time.Duration, error) {
	result, err := time.ParseDuration(value)
	if err != nil {
		return configs.DefaultPreemptionDelay, err
	}
	if int64(result) <= int64(0) {
		return configs.DefaultPreemptionDelay, fmt.Errorf("%s must be positive: %s", configs.PreemptionDelay, value)
	}
	return result, nil
}

func priorityOffset(value string) (int32, error) {
	intValue, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(intValue), nil
}

func applicationSortPriorityEnabled(value string) (bool, error) {
	switch strings.ToLower(value) {
	case configs.ApplicationSortPriorityEnabled:
		return true, nil
	case configs.ApplicationSortPriorityDisabled:
		return false, nil
	default:
		return true, fmt.Errorf("unknown %s value: %s", configs.ApplicationSortPriority, value)
	}
}

// filterParentProperty modifies values from parent queues where necessary
func filterParentProperty(key string, value string) string {
	switch key {
	case configs.PriorityPolicy:
		// default is always used unless explicitly specified
		return policies.DefaultPriorityPolicy.String()
	case configs.PriorityOffset:
		// priority offsets are not inherited as they are additive
		return "0"
	case configs.PreemptionPolicy:
		// only 'disabled' should be allowed to propagate
		if pol, err := policies.PreemptionPolicyFromString(value); err != nil || pol != policies.DisabledPreemptionPolicy {
			return policies.DefaultPreemptionPolicy.String()
		}
	}
	return value
}

func unschedulableAskBackoff(value string) (uint64, error) {
	intValue, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, err
	}
	return intValue, nil
}

func backoffDelay(value string) (time.Duration, error) {
	result, err := time.ParseDuration(value)
	if err != nil {
		return configs.DefaultAskBackOffDelay, err
	}
	if int64(result) <= int64(0) {
		return configs.DefaultAskBackOffDelay, fmt.Errorf("%s must be positive: %s", configs.ApplicationUnschedulableAsksBackoffDelay, value)
	}
	return result, nil
}

// ApplyConf is the locked version of applyConf
func (sq *Queue) ApplyConf(conf configs.QueueConfig) error {
	sq.Lock()
	defer sq.Unlock()
	return sq.applyConf(conf, false)
}

// applyConf applies all the properties to the queue from the config.
// lock free call, must be called holding the queue lock or during create only.
// If the silence flag is set to true, the function will not log when setting users and groups.
func (sq *Queue) applyConf(conf configs.QueueConfig, silence bool) error {
	// Set the ACLs
	var err error
	sq.submitACL, err = security.NewACL(conf.SubmitACL, silence)
	if err != nil {
		log.Log(log.SchedQueue).Error("parsing submit ACL failed this should not happen",
			zap.Error(err))
		return err
	}
	sq.adminACL, err = security.NewACL(conf.AdminACL, silence)
	if err != nil {
		log.Log(log.SchedQueue).Error("parsing admin ACL failed this should not happen",
			zap.Error(err))
		return err
	}
	// Change from unmanaged to managed
	if !sq.isManaged {
		log.Log(log.SchedQueue).Info("changed dynamic queue to managed",
			zap.String("queue", sq.QueuePath))
		sq.isManaged = true
	}

	// if the queue is marked for removal reverse that state
	if !sq.IsRunning() {
		err = sq.handleQueueEvent(Start)
		if err != nil {
			log.Log(log.SchedQueue).Info("managed queue state change failed",
				zap.String("queue", sq.QueuePath))
		}
	}
	prevLeaf := sq.isLeaf
	sq.isLeaf = !conf.Parent
	// Make sure the parent flag is set correctly: config might expect auto parent type creation
	if len(conf.Queues) > 0 {
		sq.isLeaf = false
	}

	if prevLeaf != sq.isLeaf && sq.queueEvents != nil {
		sq.queueEvents.SendTypeChangedEvent(sq.QueuePath, sq.isLeaf)
	}

	if !sq.isLeaf {
		if err = sq.setTemplate(conf.ChildTemplate); err != nil {
			return err
		}
	}

	// Load the max & guaranteed resources and maxApps for all but the root queue
	if sq.Name != configs.RootQueue {
		oldMaxResource := sq.maxResource
		if err = sq.setResourcesFromConf(conf.Resources); err != nil {
			return err
		}
		sq.maxRunningApps = conf.MaxApplications
		sq.updateMaxRunningAppsMetrics()
		sq.setPreemptionSettings(oldMaxResource, conf)
	}

	sq.properties = conf.Properties
	return nil
}

// setPreemptionSettings Set Quota change preemption settings
func (sq *Queue) setPreemptionSettings(oldMaxResource *resources.Resource, conf configs.QueueConfig) {
	newMaxResource, err := resources.NewResourceFromConf(conf.Resources.Max)
	if err != nil {
		log.Log(log.SchedQueue).Error("parsing failed on max resources this should not happen",
			zap.String("queue", sq.QueuePath),
			zap.Error(err))
		return
	}

	switch {
	// Set max res earlier but not now
	case resources.IsZero(newMaxResource) && !resources.IsZero(oldMaxResource):
		sq.quotaChangePreemptionDelay = 0
		sq.quotaChangePreemptionStartTime = time.Time{}
		// Set max res now but not earlier
	case !resources.IsZero(newMaxResource) && resources.IsZero(oldMaxResource) && conf.Preemption.Delay != 0:
		sq.quotaChangePreemptionDelay = conf.Preemption.Delay
		sq.quotaChangePreemptionStartTime = time.Now().Add(time.Duration(int64(sq.quotaChangePreemptionDelay)) * time.Second) //nolint:gosec
		// Set max res earlier and now as well
	default:
		switch {
		// Quota decrease
		case resources.StrictlyGreaterThan(oldMaxResource, newMaxResource) && conf.Preemption.Delay != 0:
			sq.quotaChangePreemptionDelay = conf.Preemption.Delay
			sq.quotaChangePreemptionStartTime = time.Now().Add(time.Duration(sq.quotaChangePreemptionDelay) * time.Second) //nolint:gosec
			// Quota increase
		case resources.StrictlyGreaterThan(newMaxResource, oldMaxResource) && conf.Preemption.Delay != 0:
			sq.quotaChangePreemptionDelay = 0
			sq.quotaChangePreemptionStartTime = time.Time{}
			// Quota remains as is but delay has changed
		case resources.Equals(oldMaxResource, newMaxResource) && conf.Preemption.Delay != 0 && sq.quotaChangePreemptionDelay != conf.Preemption.Delay:
			sq.quotaChangePreemptionDelay = conf.Preemption.Delay
			sq.quotaChangePreemptionStartTime = time.Now().Add(time.Duration(sq.quotaChangePreemptionDelay) * time.Second) //nolint:gosec
		default:
			// noop
		}
	}
}

// resetPreemptionSettings Reset Quota change preemption settings
func (sq *Queue) resetPreemptionSettings() {
	sq.Lock()
	defer sq.Unlock()
	sq.quotaChangePreemptionDelay = 0
	sq.quotaChangePreemptionStartTime = time.Time{}
}

// shouldTriggerPreemption Should preemption be triggered or not to enforce new max quota?
func (sq *Queue) shouldTriggerPreemption() bool {
	sq.RLock()
	defer sq.RUnlock()
	return sq.quotaChangePreemptionDelay != 0 && !sq.quotaChangePreemptionStartTime.IsZero() && time.Now().After(sq.quotaChangePreemptionStartTime)
}

// getPreemptionSettings Get preemption settings. Only for testing
func (sq *Queue) getPreemptionSettings() (uint64, time.Time) {
	sq.RLock()
	defer sq.RUnlock()
	return sq.quotaChangePreemptionDelay, sq.quotaChangePreemptionStartTime
}

// setResourcesFromConf sets the maxResource and guaranteedResource of the queue from the config.
func (sq *Queue) setResourcesFromConf(resource configs.Resources) error {
	maxResource, err := resources.NewResourceFromConf(resource.Max)
	if err != nil {
		log.Log(log.SchedQueue).Error("parsing failed on max resources this should not happen",
			zap.String("queue", sq.QueuePath),
			zap.Error(err))
		return err
	}

	var guaranteedResource *resources.Resource
	guaranteedResource, err = resources.NewResourceFromConf(resource.Guaranteed)
	if err != nil {
		log.Log(log.SchedQueue).Error("parsing failed on guaranteed resources this should not happen",
			zap.String("queue", sq.QueuePath),
			zap.Error(err))
		return err
	}
	sq.setResources(guaranteedResource, maxResource)
	return nil
}

func (sq *Queue) setResources(guaranteedResource, maxResource *resources.Resource) {
	switch {
	case resources.StrictlyGreaterThanZero(maxResource):
		log.Log(log.SchedQueue).Debug("setting max resources",
			zap.String("queue", sq.QueuePath),
			zap.Stringer("current", sq.maxResource),
			zap.Stringer("new", maxResource))
		if !resources.Equals(sq.maxResource, maxResource) && sq.queueEvents != nil {
			sq.queueEvents.SendMaxResourceChangedEvent(sq.QueuePath, maxResource)
		}
		sq.maxResource = maxResource
		sq.updateMaxResourceMetrics()
	case sq.maxResource != nil:
		log.Log(log.SchedQueue).Debug("setting max resources",
			zap.String("queue", sq.QueuePath),
			zap.Stringer("current", sq.maxResource),
			zap.Stringer("new", maxResource))
		if sq.queueEvents != nil {
			sq.queueEvents.SendMaxResourceChangedEvent(sq.QueuePath, maxResource)
		}
		sq.maxResource = nil
		sq.updateMaxResourceMetrics()
	default:
		log.Log(log.SchedQueue).Debug("max resources setting ignored: cannot set zero max resources",
			zap.String("queue", sq.QueuePath))
	}

	switch {
	case resources.StrictlyGreaterThanZero(guaranteedResource):
		log.Log(log.SchedQueue).Debug("setting guaranteed resources",
			zap.String("queue", sq.QueuePath),
			zap.Stringer("current", sq.guaranteedResource),
			zap.Stringer("new", guaranteedResource))
		if !resources.Equals(sq.guaranteedResource, guaranteedResource) && sq.queueEvents != nil {
			sq.queueEvents.SendGuaranteedResourceChangedEvent(sq.QueuePath, guaranteedResource)
		}
		sq.guaranteedResource = guaranteedResource
		sq.updateGuaranteedResourceMetrics()
	case sq.guaranteedResource != nil:
		log.Log(log.SchedQueue).Debug("setting guaranteed resources",
			zap.String("queue", sq.QueuePath),
			zap.Stringer("current", sq.guaranteedResource),
			zap.Stringer("new", guaranteedResource))
		if sq.queueEvents != nil {
			sq.queueEvents.SendGuaranteedResourceChangedEvent(sq.QueuePath, guaranteedResource)
		}
		sq.guaranteedResource = nil
		sq.updateGuaranteedResourceMetrics()
	default:
		log.Log(log.SchedQueue).Debug("guaranteed resources setting ignored: cannot set zero guaranteed resources",
			zap.String("queue", sq.QueuePath))
	}
}

func (sq *Queue) SetResources(guaranteedResource, maxResource *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()
	sq.setResources(guaranteedResource, maxResource)
}

// SetMaxRunningApps allows setting the maximum running apps on a queue
func (sq *Queue) SetMaxRunningApps(maxApps uint64) {
	if sq == nil {
		return
	}
	sq.Lock()
	defer sq.Unlock()
	sq.maxRunningApps = maxApps
	sq.updateMaxRunningAppsMetrics()
}

// setTemplate sets the template on the queue based on the config.
// lock free call, must be called holding the queue lock or during create only
func (sq *Queue) setTemplate(conf configs.ChildTemplate) error {
	t, err := template.FromConf(&conf)
	if err != nil {
		return err
	}
	sq.template = t
	return nil
}

// UpdateQueueProperties updates the queue properties defined as text
func (sq *Queue) UpdateQueueProperties() {
	sq.Lock()
	defer sq.Unlock()
	if common.IsRecoveryQueue(sq.QueuePath) {
		// recovery queue properties should never be updated
		sq.sortType = policies.FifoSortPolicy
		return
	}
	if !sq.isLeaf {
		// set the sorting type for parent queues
		sq.sortType = policies.FairSortPolicy
	}
	// walk over all properties and process
	var err error
	for key, value := range sq.properties {
		switch key {
		case configs.ApplicationSortPolicy:
			if sq.isLeaf {
				sq.sortType, err = policies.SortPolicyFromString(value)
				if err != nil {
					log.Log(log.SchedQueue).Debug("application sort property configuration error",
						zap.Error(err))
				}
				// if it is not defined default to fifo
				if sq.sortType == policies.Undefined {
					sq.sortType = policies.FifoSortPolicy
				}
			}
		case configs.ApplicationSortPriority:
			sq.prioritySortEnabled, err = applicationSortPriorityEnabled(value)
			if err != nil {
				log.Log(log.SchedQueue).Debug("queue application sort priority configuration error",
					zap.Error(err))
			}
		case configs.PriorityOffset:
			sq.priorityOffset, err = priorityOffset(value)
			if err != nil {
				log.Log(log.SchedQueue).Debug("queue priority offset configuration error",
					zap.Error(err))
			}
		case configs.PriorityPolicy:
			sq.priorityPolicy, err = policies.PriorityPolicyFromString(value)
			if err != nil {
				log.Log(log.SchedQueue).Debug("queue priority policy configuration error",
					zap.Error(err))
			}
		case configs.PreemptionPolicy:
			sq.preemptionPolicy, err = policies.PreemptionPolicyFromString(value)
			if err != nil {
				log.Log(log.SchedQueue).Debug("queue preemption policy configuration error",
					zap.Error(err))
			}
		case configs.PreemptionDelay:
			if sq.isLeaf {
				sq.preemptionDelay, err = preemptionDelay(value)
				if err != nil {
					log.Log(log.SchedQueue).Debug("preemption delay property configuration error",
						zap.Error(err))
				}
			}
		case configs.ApplicationUnschedulableAsksBackoff:
			unschedAskBackoff, err := unschedulableAskBackoff(value)
			if err != nil {
				log.Log(log.SchedQueue).Debug("unschedulable ask backoff configuration error",
					zap.Error(err))
			}
			sq.unschedAskBackoff = unschedAskBackoff
		case configs.ApplicationUnschedulableAsksBackoffDelay:
			askBackoffDelay, err := backoffDelay(value)
			if err != nil {
				log.Log(log.SchedQueue).Debug("unschedulable ask backoff delay configuration error",
					zap.Error(err))
			}
			sq.askBackoffDelay = askBackoffDelay
		default:
			// skip unknown properties just log them
			log.Log(log.SchedQueue).Debug("queue property skipped",
				zap.String("key", key),
				zap.String("value", value))
		}
	}
}

// GetQueuePath returns the fully qualified path of this queue.
func (sq *Queue) GetQueuePath() string {
	sq.RLock()
	defer sq.RUnlock()
	return sq.QueuePath
}

// IsDraining returns true if the queue in Draining state.
// Existing applications will still be scheduled
// No new applications will be accepted.
func (sq *Queue) IsDraining() bool {
	return sq.stateMachine.Is(Draining.String())
}

// IsRunning returns true if the queue in Active state.
func (sq *Queue) IsRunning() bool {
	return sq.stateMachine.Is(Active.String())
}

// IsStopped returns true if the queue in Stopped state.
// The queue is skipped for scheduling in this state.
func (sq *Queue) IsStopped() bool {
	return sq.stateMachine.Is(Stopped.String())
}

// CurrentState returns the current state of the queue in string form.
func (sq *Queue) CurrentState() string {
	return sq.stateMachine.Current()
}

// handleQueueEvent processes the state event for the queue.
// The state machine handles the locking.
func (sq *Queue) handleQueueEvent(event ObjectEvent) error {
	err := sq.stateMachine.Event(context.Background(), event.String(), sq.QueuePath)
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

// GetAllocatedResource returns a clone of the allocated resources for this queue.
func (sq *Queue) GetAllocatedResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return sq.allocatedResource.Clone()
}

// GetPreemptingResource returns a clone of the preempting resources for this queue.
func (sq *Queue) GetPreemptingResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return sq.preemptingResource.Clone()
}

// GetGuaranteedResource returns a clone of the guaranteed resource for the queue.
func (sq *Queue) GetGuaranteedResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return sq.guaranteedResource
}

// GetMaxApps returns the maximum number of applications that can run in this queue.
func (sq *Queue) GetMaxApps() uint64 {
	sq.RLock()
	defer sq.RUnlock()
	return sq.maxRunningApps
}

// GetActualGuaranteedResources returns the actual (including parent) guaranteed resources for the queue.
func (sq *Queue) GetActualGuaranteedResource() *resources.Resource {
	if sq == nil {
		return resources.NewResource()
	}
	parentGuaranteed := sq.parent.GetActualGuaranteedResource()
	sq.RLock()
	defer sq.RUnlock()
	return resources.ComponentWiseMin(sq.guaranteedResource, parentGuaranteed)
}

func (sq *Queue) GetPreemptionDelay() time.Duration {
	sq.RLock()
	defer sq.RUnlock()
	return sq.preemptionDelay
}

// CheckSubmitAccess checks if the user has access to the queue to submit an application.
// The check is performed recursively: i.e. access to the parent allows access to this queue.
// This will check both submitACL and adminACL.
func (sq *Queue) CheckSubmitAccess(user security.UserGroup) bool {
	if common.IsRecoveryQueue(sq.QueuePath) {
		// recovery queue can never pass ACL checks
		return false
	}
	sq.RLock()
	allow := sq.submitACL.CheckAccess(user) || sq.adminACL.CheckAccess(user)
	sq.RUnlock()
	if !allow && sq.parent != nil {
		allow = sq.parent.CheckSubmitAccess(user)
	}
	return allow
}

// CheckAdminAccess checks if the user has access to the queue to perform administrative actions.
// The check is performed recursively: i.e. access to the parent allows access to this queue.
func (sq *Queue) CheckAdminAccess(user security.UserGroup) bool {
	sq.RLock()
	allow := sq.adminACL.CheckAccess(user)
	sq.RUnlock()
	if !allow && sq.parent != nil {
		allow = sq.parent.CheckAdminAccess(user)
	}
	return allow
}

// GetPartitionQueueDAOInfo returns the queue hierarchy as an object for a REST call.
// Include is false, which means that returns the specified queue object, but does not return the children of the specified queue.
func (sq *Queue) GetPartitionQueueDAOInfo(include bool) dao.PartitionQueueDAOInfo {
	queueInfo := dao.PartitionQueueDAOInfo{}
	children := sq.GetCopyOfChildren()
	if include {
		queueInfo.Children = make([]dao.PartitionQueueDAOInfo, 0, len(children))
		for _, child := range children {
			queueInfo.Children = append(queueInfo.Children, child.GetPartitionQueueDAOInfo(true))
		}
	}
	// we have held the read lock so following method should not take lock again.
	queueInfo.HeadRoom = sq.getHeadRoom().DAOMap()
	sq.RLock()
	defer sq.RUnlock()

	for _, child := range children {
		queueInfo.ChildNames = append(queueInfo.ChildNames, child.QueuePath)
	}
	queueInfo.QueueName = sq.QueuePath
	queueInfo.Status = sq.stateMachine.Current()
	queueInfo.PendingResource = sq.pending.DAOMap()
	queueInfo.MaxResource = sq.maxResource.DAOMap()
	queueInfo.GuaranteedResource = sq.guaranteedResource.DAOMap()
	queueInfo.AllocatedResource = sq.allocatedResource.DAOMap()
	queueInfo.PreemptingResource = sq.preemptingResource.DAOMap()
	queueInfo.IsLeaf = sq.isLeaf
	queueInfo.IsManaged = sq.isManaged
	queueInfo.CurrentPriority = sq.getCurrentPriority()
	queueInfo.TemplateInfo = sq.template.GetTemplateInfo()
	queueInfo.AbsUsedCapacity = resources.CalculateAbsUsedCapacity(sq.maxResource, sq.allocatedResource).DAOMap()
	queueInfo.SortingPolicy = sq.sortType.String()
	queueInfo.PrioritySorting = sq.prioritySortEnabled
	queueInfo.PreemptionEnabled = sq.preemptionPolicy != policies.DisabledPreemptionPolicy
	queueInfo.IsPreemptionFence = sq.preemptionPolicy == policies.FencePreemptionPolicy
	queueInfo.PreemptionDelay = sq.preemptionDelay.String()
	queueInfo.IsPriorityFence = sq.priorityPolicy == policies.FencePriorityPolicy
	queueInfo.PriorityOffset = sq.priorityOffset
	queueInfo.Properties = make(map[string]string)
	for k, v := range sq.properties {
		queueInfo.Properties[k] = v
	}
	if sq.parent == nil {
		queueInfo.Parent = ""
	} else {
		queueInfo.Parent = sq.QueuePath[:strings.LastIndex(sq.QueuePath, configs.DOT)]
	}
	queueInfo.MaxRunningApps = sq.maxRunningApps
	queueInfo.RunningApps = sq.runningApps
	queueInfo.AllocatingAcceptedApps = make([]string, 0)
	for appID, result := range sq.allocatingAcceptedApps {
		if result {
			queueInfo.AllocatingAcceptedApps = append(queueInfo.AllocatingAcceptedApps, appID)
		}
	}
	return queueInfo
}

// GetPendingResource returns the pending resources for this queue.
func (sq *Queue) GetPendingResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return sq.pending
}

// incPendingResource increments pending resource of this queue and its parents.
func (sq *Queue) incPendingResource(delta *resources.Resource) {
	// update the parent
	if sq.parent != nil {
		sq.parent.incPendingResource(delta)
	}
	// update this queue
	sq.Lock()
	defer sq.Unlock()
	sq.pending = resources.Add(sq.pending, delta)
	sq.updatePendingResourceMetrics()
}

// decPendingResource decrements pending resource of this queue and its parents.
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
		log.Log(log.SchedQueue).Warn("Pending resources went negative",
			zap.String("queueName", sq.QueuePath),
			zap.Error(err))
	} else {
		sq.updatePendingResourceMetrics()
		// We should update the metrics before pruning the resource.
		// For example:
		// If we prune the resource first and the resource become nil after pruning,
		// the metrics will not be updated with nil resource, this is not expected.
		sq.pending.Prune()
	}
}

// AddApplication adds the application to the queue. All checks are assumed to have passed before we get here.
// No update of pending resource is needed as it should not have any requests yet.
// Replaces the existing application without further checks.
func (sq *Queue) AddApplication(app *Application) {
	sq.Lock()
	defer sq.Unlock()
	appID := app.ApplicationID
	sq.applications[appID] = app
	sq.queueEvents.SendNewApplicationEvent(sq.QueuePath, appID)
}

// RemoveApplication removes the app from the list of tracked applications. Make sure that the app
// is assigned to this queue and not removed yet.
// If not found this call is a noop
func (sq *Queue) RemoveApplication(app *Application) {
	// clean up any outstanding pending resources
	appID := app.ApplicationID
	if !sq.appExists(appID) {
		log.Log(log.SchedQueue).Debug("Application not found while removing from queue",
			zap.String("queueName", sq.QueuePath),
			zap.String("applicationID", appID))
		return
	}
	sq.queueEvents.SendRemoveApplicationEvent(sq.QueuePath, appID)
	if appPending := app.GetPendingResource(); !resources.IsZero(appPending) {
		sq.decPendingResource(appPending)
	}
	// clean up the allocated resource
	if appAllocated := app.GetAllocatedResource(); !resources.IsZero(appAllocated) {
		if err := sq.DecAllocatedResource(appAllocated); err != nil {
			log.Log(log.SchedQueue).Warn("failed to release allocated resources from queue",
				zap.String("appID", appID),
				zap.Error(err))
		}
	}
	// clean up the allocated placeholder resource
	if phAllocated := app.GetPlaceholderResource(); !resources.IsZero(phAllocated) {
		if err := sq.DecAllocatedResource(phAllocated); err != nil {
			log.Log(log.SchedQueue).Warn("failed to release placeholder resources from queue",
				zap.String("appID", appID),
				zap.Error(err))
		}
	}
	// clean up preempting resources
	preempting := resources.NewResource()
	for _, alloc := range app.GetAllAllocations() {
		if alloc.IsPreempted() {
			preempting.AddTo(alloc.GetAllocatedResource())
		}
	}
	if !resources.IsZero(preempting) {
		sq.DecPreemptingResource(preempting)
	}

	sq.Lock()
	delete(sq.applications, appID)
	delete(sq.appPriorities, appID)
	delete(sq.allocatingAcceptedApps, appID)
	priority := sq.recalculatePriority()
	sq.Unlock()
	app.appEvents.SendRemoveApplicationEvent(appID)

	sq.parent.UpdateQueuePriority(sq.Name, priority)

	log.Log(log.SchedQueue).Info("Application completed and removed from queue",
		zap.String("queueName", sq.QueuePath),
		zap.String("applicationID", appID))
}

func (sq *Queue) appExists(appID string) bool {
	sq.RLock()
	defer sq.RUnlock()

	_, ok := sq.applications[appID]
	return ok
}

// GetCopyOfApps gets a shallow copy of all non-completed apps holding the lock
func (sq *Queue) GetCopyOfApps() map[string]*Application {
	sq.RLock()
	defer sq.RUnlock()
	appsCopy := make(map[string]*Application, len(sq.applications))
	for appID, app := range sq.applications {
		appsCopy[appID] = app
	}
	return appsCopy
}

// GetCopyOfChildren return a shallow copy of the child queue map.
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

// IsEmpty returns true if a queue is empty based on the following definition:
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

// removeChildQueue removes a child queue from this queue.
// No checks are performed: if the child has been removed already it is a noop.
// This may only be called by the queue removal itself on the registered parent.
// Queue removal is always a bottom up action: leaves first then the parent.
func (sq *Queue) removeChildQueue(name string) {
	sq.Lock()
	delete(sq.children, name)
	delete(sq.childPriorities, name)
	priority := sq.recalculatePriority()
	sq.Unlock()

	sq.parent.UpdateQueuePriority(sq.Name, priority)
}

// addChildQueue add a child queue to this queue.
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
	sq.childPriorities[child.Name] = child.getCurrentPriority()

	if child.isLeaf {
		// managed (configured) leaf queue can't use template
		if child.isManaged {
			return nil
		}
		// try to use template if it is not nil
		if sq.template != nil {
			log.Log(log.SchedQueue).Debug("applying child template to new leaf queue",
				zap.String("child queue", child.QueuePath),
				zap.String("parent queue", sq.QueuePath),
				zap.Any("template", sq.template))
			child.applyTemplate(sq.template)
		}
		return nil
	}
	// don't override the template of non-leaf queue
	if child.template == nil {
		child.template = sq.template
		log.Log(log.SchedQueue).Debug("new parent queue inheriting template from parent queue",
			zap.String("child queue", child.QueuePath),
			zap.String("parent queue", sq.QueuePath))
	}
	return nil
}

// MarkQueueForRemoval marks the managed queue for removal from the system.
// This can be executed multiple times and is only effective the first time.
// This is a noop on an unmanaged queue.
func (sq *Queue) MarkQueueForRemoval() {
	if !sq.IsManaged() {
		return
	}
	children := sq.GetCopyOfChildren()
	// Mark the managed queue for deletion: it is removed from the config let it drain.
	// Also mark all the managed children for deletion.
	log.Log(log.SchedQueue).Info("marking managed queue for deletion",
		zap.String("queue", sq.QueuePath))
	sq.doRemoveQueue()
	if len(sq.children) > 0 {
		for _, child := range children {
			child.MarkQueueForRemoval()
		}
	}
}

func (sq *Queue) doRemoveQueue() {
	sq.Lock()
	defer sq.Unlock()
	if err := sq.handleQueueEvent(Remove); err != nil {
		log.Log(log.SchedQueue).Warn("failed to mark managed queue for deletion",
			zap.String("queue", sq.QueuePath),
			zap.Error(err))
	}
}

// GetChildQueue returns a queue if the name exists in the child map as a key.
func (sq *Queue) GetChildQueue(name string) *Queue {
	sq.RLock()
	defer sq.RUnlock()

	return sq.children[name]
}

// RemoveQueue remove the queue from the structure.
// Since nothing is allocated there shouldn't be anything referencing this queue anymore.
// The real removal is the removal of the queue from the parent's child list.
// Use a read lock on this queue to prevent other changes but allow status checks etc.
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
	log.Log(log.SchedQueue).Info("removing queue", zap.String("queue", sq.QueuePath))
	sq.removeMetrics()
	// root is always managed and is the only queue with a nil parent: no need to guard
	sq.parent.removeChildQueue(sq.Name)
	sq.queueEvents.SendRemoveQueueEvent(sq.QueuePath, sq.isManaged)
	return true
}

// IsLeafQueue returns true is the queue a leaf. Returns false for a parent queue.
func (sq *Queue) IsLeafQueue() bool {
	sq.RLock()
	defer sq.RUnlock()
	return sq.isLeaf
}

// IsManaged returns true for a managed queue. Returns false for a dynamic queue.
func (sq *Queue) IsManaged() bool {
	sq.RLock()
	defer sq.RUnlock()
	return sq.isManaged
}

// test only
func (sq *Queue) isRoot() bool {
	return sq.parent == nil
}

// TryIncAllocatedResource increments the allocated resources for this queue (recursively).
// Guard against going over max resources if set
func (sq *Queue) TryIncAllocatedResource(alloc *resources.Resource) error {
	// check this queue: failure stops checks if the allocation is not part of a node addition
	if !sq.allocatedResFits(alloc) {
		return fmt.Errorf("allocation (%v) puts queue '%s' over maximum allocation (%v), current usage (%v)",
			alloc, sq.QueuePath, sq.maxResource, sq.allocatedResource)
	}
	// check the parent: need to pass before updating
	if sq.parent != nil {
		if err := sq.parent.TryIncAllocatedResource(alloc); err != nil {
			// only log the warning if we get to the leaf: otherwise we could spam the log with the same message
			// each time we return from a recursive call. Worst case (hierarchy depth-1) times.
			if sq.isLeaf {
				log.Log(log.SchedQueue).Warn("parent queue exceeds maximum resource",
					zap.String("leafQueue", sq.QueuePath),
					zap.Stringer("allocationRequest", alloc),
					zap.Stringer("queueUsage", sq.allocatedResource),
					zap.Stringer("maxResource", sq.maxResource),
					zap.Error(err))
			}
			return err
		}
	}
	sq.Lock()
	defer sq.Unlock()
	// all OK update this queue
	sq.allocatedResource = resources.Add(sq.allocatedResource, alloc)
	sq.updateAllocatedResourceMetrics()
	return nil
}

// IncAllocatedResource increments the allocated resources for this queue (recursively). No queue limits are checked.
func (sq *Queue) IncAllocatedResource(alloc *resources.Resource) {
	// fall through if nil
	if sq == nil {
		return
	}

	// update parent
	sq.parent.IncAllocatedResource(alloc)

	// update this queue
	sq.Lock()
	defer sq.Unlock()
	sq.allocatedResource = resources.Add(sq.allocatedResource, alloc)
	sq.updateAllocatedResourceMetrics()
}

// allocatedResFits adds the passed in resource to the allocatedResource of the queue and checks if it still fits in the
// queues' maximum. If the resource fits it returns true otherwise false.
// small helper method to access sq.maxResource+sq.allocatedResource and avoid Clone() call
func (sq *Queue) allocatedResFits(alloc *resources.Resource) bool {
	sq.RLock()
	defer sq.RUnlock()
	// on the root we want to reject a new allocation if it asks for resources not registered
	// so do not use the "undefined" flag, also handles pruned max for root
	if sq.isRoot() {
		return sq.maxResource.FitIn(resources.AddOnlyExisting(alloc, sq.allocatedResource))
	}
	// any other queue undefined is always good
	return sq.maxResource.FitInMaxUndef(resources.AddOnlyExisting(alloc, sq.allocatedResource))
}

// DecAllocatedResource decrement the allocated resources for this queue (recursively)
// Guard against going below zero resources.
func (sq *Queue) DecAllocatedResource(alloc *resources.Resource) error {
	if sq == nil {
		return fmt.Errorf("queue is nil")
	}

	// check this queue: failure stops checks
	if alloc != nil && !sq.resourceFitsAllocated(alloc) {
		return fmt.Errorf("released allocation (%v) is larger than '%s' queue allocation (%v)",
			alloc, sq.QueuePath, sq.allocatedResource)
	}
	// check the parent: need to pass before updating
	if sq.parent != nil {
		if err := sq.parent.DecAllocatedResource(alloc); err != nil {
			// only log the warning if we get to the leaf: otherwise we spam the log with the same message
			// each time we return from a recursive call. Worst case (hierarchy depth-1) times.
			if sq.isLeaf {
				log.Log(log.SchedQueue).Warn("released allocation is larger than parent queue allocated resource",
					zap.String("leafQueue", sq.QueuePath),
					zap.Stringer("allocationRequest", alloc),
					zap.Stringer("queueUsage", sq.allocatedResource),
					zap.Stringer("maxResource", sq.maxResource),
					zap.Error(err))
			}
			return err
		}
	}
	sq.Lock()
	defer sq.Unlock()
	// all OK update the queue
	sq.allocatedResource = resources.Sub(sq.allocatedResource, alloc)
	// We should update the metrics before pruning the resource.
	// For example:
	// If we prune the resource first and the resource become nil after pruning,
	// the metrics will not be updated with nil resource, this is not expected.
	sq.updateAllocatedResourceMetrics()
	sq.allocatedResource.Prune()
	return nil
}

// small helper method to access sq.allocatedResource and avoid Clone() call
func (sq *Queue) resourceFitsAllocated(res *resources.Resource) bool {
	sq.RLock()
	defer sq.RUnlock()
	return sq.allocatedResource.FitIn(res)
}

// IncPreemptingResource increments the preempting resources for this queue (recursively).
func (sq *Queue) IncPreemptingResource(alloc *resources.Resource) {
	if sq == nil {
		return
	}
	sq.Lock()
	defer sq.Unlock()
	sq.parent.IncPreemptingResource(alloc)
	sq.preemptingResource = resources.Add(sq.preemptingResource, alloc)
	sq.updatePreemptingResourceMetrics()
}

// DecPreemptingResource decrements the preempting resources for this queue (recursively).
func (sq *Queue) DecPreemptingResource(alloc *resources.Resource) {
	if sq == nil {
		return
	}
	sq.Lock()
	defer sq.Unlock()
	sq.parent.DecPreemptingResource(alloc)
	sq.preemptingResource = resources.Sub(sq.preemptingResource, alloc)
	sq.updatePreemptingResourceMetrics()
	// We should update the metrics before pruning the resource.
	// For example:
	// If we prune the resource first and the resource become nil after pruning,
	// the metrics will not be updated with nil resource, this is not expected.
	sq.preemptingResource.Prune()
}

func (sq *Queue) IsPrioritySortEnabled() bool {
	sq.RLock()
	defer sq.RUnlock()
	return sq.prioritySortEnabled
}

// sortApplications returns a sorted shallow copy of the applications in the queue.
// Applications are sorted using the sorting type of the queue.
// Only applications with a pending resource request are considered.
// Lock free call all locks are taken when needed in called functions
// If withPlaceholdersOnly is true, then only applications with at least one placeholder allocation are considered.
func (sq *Queue) sortApplications(withPlaceholdersOnly bool) []*Application {
	if !sq.IsLeafQueue() {
		return nil
	}

	apps := sq.GetCopyOfApps()
	if withPlaceholdersOnly {
		for key, app := range apps {
			if !app.HasPlaceholderAllocation() {
				delete(apps, key)
			}
		}
	}
	if len(apps) == 0 {
		return nil
	}

	// sort applications based on the sorting policy
	return sortApplications(apps, sq.getSortType(), sq.IsPrioritySortEnabled(), sq.GetGuaranteedResource())
}

// sortQueues returns a sorted shallow copy of the queues for this parent queue.
// Only queues with a pending resource request are considered. The queues are sorted using the
// sorting type for the parent queue.
// Lock free call all locks are taken when needed in called functions
func (sq *Queue) sortQueues() []*Queue {
	if sq.IsLeafQueue() {
		return nil
	}
	// Create a list of the queues with pending resources
	sortedQueues := make([]*Queue, 0)
	sortedMaxFairResources := make([]*resources.Resource, 0)
	for _, child := range sq.GetCopyOfChildren() {
		// a stopped queue cannot be scheduled
		if child.IsStopped() {
			continue
		}
		// queue must have pending resources to be considered for scheduling
		if resources.StrictlyGreaterThanZero(child.GetPendingResource()) {
			sortedQueues = append(sortedQueues, child)
			sortedMaxFairResources = append(sortedMaxFairResources, child.GetFairMaxResource())
		}
	}
	// Sort the queues
	sortQueue(sortedQueues, sortedMaxFairResources, sq.getSortType(), sq.IsPrioritySortEnabled())

	return sortedQueues
}

// GetSchedulingOrder returns a sorted shallow copy of the queues for this parent queue along with
// their eligible applications that would be tried in the current scheduling cycle.
// This follows the same logic as TryAllocate method.
// Only queues with a pending resource request are considered. The queues are sorted using the
// sorting type for the parent queue.
// Lock free call all locks are taken when needed in called functions
func (sq *Queue) GetSchedulingOrder() []*dao.SchedulingOrderDAO {
	if sq.IsLeafQueue() {
		// For leaf queues, return the queue with its eligible applications
		appIDs := make([]string, 0)

		// Process the apps (filters out app without pending requests) - same logic as TryAllocate
		for _, app := range sq.sortApplications(false) {
			runnableInQueue := sq.canRunApp(app.ApplicationID)
			runnableByUserLimit := ugm.GetUserManager().CanRunApp(sq.QueuePath, app.ApplicationID, app.user)
			app.updateRunnableStatus(runnableInQueue, runnableByUserLimit)
			if app.IsAccepted() && (!runnableInQueue || !runnableByUserLimit) {
				continue
			}
			appIDs = append(appIDs, app.ApplicationID)
		}

		// Only return this queue if it has eligible applications or pending resources
		if len(appIDs) > 0 || resources.StrictlyGreaterThanZero(sq.GetPendingResource()) {
			return []*dao.SchedulingOrderDAO{{
				QueueName:      sq.QueuePath,
				ApplicationIDs: appIDs,
			}}
		}
		return nil
	} else {
		// For parent queues, process child queues - same logic as TryAllocate
		result := make([]*dao.SchedulingOrderDAO, 0)

		// Process each sorted child queue using the original sortQueues method
		for _, child := range sq.sortQueues() {
			childInfo := child.GetSchedulingOrder()
			result = append(result, childInfo...)
		}
		return result
	}
}

// getHeadRoom returns the headroom for the queue. This can never be more than the headroom for the parent.
// In case there are no nodes in a newly started cluster and no queues have a limit configured this call
// will return nil.
// NOTE: if a resource quantity is missing and a limit is defined the missing quantity will be seen as no limit.
func (sq *Queue) getHeadRoom() *resources.Resource {
	var parentHeadRoom *resources.Resource
	if sq.parent != nil {
		parentHeadRoom = sq.parent.getHeadRoom()
	}
	return sq.internalHeadRoom(parentHeadRoom)
}

// getMaxHeadRoom returns the maximum headRoom of a queue. The cluster size, which defines the root limit,
// is not relevant for this call. Contrary to the getHeadRoom call. This will return nil unless a limit is set.
// Used during scheduling in an auto-scaling cluster.
// NOTE: if a resource quantity is missing and a limit is defined the missing quantity will be seen as no limit.
func (sq *Queue) getMaxHeadRoom() *resources.Resource {
	var parentHeadRoom *resources.Resource
	if sq.parent != nil {
		parentHeadRoom = sq.parent.getMaxHeadRoom()
	} else {
		return nil
	}
	return sq.internalHeadRoom(parentHeadRoom)
}

// internalHeadRoom does the real headroom calculation.
func (sq *Queue) internalHeadRoom(parentHeadRoom *resources.Resource) *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	headRoom := sq.maxResource

	// if we have no max set headroom is always the same as the parent
	if headRoom == nil {
		return parentHeadRoom
	}

	// calculate what we have left over after removing all allocation
	// ignore unlimited resource types (ie the ones not defined in max)
	headRoom = resources.SubOnlyExisting(headRoom, sq.allocatedResource)

	// check the minimum of the two: parentHeadRoom is nil for root
	if parentHeadRoom == nil {
		return headRoom
	}
	// take the minimum value of *all* resource types defined
	return resources.ComponentWiseMin(headRoom, parentHeadRoom)
}

// GetMaxResource returns the max resource for the queue. The max resource should never be larger than the
// max resource of the parent. The root queue always has its limit set to the total cluster size (dynamic
// based on node registration)
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

func (sq *Queue) CloneMaxResource() *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	return sq.maxResource.Clone()
}

// GetFairMaxResource computes the fair max resources for a given queue.
// Starting with the root, descend down to the target queue allowing children to override Resource values .
// If the root includes an explicit 0 value for a Resource, do not include it in the accumulator and treat it as missing.
// If no children provide a maximum capacity override, the resulting value will be the value found on the Root.
// It is useful for fair-scheduling to allow a ratio to be produced representing the rough utilization % of a given queue.
func (sq *Queue) GetFairMaxResource() *resources.Resource {
	var limit *resources.Resource
	if sq.parent == nil {
		return sq.GetMaxResource().Clone()
	}

	limit = sq.parent.GetFairMaxResource()
	return sq.internalGetFairMaxResource(limit)
}

func (sq *Queue) internalGetFairMaxResource(limit *resources.Resource) *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()

	out := limit.Clone()
	if sq.maxResource.IsEmpty() || out.IsEmpty() {
		return out
	}

	// perform merge. child wins every resources collision
	for k, v := range sq.maxResource.Resources {
		out.Resources[k] = v
	}

	return out
}

// GetMaxQueueSet returns the max resource for the queue. The max resource should never be larger than the
// max resource of the parent. The cluster size, which defines the root limit, is not relevant for this call.
// Contrary to the GetMaxResource call. This will return nil unless a limit is set.
// Used during scheduling in an auto-scaling cluster.
// NOTE: if a resource quantity is missing and a limit is defined the missing quantity will be seen as a limit of 0.
// When defining a limit you therefore should define all resource quantities.
func (sq *Queue) GetMaxQueueSet() *resources.Resource {
	// get the limit for the parent first and check against the queue's own
	if sq.parent == nil {
		return nil
	}
	return sq.internalGetMax(sq.parent.GetMaxQueueSet())
}

// internalGetMax does the real max calculation.
func (sq *Queue) internalGetMax(parentLimit *resources.Resource) *resources.Resource {
	sq.RLock()
	defer sq.RUnlock()
	// no parent queue limit set, not even for root
	if parentLimit == nil {
		return sq.maxResource.Clone()
	}
	// parent limit set, no queue limit return parent
	if sq.maxResource == nil {
		return parentLimit
	}
	// calculate the smallest value for each type
	return resources.ComponentWiseMin(parentLimit, sq.maxResource)
}

// SetMaxResource sets the max resource for the root queue. Called as part of adding or removing a node.
// Should only happen on the root, all other queues get it from the config via properties.
func (sq *Queue) SetMaxResource(max *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()

	if sq.parent != nil {
		log.Log(log.SchedQueue).Warn("Max resources set on a queue that is not the root",
			zap.String("queueName", sq.QueuePath))
		return
	}
	log.Log(log.SchedQueue).Info("updating root queue max resources",
		zap.Stringer("current max", sq.maxResource),
		zap.Stringer("new max", max))

	switch {
	case resources.StrictlyGreaterThanZero(max):
		log.Log(log.SchedQueue).Debug("setting max resources",
			zap.String("queue", sq.QueuePath),
			zap.Stringer("current", sq.maxResource),
			zap.Stringer("new", max))
		if !resources.Equals(sq.maxResource, max) && sq.queueEvents != nil {
			sq.queueEvents.SendMaxResourceChangedEvent(sq.QueuePath, sq.maxResource)
		}
		sq.maxResource = max.Clone()
		sq.updateMaxResourceMetrics()
	case sq.maxResource != nil:
		log.Log(log.SchedQueue).Debug("setting max resources",
			zap.String("queue", sq.QueuePath),
			zap.Stringer("current", sq.maxResource),
			zap.Stringer("new", max))
		if sq.queueEvents != nil {
			sq.queueEvents.SendMaxResourceChangedEvent(sq.QueuePath, sq.maxResource)
		}
		sq.maxResource = nil
		sq.updateMaxResourceMetrics()
	default:
		log.Log(log.SchedQueue).Debug("max resources setting ignored: cannot set zero max resources",
			zap.String("queue", sq.QueuePath))
	}
}

// canRunApp returns if the queue could run a new app for this queue (recursively).
// It takes into account allocatingAcceptedApps
func (sq *Queue) canRunApp(appID string) bool {
	if sq == nil {
		return true
	}
	if sq.parent != nil {
		parentCanRun := sq.parent.canRunApp(appID)
		if !parentCanRun {
			return false
		}
	}
	sq.Lock()
	defer sq.Unlock()
	// if we do not have a max set or this app is already tracked proceed
	if sq.maxRunningApps == 0 || sq.allocatingAcceptedApps[appID] {
		return true
	}
	running := sq.runningApps + uint64(len(sq.allocatingAcceptedApps)+1) //nolint: gosec
	return running <= sq.maxRunningApps
}

// TryAllocate tries to allocate a pending requests. This only gets called if there is a pending request
// on this queue or its children. This is a depth first algorithm: descend into the depth of the queue
// tree first. Child queues are sorted based on the configured queue sortPolicy. Queues without pending
// resources are skipped.
// Applications are sorted based on the application sortPolicy. Applications without pending resources are skipped.
// Lock free call this all locks are taken when needed in called functions
func (sq *Queue) TryAllocate(iterator func() NodeIterator, fullIterator func() NodeIterator, getnode func(string) *Node, allowPreemption bool) *AllocationResult {
	if sq.IsLeafQueue() {
		// get the headroom
		headRoom := sq.getHeadRoom()
		preemptionDelay := sq.GetPreemptionDelay()
		preemptAttemptsRemaining := maxPreemptionsPerQueue

		// process the apps (filters out app without pending requests)
		for _, app := range sq.sortApplications(false) {
			runnableInQueue := sq.canRunApp(app.ApplicationID)
			runnableByUserLimit := ugm.GetUserManager().CanRunApp(sq.QueuePath, app.ApplicationID, app.user)
			app.updateRunnableStatus(runnableInQueue, runnableByUserLimit)
			if app.IsAccepted() && (!runnableInQueue || !runnableByUserLimit) {
				continue
			}
			deadline := app.GetBackoffDeadline()
			if !deadline.IsZero() && time.Now().Before(deadline) {
				continue
			}
			result := app.tryAllocate(headRoom, allowPreemption, preemptionDelay, &preemptAttemptsRemaining, iterator, fullIterator, getnode)
			if result != nil {
				log.Log(log.SchedQueue).Info("allocation found on queue",
					zap.String("queueName", sq.QueuePath),
					zap.String("appID", app.ApplicationID),
					zap.Stringer("resultType", result.ResultType),
					zap.Stringer("allocation", result.Request))
				// if the app is still in Accepted state we're allocating placeholders.
				// we want to count these apps as running
				if app.IsAccepted() {
					sq.setAllocatingAccepted(app.ApplicationID)
				}
				return result
			}
		}

		// Should we trigger preemption to enforce new quota?
		if sq.shouldTriggerPreemption() {
			go func() {
				log.Log(log.SchedQueue).Info("Trigger preemption to enforce new max resources",
					zap.String("queueName", sq.QueuePath),
					zap.String("max resources", sq.maxResource.String()))
				preemptor := NewQuotaChangePreemptor(sq)
				if preemptor.CheckPreconditions() {
					log.Log(log.SchedQueue).Info("Preconditions has passed to trigger preemption to enforce new max resources",
						zap.String("queueName", sq.QueuePath),
						zap.String("max resources", sq.maxResource.String()))
					preemptor.tryPreemption()
				}
			}()
		}
	} else {
		// process the child queues (filters out queues without pending requests)
		for _, child := range sq.sortQueues() {
			result := child.TryAllocate(iterator, fullIterator, getnode, allowPreemption)
			if result != nil {
				return result
			}
		}
	}
	return nil
}

// TryPlaceholderAllocate tries to replace a placeholders with a real allocation.
// This only gets called if there is a pending request on this queue or its children.
// This is a depth first algorithm: descend into the depth of the queue tree first. Child queues are sorted based on
// the configured queue sortPolicy. Queues without pending resources are skipped.
// Applications are sorted based on the application sortPolicy. Applications without pending resources are skipped.
// Lock free call this all locks are taken when needed in called functions
func (sq *Queue) TryPlaceholderAllocate(iterator func() NodeIterator, getnode func(string) *Node) *AllocationResult {
	if sq.IsLeafQueue() {
		// process the apps (filters out app without pending requests)
		for _, app := range sq.sortApplications(true) {
			result := app.tryPlaceholderAllocate(iterator, getnode)
			if result != nil {
				log.Log(log.SchedQueue).Info("allocation found on queue",
					zap.String("queueName", sq.QueuePath),
					zap.String("appID", app.ApplicationID),
					zap.Stringer("resultType", result.ResultType),
					zap.Stringer("allocation", result.Request))
				return result
			}
		}
	} else {
		// process the child queues (filters out queues without pending requests)
		for _, child := range sq.sortQueues() {
			result := child.TryPlaceholderAllocate(iterator, getnode)
			if result != nil {
				return result
			}
		}
	}
	return nil
}

// GetQueueOutstandingRequests builds a slice of pending allocation asks that fits into the queue's headroom.
func (sq *Queue) GetQueueOutstandingRequests(total *[]*Allocation) {
	if sq.IsLeafQueue() {
		headRoom := sq.getMaxHeadRoom()
		// while calculating outstanding requests, we calculate all the requests that can fit into the queue's headroom,
		// all these requests are qualified to trigger the up scaling.
		for _, app := range sq.sortApplications(false) {
			// calculate the users' headroom
			userHeadroom := ugm.GetUserManager().Headroom(app.queuePath, app.ApplicationID, app.user)
			app.getOutstandingRequests(headRoom, userHeadroom, total)
		}
	} else {
		for _, child := range sq.sortQueues() {
			child.GetQueueOutstandingRequests(total)
		}
	}
}

// TryReservedAllocate tries to allocate a reservation.
// This only gets called if there is a pending request on this queue or its children.
// This is a depth first algorithm: descend into the depth of the queue tree first. Child queues are sorted based on
// the configured queue sortPolicy. Queues without pending resources are skipped.
// Applications are currently NOT sorted and are iterated over in a random order.
// Lock free call this all locks are taken when needed in called functions
func (sq *Queue) TryReservedAllocate(iterator func() NodeIterator) *AllocationResult {
	if sq.IsLeafQueue() {
		// skip if it has no reservations
		reservedCopy := sq.GetReservedApps()
		if len(reservedCopy) != 0 {
			// get the headroom
			headRoom := sq.getHeadRoom()
			// process the apps
			for appID, numRes := range reservedCopy {
				if numRes > 1 {
					log.Log(log.SchedQueue).Debug("multiple reservations found for application trying to allocate one",
						zap.String("appID", appID),
						zap.Int("reservations", numRes))
				}
				app := sq.GetApplication(appID)
				if app == nil {
					log.Log(log.SchedQueue).Debug("reservation(s) found but application did not exist in queue",
						zap.String("queueName", sq.QueuePath),
						zap.String("appID", appID))
					return nil
				}
				if app.IsAccepted() && (!sq.canRunApp(appID) || !ugm.GetUserManager().CanRunApp(sq.QueuePath, appID, app.user)) {
					continue
				}
				result := app.tryReservedAllocate(headRoom, iterator)
				if result != nil {
					log.Log(log.SchedQueue).Info("reservation found for allocation found on queue",
						zap.String("queueName", sq.QueuePath),
						zap.String("appID", appID),
						zap.Stringer("resultType", result.ResultType),
						zap.Stringer("allocation", result.Request),
						zap.String("appStatus", app.CurrentState()))
					// if the app is still in Accepted state we're allocating placeholders.
					// we want to count these apps as running
					if app.IsAccepted() {
						sq.setAllocatingAccepted(app.ApplicationID)
					}
					return result
				}
			}
		}
	} else {
		// process the child queues (filters out queues that have no pending requests)
		for _, child := range sq.sortQueues() {
			result := child.TryReservedAllocate(iterator)
			if result != nil {
				return result
			}
		}
	}
	return nil
}

// GetReservedApps returns a shallow copy of the reserved app list
// locked to prevent race conditions from event updates
func (sq *Queue) GetReservedApps() map[string]int {
	sq.RLock()
	defer sq.RUnlock()

	copied := make(map[string]int)
	for appID, numRes := range sq.reservedApps {
		copied[appID] = numRes
	}
	// increase the number of reservations for this app
	return copied
}

// Reserve increments the number of reservations for the application adding it to the map if needed.
// No checks this is only called when a reservation is processed using the app stored in the queue.
func (sq *Queue) Reserve(appID string) {
	sq.Lock()
	defer sq.Unlock()
	// increase the number of reservations for this app
	sq.reservedApps[appID]++
}

// UnReserve decrements the number of reservations for the application removing it to the map if all
// reservations are removed.
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

// getApplication return the Application based on the ID.
func (sq *Queue) GetApplication(appID string) *Application {
	sq.RLock()
	defer sq.RUnlock()
	return sq.applications[appID]
}

// GetQueueByAppID returns the queue that the application with the given appID belongs to
func (sq *Queue) GetQueueByAppID(appID string) *Queue {
	return sq.appQueueMapping.GetQueueByAppId(appID)
}

// getSortType return the queue sort type.
func (sq *Queue) getSortType() policies.SortPolicy {
	sq.RLock()
	defer sq.RUnlock()
	return sq.sortType
}

// SupportTaskGroup returns true if the queue supports task groups.
// FIFO policy is required to support this.
// NOTE: this call does not make sense for a parent queue, and always returns false
func (sq *Queue) SupportTaskGroup() bool {
	sq.RLock()
	defer sq.RUnlock()
	if !sq.isLeaf {
		return false
	}
	return sq.sortType == policies.FifoSortPolicy
}

// updateGuaranteedResourceMetrics updates guaranteed resource metrics.
func (sq *Queue) updateGuaranteedResourceMetrics() {
	queueMetrics := metrics.GetQueueMetrics(sq.QueuePath)
	resourcesToUpdate := map[string]resources.Quantity{}
	if sq.guaranteedResource != nil {
		resourcesToUpdate = sq.guaranteedResource.Resources
	}
	queueMetrics.UpdateQueueResourceMetrics(metrics.QueueGuaranteed, resourcesToUpdate)
}

// updateMaxResourceMetrics updates max resource metrics.
func (sq *Queue) updateMaxResourceMetrics() {
	queueMetrics := metrics.GetQueueMetrics(sq.QueuePath)
	resourcesToUpdate := map[string]resources.Quantity{}
	if sq.maxResource != nil {
		resourcesToUpdate = sq.maxResource.Resources
	}
	queueMetrics.UpdateQueueResourceMetrics(metrics.QueueMax, resourcesToUpdate)
}

// updateAllocatedResourceMetrics updates allocated resource metrics for all queue types.
func (sq *Queue) updateAllocatedResourceMetrics() {
	for k, v := range sq.allocatedResource.Resources {
		metrics.GetQueueMetrics(sq.QueuePath).SetQueueAllocatedResourceMetrics(k, float64(v))
	}
}

// updatePendingResourceMetrics updates pending resource metrics for all queue types.
func (sq *Queue) updatePendingResourceMetrics() {
	for k, v := range sq.pending.Resources {
		metrics.GetQueueMetrics(sq.QueuePath).SetQueuePendingResourceMetrics(k, float64(v))
	}
}

// updatePreemptingResourceMetrics updates preempting resource metrics for all queue types.
func (sq *Queue) updatePreemptingResourceMetrics() {
	for k, v := range sq.preemptingResource.Resources {
		metrics.GetQueueMetrics(sq.QueuePath).SetQueuePreemptingResourceMetrics(k, float64(v))
	}
}

func (sq *Queue) updateMaxRunningAppsMetrics() {
	metrics.GetQueueMetrics(sq.QueuePath).SetQueueMaxRunningAppsMetrics(sq.maxRunningApps)
}

func (sq *Queue) removeMetrics() {
	metrics.RemoveQueueMetrics(sq.QueuePath)
}

func (sq *Queue) String() string {
	sq.RLock()
	defer sq.RUnlock()
	return fmt.Sprintf("{QueuePath: %s, State: %s, StateTime: %x, MaxResource: %s}",
		sq.QueuePath, sq.stateMachine.Current(), sq.stateTime, sq.maxResource)
}

// incRunningApps increments the number of running applications for this queue (recursively).
// Guarded against going over the max set. Combined with the decRunningApps guard against below zero
// this guard should allow self-heal.
func (sq *Queue) incRunningApps(appID string) {
	if sq == nil {
		return
	}
	if sq.parent != nil {
		sq.parent.incRunningApps(appID)
	}
	sq.Lock()
	defer sq.Unlock()
	delete(sq.allocatingAcceptedApps, appID)
	sq.runningApps++
	// sanity check
	if sq.maxRunningApps > 0 && sq.runningApps > sq.maxRunningApps {
		log.Log(log.SchedQueue).Debug("queue running apps went over maximum",
			zap.String("queueName", sq.QueuePath))
		sq.runningApps = sq.maxRunningApps
	}
}

// decRunningApps decrements the number of running applications for this queue (recursively).
// Guarded against going negative. Combined with the incRunningApps guard against below zero
// this guard should allow self-heal.
func (sq *Queue) decRunningApps() {
	if sq == nil {
		return
	}
	if sq.parent != nil {
		sq.parent.decRunningApps()
	}
	sq.Lock()
	defer sq.Unlock()
	// sanity check
	if sq.runningApps > 0 {
		sq.runningApps--
	} else {
		log.Log(log.SchedQueue).Debug("queue running apps went negative",
			zap.String("queueName", sq.QueuePath))
	}
}

// setAllocatingAccepted tracks the application in accepted state that have placeholders allocated.
// These applications are considered "running" inside the queue for max running applications' enforcement.
// For this queue (recursively).
func (sq *Queue) setAllocatingAccepted(appID string) {
	if sq == nil {
		return
	}
	if sq.parent != nil {
		sq.parent.setAllocatingAccepted(appID)
	}
	sq.Lock()
	defer sq.Unlock()
	sq.allocatingAcceptedApps[appID] = true
}

func (sq *Queue) GetPreemptionPolicy() policies.PreemptionPolicy {
	sq.RLock()
	defer sq.RUnlock()
	return sq.preemptionPolicy
}

// FindEligiblePreemptionVictims is used to locate tasks which may be preempted for the given ask.
// queuePath is the fully-qualified path of the queue where ask resides
// ask is the ask we are attempting to preempt for
// return is a map of potential victims keyed by queue path
func (sq *Queue) FindEligiblePreemptionVictims(queuePath string, ask *Allocation) map[string]*QueuePreemptionSnapshot {
	results := make(map[string]*QueuePreemptionSnapshot)
	priorityMap := make(map[string]int64)

	// get the queue which acts as the fence boundary
	fence := sq.findPreemptionFenceRoot(priorityMap, int64(ask.priority))
	if fence == nil {
		return nil
	}

	// now, starting from the fence, we travel downward (going depth-first) to find victims of equal or lower priority
	queuePriority, ok := priorityMap[fence.QueuePath]
	if !ok {
		// this shouldn't happen
		log.Log(log.SchedQueue).Error("BUG: No computed priority found for queue", zap.String("queue", fence.QueuePath))
		return nil
	}

	// create snapshot for ask or preemptor queue
	sq.createPreemptionSnapshot(results, queuePath)
	c := sq
	// set the ask queue for all queues in the ask queue hierarchy
	for c.parent != nil {
		results[c.QueuePath].AskQueue = results[queuePath]
		c = c.parent
	}

	// walk the subtree contained within the preemption fence and collect potential victims organized by nodeID
	fence.findEligiblePreemptionVictims(results, queuePath, ask, priorityMap, queuePriority, false)

	return results
}

// createPreemptionSnapshot is used to create a snapshot of the current queue's resource usage and potential preemption victims
func (sq *Queue) createPreemptionSnapshot(cache map[string]*QueuePreemptionSnapshot, askQueuePath string) *QueuePreemptionSnapshot {
	if sq == nil {
		return nil
	}
	snapshot, ok := cache[sq.QueuePath]
	if ok {
		// already built
		return snapshot
	}

	parentSnapshot := sq.parent.createPreemptionSnapshot(cache, askQueuePath)
	sq.RLock()
	defer sq.RUnlock()
	snapshot = &QueuePreemptionSnapshot{
		Parent:             parentSnapshot,
		QueuePath:          sq.QueuePath,
		Leaf:               sq.isLeaf,
		AllocatedResource:  sq.allocatedResource.Clone(),
		PreemptingResource: sq.preemptingResource.Clone(),
		MaxResource:        sq.maxResource.Clone(),
		GuaranteedResource: sq.guaranteedResource.Clone(),
		PotentialVictims:   make([]*Allocation, 0),
		AskQueue:           cache[askQueuePath],
	}
	cache[sq.QueuePath] = snapshot
	return snapshot
}

func (sq *Queue) findEligiblePreemptionVictims(results map[string]*QueuePreemptionSnapshot, queuePath string, ask *Allocation, priorityMap map[string]int64, askPriority int64, fenced bool) {
	if sq == nil {
		return
	}
	if sq.GetQueuePath() == queuePath {
		return
	}
	if sq.IsLeafQueue() {
		// leaf queue, skip queue if preemption is disabled
		if sq.GetPreemptionPolicy() == policies.DisabledPreemptionPolicy {
			return
		}

		victims := sq.createPreemptionSnapshot(results, queuePath)

		// skip this queue if we are within guaranteed limits
		remaining := results[sq.QueuePath].GetRemainingGuaranteedResource()
		if remaining != nil && resources.StrictlyGreaterThanOrEquals(remaining, resources.Zero) {
			return
		}

		// walk allocations and select those that are equal or lower than current priority
		for _, app := range sq.GetCopyOfApps() {
			for _, alloc := range app.GetAllAllocations() {
				// at least any one of the ask resource type should match with potential victim
				if !ask.GetAllocatedResource().MatchAny(alloc.GetAllocatedResource()) {
					continue
				}

				// skip tasks which require a specific node
				if alloc.GetRequiredNode() != "" {
					continue
				}

				// skip placeholder tasks which are marked released
				if alloc.IsReleased() {
					continue
				}

				// skip allocs which have already been preempted
				if alloc.IsPreempted() {
					continue
				}

				// if we have encountered a fence then all tasks are eligible for preemption
				// otherwise the task is a candidate if its priority is less than or equal to the ask priority
				if fenced || int64(alloc.GetPriority()) <= askPriority {
					victims.PotentialVictims = append(victims.PotentialVictims, alloc)
				}
			}
		}

		// remove from potential victim list if there are no potential victims
		if len(victims.PotentialVictims) == 0 {
			delete(results, sq.QueuePath)
		}
	} else {
		// parent queue, walk child queues and evaluate
		for _, child := range sq.GetCopyOfChildren() {
			childFenced := false
			childPriority, ok := priorityMap[child.QueuePath]
			if !ok {
				// we are evaluating a distinct subtree from the one containing the ask, so check policy to compute relative priority
				policy, offset := child.GetPriorityPolicyAndOffset()
				if policy == policies.FencePriorityPolicy {
					// if the queue offset is greater than the ask priority, then none of the child subtasks may be preempted
					if int64(offset) > askPriority {
						continue
					}

					// all tasks in subtree can be preempted, so mark fenced as true
					childFenced = true
					childPriority = askPriority
				} else {
					// queue is not fenced, evaluate child by subtracting the offset from the ask when traversing downward
					childPriority = askPriority - int64(offset)
				}
			}

			// retrieve candidate tasks from child queue
			child.findEligiblePreemptionVictims(results, queuePath, ask, priorityMap, childPriority, fenced || childFenced)
		}
	}
}

func (sq *Queue) findPreemptionFenceRoot(priorityMap map[string]int64, currentPriority int64) *Queue {
	if sq == nil {
		return nil
	}
	policy, offset := sq.GetPriorityPolicyAndOffset()
	switch policy {
	case policies.FencePriorityPolicy:
		currentPriority = int64(offset)
	default:
		currentPriority += int64(offset)
	}
	priorityMap[sq.QueuePath] = currentPriority

	// Return this queue as fence root if: 1. FencePreemptionPolicy is set 2. root queue 3. allocations in the queue reached maximum resources
	if sq.parent == nil || sq.GetPreemptionPolicy() == policies.FencePreemptionPolicy || resources.Equals(sq.maxResource, sq.allocatedResource) {
		return sq
	}
	return sq.parent.findPreemptionFenceRoot(priorityMap, currentPriority)
}

func (sq *Queue) GetCurrentPriority() int32 {
	sq.RLock()
	defer sq.RUnlock()
	return sq.getCurrentPriority()
}

func (sq *Queue) getCurrentPriority() int32 {
	return priorityValueByPolicy(sq.priorityPolicy, sq.priorityOffset, sq.currentPriority)
}

func (sq *Queue) GetPriorityPolicyAndOffset() (policies.PriorityPolicy, int32) {
	sq.RLock()
	defer sq.RUnlock()
	return sq.priorityPolicy, sq.priorityOffset
}

func priorityValueByPolicy(policy policies.PriorityPolicy, offset int32, priority int32) int32 {
	// special case - if min, just use that
	if priority == configs.MinPriority {
		return priority
	}

	switch policy {
	case policies.FencePriorityPolicy:
		return offset
	default:
		// add offset to priority, checking for overflow/underflow
		result := int64(offset) + int64(priority)
		if result > int64(configs.MaxPriority) {
			return configs.MaxPriority
		}
		if result < int64(configs.MinPriority) {
			return configs.MinPriority
		}
		return int32(result) //nolint: gosec
	}
}

func (sq *Queue) UpdateApplicationPriority(applicationID string, priority int32) {
	if sq == nil || !sq.IsLeafQueue() {
		return
	}
	value := sq.updateApplicationPriorityInternal(applicationID, priority)
	sq.parent.UpdateQueuePriority(sq.Name, value)
}

func (sq *Queue) updateApplicationPriorityInternal(applicationID string, priority int32) int32 {
	sq.Lock()
	defer sq.Unlock()

	if _, ok := sq.applications[applicationID]; !ok {
		log.Log(log.SchedQueue).Debug("Unknown application", zap.String("applicationID", applicationID))
		return sq.currentPriority
	}

	sq.appPriorities[applicationID] = priority
	return sq.recalculatePriority()
}

func (sq *Queue) UpdateQueuePriority(queueName string, priority int32) {
	if sq == nil || sq.IsLeafQueue() {
		return
	}
	value := sq.updateQueuePriorityInternal(queueName, priority)
	sq.parent.UpdateQueuePriority(sq.Name, value)
}

func (sq *Queue) updateQueuePriorityInternal(queueName string, priority int32) int32 {
	sq.Lock()
	defer sq.Unlock()

	if _, ok := sq.children[queueName]; !ok {
		log.Log(log.SchedQueue).Debug("Unknown queue", zap.String("queueName", queueName))
		return sq.currentPriority
	}

	sq.childPriorities[queueName] = priority
	return sq.recalculatePriority()
}

func (sq *Queue) recalculatePriority() int32 {
	var items map[string]int32
	if sq.isLeaf {
		items = sq.appPriorities
	} else {
		items = sq.childPriorities
	}

	curr := configs.MinPriority
	for _, v := range items {
		curr = max(v, curr)
	}
	sq.currentPriority = curr
	return priorityValueByPolicy(sq.priorityPolicy, sq.priorityOffset, curr)
}

func (sq *Queue) MarkQuotaChangePreemptionRunning(run bool) {
	sq.Lock()
	defer sq.Unlock()
	sq.isQuotaChangePreemptionRunning = run
}

func (sq *Queue) IsQuotaChangePreemptionRunning() bool {
	sq.RLock()
	defer sq.RUnlock()
	return sq.isQuotaChangePreemptionRunning
}

func (sq *Queue) GetMaxAppUnschedAskBackoff() uint64 {
	sq.RLock()
	defer sq.RUnlock()
	return sq.unschedAskBackoff
}

func (sq *Queue) GetBackoffDelay() time.Duration {
	sq.RLock()
	defer sq.RUnlock()
	return sq.askBackoffDelay
}
