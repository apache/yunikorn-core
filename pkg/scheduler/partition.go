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
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/placement"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type PartitionContext struct {
	RmID string // the RM the partition belongs to
	Name string // name of the partition

	// Private fields need protection
	root                   *objects.Queue                  // start of the queue hierarchy
	applications           map[string]*objects.Application // applications assigned to this partition
	completedApplications  map[string]*objects.Application // completed applications from this partition
	rejectedApplications   map[string]*objects.Application // rejected applications from this partition
	nodes                  objects.NodeCollection          // nodes assigned to this partition
	placementManager       *placement.AppPlacementManager  // placement manager for this partition
	partitionManager       *partitionManager               // manager for this partition
	stateMachine           *fsm.FSM                        // the state of the partition for scheduling
	stateTime              time.Time                       // last time the state was updated (needed for cleanup)
	userGroupCache         *security.UserGroupCache        // user cache per partition
	totalPartitionResource *resources.Resource             // Total node resources
	allocations            int                             // Number of allocations on the partition
	reservations           int                             // number of reservations
	placeholderAllocations int                             // number of placeholder allocations
	preemptionEnabled      bool                            // whether preemption is enabled or not

	// The partition write lock must not be held while manipulating an application.
	// Scheduling is running continuously as a lock free background task. Scheduling an application
	// acquires a write lock of the application object. While holding the write lock a list of nodes is
	// requested from the partition. This requires a read lock on the partition.
	// If the partition write lock is held while manipulating an application a dead lock could occur.
	// Since application objects handle their own locks there is no requirement to hold the partition lock
	// while manipulating the application.
	// Similarly adding, updating or removing a node or a queue should only hold the partition write lock
	// while manipulating the partition information not while manipulating the underlying objects.
	locking.RWMutex
}

func newPartitionContext(conf configs.PartitionConfig, rmID string, cc *ClusterContext) (*PartitionContext, error) {
	if conf.Name == "" || rmID == "" {
		log.Log(log.SchedPartition).Info("partition cannot be created",
			zap.String("partition name", conf.Name),
			zap.String("rmID", rmID),
			zap.Any("cluster context", cc))
		return nil, fmt.Errorf("partition cannot be created without name or RM, one is not set")
	}
	pc := &PartitionContext{
		Name:                  conf.Name,
		RmID:                  rmID,
		stateMachine:          objects.NewObjectState(),
		stateTime:             time.Now(),
		applications:          make(map[string]*objects.Application),
		completedApplications: make(map[string]*objects.Application),
		nodes:                 objects.NewNodeCollection(conf.Name),
	}
	pc.partitionManager = newPartitionManager(pc, cc)
	if err := pc.initialPartitionFromConfig(conf); err != nil {
		return nil, err
	}

	return pc, nil
}

// Initialise the partition
func (pc *PartitionContext) initialPartitionFromConfig(conf configs.PartitionConfig) error {
	if len(conf.Queues) == 0 || conf.Queues[0].Name != configs.RootQueue {
		return fmt.Errorf("partition cannot be created without root queue")
	}

	// Setup the queue structure: root first it should be the only queue at this level
	// Add the rest of the queue structure recursively
	queueConf := conf.Queues[0]
	var err error
	if pc.root, err = objects.NewConfiguredQueue(queueConf, nil); err != nil {
		return err
	}
	// recursively add the queues to the root
	if err = pc.addQueue(queueConf.Queues, pc.root); err != nil {
		return err
	}
	log.Log(log.SchedPartition).Info("root queue added",
		zap.String("partitionName", pc.Name),
		zap.String("rmID", pc.RmID))

	// We need to pass in the locked version of the GetQueue function.
	// Placing an application will not have a lock on the partition context.
	pc.placementManager = placement.NewPlacementManager(conf.PlacementRules, pc.GetQueue)
	// get the user group cache for the partition
	pc.userGroupCache = security.GetUserGroupCache("")
	pc.updateNodeSortingPolicy(conf)
	pc.updatePreemption(conf)

	// update limit settings: start at the root
	return ugm.GetUserManager().UpdateConfig(queueConf, conf.Queues[0].Name)
}

// NOTE: this is a lock free call. It should only be called holding the PartitionContext lock.
func (pc *PartitionContext) updateNodeSortingPolicy(conf configs.PartitionConfig) {
	var configuredPolicy policies.SortingPolicy
	configuredPolicy, err := policies.SortingPolicyFromString(conf.NodeSortPolicy.Type)
	if err != nil {
		log.Log(log.SchedPartition).Debug("NodeSorting policy incorrectly set or unknown",
			zap.Error(err))
		log.Log(log.SchedPartition).Info(fmt.Sprintf("NodeSorting policy not set using '%s' as default", configuredPolicy))
	} else {
		log.Log(log.SchedPartition).Info("NodeSorting policy set from config",
			zap.Stringer("policyName", configuredPolicy))
	}
	pc.nodes.SetNodeSortingPolicy(objects.NewNodeSortingPolicy(conf.NodeSortPolicy.Type, conf.NodeSortPolicy.ResourceWeights))
}

// NOTE: this is a lock free call. It should only be called holding the PartitionContext lock.
func (pc *PartitionContext) updatePreemption(conf configs.PartitionConfig) {
	pc.preemptionEnabled = conf.Preemption.Enabled == nil || *conf.Preemption.Enabled
}

func (pc *PartitionContext) updatePartitionDetails(conf configs.PartitionConfig) error {
	// the following piece of code (before pc.Lock()) must be performed without locking
	// to avoid lock order differences between PartitionContext and AppPlacementManager
	if len(conf.Queues) == 0 || conf.Queues[0].Name != configs.RootQueue {
		return fmt.Errorf("partition cannot be created without root queue")
	}
	log.Log(log.SchedPartition).Info("Updating placement manager rules on config reload")
	err := pc.getPlacementManager().UpdateRules(conf.PlacementRules)
	if err != nil {
		log.Log(log.SchedPartition).Info("New placement rules not activated, config reload failed", zap.Error(err))
		return err
	}
	pc.updateNodeSortingPolicy(conf)

	pc.Lock()
	defer pc.Unlock()
	pc.updatePreemption(conf)
	// start at the root: there is only one queue
	queueConf := conf.Queues[0]
	root := pc.root
	// update the root queue
	if err := root.ApplyConf(queueConf); err != nil {
		return err
	}
	root.UpdateQueueProperties()
	// update the rest of the queues recursively
	if err := pc.updateQueues(queueConf.Queues, root); err != nil {
		return err
	}
	// update limit settings: start at the root
	return ugm.GetUserManager().UpdateConfig(queueConf, conf.Queues[0].Name)
}

// Process the config structure and create a queue info tree for this partition
func (pc *PartitionContext) addQueue(conf []configs.QueueConfig, parent *objects.Queue) error {
	// create the queue at this level
	for _, queueConf := range conf {
		thisQueue, err := objects.NewConfiguredQueue(queueConf, parent)
		if err != nil {
			return err
		}
		// recursive create the queues below
		if len(queueConf.Queues) > 0 {
			err = pc.addQueue(queueConf.Queues, thisQueue)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Update the passed in queues and then do this recursively for the children
//
// NOTE: this is a lock free call. It should only be called holding the PartitionContext lock.
func (pc *PartitionContext) updateQueues(config []configs.QueueConfig, parent *objects.Queue) error {
	// get the name of the passed in queue
	parentPath := parent.QueuePath + configs.DOT
	// keep track of which children we have updated
	visited := map[string]bool{}
	// walk over the queues recursively
	for _, queueConfig := range config {
		pathName := parentPath + queueConfig.Name
		queue := pc.getQueueInternal(pathName)
		var err error
		if queue == nil {
			queue, err = objects.NewConfiguredQueue(queueConfig, parent)
		} else {
			err = queue.ApplyConf(queueConfig)
		}
		if err != nil {
			return err
		}
		// special call to convert to a real policy from the property
		queue.UpdateQueueProperties()
		if err = pc.updateQueues(queueConfig.Queues, queue); err != nil {
			return err
		}
		visited[queue.Name] = true
	}
	// remove all children that were not visited
	for childName, childQueue := range parent.GetCopyOfChildren() {
		if !visited[childName] {
			childQueue.MarkQueueForRemoval()
		}
	}
	return nil
}

// Mark the partition  for removal from the system.
// This can be executed multiple times and is only effective the first time.
// The current cleanup sequence is "immediate". This is implemented to allow a graceful cleanup.
func (pc *PartitionContext) markPartitionForRemoval() {
	if err := pc.handlePartitionEvent(objects.Remove); err != nil {
		log.Log(log.SchedPartition).Error("failed to mark partition for deletion",
			zap.String("partitionName", pc.Name),
			zap.Error(err))
	}
}

// Get the state of the partition.
// No new nodes and applications will be accepted if stopped or being removed.
func (pc *PartitionContext) isDraining() bool {
	return pc.stateMachine.Current() == objects.Draining.String()
}

func (pc *PartitionContext) isStopped() bool {
	return pc.stateMachine.Current() == objects.Stopped.String()
}

// Handle the state event for the partition.
// The state machine handles the locking.
func (pc *PartitionContext) handlePartitionEvent(event objects.ObjectEvent) error {
	err := pc.stateMachine.Event(context.Background(), event.String(), pc.Name)
	if err == nil {
		pc.stateTime = time.Now()
		return nil
	}
	// handle the same state transition not nil error (limit of fsm).
	if err.Error() == "no transition" {
		return nil
	}
	return err
}

// Get the placement manager. The manager could change when we process the configuration changes
// we thus need to lock.
func (pc *PartitionContext) getPlacementManager() *placement.AppPlacementManager {
	pc.RLock()
	defer pc.RUnlock()
	return pc.placementManager
}

// AddApplication adds a new application to the partition.
// Runs the placement rules for the queue resolution. Creates a new dynamic queue if the queue does not yet
// exists.
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) AddApplication(app *objects.Application) error {
	if pc.isDraining() || pc.isStopped() {
		return fmt.Errorf("partition %s is stopped cannot add a new application %s", pc.Name, app.ApplicationID)
	}

	// Check if the app exists
	appID := app.ApplicationID
	if pc.getApplication(appID) != nil {
		return fmt.Errorf("adding application %s to partition %s, but application already existed", appID, pc.Name)
	}

	// Resolve the queue for this app using the placement rules
	// We either have an error or a queue name is set on the application.
	err := pc.getPlacementManager().PlaceApplication(app)
	if err != nil {
		return fmt.Errorf("failed to place application %s: %v", appID, err)
	}
	queueName := app.GetQueuePath()

	// lock the partition and make the last change: we need to do this before creating the queues.
	// queue cleanup might otherwise remove the queue again before we can add the application
	pc.Lock()
	defer pc.Unlock()
	// we have a queue name either from placement or direct, get the queue
	queue := pc.getQueueInternal(queueName)

	// create the queue if necessary
	isRecoveryQueue := common.IsRecoveryQueue(queueName)
	if queue == nil {
		if isRecoveryQueue {
			queue, err = pc.createRecoveryQueue()
			if err != nil {
				return errors.Join(fmt.Errorf("failed to create recovery queue %s for application %s", common.RecoveryQueueFull, appID), err)
			}
		} else {
			queue, err = pc.createQueue(queueName, app.GetUser())
			if err != nil {
				return errors.Join(fmt.Errorf("failed to create rule based queue %s for application %s", queueName, appID), err)
			}
		}
	}

	// check the queue: is a leaf queue
	if !queue.IsLeafQueue() {
		return fmt.Errorf("failed to find queue %s for application %s", queueName, appID)
	}

	guaranteedRes := app.GetGuaranteedResource()
	maxRes := app.GetMaxResource()
	if !isRecoveryQueue && (guaranteedRes != nil || maxRes != nil) {
		// set resources based on tags, but only if the queue is dynamic (unmanaged)
		if queue.IsManaged() {
			log.Log(log.SchedQueue).Warn("Trying to set resources on a queue that is not an unmanaged leaf",
				zap.String("queueName", queue.QueuePath))
		} else {
			queue.SetResources(guaranteedRes, maxRes)
		}
	}

	// check only for gang request
	// - make sure the taskgroup request fits in the maximum set for the queue hierarchy
	// - task groups should only be used in FIFO queues
	// if the check fails remove the app from the queue again
	if placeHolder := app.GetPlaceholderAsk(); !resources.IsZero(placeHolder) {
		// check the queue sorting
		if !queue.SupportTaskGroup() {
			return fmt.Errorf("queue %s cannot run application %s with task group request: unsupported sort type", queueName, appID)
		}
		if maxQueue := queue.GetMaxQueueSet(); maxQueue != nil {
			if !maxQueue.FitInMaxUndef(placeHolder) {
				return fmt.Errorf("queue %s cannot fit application %s: task group request %s larger than max queue allocation %s", queueName, appID, placeHolder.String(), maxQueue.String())
			}
		}
	}
	// all is OK update the app and add it to the partition
	app.SetQueue(queue)
	app.SetTerminatedCallback(pc.moveTerminatedApp)
	queue.AddApplication(app)
	pc.applications[appID] = app

	return nil
}

// Remove the application from the partition.
// This does not fail and handles missing app/queue/node/allocations internally
func (pc *PartitionContext) removeApplication(appID string) []*objects.Allocation {
	// update the partition details, must be locked but all other updates should not hold partition lock
	app := pc.removeAppInternal(appID)
	if app == nil {
		return nil
	}
	// Remove all asks and thus all reservations and pending resources (queue included)
	_ = app.RemoveAllocationAsk("")
	// Remove app from queue
	if queue := app.GetQueue(); queue != nil {
		queue.RemoveApplication(app)
	}
	// Remove all allocations
	allocations := app.RemoveAllAllocations()
	// Remove all allocations from node(s) (queues have been updated already)
	if len(allocations) != 0 {
		// track the number of allocations
		pc.updateAllocationCount(-len(allocations))
		for _, alloc := range allocations {
			currentAllocationKey := alloc.GetAllocationKey()
			node := pc.GetNode(alloc.GetNodeID())
			if node == nil {
				log.Log(log.SchedPartition).Warn("unknown node: not found in active node list",
					zap.String("appID", appID),
					zap.String("nodeID", alloc.GetNodeID()))
				continue
			}
			if nodeAlloc := node.RemoveAllocation(currentAllocationKey); nodeAlloc == nil {
				log.Log(log.SchedPartition).Warn("unknown allocation: not found on the node",
					zap.String("appID", appID),
					zap.String("allocationKey", currentAllocationKey),
					zap.String("nodeID", alloc.GetNodeID()))
			}
		}
	}
	return allocations
}

// Locked updates of the partition tracking info
func (pc *PartitionContext) removeAppInternal(appID string) *objects.Application {
	pc.Lock()
	defer pc.Unlock()

	// Remove from applications map
	app := pc.applications[appID]
	if app == nil {
		return nil
	}
	// remove from partition then cleanup underlying objects
	delete(pc.applications, appID)
	return app
}

func (pc *PartitionContext) GetApplication(appID string) *objects.Application {
	return pc.getApplication(appID)
}

func (pc *PartitionContext) getApplication(appID string) *objects.Application {
	pc.RLock()
	defer pc.RUnlock()

	return pc.applications[appID]
}

func (pc *PartitionContext) getRejectedApplication(appID string) *objects.Application {
	pc.RLock()
	defer pc.RUnlock()

	return pc.rejectedApplications[appID]
}

// GetQueue returns queue from the structure based on the fully qualified name.
// Wrapper around the unlocked version getQueueInternal()
// Visible by tests
func (pc *PartitionContext) GetQueue(name string) *objects.Queue {
	pc.RLock()
	defer pc.RUnlock()
	return pc.getQueueInternal(name)
}

// Get the queue from the structure based on the fully qualified name.
// The name is not syntax checked and must be valid.
// Returns nil if the queue is not found otherwise the queue object.
//
// NOTE: this is a lock free call. It should only be called holding the PartitionContext lock.
func (pc *PartitionContext) getQueueInternal(name string) *objects.Queue {
	// start at the root
	queue := pc.root
	part := strings.Split(strings.ToLower(name), configs.DOT)
	// no input
	if len(part) == 0 || part[0] != configs.RootQueue {
		return nil
	}
	// walk over the parts going down towards the requested queue
	for i := 1; i < len(part); i++ {
		// if child not found break out and return
		if queue = queue.GetChildQueue(part[i]); queue == nil {
			break
		}
	}
	return queue
}

// GetPartitionQueues builds the queue info for the whole queue structure to pass to the webservice
func (pc *PartitionContext) GetPartitionQueues() dao.PartitionQueueDAOInfo {
	partitionQueueDAOInfo := pc.root.GetPartitionQueueDAOInfo(true)
	partitionQueueDAOInfo.Partition = common.GetPartitionNameWithoutClusterID(pc.Name)
	return partitionQueueDAOInfo
}

// GetPlacementRules returns the current active rule set as dao to expose to the webservice
func (pc *PartitionContext) GetPlacementRules() []*dao.RuleDAO {
	return pc.getPlacementManager().GetRulesDAO()
}

// createRecoveryQueue creates the recovery queue to add to the hierarchy
func (pc *PartitionContext) createRecoveryQueue() (*objects.Queue, error) {
	return objects.NewRecoveryQueue(pc.root)
}

// Create a queue with full hierarchy. This is called when a new queue is created from a placement rule.
// The final leaf queue does not exist otherwise we would not get here.
// This means that at least 1 queue (a leaf queue) will be created
func (pc *PartitionContext) createQueue(name string, user security.UserGroup) (*objects.Queue, error) {
	// find the queue furthest down the hierarchy that exists
	var toCreate []string
	if !strings.HasPrefix(name, configs.RootQueue) || !strings.Contains(name, configs.DOT) {
		return nil, fmt.Errorf("illegal queue name passed in: %s", name)
	}
	current := name
	queue := pc.getQueueInternal(current)
	log.Log(log.SchedPartition).Debug("Checking queue creation")
	for queue == nil {
		toCreate = append(toCreate, current[strings.LastIndex(current, configs.DOT)+1:])
		current = current[0:strings.LastIndex(current, configs.DOT)]
		queue = pc.getQueueInternal(current)
	}
	// Check the ACL before we really create
	// The existing parent queue is the lowest we need to look at
	if !queue.CheckSubmitAccess(user) {
		return nil, fmt.Errorf("submit access to queue %s denied during create of: %s", current, name)
	}
	if queue.IsLeafQueue() {
		return nil, fmt.Errorf("creation of queue %s failed parent is already a leaf: %s", name, current)
	}
	log.Log(log.SchedPartition).Debug("Creating queue(s)",
		zap.String("parent", current),
		zap.String("fullPath", name))
	for i := len(toCreate) - 1; i >= 0; i-- {
		// everything is checked and there should be no errors
		var err error
		queue, err = objects.NewDynamicQueue(toCreate[i], i == 0, queue)
		if err != nil {
			log.Log(log.SchedPartition).Warn("Queue auto create failed unexpected",
				zap.String("queueName", toCreate[i]),
				zap.Error(err))
			return nil, err
		}
	}
	return queue, nil
}

// Get a node from the partition by nodeID.
func (pc *PartitionContext) GetNode(nodeID string) *objects.Node {
	return pc.nodes.GetNode(nodeID)
}

// Add the node to the partition and process the allocations that are reported by the node.
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) AddNode(node *objects.Node, existingAllocations []*objects.Allocation) error {
	if node == nil {
		return fmt.Errorf("cannot add 'nil' node to partition %s", pc.Name)
	}
	log.Log(log.SchedPartition).Info("adding node to partition",
		zap.String("partition", pc.Name),
		zap.String("nodeID", node.NodeID))
	if pc.isDraining() || pc.isStopped() {
		return fmt.Errorf("partition %s is stopped cannot add a new node %s", pc.Name, node.NodeID)
	}
	if err := pc.addNodeToList(node); err != nil {
		return err
	}

	// Add allocations that exist on the node when added
	if len(existingAllocations) > 0 {
		for current, alloc := range existingAllocations {
			if err := pc.addAllocation(alloc); err != nil {
				// not expecting any inflight replacements on node recovery
				released, _ := pc.removeNode(node.NodeID)
				log.Log(log.SchedPartition).Info("Failed to add existing allocations, changes reversed",
					zap.String("nodeID", node.NodeID),
					zap.Int("existingAllocations", len(existingAllocations)),
					zap.Int("releasedAllocations", len(released)),
					zap.Int("processingAlloc", current),
					zap.Stringer("allocation", alloc),
					zap.Error(err))
				// update failed metric, active metrics are tracked in add/remove from list
				metrics.GetSchedulerMetrics().IncFailedNodes()
				return err
			}
		}
	}
	return nil
}

// Update the partition resources based on the change of the node information
func (pc *PartitionContext) updatePartitionResource(delta *resources.Resource) {
	pc.Lock()
	defer pc.Unlock()
	if delta != nil {
		if pc.totalPartitionResource == nil {
			pc.totalPartitionResource = delta.Clone()
		} else {
			pc.totalPartitionResource.AddTo(delta)
		}
		pc.root.SetMaxResource(pc.totalPartitionResource)
	}
}

// Update the partition details when adding a node.
func (pc *PartitionContext) addNodeToList(node *objects.Node) error {
	// we don't grab a lock here because we only update pc.nodes which is internally protected
	if err := pc.nodes.AddNode(node); err != nil {
		return fmt.Errorf("failed to add node %s to partition %s, error: %v", node.NodeID, pc.Name, err)
	}

	pc.addNodeResources(node)
	return nil
}

// Update metrics & resource tracking information.
// This locks the partition. The partition may not be locked when we process the allocation
// additions to the node as that takes further app, queue or node locks.
func (pc *PartitionContext) addNodeResources(node *objects.Node) {
	pc.Lock()
	defer pc.Unlock()
	metrics.GetSchedulerMetrics().IncActiveNodes()
	// update/set the resources available in the cluster
	if pc.totalPartitionResource == nil {
		pc.totalPartitionResource = node.GetCapacity().Clone()
	} else {
		pc.totalPartitionResource.AddTo(node.GetCapacity())
	}
	pc.root.SetMaxResource(pc.totalPartitionResource)
	log.Log(log.SchedPartition).Info("Updated available resources from added node",
		zap.String("partitionName", pc.Name),
		zap.String("nodeID", node.NodeID),
		zap.Stringer("partitionResource", pc.totalPartitionResource))
}

// removeNodeFromList removes the node from the list of partition nodes.
func (pc *PartitionContext) removeNodeFromList(nodeID string) *objects.Node {
	node := pc.nodes.RemoveNode(nodeID)
	if node == nil {
		log.Log(log.SchedPartition).Debug("node was not found, node already removed",
			zap.String("nodeID", nodeID),
			zap.String("partitionName", pc.Name))
		return nil
	}

	// Remove node from list of tracked nodes
	metrics.GetSchedulerMetrics().DecActiveNodes()
	log.Log(log.SchedPartition).Info("Removed node from available partition nodes",
		zap.String("partitionName", pc.Name),
		zap.String("nodeID", node.NodeID))
	return node
}

// removeNodeResources updates the partition and root queue resources as part of the node removal process.
// This locks the partition.
func (pc *PartitionContext) removeNodeResources(node *objects.Node) {
	pc.Lock()
	defer pc.Unlock()
	// cleanup the available resources, partition resources cannot be nil at this point
	pc.totalPartitionResource.SubFrom(node.GetCapacity())
	pc.root.SetMaxResource(pc.totalPartitionResource)
	log.Log(log.SchedPartition).Info("Updated available resources from removed node",
		zap.String("partitionName", pc.Name),
		zap.String("nodeID", node.NodeID),
		zap.Stringer("partitionResource", pc.totalPartitionResource))
}

// removeNode removes a node from the partition. It returns all released and confirmed allocations.
// The released allocations are all linked to the current node.
// The confirmed allocations are real allocations that are linked to placeholders on the current node and are linked to
// other nodes.
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) removeNode(nodeID string) ([]*objects.Allocation, []*objects.Allocation) {
	log.Log(log.SchedPartition).Info("Removing node from partition",
		zap.String("partition", pc.Name),
		zap.String("nodeID", nodeID))

	// remove the node: it will no longer be seen by the scheduling cycle
	node := pc.removeNodeFromList(nodeID)
	if node == nil {
		return nil, nil
	}

	// unreserve all the apps that were reserved on the node.
	// The node is not reachable anymore unless you have the pointer.
	for _, r := range node.GetReservations() {
		_, app, ask := r.GetObjects()
		pc.unReserve(app, node, ask)
	}
	// cleanup the allocations linked to the node. do this before changing the root queue max: otherwise if
	// scheduling and removal of a node race on a full cluster we could cause all headroom to disappear for
	// the time the allocations are not removed.
	released, confirmed := pc.removeNodeAllocations(node)

	// update the resource linked to this node, all allocations are removed, queue usage should have decreased
	pc.removeNodeResources(node)
	return released, confirmed
}

// removeNodeAllocations removes all allocations that are assigned to a node as part of the node removal. This is not part
// of the node object as updating the applications and queues is the only goal. Applications and queues are not accessible
// from the node. The removed and confirmed allocations are returned.
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) removeNodeAllocations(node *objects.Node) ([]*objects.Allocation, []*objects.Allocation) {
	released := make([]*objects.Allocation, 0)
	confirmed := make([]*objects.Allocation, 0)
	// walk over all allocations still registered for this node
	for _, alloc := range node.GetAllAllocations() {
		allocationKey := alloc.GetAllocationKey()
		// since we are not locking the node and or application we could have had an update while processing
		// note that we do not return the allocation if the app or allocation is not found and assume that it
		// was already removed
		app := pc.getApplication(alloc.GetApplicationID())
		if app == nil {
			log.Log(log.SchedPartition).Info("app is not found, skipping while removing the node",
				zap.String("appID", alloc.GetApplicationID()),
				zap.String("nodeID", node.NodeID))
			continue
		}
		// Processing a removal while in the Completing state could race with the state change.
		// Retrieve the queue early before a possible race.
		queue := app.GetQueue()
		// check for an inflight replacement.
		if alloc.HasRelease() {
			release := alloc.GetRelease()
			// allocation to update the ask on: this needs to happen on the real alloc never the placeholder
			askAlloc := alloc
			// placeholder gets handled differently from normal
			if alloc.IsPlaceholder() {
				// Check if the real allocation is made on the same node if not we should trigger a confirmation of
				// the replacement. Trigger the replacement only if it is NOT on the same node.
				// If it is on the same node we just keep going as the real allocation will be unlinked as a result of
				// the removal of this placeholder. The ask update will trigger rescheduling later for the real alloc.
				if alloc.GetNodeID() != release.GetNodeID() {
					// ignore the return as that is the same as alloc, the alloc is gone after this call
					_ = app.ReplaceAllocation(allocationKey)
					// we need to check the resources equality
					delta := resources.Sub(release.GetAllocatedResource(), alloc.GetAllocatedResource())
					// Any negative value in the delta means that at least one of the requested resource in the
					// placeholder is larger than the real allocation. The nodes are correct the queue needs adjusting.
					// The reverse case is handled during allocation.
					if delta.HasNegativeValue() {
						// this looks incorrect but the delta is negative and the result will be a real decrease
						err := queue.IncAllocatedResource(delta, false)
						// this should not happen as we really decrease the value
						if err != nil {
							log.Log(log.SchedPartition).Warn("unexpected failure during queue update: replacing placeholder",
								zap.String("appID", alloc.GetApplicationID()),
								zap.String("placeholderKey", alloc.GetAllocationKey()),
								zap.String("allocationKey", release.GetAllocationKey()),
								zap.Error(err))
						}
						log.Log(log.SchedPartition).Warn("replacing placeholder: placeholder is larger than real allocation",
							zap.String("allocationKey", release.GetAllocationKey()),
							zap.Stringer("requested resource", release.GetAllocatedResource()),
							zap.String("placeholderKey", alloc.GetAllocationKey()),
							zap.Stringer("placeholder resource", alloc.GetAllocatedResource()))
					}
					// track what we confirm on the other node to confirm it in the shim and get is bound
					confirmed = append(confirmed, release)
					// the allocation is removed so add it to the list that we return
					released = append(released, alloc)
					log.Log(log.SchedPartition).Info("allocation removed from node and replacement confirmed",
						zap.String("nodeID", node.NodeID),
						zap.String("allocationKey", allocationKey),
						zap.String("replacement nodeID", release.GetNodeID()),
						zap.String("replacement allocationKey", release.GetAllocationKey()))
					continue
				}
				askAlloc = release
			}
			// unlink the placeholder and allocation
			release.ClearRelease()
			alloc.ClearRelease()
			// mark ask as unallocated to get it re-scheduled
			_, err := app.DeallocateAsk(askAlloc.GetAllocationKey())
			if err == nil {
				log.Log(log.SchedPartition).Info("inflight placeholder replacement reversed due to node removal",
					zap.String("appID", askAlloc.GetApplicationID()),
					zap.String("allocationKey", askAlloc.GetAllocationKey()),
					zap.String("nodeID", node.NodeID),
					zap.String("replacement allocationKey", askAlloc.GetAllocationKey()))
			} else {
				log.Log(log.SchedPartition).Error("node removal: repeat update failure for inflight replacement",
					zap.String("appID", askAlloc.GetApplicationID()),
					zap.String("allocationKey", askAlloc.GetAllocationKey()),
					zap.String("nodeID", node.NodeID),
					zap.Error(err))
			}
		}
		// check allocations on the app
		if app.RemoveAllocation(allocationKey, si.TerminationType_UNKNOWN_TERMINATION_TYPE) == nil {
			log.Log(log.SchedPartition).Info("allocation is not found, skipping while removing the node",
				zap.String("allocationKey", allocationKey),
				zap.String("appID", app.ApplicationID),
				zap.String("nodeID", node.NodeID))
			continue
		}
		if err := queue.DecAllocatedResource(alloc.GetAllocatedResource()); err != nil {
			log.Log(log.SchedPartition).Warn("failed to release resources from queue",
				zap.String("appID", alloc.GetApplicationID()),
				zap.Error(err))
		}
		// remove preempted resources
		if alloc.IsPreempted() {
			queue.DecPreemptingResource(alloc.GetAllocatedResource())
		}
		if alloc.IsPlaceholder() {
			pc.decPhAllocationCount(1)
		}

		// the allocation is removed so add it to the list that we return
		released = append(released, alloc)
		metrics.GetQueueMetrics(queue.GetQueuePath()).IncReleasedContainer()
		log.Log(log.SchedPartition).Info("node removal: allocation removed",
			zap.String("nodeID", node.NodeID),
			zap.String("queueName", queue.GetQueuePath()),
			zap.String("appID", app.ApplicationID),
			zap.Stringer("allocation", alloc))
	}
	// track the number of allocations: decrement the released allocation AND increment with the confirmed
	pc.updateAllocationCount(len(confirmed) - len(released))
	return released, confirmed
}

func (pc *PartitionContext) calculateOutstandingRequests() []*objects.Allocation {
	if !resources.StrictlyGreaterThanZero(pc.root.GetPendingResource()) {
		return nil
	}
	outstanding := make([]*objects.Allocation, 0)
	pc.root.GetQueueOutstandingRequests(&outstanding)
	return outstanding
}

// Try regular allocation for the partition
// Lock free call this all locks are taken when needed in called functions
func (pc *PartitionContext) tryAllocate() *objects.AllocationResult {
	if !resources.StrictlyGreaterThanZero(pc.root.GetPendingResource()) {
		// nothing to do just return
		return nil
	}
	// try allocating from the root down
	result := pc.root.TryAllocate(pc.GetNodeIterator, pc.GetFullNodeIterator, pc.GetNode, pc.isPreemptionEnabled())
	if result != nil {
		return pc.allocate(result)
	}
	return nil
}

// Try process reservations for the partition
// Lock free call this all locks are taken when needed in called functions
func (pc *PartitionContext) tryReservedAllocate() *objects.AllocationResult {
	if pc.getReservationCount() == 0 {
		return nil
	}
	if !resources.StrictlyGreaterThanZero(pc.root.GetPendingResource()) {
		// nothing to do just return
		return nil
	}
	// try allocating from the root down
	result := pc.root.TryReservedAllocate(pc.GetNodeIterator)
	if result != nil {
		return pc.allocate(result)
	}
	return nil
}

// Try process placeholder for the partition
// Lock free call this all locks are taken when needed in called functions
func (pc *PartitionContext) tryPlaceholderAllocate() *objects.AllocationResult {
	if pc.getPhAllocationCount() == 0 {
		return nil
	}
	if !resources.StrictlyGreaterThanZero(pc.root.GetPendingResource()) {
		// nothing to do just return
		return nil
	}
	// try allocating from the root down
	result := pc.root.TryPlaceholderAllocate(pc.GetNodeIterator, pc.GetNode)
	if result != nil {
		log.Log(log.SchedPartition).Info("scheduler replace placeholder processed",
			zap.String("appID", result.Request.GetApplicationID()),
			zap.String("allocationKey", result.Request.GetAllocationKey()),
			zap.String("placeholder released allocationKey", result.Request.GetRelease().GetAllocationKey()))
		// pass the release back to the RM via the cluster context
		return result
	}
	return nil
}

// Process the allocation and make the left over changes in the partition.
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) allocate(result *objects.AllocationResult) *objects.AllocationResult {
	// find the app make sure it still exists
	appID := result.Request.GetApplicationID()
	app := pc.getApplication(appID)
	if app == nil {
		log.Log(log.SchedPartition).Info("Application was removed while allocating",
			zap.String("appID", appID))
		return nil
	}
	// find the node make sure it still exists
	// if the node was passed in use that ID instead of the one from the allocation
	// the node ID is set when a reservation is allocated on a non-reserved node
	var nodeID string
	alloc := result.Request
	if result.ReservedNodeID == "" {
		nodeID = result.NodeID
	} else {
		nodeID = result.ReservedNodeID
		log.Log(log.SchedPartition).Debug("Reservation allocated on different node",
			zap.String("current node", result.NodeID),
			zap.String("reserved node", nodeID),
			zap.String("appID", appID))
	}
	node := pc.GetNode(nodeID)
	if node == nil {
		log.Log(log.SchedPartition).Info("Node was removed while allocating",
			zap.String("nodeID", nodeID),
			zap.String("appID", appID))

		// attempt to deallocate
		if alloc.IsAllocated() {
			allocKey := alloc.GetAllocationKey()
			if _, err := app.DeallocateAsk(allocKey); err != nil {
				log.Log(log.SchedPartition).Warn("Failed to unwind allocation",
					zap.String("nodeID", nodeID),
					zap.String("appID", appID),
					zap.String("allocationKey", allocKey),
					zap.Error(err))
			}
		}
		return nil
	}
	// reservation
	if result.ResultType == objects.Reserved {
		pc.reserve(app, node, result.Request)
		return nil
	}
	// unreserve
	if result.ResultType == objects.Unreserved || result.ResultType == objects.AllocatedReserved {
		pc.unReserve(app, node, result.Request)
		if result.ResultType == objects.Unreserved {
			return nil
		}
		// remove the link to the reserved node
		result.ReservedNodeID = ""
	}

	alloc.SetBindTime(time.Now())
	alloc.SetNodeID(nodeID)
	alloc.SetInstanceType(node.GetInstanceType())

	// track the number of allocations
	pc.updateAllocationCount(1)
	if result.Request.IsPlaceholder() {
		pc.incPhAllocationCount()
	}

	log.Log(log.SchedPartition).Info("scheduler allocation processed",
		zap.String("appID", result.Request.GetApplicationID()),
		zap.String("allocationKey", result.Request.GetAllocationKey()),
		zap.Stringer("allocatedResource", result.Request.GetAllocatedResource()),
		zap.Bool("placeholder", result.Request.IsPlaceholder()),
		zap.String("targetNode", alloc.GetNodeID()))
	// pass the allocation result back to the RM via the cluster context
	return result
}

// Process the reservation in the scheduler
// Lock free call this must be called holding the context lock
func (pc *PartitionContext) reserve(app *objects.Application, node *objects.Node, ask *objects.Allocation) {
	appID := app.ApplicationID
	// app has node already reserved cannot reserve again
	if app.IsReservedOnNode(node.NodeID) {
		log.Log(log.SchedPartition).Info("Application is already reserved on node",
			zap.String("appID", appID),
			zap.String("nodeID", node.NodeID))
		return
	}
	// all ok, add the reservation to the app, this will also reserve the node
	if err := app.Reserve(node, ask); err != nil {
		log.Log(log.SchedPartition).Debug("Failed to handle reservation, error during update of app",
			zap.Error(err))
		return
	}

	// add the reservation to the queue list
	app.GetQueue().Reserve(appID)
	pc.incReservationCount()

	log.Log(log.SchedPartition).Info("allocation ask is reserved",
		zap.String("appID", appID),
		zap.String("queue", app.GetQueuePath()),
		zap.String("allocationKey", ask.GetAllocationKey()),
		zap.String("node", node.NodeID))
}

// unReserve removes the reservation from the objects in the scheduler
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) unReserve(app *objects.Application, node *objects.Node, ask *objects.Allocation) {
	// remove the reservation of the app, this will also unReserve the node
	var err error
	var num int
	if num, err = app.UnReserve(node, ask); err != nil {
		log.Log(log.SchedPartition).Info("Failed to unreserve, error during allocate on the app",
			zap.Error(err))
		return
	}
	// remove the reservation of the queue
	appID := app.ApplicationID
	app.GetQueue().UnReserve(appID, num)
	pc.decReservationCount(num)

	log.Log(log.SchedPartition).Info("allocation ask is unreserved",
		zap.String("appID", appID),
		zap.String("queue", app.GetQueuePath()),
		zap.String("allocationKey", ask.GetAllocationKey()),
		zap.String("node", node.NodeID),
		zap.Int("reservationsRemoved", num))
}

// Create an ordered node iterator based on the node sort policy set for this partition.
// The iterator is nil if there are no unreserved nodes available.
func (pc *PartitionContext) GetNodeIterator() objects.NodeIterator {
	return pc.nodes.GetNodeIterator()
}

// Create an ordered node iterator based on the node sort policy set for this partition.
// The iterator is nil if there are no nodes available.
func (pc *PartitionContext) GetFullNodeIterator() objects.NodeIterator {
	return pc.nodes.GetFullNodeIterator()
}

// Updated the allocations counter for the partition
func (pc *PartitionContext) updateAllocationCount(allocs int) {
	pc.Lock()
	defer pc.Unlock()
	pc.allocations += allocs
}

func (pc *PartitionContext) GetTotalPartitionResource() *resources.Resource {
	pc.RLock()
	defer pc.RUnlock()

	return pc.totalPartitionResource.Clone()
}

func (pc *PartitionContext) GetAllocatedResource() *resources.Resource {
	pc.RLock()
	defer pc.RUnlock()

	return pc.root.GetAllocatedResource()
}

func (pc *PartitionContext) GetTotalAllocationCount() int {
	pc.RLock()
	defer pc.RUnlock()
	return pc.allocations
}

func (pc *PartitionContext) GetTotalNodeCount() int {
	return pc.nodes.GetNodeCount()
}

// GetApplications returns a slice of the current applications tracked by the partition.
func (pc *PartitionContext) GetApplications() []*objects.Application {
	pc.RLock()
	defer pc.RUnlock()
	var appList []*objects.Application
	for _, app := range pc.applications {
		appList = append(appList, app)
	}
	return appList
}

// GetCompletedApplications returns a slice of the completed applications tracked by the partition.
func (pc *PartitionContext) GetCompletedApplications() []*objects.Application {
	pc.RLock()
	defer pc.RUnlock()
	var appList []*objects.Application
	for _, app := range pc.completedApplications {
		appList = append(appList, app)
	}
	return appList
}

// GetRejectedApplications returns a slice of the rejected applications tracked by the partition.
func (pc *PartitionContext) GetRejectedApplications() []*objects.Application {
	pc.RLock()
	defer pc.RUnlock()
	var appList []*objects.Application
	for _, app := range pc.rejectedApplications {
		appList = append(appList, app)
	}
	return appList
}

func (pc *PartitionContext) getAppsState(appMap map[string]*objects.Application, state string) []string {
	pc.RLock()
	defer pc.RUnlock()
	apps := []string{}
	for appID, app := range appMap {
		if app.CurrentState() == state {
			apps = append(apps, appID)
		}
	}
	return apps
}

// getAppsByState returns a slice of applicationIDs for the current applications filtered by state
// Completed and Rejected applications are tracked in a separate map and will never be included.
func (pc *PartitionContext) getAppsByState(state string) []string {
	return pc.getAppsState(pc.applications, state)
}

// getRejectedAppsByState returns a slice of applicationIDs for the rejected applications filtered by state.
func (pc *PartitionContext) getRejectedAppsByState(state string) []string {
	return pc.getAppsState(pc.rejectedApplications, state)
}

// getCompletedAppsByState returns a slice of applicationIDs for the completed applicationIDs filtered by state.
func (pc *PartitionContext) getCompletedAppsByState(state string) []string {
	return pc.getAppsState(pc.completedApplications, state)
}

// cleanupExpiredApps cleans up applications in the Expired state from the three tracking maps
func (pc *PartitionContext) cleanupExpiredApps() {
	for _, appID := range pc.getAppsByState(objects.Expired.String()) {
		pc.Lock()
		delete(pc.applications, appID)
		pc.Unlock()
	}
	for _, appID := range pc.getRejectedAppsByState(objects.Expired.String()) {
		pc.Lock()
		delete(pc.rejectedApplications, appID)
		pc.Unlock()
	}
	for _, appID := range pc.getCompletedAppsByState(objects.Expired.String()) {
		pc.Lock()
		delete(pc.completedApplications, appID)
		pc.Unlock()
	}
}

// GetNodes returns a slice of all nodes unfiltered from the iterator
func (pc *PartitionContext) GetNodes() []*objects.Node {
	return pc.nodes.GetNodes()
}

// Add an allocation to the partition/node/application/queue during node registration.
// Queue max allocation is not checked as the allocation is part of a new node addition.
//
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) addAllocation(alloc *objects.Allocation) error {
	// cannot do anything with a nil alloc, should only happen if the shim broke things badly
	if alloc == nil {
		return nil
	}
	if pc.isStopped() {
		return fmt.Errorf("partition %s is stopped cannot add new allocation %s", pc.Name, alloc.GetAllocationKey())
	}

	log.Log(log.SchedPartition).Info("adding recovered allocation",
		zap.String("partitionName", pc.Name),
		zap.String("appID", alloc.GetApplicationID()),
		zap.String("allocationKey", alloc.GetAllocationKey()))

	// Check if allocation violates any resource restriction, or allocate on a
	// non-existent application or nodes.
	node := pc.GetNode(alloc.GetNodeID())
	if node == nil {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("failed to find node %s", alloc.GetNodeID())
	}

	app := pc.getApplication(alloc.GetApplicationID())
	if app == nil {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("failed to find application %s", alloc.GetApplicationID())
	}
	queue := app.GetQueue()

	// Do not check if the new allocation goes beyond the queue's max resource (recursive).
	// still handle a returned error but they should never happen.
	if err := queue.IncAllocatedResource(alloc.GetAllocatedResource(), true); err != nil {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("cannot allocate resource from application %s: %v ",
			alloc.GetApplicationID(), err)
	}

	metrics.GetQueueMetrics(queue.GetQueuePath()).IncAllocatedContainer()
	node.AddAllocation(alloc)
	alloc.SetInstanceType(node.GetInstanceType())
	app.RecoverAllocationAsk(alloc)
	app.AddAllocation(alloc)

	// track the number of allocations
	pc.updateAllocationCount(1)
	if alloc.IsPlaceholder() {
		pc.incPhAllocationCount()
	}

	log.Log(log.SchedPartition).Info("recovered allocation",
		zap.String("partitionName", pc.Name),
		zap.String("appID", alloc.GetApplicationID()),
		zap.String("allocationKey", alloc.GetAllocationKey()),
		zap.Bool("placeholder", alloc.IsPlaceholder()))
	return nil
}

func (pc *PartitionContext) convertUGI(ugi *si.UserGroupInformation, forced bool) (security.UserGroup, error) {
	pc.RLock()
	defer pc.RUnlock()
	return pc.userGroupCache.ConvertUGI(ugi, forced)
}

// calculate overall nodes resource usage and returns a map as the result,
// where the key is the resource name, e.g memory, and the value is a []int,
// which is a slice with 10 elements,
// each element represents a range of resource usage,
// such as
//
//	0: 0%->10%
//	1: 10% -> 20%
//	...
//	9: 90% -> 100%
//
// the element value represents number of nodes fall into this bucket.
// if slice[9] = 3, this means there are 3 nodes resource usage is in the range 80% to 90%.
//
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) calculateNodesResourceUsage() map[string][]int {
	nodesCopy := pc.GetNodes()
	mapResult := make(map[string][]int)
	for _, node := range nodesCopy {
		capacity := node.GetCapacity()
		allocated := node.GetAllocatedResource()
		for name, total := range capacity.Resources {
			if total > 0 {
				resourceAllocated := float64(allocated.Resources[name])
				// Consider over-allocated node as 100% utilized.
				v := math.Min(resourceAllocated/float64(total), 1)
				idx := int(math.Dim(math.Ceil(v*10), 1))
				if dist, ok := mapResult[name]; !ok {
					newDist := make([]int, 10)
					for i := range newDist {
						newDist[i] = 0
					}
					mapResult[name] = newDist
					mapResult[name][idx]++
				} else {
					dist[idx]++
				}
			}
		}
	}
	return mapResult
}

func (pc *PartitionContext) generateReleased(release *si.AllocationRelease, app *objects.Application) []*objects.Allocation {
	released := make([]*objects.Allocation, 0)
	// when allocationKey is not specified, remove all allocations from the app
	allocationKey := release.GetAllocationKey()
	if allocationKey == "" {
		log.Log(log.SchedPartition).Info("remove all allocations",
			zap.String("appID", app.ApplicationID))
		released = append(released, app.RemoveAllAllocations()...)
	} else {
		// if we have an allocationKey the termination type is important
		if release.TerminationType == si.TerminationType_PLACEHOLDER_REPLACED {
			log.Log(log.SchedPartition).Info("replacing placeholder allocation",
				zap.String("appID", app.ApplicationID),
				zap.String("allocationKey", allocationKey))
			if alloc := app.ReplaceAllocation(allocationKey); alloc != nil {
				released = append(released, alloc)
			}
		} else {
			log.Log(log.SchedPartition).Info("removing allocation from application",
				zap.String("appID", app.ApplicationID),
				zap.String("allocationKey", allocationKey),
				zap.Stringer("terminationType", release.TerminationType))
			if alloc := app.RemoveAllocation(allocationKey, release.TerminationType); alloc != nil {
				released = append(released, alloc)
			}
		}
	}
	return released
}

// removeAllocation removes the referenced allocation(s) from the applications and nodes
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) removeAllocation(release *si.AllocationRelease) ([]*objects.Allocation, *objects.Allocation) {
	if release == nil {
		return nil, nil
	}
	appID := release.ApplicationID
	allocationKey := release.GetAllocationKey()
	app := pc.getApplication(appID)
	// no app nothing to do everything should already be clean
	if app == nil {
		log.Log(log.SchedPartition).Info("Application not found while releasing allocation",
			zap.String("appID", appID),
			zap.String("allocationId", allocationKey),
			zap.Stringer("terminationType", release.TerminationType))
		return nil, nil
	}

	// temp store for allocations manipulated
	released := pc.generateReleased(release, app)
	var confirmed *objects.Allocation

	// all releases are collected: placeholder count needs updating for all placeholder releases
	// regardless of what happens later
	phReleases := 0
	for _, r := range released {
		if r.IsPlaceholder() {
			phReleases++
		}
	}
	if phReleases > 0 {
		pc.decPhAllocationCount(phReleases)
	}

	// for each allocation to release, update the node and queue.
	total := resources.NewResource()
	totalPreempting := resources.NewResource()
	for _, alloc := range released {
		node := pc.GetNode(alloc.GetNodeID())
		if node == nil {
			log.Log(log.SchedPartition).Warn("node not found while releasing allocation",
				zap.String("appID", appID),
				zap.String("allocationKey", alloc.GetAllocationKey()),
				zap.String("nodeID", alloc.GetNodeID()))
			continue
		}
		if release.TerminationType == si.TerminationType_PLACEHOLDER_REPLACED {
			confirmed = alloc.GetRelease()
			// we need to check the resources equality
			delta := resources.Sub(confirmed.GetAllocatedResource(), alloc.GetAllocatedResource())
			// Any negative value in the delta means that at least one of the requested resource in the
			// placeholder is larger than the real allocation. The node and queue need adjusting.
			// The reverse case is handled during allocation.
			if delta.HasNegativeValue() {
				// This looks incorrect but the delta is negative and the result will be an increase of the
				// total tracked. The total will later be deducted from the queue usage.
				total.SubFrom(delta)
				log.Log(log.SchedPartition).Warn("replacing placeholder: placeholder is larger than real allocation",
					zap.String("allocationKey", confirmed.GetAllocationKey()),
					zap.Stringer("requested resource", confirmed.GetAllocatedResource()),
					zap.String("placeholderKey", alloc.GetAllocationKey()),
					zap.Stringer("placeholder resource", alloc.GetAllocatedResource()))
			}
			// replacements could be on a different node and different size handle all cases
			if confirmed.GetNodeID() == alloc.GetNodeID() {
				// this is the real swap on the node, adjust usage if needed
				node.ReplaceAllocation(alloc.GetAllocationKey(), confirmed, delta)
			} else {
				// we have already added the real allocation to the new node, just remove the placeholder
				node.RemoveAllocation(alloc.GetAllocationKey())
			}
			log.Log(log.SchedPartition).Info("replacing placeholder allocation on node",
				zap.String("nodeID", alloc.GetNodeID()),
				zap.String("allocationKey", alloc.GetAllocationKey()),
				zap.String("allocation nodeID", confirmed.GetNodeID()))
		} else if node.RemoveAllocation(alloc.GetAllocationKey()) != nil {
			// all non replacement are real removes: must update the queue usage
			total.AddTo(alloc.GetAllocatedResource())
			log.Log(log.SchedPartition).Info("removing allocation from node",
				zap.String("nodeID", alloc.GetNodeID()),
				zap.String("allocationKey", alloc.GetAllocationKey()))
		}
		if alloc.IsPreempted() {
			totalPreempting.AddTo(alloc.GetAllocatedResource())
		}
	}

	// Processing a removal while in the Completing state could race with the state change.
	// The race occurs between removing the allocation and updating the queue after node processing.
	// If the state change removes the queue link before we get to updating the queue after the node we
	// leave the resources as allocated on the queue. The queue cannot be removed yet at this point as
	// there are still allocations left. So retrieve the queue early to sidestep the race.
	queue := app.GetQueue()

	if resources.StrictlyGreaterThanZero(total) {
		if err := queue.DecAllocatedResource(total); err != nil {
			log.Log(log.SchedPartition).Warn("failed to release resources from queue",
				zap.String("appID", appID),
				zap.String("allocationKey", allocationKey),
				zap.Error(err))
		}
	}
	if resources.StrictlyGreaterThanZero(totalPreempting) {
		queue.DecPreemptingResource(totalPreempting)
	}

	// if confirmed is set we can assume there will just be one alloc in the released
	// that allocation was already released by the shim, so clean up released
	if confirmed != nil {
		released = nil
	}
	// track the number of allocations, when we replace the result is no change
	if allocReleases := len(released); allocReleases > 0 {
		pc.updateAllocationCount(-allocReleases)
		metrics.GetQueueMetrics(queue.GetQueuePath()).AddReleasedContainers(allocReleases)
	}

	// if the termination type is TIMEOUT/PREEMPTED_BY_SCHEDULER, we don't notify the shim,
	// because the release that is processed now is a confirmation returned by the shim to the core
	if release.TerminationType == si.TerminationType_TIMEOUT || release.TerminationType == si.TerminationType_PREEMPTED_BY_SCHEDULER {
		released = nil
	}
	return released, confirmed
}

// Remove the allocation ask from the specified application
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) removeAllocationAsk(release *si.AllocationAskRelease) {
	if release == nil {
		return
	}
	appID := release.ApplicationID
	allocKey := release.AllocationKey
	// A timeout termination is send by the core to the shim, ignore on return.
	if release.TerminationType == si.TerminationType_TIMEOUT {
		log.Log(log.SchedPartition).Debug("Ignoring ask release with termination type Timeout",
			zap.String("appID", appID),
			zap.String("ask", allocKey))
		return
	}
	app := pc.getApplication(appID)
	if app == nil {
		log.Log(log.SchedPartition).Info("Invalid ask release requested by shim",
			zap.String("appID", appID),
			zap.String("ask", allocKey),
			zap.Stringer("terminationType", release.TerminationType))
		return
	}
	// remove the allocation asks from the app
	_ = app.RemoveAllocationAsk(allocKey)
}

// Add the allocation ask to the specified application
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) addAllocationAsk(siAsk *si.AllocationAsk) error {
	if siAsk == nil {
		return nil
	}
	app := pc.getApplication(siAsk.ApplicationID)
	if app == nil {
		return fmt.Errorf("failed to find application %s, for allocation ask %s", siAsk.ApplicationID, siAsk.AllocationKey)
	}
	// add the allocation asks to the app
	return app.AddAllocationAsk(objects.NewAllocationAskFromSI(siAsk))
}

func (pc *PartitionContext) GetCurrentState() string {
	return pc.stateMachine.Current()
}

func (pc *PartitionContext) GetStateTime() time.Time {
	pc.RLock()
	defer pc.RUnlock()
	return pc.stateTime
}

func (pc *PartitionContext) GetNodeSortingPolicyType() policies.SortingPolicy {
	policy := pc.nodes.GetNodeSortingPolicy()
	return policy.PolicyType()
}

func (pc *PartitionContext) GetNodeSortingResourceWeights() map[string]float64 {
	policy := pc.nodes.GetNodeSortingPolicy()
	return policy.ResourceWeights()
}

func (pc *PartitionContext) isPreemptionEnabled() bool {
	pc.RLock()
	defer pc.RUnlock()
	return pc.preemptionEnabled
}

func (pc *PartitionContext) moveTerminatedApp(appID string) {
	app := pc.getApplication(appID)
	// nothing to do if the app is not found on the partition
	if app == nil {
		log.Log(log.SchedPartition).Debug("Application already removed from app list",
			zap.String("appID", appID))
		return
	}
	app.UnSetQueue()
	// new ID as completedApplications map key, use negative value to get a divider
	newID := appID + strconv.FormatInt(-(time.Now()).Unix(), 10)
	log.Log(log.SchedPartition).Info("Removing terminated application from the application list",
		zap.String("appID", appID),
		zap.String("app status", app.CurrentState()))
	app.LogAppSummary(pc.RmID)
	app.CleanupTrackedResource()
	pc.Lock()
	defer pc.Unlock()
	delete(pc.applications, appID)
	pc.completedApplications[newID] = app
}

func (pc *PartitionContext) AddRejectedApplication(rejectedApplication *objects.Application, rejectedMessage string) {
	if err := rejectedApplication.RejectApplication(rejectedMessage); err != nil {
		log.Log(log.SchedPartition).Warn("BUG: Unexpected failure: Application state not changed to Rejected",
			zap.String("currentState", rejectedApplication.CurrentState()),
			zap.Error(err))
	}
	if pc.rejectedApplications == nil {
		pc.rejectedApplications = make(map[string]*objects.Application)
	}
	pc.rejectedApplications[rejectedApplication.ApplicationID] = rejectedApplication
}

func (pc *PartitionContext) incPhAllocationCount() {
	pc.Lock()
	defer pc.Unlock()
	pc.placeholderAllocations++
}

func (pc *PartitionContext) decPhAllocationCount(num int) {
	pc.Lock()
	defer pc.Unlock()
	pc.placeholderAllocations -= num
}

func (pc *PartitionContext) getPhAllocationCount() int {
	pc.RLock()
	defer pc.RUnlock()
	return pc.placeholderAllocations
}

func (pc *PartitionContext) incReservationCount() {
	pc.Lock()
	defer pc.Unlock()
	pc.reservations++
}

func (pc *PartitionContext) decReservationCount(num int) {
	pc.Lock()
	defer pc.Unlock()
	pc.reservations -= num
}

func (pc *PartitionContext) getReservationCount() int {
	pc.RLock()
	defer pc.RUnlock()
	return pc.reservations
}
