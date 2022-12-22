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
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/placement"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type PartitionContext struct {
	RmID string // the RM the partition belongs to
	Name string // name of the partition (logging mainly)

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
	isPreemptable          bool                            // can allocations be preempted
	rules                  *[]configs.PlacementRule        // placement rules to be loaded by the scheduler
	userGroupCache         *security.UserGroupCache        // user cache per partition
	totalPartitionResource *resources.Resource             // Total node resources
	allocations            int                             // Number of allocations on the partition

	// The partition write lock must not be held while manipulating an application.
	// Scheduling is running continuously as a lock free background task. Scheduling an application
	// acquires a write lock of the application object. While holding the write lock a list of nodes is
	// requested from the partition. This requires a read lock on the partition.
	// If the partition write lock is held while manipulating an application a dead lock could occur.
	// Since application objects handle their own locks there is no requirement to hold the partition lock
	// while manipulating the application.
	// Similarly adding, updating or removing a node or a queue should only hold the partition write lock
	// while manipulating the partition information not while manipulating the underlying objects.
	sync.RWMutex
}

func newPartitionContext(conf configs.PartitionConfig, rmID string, cc *ClusterContext) (*PartitionContext, error) {
	if conf.Name == "" || rmID == "" {
		log.Logger().Info("partition cannot be created",
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
	log.Logger().Info("root queue added",
		zap.String("partitionName", pc.Name),
		zap.String("rmID", pc.RmID))

	// set preemption needed flag
	pc.isPreemptable = conf.Preemption.Enabled

	pc.rules = &conf.PlacementRules
	// We need to pass in the locked version of the GetQueue function.
	// Placing an application will not have a lock on the partition context.
	pc.placementManager = placement.NewPlacementManager(*pc.rules, pc.GetQueue)
	// get the user group cache for the partition
	// TODO get the resolver from the config
	pc.userGroupCache = security.GetUserGroupCache("")
	pc.updateNodeSortingPolicy(conf)
	return nil
}

// NOTE: this is a lock free call. It should only be called holding the PartitionContext lock.
func (pc *PartitionContext) updateNodeSortingPolicy(conf configs.PartitionConfig) {
	var configuredPolicy policies.SortingPolicy
	configuredPolicy, err := policies.SortingPolicyFromString(conf.NodeSortPolicy.Type)
	if err != nil {
		log.Logger().Debug("NodeSorting policy incorrectly set or unknown",
			zap.Error(err))
		log.Logger().Info(fmt.Sprintf("NodeSorting policy not set using '%s' as default", configuredPolicy))
	} else {
		log.Logger().Info("NodeSorting policy set from config",
			zap.String("policyName", configuredPolicy.String()))
	}
	pc.nodes.SetNodeSortingPolicy(objects.NewNodeSortingPolicy(conf.NodeSortPolicy.Type, conf.NodeSortPolicy.ResourceWeights))
}

func (pc *PartitionContext) updatePartitionDetails(conf configs.PartitionConfig) error {
	pc.Lock()
	defer pc.Unlock()
	if len(conf.Queues) == 0 || conf.Queues[0].Name != configs.RootQueue {
		return fmt.Errorf("partition cannot be created without root queue")
	}

	if pc.placementManager.IsInitialised() {
		log.Logger().Info("Updating placement manager rules on config reload")
		err := pc.placementManager.UpdateRules(conf.PlacementRules)
		if err != nil {
			log.Logger().Info("New placement rules not activated, config reload failed", zap.Error(err))
			return err
		}
		pc.rules = &conf.PlacementRules
	} else {
		log.Logger().Info("Creating new placement manager on config reload")
		pc.rules = &conf.PlacementRules
		// We need to pass in the locked version of the GetQueue function.
		// Placing an application will not have a lock on the partition context.
		pc.placementManager = placement.NewPlacementManager(*pc.rules, pc.GetQueue)
	}
	pc.updateNodeSortingPolicy(conf)
	// start at the root: there is only one queue
	queueConf := conf.Queues[0]
	root := pc.root
	// update the root queue
	if err := root.ApplyConf(queueConf); err != nil {
		return err
	}
	root.UpdateQueueProperties()
	// update the rest of the queues recursively
	return pc.updateQueues(queueConf.Queues, root)
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
		log.Logger().Error("failed to mark partition for deletion",
			zap.String("partitionName", pc.Name),
			zap.Error(err))
	}
}

// Get the state of the partition.
// No new nodes and applications will be accepted if stopped or being removed.
func (pc *PartitionContext) isDraining() bool {
	return pc.stateMachine.Current() == objects.Draining.String()
}

func (pc *PartitionContext) isRunning() bool {
	return pc.stateMachine.Current() == objects.Active.String()
}

func (pc *PartitionContext) isStopped() bool {
	return pc.stateMachine.Current() == objects.Stopped.String()
}

// Handle the state event for the partition.
// The state machine handles the locking.
func (pc *PartitionContext) handlePartitionEvent(event objects.ObjectEvent) error {
	err := pc.stateMachine.Event(event.String(), pc.Name)
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

// Add a new application to the partition.
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

	// Put app under the queue
	queueName := app.GetQueuePath()
	pm := pc.getPlacementManager()
	if pm.IsInitialised() {
		err := pm.PlaceApplication(app)
		if err != nil {
			return fmt.Errorf("failed to place application %s: %v", appID, err)
		}
		queueName = app.GetQueuePath()
		if queueName == "" {
			return fmt.Errorf("application rejected by placement rules: %s", appID)
		}
	}
	// lock the partition and make the last change: we need to do this before creating the queues.
	// queue cleanup might otherwise remove the queue again before we can add the application
	pc.Lock()
	defer pc.Unlock()
	// we have a queue name either from placement or direct, get the queue
	queue := pc.getQueueInternal(queueName)
	if queue == nil {
		// queue must exist if not using placement rules
		if !pm.IsInitialised() {
			return fmt.Errorf("application '%s' rejected, cannot create queue '%s' without placement rules", appID, queueName)
		}
		// with placement rules the hierarchy might not exist so try and create it
		var err error
		queue, err = pc.createQueue(queueName, app.GetUser())
		if err != nil {
			return fmt.Errorf("failed to create rule based queue %s for application %s", queueName, appID)
		}
	}
	// check the queue: is a leaf queue with submit access
	if !queue.IsLeafQueue() || !queue.CheckSubmitAccess(app.GetUser()) {
		return fmt.Errorf("failed to find queue %s for application %s", queueName, appID)
	}

	// add the app to the queue to set the quota on the queue if needed
	queue.AddApplication(app)
	// check only for gang request
	// - make sure the taskgroup request fits in the maximum set for the queue hierarchy
	// - task groups should only be used in FIFO or StateAware queues
	// if the check fails remove the app from the queue again
	if placeHolder := app.GetPlaceholderAsk(); !resources.IsZero(placeHolder) {
		// check the queue sorting
		if !queue.SupportTaskGroup() {
			queue.RemoveApplication(app)
			return fmt.Errorf("queue %s cannot run application %s with task group request: unsupported sort type", queueName, appID)
		}
		// retrieve the max set
		if maxQueue := queue.GetMaxQueueSet(); maxQueue != nil {
			if !maxQueue.FitInMaxUndef(placeHolder) {
				queue.RemoveApplication(app)
				return fmt.Errorf("queue %s cannot fit application %s: task group request %s larger than max queue allocation %s", queueName, appID, placeHolder.String(), maxQueue.String())
			}
		}
	}

	// all is OK update the app and add it to the partition
	app.SetQueue(queue)
	app.SetTerminatedCallback(pc.moveTerminatedApp)
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
			currentUUID := alloc.GetUUID()
			node := pc.GetNode(alloc.GetNodeID())
			if node == nil {
				log.Logger().Warn("unknown node: not found in active node list",
					zap.String("appID", appID),
					zap.String("nodeID", alloc.GetNodeID()))
				continue
			}
			if nodeAlloc := node.RemoveAllocation(currentUUID); nodeAlloc == nil {
				log.Logger().Warn("unknown allocation: not found on the node",
					zap.String("appID", appID),
					zap.String("allocationId", currentUUID),
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

// Get the queue info for the whole queue structure to pass to the webservice
func (pc *PartitionContext) GetQueueInfo() dao.QueueDAOInfo {
	return pc.root.GetQueueInfo()
}

// Get the queue info for the whole queue structure to pass to the webservice
func (pc *PartitionContext) GetPartitionQueues() dao.PartitionQueueDAOInfo {
	var PartitionQueueDAOInfo = dao.PartitionQueueDAOInfo{}
	PartitionQueueDAOInfo = pc.root.GetPartitionQueueDAOInfo()
	PartitionQueueDAOInfo.Partition = common.GetPartitionNameWithoutClusterID(pc.Name)
	return PartitionQueueDAOInfo
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
	log.Logger().Debug("Checking queue creation")
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
	log.Logger().Debug("Creating queue(s)",
		zap.String("parent", current),
		zap.String("fullPath", name))
	for i := len(toCreate) - 1; i >= 0; i-- {
		// everything is checked and there should be no errors
		var err error
		queue, err = objects.NewDynamicQueue(toCreate[i], i == 0, queue)
		if err != nil {
			log.Logger().Warn("Queue auto create failed unexpected",
				zap.String("queueName", toCreate[i]),
				zap.Error(err))
			return nil, err
		}
	}
	return queue, nil
}

// Get a node from the partition by nodeID.
func (pc *PartitionContext) GetNode(nodeID string) *objects.Node {
	pc.RLock()
	defer pc.RUnlock()

	return pc.nodes.GetNode(nodeID)
}

// Add the node to the partition and process the allocations that are reported by the node.
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) AddNode(node *objects.Node, existingAllocations []*objects.Allocation) error {
	if node == nil {
		return fmt.Errorf("cannot add 'nil' node to partition %s", pc.Name)
	}
	log.Logger().Info("adding node to partition",
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
				log.Logger().Info("Failed to add existing allocations, changes reversed",
					zap.String("nodeID", node.NodeID),
					zap.Int("existingAllocations", len(existingAllocations)),
					zap.Int("releasedAllocations", len(released)),
					zap.Int("processingAlloc", current),
					zap.String("allocation", alloc.String()),
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

// Update the partition details when removing a node.
// This locks the partition. The partition may not be locked when we process the allocation
// additions to the node as that takes further app, queue or node locks
func (pc *PartitionContext) addNodeToList(node *objects.Node) error {
	pc.Lock()
	defer pc.Unlock()
	// Node can be added to the system to allow processing of the allocations
	if err := pc.nodes.AddNode(node); err != nil {
		return fmt.Errorf("failed to add node %s to partition %s, error: %v", node.NodeID, pc.Name, err)
	}
	metrics.GetSchedulerMetrics().IncActiveNodes()

	// update/set the resources available in the cluster
	if pc.totalPartitionResource == nil {
		pc.totalPartitionResource = node.GetCapacity().Clone()
	} else {
		pc.totalPartitionResource.AddTo(node.GetCapacity())
	}
	pc.root.SetMaxResource(pc.totalPartitionResource)
	log.Logger().Info("Updated available resources from added node",
		zap.String("partitionName", pc.Name),
		zap.String("nodeID", node.NodeID),
		zap.String("partitionResource", pc.totalPartitionResource.String()))
	return nil
}

// Update the partition details when removing a node.
// This locks the partition. The partition may not be locked when we process the allocation
// removal from the node as that takes further app, queue or node locks
func (pc *PartitionContext) removeNodeFromList(nodeID string) *objects.Node {
	pc.Lock()
	defer pc.Unlock()
	node := pc.nodes.RemoveNode(nodeID)
	if node == nil {
		log.Logger().Debug("node was not found, node already removed",
			zap.String("nodeID", nodeID),
			zap.String("partitionName", pc.Name))
		return nil
	}

	// Remove node from list of tracked nodes
	metrics.GetSchedulerMetrics().DecActiveNodes()

	// found the node cleanup the available resources, partition resources cannot be nil at this point
	pc.totalPartitionResource.SubFrom(node.GetCapacity())
	pc.root.SetMaxResource(pc.totalPartitionResource)
	log.Logger().Info("Updated available resources from removed node",
		zap.String("partitionName", pc.Name),
		zap.String("nodeID", node.NodeID),
		zap.String("partitionResource", pc.totalPartitionResource.String()))
	return node
}

// Remove a node from the partition. It returns all removed and confirmed allocations.
// The removed allocations are all linked to the current node.
// The confirmed allocations are real allocations that are linked to placeholders on the current node and are linked to
// other nodes.
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) removeNode(nodeID string) ([]*objects.Allocation, []*objects.Allocation) {
	log.Logger().Info("removing node from partition",
		zap.String("partition", pc.Name),
		zap.String("nodeID", nodeID))

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
	// cleanup the allocations linked to the node
	return pc.removeNodeAllocations(node)
}

// Remove all allocations that are assigned to a node as part of the node removal. This is not part of the node object
// as updating the applications and queues is the only goal. Applications and queues are not accessible from the node.
// The removed and confirmed allocations are returned.
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) removeNodeAllocations(node *objects.Node) ([]*objects.Allocation, []*objects.Allocation) {
	released := make([]*objects.Allocation, 0)
	confirmed := make([]*objects.Allocation, 0)
	// walk over all allocations still registered for this node
	for _, alloc := range node.GetAllAllocations() {
		allocID := alloc.GetUUID()
		// since we are not locking the node and or application we could have had an update while processing
		// note that we do not return the allocation if the app or allocation is not found and assume that it
		// was already removed
		app := pc.getApplication(alloc.GetApplicationID())
		if app == nil {
			log.Logger().Info("app is not found, skipping while removing the node",
				zap.String("appID", alloc.GetApplicationID()),
				zap.String("nodeID", node.NodeID))
			continue
		}
		// check for an inflight replacement.
		if alloc.GetReleaseCount() != 0 {
			release := alloc.GetFirstRelease()
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
					_ = app.ReplaceAllocation(allocID)
					// we need to check the resources equality
					delta := resources.Sub(release.GetAllocatedResource(), alloc.GetAllocatedResource())
					// Any negative value in the delta means that at least one of the requested resource in the
					// placeholder is larger than the real allocation. The nodes are correct the queue needs adjusting.
					// The reverse case is handled during allocation.
					if delta.HasNegativeValue() {
						// this looks incorrect but the delta is negative and the result will be a real decrease
						err := app.GetQueue().IncAllocatedResource(delta, false)
						// this should not happen as we really decrease the value
						if err != nil {
							log.Logger().Warn("unexpected failure during queue update: replacing placeholder",
								zap.String("appID", alloc.GetApplicationID()),
								zap.String("placeholderID", alloc.GetUUID()),
								zap.String("allocationID", release.GetUUID()),
								zap.Error(err))
						}
						log.Logger().Warn("replacing placeholder: placeholder is larger than real allocation",
							zap.String("allocationID", release.GetUUID()),
							zap.String("requested resource", release.GetAllocatedResource().String()),
							zap.String("placeholderID", alloc.GetUUID()),
							zap.String("placeholder resource", alloc.GetAllocatedResource().String()))
					}
					// track what we confirm on the other node to confirm it in the shim and get is bound
					confirmed = append(confirmed, release)
					// the allocation is removed so add it to the list that we return
					released = append(released, alloc)
					log.Logger().Info("allocation removed from node and replacement confirmed",
						zap.String("nodeID", node.NodeID),
						zap.String("allocationId", allocID),
						zap.String("replacement nodeID", release.GetNodeID()),
						zap.String("replacement allocationId", release.GetUUID()))
					continue
				}
				askAlloc = release
			}
			// unlink the placeholder and allocation
			release.ClearReleases()
			alloc.ClearReleases()
			// update the repeat on the real alloc to get it re-scheduled
			_, err := app.UpdateAskRepeat(askAlloc.GetAsk().GetAllocationKey(), 1)
			if err == nil {
				log.Logger().Info("inflight placeholder replacement reversed due to node removal",
					zap.String("appID", askAlloc.GetApplicationID()),
					zap.String("allocationKey", askAlloc.GetAsk().GetAllocationKey()),
					zap.String("nodeID", node.NodeID),
					zap.String("replacement allocationId", askAlloc.GetUUID()))
			} else {
				log.Logger().Error("node removal: repeat update failure for inflight replacement",
					zap.String("appID", askAlloc.GetApplicationID()),
					zap.String("allocationKey", askAlloc.GetAsk().GetAllocationKey()),
					zap.String("nodeID", node.NodeID),
					zap.Error(err))
			}
		}
		// check allocations on the app
		if app.RemoveAllocation(allocID, si.TerminationType_UNKNOWN_TERMINATION_TYPE) == nil {
			log.Logger().Info("allocation is not found, skipping while removing the node",
				zap.String("allocationId", allocID),
				zap.String("appID", app.ApplicationID),
				zap.String("nodeID", node.NodeID))
			continue
		}
		if err := app.GetQueue().DecAllocatedResource(alloc.GetAllocatedResource()); err != nil {
			log.Logger().Warn("failed to release resources from queue",
				zap.String("appID", alloc.GetApplicationID()),
				zap.Error(err))
		} else {
			metrics.GetQueueMetrics(app.GetQueuePath()).IncReleasedContainer()
		}

		// the allocation is removed so add it to the list that we return
		released = append(released, alloc)
		log.Logger().Info("allocation removed from node",
			zap.String("nodeID", node.NodeID),
			zap.String("allocationId", allocID))
	}
	// track the number of allocations: decrement the released allocation AND increment with the confirmed
	pc.updateAllocationCount(len(confirmed) - len(released))
	return released, confirmed
}

func (pc *PartitionContext) calculateOutstandingRequests() []*objects.AllocationAsk {
	if !resources.StrictlyGreaterThanZero(pc.root.GetPendingResource()) {
		return nil
	}
	outstanding := make([]*objects.AllocationAsk, 0)
	pc.root.GetQueueOutstandingRequests(&outstanding)
	return outstanding
}

// Try regular allocation for the partition
// Lock free call this all locks are taken when needed in called functions
func (pc *PartitionContext) tryAllocate() *objects.Allocation {
	if !resources.StrictlyGreaterThanZero(pc.root.GetPendingResource()) {
		// nothing to do just return
		return nil
	}
	// try allocating from the root down
	alloc := pc.root.TryAllocate(pc.GetNodeIterator, pc.GetNode)
	if alloc != nil {
		return pc.allocate(alloc)
	}
	return nil
}

// Try process reservations for the partition
// Lock free call this all locks are taken when needed in called functions
func (pc *PartitionContext) tryReservedAllocate() *objects.Allocation {
	if !resources.StrictlyGreaterThanZero(pc.root.GetPendingResource()) {
		// nothing to do just return
		return nil
	}
	// try allocating from the root down
	alloc := pc.root.TryReservedAllocate(pc.GetNodeIterator)
	if alloc != nil {
		return pc.allocate(alloc)
	}
	return nil
}

// Try process placeholder for the partition
// Lock free call this all locks are taken when needed in called functions
func (pc *PartitionContext) tryPlaceholderAllocate() *objects.Allocation {
	if !resources.StrictlyGreaterThanZero(pc.root.GetPendingResource()) {
		// nothing to do just return
		return nil
	}
	// try allocating from the root down
	alloc := pc.root.TryPlaceholderAllocate(pc.GetNodeIterator, pc.GetNode)
	if alloc != nil {
		log.Logger().Info("scheduler replace placeholder processed",
			zap.String("appID", alloc.GetApplicationID()),
			zap.String("allocationKey", alloc.GetAllocationKey()),
			zap.String("uuid", alloc.GetUUID()),
			zap.String("placeholder released uuid", alloc.GetFirstRelease().GetUUID()))
		// pass the release back to the RM via the cluster context
		return alloc
	}
	return nil
}

// Process the allocation and make the left over changes in the partition.
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) allocate(alloc *objects.Allocation) *objects.Allocation {
	// find the app make sure it still exists
	appID := alloc.GetApplicationID()
	app := pc.getApplication(appID)
	if app == nil {
		log.Logger().Info("Application was removed while allocating",
			zap.String("appID", appID))
		return nil
	}
	// find the node make sure it still exists
	// if the node was passed in use that ID instead of the one from the allocation
	// the node ID is set when a reservation is allocated on a non-reserved node
	var nodeID string
	if alloc.GetReservedNodeID() == "" {
		nodeID = alloc.GetNodeID()
	} else {
		nodeID = alloc.GetReservedNodeID()
		log.Logger().Debug("Reservation allocated on different node",
			zap.String("current node", alloc.GetNodeID()),
			zap.String("reserved node", nodeID),
			zap.String("appID", appID))
	}
	node := pc.GetNode(nodeID)
	if node == nil {
		log.Logger().Info("Node was removed while allocating",
			zap.String("nodeID", nodeID),
			zap.String("appID", appID))
		return nil
	}
	// reservation
	if alloc.GetResult() == objects.Reserved {
		pc.reserve(app, node, alloc.GetAsk())
		return nil
	}
	// unreserve
	if alloc.GetResult() == objects.Unreserved || alloc.GetResult() == objects.AllocatedReserved {
		pc.unReserve(app, node, alloc.GetAsk())
		if alloc.GetResult() == objects.Unreserved {
			return nil
		}
		// remove the link to the reserved node
		alloc.SetReservedNodeID("")
	}

	// track the number of allocations
	pc.updateAllocationCount(1)

	log.Logger().Info("scheduler allocation processed",
		zap.String("appID", alloc.GetApplicationID()),
		zap.String("allocationKey", alloc.GetAllocationKey()),
		zap.String("uuid", alloc.GetUUID()),
		zap.String("allocatedResource", alloc.GetAllocatedResource().String()),
		zap.Bool("placeholder", alloc.IsPlaceholder()),
		zap.String("targetNode", alloc.GetNodeID()))
	// pass the allocation back to the RM via the cluster context
	return alloc
}

// Process the reservation in the scheduler
// Lock free call this must be called holding the context lock
func (pc *PartitionContext) reserve(app *objects.Application, node *objects.Node, ask *objects.AllocationAsk) {
	appID := app.ApplicationID
	// app has node already reserved cannot reserve again
	if app.IsReservedOnNode(node.NodeID) {
		log.Logger().Info("Application is already reserved on node",
			zap.String("appID", appID),
			zap.String("nodeID", node.NodeID))
		return
	}
	// all ok, add the reservation to the app, this will also reserve the node
	if err := app.Reserve(node, ask); err != nil {
		log.Logger().Debug("Failed to handle reservation, error during update of app",
			zap.Error(err))
		return
	}

	// add the reservation to the queue list
	app.GetQueue().Reserve(appID)

	log.Logger().Info("allocation ask is reserved",
		zap.String("appID", appID),
		zap.String("queue", app.GetQueuePath()),
		zap.String("allocationKey", ask.GetAllocationKey()),
		zap.String("node", node.NodeID))
}

// unReserve removes the reservation from the objects in the scheduler
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) unReserve(app *objects.Application, node *objects.Node, ask *objects.AllocationAsk) {
	// remove the reservation of the app, this will also unReserve the node
	var err error
	var num int
	if num, err = app.UnReserve(node, ask); err != nil {
		log.Logger().Info("Failed to unreserve, error during allocate on the app",
			zap.Error(err))
		return
	}
	// remove the reservation of the queue
	appID := app.ApplicationID
	app.GetQueue().UnReserve(appID, num)

	log.Logger().Info("allocation ask is unreserved",
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

// Updated the allocations counter for the partition
func (pc *PartitionContext) updateAllocationCount(allocs int) {
	pc.Lock()
	defer pc.Unlock()
	pc.allocations += allocs
}

func (pc *PartitionContext) GetTotalPartitionResource() *resources.Resource {
	pc.RLock()
	defer pc.RUnlock()

	return pc.totalPartitionResource
}

func (pc *PartitionContext) GetAllocatedResource() *resources.Resource {
	pc.RLock()
	defer pc.RUnlock()

	return pc.root.GetAllocatedResource()
}

func (pc *PartitionContext) GetTotalApplicationCount() int {
	pc.RLock()
	defer pc.RUnlock()
	return len(pc.applications)
}

func (pc *PartitionContext) GetTotalAllocationCount() int {
	pc.RLock()
	defer pc.RUnlock()
	return pc.allocations
}

func (pc *PartitionContext) GetTotalNodeCount() int {
	pc.RLock()
	defer pc.RUnlock()
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

// getAppsByState returns a slice of applicationIDs for the current applications filtered by state
// Completed and Rejected applications are tracked in a separate map and will never be included.
func (pc *PartitionContext) getAppsByState(state string) []string {
	pc.RLock()
	defer pc.RUnlock()
	var apps []string
	for appID, app := range pc.applications {
		if app.CurrentState() == state {
			apps = append(apps, appID)
		}
	}
	return apps
}

// getRejectedAppsByState returns a slice of applicationIDs for the rejected applications filtered by state.
func (pc *PartitionContext) getRejectedAppsByState(state string) []string {
	pc.RLock()
	defer pc.RUnlock()
	var apps []string
	for appID, app := range pc.rejectedApplications {
		if app.CurrentState() == state {
			apps = append(apps, appID)
		}
	}
	return apps
}

// getCompletedAppsByState returns a slice of applicationIDs for the completed applicationIDs filtered by state.
func (pc *PartitionContext) getCompletedAppsByState(state string) []string {
	pc.RLock()
	defer pc.RUnlock()
	var apps []string
	for appID, app := range pc.completedApplications {
		if app.CurrentState() == state {
			apps = append(apps, appID)
		}
	}
	return apps
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
	pc.RLock()
	defer pc.RUnlock()
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

	// We must not generate a new uuid for it, we directly use the uuid reported by shim
	// to track this allocation, a missing uuid is a broken allocation
	if alloc.GetUUID() == "" {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("failing to restore allocation %s for application %s: missing uuid",
			alloc.GetAllocationKey(), alloc.GetApplicationID())
	}

	log.Logger().Info("adding recovered allocation",
		zap.String("partitionName", pc.Name),
		zap.String("appID", alloc.GetApplicationID()),
		zap.String("allocKey", alloc.GetAllocationKey()),
		zap.String("uuid", alloc.GetUUID()))

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
	app.RecoverAllocationAsk(alloc.GetAsk())
	app.AddAllocation(alloc)

	// track the number of allocations
	pc.updateAllocationCount(1)

	log.Logger().Info("recovered allocation",
		zap.String("partitionName", pc.Name),
		zap.String("appID", alloc.GetApplicationID()),
		zap.String("allocKey", alloc.GetAllocationKey()),
		zap.String("allocationUid", alloc.GetUUID()),
		zap.Bool("placeholder", alloc.IsPlaceholder()))
	return nil
}

func (pc *PartitionContext) convertUGI(ugi *si.UserGroupInformation) (security.UserGroup, error) {
	pc.RLock()
	defer pc.RUnlock()
	return pc.userGroupCache.ConvertUGI(ugi)
}

// calculate overall nodes resource usage and returns a map as the result,
// where the key is the resource name, e.g memory, and the value is a []int,
// which is a slice with 10 elements,
// each element represents a range of resource usage,
// such as
//   0: 0%->10%
//   1: 10% -> 20%
//   ...
//   9: 90% -> 100%
// the element value represents number of nodes fall into this bucket.
// if slice[9] = 3, this means there are 3 nodes resource usage is in the range 80% to 90%.
//
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) calculateNodesResourceUsage() map[string][]int {
	nodesCopy := pc.GetNodes()
	mapResult := make(map[string][]int)
	for _, node := range nodesCopy {
		for name, total := range node.GetCapacity().Resources {
			if float64(total) > 0 {
				resourceAllocated := float64(node.GetAllocatedResource().Resources[name])
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

// removeAllocation removes the referenced allocation(s) from the applications and nodes
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) removeAllocation(release *si.AllocationRelease) ([]*objects.Allocation, *objects.Allocation) {
	if release == nil {
		return nil, nil
	}
	appID := release.ApplicationID
	uuid := release.GetUUID()
	app := pc.getApplication(appID)
	// no app nothing to do everything should already be clean
	if app == nil {
		log.Logger().Info("Application not found while releasing allocation",
			zap.String("appID", appID),
			zap.String("allocationId", uuid),
			zap.String("terminationType", release.TerminationType.String()))
		return nil, nil
	}
	// temp store for allocations manipulated
	released := make([]*objects.Allocation, 0)
	var confirmed *objects.Allocation
	// when uuid is not specified, remove all allocations from the app
	if uuid == "" {
		log.Logger().Info("remove all allocations",
			zap.String("appID", appID))
		released = append(released, app.RemoveAllAllocations()...)
	} else {
		// if we have an uuid the termination type is important
		if release.TerminationType == si.TerminationType_PLACEHOLDER_REPLACED {
			log.Logger().Info("replacing placeholder allocation",
				zap.String("appID", appID),
				zap.String("allocationId", uuid))
			if alloc := app.ReplaceAllocation(uuid); alloc != nil {
				released = append(released, alloc)
			}
		} else {
			log.Logger().Info("removing allocation from application",
				zap.String("appID", appID),
				zap.String("allocationId", uuid),
				zap.String("terminationType", release.TerminationType.String()))
			if alloc := app.RemoveAllocation(uuid, release.TerminationType); alloc != nil {
				released = append(released, alloc)
			}
		}
	}

	// for each allocation to release, update the node and queue.
	total := resources.NewResource()
	for _, alloc := range released {
		node := pc.GetNode(alloc.GetNodeID())
		if node == nil {
			log.Logger().Info("node not found while releasing allocation",
				zap.String("appID", appID),
				zap.String("allocationId", alloc.GetUUID()),
				zap.String("nodeID", alloc.GetNodeID()))
			continue
		}
		if release.TerminationType == si.TerminationType_PLACEHOLDER_REPLACED {
			confirmed = alloc.GetFirstRelease()
			// we need to check the resources equality
			delta := resources.Sub(confirmed.GetAllocatedResource(), alloc.GetAllocatedResource())
			// Any negative value in the delta means that at least one of the requested resource in the
			// placeholder is larger than the real allocation. The node and queue need adjusting.
			// The reverse case is handled during allocation.
			if delta.HasNegativeValue() {
				// This looks incorrect but the delta is negative and the result will be an increase of the
				// total tracked. The total will later be deducted from the queue usage.
				total.SubFrom(delta)
				log.Logger().Warn("replacing placeholder: placeholder is larger than real allocation",
					zap.String("allocationID", confirmed.GetUUID()),
					zap.String("requested resource", confirmed.GetAllocatedResource().String()),
					zap.String("placeholderID", alloc.GetUUID()),
					zap.String("placeholder resource", alloc.GetAllocatedResource().String()))
			}
			// replacements could be on a different node and different size handle all cases
			if confirmed.GetNodeID() == alloc.GetNodeID() {
				// this is the real swap on the node, adjust usage if needed
				node.ReplaceAllocation(alloc.GetUUID(), confirmed, delta)
			} else {
				// we have already added the real allocation to the new node, just remove the placeholder
				node.RemoveAllocation(alloc.GetUUID())
			}
			log.Logger().Info("replacing placeholder allocation on node",
				zap.String("nodeID", alloc.GetNodeID()),
				zap.String("allocationId", alloc.GetUUID()),
				zap.String("allocation nodeID", confirmed.GetNodeID()))
		} else if node.RemoveAllocation(alloc.GetUUID()) != nil {
			// all non replacement are real removes: must update the queue usage
			total.AddTo(alloc.GetAllocatedResource())
			log.Logger().Info("removing allocation from node",
				zap.String("nodeID", alloc.GetNodeID()),
				zap.String("allocationId", alloc.GetUUID()))
		}
	}
	if resources.StrictlyGreaterThanZero(total) {
		queue := app.GetQueue()
		if err := queue.DecAllocatedResource(total); err != nil {
			log.Logger().Warn("failed to release resources from queue",
				zap.String("appID", appID),
				zap.String("allocationId", uuid),
				zap.Error(err))
		} else {
			metrics.GetQueueMetrics(queue.GetQueuePath()).IncReleasedContainer()
		}
	}
	// if confirmed is set we can assume there will just be one alloc in the released
	// that allocation was already released by the shim, so clean up released

	if confirmed != nil {
		released = nil
	}
	// track the number of allocations, when we replace the result is no change
	pc.updateAllocationCount(-len(released))

	// if the termination type is timeout, we don't notify the shim, because it's
	// originated from that side
	if release.TerminationType == si.TerminationType_TIMEOUT {
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
		log.Logger().Debug("Ignoring ask release with termination type Timeout",
			zap.String("appID", appID),
			zap.String("ask", allocKey))
		return
	}
	app := pc.getApplication(appID)
	if app == nil {
		log.Logger().Info("Invalid ask release requested by shim",
			zap.String("appID", appID),
			zap.String("ask", allocKey),
			zap.String("terminationType", release.TerminationType.String()))
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
	pc.RLock()
	defer pc.RUnlock()
	policy := pc.nodes.GetNodeSortingPolicy()
	return policy.PolicyType()
}

func (pc *PartitionContext) GetNodeSortingResourceWeights() map[string]float64 {
	pc.RLock()
	defer pc.RUnlock()
	policy := pc.nodes.GetNodeSortingPolicy()
	return policy.ResourceWeights()
}

func (pc *PartitionContext) moveTerminatedApp(appID string) {
	app := pc.getApplication(appID)
	// nothing to do if the app is not found on the partition
	if app == nil {
		log.Logger().Debug("Application already removed from app list",
			zap.String("appID", appID))
		return
	}
	app.UnSetQueue()
	// new ID as completedApplications map key, use negative value to get a divider
	newID := appID + strconv.FormatInt(-(time.Now()).Unix(), 10)
	log.Logger().Info("Removing terminated application from the application list",
		zap.String("appID", appID),
		zap.String("app status", app.CurrentState()))
	pc.Lock()
	defer pc.Unlock()
	delete(pc.applications, appID)
	pc.completedApplications[newID] = app
}

func (pc *PartitionContext) AddRejectedApplication(rejectedApplication *objects.Application, rejectedMessage string) {
	if err := rejectedApplication.RejectApplication(rejectedMessage); err != nil {
		log.Logger().Warn("BUG: Unexpected failure: Application state not changed to Rejected",
			zap.String("currentState", rejectedApplication.CurrentState()),
			zap.Error(err))
	}
	if pc.rejectedApplications == nil {
		pc.rejectedApplications = make(map[string]*objects.Application)
	}
	pc.rejectedApplications[rejectedApplication.ApplicationID] = rejectedApplication
}
