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

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/placement"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type PartitionContext struct {
	RmID string // the RM the partition belongs to
	Name string // name of the partition (logging mainly)

	// Private fields need protection
	root                   *objects.Queue                  // start of the queue hierarchy
	applications           map[string]*objects.Application // applications assigned to this partition
	completedApplications  map[string]*objects.Application // completed applications from this partition
	reservedApps           map[string]int                  // applications reserved within this partition, with reservation count
	nodes                  map[string]*objects.Node        // nodes assigned to this partition
	placementManager       *placement.AppPlacementManager  // placement manager for this partition
	partitionManager       *partitionManager               // manager for this partition
	stateMachine           *fsm.FSM                        // the state of the partition for scheduling
	stateTime              time.Time                       // last time the state was updated (needed for cleanup)
	isPreemptable          bool                            // can allocations be preempted
	rules                  *[]configs.PlacementRule        // placement rules to be loaded by the scheduler
	userGroupCache         *security.UserGroupCache        // user cache per partition
	totalPartitionResource *resources.Resource             // Total node resources
	nodeSortingPolicy      *policies.NodeSortingPolicy     // Global Node Sorting Policies
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
		reservedApps:          make(map[string]int),
		nodes:                 make(map[string]*objects.Node),
	}
	pc.partitionManager = &partitionManager{
		pc: pc,
		cc: cc,
	}
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

	// TODO Need some more cleaner interface here.
	var configuredPolicy policies.SortingPolicy
	configuredPolicy, err = policies.FromString(conf.NodeSortPolicy.Type)
	if err != nil {
		log.Logger().Debug("NodeSorting policy incorrectly set or unknown",
			zap.Error(err))
	}
	switch configuredPolicy {
	case policies.BinPackingPolicy, policies.FairnessPolicy:
		log.Logger().Info("NodeSorting policy set from config",
			zap.String("policyName", configuredPolicy.String()))
		pc.nodeSortingPolicy = policies.NewNodeSortingPolicy(conf.NodeSortPolicy.Type)
	case policies.Unknown:
		log.Logger().Info("NodeSorting policy not set using 'fair' as default")
		pc.nodeSortingPolicy = policies.NewNodeSortingPolicy("fair")
	}
	return nil
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
	// start at the root: there is only one queue
	queueConf := conf.Queues[0]
	root := pc.root
	// update the root queue
	if err := root.SetQueueConfig(queueConf); err != nil {
		return err
	}
	root.UpdateSortType()
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
			err = queue.SetQueueConfig(queueConfig)
		}
		if err != nil {
			return err
		}
		// special call to convert to a real policy from the property
		queue.UpdateSortType()
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
	queueName := app.QueueName
	pm := pc.getPlacementManager()
	if pm.IsInitialised() {
		err := pm.PlaceApplication(app)
		if err != nil {
			return fmt.Errorf("failed to place application %s: %v", appID, err)
		}
		queueName = app.QueueName
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
			if !resources.FitIn(maxQueue, placeHolder) {
				queue.RemoveApplication(app)
				return fmt.Errorf("queue %s cannot fit application %s: task group request %s larger than max queue allocation", queueName, appID, placeHolder.String())
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
			currentUUID := alloc.UUID
			node := pc.GetNode(alloc.NodeID)
			if node == nil {
				log.Logger().Warn("unknown node: not found in active node list",
					zap.String("appID", appID),
					zap.String("nodeID", alloc.NodeID))
				continue
			}
			if nodeAlloc := node.RemoveAllocation(currentUUID); nodeAlloc == nil {
				log.Logger().Warn("unknown allocation: not found on the node",
					zap.String("appID", appID),
					zap.String("allocationId", currentUUID),
					zap.String("nodeID", alloc.NodeID))
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
	delete(pc.reservedApps, appID)
	return app
}

func (pc *PartitionContext) getApplication(appID string) *objects.Application {
	pc.RLock()
	defer pc.RUnlock()

	return pc.applications[appID]
}

// Return a copy of the map of all reservations for the partition.
// This will return an empty map if there are no reservations.
// Visible for tests
func (pc *PartitionContext) getReservations() map[string]int {
	pc.RLock()
	defer pc.RUnlock()
	reserve := make(map[string]int)
	for key, num := range pc.reservedApps {
		reserve[key] = num
	}
	return reserve
}

// Get the queue from the structure based on the fully qualified name.
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
func (pc *PartitionContext) GetQueueInfos() dao.QueueDAOInfo {
	return pc.root.GetQueueInfos()
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

	return pc.nodes[nodeID]
}

// Get a copy of the nodes from the partition.
// This list does not include reserved nodes or nodes marked unschedulable
func (pc *PartitionContext) getSchedulableNodes() []*objects.Node {
	return pc.getNodes(true)
}

// Get a copy of the nodes from the partition.
// Excludes unschedulable nodes only, reserved node inclusion depends on the parameter passed in.
func (pc *PartitionContext) getNodes(excludeReserved bool) []*objects.Node {
	pc.RLock()
	defer pc.RUnlock()

	nodes := make([]*objects.Node, 0)
	for _, node := range pc.nodes {
		// filter out the nodes that are not scheduling
		if !node.IsSchedulable() || (excludeReserved && node.IsReserved()) {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes
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
				released := pc.removeNode(node.NodeID)
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
	if pc.nodes[node.NodeID] != nil {
		return fmt.Errorf("partition %s has an existing node %s, node name must be unique", pc.Name, node.NodeID)
	}
	// Node can be added to the system to allow processing of the allocations
	pc.nodes[node.NodeID] = node
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
	node := pc.nodes[nodeID]
	if node == nil {
		log.Logger().Debug("node was not found, node already removed",
			zap.String("nodeID", nodeID),
			zap.String("partitionName", pc.Name))
		return nil
	}

	// Remove node from list of tracked nodes
	delete(pc.nodes, nodeID)
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

// Remove a node from the partition. It returns all removed allocations.
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) removeNode(nodeID string) []*objects.Allocation {
	log.Logger().Info("removing node from partition",
		zap.String("partition", pc.Name),
		zap.String("nodeID", nodeID))

	node := pc.removeNodeFromList(nodeID)
	if node == nil {
		return nil
	}
	// found the node cleanup the allocations linked to the node
	released := pc.removeNodeAllocations(node)

	// unreserve all the apps that were reserved on the node
	reservedKeys, releasedAsks := node.UnReserveApps()
	// update the partition reservations based on the node clean up
	for i, appID := range reservedKeys {
		pc.unReserveCount(appID, releasedAsks[i])
	}
	return released
}

// Remove all allocations that are assigned to a node as part of the node removal. This is not part of the node object
// as updating the applications and queues is the only goal. Applications and queues are not accessible from the node.
// The removed allocations are returned.
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) removeNodeAllocations(node *objects.Node) []*objects.Allocation {
	released := make([]*objects.Allocation, 0)
	// walk over all allocations still registered for this node
	for _, alloc := range node.GetAllAllocations() {
		allocID := alloc.UUID
		// since we are not locking the node and or application we could have had an update while processing
		// note that we do not return the allocation if the app or allocation is not found and assume that it
		// was already removed
		app := pc.getApplication(alloc.ApplicationID)
		if app == nil {
			log.Logger().Info("app is not found, skipping while removing the node",
				zap.String("appID", alloc.ApplicationID),
				zap.String("nodeID", node.NodeID))
			continue
		}
		// check allocations on the app
		if app.RemoveAllocation(allocID) == nil {
			log.Logger().Info("allocation is not found, skipping while removing the node",
				zap.String("allocationId", allocID),
				zap.String("appID", app.ApplicationID),
				zap.String("nodeID", node.NodeID))
			continue
		}
		if err := app.GetQueue().DecAllocatedResource(alloc.AllocatedResource); err != nil {
			log.Logger().Warn("failed to release resources from queue",
				zap.String("appID", alloc.ApplicationID),
				zap.Error(err))
		}

		// the allocation is removed so add it to the list that we return
		released = append(released, alloc)
		log.Logger().Info("allocation removed from node",
			zap.String("nodeID", node.NodeID),
			zap.String("allocationId", allocID))
	}
	// track the number of allocations
	pc.updateAllocationCount(-len(released))
	return released
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
	alloc := pc.root.TryAllocate(pc.GetNodeIterator)
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
			zap.String("appID", alloc.ApplicationID),
			zap.String("allocationKey", alloc.AllocationKey),
			zap.String("UUID", alloc.UUID),
			zap.String("placeholder released UUID", alloc.Releases[0].UUID))
		// pass the release back to the RM via the cluster context
		return alloc
	}
	return nil
}

// Process the allocation and make the left over changes in the partition.
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) allocate(alloc *objects.Allocation) *objects.Allocation {
	// find the app make sure it still exists
	appID := alloc.ApplicationID
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
	if alloc.ReservedNodeID == "" {
		nodeID = alloc.NodeID
	} else {
		nodeID = alloc.ReservedNodeID
		log.Logger().Debug("Reservation allocated on different node",
			zap.String("current node", alloc.NodeID),
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
	if alloc.Result == objects.Reserved {
		pc.reserve(app, node, alloc.Ask)
		return nil
	}
	// unreserve
	if alloc.Result == objects.Unreserved || alloc.Result == objects.AllocatedReserved {
		pc.unReserve(app, node, alloc.Ask)
		if alloc.Result == objects.Unreserved {
			return nil
		}
		// remove the link to the reserved node
		alloc.ReservedNodeID = ""
	}

	// track the number of allocations
	pc.updateAllocationCount(1)

	log.Logger().Info("scheduler allocation processed",
		zap.String("appID", alloc.ApplicationID),
		zap.String("allocationKey", alloc.AllocationKey),
		zap.String("UUID", alloc.UUID),
		zap.String("allocatedResource", alloc.AllocatedResource.String()),
		zap.Bool("placeholder", alloc.IsPlaceholder()),
		zap.String("targetNode", alloc.NodeID))
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
	// increase the number of reservations for this app
	pc.reservedApps[appID]++

	log.Logger().Info("allocation ask is reserved",
		zap.String("appID", appID),
		zap.String("queue", app.QueueName),
		zap.String("allocationKey", ask.AllocationKey),
		zap.String("node", node.NodeID))
}

// Process the unreservation in the scheduler
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) unReserve(app *objects.Application, node *objects.Node, ask *objects.AllocationAsk) {
	appID := app.ApplicationID
	if pc.reservedApps[appID] == 0 {
		log.Logger().Info("Application is not reserved in partition",
			zap.String("appID", appID))
		return
	}
	// all ok, remove the reservation of the app, this will also unReserve the node
	var err error
	var num int
	if num, err = app.UnReserve(node, ask); err != nil {
		log.Logger().Info("Failed to unreserve, error during allocate on the app",
			zap.Error(err))
		return
	}
	// remove the reservation of the queue
	app.GetQueue().UnReserve(appID, num)
	// make sure we cannot go below 0
	pc.unReserveCount(appID, num)

	log.Logger().Info("allocation ask is unreserved",
		zap.String("appID", appID),
		zap.String("queue", app.QueueName),
		zap.String("allocationKey", ask.AllocationKey),
		zap.String("node", node.NodeID),
		zap.Int("reservationsRemoved", num))
}

// Get the iterator for the sorted nodes list from the partition.
// Sorting should use a copy of the node list not the main list.
func (pc *PartitionContext) getNodeIteratorForPolicy(nodes []*objects.Node) interfaces.NodeIterator {
	configuredPolicy := pc.GetNodeSortingPolicy()
	if configuredPolicy == policies.Unknown {
		return nil
	}
	// Sort Nodes based on the policy configured.
	objects.SortNodes(nodes, configuredPolicy)
	return newDefaultNodeIterator(nodes)
}

// Create a node iterator for the schedulable nodes based on the policy set for this partition.
// The iterator is nil if there are no schedulable nodes available.
func (pc *PartitionContext) GetNodeIterator() interfaces.NodeIterator {
	if nodeList := pc.getSchedulableNodes(); len(nodeList) != 0 {
		return pc.getNodeIteratorForPolicy(nodeList)
	}
	return nil
}

// Update the reservation counter for the app
// Locked version of unReserveCountInternal
func (pc *PartitionContext) unReserveCount(appID string, asks int) {
	pc.Lock()
	defer pc.Unlock()
	if num, found := pc.reservedApps[appID]; found {
		// decrease the number of reservations for this app and cleanup
		// do not go negative, if it would happen cleanup
		if asks >= num {
			delete(pc.reservedApps, appID)
		} else {
			pc.reservedApps[appID] -= asks
		}
	}
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
	return len(pc.nodes)
}

func (pc *PartitionContext) GetApplications() []*objects.Application {
	pc.RLock()
	defer pc.RUnlock()
	var appList []*objects.Application
	for _, app := range pc.applications {
		appList = append(appList, app)
	}
	return appList
}

func (pc *PartitionContext) GetCompletedApplications() []*objects.Application {
	pc.RLock()
	defer pc.RUnlock()
	var appList []*objects.Application
	for _, app := range pc.completedApplications {
		appList = append(appList, app)
	}
	return appList
}

func (pc *PartitionContext) GetAppsByState(state string) []*objects.Application {
	pc.RLock()
	defer pc.RUnlock()
	var appList []*objects.Application
	if state == objects.Completed.String() {
		for _, app := range pc.completedApplications {
			if app.CurrentState() == state {
				appList = append(appList, app)
			}
		}
		return appList
	}

	for _, app := range pc.applications {
		if app.CurrentState() == state {
			appList = append(appList, app)
		}
	}
	return appList
}

func (pc *PartitionContext) GetAppsInTerminatedState() []*objects.Application {
	pc.RLock()
	defer pc.RUnlock()
	appList := pc.GetAppsByState(objects.Completed.String())
	appList = append(appList, pc.GetAppsByState(objects.Failed.String())...)
	return appList
}

func (pc *PartitionContext) GetNodes() []*objects.Node {
	pc.RLock()
	defer pc.RUnlock()
	var nodeList []*objects.Node
	for _, node := range pc.nodes {
		nodeList = append(nodeList, node)
	}
	return nodeList
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
		return fmt.Errorf("partition %s is stopped cannot add new allocation %s", pc.Name, alloc.AllocationKey)
	}

	// We must not generate a new UUID for it, we directly use the UUID reported by shim
	// to track this allocation, a missing UUID is a broken allocation
	if alloc.UUID == "" {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("failing to restore allocation %s for application %s: missing UUID",
			alloc.AllocationKey, alloc.ApplicationID)
	}

	log.Logger().Info("adding recovered allocation",
		zap.String("partitionName", pc.Name),
		zap.String("appID", alloc.ApplicationID),
		zap.String("allocKey", alloc.AllocationKey),
		zap.String("UUID", alloc.UUID))

	// Check if allocation violates any resource restriction, or allocate on a
	// non-existent application or nodes.
	node := pc.GetNode(alloc.NodeID)
	if node == nil {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("failed to find node %s", alloc.NodeID)
	}
	// check the node status again
	if !node.IsSchedulable() {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("node %s is not in schedulable state", node.NodeID)
	}

	app := pc.getApplication(alloc.ApplicationID)
	if app == nil {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("failed to find application %s", alloc.ApplicationID)
	}
	queue := app.GetQueue()

	// Do not check if the new allocation goes beyond the queue's max resource (recursive).
	// still handle a returned error but they should never happen.
	if err := queue.IncAllocatedResource(alloc.AllocatedResource, true); err != nil {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("cannot allocate resource from application %s: %v ",
			alloc.ApplicationID, err)
	}

	node.AddAllocation(alloc)
	app.RecoverAllocationAsk(alloc.Ask)
	app.AddAllocation(alloc)

	// track the number of allocations
	pc.updateAllocationCount(1)

	log.Logger().Info("recovered allocation",
		zap.String("partitionName", pc.Name),
		zap.String("appID", alloc.ApplicationID),
		zap.String("allocKey", alloc.AllocationKey),
		zap.String("allocationUid", alloc.UUID),
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
	nodesCopy := pc.getNodes(false)
	mapResult := make(map[string][]int)
	for _, node := range nodesCopy {
		for name, total := range node.GetCapacity().Resources {
			if float64(total) > 0 {
				resourceAllocated := float64(node.GetAllocatedResource().Resources[name])
				v := resourceAllocated / float64(total)
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

// Remove the allocation(s) from the app and nodes
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) removeAllocation(release *si.AllocationRelease) ([]*objects.Allocation, *objects.Allocation) {
	if release == nil {
		return nil, nil
	}
	appID := release.ApplicationID
	uuid := release.UUID
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
			if alloc := app.RemoveAllocation(uuid); alloc != nil {
				released = append(released, alloc)
			}
		}
	}

	// for each allocations to release, update node.
	total := resources.NewResource()
	for _, alloc := range released {
		node := pc.GetNode(alloc.NodeID)
		if node == nil {
			log.Logger().Info("node not found while releasing allocation",
				zap.String("appID", appID),
				zap.String("allocationId", alloc.UUID),
				zap.String("nodeID", alloc.NodeID))
			continue
		}
		if release.TerminationType == si.TerminationType_PLACEHOLDER_REPLACED {
			// replacements could be on a different node but the queue should not change
			confirmed = alloc.Releases[0]
			if confirmed.NodeID == alloc.NodeID {
				// this is the real swap on the node
				node.ReplaceAllocation(alloc.UUID, confirmed)
			} else {
				// we have already added the allocation to the new node in this case just remove
				// the old one, never update the queue
				node.RemoveAllocation(alloc.UUID)
			}
			log.Logger().Info("replacing placeholder allocation on node",
				zap.String("nodeID", alloc.NodeID),
				zap.String("allocationId", alloc.UUID),
				zap.String("allocation nodeID", confirmed.NodeID))
		} else if node.RemoveAllocation(alloc.UUID) != nil {
			// all non replacement removes, update the queue
			total.AddTo(alloc.AllocatedResource)
			log.Logger().Info("removing allocation from node",
				zap.String("nodeID", alloc.NodeID),
				zap.String("allocationId", alloc.UUID))
		}
	}
	if resources.StrictlyGreaterThanZero(total) {
		queue := app.GetQueue()
		if err := queue.DecAllocatedResource(total); err != nil {
			log.Logger().Warn("failed to release resources from queue",
				zap.String("appID", appID),
				zap.String("allocationId", uuid),
				zap.Error(err))
		}
	}
	// if confirmed is set we can assume there will just be one alloc in the released
	// that allocation was already released by the shim, so clean up released
	if confirmed != nil {
		released = nil
	}
	// track the number of allocations, when we replace the result is no change
	pc.updateAllocationCount(-len(released))
	return released, confirmed
}

// Remove the allocation Ask from the specified application
// NOTE: this is a lock free call. It must NOT be called holding the PartitionContext lock.
func (pc *PartitionContext) removeAllocationAsk(release *si.AllocationAskRelease) {
	if release == nil {
		return
	}
	appID := release.ApplicationID
	allocKey := release.Allocationkey
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
	reservedAsks := app.RemoveAllocationAsk(allocKey)
	// update the partition if the asks were reserved (clean up)
	if reservedAsks != 0 {
		pc.unReserveCount(appID, reservedAsks)
	}
}

// Add the allocation Ask to the specified application
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
	return app.AddAllocationAsk(objects.NewAllocationAsk(siAsk))
}

func (pc *PartitionContext) cleanupExpiredApps() {
	for _, app := range pc.GetAppsByState(objects.Expired.String()) {
		pc.Lock()
		delete(pc.applications, app.ApplicationID)
		pc.Unlock()
	}
}

func (pc *PartitionContext) GetCurrentState() string {
	return pc.stateMachine.Current()
}

func (pc *PartitionContext) GetStateTime() time.Time {
	pc.RLock()
	defer pc.RUnlock()
	return pc.stateTime
}

func (pc *PartitionContext) GetNodeSortingPolicy() policies.SortingPolicy {
	pc.RLock()
	defer pc.RUnlock()
	return pc.nodeSortingPolicy.PolicyType
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
