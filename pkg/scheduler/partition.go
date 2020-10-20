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
	"strings"
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/placement"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type PartitionContext struct {
	RmID string // the RM the partition belongs to
	Name string // name of the partition (logging mainly)

	// Private fields need protection
	root                   *objects.Queue                  // start of the scheduling queue hierarchy
	applications           map[string]*objects.Application // applications assigned to this partition
	reservedApps           map[string]int                  // applications reserved within this partition, with reservation count
	nodes                  map[string]*objects.Node        // nodes assigned to this partition
	allocations            map[string]*objects.Allocation  // allocations
	placementManager       *placement.AppPlacementManager  // placement manager for this partition
	partitionManager       *partitionManager               // manager for this partition
	stateMachine           *fsm.FSM                        // the state of the queue for scheduling
	stateTime              time.Time                       // last time the state was updated (needed for cleanup)
	isPreemptable          bool                            // can allocations be preempted
	rules                  *[]configs.PlacementRule        // placement rules to be loaded by the scheduler
	userGroupCache         *security.UserGroupCache        // user cache per partition
	totalPartitionResource *resources.Resource             // Total node resources
	nodeSortingPolicy      *policies.NodeSortingPolicy     // Global Node Sorting Policies

	sync.RWMutex
}

func newPartitionContext(conf configs.PartitionConfig, rmID string, csc *ClusterSchedulingContext) (*PartitionContext, error) {
	psc := &PartitionContext{
		Name:         conf.Name,
		RmID:         rmID,
		stateMachine: objects.NewObjectState(),
		stateTime:    time.Now(),
		applications: make(map[string]*objects.Application),
		reservedApps: make(map[string]int),
		nodes:        make(map[string]*objects.Node),
		allocations:  make(map[string]*objects.Allocation),
	}
	psc.partitionManager = &partitionManager{
		psc: psc,
		csc: csc,
	}
	if err := psc.initialPartitionFromConfig(conf); err != nil {
		return nil, err
	}
	return psc, nil
}

// Initialise the partition
func (psc *PartitionContext) initialPartitionFromConfig(conf configs.PartitionConfig) error {
	// Setup the queue structure: root first it should be the only queue at this level
	// Add the rest of the queue structure recursively
	queueConf := conf.Queues[0]
	var err error
	if psc.root, err = objects.NewConfiguredQueue(queueConf, nil); err != nil {
		return err
	}
	// recursively add the queues to the root
	if err = psc.addQueue(queueConf.Queues, psc.root); err != nil {
		return err
	}
	log.Logger().Info("root queue added",
		zap.String("partitionName", psc.Name),
		zap.String("rmID", psc.RmID))

	// set preemption needed flag
	psc.isPreemptable = conf.Preemption.Enabled

	psc.rules = &conf.PlacementRules
	psc.placementManager = placement.NewPlacementManager(*psc.rules, psc.GetQueue)
	// get the user group cache for the partition
	// TODO get the resolver from the config
	psc.userGroupCache = security.GetUserGroupCache("")

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
		psc.nodeSortingPolicy = policies.NewNodeSortingPolicy(conf.NodeSortPolicy.Type)
	case policies.Unknown:
		log.Logger().Info("NodeSorting policy not set using 'fair' as default")
		psc.nodeSortingPolicy = policies.NewNodeSortingPolicy("fair")
	}
	return nil
}

func (psc *PartitionContext) updatePartitionDetails(conf configs.PartitionConfig) error {
	psc.Lock()
	defer psc.Unlock()

	if psc.placementManager.IsInitialised() {
		log.Logger().Info("Updating placement manager rules on config reload")
		err := psc.placementManager.UpdateRules(conf.PlacementRules)
		if err != nil {
			log.Logger().Info("New placement rules not activated, config reload failed", zap.Error(err))
			return err
		}
		psc.rules = &conf.PlacementRules
	} else {
		log.Logger().Info("Creating new placement manager on config reload")
		psc.rules = &conf.PlacementRules
		psc.placementManager = placement.NewPlacementManager(*psc.rules, psc.GetQueue)
	}
	// start at the root: there is only one queue
	queueConf := conf.Queues[0]
	root := psc.root
	// update the root queue
	if err := root.SetQueueConfig(queueConf); err != nil {
		return err
	}
	root.UpdateSortType()
	// update the rest of the queues recursively
	return psc.updateQueues(queueConf.Queues, root)
}

// Process the config structure and create a queue info tree for this partition
func (psc *PartitionContext) addQueue(conf []configs.QueueConfig, parent *objects.Queue) error {
	// create the queue at this level
	for _, queueConf := range conf {
		thisQueue, err := objects.NewConfiguredQueue(queueConf, parent)
		if err != nil {
			return err
		}
		// recursive create the queues below
		if len(queueConf.Queues) > 0 {
			err = psc.addQueue(queueConf.Queues, thisQueue)
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
func (psc *PartitionContext) updateQueues(config []configs.QueueConfig, parent *objects.Queue) error {
	// get the name of the passed in queue
	parentPath := parent.QueuePath + configs.DOT
	// keep track of which children we have updated
	visited := map[string]bool{}
	// walk over the queues recursively
	for _, queueConfig := range config {
		pathName := parentPath + queueConfig.Name
		queue := psc.getQueue(pathName)
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
		if err = psc.updateQueues(queueConfig.Queues, queue); err != nil {
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
func (psc *PartitionContext) markPartitionForRemoval() {
	if err := psc.handlePartitionEvent(objects.Remove); err != nil {
		log.Logger().Error("failed to mark partition for deletion",
			zap.String("partitionName", psc.Name),
			zap.Error(err))
	}
}

// Get the state of the partition.
// No new nodes and applications will be accepted if stopped or being removed.
func (psc *PartitionContext) isDraining() bool {
	return psc.stateMachine.Current() == objects.Draining.String()
}

func (psc *PartitionContext) isRunning() bool {
	return psc.stateMachine.Current() == objects.Active.String()
}

func (psc *PartitionContext) isStopped() bool {
	return psc.stateMachine.Current() == objects.Stopped.String()
}

// Handle the state event for the partition.
// The state machine handles the locking.
func (psc *PartitionContext) handlePartitionEvent(event objects.SchedulingObjectEvent) error {
	err := psc.stateMachine.Event(event.String(), psc.Name)
	if err == nil {
		psc.stateTime = time.Now()
		return nil
	}
	// handle the same state transition not nil error (limit of fsm).
	if err.Error() == "no transition" {
		return nil
	}
	return err
}

// Add a new application to the scheduling partition.
func (psc *PartitionContext) addApplication(schedulingApp *objects.Application) error {
	psc.Lock()
	defer psc.Unlock()

	if psc.isDraining() || psc.isStopped() {
		return fmt.Errorf("partition %s is stopped cannot add a new application %s", psc.Name, schedulingApp.ApplicationID)
	}

	// Add to applications
	appID := schedulingApp.ApplicationID
	if psc.applications[appID] != nil {
		return fmt.Errorf("adding application %s to partition %s, but application already existed", appID, psc.Name)
	}

	// Put app under the scheduling queue
	var queueName string
	if psc.placementManager.IsInitialised() {
		err := psc.placementManager.PlaceApplication(schedulingApp)
		if err != nil {
			return fmt.Errorf("failed to place application %s: %v", appID, err)
		}
		queueName = schedulingApp.QueueName
		if queueName == "" {
			return fmt.Errorf("application rejected by placement rules: %s", appID)
		}
	}
	// we have a queue name either from placement or direct, get the queue
	schedulingQueue := psc.getQueue(queueName)
	if schedulingQueue == nil {
		// queue must exist if not using placement rules
		if !psc.placementManager.IsInitialised() {
			return fmt.Errorf("application '%s' rejected, cannot create queue '%s' without placement rules", appID, queueName)
		}
		// with placement rules the hierarchy might not exist so try and create it
		var err error
		schedulingQueue, err = psc.createQueue(queueName, schedulingApp.GetUser())
		if err != nil {
			return fmt.Errorf("failed to create rule based queue %s for application %s", queueName, appID)
		}
	}
	// check the queue: is a leaf queue with submit access
	if !schedulingQueue.IsLeafQueue() || !schedulingQueue.CheckSubmitAccess(schedulingApp.GetUser()) {
		return fmt.Errorf("failed to find queue %s for application %s", queueName, appID)
	}

	// all is OK update the app and partition
	schedulingApp.SetQueue(schedulingQueue)
	schedulingQueue.AddSchedulingApplication(schedulingApp)
	psc.applications[appID] = schedulingApp

	return nil
}

// Remove the application from the scheduling partition.
// This does not fail and handles missing /app/queue/node/allocations internally
func (psc *PartitionContext) removeApplication(appID string) []*objects.Allocation {
	psc.Lock()
	defer psc.Unlock()

	// Remove from applications map
	if psc.applications[appID] == nil {
		return nil
	}
	schedulingApp := psc.applications[appID]
	// remove from partition then cleanup underlying objects
	delete(psc.applications, appID)
	delete(psc.reservedApps, appID)

	queueName := schedulingApp.QueueName
	// Remove all asks and thus all reservations and pending resources (queue included)
	_ = schedulingApp.RemoveAllocationAsk("")
	// Remove all allocations (queue and nodes included)
	allocations := schedulingApp.RemoveAllAllocations()
	// Remove all allocations from nodes/queue
	if len(allocations) != 0 {
		for _, alloc := range allocations {
			currentUUID := alloc.UUID
			// Remove from partition
			if globalAlloc := psc.allocations[currentUUID]; globalAlloc == nil {
				log.Logger().Warn("unknown allocation: not found on the partition",
					zap.String("appID", appID),
					zap.String("allocationId", currentUUID))
			} else {
				delete(psc.allocations, currentUUID)
			}

			// Remove from node: even if not found on the partition to keep things clean
			node := psc.nodes[alloc.NodeID]
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

	// Remove app from queue
	if schedulingQueue := psc.getQueue(queueName); schedulingQueue != nil {
		schedulingQueue.RemoveSchedulingApplication(schedulingApp)
	}

	log.Logger().Debug("application removed from the scheduler",
		zap.String("queue", queueName),
		zap.String("applicationID", appID))

	return allocations
}

func (psc *PartitionContext) getApplication(appID string) *objects.Application {
	psc.RLock()
	defer psc.RUnlock()

	return psc.applications[appID]
}

// Return a copy of the map of all reservations for the partition.
// This will return an empty map if there are no reservations.
// Visible for tests
func (psc *PartitionContext) getReservations() map[string]int {
	psc.RLock()
	defer psc.RUnlock()
	reserve := make(map[string]int)
	for key, num := range psc.reservedApps {
		reserve[key] = num
	}
	return reserve
}

// Get the queue from the structure based on the fully qualified name.
// Wrapper around the unlocked version getQueue()
// Visible by tests
func (psc *PartitionContext) GetQueue(name string) *objects.Queue {
	psc.RLock()
	defer psc.RUnlock()
	return psc.getQueue(name)
}

// Get the queue from the structure based on the fully qualified name.
// The name is not syntax checked and must be valid.
// Returns nil if the queue is not found otherwise the queue object.
//
// NOTE: this is a lock free call. It should only be called holding the PartitionContext lock.
func (psc *PartitionContext) getQueue(name string) *objects.Queue {
	// start at the root
	queue := psc.root
	part := strings.Split(strings.ToLower(name), configs.DOT)
	// no input
	if len(part) == 0 || part[0] != "root" {
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
func (psc *PartitionContext) GetQueueInfos() dao.QueueDAOInfo {
	return psc.root.GetQueueInfos()
}

// Create a scheduling queue with full hierarchy. This is called when a new queue is created from a placement rule.
// The final leaf queue does not exist otherwise we would not get here.
// This means that at least 1 queue (a leaf queue) will be created
// NOTE: this is a lock free call. It should only be called holding the PartitionContext lock.
func (psc *PartitionContext) createQueue(name string, user security.UserGroup) (*objects.Queue, error) {
	// find the scheduling furthest down the hierarchy that exists
	var toCreate []string
	current := name
	queue := psc.getQueue(current)
	log.Logger().Debug("Checking queue creation")
	for queue == nil {
		toCreate = append(toCreate, current[strings.LastIndex(current, configs.DOT)+1:])
		current = current[0:strings.LastIndex(current, configs.DOT)]
		queue = psc.getQueue(current)
	}
	// Check the ACL before we really create
	// The existing parent scheduling queue is the lowest we need to look at
	if !queue.CheckSubmitAccess(user) {
		return nil, fmt.Errorf("submit access to queue %s denied during create of: %s", current, name)
	}
	if queue.IsLeafQueue() {
		return nil, fmt.Errorf("creation of queue %s failed parent is already a leaf: %s", name, current)
	}
	log.Logger().Debug("Creating scheduling queue(s)",
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

// Get a scheduling node from the partition by nodeID.
func (psc *PartitionContext) getNode(nodeID string) *objects.Node {
	psc.RLock()
	defer psc.RUnlock()

	return psc.nodes[nodeID]
}

// Get a copy of the scheduling nodes from the partition.
// This list does not include reserved nodes or nodes marked unschedulable
func (psc *PartitionContext) getSchedulableNodes() []*objects.Node {
	return psc.getNodes(true)
}

// Get a copy of the scheduling nodes from the partition.
// Excludes unschedulable nodes only, reserved node inclusion depends on the parameter passed in.
func (psc *PartitionContext) getNodes(excludeReserved bool) []*objects.Node {
	psc.RLock()
	defer psc.RUnlock()

	schedulingNodes := make([]*objects.Node, 0)
	for _, node := range psc.nodes {
		// filter out the nodes that are not scheduling
		if !node.IsSchedulable() || (excludeReserved && node.IsReserved()) {
			continue
		}
		schedulingNodes = append(schedulingNodes, node)
	}
	return schedulingNodes
}

// Add the node to the partition and process the allocations that are reported by the node.
func (psc *PartitionContext) addNode(node *objects.Node, existingAllocations []*objects.Allocation) error {
	if node == nil {
		return nil
	}
	psc.Lock()
	defer psc.Unlock()

	if psc.isDraining() || psc.isStopped() {
		return fmt.Errorf("partition %s is stopped cannot add a new node %s", psc.Name, node.NodeID)
	}

	if psc.nodes[node.NodeID] != nil {
		return fmt.Errorf("partition %s has an existing node %s, node name must be unique", psc.Name, node.NodeID)
	}

	log.Logger().Debug("adding node to partition",
		zap.String("nodeID", node.NodeID),
		zap.String("partition", psc.Name))

	// update the resources available in the cluster
	psc.totalPartitionResource.AddTo(node.GetCapacity())
	psc.root.SetMaxResource(psc.totalPartitionResource)

	// Node is added to the system to allow processing of the allocations
	psc.nodes[node.NodeID] = node
	// Add allocations that exist on the node when added
	if len(existingAllocations) > 0 {
		log.Logger().Info("add existing allocations",
			zap.String("nodeID", node.NodeID),
			zap.Int("existingAllocations", len(existingAllocations)))
		for current, alloc := range existingAllocations {
			if err := psc.addAllocationInternal(alloc); err != nil {
				released := psc.removeNode(node.NodeID)
				log.Logger().Info("failed to add existing allocations",
					zap.String("nodeID", node.NodeID),
					zap.Int("existingAllocations", len(existingAllocations)),
					zap.Int("releasedAllocations", len(released)),
					zap.Int("processingAlloc", current))
				metrics.GetSchedulerMetrics().IncFailedNodes()
				return err
			}
		}
	}

	// Node is added update the metrics
	metrics.GetSchedulerMetrics().IncActiveNodes()
	log.Logger().Info("added node to partition",
		zap.String("nodeID", node.NodeID),
		zap.String("partition", psc.Name))

	return nil
}

// Remove a node from the partition. It returns all removed allocations.
func (psc *PartitionContext) removeNode(nodeID string) []*objects.Allocation {
	psc.Lock()
	defer psc.Unlock()

	log.Logger().Info("remove node from partition",
		zap.String("nodeID", nodeID),
		zap.String("partition", psc.Name))

	node := psc.nodes[nodeID]
	if node == nil {
		log.Logger().Debug("node was not found",
			zap.String("nodeID", nodeID),
			zap.String("partitionName", psc.Name))
		return nil
	}

	// Remove node from list of tracked nodes
	delete(psc.nodes, nodeID)
	metrics.GetSchedulerMetrics().DecActiveNodes()

	// found the node cleanup the node and all linked data
	released := psc.removeNodeAllocations(node)
	psc.totalPartitionResource.SubFrom(node.GetCapacity())
	psc.root.SetMaxResource(psc.totalPartitionResource)

	// unreserve all the apps that were reserved on the node
	reservedKeys, releasedAsks := node.UnReserveApps()
	// update the partition reservations based on the node clean up
	for i, appID := range reservedKeys {
		psc.unReserveCount(appID, releasedAsks[i])
	}

	log.Logger().Info("node removed",
		zap.String("partitionName", psc.Name),
		zap.String("nodeID", node.NodeID))
	return released
}

// Remove all allocations that are assigned to a node as part of the node removal. This is not part of the node object
// as updating the applications and queues is the only goal. Applications and queues are not accessible from the node.
// The removed allocations are returned.
func (psc *PartitionContext) removeNodeAllocations(node *objects.Node) []*objects.Allocation {
	released := make([]*objects.Allocation, 0)
	// walk over all allocations still registered for this node
	for _, alloc := range node.GetAllAllocations() {
		allocID := alloc.UUID
		// since we are not locking the node and or application we could have had an update while processing
		// note that we do not return the allocation if the app or allocation is not found and assume that it
		// was already removed
		app := psc.applications[alloc.ApplicationID]
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
		log.Logger().Info("allocation removed",
			zap.String("allocationId", allocID),
			zap.String("nodeID", node.NodeID))
	}
	return released
}

func (psc *PartitionContext) calculateOutstandingRequests() []*objects.AllocationAsk {
	if !resources.StrictlyGreaterThanZero(psc.root.GetPendingResource()) {
		return nil
	}
	outstanding := make([]*objects.AllocationAsk, 0)
	psc.root.GetQueueOutstandingRequests(&outstanding)
	return outstanding
}

// Try regular allocation for the partition
// Lock free call this all locks are taken when needed in called functions
func (psc *PartitionContext) tryAllocate() *objects.Allocation {
	if !resources.StrictlyGreaterThanZero(psc.root.GetPendingResource()) {
		// nothing to do just return
		return nil
	}
	// try allocating from the root down
	alloc := psc.root.TryAllocate(psc.GetNodeIterator)
	if alloc != nil {
		return psc.allocate(alloc)
	}
	return nil
}

// Try process reservations for the partition
// Lock free call this all locks are taken when needed in called functions
func (psc *PartitionContext) tryReservedAllocate() *objects.Allocation {
	if !resources.StrictlyGreaterThanZero(psc.root.GetPendingResource()) {
		// nothing to do just return
		return nil
	}
	// try allocating from the root down
	alloc := psc.root.TryReservedAllocate(psc.GetNodeIterator)
	if alloc != nil {
		return psc.allocate(alloc)
	}
	return nil
}

// Process the allocation and make the left over changes in the partition.
func (psc *PartitionContext) allocate(alloc *objects.Allocation) *objects.Allocation {
	psc.Lock()
	defer psc.Unlock()
	// partition is locked nothing can change from now on
	// find the app make sure it still exists
	appID := alloc.ApplicationID
	app := psc.applications[appID]
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
	node := psc.nodes[nodeID]
	if node == nil {
		log.Logger().Info("Node was removed while allocating",
			zap.String("nodeID", nodeID),
			zap.String("appID", appID))
		return nil
	}
	// reservation
	if alloc.Result == objects.Reserved {
		psc.reserve(app, node, alloc.SchedulingAsk)
		return nil
	}
	// unreserve
	if alloc.Result == objects.Unreserved || alloc.Result == objects.AllocatedReserved {
		psc.unReserve(app, node, alloc.SchedulingAsk)
		if alloc.Result == objects.Unreserved {
			return nil
		}
		// remove the link to the reserved node
		alloc.ReservedNodeID = ""
	}

	// Safeguard against the unlikely case that we have clashes.
	// A clash points to entropy issues on the node.
	if _, found := psc.allocations[alloc.UUID]; found {
		log.Logger().Warn("UUID clash, random generator might be lacking entropy")
		for {
			allocationUUID := common.GetNewUUID()
			if psc.allocations[allocationUUID] == nil {
				alloc.UUID = allocationUUID
			}
		}
	}
	psc.allocations[alloc.UUID] = alloc
	log.Logger().Info("scheduler allocation processed",
		zap.String("appID", alloc.ApplicationID),
		zap.String("queue", alloc.QueueName),
		zap.String("allocationKey", alloc.AllocationKey),
		zap.String("allocatedResource", alloc.AllocatedResource.String()),
		zap.String("targetNode", alloc.NodeID))
	// pass the allocation back to the RM via the cluster context
	return alloc
}

// Process the reservation in the scheduler
// Lock free call this must be called holding the context lock
func (psc *PartitionContext) reserve(app *objects.Application, node *objects.Node, ask *objects.AllocationAsk) {
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
	psc.reservedApps[appID]++

	log.Logger().Info("allocation ask is reserved",
		zap.String("appID", ask.ApplicationID),
		zap.String("queue", ask.QueueName),
		zap.String("allocationKey", ask.AllocationKey),
		zap.String("node", node.NodeID))
}

// Process the unreservation in the scheduler
// Lock free call this must be called holding the context lock
func (psc *PartitionContext) unReserve(app *objects.Application, node *objects.Node, ask *objects.AllocationAsk) {
	appID := app.ApplicationID
	if psc.reservedApps[appID] == 0 {
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
	psc.unReserveCount(appID, num)

	log.Logger().Info("allocation ask is unreserved",
		zap.String("appID", ask.ApplicationID),
		zap.String("queue", ask.QueueName),
		zap.String("allocationKey", ask.AllocationKey),
		zap.String("node", node.NodeID),
		zap.Int("reservationsRemoved", num))
}

// Get the iterator for the sorted nodes list from the partition.
// Sorting should use a copy of the node list not the main list.
func (psc *PartitionContext) getNodeIteratorForPolicy(nodes []*objects.Node) interfaces.NodeIterator {
	psc.RLock()
	configuredPolicy := psc.nodeSortingPolicy.PolicyType
	psc.RUnlock()
	if configuredPolicy == policies.Unknown {
		return nil
	}
	// Sort Nodes based on the policy configured.
	objects.SortNodes(nodes, configuredPolicy)
	return newDefaultNodeIterator(nodes)
}

// Create a node iterator for the schedulable nodes based on the policy set for this partition.
// The iterator is nil if there are no schedulable nodes available.
func (psc *PartitionContext) GetNodeIterator() interfaces.NodeIterator {
	if nodeList := psc.getSchedulableNodes(); len(nodeList) != 0 {
		return psc.getNodeIteratorForPolicy(nodeList)
	}
	return nil
}

// Update the reservation counter for the app
// Lock free call this must be called holding the context lock
func (psc *PartitionContext) unReserveCount(appID string, asks int) {
	if num, found := psc.reservedApps[appID]; found {
		// decrease the number of reservations for this app and cleanup
		if num == asks {
			delete(psc.reservedApps, appID)
		} else {
			psc.reservedApps[appID] -= asks
		}
	}
}

func (psc *PartitionContext) GetTotalPartitionResource() *resources.Resource {
	psc.RLock()
	defer psc.RUnlock()

	return psc.totalPartitionResource
}

func (psc *PartitionContext) GetAllocatedResource() *resources.Resource {
	psc.RLock()
	defer psc.RUnlock()

	return psc.root.GetAllocatedResource()
}

func (psc *PartitionContext) GetTotalApplicationCount() int {
	psc.RLock()
	defer psc.RUnlock()
	return len(psc.applications)
}

func (psc *PartitionContext) GetTotalAllocationCount() int {
	psc.RLock()
	defer psc.RUnlock()
	return len(psc.allocations)
}

func (psc *PartitionContext) GetTotalNodeCount() int {
	psc.RLock()
	defer psc.RUnlock()
	return len(psc.nodes)
}

func (psc *PartitionContext) GetApplications() []*objects.Application {
	psc.RLock()
	defer psc.RUnlock()
	var appList []*objects.Application
	for _, app := range psc.applications {
		appList = append(appList, app)
	}
	return appList
}

func (psc *PartitionContext) GetNodes() []*objects.Node {
	psc.RLock()
	defer psc.RUnlock()
	var nodeList []*objects.Node
	for _, node := range psc.nodes {
		nodeList = append(nodeList, node)
	}
	return nodeList
}

// Add an allocation to the partition/node/application/queue during node registration.
// Queue max allocation is not checked as the allocation is part of a new node addition.
//
// NOTE: this is a lock free call. It should only be called holding the Partition lock.
// If access outside is needed a locked version must used, see addAllocation
func (psc *PartitionContext) addAllocationInternal(alloc *objects.Allocation) error {
	if psc.isStopped() {
		return fmt.Errorf("partition %s is stopped cannot add new allocation %s", psc.Name, alloc.AllocationKey)
	}

	// if the allocation is node reported (aka recovery an existing allocation),
	// we must not generate an UUID for it, we directly use the UUID reported by shim
	// to track this allocation, a missing UUID is a broken allocation
	if alloc.UUID == "" {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("failing to restore allocation %s for application %s: missing UUID",
			alloc.AllocationKey, alloc.ApplicationID)
	}

	log.Logger().Debug("adding recovered allocation",
		zap.String("partitionName", psc.Name),
		zap.String("appID", alloc.ApplicationID),
		zap.String("allocKey", alloc.AllocationKey),
		zap.String("UUID", alloc.UUID))

	// Check if allocation violates any resource restriction, or allocate on a
	// non-existent application or nodes.
	var node *objects.Node
	var app *objects.Application
	var ok bool

	if node, ok = psc.nodes[alloc.NodeID]; !ok {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("failed to find node %s", alloc.NodeID)
	}

	if app, ok = psc.applications[alloc.ApplicationID]; !ok {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("failed to find application %s", alloc.ApplicationID)
	}
	queue := app.GetQueue()

	// check the node status again
	if !node.IsSchedulable() {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("node %s is not in schedulable state", node.NodeID)
	}

	// If the new allocation goes beyond the queue's max resource (recursive)?
	// Only check if it is allocated not when it is node reported.
	if err := queue.IncAllocatedResource(alloc.AllocatedResource, true); err != nil {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return fmt.Errorf("cannot allocate resource from application %s: %v ",
			alloc.ApplicationID, err)
	}

	node.AddAllocation(alloc)
	app.AddAllocation(alloc)
	psc.allocations[alloc.UUID] = alloc

	log.Logger().Debug("added allocation",
		zap.String("partitionName", psc.Name),
		zap.String("appID", alloc.ApplicationID),
		zap.String("allocationUid", alloc.UUID),
		zap.String("allocKey", alloc.AllocationKey))
	return nil
}

func (psc *PartitionContext) convertUGI(ugi *si.UserGroupInformation) (security.UserGroup, error) {
	psc.RLock()
	defer psc.RUnlock()
	return psc.userGroupCache.ConvertUGI(ugi)
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
func (psc *PartitionContext) CalculateNodesResourceUsage() map[string][]int {
	psc.RLock()
	defer psc.RUnlock()
	mapResult := make(map[string][]int)
	for _, node := range psc.nodes {
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

func (psc *PartitionContext) removeAllocation(appID string, uuid string) []*objects.Allocation {
	psc.Lock()
	defer psc.Unlock()
	releasedAllocs := make([]*objects.Allocation, 0)
	var queue *objects.Queue = nil
	if app := psc.applications[appID]; app != nil {
		// when uuid not specified, remove all allocations from the app
		if uuid == "" {
			log.Logger().Debug("remove all allocations",
				zap.String("appID", app.ApplicationID))
			releasedAllocs = append(releasedAllocs, app.RemoveAllAllocations()...)
		} else {
			log.Logger().Debug("removing allocation",
				zap.String("appID", app.ApplicationID),
				zap.String("allocationId", uuid))
			if alloc := app.RemoveAllocation(uuid); alloc != nil {
				releasedAllocs = append(releasedAllocs, alloc)
			}
		}
		queue = app.GetQueue()
	}
	// for each allocations to release, update node.
	total := resources.NewResource()

	for _, alloc := range releasedAllocs {
		// remove allocation from node
		node := psc.nodes[alloc.NodeID]
		if node == nil || node.RemoveAllocation(uuid) == nil {
			log.Logger().Info("node allocation is not found while releasing resources",
				zap.String("appID", appID),
				zap.String("allocationId", uuid))
			continue
		}
		// remove from partition
		delete(psc.allocations, uuid)
		// track total resources
		total.AddTo(alloc.AllocatedResource)
	}
	// this nil check is not really needed as we can only reach here with a queue set, IDE complains without this
	if queue != nil {
		if err := queue.DecAllocatedResource(total); err != nil {
			log.Logger().Warn("failed to release resources from queue",
				zap.String("appID", appID),
				zap.String("allocationId", uuid),
				zap.Error(err))
		}
	}
	return releasedAllocs
}

func (psc *PartitionContext) removeAllocationAsk(appID string, allocationKey string) {
	psc.Lock()
	defer psc.Unlock()
	if app := psc.applications[appID]; app != nil {
		// remove the allocation asks from the app
		reservedAsks := app.RemoveAllocationAsk(allocationKey)
		log.Logger().Info("release allocation ask",
			zap.String("allocation", allocationKey),
			zap.String("appID", appID),
			zap.Int("reservedAskReleased", reservedAsks))
		// update the partition if the asks were reserved (clean up)
		if reservedAsks != 0 {
			psc.unReserveCount(appID, reservedAsks)
		}
	}
}
