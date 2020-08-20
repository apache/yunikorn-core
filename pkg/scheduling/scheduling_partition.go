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

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
	"github.com/looplab/fsm"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/commonevents"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/placement"
)

type PartitionSchedulingContext struct {
	RmID string // the RM the partition belongs to
	Name string // name of the partition (logging mainly)

	// Private fields need protection
	partition              *cache.PartitionInfo              // link back to the partition in the cache
	root                   *SchedulingQueue                  // start of the scheduling queue hierarchy
	applications           map[string]*SchedulingApplication // applications assigned to this partition
	reservedApps           map[string]int                    // applications reserved within this partition, with reservation count
	nodes                  map[string]*SchedulingNode        // nodes assigned to this partition
	placementManager       *placement.AppPlacementManager    // placement manager for this partition
	partitionManager       *partitionManager                 // manager for this partition
	allocations            map[string]*schedulingAllocation        // allocations
	stateMachine           *fsm.FSM                          // the state of the queue for scheduling
	stateTime              time.Time                         // last time the state was updated (needed for cleanup)
	isPreemptable          bool                              // can allocations be preempted
	rules                  *[]configs.PlacementRule          // placement rules to be loaded by the scheduler
	userGroupCache         *security.UserGroupCache          // user cache per partition
	totalPartitionResource *resources.Resource               // Total node resources
	nodeSortingPolicy      *common.NodeSortingPolicy         // Global Node Sorting Policies

	sync.RWMutex
}

// Create a new partition from scratch based on a validated configuration.
// If the configuration did not pass validation and is processed weird errors could occur.
func newSchedulingPartitionFromConfig(conf configs.PartitionConfig, rmID string) (*PartitionSchedulingContext, error) {
	p := &PartitionSchedulingContext{
		Name:                   conf.Name,
		RmID:                   rmID,
		stateMachine:           newObjectState(),
		allocations:            make(map[string]*schedulingAllocation),
		nodes:                  make(map[string]*SchedulingNode),
		applications:           make(map[string]*SchedulingApplication),
		totalPartitionResource: resources.NewResource(),
		reservedApps:           make(map[string]int),
	}

	log.Logger().Info("creating partition",
		zap.String("partitionName", p.Name),
		zap.String("rmID", p.RmID))

	if err := initialPartitionFromConfig(p, conf); err != nil {
		return nil, err
	}

	p.placementManager = placement.NewPlacementManager(p)

	return p, nil
}

func initialPartitionFromConfig(p *PartitionSchedulingContext, conf configs.PartitionConfig) error {
	// Setup the queue structure: root first it should be the only queue at this level
	// Add the rest of the queue structure recursively
	queueConf := conf.Queues[0]
	root, err := NewPreConfiguredQueue(queueConf, nil)
	if err != nil {
		return err
	}
	err = addQueueInfo(queueConf.Queues, root)
	if err != nil {
		return err
	}
	p.root = root
	log.Logger().Info("root queue added",
		zap.String("partitionName", p.Name),
		zap.String("rmID", p.RmID))

	// set preemption needed flag
	p.isPreemptable = conf.Preemption.Enabled

	p.rules = &conf.PlacementRules
	// get the user group cache for the conf
	// TODO get the resolver from the config
	p.userGroupCache = security.GetUserGroupCache("")

	// TODO Need some more cleaner interface here.
	var configuredPolicy common.SortingPolicy
	configuredPolicy, err = common.FromString(conf.NodeSortPolicy.Type)
	if err != nil {
		log.Logger().Debug("NodeSorting policy incorrectly set or unknown",
			zap.Error(err))
	}
	switch configuredPolicy {
	case common.BinPackingPolicy, common.FairnessPolicy:
		log.Logger().Info("NodeSorting policy set from config",
			zap.String("policyName", configuredPolicy.String()))
		p.nodeSortingPolicy = common.NewNodeSortingPolicy(conf.NodeSortPolicy.Type)
	case common.Undefined:
		log.Logger().Info("NodeSorting policy not set using 'fair' as default")
		p.nodeSortingPolicy = common.NewNodeSortingPolicy("fair")
	}

	return nil
}

// Update the scheduling partition based on the reloaded config.
func (psc *PartitionSchedulingContext) updatePartitionSchedulingContext(updatePartition *PartitionSchedulingContext) {
	psc.Lock()
	defer psc.Unlock()

	if psc.placementManager.IsInitialised() {
		log.Logger().Info("Updating placement manager rules on config reload")
		err := psc.placementManager.UpdateRules(updatePartition.GetRules())
		if err != nil {
			log.Logger().Info("New placement rules not activated, config reload failed", zap.Error(err))
		}
	} else {
		log.Logger().Info("Creating new placement manager on config reload")
		psc.placementManager = placement.NewPlacementManager(psc)
	}
	root := psc.root
	// update the root queue properties
	root.updateSchedulingQueueProperties(updatePartition.root.GetProperties())
	// update the rest of the queues recursively
	root.updateSchedulingQueueInfo(updatePartition.root.GetCopyOfChildren(), root)
}

// Add a new application to the scheduling partition.
func (psc *PartitionSchedulingContext) addSchedulingApplication(schedulingApp *SchedulingApplication) error {
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

	// Put app under the scheduling queue, the app has already been placed in the partition cache
	queueName := schedulingApp.QueueName
	if psc.placementManager.IsInitialised() {
		err := psc.placementManager.PlaceApplication(schedulingApp)
		if err != nil {
			return fmt.Errorf("failed to place app in requested queue '%s' for application %s: %v", queueName, appID, err)
		}
		// pull out the queue name from the placement
		queueName = schedulingApp.QueueName
	}
	// we have a queue name either from placement or direct
	schedulingQueue := psc.getQueue(queueName)
	// check if the queue already exist and what we have is a leaf queue with submit access
	if schedulingQueue != nil &&
		(!schedulingQueue.isLeafQueue() || !schedulingQueue.checkSubmitAccess(schedulingApp.GetUser())) {
		return fmt.Errorf("failed to find queue %s for application %s", schedulingApp.QueueName, appID)
	}
	// with placement rules the hierarchy might not exist so try and create it
	if schedulingQueue == nil {
		psc.createSchedulingQueue(queueName, schedulingApp.GetUser())
		// find the scheduling queue: if it still does not exist we fail the app
		schedulingQueue = psc.getQueue(queueName)
		if schedulingQueue == nil {
			return fmt.Errorf("failed to find queue %s for application %s", schedulingApp.QueueName, appID)
		}
	}

	// all is OK update the app and partition
	schedulingApp.queue = schedulingQueue
	schedulingQueue.addSchedulingApplication(schedulingApp)
	psc.applications[appID] = schedulingApp

	return nil
}

// Return a copy of the map of all reservations for the partition.
// This will return an empty map if there are no reservations.
// Visible for tests
func (psc *PartitionSchedulingContext) getReservations() map[string]int {
	psc.RLock()
	defer psc.RUnlock()
	reserve := make(map[string]int)
	for key, num := range psc.reservedApps {
		reserve[key] = num
	}
	return reserve
}

// Get the queue from the structure based on the fully qualified name.
// The name is not syntax checked and must be valid.
// Returns nil if the queue is not found otherwise the queue object.
//
// NOTE: this is a lock free call. It should only be called holding the PartitionSchedulingContext lock.
func (psc *PartitionSchedulingContext) getQueue(name string) *SchedulingQueue {
	// start at the root
	queue := psc.root
	part := strings.Split(strings.ToLower(name), cache.DOT)
	// no input
	if len(part) == 0 || part[0] != "root" {
		return nil
	}
	// walk over the parts going down towards the requested queue
	for i := 1; i < len(part); i++ {
		// if child not found break out and return
		if queue = queue.childrenQueues[part[i]]; queue == nil {
			break
		}
	}
	return queue
}

func (psc *PartitionSchedulingContext) getApplication(appID string) *SchedulingApplication {
	psc.RLock()
	defer psc.RUnlock()

	return psc.applications[appID]
}

// Create a scheduling queue with full hierarchy. This is called when a new queue is created from a placement rule.
// It will not return anything and cannot "fail". A failure is picked up by the queue not existing after this call.
//
// NOTE: this is a lock free call. It should only be called holding the PartitionSchedulingContext lock.
func (psc *PartitionSchedulingContext) createSchedulingQueue(name string, user security.UserGroup) {
	// find the scheduling furthest down the hierarchy that exists
	schedQueue := name // the scheduling queue that exists
	cacheQueue := ""   // the cache queue that needs to be created (with children)
	parent := psc.getQueue(schedQueue)
	for parent == nil {
		cacheQueue = schedQueue
		schedQueue = name[0:strings.LastIndex(name, cache.DOT)]
		parent = psc.getQueue(schedQueue)
	}
	// found the last known scheduling queue,
	// create the corresponding scheduler queue based on the already created cache queue
	queue := psc.partition.GetQueue(cacheQueue)
	// if the cache queue does not exist we should fail this create
	if queue == nil {
		return
	}
	// Check the ACL before we really create
	// The existing parent scheduling queue is the lowest we need to look at
	if !parent.checkSubmitAccess(user) {
		log.Logger().Debug("Submit access denied by scheduler on queue",
			zap.String("deniedQueueName", schedQueue),
			zap.String("requestedQueue", name))
		return
	}
	log.Logger().Debug("Creating scheduling queue(s)",
		zap.String("parent", schedQueue),
		zap.String("child", cacheQueue),
		zap.String("fullPath", name))
	// FIXME: need revisit how placement rule works in the new code structure
	newSchedulingQueueInfo(queue, parent)
}

// Get a scheduling node from the partition by nodeID.
func (psc *PartitionSchedulingContext) getSchedulingNode(nodeID string) *SchedulingNode {
	psc.RLock()
	defer psc.RUnlock()

	return psc.nodes[nodeID]
}

// Get a copy of the scheduling nodes from the partition.
// This list does not include reserved nodes or nodes marked unschedulable
func (psc *PartitionSchedulingContext) getSchedulableNodes() []*SchedulingNode {
	return psc.getSchedulingNodes(true)
}

// Get a copy of the scheduling nodes from the partition.
// Excludes unschedulable nodes only, reserved node inclusion depends on the parameter passed in.
func (psc *PartitionSchedulingContext) getSchedulingNodes(excludeReserved bool) []*SchedulingNode {
	psc.RLock()
	defer psc.RUnlock()

	schedulingNodes := make([]*SchedulingNode, 0)
	for _, node := range psc.nodes {
		// filter out the nodes that are not scheduling
		if !node.IsSchedulable() || (excludeReserved && node.isReserved()) {
			continue
		}
		schedulingNodes = append(schedulingNodes, node)
	}
	return schedulingNodes
}

// Add a new scheduling node triggered on the addition of the cache node.
// This will log if the scheduler is out of sync with the cache.
// As a side effect it will bring the cache and scheduler back into sync.
func (psc *PartitionSchedulingContext) addSchedulingNode(node *SchedulingNode) {
	if node == nil {
		return
	}

	psc.Lock()
	defer psc.Unlock()
	// check consistency and reset to make sure it is consistent again
	if _, ok := psc.nodes[node.NodeID]; ok {
		log.Logger().Debug("new node already existed: cache out of sync with scheduler",
			zap.String("nodeID", node.NodeID))
	}
	// add the node, this will also get the sync back between the two lists
	psc.nodes[node.NodeID] = node
}

func (psc *PartitionSchedulingContext) updateSchedulingNode(info *cache.NodeInfo) {
	if info == nil {
		return
	}

	psc.Lock()
	defer psc.Unlock()
	if schedulingNode, ok := psc.nodes[info.NodeID]; ok {
		schedulingNode.updateNodeInfo(info)
	} else {
		log.Logger().Warn("node is not found in PartitionSchedulingContext while attempting to update it",
			zap.String("nodeID", info.NodeID))
	}
}

// Remove a scheduling node triggered by the removal of the cache node.
// This will log if the scheduler is out of sync with the cache.
// Should never be called directly as it will bring the scheduler out of sync with the cache.
func (psc *PartitionSchedulingContext) removeSchedulingNode(nodeID string) {
	if nodeID == "" {
		return
	}

	psc.Lock()
	defer psc.Unlock()
	// check consistency just for debug
	node, ok := psc.nodes[nodeID]
	if !ok {
		log.Logger().Debug("node to be removed does not exist: cache out of sync with scheduler",
			zap.String("nodeID", nodeID))
		return
	}
	// remove the node, this will also get the sync back between the two lists
	delete(psc.nodes, nodeID)
	// unreserve all the apps that were reserved on the node
	reservedKeys, releasedAsks := node.unReserveApps()
	// update the partition reservations based on the node clean up
	for i, appID := range reservedKeys {
		psc.unReserveCount(appID, releasedAsks[i])
	}
}

func (psc *PartitionSchedulingContext) calculateOutstandingRequests() []*schedulingAllocationAsk {
	if !resources.StrictlyGreaterThanZero(psc.root.GetPendingResource()) {
		return nil
	}
	outstanding := make([]*schedulingAllocationAsk, 0)
	psc.root.getQueueOutstandingRequests(&outstanding)
	return outstanding
}

// Try regular allocation for the partition
// Lock free call this all locks are taken when needed in called functions
func (psc *PartitionSchedulingContext) tryAllocate() *schedulingAllocation {
	if !resources.StrictlyGreaterThanZero(psc.root.GetPendingResource()) {
		// nothing to do just return
		return nil
	}
	// try allocating from the root down
	return psc.root.tryAllocate(psc)
}

// Try process reservations for the partition
// Lock free call this all locks are taken when needed in called functions
func (psc *PartitionSchedulingContext) tryReservedAllocate() *schedulingAllocation {
	// try allocating from the root down
	return psc.root.tryReservedAllocate(psc)
}

// Process the allocation and make the changes in the partition.
// If the allocation needs to be passed on to the cache true will be returned if not false is returned
func (psc *PartitionSchedulingContext) allocate(alloc *schedulingAllocation) bool {
	psc.Lock()
	defer psc.Unlock()
	// partition is locked nothing can change from now on
	// find the app make sure it still exists
	appID := alloc.SchedulingAsk.ApplicationID
	app := psc.applications[appID]
	if app == nil {
		log.Logger().Info("Application was removed while allocating",
			zap.String("appID", appID))
		return false
	}
	// find the node make sure it still exists
	// if the node was passed in use that ID instead of the one from the allocation
	// the node ID is set when a reservation is allocated on a non-reserved node
	var nodeID string
	if alloc.reservedNodeID == "" {
		nodeID = alloc.nodeID
	} else {
		nodeID = alloc.reservedNodeID
		log.Logger().Debug("Reservation allocated on different node",
			zap.String("current node", alloc.nodeID),
			zap.String("reserved node", nodeID),
			zap.String("appID", appID))
	}
	node := psc.nodes[nodeID]
	if node == nil {
		log.Logger().Info("Node was removed while allocating",
			zap.String("nodeID", nodeID),
			zap.String("appID", appID))
		return false
	}
	// reservation does not leave the scheduler
	if alloc.result == reserved {
		psc.reserve(app, node, alloc.SchedulingAsk)
		return false
	}
	// unreserve does not leave the scheduler
	if alloc.result == unreserved || alloc.result == allocatedReserved {
		// unreserve only in the scheduler
		psc.unReserve(app, node, alloc.SchedulingAsk)
		// real allocation after reservation does get passed on to the cache
		if alloc.result == unreserved {
			return false
		}
	}

	log.Logger().Info("scheduler allocation proposal",
		zap.String("appID", alloc.SchedulingAsk.ApplicationID),
		zap.String("queue", alloc.SchedulingAsk.QueueName),
		zap.String("allocationKey", alloc.SchedulingAsk.AskProto.AllocationKey),
		zap.String("allocatedResource", alloc.SchedulingAsk.AllocatedResource.String()),
		zap.String("targetNode", alloc.nodeID))
	return true
}

// Confirm the allocation. This is called as the result of the scheduler passing the proposal to the cache.
// This updates the allocating resources for app, queue and node in the scheduler
// Called for both allocations from reserved as well as for direct allocations.
// The unreserve is already handled before we get here so there is no difference in handling.
func (psc *PartitionSchedulingContext) confirmAllocation(allocProposal *commonevents.AllocationProposal, confirm bool) error {
	psc.RLock()
	defer psc.RUnlock()
	// partition is locked nothing can change from now on
	// find the app make sure it still exists
	appID := allocProposal.ApplicationID
	app := psc.applications[appID]
	if app == nil {
		return fmt.Errorf("application was removed while allocating: %s", appID)
	}
	// find the node make sure it still exists
	nodeID := allocProposal.NodeID
	node := psc.nodes[nodeID]
	if node == nil {
		return fmt.Errorf("node was removed while allocating app %s: %s", appID, nodeID)
	}

	// Node and app exist we now can confirm the allocation
	allocKey := allocProposal.AllocationKey
	log.Logger().Debug("processing allocation proposal",
		zap.String("partition", psc.Name),
		zap.String("appID", appID),
		zap.String("nodeID", nodeID),
		zap.String("allocKey", allocKey),
		zap.Bool("confirmation", confirm))

	// this is a confirmation or rejection update inflight allocating resources of all objects
	delta := allocProposal.AllocatedResource
	if !resources.IsZero(delta) {
		log.Logger().Debug("confirm allocation: update allocating resources",
			zap.String("partition", psc.Name),
			zap.String("appID", appID),
			zap.String("nodeID", nodeID),
			zap.String("allocKey", allocKey),
			zap.String("delta", delta.String()))
		// update the allocating values with the delta
		app.decAllocatingResource(delta)
		app.GetQueue().decAllocatingResource(delta)
		node.decAllocatingResource(delta)
	}

	if !confirm {
		// The repeat gets "added back" when rejected
		// This is a noop when the ask was removed in the mean time: no follow up needed
		_, err := app.updateAskRepeat(allocKey, 1)
		if err != nil {
			return err
		}
	} else if app.GetSchedulingAllocationAsk(allocKey) == nil {
		// The ask was removed while we processed the proposal. The scheduler is updated but we need
		// to make sure the cache removes the allocation that we cannot confirm
		return fmt.Errorf("ask was removed while allocating for app %s: %s", appID, allocKey)
	}

	// all is ok when we are here
	log.Logger().Info("allocation proposal confirmed",
		zap.String("appID", appID),
		zap.String("allocationKey", allocKey),
		zap.String("nodeID", nodeID))
	return nil
}

// Process the reservation in the scheduler
// Lock free call this must be called holding the context lock
func (psc *PartitionSchedulingContext) reserve(app *SchedulingApplication, node *SchedulingNode, ask *schedulingAllocationAsk) {
	appID := app.ApplicationID
	// app has node already reserved cannot reserve again
	if app.isReservedOnNode(node.NodeID) {
		log.Logger().Info("Application is already reserved on node",
			zap.String("appID", appID),
			zap.String("nodeID", node.NodeID))
		return
	}
	// all ok, add the reservation to the app, this will also reserve the node
	if err := app.reserve(node, ask); err != nil {
		log.Logger().Debug("Failed to handle reservation, error during update of app",
			zap.Error(err))
		return
	}

	// add the reservation to the queue list
	app.GetQueue().reserve(appID)
	// increase the number of reservations for this app
	psc.reservedApps[appID]++

	log.Logger().Info("allocation ask is reserved",
		zap.String("appID", ask.ApplicationID),
		zap.String("queue", ask.QueueName),
		zap.String("allocationKey", ask.AskProto.AllocationKey),
		zap.String("node", node.NodeID))
}

// Process the unreservation in the scheduler
// Lock free call this must be called holding the context lock
func (psc *PartitionSchedulingContext) unReserve(app *SchedulingApplication, node *SchedulingNode, ask *schedulingAllocationAsk) {
	appID := app.ApplicationID
	if psc.reservedApps[appID] == 0 {
		log.Logger().Info("Application is not reserved in partition",
			zap.String("appID", appID))
		return
	}
	// all ok, remove the reservation of the app, this will also unReserve the node
	var err error
	var num int
	if num, err = app.unReserve(node, ask); err != nil {
		log.Logger().Info("Failed to unreserve, error during allocate on the app",
			zap.Error(err))
		return
	}
	// remove the reservation of the queue
	app.GetQueue().unReserve(appID, num)
	// make sure we cannot go below 0
	psc.unReserveCount(appID, num)

	log.Logger().Info("allocation ask is unreserved",
		zap.String("appID", ask.ApplicationID),
		zap.String("queue", ask.QueueName),
		zap.String("allocationKey", ask.AskProto.AllocationKey),
		zap.String("node", node.NodeID),
		zap.Int("reservationsRemoved", num))
}

// Get the iterator for the sorted nodes list from the partition.
func (psc *PartitionSchedulingContext) getNodeIteratorForPolicy(nodes []*SchedulingNode) NodeIterator {
	// Sort Nodes based on the policy configured.
	configuredPolicy := psc.partition.GetNodeSortingPolicy()
	if configuredPolicy == common.Undefined {
		return nil
	}
	sortNodes(nodes, configuredPolicy)
	return NewDefaultNodeIterator(nodes)
}

// Create a node iterator for the schedulable nodes based on the policy set for this partition.
// The iterator is nil if there are no schedulable nodes available.
func (psc *PartitionSchedulingContext) getNodeIterator() NodeIterator {
	if nodeList := psc.getSchedulableNodes(); len(nodeList) != 0 {
		return psc.getNodeIteratorForPolicy(nodeList)
	}
	return nil
}

// Locked version of the reservation counter update
// Called by the scheduler
func (psc *PartitionSchedulingContext) unReserveUpdate(appID string, asks int) {
	psc.Lock()
	defer psc.Unlock()
	psc.unReserveCount(appID, asks)
}

// Update the reservation counter for the app
// Lock free call this must be called holding the context lock
func (psc *PartitionSchedulingContext) unReserveCount(appID string, asks int) {
	if num, found := psc.reservedApps[appID]; found {
		// decrease the number of reservations for this app and cleanup
		if num == asks {
			delete(psc.reservedApps, appID)
		} else {
			psc.reservedApps[appID] -= asks
		}
	}
}

// Process the config structure and create a queue info tree for this partition
func addQueueInfo(conf []configs.QueueConfig, parent *SchedulingQueue) error {
	// create the queue at this level
	for _, queueConf := range conf {
		thisQueue, err := NewPreConfiguredQueue(queueConf, parent)
		if err != nil {
			return err
		}
		// recursive create the queues below
		if len(queueConf.Queues) > 0 {
			err = addQueueInfo(queueConf.Queues, thisQueue)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Handle the state event for the application.
// The state machine handles the locking.
func (psc *PartitionSchedulingContext) handlePartitionEvent(event SchedulingObjectEvent) error {
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

func (psc *PartitionSchedulingContext) GetTotalPartitionResource() *resources.Resource {
	psc.RLock()
	defer psc.RUnlock()

	return psc.totalPartitionResource
}

// Does the partition allow pre-emption?
func (psc *PartitionSchedulingContext) NeedPreemption() bool {
	return psc.isPreemptable
}

// Return the config element for the placement rules
func (psc *PartitionSchedulingContext) GetRules() []configs.PlacementRule {
	if psc.rules == nil {
		return []configs.PlacementRule{}
	}
	return *psc.rules
}

// Is bin-packing scheduling enabled?
// TODO: more finer enum based return model here is better instead of bool.
func (psc *PartitionSchedulingContext) GetNodeSortingPolicy() common.SortingPolicy {
	if psc.nodeSortingPolicy == nil {
		return common.FairnessPolicy
	}

	return psc.nodeSortingPolicy.PolicyType
}

// Add a new node to the partition.
// If a partition is not active a new node can not be added as the partition is about to be removed.
// A new node must be added to the partition before the existing allocations can be processed. This
// means that the partition is updated before the allocations. Failure to add all reported allocations will
// cause the node to be rejected
func (psc *PartitionSchedulingContext) addNewNode(node *SchedulingNode, existingAllocations []*si.Allocation) error {
	psc.Lock()
	defer psc.Unlock()

	log.Logger().Debug("add node to partition",
		zap.String("nodeID", node.NodeID),
		zap.String("partition", psc.Name))

	if psc.isDraining() || psc.isStopped() {
		return fmt.Errorf("partition %s is stopped cannot add a new node %s", psc.Name, node.NodeID)
	}

	if psc.nodes[node.NodeID] != nil {
		return fmt.Errorf("partition %s has an existing node %s, node name must be unique", psc.Name, node.NodeID)
	}

	// update the resources available in the cluster
	psc.totalPartitionResource.AddTo(node.totalResource)
	psc.root.setMaxResource(psc.totalPartitionResource)

	// Node is added to the system to allow processing of the allocations
	psc.nodes[node.NodeID] = node

	// Add allocations that exist on the node when added
	if len(existingAllocations) > 0 {
		log.Logger().Info("add existing allocations",
			zap.String("nodeID", node.NodeID),
			zap.Int("existingAllocations", len(existingAllocations)))
		for current, alloc := range existingAllocations {
			if _, err := psc.addNodeReportedAllocations(alloc); err != nil {
				released := psc.removeNodeInternal(node.NodeID)
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

// Wrapper function to convert the reported allocation into an AllocationProposal.
// Used when a new node is added to the partition which already reports existing allocations.
// FIXME: overhaul of Allocation from schedulingAllocation needed.
func (psc *PartitionSchedulingContext) addNodeReportedAllocations(allocation *si.Allocation) (*schedulingAllocation, error) {
	return psc.addNewAllocationInternal(&commonevents.AllocationProposal{
		NodeID:            allocation.NodeID,
		ApplicationID:     allocation.ApplicationID,
		QueueName:         allocation.QueueName,
		AllocatedResource: resources.NewResourceFromProto(allocation.ResourcePerAlloc),
		AllocationKey:     allocation.AllocationKey,
		Tags:              allocation.AllocationTags,
		Priority:          allocation.Priority,
		UUID:              allocation.UUID,
	}, true)
}

// Remove a node from the partition.
// This locks the partition and calls the internal unlocked version.
func (psc *PartitionSchedulingContext) RemoveNode(nodeID string) []*schedulingAllocation {
	psc.Lock()
	defer psc.Unlock()

	return psc.removeNodeInternal(nodeID)
}

// Remove a node from the partition. It returns all removed allocations.
//
// NOTE: this is a lock free call. It should only be called holding the PartitionInfo lock.
// If access outside is needed a locked version must used, see removeNode
func (psc *PartitionSchedulingContext) removeNodeInternal(nodeID string) []*schedulingAllocation {
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

	// found the node cleanup the node and all linked data
	released := psc.removeNodeAllocations(node)
	psc.totalPartitionResource.SubFrom(node.totalResource)
	psc.root.setMaxResource(psc.totalPartitionResource)

	// Remove node from list of tracked nodes
	delete(psc.nodes, nodeID)
	metrics.GetSchedulerMetrics().DecActiveNodes()

	log.Logger().Info("node removed",
		zap.String("partitionName", psc.Name),
		zap.String("nodeID", node.NodeID))
	return released
}

// Remove all allocations that are assigned to a node as part of the node removal. This is not part of the node object
// as updating the applications and queues is the only goal. Applications and queues are not accessible from the node.
// The removed allocations are returned.
func (psc *PartitionSchedulingContext) removeNodeAllocations(node *SchedulingNode) []*schedulingAllocation {
	released := make([]*schedulingAllocation, 0)
	// walk over all allocations still registered for this node
	for _, alloc := range node.GetAllAllocations() {
		var queue *SchedulingQueue = nil
		allocID := alloc.GetUUID()
		// since we are not locking the node and or application we could have had an update while processing
		// note that we do not return the allocation if the app or allocation is not found and assume that it
		// was already removed
		if app := psc.applications[alloc.SchedulingAsk.ApplicationID]; app != nil {
			// check allocations on the node
			if app.removeAllocation(allocID) == nil {
				log.Logger().Info("allocation is not found, skipping while removing the node",
					zap.String("allocationId", allocID),
					zap.String("appID", app.ApplicationID),
					zap.String("nodeID", node.NodeID))
				continue
			}
			queue = app.GetQueue()
		} else {
			log.Logger().Info("app is not found, skipping while removing the node",
				zap.String("appID", alloc.SchedulingAsk.ApplicationID),
				zap.String("nodeID", node.NodeID))
			continue
		}

		// we should never have an error, cache is in an inconsistent state if this happens
		if queue != nil {
			if err := queue.decAllocatedResource(alloc.SchedulingAsk.AllocatedResource); err != nil {
				log.Logger().Warn("failed to release resources from queue",
					zap.String("appID", alloc.SchedulingAsk.ApplicationID),
					zap.Error(err))
			}
		}

		// the allocation is removed so add it to the list that we return
		released = append(released, alloc)
		log.Logger().Info("allocation removed",
			zap.String("allocationId", allocID),
			zap.String("nodeID", node.NodeID))
	}
	return released
}

// Get the node object for the node ID as tracked by the partition.
// This will return nil if the node is not part of this partition.
// Visible by tests
func (psc *PartitionSchedulingContext) GetNode(nodeID string) *SchedulingNode {
	psc.RLock()
	defer psc.RUnlock()

	return psc.nodes[nodeID]
}

// Remove one or more allocations for an application.
// Returns all removed allocations.
// If no specific allocation is specified via a uuid all allocations are removed.
func (psc *PartitionSchedulingContext) releaseAllocationsForApplication(toRelease *commonevents.ReleaseAllocation) []*schedulingAllocation {
	psc.Lock()
	defer psc.Unlock()

	allocationsToRelease := make([]*schedulingAllocation, 0)

	log.Logger().Debug("removing allocation from partition",
		zap.String("partitionName", psc.Name))
	if toRelease == nil {
		log.Logger().Debug("no allocations removed from partition",
			zap.String("partitionName", psc.Name))
		return allocationsToRelease
	}

	// First delete from app
	var queue *SchedulingQueue = nil
	if app := psc.applications[toRelease.ApplicationID]; app != nil {
		// when uuid not specified, remove all allocations from the app
		if toRelease.UUID == "" {
			log.Logger().Debug("remove all allocations",
				zap.String("appID", app.ApplicationID))
			allocationsToRelease = append(allocationsToRelease, app.removeAllAllocations()...)
		} else {
			log.Logger().Debug("removing allocations",
				zap.String("appID", app.ApplicationID),
				zap.String("allocationId", toRelease.UUID))
			if alloc := app.removeAllocation(toRelease.UUID); alloc != nil {
				allocationsToRelease = append(allocationsToRelease, alloc)
			}
		}
		queue = app.GetQueue()
	}

	// If nothing was released then return now: this can happen if the allocation was not found or the application did not
	// not have any allocations
	if len(allocationsToRelease) == 0 {
		log.Logger().Debug("no active allocations found to release",
			zap.String("appID", toRelease.ApplicationID))
		return allocationsToRelease
	}

	// for each allocations to release, update node.
	totalReleasedResource := resources.NewResource()

	for _, alloc := range allocationsToRelease {
		// remove allocation from node
		node := psc.nodes[alloc.nodeID]
		if node == nil || node.GetAllocation(alloc.GetUUID()) == nil {
			log.Logger().Info("node is not found for allocation",
				zap.Any("allocation", alloc))
			continue
		}
		node.RemoveAllocation(alloc.GetUUID())
		totalReleasedResource.AddTo(alloc.SchedulingAsk.AllocatedResource)
	}

	// this nil check is not really needed as we can only reach here with a queue set, IDE complains without this
	if queue != nil {
		// we should never have an error, cache is in an inconsistent state if this happens
		if err := queue.decAllocatedResource(totalReleasedResource); err != nil {
			log.Logger().Warn("failed to release resources",
				zap.Any("appID", toRelease.ApplicationID),
				zap.Error(err))
		}
	}

	// Update global allocation list
	for _, alloc := range allocationsToRelease {
		delete(psc.allocations, alloc.GetUUID())
	}

	log.Logger().Info("allocation removed",
		zap.Int("numOfAllocationReleased", len(allocationsToRelease)),
		zap.String("partitionName", psc.Name))
	return allocationsToRelease
}

// Add an allocation to the partition/node/application/queue.
// Queue max allocation is not check if the allocation is part of a new node addition (nodeReported == true)
//
// NOTE: It should only be called holding the PartitionInfo lock.
// If access outside is needed a locked version must used, see addNewAllocation
func (psc *PartitionSchedulingContext) addNewAllocationInternal(alloc *schedulingAllocation, checkQueueMaxResource bool) (*schedulingAllocation, error) {
	if psc.isStopped() {
		return nil, fmt.Errorf("partition %s is stopped cannot add new allocation %s", psc.Name, alloc.SchedulingAsk.AskProto.AllocationKey)
	}

	allocKey := alloc.SchedulingAsk.AskProto.AllocationKey
	appId := alloc.SchedulingAsk.ApplicationID
	allocatedResource := alloc.SchedulingAsk.AllocatedResource

	log.Logger().Debug("adding allocation",
		zap.String("partitionName", psc.Name),
		zap.Bool("restoredAlloc", checkQueueMaxResource),
		zap.String("appID", appId),
		zap.String("allocKey", allocKey))

	// Check if allocation violates any resource restriction, or allocate on a
	// non-existent applications or nodes.
	var node *SchedulingNode
	var app *SchedulingApplication
	var queue *SchedulingQueue
	var ok bool

	if node, ok = psc.nodes[alloc.nodeID]; !ok {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, fmt.Errorf("failed to find node %s", alloc.nodeID)
	}

	if app, ok = psc.applications[appId]; !ok {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, fmt.Errorf("failed to find application %s", appId)
	}

	// allocation inherits the app queue as the source of truth
	if queue = psc.getQueue(app.QueueName); queue == nil || !queue.IsLeafQueue() {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, fmt.Errorf("queue does not exist or is not a leaf queue %s", app.QueueName)
	}

	// check the node status again
	if !node.IsSchedulable() {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, fmt.Errorf("node %s is not in schedulable state", node.NodeID)
	}

	// Does the new allocation exceed the node's available resource?
	if !node.canAllocate(allocatedResource) {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, fmt.Errorf("cannot allocate resource [%v] for application %s on "+
			"node %s because request exceeds available resources, used [%v] node limit [%v]",
			allocatedResource, appId, node.NodeID, node.GetAllocatedResource(), node.totalResource)
	}

	// If the new allocation goes beyond the queue's max resource (recursive)?
	// Only check if it is allocated not when it is node reported.
	if err := queue.IncAllocatedResource(allocatedResource, checkQueueMaxResource); err != nil {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, fmt.Errorf("cannot allocate resource from application %s: %v ",
			appId, err)
	}

	allocationUUID := ""
	// if the allocation is node reported (aka recovery an existing allocation),
	// we do not need to generate an UUID for it, we directly use the UUID reported by shim
	// to track this allocation; otherwise this is a new allocation, then we generate an UUID
	// and this UUID will be sent to shim to keep consistent.
	if checkQueueMaxResource {
		if alloc.GetUUID() == "" {
			metrics.GetSchedulerMetrics().IncSchedulingError()
			return nil, fmt.Errorf("failing to restore allocation %s for application %s: missing UUID",
				allocKey, appId)
		}
		allocationUUID = alloc.GetUUID()
	} else {
		allocationUUID = psc.getNewAllocationUUID()
		alloc.SetUUID(allocationUUID)
	}

	node.AddAllocation(alloc)

	app.addAllocation(alloc)

	psc.allocations[alloc.GetUUID()] = alloc

	log.Logger().Debug("added allocation",
		zap.String("partitionName", psc.Name),
		zap.String("appID", app.ApplicationID),
		zap.String("allocationUid", allocationUUID),
		zap.String("allocKey", allocKey))
	return alloc, nil
}

// Add a new allocation to the partition.
// This is the locked version which calls addNewAllocationInternal() inside a partition lock.
func (psc *PartitionSchedulingContext) addNewAllocation(alloc *schedulingAllocation) (*schedulingAllocation, error) {
	psc.Lock()
	defer psc.Unlock()
	return psc.addNewAllocationInternal(alloc, false)
}

// Generate a new uuid for the allocation.
// This is guaranteed to return a unique ID for this partition.
func (psc *PartitionSchedulingContext) getNewAllocationUUID() string {
	// Retry to make sure uuid is correct
	for {
		allocationUUID := uuid.NewV4().String()
		if psc.allocations[allocationUUID] == nil {
			return allocationUUID
		}
	}
}

// Remove a rejected application from the partition.
// This is just a cleanup, the app has not been scheduled yet.
func (psc *PartitionSchedulingContext) removeRejectedApp(appID string) {
	psc.Lock()
	defer psc.Unlock()

	log.Logger().Debug("removing rejected app from partition",
		zap.String("appID", appID),
		zap.String("partitionName", psc.Name))
	// Remove app from cache there is nothing to be cleaned up
	delete(psc.applications, appID)
}

// Remove the application from the partition.
// This will also release all the allocations for application from the queue and nodes.
func (psc *PartitionSchedulingContext) removeApplication(appID string) []*schedulingAllocation {
	psc.Lock()
	defer psc.Unlock()

	log.Logger().Debug("removing app from partition",
		zap.String("appID", appID),
		zap.String("partitionName", psc.Name))

	app := psc.applications[appID]

	delete(psc.applications, appID)
	delete(psc.reservedApps, appID)

	if app == nil {
		log.Logger().Warn("app not found partition",
			zap.String("appID", appID),
			zap.String("partitionName", psc.Name))
		return nil
	}

	_ = app.removeAllocationAskInternal("")

	// Save the total allocated resources of the application.
	// Might need to base this on calculation of the real removed resources.
	totalAppAllocated := app.GetAllocatedResource()

	// Remove all allocations from the application
	allocations := app.removeAllAllocations()

	// Remove all allocations from nodes/queue
	if len(allocations) != 0 {
		for _, alloc := range allocations {
			currentUUID := alloc.GetUUID()
			// Remove from partition cache
			if globalAlloc := psc.allocations[currentUUID]; globalAlloc == nil {
				log.Logger().Warn("unknown allocation: not found in global cache",
					zap.String("appID", appID),
					zap.String("allocationId", currentUUID))
				continue
			} else {
				delete(psc.allocations, currentUUID)
			}

			// Remove from node
			node := psc.nodes[alloc.nodeID]
			if node == nil {
				log.Logger().Warn("unknown node: not found in active node list",
					zap.String("appID", appID),
					zap.String("nodeID", alloc.nodeID))
				continue
			}
			if nodeAlloc := node.RemoveAllocation(currentUUID); nodeAlloc == nil {
				log.Logger().Warn("allocation not found on node",
					zap.String("appID", appID),
					zap.String("allocationId", currentUUID),
					zap.String("nodeID", alloc.nodeID))
				continue
			}
		}
	}

	// we should never have an error, cache is in an inconsistent state if this happens
	queue := app.GetQueue()
	if queue != nil {
		queue.removeSchedulingApplication(app)
	}

	log.Logger().Info("app removed from partition",
		zap.String("appID", app.ApplicationID),
		zap.String("partitionName", app.Partition),
		zap.Any("resourceReleased", totalAppAllocated))

	return allocations
}

// Return a copy of all the nodes registers to this partition
func (psc *PartitionSchedulingContext) CopyNodeInfos() []*SchedulingNode {
	psc.RLock()
	defer psc.RUnlock()

	out := make([]*SchedulingNode, len(psc.nodes))

	var i = 0
	for _, v := range psc.nodes {
		out[i] = v
		i++
	}

	return out
}

func (psc *PartitionSchedulingContext) GetTotalApplicationCount() int {
	psc.RLock()
	defer psc.RUnlock()
	return len(psc.applications)
}

func (psc *PartitionSchedulingContext) GetTotalAllocationCount() int {
	psc.RLock()
	defer psc.RUnlock()
	return len(psc.allocations)
}

func (psc *PartitionSchedulingContext) GetTotalNodeCount() int {
	psc.RLock()
	defer psc.RUnlock()
	return len(psc.nodes)
}

func (psc *PartitionSchedulingContext) GetApplications() []*SchedulingApplication {
	psc.RLock()
	defer psc.RUnlock()
	var appList []*SchedulingApplication
	for _, app := range psc.applications {
		appList = append(appList, app)
	}
	return appList
}

func (psc *PartitionSchedulingContext) GetNodes() map[string]*SchedulingNode {
	psc.RLock()
	defer psc.RUnlock()
	return psc.nodes
}

// Get the queue from the structure based on the fully qualified name.
// Wrapper around the unlocked version getQueue()
func (psc *PartitionSchedulingContext) GetQueue(name string) *SchedulingQueue {
	psc.RLock()
	defer psc.RUnlock()
	return psc.getQueue(name)
}

// Update the passed in queues and then do this recursively for the children
func (psc *PartitionSchedulingContext) updateQueues(config []configs.QueueConfig, parent *SchedulingQueue) error {
	// get the name of the passed in queue
	parentPath := parent.QueuePath + DOT
	// keep track of which children we have updated
	visited := map[string]bool{}
	// walk over the queues recursively
	for _, queueConfig := range config {
		pathName := parentPath + queueConfig.Name
		queue := psc.getQueue(pathName)
		var err error
		if queue == nil {
			queue, err = NewPreConfiguredQueue(queueConfig, parent)
		} else {
			err = queue.updateQueueProps(queueConfig)
		}
		if err != nil {
			return err
		}
		err = psc.updateQueues(queueConfig.Queues, queue)
		if err != nil {
			return err
		}
		visited[queueConfig.Name] = true
	}
	// remove all children that were not visited
	for childName, childQueue := range parent.childrenQueues {
		if !visited[childName] {
			childQueue.MarkQueueForRemoval()
		}
	}
	return nil
}

// Mark the partition  for removal from the system.
// This can be executed multiple times and is only effective the first time.
func (psc *PartitionSchedulingContext) markPartitionForRemoval() {
	psc.RLock()
	defer psc.RUnlock()
	if err := psc.handlePartitionEvent(Remove); err != nil {
		log.Logger().Error("failed to mark partition for deletion",
			zap.String("partitionName", psc.Name),
			zap.Error(err))
	}
}

// The partition has been removed from the configuration and must be removed.
// This is the cleanup triggered by the exiting scheduler partition. Just unlinking from the cluster should be enough.
// All other clean up is triggered from the scheduler
func (psc *PartitionSchedulingContext) Remove() {
	psc.RLock()
	defer psc.RUnlock()
	// safeguard
	if psc.isDraining() || psc.isStopped() {
		// FIXME: overhaul of clusterInfo, remove partition needed.
		psc.clusterInfo.removePartition(psc.Name)
	}
}

// Is the partition marked for deletion and can only handle existing application requests.
// No new applications will be accepted.
func (psc *PartitionSchedulingContext) isDraining() bool {
	return psc.stateMachine.Current() == Draining.String()
}

func (psc *PartitionSchedulingContext) isRunning() bool {
	return psc.stateMachine.Current() == Active.String()
}

func (psc *PartitionSchedulingContext) isStopped() bool {
	return psc.stateMachine.Current() == Stopped.String()
}

// Create the new queue that is returned from a rule.
// It creates a queue with all parents needed.
func (psc *PartitionSchedulingContext) CreateQueues(queueName string) error {
	if !strings.HasPrefix(queueName, configs.RootQueue+DOT) {
		return fmt.Errorf("cannot create queue which is not qualified '%s'", queueName)
	}
	log.Logger().Debug("Creating new queue structure", zap.String("queueName", queueName))
	// two step creation process: first check then really create them
	// start at the root, which we know exists and is a parent
	current := queueName
	var toCreate []string
	parent := psc.getQueue(current)
	log.Logger().Debug("Checking queue creation")
	for parent == nil {
		toCreate = append(toCreate, current[strings.LastIndex(current, DOT)+1:])
		current = current[0:strings.LastIndex(current, DOT)]
		// check if the queue exist
		parent = psc.getQueue(current)
	}
	// check if it is a parent queue
	if parent.isLeaf {
		log.Logger().Debug("Cannot create queue below existing leaf queue",
			zap.String("requestedQueue", queueName),
			zap.String("leafQueue", current))
		return fmt.Errorf("cannot create queue below leaf queue '%s'", current)
	}
	log.Logger().Debug("Queue can be created, creating queue(s)")
	for i := len(toCreate) - 1; i >= 0; i-- {
		// everything is checked and there should be no errors
		var err error
		parent, err = NewDynamicQueue(toCreate[i], i == 0, parent)
		if err != nil {
			log.Logger().Warn("Queue auto create failed unexpected",
				zap.String("queueName", queueName),
				zap.Error(err))
		}
	}
	return nil
}

func (psc *PartitionSchedulingContext) convertUGI(ugi *si.UserGroupInformation) (security.UserGroup, error) {
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
func (psc *PartitionSchedulingContext) CalculateNodesResourceUsage() map[string][]int {
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
