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
	"github.com/apache/incubator-yunikorn-core/pkg/handler"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
	"github.com/looplab/fsm"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/placement"
)

type PartitionSchedulingContext struct {
	RmID string // the RM the partition belongs to
	Name string // name of the partition (logging mainly)

	// Private fields need protection
	root                   *SchedulingQueue                  // start of the scheduling queue hierarchy
	applications           map[string]*SchedulingApplication // applications assigned to this partition
	reservedApps           map[string]int                    // applications reserved within this partition, with reservation count
	nodes                  map[string]*SchedulingNode        // nodes assigned to this partition
	placementManager       *placement.AppPlacementManager    // placement manager for this partition
	partitionManager       *partitionManager                 // manager for this partition
	allocations            map[string]*schedulingAllocation  // allocations
	stateMachine           *fsm.FSM                          // the state of the queue for scheduling
	stateTime              time.Time                         // last time the state was updated (needed for cleanup)
	isPreemptable          bool                              // can allocations be preempted
	rules                  *[]configs.PlacementRule          // placement rules to be loaded by the scheduler
	userGroupCache         *security.UserGroupCache          // user cache per partition
	totalPartitionResource *resources.Resource               // Total node resources
	nodeSortingPolicy      *common.NodeSortingPolicy         // Global Node Sorting Policies
	eventHandler           handler.EventHandlers
	scheduler              *Scheduler

	sync.RWMutex
}

// Create a new partition from scratch based on a validated configuration.
// If the configuration did not pass validation and is processed weird errors could occur.
func newSchedulingPartitionFromConfig(conf configs.PartitionConfig, rmID string, scheduler *Scheduler) (*PartitionSchedulingContext, error) {
	p := &PartitionSchedulingContext{
		Name:                   conf.Name,
		RmID:                   rmID,
		stateMachine:           newObjectState(),
		allocations:            make(map[string]*schedulingAllocation),
		nodes:                  make(map[string]*SchedulingNode),
		applications:           make(map[string]*SchedulingApplication),
		totalPartitionResource: resources.NewResource(),
		reservedApps:           make(map[string]int),
		eventHandler:           scheduler.eventHandlers,
		scheduler:              scheduler,
	}

	log.Logger().Info("creating partition",
		zap.String("partitionName", p.Name),
		zap.String("rmID", p.RmID))

	if err := initialPartitionFromConfig(p, conf); err != nil {
		return nil, err
	}

	p.placementManager = placement.NewPlacementManager(p)
	p.partitionManager = &partitionManager{
		psc: p,
		stop: false,
		scheduler: scheduler,
	}

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
		return fmt.Errorf("failed to submit to queue %s for application %s. " +
			"It is either caused by wrong ACL permission or trying to submit to a parent queue",
			schedulingApp.QueueName, appID)
	}

	// with placement rules the hierarchy might not exist so try and create it
	if schedulingQueue == nil {
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
	part := strings.Split(strings.ToLower(name), DOT)
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

func (psc *PartitionSchedulingContext) removeSchedulingNodeInternal(nodeID string) {
	if nodeID == "" {
		return
	}

	// check consistency just for debug
	node, ok := psc.nodes[nodeID]
	if !ok {
		log.Logger().Debug("node to be removed does not exist: cache out of sync with scheduler",
			zap.String("nodeID", nodeID))
		return
	}

	node.SetSchedulable(false)

	psc.removeNodeAllocations(node)
	psc.totalPartitionResource.SubFrom(node.totalResource)
	psc.root.setMaxResource(psc.totalPartitionResource)

	// Remove node from list of tracked nodes
	metrics.GetSchedulerMetrics().DecActiveNodes()
	// remove the node, this will also get the sync back between the two lists
	delete(psc.nodes, nodeID)
	// unreserve all the apps that were reserved on the node
	reservedKeys, releasedAsks := node.unReserveApps()
	// update the partition reservations based on the node clean up
	for i, appID := range reservedKeys {
		psc.unReserveCount(appID, releasedAsks[i])
	}
}

// Remove a scheduling node triggered by the removal of the cache node.
// This will log if the scheduler is out of sync with the cache.
// Should never be called directly as it will bring the scheduler out of sync with the cache.
func (psc *PartitionSchedulingContext) removeSchedulingNode(nodeID string) {
	psc.Lock()
	defer psc.Unlock()

	psc.removeSchedulingNodeInternal(nodeID)
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

// Sanity check of allocation, and get app and node reference
func (psc *PartitionSchedulingContext) reserveUnreserveSanityCheck(alloc* schedulingAllocation) (*SchedulingApplication, *SchedulingNode, error) {
	// partition is locked nothing can change from now on
	// find the app make sure it still exists
	appID := alloc.SchedulingAsk.ApplicationID
	app := psc.applications[appID]
	if app == nil {
		return nil, nil, fmt.Errorf("application %s was removed while allocating", appID)
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
		return nil, nil, fmt.Errorf("node=%s was removed while allocating, appId=%s", nodeID, appID)
	}

	return app, node, nil
}

func (psc *PartitionSchedulingContext) handleReserveAndUnreserve(alloc* schedulingAllocation) {
	psc.Lock()
	defer psc.Unlock()

	app, node, err := psc.reserveUnreserveSanityCheck(alloc)
	if err != nil {
		log.Logger().Debug("Reservation Sanity Check failed",
			zap.Error(err))
		return
	}

	// reservation does not leave the scheduler
	if alloc.result == reserved {
		psc.reserve(app, node, alloc.SchedulingAsk)
	} else if alloc.result == unreserved || alloc.result == allocatedReserved {
		psc.unReserve(app, node, alloc.SchedulingAsk)
	}
}

// Process the allocation and make the changes in the partition.
// If the allocation needs to be passed on to the cache true will be returned if not false is returned
func (psc *PartitionSchedulingContext) allocate(alloc *schedulingAllocation) bool {
	// If there's reservation or unreserve, handle it
	if alloc.result == allocatedReserved || alloc.result == unreserved || alloc.result == reserved {
		psc.handleReserveAndUnreserve(alloc)
	}

	log.Logger().Info("scheduler allocation proposal",
		zap.String("appID", alloc.SchedulingAsk.ApplicationID),
		zap.String("queue", alloc.SchedulingAsk.QueueName),
		zap.String("allocationKey", alloc.SchedulingAsk.AskProto.AllocationKey),
		zap.String("allocatedResource", alloc.SchedulingAsk.AllocatedResource.String()),
		zap.String("targetNode", alloc.nodeID))

	// If there's allocation, no matter if it is new allocation or allocate from reserve, commit it here.
	if alloc.result == allocated || alloc.result == allocatedReserved {
		psc.commitSchedulingAllocation(alloc)
	}
	return true
}

// Process an allocation bundle which could contain release and allocation proposals.
// Lock free call, all updates occur on the underlying partition which is locked or via events.
func (psc *PartitionSchedulingContext) commitSchedulingAllocation(alloc *schedulingAllocation) {
	// Release if there is anything to release
	if len(alloc.releases) > 0 {
		psc.scheduler.processAllocationReleases(alloc.releases)
	}
	// Skip allocation if nothing here.
	if alloc.repeats == 0 {
		return
	}

	// we currently only support 1 allocation in the list, reject all but the first
	if alloc.repeats != 1 {
		log.Logger().Info("More than 1 allocation proposal rejected all but first",
			zap.Int32("allocPropLength", alloc.repeats))
	}

	partitionName := alloc.SchedulingAsk.PartitionName
	queueName := alloc.SchedulingAsk.QueueName
	allocationKey := alloc.SchedulingAsk.AskProto.AllocationKey

	// just process the first the rest is rejected already
	err := psc.addNewAllocation(alloc)
	if err != nil {
		log.Logger().Error("failed to add new allocation to partition",
			zap.String("partition", psc.Name),
			zap.String("allocationKey", alloc.SchedulingAsk.AskProto.AllocationKey),
			zap.Error(err))
		return
	}

	log.Logger().Info("allocation accepted",
		zap.String("appID", alloc.SchedulingAsk.ApplicationID),
		zap.String("queue", queueName),
		zap.String("partition", partitionName),
		zap.String("allocationKey", allocationKey))

	// Send allocation event to RM: rejects are not passed back
	rmID := common.GetRMIdFromPartitionName(partitionName)
	psc.eventHandler.RMProxyEventHandler.HandleEvent(&rmevent.RMNewAllocationsEvent{
		Allocations: []*si.Allocation{
			{
				AllocationKey:    allocationKey,
				AllocationTags:   alloc.SchedulingAsk.AskProto.Tags,
				UUID:             alloc.GetUUID(),
				ResourcePerAlloc: alloc.SchedulingAsk.AllocatedResource.ToProto(),
				Priority:         alloc.SchedulingAsk.AskProto.Priority,
				QueueName:        queueName,
				NodeID:           alloc.nodeID,
				PartitionName:    common.GetPartitionNameWithoutClusterID(partitionName),
				ApplicationID:    alloc.SchedulingAsk.ApplicationID,
			},
		},
		RmID: rmID,
	})
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
	configuredPolicy := psc.GetNodeSortingPolicy()
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
	// FIXME:
	//    note from wangda: the behavior of recovery need to be double checked:
	//    - How assume cache should work. (Do we want to check predict / assume cache?)
	if len(existingAllocations) > 0 {
		log.Logger().Info("add existing allocations",
			zap.String("nodeID", node.NodeID),
			zap.Int("existingAllocations", len(existingAllocations)))
		for current, alloc := range existingAllocations {
			if err := psc.addNodeReportedAllocations(alloc); err != nil {
				psc.removeSchedulingNodeInternal(node.NodeID)
				log.Logger().Info("failed to add existing allocations",
					zap.String("nodeID", node.NodeID),
					zap.Int("existingAllocations", len(existingAllocations)),
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
func (psc *PartitionSchedulingContext) addNodeReportedAllocations(allocation *si.Allocation) error {
	return psc.addNewAllocationInternal(newSchedulingAllocationFromProto(allocation), true)
}

// Remove all allocations that are assigned to a node as part of the node removal. This is not part of the node object
// as updating the applications and queues is the only goal. Applications and queues are not accessible from the node.
// The removed allocations are returned.
func (psc *PartitionSchedulingContext) removeNodeAllocations(node *SchedulingNode) {
	// walk over all allocations still registered for this node
	for _, alloc := range node.GetAllAllocations() {
		psc.releaseAllocationsForApplicationInternal(&ReleaseAllocation{
			UUID:          alloc.GetUUID(),
			ApplicationID: alloc.SchedulingAsk.ApplicationID,
			PartitionName: psc.Name,
			Message:       "Released after node removed",
			ReleaseType:   si.AllocationReleaseResponse_STOPPED_BY_RM,
		})
	}
}

// Get the node object for the node ID as tracked by the partition.
// This will return nil if the node is not part of this partition.
// Visible by tests
func (psc *PartitionSchedulingContext) GetNode(nodeID string) *SchedulingNode {
	psc.RLock()
	defer psc.RUnlock()

	return psc.nodes[nodeID]
}

func (psc *PartitionSchedulingContext) releaseAllocationsForApplicationInternal(toRelease *ReleaseAllocation) []*schedulingAllocation {
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

	if len(allocationsToRelease) != 0 {
		// whatever was released pass it back to the RM
		psc.notifyRMAllocationReleased(psc.RmID, allocationsToRelease, toRelease.ReleaseType, toRelease.Message)

		// if reconcile plugin is enabled, re-sync the cache now.
		// this gives the chance for the cache to update its memory about assumed pods
		// whenever we release an allocation, we must ensure the corresponding pod is successfully
		// removed from external cache, otherwise predicates will run into problems.
		if rp := plugins.GetReconcilePlugin(); rp != nil {
			// Forgot allocations is to notify shim to remove from Cache (for predicates).
			forgotAllocations := make([]*si.ForgotAllocation, 0)
			for _, alloc := range allocationsToRelease {
				forgotAllocations = append(forgotAllocations, &si.ForgotAllocation{
					AllocationKey: alloc.SchedulingAsk.AskProto.AllocationKey,
				})
			}
			if err := rp.ReSyncSchedulerCache(&si.ReSyncSchedulerCacheArgs{
				ForgetAllocations: forgotAllocations,
			}); err != nil {
				log.Logger().Error("failed to sync cache",
					zap.Error(err))
			}
		}
	}

	return allocationsToRelease
}

// Create a RM update event to notify RM of released allocations
// Lock free call, all updates occur via events.
func (psc *PartitionSchedulingContext) notifyRMAllocationReleased(rmID string, released []*schedulingAllocation, terminationType si.AllocationReleaseResponse_TerminationType,
	message string) {
	releaseEvent := &rmevent.RMReleaseAllocationEvent{
		ReleasedAllocations: make([]*si.AllocationReleaseResponse, 0),
		RmID:                rmID,
	}
	for _, alloc := range released {
		releaseEvent.ReleasedAllocations = append(releaseEvent.ReleasedAllocations, &si.AllocationReleaseResponse{
			UUID:            alloc.GetUUID(),
			TerminationType: terminationType,
			Message:         message,
		})
	}

	psc.eventHandler.RMProxyEventHandler.HandleEvent(releaseEvent)
}

// Remove one or more allocations for an application.
// Returns all removed allocations.
// If no specific allocation is specified via a uuid all allocations are removed.
func (psc *PartitionSchedulingContext) releaseAllocationsForApplication(toRelease *ReleaseAllocation) []*schedulingAllocation {
	psc.Lock()
	defer psc.Unlock()

	return psc.releaseAllocationsForApplicationInternal(toRelease)
}

func (psc *PartitionSchedulingContext) addNewAllocationSanityCheck(alloc *schedulingAllocation, nodeReported bool) (*SchedulingApplication, *SchedulingNode,
	*SchedulingQueue, error) {
	if psc.isStopped() {
		return nil, nil, nil, fmt.Errorf("partition %s is stopped cannot add new allocation %s", psc.Name, alloc.SchedulingAsk.AskProto.AllocationKey)
	}

	allocKey := alloc.SchedulingAsk.AskProto.AllocationKey
	appId := alloc.SchedulingAsk.ApplicationID
	allocatedResource := alloc.SchedulingAsk.AllocatedResource

	log.Logger().Debug("adding allocation",
		zap.String("partitionName", psc.Name),
		zap.Bool("restoredAlloc", nodeReported),
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
		return nil, nil, nil, fmt.Errorf("failed to find node %s", alloc.nodeID)
	}

	if app, ok = psc.applications[appId]; !ok {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, nil, nil, fmt.Errorf("failed to find application %s", appId)
	}

	// allocation inherits the app queue as the source of truth
	if queue = psc.getQueue(app.QueueName); queue == nil || !queue.IsLeafQueue() {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, nil, nil, fmt.Errorf("queue does not exist or is not a leaf queue %s", app.QueueName)
	}

	// create the key for the reservation
	if err := node.preAllocateCheck(allocatedResource, reservationKey(nil, app, alloc.SchedulingAsk), false); err != nil {
		// skip schedule onto node
		return nil, nil, nil, fmt.Errorf("cannot allocate resource [%v] for application %s on "+
			"node %s because pre allocation check is failed, err=%s",
			allocatedResource, appId, node.NodeID, err.Error())
	}

	// If the new allocation goes beyond the queue's max resource (recursive)?
	// Only check if it is allocated not when it is node reported.
	if err := queue.CheckAllocatedResource(allocatedResource, nodeReported); err != nil {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, nil, nil, fmt.Errorf("cannot allocate resource from application %s: %v ",
			appId, err)
	}


	// skip the node if conditions can not be satisfied
	if err := node.preAllocateConditions(allocKey); err != nil {
		return nil, nil, nil, fmt.Errorf("cannot allocate resource [%v] for application %s on "+
			"node %s because pre allocation condidition is not met, err=%s",
			allocatedResource, appId, node.NodeID, err.Error())
	}

	// App should have enough asks
	if (!nodeReported) && app.GetSchedulingAllocationAsk(allocKey).getPendingAskRepeat() < 0 {
		return nil, nil, nil, fmt.Errorf("cannot allocate resource [%v] for application %s on "+
			"node %s because app doesn't have more ask, allocateKey=%s",
			allocatedResource, appId, node.NodeID, allocKey)
	}

	// if the allocation is node reported (aka recovery an existing allocation),
	// we do not need to generate an UUID for it, we directly use the UUID reported by shim
	// to track this allocation; otherwise this is a new allocation, then we generate an UUID
	// and this UUID will be sent to shim to keep consistent.
	allocationUUID := ""
	if nodeReported {
		if alloc.GetUUID() == "" {
			metrics.GetSchedulerMetrics().IncSchedulingError()
			return nil, nil, nil, fmt.Errorf("failing to restore allocation %s for application %s: missing UUID",
				allocKey, appId)
		}
		allocationUUID = alloc.GetUUID()
	} else {
		allocationUUID = psc.getNewAllocationUUID()
		alloc.SetUUID(allocationUUID)
	}

	return app, node, queue, nil
}

// Add an allocation to the partition/node/application/queue.
// Queue max allocation is not check if the allocation is part of a new node addition (nodeReported == true)
//
// NOTE: It should only be called holding the PartitionInfo lock.
// If access outside is needed a locked version must used, see addNewAllocation
func (psc *PartitionSchedulingContext) addNewAllocationInternal(alloc *schedulingAllocation, nodeReported bool) error {

	app, node, queue, err := psc.addNewAllocationSanityCheck(alloc, nodeReported)

	if err != nil {
		return err
	}

	// Make changes to scheduler

	// before deciding on an allocation, call the reconcile plugin to sync scheduler cache
	// between core and shim if necessary. This is useful when running multiple allocations
	// in parallel and need to handle inter container affinity and anti-affinity.
	if rp := plugins.GetReconcilePlugin(); rp != nil {
		if err := rp.ReSyncSchedulerCache(&si.ReSyncSchedulerCacheArgs{
			AssumedAllocations: []*si.AssumedAllocation{
				{
					AllocationKey: alloc.SchedulingAsk.AskProto.AllocationKey,
					NodeID:        node.NodeID,
				},
			},
		}); err != nil {
			log.Logger().Error("failed to sync shim cache",
				zap.Error(err))
		}
	}

	node.AddAllocation(alloc)

	app.addAllocation(alloc, nodeReported)

	queue.IncAllocatedResource(alloc.SchedulingAsk.AllocatedResource, nodeReported)

	psc.allocations[alloc.GetUUID()] = alloc

	log.Logger().Debug("added allocation",
		zap.String("partitionName", psc.Name),
		zap.String("appID", app.ApplicationID),
		zap.String("allocationUid", alloc.GetUUID()),
		zap.String("allocKey", alloc.SchedulingAsk.AskProto.AllocationKey))

	return nil
}

// Add a new allocation to the partition.
// This is the locked version which calls addNewAllocationInternal() inside a partition lock.
func (psc *PartitionSchedulingContext) addNewAllocation(alloc *schedulingAllocation) error {
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
func (psc *PartitionSchedulingContext) removeApplication(appID string) {
	psc.Lock()
	defer psc.Unlock()

	log.Logger().Debug("removing app from partition",
		zap.String("appID", appID),
		zap.String("partitionName", psc.Name))

	// First release all allocations of the app.
	psc.releaseAllocationsForApplicationInternal(&ReleaseAllocation{
		UUID: "",
		ApplicationID: appID,
		PartitionName: psc.Name,
		Message: "",
		ReleaseType: si.AllocationReleaseResponse_STOPPED_BY_RM,
	})

	app := psc.applications[appID]

	delete(psc.applications, appID)
	delete(psc.reservedApps, appID)

	if app == nil {
		log.Logger().Warn("app not found partition",
			zap.String("appID", appID),
			zap.String("partitionName", psc.Name))
		return
	}

	app.removeAllocationAsk("")

	// we should never have an error, cache is in an inconsistent state if this happens
	queue := app.GetQueue()
	if queue != nil {
		queue.removeSchedulingApplication(app)
	}

	log.Logger().Info("app removed from partition",
		zap.String("appID", app.ApplicationID),
		zap.String("partitionName", app.Partition))
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

// Get the queue from the structure based on the fully qualified name.
// Wrapper around the unlocked version getQueue()
func (psc *PartitionSchedulingContext) Root() *SchedulingQueue {
	psc.RLock()
	defer psc.RUnlock()
	return psc.root
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

func (psc *PartitionSchedulingContext) getAllocation(uuid string) *schedulingAllocation {
	psc.RLock()
	defer psc.RUnlock()

	alloc := psc.allocations[uuid]

	return alloc
}
