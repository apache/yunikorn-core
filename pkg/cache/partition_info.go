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

package cache

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/looplab/fsm"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/commonevents"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

/* Related to partitions */
type PartitionInfo struct {
	Name string
	Root *QueueInfo
	RmID string

	// Private fields need protection
	allocations            map[string]*AllocationInfo  // allocations
	nodes                  map[string]*NodeInfo        // nodes registered
	applications           map[string]*ApplicationInfo // the application list
	stateMachine           *fsm.FSM                    // the state of the queue for scheduling
	stateTime              time.Time                   // last time the state was updated (needed for cleanup)
	isPreemptable          bool                        // can allocations be preempted
	rules                  *[]configs.PlacementRule    // placement rules to be loaded by the scheduler
	userGroupCache         *security.UserGroupCache    // user cache per partition
	clusterInfo            *ClusterInfo                // link back to the cluster info
	lock                   sync.RWMutex                // lock for updating the partition
	totalPartitionResource *resources.Resource         // Total node resources
	nodeSortingPolicy      *common.NodeSortingPolicy   // Global Node Sorting Policies
}

// Create a new partition from scratch based on a validated configuration.
// If the configuration did not pass validation and is processed weird errors could occur.
func NewPartitionInfo(partition configs.PartitionConfig, rmID string, info *ClusterInfo) (*PartitionInfo, error) {
	p := &PartitionInfo{
		Name:         partition.Name,
		RmID:         rmID,
		stateMachine: newObjectState(),
		clusterInfo:  info,
	}
	p.allocations = make(map[string]*AllocationInfo)
	p.nodes = make(map[string]*NodeInfo)
	p.applications = make(map[string]*ApplicationInfo)
	p.totalPartitionResource = resources.NewResource()
	log.Logger().Info("creating partition",
		zap.String("partitionName", p.Name),
		zap.String("rmID", p.RmID))

	// Setup the queue structure: root first it should be the only queue at this level
	// Add the rest of the queue structure recursively
	queueConf := partition.Queues[0]
	root, err := NewManagedQueue(queueConf, nil)
	if err != nil {
		return nil, err
	}
	err = addQueueInfo(queueConf.Queues, root)
	if err != nil {
		return nil, err
	}
	p.Root = root
	log.Logger().Info("root queue added",
		zap.String("partitionName", p.Name),
		zap.String("rmID", p.RmID))

	// set preemption needed flag
	p.isPreemptable = partition.Preemption.Enabled

	p.rules = &partition.PlacementRules
	// get the user group cache for the partition
	// TODO get the resolver from the config
	p.userGroupCache = security.GetUserGroupCache("")

	// TODO Need some more cleaner interface here.
	var configuredPolicy common.SortingPolicy
	configuredPolicy, err = common.FromString(partition.NodeSortPolicy.Type)
	if err != nil {
		log.Logger().Debug("NodeSorting policy incorrectly set or unknown",
			zap.Error(err))
	}
	switch configuredPolicy {
	case common.BinPackingPolicy, common.FairnessPolicy:
		log.Logger().Info("NodeSorting policy set from config",
			zap.String("policyName", configuredPolicy.String()))
		p.nodeSortingPolicy = common.NewNodeSortingPolicy(partition.NodeSortPolicy.Type)
	case common.Undefined:
		log.Logger().Info("NodeSorting policy not set using 'fair' as default")
		p.nodeSortingPolicy = common.NewNodeSortingPolicy("fair")
	}

	return p, nil
}

// Process the config structure and create a queue info tree for this partition
func addQueueInfo(conf []configs.QueueConfig, parent *QueueInfo) error {
	// create the queue at this level
	for _, queueConf := range conf {
		thisQueue, err := NewManagedQueue(queueConf, parent)
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
func (pi *PartitionInfo) HandlePartitionEvent(event SchedulingObjectEvent) error {
	err := pi.stateMachine.Event(event.String(), pi.Name)
	if err == nil {
		pi.stateTime = time.Now()
		return nil
	}
	// handle the same state transition not nil error (limit of fsm).
	if err.Error() == "no transition" {
		return nil
	}
	return err
}

func (pi *PartitionInfo) GetTotalPartitionResource() *resources.Resource {
	pi.lock.RLock()
	defer pi.lock.RUnlock()

	return pi.totalPartitionResource
}

// Does the partition allow pre-emption?
func (pi *PartitionInfo) NeedPreemption() bool {
	return pi.isPreemptable
}

// Return the config element for the placement rules
func (pi *PartitionInfo) GetRules() []configs.PlacementRule {
	if pi.rules == nil {
		return []configs.PlacementRule{}
	}
	return *pi.rules
}

// Is bin-packing scheduling enabled?
// TODO: more finer enum based return model here is better instead of bool.
func (pi *PartitionInfo) GetNodeSortingPolicy() common.SortingPolicy {
	if pi.nodeSortingPolicy == nil {
		return common.FairnessPolicy
	}

	return pi.nodeSortingPolicy.PolicyType
}

// Add a new node to the partition.
// If a partition is not active a new node can not be added as the partition is about to be removed.
// A new node must be added to the partition before the existing allocations can be processed. This
// means that the partition is updated before the allocations. Failure to add all reported allocations will
// cause the node to be rejected
func (pi *PartitionInfo) addNewNode(node *NodeInfo, existingAllocations []*si.Allocation) error {
	pi.lock.Lock()
	defer pi.lock.Unlock()

	log.Logger().Info("add node to partition",
		zap.String("nodeID", node.NodeID),
		zap.String("partition", pi.Name))

	if pi.IsDraining() || pi.IsStopped() {
		return fmt.Errorf("partition %s is stopped cannot add a new node %s", pi.Name, node.NodeID)
	}

	if pi.nodes[node.NodeID] != nil {
		return fmt.Errorf("partition %s has an existing node %s, node name must be unique", pi.Name, node.NodeID)
	}

	// update the resources available in the cluster
	pi.totalPartitionResource = resources.Add(pi.totalPartitionResource, node.TotalResource)
	pi.Root.MaxResource = pi.totalPartitionResource

	// Node is added to the system to allow processing of the allocations
	pi.nodes[node.NodeID] = node

	// Add allocations that exist on the node when added
	if len(existingAllocations) > 0 {
		log.Logger().Info("add existing allocations",
			zap.String("nodeID", node.NodeID),
			zap.Int("existingAllocations", len(existingAllocations)))
		for current, alloc := range existingAllocations {
			if _, err := pi.addNodeReportedAllocations(alloc); err != nil {
				released := pi.removeNodeInternal(node.NodeID)
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

	// Node is accepted, scan all recovered allocations again, handle state transition.
	// The node and its existing allocations might not be added (removed immediately after
	// the add) if some allocation cannot be recovered.
	if len(existingAllocations) > 0 {
		// if an allocation of a app is accepted,
		// transit app's state from Accepted to Running
		for _, alloc := range existingAllocations {
			if app, ok := pi.applications[alloc.ApplicationID]; ok {
				log.Logger().Info("", zap.String("state", app.GetApplicationState()))
				if app.GetApplicationState() == Accepted.String() {
					if err := app.HandleApplicationEvent(RunApplication); err != nil {
						log.Logger().Warn("unable to handle app event - RunApplication",
							zap.Error(err))
					}
				}
			}
		}
	}

	// Node is added update the metrics
	metrics.GetSchedulerMetrics().IncActiveNodes()
	log.Logger().Info("added node to partition",
		zap.String("nodeID", node.NodeID),
		zap.String("partition", pi.Name))

	return nil
}

// Wrapper function to convert the reported allocation into an AllocationProposal.
// Used when a new node is added to the partition which already reports existing allocations.
func (pi *PartitionInfo) addNodeReportedAllocations(allocation *si.Allocation) (*AllocationInfo, error) {
	return pi.addNewAllocationInternal(&commonevents.AllocationProposal{
		NodeID:            allocation.NodeID,
		ApplicationID:     allocation.ApplicationID,
		QueueName:         allocation.QueueName,
		AllocatedResource: resources.NewResourceFromProto(allocation.ResourcePerAlloc),
		AllocationKey:     allocation.AllocationKey,
		Tags:              allocation.AllocationTags,
		Priority:          allocation.Priority,
	}, true)
}

// Remove a node from the partition.
// This locks the partition and calls the internal unlocked version.
func (pi *PartitionInfo) RemoveNode(nodeID string) []*AllocationInfo {
	pi.lock.Lock()
	defer pi.lock.Unlock()

	return pi.removeNodeInternal(nodeID)
}

// Remove a node from the partition. It returns all removed allocations.
//
// NOTE: this is a lock free call. It should only be called holding the PartitionInfo lock.
// If access outside is needed a locked version must used, see removeNode
func (pi *PartitionInfo) removeNodeInternal(nodeID string) []*AllocationInfo {
	log.Logger().Info("remove node from partition",
		zap.String("nodeID", nodeID),
		zap.String("partition", pi.Name))

	node := pi.nodes[nodeID]
	if node == nil {
		log.Logger().Debug("node was not found",
			zap.String("nodeID", nodeID),
			zap.String("partitionName", pi.Name))
		return nil
	}

	// found the node cleanup the node and all linked data
	released := pi.removeNodeAllocations(node)
	pi.totalPartitionResource = resources.Sub(pi.totalPartitionResource, node.TotalResource)
	pi.Root.MaxResource = pi.totalPartitionResource

	// Remove node from list of tracked nodes
	delete(pi.nodes, nodeID)
	metrics.GetSchedulerMetrics().DecActiveNodes()

	log.Logger().Info("node removed",
		zap.String("partitionName", pi.Name),
		zap.String("nodeID", node.NodeID))
	return released
}

// Remove all allocations that are assigned to a node as part of the node removal. This is not part of the node object
// as updating the applications and queues is the only goal. Applications and queues are not accessible from the node.
// The removed allocations are returned.
func (pi *PartitionInfo) removeNodeAllocations(node *NodeInfo) []*AllocationInfo {
	released := make([]*AllocationInfo, 0)
	// walk over all allocations still registered for this node
	for _, alloc := range node.GetAllAllocations() {
		var queue *QueueInfo = nil
		allocID := alloc.AllocationProto.UUID
		// since we are not locking the node and or application we could have had an update while processing
		// note that we do not return the allocation if the app or allocation is not found and assume that it
		// was already removed
		if app := pi.applications[alloc.ApplicationID]; app != nil {
			// check allocations on the node
			if app.removeAllocation(allocID) == nil {
				log.Logger().Info("allocation is not found, skipping while removing the node",
					zap.String("allocationId", allocID),
					zap.String("appID", app.ApplicationID),
					zap.String("nodeID", node.NodeID))
				continue
			}
			queue = app.leafQueue
		} else {
			log.Logger().Info("app is not found, skipping while removing the node",
				zap.String("appID", alloc.ApplicationID),
				zap.String("nodeID", node.NodeID))
			continue
		}

		// we should never have an error, cache is in an inconsistent state if this happens
		if queue != nil {
			if err := queue.DecAllocatedResource(alloc.AllocatedResource); err != nil {
				log.Logger().Warn("failed to release resources from queue",
					zap.String("appID", alloc.ApplicationID),
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

// Add a new application to the partition.
// A new application should not be part of the partition yet. The failure behaviour is managed by the failIfExist flag.
// If the flag is true the add will fail returning an error. If the flag is false the add will not fail if the application
// exists but no change is made.
func (pi *PartitionInfo) addNewApplication(info *ApplicationInfo, failIfExist bool) error {
	pi.lock.Lock()
	defer pi.lock.Unlock()

	log.Logger().Info("adding app to partition",
		zap.String("appID", info.ApplicationID),
		zap.String("queue", info.QueueName),
		zap.String("partitionName", pi.Name))
	if pi.IsDraining() || pi.IsStopped() {
		return fmt.Errorf("partition %s is stopped cannot add a new application %s", pi.Name, info.ApplicationID)
	}

	if app := pi.applications[info.ApplicationID]; app != nil {
		if failIfExist {
			return fmt.Errorf("application %s already exists in partition %s", info.ApplicationID, pi.Name)
		}
		log.Logger().Info("app already exists in partition",
			zap.String("appID", info.ApplicationID),
			zap.String("partitionName", pi.Name))
		return nil
	}

	// queue is checked later and overwritten based on placement rules
	info.leafQueue = pi.getQueue(info.QueueName)
	// Add app to the partition
	pi.applications[info.ApplicationID] = info

	log.Logger().Info("app added to partition",
		zap.String("appID", info.ApplicationID),
		zap.String("partitionName", pi.Name))
	return nil
}

// Get the application object for the application ID as tracked by the partition.
// This will return nil if the application is not part of this partition.
func (pi *PartitionInfo) getApplication(appID string) *ApplicationInfo {
	pi.lock.RLock()
	defer pi.lock.RUnlock()

	return pi.applications[appID]
}

// Get the node object for the node ID as tracked by the partition.
// This will return nil if the node is not part of this partition.
// Visible by tests
func (pi *PartitionInfo) GetNode(nodeID string) *NodeInfo {
	pi.lock.RLock()
	defer pi.lock.RUnlock()

	return pi.nodes[nodeID]
}

// Remove one or more allocations for an application.
// Returns all removed allocations.
// If no specific allocation is specified via a uuid all allocations are removed.
func (pi *PartitionInfo) releaseAllocationsForApplication(toRelease *commonevents.ReleaseAllocation) []*AllocationInfo {
	pi.lock.Lock()
	defer pi.lock.Unlock()

	allocationsToRelease := make([]*AllocationInfo, 0)

	log.Logger().Debug("removing allocation from partition",
		zap.String("partitionName", pi.Name))
	if toRelease == nil {
		log.Logger().Debug("no allocations removed from partition",
			zap.String("partitionName", pi.Name))
		return allocationsToRelease
	}

	// First delete from app
	var queue *QueueInfo = nil
	if app := pi.applications[toRelease.ApplicationID]; app != nil {
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
		queue = app.leafQueue
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
		node := pi.nodes[alloc.AllocationProto.NodeID]
		if node == nil || node.GetAllocation(alloc.AllocationProto.UUID) == nil {
			log.Logger().Info("node is not found for allocation",
				zap.Any("allocation", alloc))
			continue
		}
		node.RemoveAllocation(alloc.AllocationProto.UUID)
		totalReleasedResource.AddTo(alloc.AllocatedResource)
	}

	// this nil check is not really needed as we can only reach here with a queue set, IDE complains without this
	if queue != nil {
		// we should never have an error, cache is in an inconsistent state if this happens
		if err := queue.DecAllocatedResource(totalReleasedResource); err != nil {
			log.Logger().Warn("failed to release resources",
				zap.Any("appID", toRelease.ApplicationID),
				zap.Error(err))
		}
	}

	// Update global allocation list
	for _, alloc := range allocationsToRelease {
		delete(pi.allocations, alloc.AllocationProto.UUID)
	}

	log.Logger().Info("allocation removed",
		zap.Int("numOfAllocationReleased", len(allocationsToRelease)),
		zap.String("partitionName", pi.Name))
	return allocationsToRelease
}

// Add an allocation to the partition/node/application/queue.
// Queue max allocation is not check if the allocation is part of a new node addition (nodeReported == true)
//
// NOTE: this is a lock free call. It should only be called holding the PartitionInfo lock.
// If access outside is needed a locked version must used, see addNewAllocation
func (pi *PartitionInfo) addNewAllocationInternal(alloc *commonevents.AllocationProposal, nodeReported bool) (*AllocationInfo, error) {
	log.Logger().Debug("adding allocation",
		zap.String("partitionName", pi.Name))

	if pi.IsStopped() {
		return nil, fmt.Errorf("partition %s is stopped cannot add new allocation %s", pi.Name, alloc.AllocationKey)
	}

	// Check if allocation violates any resource restriction, or allocate on a
	// non-existent applications or nodes.
	var node *NodeInfo
	var app *ApplicationInfo
	var queue *QueueInfo
	var ok bool

	if node, ok = pi.nodes[alloc.NodeID]; !ok {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, fmt.Errorf("failed to find node %s", alloc.NodeID)
	}

	if app, ok = pi.applications[alloc.ApplicationID]; !ok {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, fmt.Errorf("failed to find application %s", alloc.ApplicationID)
	}

	// allocation inherits the app queue as the source of truth
	if queue = pi.getQueue(app.QueueName); queue == nil || !queue.IsLeafQueue() {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, fmt.Errorf("queue does not exist or is not a leaf queue %s", alloc.QueueName)
	}

	// check the node status again
	if !node.IsSchedulable() {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, fmt.Errorf("node %s is not in schedulable state", node.NodeID)
	}

	// Does the new allocation exceed the node's available resource?
	if !node.canAllocate(alloc.AllocatedResource) {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, fmt.Errorf("cannot allocate resource [%v] for application %s on "+
			"node %s because request exceeds available resources, used [%v] node limit [%v]",
			alloc.AllocatedResource, alloc.ApplicationID, node.NodeID, node.GetAllocatedResource(), node.TotalResource)
	}

	// If the new allocation goes beyond the queue's max resource (recursive)?
	// Only check if it is allocated not when it is node reported.
	if err := queue.IncAllocatedResource(alloc.AllocatedResource, nodeReported); err != nil {
		metrics.GetSchedulerMetrics().IncSchedulingError()
		return nil, fmt.Errorf("cannot allocate resource from application %s: %v ",
			alloc.ApplicationID, err)
	}

	// Start allocation
	allocationUUID := pi.getNewAllocationUUID()
	allocation := NewAllocationInfo(allocationUUID, alloc)

	node.AddAllocation(allocation)

	app.addAllocation(allocation)

	pi.allocations[allocation.AllocationProto.UUID] = allocation

	log.Logger().Debug("added allocation",
		zap.String("allocationUid", allocationUUID),
		zap.String("partitionName", pi.Name))
	return allocation, nil
}

// Add a new allocation to the partition.
// This is the locked version which calls addNewAllocationInternal() inside a partition lock.
func (pi *PartitionInfo) addNewAllocation(proposal *commonevents.AllocationProposal) (*AllocationInfo, error) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	return pi.addNewAllocationInternal(proposal, false)
}

// Generate a new uuid for the allocation.
// This is guaranteed to return a unique ID for this partition.
func (pi *PartitionInfo) getNewAllocationUUID() string {
	// Retry to make sure uuid is correct
	for {
		allocationUUID := uuid.NewV4().String()
		if pi.allocations[allocationUUID] == nil {
			return allocationUUID
		}
	}
}

// Remove a rejected application from the partition.
// This is just a cleanup, the app has not been scheduled yet.
func (pi *PartitionInfo) RemoveRejectedApp(appID string) {
	pi.lock.Lock()
	defer pi.lock.Unlock()

	log.Logger().Debug("removing rejected app from partition",
		zap.String("appID", appID),
		zap.String("partitionName", pi.Name))
	// Remove app from cache there is nothing to be cleaned up
	delete(pi.applications, appID)
}

// Remove the application from the partition.
// This will also release all the allocations for application from the queue and nodes.
func (pi *PartitionInfo) RemoveApplication(appID string) (*ApplicationInfo, []*AllocationInfo) {
	pi.lock.Lock()
	defer pi.lock.Unlock()

	log.Logger().Debug("removing app from partition",
		zap.String("appID", appID),
		zap.String("partitionName", pi.Name))

	app := pi.applications[appID]
	if app == nil {
		log.Logger().Warn("app not found partition",
			zap.String("appID", appID),
			zap.String("partitionName", pi.Name))
		return nil, make([]*AllocationInfo, 0)
	}
	// Save the total allocated resources of the application.
	// Might need to base this on calculation of the real removed resources.
	totalAppAllocated := app.GetAllocatedResource()

	// Remove all allocations from the application
	allocations := app.removeAllAllocations()

	// Remove all allocations from nodes/queue
	if len(allocations) != 0 {
		for _, alloc := range allocations {
			currentUUID := alloc.AllocationProto.UUID
			log.Logger().Warn("removing allocations",
				zap.String("appID", appID),
				zap.String("allocationId", currentUUID))
			// Remove from partition cache
			if globalAlloc := pi.allocations[currentUUID]; globalAlloc == nil {
				log.Logger().Warn("unknown allocation: not found in global cache",
					zap.String("appID", appID),
					zap.String("allocationId", currentUUID))
				continue
			} else {
				delete(pi.allocations, currentUUID)
			}

			// Remove from node
			node := pi.nodes[alloc.AllocationProto.NodeID]
			if node == nil {
				log.Logger().Warn("unknown node: not found in active node list",
					zap.String("appID", appID),
					zap.String("nodeID", alloc.AllocationProto.NodeID))
				continue
			}
			if nodeAlloc := node.RemoveAllocation(currentUUID); nodeAlloc == nil {
				log.Logger().Warn("allocation not found on node",
					zap.String("appID", appID),
					zap.String("allocationId", currentUUID),
					zap.String("nodeID", alloc.AllocationProto.NodeID))
				continue
			}
		}

		// we should never have an error, cache is in an inconsistent state if this happens
		queue := app.leafQueue
		if queue != nil {
			if err := queue.DecAllocatedResource(totalAppAllocated); err != nil {
				log.Logger().Error("failed to release resources for app",
					zap.String("appID", app.ApplicationID),
					zap.Error(err))
			}
		}
	}
	// Remove app from cache now that everything is cleaned up
	delete(pi.applications, appID)

	log.Logger().Info("app removed from partition",
		zap.String("appID", app.ApplicationID),
		zap.String("partitionName", app.Partition),
		zap.Any("resourceReleased", totalAppAllocated))

	return app, allocations
}

// Return a copy of all the nodes registers to this partition
func (pi *PartitionInfo) CopyNodeInfos() []*NodeInfo {
	pi.lock.RLock()
	defer pi.lock.RUnlock()

	out := make([]*NodeInfo, len(pi.nodes))

	var i = 0
	for _, v := range pi.nodes {
		out[i] = v
		i++
	}

	return out
}

func checkAndSetResource(resource *resources.Resource) string {
	if resource != nil {
		return strings.Trim(resource.String(), "map")
	}
	return ""
}

// TODO fix this:
// should only return one element, only a root queue
// remove hard coded values and unknown AbsUsedCapacity
// map status to the draining flag
func (pi *PartitionInfo) GetQueueInfos() []dao.QueueDAOInfo {
	pi.lock.RLock()
	defer pi.lock.RUnlock()

	var queueInfos []dao.QueueDAOInfo

	info := dao.QueueDAOInfo{}
	info.QueueName = pi.Root.Name
	info.Status = "RUNNING"
	info.Capacities = dao.QueueCapacity{
		Capacity:        checkAndSetResource(pi.Root.GuaranteedResource),
		MaxCapacity:     checkAndSetResource(pi.Root.MaxResource),
		UsedCapacity:    checkAndSetResource(pi.Root.GetAllocatedResource()),
		AbsUsedCapacity: "20",
	}
	info.ChildQueues = GetChildQueueInfos(pi.Root)
	queueInfos = append(queueInfos, info)

	return queueInfos
}

// TODO fix this:
// should only return one element, only a root queue
// remove hard coded values and unknown AbsUsedCapacity
// map status to the draining flag
func GetChildQueueInfos(info *QueueInfo) []dao.QueueDAOInfo {
	var infos []dao.QueueDAOInfo
	for _, v := range info.children {
		queue := dao.QueueDAOInfo{}
		queue.QueueName = v.Name
		queue.Status = "RUNNING"
		queue.Capacities = dao.QueueCapacity{
			Capacity:        checkAndSetResource(v.GuaranteedResource),
			MaxCapacity:     checkAndSetResource(v.MaxResource),
			UsedCapacity:    checkAndSetResource(v.GetAllocatedResource()),
			AbsUsedCapacity: "20",
		}
		queue.ChildQueues = GetChildQueueInfos(v)
		infos = append(infos, queue)
	}

	return infos
}

func (pi *PartitionInfo) GetTotalApplicationCount() int {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	return len(pi.applications)
}

func (pi *PartitionInfo) GetTotalAllocationCount() int {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	return len(pi.allocations)
}

func (pi *PartitionInfo) GetTotalNodeCount() int {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	return len(pi.nodes)
}

func (pi *PartitionInfo) GetApplications() []*ApplicationInfo {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	var appList []*ApplicationInfo
	for _, app := range pi.applications {
		appList = append(appList, app)
	}
	return appList
}

// Get the queue from the structure based on the fully qualified name.
// Wrapper around the unlocked version getQueue()
func (pi *PartitionInfo) GetQueue(name string) *QueueInfo {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	return pi.getQueue(name)
}

// Get the queue from the structure based on the fully qualified name.
// The name is not syntax checked and must be valid.
// Returns nil if the queue is not found otherwise the queue object.
//
// NOTE: this is a lock free call. It should only be called holding the PartitionInfo lock.
func (pi *PartitionInfo) getQueue(name string) *QueueInfo {
	// start at the root
	queue := pi.Root
	part := strings.Split(strings.ToLower(name), DOT)
	// short circuit the root queue
	if len(part) == 1 {
		return queue
	}
	// walk over the parts going down towards the requested queue
	for i := 1; i < len(part); i++ {
		// if child not found break out and return
		if queue = queue.children[part[i]]; queue == nil {
			break
		}
	}
	return queue
}

// Update the queues in the partition based on the reloaded and checked config
func (pi *PartitionInfo) updatePartitionDetails(partition configs.PartitionConfig) error {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	// update preemption needed flag
	pi.isPreemptable = partition.Preemption.Enabled
	// start at the root: there is only one queue
	queueConf := partition.Queues[0]
	root := pi.getQueue(queueConf.Name)
	err := root.updateQueueProps(queueConf)
	if err != nil {
		return err
	}
	return pi.updateQueues(queueConf.Queues, root)
}

// Update the passed in queues and then do this recursively for the children
func (pi *PartitionInfo) updateQueues(config []configs.QueueConfig, parent *QueueInfo) error {
	// get the name of the passed in queue
	parentPath := parent.GetQueuePath() + DOT
	// keep track of which children we have updated
	visited := map[string]bool{}
	// walk over the queues recursively
	for _, queueConfig := range config {
		pathName := parentPath + queueConfig.Name
		queue := pi.getQueue(pathName)
		var err error
		if queue == nil {
			queue, err = NewManagedQueue(queueConfig, parent)
		} else {
			err = queue.updateQueueProps(queueConfig)
		}
		if err != nil {
			return err
		}
		err = pi.updateQueues(queueConfig.Queues, queue)
		if err != nil {
			return err
		}
		visited[queueConfig.Name] = true
	}
	// remove all children that were not visited
	for childName, childQueue := range parent.children {
		if !visited[childName] {
			childQueue.MarkQueueForRemoval()
		}
	}
	return nil
}

// Mark the partition  for removal from the system.
// This can be executed multiple times and is only effective the first time.
func (pi *PartitionInfo) MarkPartitionForRemoval() {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	if err := pi.HandlePartitionEvent(Remove); err != nil {
		log.Logger().Error("failed to mark partition for deletion",
			zap.String("partitionName", pi.Name),
			zap.Error(err))
	}
}

// The partition has been removed from the configuration and must be removed.
// This is the cleanup triggered by the exiting scheduler partition. Just unlinking from the cluster should be enough.
// All other clean up is triggered from the scheduler
func (pi *PartitionInfo) Remove() {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	// safeguard
	if pi.IsDraining() || pi.IsStopped() {
		pi.clusterInfo.removePartition(pi.Name)
	}
}

// Is the partition marked for deletion and can only handle existing application requests.
// No new applications will be accepted.
func (pi *PartitionInfo) IsDraining() bool {
	return pi.stateMachine.Current() == Draining.String()
}

func (pi *PartitionInfo) IsRunning() bool {
	return pi.stateMachine.Current() == Active.String()
}

func (pi *PartitionInfo) IsStopped() bool {
	return pi.stateMachine.Current() == Stopped.String()
}

// Create the new queue that is returned from a rule.
// It creates a queue with all parents needed.
func (pi *PartitionInfo) CreateQueues(queueName string) error {
	if !strings.HasPrefix(queueName, configs.RootQueue+DOT) {
		return fmt.Errorf("cannot create queue which is not qualified '%s'", queueName)
	}
	log.Logger().Debug("Creating new queue structure", zap.String("queueName", queueName))
	// two step creation process: first check then really create them
	// start at the root, which we know exists and is a parent
	current := queueName
	var toCreate []string
	parent := pi.getQueue(current)
	log.Logger().Debug("Checking queue creation")
	for parent == nil {
		toCreate = append(toCreate, current[strings.LastIndex(current, DOT)+1:])
		current = current[0:strings.LastIndex(current, DOT)]
		// check if the queue exist
		parent = pi.getQueue(current)
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
		parent, err = NewUnmanagedQueue(toCreate[i], i == 0, parent)
		if err != nil {
			log.Logger().Warn("Queue auto create failed unexpected",
				zap.String("queueName", queueName),
				zap.Error(err))
		}
	}
	return nil
}

func (pi *PartitionInfo) convertUGI(ugi *si.UserGroupInformation) (security.UserGroup, error) {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	return pi.userGroupCache.ConvertUGI(ugi)
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
func (pi *PartitionInfo) CalculateNodesResourceUsage() map[string][]int {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	mapResult := make(map[string][]int)
	for _, node := range pi.nodes {
		for name, total := range node.TotalResource.Resources {
			if float64(total) > 0 {
				resourceAllocated := float64(node.allocatedResource.Resources[name])
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
