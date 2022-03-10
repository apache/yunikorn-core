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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/handler"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects"
	siCommon "github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

const disableReservation = "DISABLE_RESERVATION"

type ClusterContext struct {
	partitions     map[string]*PartitionContext
	policyGroup    string
	rmEventHandler handler.EventHandler

	// config values that change scheduling behaviour
	needPreemption      bool
	reservationDisabled bool

	rmInfos   map[string]*RMInformation
	startTime time.Time

	sync.RWMutex
}

type RMInformation struct {
	RMBuildInformation map[string]string
}

// Create a new cluster context to be used outside of the event system.
// test only
func NewClusterContext(rmID, policyGroup string) (*ClusterContext, error) {
	// load the config this returns a validated configuration
	conf, err := configs.SchedulerConfigLoader(policyGroup)
	if err != nil {
		return nil, err
	}
	// create the context and set the policyGroup
	cc := &ClusterContext{
		partitions:          make(map[string]*PartitionContext),
		policyGroup:         policyGroup,
		reservationDisabled: common.GetBoolEnvVar(disableReservation, false),
		startTime:           time.Now(),
	}
	// If reservation is turned off set the reservation delay to the maximum duration defined.
	// The time package does not export maxDuration so use the equivalent from the math package.
	if cc.reservationDisabled {
		objects.SetReservationDelay(math.MaxInt64)
	}
	err = cc.updateSchedulerConfig(conf, rmID)
	if err != nil {
		return nil, err
	}
	// update the global config
	configs.ConfigContext.Set(policyGroup, conf)
	return cc, nil
}

func newClusterContext() *ClusterContext {
	cc := &ClusterContext{
		partitions:          make(map[string]*PartitionContext),
		reservationDisabled: common.GetBoolEnvVar(disableReservation, false),
		startTime:           time.Now(),
	}
	// If reservation is turned off set the reservation delay to the maximum duration defined.
	// The time package does not export maxDuration so use the equivalent from the math package.
	if cc.reservationDisabled {
		objects.SetReservationDelay(math.MaxInt64)
	}
	return cc
}

func (cc *ClusterContext) setEventHandler(rmHandler handler.EventHandler) {
	cc.rmEventHandler = rmHandler
}

// schedule is the main scheduling routine.
// Process each partition in the scheduler, walk over each queue and app to check if anything can be scheduled.
// This can be forked into a go routine per partition if needed to increase parallel allocations.
// Returns true if an allocation was able to be scheduled.
func (cc *ClusterContext) schedule() bool {
	// schedule each partition defined in the cluster
	activity := false
	for _, psc := range cc.GetPartitionMapClone() {
		// if there are no resources in the partition just skip
		if psc.root.GetMaxResource() == nil {
			continue
		}
		// a stopped partition does not allocate
		if psc.isStopped() {
			continue
		}
		// try reservations first
		schedulingStart := time.Now()
		alloc := psc.tryReservedAllocate()
		if alloc == nil {
			// placeholder replacement second
			alloc = psc.tryPlaceholderAllocate()
			// nothing reserved that can be allocated try normal allocate
			if alloc == nil {
				alloc = psc.tryAllocate()
			}
		}
		if alloc != nil {
			metrics.GetSchedulerMetrics().ObserveSchedulingLatency(schedulingStart)
			if alloc.Result == objects.Replaced {
				// communicate the removal to the RM
				cc.notifyRMAllocationReleased(psc.RmID, alloc.Releases, si.TerminationType_PLACEHOLDER_REPLACED, "replacing UUID: "+alloc.UUID)
			} else {
				cc.notifyRMNewAllocation(psc.RmID, alloc)
			}
			activity = true
		}
	}
	return activity
}

func (cc *ClusterContext) processRMRegistrationEvent(event *rmevent.RMRegistrationEvent) {
	cc.Lock()
	defer cc.Unlock()
	rmID := event.Registration.RmID
	// we should not have any partitions set at this point
	if len(cc.partitions) > 0 {
		event.Channel <- &rmevent.Result{
			Reason:    fmt.Sprintf("RM %s has been registered before, active partitions %d", rmID, len(cc.partitions)),
			Succeeded: false,
		}
		return
	}
	policyGroup := event.Registration.PolicyGroup
	// load the config this returns a validated configuration
	conf, err := configs.SchedulerConfigLoader(policyGroup)
	if err != nil {
		event.Channel <- &rmevent.Result{Succeeded: false, Reason: err.Error()}
		return
	}
	err = cc.updateSchedulerConfig(conf, rmID)
	if err != nil {
		event.Channel <- &rmevent.Result{Succeeded: false, Reason: err.Error()}
		return
	}

	// update global scheduler configs, set the policyGroup for this cluster
	cc.policyGroup = policyGroup
	configs.ConfigContext.Set(policyGroup, conf)

	// store the build information of RM
	cc.SetRMInfos(rmID, event.Registration.BuildInfo)

	// Done, notify channel
	event.Channel <- &rmevent.Result{
		Succeeded: true,
	}
}

func (cc *ClusterContext) processRMConfigUpdateEvent(event *rmevent.RMConfigUpdateEvent) {
	cc.Lock()
	defer cc.Unlock()
	rmID := event.RmID
	// need to enhance the check to support multiple RMs
	if len(cc.partitions) == 0 {
		event.Channel <- &rmevent.Result{
			Reason:    fmt.Sprintf("RM %s has no active partitions, make sure it is registered", rmID),
			Succeeded: false,
		}
		return
	}
	// load the config this returns a validated configuration
	conf, err := configs.SchedulerConfigLoader(cc.policyGroup)
	if err != nil {
		event.Channel <- &rmevent.Result{Succeeded: false, Reason: err.Error()}
		return
	}
	err = cc.updateSchedulerConfig(conf, rmID)
	if err != nil {
		event.Channel <- &rmevent.Result{Succeeded: false, Reason: err.Error()}
		return
	}
	// Done, notify channel
	event.Channel <- &rmevent.Result{
		Succeeded: true,
	}
	// update global scheduler configs
	configs.ConfigContext.Set(cc.policyGroup, conf)
}

func (cc *ClusterContext) handleRMUpdateNodeEvent(event *rmevent.RMUpdateNodeEvent) {
	request := event.Request
	cc.processNodes(request)
}

// processNodes process all node requests: add, remove and update.
func (cc *ClusterContext) processNodes(request *si.NodeRequest) {
	nodeCount := len(request.GetNodes())
	if nodeCount > 0 {
		acceptedNodes := make([]*si.AcceptedNode, 0)
		rejectedNodes := make([]*si.RejectedNode, 0)
		for _, nodeInfo := range request.GetNodes() {
			// nil nodes must be skipped
			if nodeInfo == nil {
				continue
			}
			switch nodeInfo.Action {
			case si.NodeInfo_CREATE:
				err := cc.addNode(nodeInfo, nodeCount)
				if err == nil {
					acceptedNodes = append(acceptedNodes, &si.AcceptedNode{
						NodeID: nodeInfo.NodeID,
					})
				} else {
					rejectedNodes = append(rejectedNodes, &si.RejectedNode{
						NodeID: nodeInfo.NodeID,
						Reason: err.Error(),
					})
				}
			default:
				cc.updateNode(nodeInfo)
			}
		}

		if len(acceptedNodes) > 0 || len(rejectedNodes) > 0 {
			// inform the RM which nodes have been accepted/rejected
			cc.rmEventHandler.HandleEvent(
				&rmevent.RMNodeUpdateEvent{
					RmID:          request.RmID,
					AcceptedNodes: acceptedNodes,
					RejectedNodes: rejectedNodes,
				})
		}
	}
}

// Called when a RM re-registers. This triggers a full clean up.
// Registration expects everything to be clean.
func (cc *ClusterContext) removePartitionsByRMID(event *rmevent.RMPartitionsRemoveEvent) {
	cc.Lock()
	defer cc.Unlock()
	partitionToRemove := make(map[string]bool)

	// Just remove corresponding partitions
	for k, partition := range cc.partitions {
		if partition.RmID == event.RmID {
			partition.partitionManager.Stop()
			partitionToRemove[k] = true
		}
	}

	for partitionName := range partitionToRemove {
		delete(cc.partitions, partitionName)
	}
	// Done, notify channel
	event.Channel <- &rmevent.Result{
		Succeeded: true,
	}
}

// Locked version of the configuration update called from the webservice
// NOTE: this call assumes one RM which is registered and uses that RM for the updates
func (cc *ClusterContext) UpdateSchedulerConfig(conf *configs.SchedulerConfig) error {
	cc.Lock()
	defer cc.Unlock()
	// hack around the missing rmID
	for _, pi := range cc.partitions {
		rmID := pi.RmID
		log.Logger().Debug("Assuming one RM on config update call from webservice",
			zap.String("rmID", rmID))
		if err := cc.updateSchedulerConfig(conf, rmID); err != nil {
			return err
		}
		// update global scheduler configs
		configs.ConfigContext.Set(cc.policyGroup, conf)
		return nil
	}
	return fmt.Errorf("RM has no active partitions, make sure it is registered")
}

// Locked version of the configuration update called outside of event system.
// Updates the current config via the config loader.
// Used in test only, normal updates use the internal call and the webservice must use the UpdateSchedulerConfig
func (cc *ClusterContext) UpdateRMSchedulerConfig(rmID string) error {
	cc.Lock()
	defer cc.Unlock()
	if len(cc.partitions) == 0 {
		return fmt.Errorf("RM %s has no active partitions, make sure it is registered", rmID)
	}
	// load the config this returns a validated configuration
	conf, err := configs.SchedulerConfigLoader(cc.policyGroup)
	if err != nil {
		return err
	}
	err = cc.updateSchedulerConfig(conf, rmID)
	if err != nil {
		return err
	}
	// update global scheduler configs
	configs.ConfigContext.Set(cc.policyGroup, conf)
	return nil
}

// Update or set the scheduler config. If the partitions list does not contain the specific partition it creates a new
// partition otherwise it performs an update.
// Called if the config file is updated, indirectly when the webservice is called.
// During tests this is called outside of the even system to init.
// unlocked call must only be called holding the ClusterContext lock
func (cc *ClusterContext) updateSchedulerConfig(conf *configs.SchedulerConfig, rmID string) error {
	visited := map[string]bool{}
	var err error
	// walk over the partitions in the config: update existing ones
	for _, p := range conf.Partitions {
		partitionName := common.GetNormalizedPartitionName(p.Name, rmID)
		p.Name = partitionName
		part, ok := cc.partitions[p.Name]
		if ok {
			// make sure the new info passes all checks
			_, err = newPartitionContext(p, rmID, nil)
			if err != nil {
				return err
			}
			// checks passed perform the real update
			log.Logger().Info("updating partitions", zap.String("partitionName", partitionName))
			err = part.updatePartitionDetails(p)
			if err != nil {
				return err
			}
		} else {
			// not found: new partition, no checks needed
			log.Logger().Info("added partitions", zap.String("partitionName", partitionName))

			part, err = newPartitionContext(p, rmID, cc)
			if err != nil {
				return err
			}
			go part.partitionManager.Run()
			cc.partitions[partitionName] = part
		}
		// add it to the partitions to update
		visited[p.Name] = true
	}

	// get the removed partitions, mark them as deleted
	for _, part := range cc.partitions {
		if !visited[part.Name] {
			part.partitionManager.Stop()
			log.Logger().Info("marked partition for removal",
				zap.String("partitionName", part.Name))
		}
	}

	return nil
}

// Get the config name.
func (cc *ClusterContext) GetPolicyGroup() string {
	cc.RLock()
	defer cc.RUnlock()
	return cc.policyGroup
}

func (cc *ClusterContext) GetStartTime() time.Time {
	cc.RLock()
	defer cc.RUnlock()
	return cc.startTime
}

func (cc *ClusterContext) GetRMInfoMapClone() map[string]*RMInformation {
	cc.RLock()
	defer cc.RUnlock()

	newMap := make(map[string]*RMInformation)
	for k, v := range cc.rmInfos {
		newMap[k] = v
	}
	return newMap
}

func (cc *ClusterContext) GetPartitionMapClone() map[string]*PartitionContext {
	cc.RLock()
	defer cc.RUnlock()

	newMap := make(map[string]*PartitionContext)
	for k, v := range cc.partitions {
		newMap[k] = v
	}
	return newMap
}

func (cc *ClusterContext) GetPartition(partitionName string) *PartitionContext {
	cc.RLock()
	defer cc.RUnlock()
	return cc.partitions[partitionName]
}

func (cc *ClusterContext) GetPartitionWithoutClusterID(partitionName string) *PartitionContext {
	cc.RLock()
	defer cc.RUnlock()
	for k, v := range cc.partitions {
		if len(partitionName) > 0 && common.GetPartitionNameWithoutClusterID(k) == partitionName {
			return v
		}
	}
	return nil
}

// Get the scheduling application based on the ID from the partition.
// Returns nil if the partition or app cannot be found.
// Visible for tests
func (cc *ClusterContext) GetApplication(appID, partitionName string) *objects.Application {
	cc.RLock()
	defer cc.RUnlock()

	if partition := cc.partitions[partitionName]; partition != nil {
		return partition.getApplication(appID)
	}

	return nil
}

// Get the scheduling queue based on the queue path name from the partition.
// Returns nil if the partition or queue cannot be found.
// Visible for tests
func (cc *ClusterContext) GetQueue(queueName string, partitionName string) *objects.Queue {
	cc.RLock()
	defer cc.RUnlock()

	if partition := cc.partitions[partitionName]; partition != nil {
		return partition.GetQueue(queueName)
	}

	return nil
}

// Return the list of reservations for the partition.
// Returns nil if the partition cannot be found or an empty map if there are no reservations
// Visible for tests
func (cc *ClusterContext) GetReservations(partitionName string) map[string]int {
	cc.RLock()
	defer cc.RUnlock()

	if partition := cc.partitions[partitionName]; partition != nil {
		return partition.getReservations()
	}

	return nil
}

// Process the application update. Add and remove applications from the partitions.
// Lock free call, all updates occur on the underlying partition which is locked, or via events.
func (cc *ClusterContext) handleRMUpdateApplicationEvent(event *rmevent.RMUpdateApplicationEvent) {
	request := event.Request
	if len(request.New) == 0 && len(request.Remove) == 0 {
		return
	}
	acceptedApps := make([]*si.AcceptedApplication, 0)
	rejectedApps := make([]*si.RejectedApplication, 0)

	for _, app := range request.New {
		partition := cc.GetPartition(app.PartitionName)
		if partition == nil {
			msg := fmt.Sprintf("Failed to add application %s to partition %s, partition doesn't exist", app.ApplicationID, app.PartitionName)
			rejectedApps = append(rejectedApps, &si.RejectedApplication{
				ApplicationID: app.ApplicationID,
				Reason:        msg,
			})
			log.Logger().Error("Failed to add application to non existing partition",
				zap.String("applicationID", app.ApplicationID),
				zap.String("partitionName", app.PartitionName))
			continue
		}
		// convert and resolve the user: cache can be set per partition
		// need to do this before we create the application
		ugi, err := partition.convertUGI(app.Ugi)
		if err != nil {
			rejectedApps = append(rejectedApps, &si.RejectedApplication{
				ApplicationID: app.ApplicationID,
				Reason:        err.Error(),
			})
			partition.AddRejectedApplication(objects.NewApplication(app, ugi, cc.rmEventHandler, request.RmID), err.Error())
			log.Logger().Error("Failed to add application to partition (user rejected)",
				zap.String("applicationID", app.ApplicationID),
				zap.String("partitionName", app.PartitionName),
				zap.Error(err))
			continue
		}
		// create a new app object and add it to the partition (partition logs details)
		schedApp := objects.NewApplication(app, ugi, cc.rmEventHandler, request.RmID)
		if err = partition.AddApplication(schedApp); err != nil {
			rejectedApps = append(rejectedApps, &si.RejectedApplication{
				ApplicationID: app.ApplicationID,
				Reason:        err.Error(),
			})
			partition.AddRejectedApplication(schedApp, err.Error())
			log.Logger().Error("Failed to add application to partition (placement rejected)",
				zap.String("applicationID", app.ApplicationID),
				zap.String("partitionName", app.PartitionName),
				zap.Error(err))
			continue
		}
		acceptedApps = append(acceptedApps, &si.AcceptedApplication{
			ApplicationID: schedApp.ApplicationID,
		})
		log.Logger().Info("Added application to partition",
			zap.String("applicationID", app.ApplicationID),
			zap.String("partitionName", app.PartitionName),
			zap.String("requested queue", app.QueueName),
			zap.String("placed queue", schedApp.GetQueuePath()))
	}

	// Respond to RMProxy with accepted and rejected apps if needed
	if len(rejectedApps) > 0 || len(acceptedApps) > 0 {
		cc.rmEventHandler.HandleEvent(
			&rmevent.RMApplicationUpdateEvent{
				RmID:                 request.RmID,
				AcceptedApplications: acceptedApps,
				RejectedApplications: rejectedApps,
			})
	}
	// Update metrics with removed applications
	if len(request.Remove) > 0 {
		metrics.GetSchedulerMetrics().SubTotalApplicationsRunning(len(request.Remove))
		// ToDO: need to improve this once we have state in YuniKorn for apps.
		metrics.GetSchedulerMetrics().AddTotalApplicationsCompleted(len(request.Remove))
		for _, app := range request.Remove {
			partition := cc.GetPartition(app.PartitionName)
			if partition == nil {
				continue
			}
			allocations := partition.removeApplication(app.ApplicationID)
			if len(allocations) > 0 {
				cc.notifyRMAllocationReleased(partition.RmID, allocations, si.TerminationType_STOPPED_BY_RM,
					fmt.Sprintf("Application %s Removed", app.ApplicationID))
			}
			log.Logger().Info("Application removed from partition",
				zap.String("applicationID", app.ApplicationID),
				zap.String("partitionName", app.PartitionName),
				zap.Int("allocations released", len(allocations)))
		}
	}
}

func (cc *ClusterContext) NeedPreemption() bool {
	cc.RLock()
	defer cc.RUnlock()

	return cc.needPreemption
}

// Callback from the partition manager to finalise the removal of the partition
func (cc *ClusterContext) removePartition(partitionName string) {
	cc.Lock()
	defer cc.Unlock()

	delete(cc.partitions, partitionName)
}

// addNode adds a new node to the cluster enforcing just one unlimited node in the cluster.
// nil nodeInfo objects must be filtered out before calling this function
func (cc *ClusterContext) addNode(nodeInfo *si.NodeInfo, nodeCount int) error {
	sn := objects.NewNode(nodeInfo)
	if !sn.IsReady() {
		metrics.GetSchedulerMetrics().IncUnhealthyNodes()
	}
	// if this node is unlimited, check the following things:
	// 1. if reservation is enabled, reject the node
	// 2. any other nodes in the list, reject the node
	if sn.IsUnlimited() {
		if nodeCount > 1 {
			return fmt.Errorf("more than one node to be added cannot register a unlimited node")
		}
		if !cc.reservationDisabled {
			return fmt.Errorf("reservations must be disabled when registering a unlimited node")
		}
	}
	partition := cc.GetPartition(sn.Partition)
	if partition == nil {
		err := fmt.Errorf("failed to find partition %s for new node %s", sn.Partition, sn.NodeID)
		//nolint: TODO assess impact of partition metrics (this never hit the partition)
		metrics.GetSchedulerMetrics().IncFailedNodes()
		log.Logger().Error("Failed to add node to non existing partition",
			zap.String("nodeID", sn.NodeID),
			zap.String("partitionName", sn.Partition))
		return err
	}
	// check that we only have one unlimited node and never a mix of unlimited and limited
	if partition.hasUnlimitedNode() {
		return fmt.Errorf("the partition has an unlimited node registered, registering other nodes is forbidden")
	}
	if sn.IsUnlimited() && partition.nodes.GetNodeCount() > 0 {
		return fmt.Errorf("the unlimited node should be registered first, there are other nodes registered in the partition")
	}

	existingAllocations := cc.convertAllocations(nodeInfo.ExistingAllocations)
	err := partition.AddNode(sn, existingAllocations)
	if err != nil {
		wrapped := fmt.Errorf("failure while adding new node, node rejected with error: %w", err)
		log.Logger().Error("Failed to add node to partition (rejected)",
			zap.String("nodeID", sn.NodeID),
			zap.String("partitionName", sn.Partition),
			zap.Error(err))
		return wrapped
	}
	log.Logger().Info("successfully added node",
		zap.String("nodeID", sn.NodeID),
		zap.String("partition", sn.Partition))
	return nil
}

// updateNode updates an existing node of the cluster.
// nil nodeInfo objects must be filtered out before calling this function
func (cc *ClusterContext) updateNode(nodeInfo *si.NodeInfo) {
	var partition *PartitionContext
	if p, ok := nodeInfo.Attributes[siCommon.NodePartition]; ok {
		partition = cc.GetPartition(p)
	} else {
		log.Logger().Error("node partition not specified",
			zap.String("nodeID", nodeInfo.NodeID),
			zap.String("nodeAction", nodeInfo.Action.String()))
		return
	}

	if partition == nil {
		log.Logger().Error("Failed to update node on non existing partition",
			zap.String("nodeID", nodeInfo.NodeID),
			zap.String("partitionName", nodeInfo.Attributes[siCommon.NodePartition]),
			zap.String("nodeAction", nodeInfo.Action.String()))
		return
	}

	node := partition.GetNode(nodeInfo.NodeID)
	if node == nil {
		log.Logger().Error("Failed to update non existing node",
			zap.String("nodeID", nodeInfo.NodeID),
			zap.String("partitionName", nodeInfo.Attributes[siCommon.NodePartition]),
			zap.String("nodeAction", nodeInfo.Action.String()))
		return
	}

	switch nodeInfo.Action {
	case si.NodeInfo_UPDATE:
		var newReadyStatus bool
		var err error
		if newReadyStatus, err = strconv.ParseBool(nodeInfo.Attributes[objects.ReadyFlag]); err != nil {
			log.Logger().Error("Could not parse ready attribute, assuming true", zap.Any("attributes", nodeInfo.Attributes))
			newReadyStatus = true
		}

		if node.IsReady() && !newReadyStatus {
			log.Logger().Info("Node has become unhealthy", zap.String("Node ID", node.NodeID))
			metrics.GetSchedulerMetrics().IncUnhealthyNodes()
		}
		if !node.IsReady() && newReadyStatus {
			log.Logger().Info("Node has become healthy", zap.String("Node ID", node.NodeID))
			metrics.GetSchedulerMetrics().DecUnhealthyNodes()
		}
		node.SetReady(newReadyStatus)

		if sr := nodeInfo.SchedulableResource; sr != nil {
			partition.updatePartitionResource(node.SetCapacity(resources.NewResourceFromProto(sr)))
		}
		if or := nodeInfo.OccupiedResource; or != nil {
			node.SetOccupiedResource(resources.NewResourceFromProto(or))
		}
	case si.NodeInfo_DRAIN_NODE:
		if node.IsSchedulable() {
			// set the state to not schedulable
			node.SetSchedulable(false)
			metrics.GetSchedulerMetrics().IncDrainingNodes()
		}
	case si.NodeInfo_DRAIN_TO_SCHEDULABLE:
		if !node.IsSchedulable() {
			metrics.GetSchedulerMetrics().DecDrainingNodes()
			// set the state to schedulable
			node.SetSchedulable(true)
		}
	case si.NodeInfo_DECOMISSION:
		if !node.IsSchedulable() {
			metrics.GetSchedulerMetrics().DecDrainingNodes()
		}
		if !node.IsReady() {
			metrics.GetSchedulerMetrics().DecUnhealthyNodes()
		}
		metrics.GetSchedulerMetrics().IncTotalDecommissionedNodes()
		// set the state to not schedulable then tell the partition to clean up
		node.SetSchedulable(false)
		released, confirmed := partition.removeNode(node.NodeID)
		// notify the shim allocations have been released from node
		if len(released) != 0 {
			cc.notifyRMAllocationReleased(partition.RmID, released, si.TerminationType_STOPPED_BY_RM,
				fmt.Sprintf("Node %s Removed", node.NodeID))
		}
		for _, confirm := range confirmed {
			cc.notifyRMNewAllocation(partition.RmID, confirm)
		}
	default:
		log.Logger().Debug("unknown action for node update",
			zap.String("nodeID", nodeInfo.NodeID),
			zap.String("partitionName", nodeInfo.Attributes[siCommon.NodePartition]),
			zap.String("nodeAction", nodeInfo.Action.String()))
	}
}

// Process an ask and allocation update request.
// - Add new asks and remove released asks for the application(s).
// - Release allocations for the application(s).
// Lock free call, all updates occur on the underlying application which is locked or via events.
func (cc *ClusterContext) handleRMUpdateAllocationEvent(event *rmevent.RMUpdateAllocationEvent) {
	request := event.Request
	if len(request.Asks) != 0 {
		cc.processAsks(request)
	}
	if request.Releases != nil {
		if len(request.Releases.AllocationAsksToRelease) > 0 {
			cc.processAskReleases(request.Releases.AllocationAsksToRelease)
		}
		if len(request.Releases.AllocationsToRelease) > 0 {
			cc.processAllocationReleases(request.Releases.AllocationsToRelease, request.RmID)
		}
	}
}

func (cc *ClusterContext) processAsks(request *si.AllocationRequest) {
	// Send rejects back to RM
	rejectedAsks := make([]*si.RejectedAllocationAsk, 0)

	// Send to scheduler
	for _, siAsk := range request.Asks {
		// try to get ApplicationInfo
		partition := cc.GetPartition(siAsk.PartitionName)
		if partition == nil {
			msg := fmt.Sprintf("Failed to find partition %s, for application %s and allocation %s", siAsk.PartitionName, siAsk.ApplicationID, siAsk.AllocationKey)
			log.Logger().Error("Invalid ask add requested by shim, partition not found",
				zap.String("partition", siAsk.PartitionName),
				zap.String("applicationID", siAsk.ApplicationID),
				zap.String("askKey", siAsk.AllocationKey))
			rejectedAsks = append(rejectedAsks, &si.RejectedAllocationAsk{
				AllocationKey: siAsk.AllocationKey,
				ApplicationID: siAsk.ApplicationID,
				Reason:        msg,
			})
			continue
		}

		// try adding to app
		if err := partition.addAllocationAsk(siAsk); err != nil {
			rejectedAsks = append(rejectedAsks,
				&si.RejectedAllocationAsk{
					AllocationKey: siAsk.AllocationKey,
					ApplicationID: siAsk.ApplicationID,
					Reason:        err.Error(),
				})
			log.Logger().Error("Invalid ask add requested by shim",
				zap.String("partition", siAsk.PartitionName),
				zap.String("applicationID", siAsk.ApplicationID),
				zap.String("askKey", siAsk.AllocationKey),
				zap.Error(err))
		} else {
			if siAsk.Placeholder {
				partition.applications[siAsk.ApplicationID].SetPlaceholderData(siAsk.TaskGroupName, resources.NewResourceFromProto(siAsk.ResourceAsk), common.GetRequiredNodeFromTag(siAsk.Tags))
			}
		}
	}

	// Reject asks returned to RM Proxy for the apps and partitions not found
	if len(rejectedAsks) > 0 {
		cc.rmEventHandler.HandleEvent(&rmevent.RMRejectedAllocationAskEvent{
			RmID:                   request.RmID,
			RejectedAllocationAsks: rejectedAsks,
		})
	}
}

func (cc *ClusterContext) processAskReleases(releases []*si.AllocationAskRelease) {
	for _, toRelease := range releases {
		partition := cc.GetPartition(toRelease.PartitionName)
		if partition == nil {
			log.Logger().Error("Invalid ask release requested by shim, partition not found",
				zap.String("partition", toRelease.PartitionName),
				zap.String("applicationID", toRelease.ApplicationID),
				zap.String("askKey", toRelease.AllocationKey))
			continue
		}
		partition.removeAllocationAsk(toRelease)
	}
}

func (cc *ClusterContext) processAllocationReleases(releases []*si.AllocationRelease, rmID string) {
	for _, toRelease := range releases {
		partition := cc.GetPartition(toRelease.PartitionName)
		if partition != nil {
			allocs, confirmed := partition.removeAllocation(toRelease)
			// notify the RM of the exact released allocations
			if len(allocs) > 0 {
				cc.notifyRMAllocationReleased(rmID, allocs, si.TerminationType_STOPPED_BY_RM, "allocation remove as per RM request")
			}
			// notify the RM of the confirmed allocations (placeholder swap & preemption)
			if confirmed != nil {
				cc.notifyRMNewAllocation(rmID, confirmed)
			}
		}
	}
}

// Convert the si allocation to a proposal to add to the node
func (cc *ClusterContext) convertAllocations(allocations []*si.Allocation) []*objects.Allocation {
	convert := make([]*objects.Allocation, len(allocations))
	for current, allocation := range allocations {
		convert[current] = objects.NewAllocationFromSI(allocation)
	}

	return convert
}

// Create a RM update event to notify RM of new allocations
// Lock free call, all updates occur via events.
func (cc *ClusterContext) notifyRMNewAllocation(rmID string, alloc *objects.Allocation) {
	c := make(chan *rmevent.Result)
	// communicate the allocation to the RM synchronously
	cc.rmEventHandler.HandleEvent(&rmevent.RMNewAllocationsEvent{
		Allocations: []*si.Allocation{alloc.NewSIFromAllocation()},
		RmID:        rmID,
		Channel:     c,
	})
	// Wait from channel
	result := <-c
	if result.Succeeded {
		log.Logger().Debug("Successfully synced shim on new allocation. response: " + result.Reason)
	} else {
		log.Logger().Info("failed to sync shim on new allocation",
			zap.String("Allocation key: ", alloc.AllocationKey))
	}
}

// Create a RM update event to notify RM of released allocations
// Lock free call, all updates occur via events.
func (cc *ClusterContext) notifyRMAllocationReleased(rmID string, released []*objects.Allocation, terminationType si.TerminationType, message string) {
	c := make(chan *rmevent.Result)
	releaseEvent := &rmevent.RMReleaseAllocationEvent{
		ReleasedAllocations: make([]*si.AllocationRelease, 0),
		RmID:                rmID,
		Channel:             c,
	}
	for _, alloc := range released {
		releaseEvent.ReleasedAllocations = append(releaseEvent.ReleasedAllocations, &si.AllocationRelease{
			ApplicationID:   alloc.ApplicationID,
			PartitionName:   alloc.PartitionName,
			UUID:            alloc.UUID,
			TerminationType: terminationType,
			Message:         message,
			AllocationKey:   alloc.AllocationKey,
		})
	}

	cc.rmEventHandler.HandleEvent(releaseEvent)
	// Wait from channel
	result := <-c
	if result.Succeeded {
		log.Logger().Debug("Successfully synced shim on released allocations. response: " + result.Reason)
	} else {
		log.Logger().Info("failed to sync shim on released allocations")
	}
}

// Get a scheduling node based on its name from the partition.
// Returns nil if the partition or node cannot be found.
// Visible for tests
func (cc *ClusterContext) GetNode(nodeID, partitionName string) *objects.Node {
	cc.Lock()
	defer cc.Unlock()

	partition := cc.partitions[partitionName]
	if partition == nil {
		log.Logger().Info("partition not found for scheduling node",
			zap.String("nodeID", nodeID),
			zap.String("partitionName", partitionName))
		return nil
	}
	return partition.GetNode(nodeID)
}

func (cc *ClusterContext) SetRMInfos(rmID string, rmBuildInformation map[string]string) {
	if cc.rmInfos == nil {
		cc.rmInfos = make(map[string]*RMInformation)
	}
	buildInfo := make(map[string]string)
	for k, v := range rmBuildInformation {
		buildInfo[k] = v
	}
	buildInfo["rmId"] = rmID
	cc.rmInfos[rmID] = &RMInformation{
		RMBuildInformation: buildInfo,
	}
}
