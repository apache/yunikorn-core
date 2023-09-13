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

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/handler"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const disableReservation = "DISABLE_RESERVATION"

type ClusterContext struct {
	partitions     map[string]*PartitionContext
	policyGroup    string
	rmEventHandler handler.EventHandler
	uuid           string

	// config values that change scheduling behaviour
	needPreemption      bool
	reservationDisabled bool

	rmInfo    map[string]*RMInformation
	startTime time.Time

	sync.RWMutex

	lastHealthCheckResult *dao.SchedulerHealthDAOInfo
}

type RMInformation struct {
	RMBuildInformation map[string]string
}

// Create a new cluster context to be used outside of the event system.
// test only
func NewClusterContext(rmID, policyGroup string, config []byte) (*ClusterContext, error) {
	// load the config this returns a validated configuration
	conf, err := configs.LoadSchedulerConfigFromByteArray(config)
	if err != nil {
		return nil, err
	}
	// create the context and set the policyGroup
	cc := &ClusterContext{
		partitions:          make(map[string]*PartitionContext),
		policyGroup:         policyGroup,
		reservationDisabled: common.GetBoolEnvVar(disableReservation, false),
		startTime:           time.Now(),
		uuid:                common.GetNewUUID(),
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
		uuid:                common.GetNewUUID(),
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
			if alloc.GetResult() == objects.Replaced {
				// communicate the removal to the RM
				cc.notifyRMAllocationReleased(psc.RmID, alloc.GetReleasesClone(), si.TerminationType_PLACEHOLDER_REPLACED, "replacing uuid: "+alloc.GetUUID())
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
	config := event.Registration.Config
	configs.SetConfigMap(event.Registration.ExtraConfig)

	// load the config this returns a validated configuration
	if len(config) == 0 {
		log.Log(log.SchedContext).Info("No scheduler configuration supplied, using defaults", zap.String("rmID", rmID))
		config = configs.DefaultSchedulerConfig
	}
	conf, err := configs.LoadSchedulerConfigFromByteArray([]byte(config))
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
	cc.SetRMInfo(rmID, event.Registration.BuildInfo)

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

	// set extra configuration
	configs.SetConfigMap(event.ExtraConfig)

	// load the config this returns a validated configuration
	config := event.Config
	if len(config) == 0 {
		log.Log(log.SchedContext).Info("No scheduler configuration supplied, using defaults", zap.String("rmID", rmID))
		config = configs.DefaultSchedulerConfig
	}
	conf, err := configs.LoadSchedulerConfigFromByteArray([]byte(config))
	if err != nil {
		event.Channel <- &rmevent.Result{Succeeded: false, Reason: err.Error()}
		return
	}
	// skip update if config has not changed
	oldConf := configs.ConfigContext.Get(cc.policyGroup)
	if conf.Checksum == oldConf.Checksum {
		event.Channel <- &rmevent.Result{
			Succeeded: true,
		}
		return
	}
	// update scheduler configuration
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
			create := false
			schedulable := false
			switch nodeInfo.Action {
			case si.NodeInfo_CREATE:
				create = true
				schedulable = true
			case si.NodeInfo_CREATE_DRAIN:
				create = true
			}
			if create {
				err := cc.addNode(nodeInfo, schedulable)
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
			} else {
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

// Locked version of the configuration update called outside of event system.
// Updates the current config via the config loader.
// Used in test only, normal updates use the internal call
func (cc *ClusterContext) UpdateRMSchedulerConfig(rmID string, config []byte) error {
	cc.Lock()
	defer cc.Unlock()
	if len(cc.partitions) == 0 {
		return fmt.Errorf("RM %s has no active partitions, make sure it is registered", rmID)
	}
	// load the config this returns a validated configuration
	conf, err := configs.LoadSchedulerConfigFromByteArray(config)
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
			log.Log(log.SchedContext).Info("updating partitions", zap.String("partitionName", partitionName))
			err = part.updatePartitionDetails(p)
			if err != nil {
				return err
			}
		} else {
			// not found: new partition, no checks needed
			log.Log(log.SchedContext).Info("added partitions", zap.String("partitionName", partitionName))

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
			log.Log(log.SchedContext).Info("marked partition for removal",
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
	for k, v := range cc.rmInfo {
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
			log.Log(log.SchedContext).Error("Failed to add application to non existing partition",
				zap.String("applicationID", app.ApplicationID),
				zap.String("partitionName", app.PartitionName))
			continue
		}
		// convert and resolve the user: cache can be set per partition
		// need to do this before we create the application
		ugi, err := partition.convertUGI(app.Ugi, common.IsAppCreationForced(app.Tags))
		if err != nil {
			rejectedApps = append(rejectedApps, &si.RejectedApplication{
				ApplicationID: app.ApplicationID,
				Reason:        err.Error(),
			})
			partition.AddRejectedApplication(objects.NewApplication(app, ugi, cc.rmEventHandler, request.RmID), err.Error())
			log.Log(log.SchedContext).Error("Failed to add application to partition (user rejected)",
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
			log.Log(log.SchedContext).Error("Failed to add application to partition (placement rejected)",
				zap.String("applicationID", app.ApplicationID),
				zap.String("partitionName", app.PartitionName),
				zap.Error(err))
			continue
		}
		acceptedApps = append(acceptedApps, &si.AcceptedApplication{
			ApplicationID: schedApp.ApplicationID,
		})
		log.Log(log.SchedContext).Info("Added application to partition",
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
			log.Log(log.SchedContext).Info("Application removed from partition",
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
func (cc *ClusterContext) addNode(nodeInfo *si.NodeInfo, schedulable bool) error {
	sn := objects.NewNode(nodeInfo)
	sn.SetSchedulable(schedulable)

	partition := cc.GetPartition(sn.Partition)
	if partition == nil {
		err := fmt.Errorf("failed to find partition %s for new node %s", sn.Partition, sn.NodeID)
		//nolint: TODO assess impact of partition metrics (this never hit the partition)
		metrics.GetSchedulerMetrics().IncFailedNodes()
		log.Log(log.SchedContext).Error("Failed to add node to non existing partition",
			zap.String("nodeID", sn.NodeID),
			zap.String("partitionName", sn.Partition))
		return err
	}

	existingAllocations := cc.convertAllocations(nodeInfo.ExistingAllocations)
	err := partition.AddNode(sn, existingAllocations)
	sn.SendNodeAddedEvent()
	if err != nil {
		wrapped := fmt.Errorf("failure while adding new node, node rejected with error: %w", err)
		log.Log(log.SchedContext).Error("Failed to add node to partition (rejected)",
			zap.String("nodeID", sn.NodeID),
			zap.String("partitionName", sn.Partition),
			zap.Error(err))
		return wrapped
	}

	if !sn.IsReady() {
		metrics.GetSchedulerMetrics().IncUnhealthyNodes()
	}
	if !sn.IsSchedulable() {
		metrics.GetSchedulerMetrics().IncDrainingNodes()
	}
	log.Log(log.SchedContext).Info("successfully added node",
		zap.String("nodeID", sn.NodeID),
		zap.String("partition", sn.Partition),
		zap.Bool("schedulable", sn.IsSchedulable()))
	return nil
}

// updateNode updates an existing node of the cluster.
// nil nodeInfo objects must be filtered out before calling this function
func (cc *ClusterContext) updateNode(nodeInfo *si.NodeInfo) {
	var partition *PartitionContext
	if p, ok := nodeInfo.Attributes[siCommon.NodePartition]; ok {
		partition = cc.GetPartition(p)
	} else {
		log.Log(log.SchedContext).Error("node partition not specified",
			zap.String("nodeID", nodeInfo.NodeID),
			zap.Stringer("nodeAction", nodeInfo.Action))
		return
	}

	if partition == nil {
		log.Log(log.SchedContext).Error("Failed to update node on non existing partition",
			zap.String("nodeID", nodeInfo.NodeID),
			zap.String("partitionName", nodeInfo.Attributes[siCommon.NodePartition]),
			zap.Stringer("nodeAction", nodeInfo.Action))
		return
	}

	node := partition.GetNode(nodeInfo.NodeID)
	if node == nil {
		log.Log(log.SchedContext).Error("Failed to update non existing node",
			zap.String("nodeID", nodeInfo.NodeID),
			zap.String("partitionName", nodeInfo.Attributes[siCommon.NodePartition]),
			zap.Stringer("nodeAction", nodeInfo.Action))
		return
	}

	switch nodeInfo.Action {
	case si.NodeInfo_UPDATE:
		var newReadyStatus bool
		var err error
		if newReadyStatus, err = strconv.ParseBool(nodeInfo.Attributes[siCommon.NodeReadyAttribute]); err != nil {
			log.Log(log.SchedContext).Error("Could not parse ready attribute, assuming true", zap.Any("attributes", nodeInfo.Attributes))
			newReadyStatus = true
		}

		if node.IsReady() && !newReadyStatus {
			log.Log(log.SchedContext).Info("Node has become unhealthy", zap.String("Node ID", node.NodeID))
			metrics.GetSchedulerMetrics().IncUnhealthyNodes()
			node.SetReady(newReadyStatus)
		}
		if !node.IsReady() && newReadyStatus {
			log.Log(log.SchedContext).Info("Node has become healthy", zap.String("Node ID", node.NodeID))
			metrics.GetSchedulerMetrics().DecUnhealthyNodes()
			node.SetReady(newReadyStatus)
		}

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
		node.SendNodeRemovedEvent()
		// notify the shim allocations have been released from node
		if len(released) != 0 {
			cc.notifyRMAllocationReleased(partition.RmID, released, si.TerminationType_STOPPED_BY_RM,
				fmt.Sprintf("Node %s Removed", node.NodeID))
		}
		for _, confirm := range confirmed {
			cc.notifyRMNewAllocation(partition.RmID, confirm)
		}
	default:
		log.Log(log.SchedContext).Debug("unknown action for node update",
			zap.String("nodeID", nodeInfo.NodeID),
			zap.String("partitionName", nodeInfo.Attributes[siCommon.NodePartition]),
			zap.Stringer("nodeAction", nodeInfo.Action))
	}
}

// Process an ask and allocation update request.
// - Add new allocations for the application(s).
// - Add new asks and remove released asks for the application(s).
// - Release allocations for the application(s).
// Lock free call, all updates occur on the underlying application which is locked or via events.
func (cc *ClusterContext) handleRMUpdateAllocationEvent(event *rmevent.RMUpdateAllocationEvent) {
	request := event.Request
	if len(request.Allocations) != 0 {
		cc.processAllocations(request)
	}
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

func (cc *ClusterContext) processAllocations(request *si.AllocationRequest) {
	// Send rejected allocations back to RM
	rejectedAllocs := make([]*si.RejectedAllocation, 0)

	// Send to scheduler
	for _, siAlloc := range request.Allocations {
		// try to get partition
		partition := cc.GetPartition(siAlloc.PartitionName)
		if partition == nil {
			msg := fmt.Sprintf("Failed to find partition %s, for application %s and allocation %s", siAlloc.PartitionName, siAlloc.ApplicationID, siAlloc.AllocationKey)
			log.Log(log.SchedContext).Error("Invalid allocation add requested by shim, partition not found",
				zap.String("partition", siAlloc.PartitionName),
				zap.String("nodeID", siAlloc.NodeID),
				zap.String("applicationID", siAlloc.ApplicationID),
				zap.String("allocationKey", siAlloc.AllocationKey))
			rejectedAllocs = append(rejectedAllocs, &si.RejectedAllocation{
				AllocationKey: siAlloc.AllocationKey,
				ApplicationID: siAlloc.ApplicationID,
				Reason:        msg,
			})
			continue
		}

		alloc := objects.NewAllocationFromSI(siAlloc)
		if err := partition.addAllocation(alloc); err != nil {
			rejectedAllocs = append(rejectedAllocs, &si.RejectedAllocation{
				AllocationKey: siAlloc.AllocationKey,
				ApplicationID: siAlloc.ApplicationID,
				Reason:        err.Error(),
			})
			log.Log(log.SchedContext).Error("Invalid allocation add requested by shim",
				zap.String("partition", siAlloc.PartitionName),
				zap.String("nodeID", siAlloc.NodeID),
				zap.String("applicationID", siAlloc.ApplicationID),
				zap.String("allocationKey", siAlloc.AllocationKey),
				zap.Error(err))
		}
	}

	// Reject allocs returned to RM proxy for the apps and partitions not found
	if len(rejectedAllocs) > 0 {
		cc.rmEventHandler.HandleEvent(&rmevent.RMRejectedAllocationEvent{
			RmID:                request.RmID,
			RejectedAllocations: rejectedAllocs,
		})
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
			log.Log(log.SchedContext).Error("Invalid ask add requested by shim, partition not found",
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
			log.Log(log.SchedContext).Error("Invalid ask add requested by shim",
				zap.String("partition", siAsk.PartitionName),
				zap.String("applicationID", siAsk.ApplicationID),
				zap.String("askKey", siAsk.AllocationKey),
				zap.Error(err))
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
			log.Log(log.SchedContext).Error("Invalid ask release requested by shim, partition not found",
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
		log.Log(log.SchedContext).Debug("Successfully synced shim on new allocation. response: " + result.Reason)
	} else {
		log.Log(log.SchedContext).Info("failed to sync shim on new allocation",
			zap.String("Allocation key: ", alloc.GetAllocationKey()))
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
			ApplicationID:   alloc.GetApplicationID(),
			PartitionName:   alloc.GetPartitionName(),
			UUID:            alloc.GetUUID(),
			TerminationType: terminationType,
			Message:         message,
			AllocationKey:   alloc.GetAllocationKey(),
		})
	}

	cc.rmEventHandler.HandleEvent(releaseEvent)
	// Wait from channel
	result := <-c
	if result.Succeeded {
		log.Log(log.SchedContext).Debug("Successfully synced shim on released allocations. response: " + result.Reason)
	} else {
		log.Log(log.SchedContext).Info("failed to sync shim on released allocations")
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
		log.Log(log.SchedContext).Info("partition not found for scheduling node",
			zap.String("nodeID", nodeID),
			zap.String("partitionName", partitionName))
		return nil
	}
	return partition.GetNode(nodeID)
}

func (cc *ClusterContext) SetRMInfo(rmID string, rmBuildInformation map[string]string) {
	if cc.rmInfo == nil {
		cc.rmInfo = make(map[string]*RMInformation)
	}
	buildInfo := make(map[string]string)
	for k, v := range rmBuildInformation {
		buildInfo[k] = v
	}
	buildInfo["rmId"] = rmID
	cc.rmInfo[rmID] = &RMInformation{
		RMBuildInformation: buildInfo,
	}
}

func (cc *ClusterContext) GetLastHealthCheckResult() *dao.SchedulerHealthDAOInfo {
	cc.RLock()
	defer cc.RUnlock()
	return cc.lastHealthCheckResult
}

func (cc *ClusterContext) SetLastHealthCheckResult(c *dao.SchedulerHealthDAOInfo) {
	cc.Lock()
	defer cc.Unlock()
	cc.lastHealthCheckResult = c
}

func (cc *ClusterContext) GetUUID() string {
	return cc.uuid
}
