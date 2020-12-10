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
	"sync"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/handler"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
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

	sync.RWMutex
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

// The main scheduling routine.
// Process each partition in the scheduler, walk over each queue and app to check if anything can be scheduled.
// This can be forked into a go routine per partition if needed to increase parallel allocations
func (cc *ClusterContext) schedule() {
	// schedule each partition defined in the cluster
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
		alloc := psc.tryReservedAllocate()
		// nothing reserved that can be allocated try normal allocate
		if alloc == nil {
			alloc = psc.tryAllocate()
		}
		if alloc != nil {
			// TODO: The alloc is passed to the RM twice why do we need event + callback?
			// See YUNIKORN-462, there are two separate communications for the same allocation
			// between the core and the shim they should be merged into one communication.

			// communicate the allocation to the RM
			cc.rmEventHandler.HandleEvent(&rmevent.RMNewAllocationsEvent{
				Allocations: []*si.Allocation{alloc.NewSIFromAllocation()},
				RmID:        psc.RmID,
			})
			// if reconcile plugin is enabled, re-sync the cache now.
			// before deciding on an allocation, call the reconcile plugin to sync scheduler cache
			// between core and shim if necessary. This is useful when running multiple allocations
			// in parallel and need to handle inter container affinity and anti-affinity.
			if rp := plugins.GetReconcilePlugin(); rp != nil {
				if err := rp.ReSyncSchedulerCache(&si.ReSyncSchedulerCacheArgs{
					AssumedAllocations: []*si.AssumedAllocation{
						{
							AllocationKey: alloc.AllocationKey,
							NodeID:        alloc.NodeID,
						},
					},
				}); err != nil {
					log.Logger().Error("failed to sync shim",
						zap.Error(err))
				}
			}
		}
	}
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

// Main update processing: the RM passes a large multi part update which needs to be unravelled.
// Order of following operations is fixed, don't change unless carefully thought through.
// 1) applications
// 2) allocations on existing applications
// 3) nodes
// Updating allocations on existing applications requires the application to exist.
// Node updates include recovered nodes which are linked to applications that must exist.
func (cc *ClusterContext) processRMUpdateEvent(event *rmevent.RMUpdateRequestEvent) {
	request := event.Request
	// 1) Add / remove app requested by RM.
	cc.processApplications(request)
	// 2) Add new request, release allocation, cancel request
	cc.processAllocations(request)
	// 3) Add / remove / update Nodes
	cc.processNodes(request)
}

func (cc *ClusterContext) processNodes(request *si.UpdateRequest) {
	// 3) Add / remove / update Nodes
	// Process add node
	if len(request.NewSchedulableNodes) > 0 {
		cc.addNodes(request)
	}
	if len(request.UpdatedNodes) > 0 {
		cc.updateNodes(request)
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
			partition.partitionManager.stop = true
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
func (cc *ClusterContext) processApplications(request *si.UpdateRequest) {
	if len(request.NewApplications) == 0 && len(request.RemoveApplications) == 0 {
		return
	}
	acceptedApps := make([]*si.AcceptedApplication, 0)
	rejectedApps := make([]*si.RejectedApplication, 0)

	for _, app := range request.NewApplications {
		partition := cc.GetPartition(app.PartitionName)
		if partition == nil {
			msg := fmt.Sprintf("Failed to add application %s to partition %s, partition doesn't exist", app.ApplicationID, app.PartitionName)
			log.Logger().Info(msg)
			rejectedApps = append(rejectedApps, &si.RejectedApplication{
				ApplicationID: app.ApplicationID,
				Reason:        msg,
			})
			continue
		}
		// convert and resolve the user: cache can be set per partition
		ugi, err := partition.convertUGI(app.Ugi)
		if err != nil {
			rejectedApps = append(rejectedApps, &si.RejectedApplication{
				ApplicationID: app.ApplicationID,
				Reason:        err.Error(),
			})
			continue
		}
		// create a new app object and add it to the partition (partition logs details)
		schedApp := objects.NewApplication(app.ApplicationID, app.PartitionName, app.QueueName, ugi, app.Tags, cc.rmEventHandler, request.RmID)
		if err = partition.AddApplication(schedApp); err != nil {
			rejectedApps = append(rejectedApps, &si.RejectedApplication{
				ApplicationID: app.ApplicationID,
				Reason:        err.Error(),
			})
			continue
		}
		acceptedApps = append(acceptedApps, &si.AcceptedApplication{
			ApplicationID: schedApp.ApplicationID,
		})
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
	if len(request.RemoveApplications) > 0 {
		metrics.GetSchedulerMetrics().SubTotalApplicationsRunning(len(request.RemoveApplications))
		// ToDO: need to improve this once we have state in YuniKorn for apps.
		metrics.GetSchedulerMetrics().AddTotalApplicationsCompleted(len(request.RemoveApplications))
		for _, app := range request.RemoveApplications {
			partition := cc.GetPartition(app.PartitionName)
			if partition == nil {
				continue
			}
			allocations := partition.removeApplication(app.ApplicationID)
			if len(allocations) > 0 {
				cc.notifyRMAllocationReleased(partition.RmID, allocations, si.AllocationReleaseResponse_STOPPED_BY_RM,
					fmt.Sprintf("Application %s Removed", app.ApplicationID))
			}
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
	cc.RLock()
	defer cc.RUnlock()

	delete(cc.partitions, partitionName)
}

func (cc *ClusterContext) updateNodes(request *si.UpdateRequest) {
	for _, update := range request.UpdatedNodes {
		var partition *PartitionContext
		if p, ok := update.Attributes[siCommon.NodePartition]; ok {
			partition = cc.GetPartition(p)
		} else {
			log.Logger().Debug("node partition not specified",
				zap.String("nodeID", update.NodeID),
				zap.String("nodeAction", update.Action.String()))
			continue
		}

		if partition == nil {
			continue
		}

		if node, ok := partition.nodes[update.NodeID]; ok {
			switch update.Action {
			case si.UpdateNodeInfo_UPDATE:
				if sr := update.SchedulableResource; sr != nil {
					partition.updateNode(node, resources.NewResourceFromProto(sr))
				}
				if or := update.OccupiedResource; or != nil {
					newOccupied := resources.NewResourceFromProto(or)
					node.SetOccupiedResource(newOccupied)
				}
			case si.UpdateNodeInfo_DRAIN_NODE:
				// set the state to not schedulable
				node.SetSchedulable(false)
			case si.UpdateNodeInfo_DRAIN_TO_SCHEDULABLE:
				// set the state to schedulable
				node.SetSchedulable(true)
			case si.UpdateNodeInfo_DECOMISSION:
				// set the state to not schedulable then tell the partition to clean up
				node.SetSchedulable(false)
				released := partition.removeNode(node.NodeID)
				// notify the shim allocations have been released from node
				if len(released) != 0 {
					cc.notifyRMAllocationReleased(partition.RmID, released, si.AllocationReleaseResponse_STOPPED_BY_RM,
						fmt.Sprintf("Node %s Removed", node.NodeID))
				}
			}
		}
	}
}

func (cc *ClusterContext) addNodes(request *si.UpdateRequest) {
	acceptedNodes := make([]*si.AcceptedNode, 0)
	rejectedNodes := make([]*si.RejectedNode, 0)
	for _, node := range request.NewSchedulableNodes {
		sn := objects.NewNode(node)
		partition := cc.GetPartition(sn.Partition)
		if partition == nil {
			msg := fmt.Sprintf("Failed to find partition %s for new node %s", sn.Partition, node.NodeID)
			log.Logger().Info(msg)
			// TODO assess impact of partition metrics (this never hit the partition)
			metrics.GetSchedulerMetrics().IncFailedNodes()
			rejectedNodes = append(rejectedNodes,
				&si.RejectedNode{
					NodeID: node.NodeID,
					Reason: msg,
				})
			continue
		}
		existingAllocations := cc.convertAllocations(node.ExistingAllocations)
		err := partition.AddNode(sn, existingAllocations)
		if err != nil {
			msg := fmt.Sprintf("Failure while adding new node, node rejected with error %s", err.Error())
			log.Logger().Warn(msg)
			rejectedNodes = append(rejectedNodes,
				&si.RejectedNode{
					NodeID: node.NodeID,
					Reason: msg,
				})
			continue
		}
		log.Logger().Info("successfully added node",
			zap.String("nodeID", node.NodeID),
			zap.String("partition", sn.Partition))
		acceptedNodes = append(acceptedNodes, &si.AcceptedNode{NodeID: node.NodeID})
	}

	// inform the RM which nodes have been accepted/rejected
	cc.rmEventHandler.HandleEvent(
		&rmevent.RMNodeUpdateEvent{
			RmID:          request.RmID,
			AcceptedNodes: acceptedNodes,
			RejectedNodes: rejectedNodes,
		})
}

// Process an ask and allocation update request.
// - Add new asks and remove released asks for the application(s).
// - Release allocations for the application(s).
// Lock free call, all updates occur on the underlying application which is locked or via events.
func (cc *ClusterContext) processAllocations(request *si.UpdateRequest) {
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

func (cc *ClusterContext) processAsks(request *si.UpdateRequest) {
	// Send rejects back to RM
	rejectedAsks := make([]*si.RejectedAllocationAsk, 0)

	// Send to scheduler
	for _, siAsk := range request.Asks {
		// try to get ApplicationInfo
		partition := cc.GetPartition(siAsk.PartitionName)
		if partition == nil {
			msg := fmt.Sprintf("Failed to find partition %s, for application %s and allocation %s", siAsk.PartitionName, siAsk.ApplicationID, siAsk.AllocationKey)
			log.Logger().Info(msg)
			rejectedAsks = append(rejectedAsks, &si.RejectedAllocationAsk{
				AllocationKey: siAsk.AllocationKey,
				ApplicationID: siAsk.ApplicationID,
				Reason:        msg,
			})
			continue
		}

		// if app info doesn't exist, reject the request
		app := partition.getApplication(siAsk.ApplicationID)
		if app == nil {
			msg := fmt.Sprintf("Failed to find application %s, for allocation %s", siAsk.ApplicationID, siAsk.AllocationKey)
			log.Logger().Info(msg)
			rejectedAsks = append(rejectedAsks,
				&si.RejectedAllocationAsk{
					AllocationKey: siAsk.AllocationKey,
					ApplicationID: siAsk.ApplicationID,
					Reason:        msg,
				})
			continue
		}
		if err := app.AddAllocationAsk(objects.NewAllocationAsk(siAsk)); err != nil {
			rejectedAsks = append(rejectedAsks,
				&si.RejectedAllocationAsk{
					AllocationKey: siAsk.AllocationKey,
					ApplicationID: siAsk.ApplicationID,
					Reason:        err.Error(),
				})
			log.Logger().Info("Invalid ask from shim for app",
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

func (cc *ClusterContext) processAskReleases(releases []*si.AllocationAskReleaseRequest) {
	for _, toRelease := range releases {
		partition := cc.GetPartition(toRelease.PartitionName)
		if partition != nil {
			partition.removeAllocationAsk(toRelease.ApplicationID, toRelease.Allocationkey)
		}
	}
}

func (cc *ClusterContext) processAllocationReleases(releases []*si.AllocationReleaseRequest, rmID string) {
	// TODO: the release comes from the RM and confirmed twice why do we need event + callback?
	// See YUNIKORN-462, there are two separate communications for the same allocation
	// between the core and the shim they should be merged into one communication.

	toReleaseAllocations := make([]*si.ForgotAllocation, 0)
	for _, toRelease := range releases {
		partition := cc.GetPartition(toRelease.PartitionName)
		if partition != nil {
			allocs := partition.removeAllocation(toRelease.ApplicationID, toRelease.UUID)
			// notify the RM of the exact released allocations
			if len(allocs) > 0 {
				cc.notifyRMAllocationReleased(rmID, allocs, si.AllocationReleaseResponse_STOPPED_BY_RM, "allocation remove as per RM request")
			}
			for _, alloc := range allocs {
				toReleaseAllocations = append(toReleaseAllocations, &si.ForgotAllocation{
					AllocationKey: alloc.AllocationKey,
				})
			}
		}
	}

	// if reconcile plugin is enabled, re-sync the cache now.
	// this gives the chance for the cache to update its memory about assumed pods
	// whenever we release an allocation, we must ensure the corresponding pod is successfully
	// removed from external cache, otherwise predicates will run into problems.
	if len(toReleaseAllocations) > 0 {
		log.Logger().Debug("notify shim to forget assumed pods",
			zap.Int("size", len(toReleaseAllocations)))
		if rp := plugins.GetReconcilePlugin(); rp != nil {
			err := rp.ReSyncSchedulerCache(&si.ReSyncSchedulerCacheArgs{
				ForgetAllocations: toReleaseAllocations,
			})
			if err != nil {
				log.Logger().Error("failed to sync shim",
					zap.Error(err))
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

// Create a RM update event to notify RM of released allocations
// Lock free call, all updates occur via events.
func (cc *ClusterContext) notifyRMAllocationReleased(rmID string, released []*objects.Allocation, terminationType si.AllocationReleaseResponse_TerminationType, message string) {
	releaseEvent := &rmevent.RMReleaseAllocationEvent{
		ReleasedAllocations: make([]*si.AllocationReleaseResponse, 0),
		RmID:                rmID,
	}
	for _, alloc := range released {
		releaseEvent.ReleasedAllocations = append(releaseEvent.ReleasedAllocations, &si.AllocationReleaseResponse{
			UUID:            alloc.UUID,
			TerminationType: terminationType,
			Message:         message,
		})
	}

	cc.rmEventHandler.HandleEvent(releaseEvent)
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
