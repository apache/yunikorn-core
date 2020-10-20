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
	"sync"

	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
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

type ClusterSchedulingContext struct {
	partitions     map[string]*PartitionContext
	needPreemption bool
	policyGroup    string
	rmEvents       chan interface{}
	rmEventHandler handler.EventHandler

	sync.RWMutex
}

func newClusterSchedulingContext() *ClusterSchedulingContext {
	return &ClusterSchedulingContext{
		partitions: make(map[string]*PartitionContext),
	}
}

func (csc *ClusterSchedulingContext) setEventHandler(rmHandler handler.EventHandler) {
	csc.rmEventHandler = rmHandler
}

// The main scheduling routine.
// Process each partition in the scheduler, walk over each queue and app to check if anything can be scheduled.
// This can be forked into a go routine per partition if needed to increase parallel allocations
func (csc *ClusterSchedulingContext) schedule() {
	// schedule each partition defined in the cluster
	for _, psc := range csc.GetPartitionMapClone() {
		// if there are no resources in the partition just skip
		if psc.root.GetMaxResource() == nil {
			continue
		}
		// try reservations first
		alloc := psc.tryReservedAllocate()
		// nothing reserved that can be allocated try normal allocate
		if alloc == nil {
			alloc = psc.tryAllocate()
		}
		if alloc != nil {
			// communicate the allocation to the RM
			csc.rmEventHandler.HandleEvent(&rmevent.RMNewAllocationsEvent{
				Allocations: []*si.Allocation{alloc.NewSIFromAllocation()},
				RmID:        psc.RmID,
			})
			// TODO: The alloc is just passed to the RM why do we need a callback?
			// TODO: The comments are from before the cache and scheduler merge
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

func (csc *ClusterSchedulingContext) processRMRegistrationEvent(event *rmevent.RMRegistrationEvent) {
	csc.Lock()
	defer csc.Unlock()
	rmID := event.Registration.RmID
	// we should not have any partitions set at this point
	if len(csc.partitions) > 0 {
		event.Channel <- &rmevent.Result{
			Reason:    fmt.Sprintf("RM %s has been registered before, active partitions %d", rmID, len(csc.partitions)),
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

	for _, p := range conf.Partitions {
		partitionName := common.GetNormalizedPartitionName(p.Name, rmID)
		p.Name = partitionName
		var partition *PartitionContext
		partition, err = newPartitionContext(p, rmID, csc)
		if err != nil {
			event.Channel <- &rmevent.Result{Succeeded: false, Reason: err.Error()}
			return
		}
		csc.partitions[partitionName] = partition
	}
	// update global scheduler configs, set the policyGroup for this cluster
	csc.policyGroup = policyGroup
	configs.ConfigContext.Set(policyGroup, conf)

	// Done, notify channel
	event.Channel <- &rmevent.Result{
		Succeeded: true,
	}
}

func (csc *ClusterSchedulingContext) processRMConfigUpdateEvent(event *rmevent.RMConfigUpdateEvent) {
	csc.Lock()
	defer csc.Unlock()
	rmID := event.RmID
	// need to enhance the check to support multiple RMs
	if len(csc.partitions) == 0 {
		event.Channel <- &rmevent.Result{
			Reason:    fmt.Sprintf("RM %s has no active partitions, make sure it is registered", rmID),
			Succeeded: false,
		}
		return
	}
	// load the config this returns a validated configuration
	conf, err := configs.SchedulerConfigLoader(csc.policyGroup)
	if err != nil {
		event.Channel <- &rmevent.Result{Succeeded: false, Reason: err.Error()}
		return
	}
	err = csc.updateSchedulerConfig(conf, rmID)
	if err != nil {
		event.Channel <- &rmevent.Result{Succeeded: false, Reason: err.Error()}
		return
	}
	// Done, notify channel
	event.Channel <- &rmevent.Result{
		Succeeded: true,
	}
}

// Main update processing: the RM passes a large multi part update which needs to be unravelled.
// Order of following operations is fixed, don't change unless carefully thought through.
// 1) applications
// 2) allocations on existing applications
// 3) nodes
// Updating allocations on existing applications requires the application to exist.
// Node updates include recovered nodes which are linked to applications that must exist.
func (csc *ClusterSchedulingContext) processRMUpdateEvent(event *rmevent.RMUpdateRequestEvent) {
	request := event.Request
	// 1) Add / remove app requested by RM.
	csc.processApplications(request)
	// 2) Add new request, release allocation, cancel request
	csc.processAllocations(request)
	// 3) Add / remove / update Nodes
	csc.processNodes(request)
}

func (csc *ClusterSchedulingContext) processNodes(request *si.UpdateRequest) {
	// 3) Add / remove / update Nodes
	// Process add node
	if len(request.NewSchedulableNodes) > 0 {
		csc.addNodes(request)
	}
	if len(request.UpdatedNodes) > 0 {
		csc.updateNodes(request)
	}
}

// Called when a RM re-registers. This triggers a full clean up.
// Registration expects everything to be clean.
func (csc *ClusterSchedulingContext) removePartitionsByRMID(event *rmevent.RMPartitionsRemoveEvent) {
	csc.Lock()
	defer csc.Unlock()
	partitionToRemove := make(map[string]bool)

	// Just remove corresponding partitions
	for k, partition := range csc.partitions {
		if partition.RmID == event.RmID {
			partition.partitionManager.stop = true
			partitionToRemove[k] = true
		}
	}

	for partitionName := range partitionToRemove {
		delete(csc.partitions, partitionName)
	}
	// Done, notify channel
	event.Channel <- &rmevent.Result{
		Succeeded: true,
	}
}

// Locked version of the configuration update called from the webservice
// NOTE: this call assumes one RM which is registered and uses that RM for the updates
func (csc *ClusterSchedulingContext) UpdateSchedulerConfig(conf *configs.SchedulerConfig) error {
	csc.Lock()
	defer csc.Unlock()
	// hack around the missing rmID
	for _, pi := range csc.partitions {
		rmID := pi.RmID
		log.Logger().Debug("Assuming one RM on config update call from webservice",
			zap.String("rmID", rmID))
		return csc.updateSchedulerConfig(conf, rmID)
	}
	return fmt.Errorf("RM has no active partitions, make sure it is registered")
}

// Update the scheduler config called when the file is updated or indirectly when the webservice is called.
func (csc *ClusterSchedulingContext) updateSchedulerConfig(conf *configs.SchedulerConfig, rmID string) error {
	visited := map[string]bool{}
	var err error
	// walk over the partitions in the config: update existing ones
	for _, p := range conf.Partitions {
		partitionName := common.GetNormalizedPartitionName(p.Name, rmID)
		p.Name = partitionName
		part, ok := csc.partitions[p.Name]
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

			part, err = newPartitionContext(p, rmID, csc)
			if err != nil {
				return err
			}
			go part.partitionManager.Run()
			csc.partitions[partitionName] = part
		}
		// add it to the partitions to update
		visited[p.Name] = true
	}

	// get the removed partitions, mark them as deleted
	for _, part := range csc.partitions {
		if !visited[part.Name] {
			part.partitionManager.Stop()
			log.Logger().Info("marked partition for removal",
				zap.String("partitionName", part.Name))
		}
	}

	return nil
}

// Get the config name.
func (csc *ClusterSchedulingContext) GetPolicyGroup() string {
	csc.RLock()
	defer csc.RUnlock()
	return csc.policyGroup
}

func (csc *ClusterSchedulingContext) GetPartitionMapClone() map[string]*PartitionContext {
	csc.RLock()
	defer csc.RUnlock()

	newMap := make(map[string]*PartitionContext)
	for k, v := range csc.partitions {
		newMap[k] = v
	}
	return newMap
}

func (csc *ClusterSchedulingContext) getPartition(partitionName string) *PartitionContext {
	csc.RLock()
	defer csc.RUnlock()
	return csc.partitions[partitionName]
}

// Get the scheduling application based on the ID from the partition.
// Returns nil if the partition or app cannot be found.
// Visible for tests
func (csc *ClusterSchedulingContext) GetSchedulingApplication(appID, partitionName string) *objects.Application {
	csc.RLock()
	defer csc.RUnlock()

	if partition := csc.partitions[partitionName]; partition != nil {
		return partition.getApplication(appID)
	}

	return nil
}

// Get the scheduling queue based on the queue path name from the partition.
// Returns nil if the partition or queue cannot be found.
// Visible for tests
func (csc *ClusterSchedulingContext) GetSchedulingQueue(queueName string, partitionName string) *objects.Queue {
	csc.RLock()
	defer csc.RUnlock()

	if partition := csc.partitions[partitionName]; partition != nil {
		return partition.GetQueue(queueName)
	}

	return nil
}

// Return the list of reservations for the partition.
// Returns nil if the partition cannot be found or an empty map if there are no reservations
// Visible for tests
func (csc *ClusterSchedulingContext) GetPartitionReservations(partitionName string) map[string]int {
	csc.RLock()
	defer csc.RUnlock()

	if partition := csc.partitions[partitionName]; partition != nil {
		return partition.getReservations()
	}

	return nil
}

// Process the application update. Add and remove applications from the partitions.
// Lock free call, all updates occur on the underlying partition which is locked, or via events.
func (csc *ClusterSchedulingContext) processApplications(request *si.UpdateRequest) {
	if len(request.NewApplications) == 0 && len(request.RemoveApplications) == 0 {
		return
	}
	acceptedApps := make([]*si.AcceptedApplication, 0)
	rejectedApps := make([]*si.RejectedApplication, 0)

	for _, app := range request.NewApplications {
		partition := csc.getPartition(app.PartitionName)
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
		schedApp := objects.NewApplication(app.ApplicationID, app.PartitionName, app.QueueName, ugi, app.Tags, csc.rmEventHandler, request.RmID)
		if err = partition.addApplication(schedApp); err != nil {
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
		csc.rmEventHandler.HandleEvent(
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
			partition := csc.getPartition(app.PartitionName)
			if partition == nil {
				continue
			}
			allocations := partition.removeApplication(app.ApplicationID)
			if len(allocations) > 0 {
				csc.notifyRMAllocationReleased(partition.RmID, allocations, si.AllocationReleaseResponse_STOPPED_BY_RM,
					fmt.Sprintf("Application %s Removed", app.ApplicationID))
			}
		}
	}
}

func (csc *ClusterSchedulingContext) NeedPreemption() bool {
	csc.RLock()
	defer csc.RUnlock()

	return csc.needPreemption
}

// Callback from the partition manager to finalise the removal of the partition
func (csc *ClusterSchedulingContext) removeSchedulingPartition(partitionName string) {
	csc.RLock()
	defer csc.RUnlock()

	delete(csc.partitions, partitionName)
}

func (csc *ClusterSchedulingContext) updateNodes(request *si.UpdateRequest) {
	for _, update := range request.UpdatedNodes {
		var partition *PartitionContext
		if p, ok := update.Attributes[siCommon.NodePartition]; ok {
			partition = csc.getPartition(p)
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
					newCapacity := resources.NewResourceFromProto(sr)
					node.SetCapacity(newCapacity)
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
					csc.notifyRMAllocationReleased(partition.RmID, released, si.AllocationReleaseResponse_STOPPED_BY_RM,
						fmt.Sprintf("Node %s Removed", node.NodeID))
				}
			}
		}
	}
}

func (csc *ClusterSchedulingContext) addNodes(request *si.UpdateRequest) {
	acceptedNodes := make([]*si.AcceptedNode, 0)
	rejectedNodes := make([]*si.RejectedNode, 0)
	for _, node := range request.NewSchedulableNodes {
		sn := objects.NewNode(node)
		partition := csc.getPartition(sn.Partition)
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
		existingAllocations := csc.convertAllocations(node.ExistingAllocations)
		err := partition.addNode(sn, existingAllocations)
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
	csc.rmEventHandler.HandleEvent(
		&rmevent.RMNodeUpdateEvent{
			RmID:          request.RmID,
			AcceptedNodes: acceptedNodes,
			RejectedNodes: rejectedNodes,
		})
}

// Process the ask and allocation updates. Add and release asks for the applications.
// Release allocations from the applications.
// Lock free call, all updates occur on the underlying application which is locked or via events.
func (csc *ClusterSchedulingContext) processAllocations(request *si.UpdateRequest) {
	if len(request.Asks) != 0 {
		csc.processAsks(request)
	}
	if request.Releases != nil {
		if len(request.Releases.AllocationAsksToRelease) > 0 {
			csc.processAskReleases(request.Releases.AllocationAsksToRelease)
		}
		if len(request.Releases.AllocationsToRelease) > 0 {
			csc.processAllocationReleases(request.Releases.AllocationsToRelease, request.RmID)
		}
	}
}

func (csc *ClusterSchedulingContext) processAsks(request *si.UpdateRequest) {
	// Send rejects back to RM
	rejectedAsks := make([]*si.RejectedAllocationAsk, 0)

	// Send to scheduler
	for _, siAsk := range request.Asks {
		// try to get ApplicationInfo
		partition := csc.getPartition(siAsk.PartitionName)
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
		csc.rmEventHandler.HandleEvent(&rmevent.RMRejectedAllocationAskEvent{
			RmID:                   request.RmID,
			RejectedAllocationAsks: rejectedAsks,
		})
	}
}

func (csc *ClusterSchedulingContext) processAskReleases(releases []*si.AllocationAskReleaseRequest) {
	for _, toRelease := range releases {
		partition := csc.getPartition(toRelease.PartitionName)
		if partition != nil {
			partition.removeAllocationAsk(toRelease.ApplicationID, toRelease.Allocationkey)
		}
	}
}

func (csc *ClusterSchedulingContext) processAllocationReleases(releases []*si.AllocationReleaseRequest, rmID string) {
	toReleaseAllocations := make([]*si.ForgotAllocation, 0)
	for _, toRelease := range releases {
		partition := csc.getPartition(toRelease.PartitionName)
		if partition != nil {
			allocs := partition.removeAllocation(toRelease.ApplicationID, toRelease.UUID)
			// notify the RM of the exact released allocations
			if len(allocs) > 0 {
				csc.notifyRMAllocationReleased(rmID, allocs, si.AllocationReleaseResponse_STOPPED_BY_RM, "allocation remove as per RM request")
			}
			for _, alloc := range allocs {
				toReleaseAllocations = append(toReleaseAllocations, &si.ForgotAllocation{
					AllocationKey: alloc.AllocationKey,
				})
			}
		}
	}

	// TODO: the release comes from the RM why do we need a callback?
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
func (csc *ClusterSchedulingContext) convertAllocations(allocations []*si.Allocation) []*objects.Allocation {
	convert := make([]*objects.Allocation, len(allocations))
	for current, allocation := range allocations {
		convert[current] = objects.NewAllocationFromSI(allocation)
	}

	return convert
}

// Create a RM update event to notify RM of released allocations
// Lock free call, all updates occur via events.
func (csc *ClusterSchedulingContext) notifyRMAllocationReleased(rmID string, released []*objects.Allocation, terminationType si.AllocationReleaseResponse_TerminationType, message string) {
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

	csc.rmEventHandler.HandleEvent(releaseEvent)
}

// Get a scheduling node based on its name from the partition.
// Returns nil if the partition or node cannot be found.
// Visible for tests
func (csc *ClusterSchedulingContext) GetSchedulingNode(nodeID, partitionName string) *objects.Node {
	csc.Lock()
	defer csc.Unlock()

	partition := csc.partitions[partitionName]
	if partition == nil {
		log.Logger().Info("partition not found for scheduling node",
			zap.String("nodeID", nodeID),
			zap.String("partitionName", partitionName))
		return nil
	}
	return partition.getNode(nodeID)
}
