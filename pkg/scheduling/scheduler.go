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
	"reflect"
	"time"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/cache/cacheevent"
	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/commonevents"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/handler"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/schedulerevent"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// Responsibility of this class is, get status from SchedulerCache, and
// send allocation / release proposal back to cache.
//
// Scheduler may maintain its local status which is different from SchedulerCache
type Scheduler struct {
	// Private fields need protection
	clusterInfo              *cache.ClusterInfo        // link to the cache object
	clusterSchedulingContext *ClusterSchedulingContext // main context
	preemptionContext        *preemptionContext        // Preemption context
	eventHandlers            handler.EventHandlers     // list of event handlers
	pendingSchedulerEvents   chan interface{}          // queue for scheduler events
}

func NewScheduler(clusterInfo *cache.ClusterInfo) *Scheduler {
	m := &Scheduler{}
	m.clusterInfo = clusterInfo
	m.clusterSchedulingContext = NewClusterSchedulingContext()
	m.pendingSchedulerEvents = make(chan interface{}, 1024*1024)
	return m
}

// Start service
func (s *Scheduler) StartService(handlers handler.EventHandlers, manualSchedule bool) {
	s.eventHandlers = handlers

	// Start event handlers
	go s.handleSchedulerEvent()

	// Start resource monitor if necessary (majorly for testing)
	monitor := newNodesResourceUsageMonitor(s)
	monitor.start()

	if !manualSchedule {
		go s.internalSchedule()
		go s.internalInspectOutstandingRequests()
		go s.internalPreemption()
	}
}

// Create single allocation
func newSingleAllocationProposal(alloc *schedulingAllocation) *cacheevent.AllocationProposalBundleEvent {
	return &cacheevent.AllocationProposalBundleEvent{
		AllocationProposals: []*commonevents.AllocationProposal{
			{
				NodeID:            alloc.nodeID,
				ApplicationID:     alloc.SchedulingAsk.ApplicationID,
				QueueName:         alloc.SchedulingAsk.QueueName,
				AllocatedResource: alloc.SchedulingAsk.AllocatedResource,
				AllocationKey:     alloc.SchedulingAsk.AskProto.AllocationKey,
				Priority:          alloc.SchedulingAsk.AskProto.Priority,
				PartitionName:     alloc.SchedulingAsk.PartitionName,
			},
		},
		ReleaseProposals: alloc.releases,
		PartitionName:    alloc.SchedulingAsk.PartitionName,
	}
}

// Internal start scheduling service
func (s *Scheduler) internalSchedule() {
	for {
		s.schedule()
	}
}

// Internal start preemption service
func (s *Scheduler) internalPreemption() {
	for {
		s.SingleStepPreemption()
		time.Sleep(1000 * time.Millisecond)
	}
}

func (s *Scheduler) internalInspectOutstandingRequests() {
	for {
		time.Sleep(1000 * time.Millisecond)
		s.inspectOutstandingRequests()
	}
}

// inspect on the outstanding requests for each of the queues,
// update request state accordingly to shim if needed.
// this function filters out all outstanding requests that being
// skipped due to insufficient cluster resources and update the
// state through the ContainerSchedulingStateUpdaterPlugin in order
// to trigger the auto-scaling.
func (s *Scheduler) inspectOutstandingRequests() {
	log.Logger().Debug("inspect outstanding requests")
	// schedule each partition defined in the cluster
	for _, psc := range s.clusterSchedulingContext.getPartitionMapClone() {
		requests := psc.calculateOutstandingRequests()
		if len(requests) > 0 {
			for _, ask := range requests {
				log.Logger().Debug("outstanding request",
					zap.String("appID", ask.AskProto.ApplicationID),
					zap.String("allocationKey", ask.AskProto.AllocationKey))
				// these asks are queue outstanding requests,
				// they can fit into the max head room, but they are pending because lack of partition resources
				if updater := plugins.GetContainerSchedulingStateUpdaterPlugin(); updater != nil {
					updater.Update(&si.UpdateContainerSchedulingStateRequest{
						ApplicartionID: ask.AskProto.ApplicationID,
						AllocationKey:  ask.AskProto.AllocationKey,
						State:          si.UpdateContainerSchedulingStateRequest_FAILED,
						Reason:         "request is waiting for cluster resources become available",
					})
				}
			}
		}
	}
}

func (s *Scheduler) updateSchedulingRequest(schedulingAsk *schedulingAllocationAsk) error {
	// Get SchedulingApplication
	app := s.clusterSchedulingContext.GetSchedulingApplication(schedulingAsk.ApplicationID, schedulingAsk.PartitionName)
	if app == nil {
		return fmt.Errorf("cannot find scheduling application %s, for allocation %s", schedulingAsk.ApplicationID, schedulingAsk.AskProto.AllocationKey)
	}

	// found now update the pending requests for the queue that the app is running in
	_, err := app.addAllocationAsk(schedulingAsk)
	return err
}

// Recovery of allocations do not go through the normal cycle and never have an "allocating" state.
// When a node registers with existing allocations this would cause issues as we cannot confirm the resources.
// Set the allocating
// Confirm an allocation proposal.
// Convenience function around updating the proposal with a change of +1.
func (s *Scheduler) confirmAllocationProposal(allocProposal *commonevents.AllocationProposal) error {
	return s.processAllocationProposal(allocProposal, true)
}

// Reject an allocation proposal.
// Convenience function for updating the proposal with a change of -1.
func (s *Scheduler) rejectAllocationProposal(allocProposal *commonevents.AllocationProposal) error {
	return s.processAllocationProposal(allocProposal, false)
}

// Process the allocation proposal by updating the app and queue.
func (s *Scheduler) processAllocationProposal(allocProposal *commonevents.AllocationProposal, confirm bool) error {
	// get the partition
	partition := s.clusterSchedulingContext.getPartition(allocProposal.PartitionName)
	if partition == nil {
		return fmt.Errorf("cannot find scheduling partition %s, for allocation ID %s", allocProposal.PartitionName, allocProposal.AllocationKey)
	}
	err := partition.confirmAllocation(allocProposal, confirm)
	if err != nil && allocProposal.UUID != "" {
		log.Logger().Debug("failed to confirm proposal, removing allocation from cache",
			zap.String("appID", allocProposal.ApplicationID),
			zap.String("partitionName", allocProposal.PartitionName),
			zap.String("allocationKey", allocProposal.AllocationKey),
			zap.String("allocation UUID", allocProposal.UUID))
		// Pass in a releaseType of preemption we need to communicate the release back to the RM. The alloc was
		// communicated to the RM from the cache but immediately removed again
		// This is an internal removal which we do not have a code for, however it is triggered by the ask removal
		// from the RM. Stopped by RM fits best.
		event := &cacheevent.ReleaseAllocationsEvent{AllocationsToRelease: make([]*commonevents.ReleaseAllocation, 0)}
		event.AllocationsToRelease = append(event.AllocationsToRelease, commonevents.NewReleaseAllocation(allocProposal.UUID, allocProposal.ApplicationID, allocProposal.PartitionName, "allocation confirmation failure (ask removal)", si.AllocationReleaseResponse_STOPPED_BY_RM))

		s.eventHandlers.CacheEventHandler.HandleEvent(event)
	}
	return err
}

// When a new app added, invoked by external
func (s *Scheduler) addNewApplication(info *cache.ApplicationInfo) error {
	schedulingApp := newSchedulingApplication(info)

	return s.clusterSchedulingContext.addSchedulingApplication(schedulingApp)
}

func (s *Scheduler) removeApplication(request *si.RemoveApplicationRequest) error {
	if _, err := s.clusterSchedulingContext.removeSchedulingApplication(request.ApplicationID, request.PartitionName); err != nil {
		log.Logger().Error("failed to remove apps",
			zap.String("appID", request.ApplicationID),
			zap.String("partitionName", request.PartitionName),
			zap.Error(err))
		return err
	}

	log.Logger().Info("app removed",
		zap.String("appID", request.ApplicationID),
		zap.String("partitionName", request.PartitionName))
	return nil
}

func enqueueAndCheckFull(queue chan interface{}, ev interface{}) {
	select {
	case queue <- ev:
		log.Logger().Debug("enqueued event",
			zap.String("eventType", reflect.TypeOf(ev).String()),
			zap.Any("event", ev),
			zap.Int("currentQueueSize", len(queue)))
	default:
		log.Logger().DPanic("failed to enqueue event",
			zap.String("event", reflect.TypeOf(ev).String()))
	}
}

// Implement methods for Scheduler events
func (s *Scheduler) HandleEvent(ev interface{}) {
	enqueueAndCheckFull(s.pendingSchedulerEvents, ev)
}

func (s *Scheduler) processAllocationReleaseByAllocationKey(allocationAsksToRelease []*si.AllocationAskReleaseRequest, allocationsToRelease []*si.AllocationReleaseRequest) {
	// For all Requests
	if len(allocationAsksToRelease) > 0 {
		for _, toRelease := range allocationAsksToRelease {
			schedulingApp := s.clusterSchedulingContext.GetSchedulingApplication(toRelease.ApplicationID, toRelease.PartitionName)
			if schedulingApp != nil {
				// remove the allocation asks from the app
				reservedAsks := schedulingApp.removeAllocationAsk(toRelease.Allocationkey)
				log.Logger().Info("release allocation ask",
					zap.String("allocation", toRelease.Allocationkey),
					zap.String("appID", toRelease.ApplicationID),
					zap.String("message", toRelease.Message),
					zap.Int("reservedAskReleased", reservedAsks))
				// update the partition if the asks were reserved (clean up)
				if reservedAsks != 0 {
					s.clusterSchedulingContext.getPartition(toRelease.PartitionName).unReserveUpdate(toRelease.ApplicationID, reservedAsks)
				}
			}
		}
	}

	if len(allocationsToRelease) > 0 {
		toReleaseAllocations := make([]*si.ForgotAllocation, len(allocationAsksToRelease))
		for _, toRelease := range allocationsToRelease {
			schedulingApp := s.clusterSchedulingContext.GetSchedulingApplication(toRelease.ApplicationID, toRelease.PartitionName)
			if schedulingApp != nil {
				for _, alloc := range schedulingApp.ApplicationInfo.GetAllAllocations() {
					if alloc.AllocationProto.UUID == toRelease.UUID {
						toReleaseAllocations = append(toReleaseAllocations, &si.ForgotAllocation{
							AllocationKey: alloc.AllocationProto.AllocationKey,
						})
					}
				}
			}
		}

		// if reconcile plugin is enabled, re-sync the cache now.
		// this gives the chance for the cache to update its memory about assumed pods
		// whenever we release an allocation, we must ensure the corresponding pod is successfully
		// removed from external cache, otherwise predicates will run into problems.
		if len(toReleaseAllocations) > 0 {
			log.Logger().Debug("notify cache to forget assumed pods",
				zap.Int("size", len(toReleaseAllocations)))
			if rp := plugins.GetReconcilePlugin(); rp != nil {
				if err := rp.ReSyncSchedulerCache(&si.ReSyncSchedulerCacheArgs{
					ForgetAllocations: toReleaseAllocations,
				}); err != nil {
					log.Logger().Error("failed to sync cache",
						zap.Error(err))
				}
			}
		}
	}
}

func (s *Scheduler) recoverExistingAllocations(existingAllocations []*si.Allocation, rmID string) {
	// A scheduling cycle for an application takes following steps:
	//  1) Add an application into schedulers and the cache, includes the partition and queue update
	//  2) Add requests to the app (can be included in the same update) updating queues and app itself
	//  3) For pending asks run the scheduling logic to propose an allocation on a node:
	//     Updating the node, queue and app with the proposal
	//     Order for processing: partition -> queue -> app
	//  4) Update data in cache using the proposal (node, queue and app) return a confirmation/reject
	//     This can still cause a reject of the proposal for certain cases
	//  5) Confirm/reject the allocation in the scheduler updating node, queue and app
	// Recovering of existing allocations looks like a replay of the scheduling process. However step 3
	// an 4 are handled directly not via the normal scheduling logic as the node, queue and app are all
	// known. The existing allocations are directly added to the cache.
	for _, alloc := range existingAllocations {
		log.Logger().Info("recovering allocations for app",
			zap.String("applicationID", alloc.ApplicationID),
			zap.String("nodeID", alloc.NodeID),
			zap.String("queueName", alloc.QueueName),
			zap.String("partition", alloc.PartitionName),
			zap.String("allocationKey", alloc.AllocationKey),
			zap.String("allocationId", alloc.UUID))

		// add scheduling asks (step 2 above)
		ask := convertFromAllocation(alloc, rmID)
		if err := s.updateSchedulingRequest(ask); err != nil {
			log.Logger().Warn("app recovery failed to update scheduling request",
				zap.Error(err))
			continue
		}

		// set the scheduler allocation in progress info (step 3)
		if err := s.updateAppAllocating(ask, alloc.NodeID); err != nil {
			log.Logger().Warn("app recovery failed to update allocating information",
				zap.Error(err))
			continue
		}

		// handle allocation proposals (step 5)
		if err := s.confirmAllocationProposal(&commonevents.AllocationProposal{
			NodeID:            alloc.NodeID,
			ApplicationID:     alloc.ApplicationID,
			QueueName:         alloc.QueueName,
			AllocatedResource: resources.NewResourceFromProto(alloc.ResourcePerAlloc),
			AllocationKey:     alloc.AllocationKey,
			Tags:              alloc.AllocationTags,
			Priority:          alloc.Priority,
			PartitionName:     common.GetNormalizedPartitionName(alloc.PartitionName, rmID),
		}); err != nil {
			log.Logger().Error("app recovery failed to confirm allocation proposal",
				zap.Error(err))
			continue
		}
		// all done move the app to running, this can only happen if all updates worked
		// this means that the app must exist (cannot be nil)
		app := s.clusterSchedulingContext.GetSchedulingApplication(ask.ApplicationID, ask.PartitionName)
		app.finishRecovery()
	}
}

func (s *Scheduler) processAllocationUpdateEvent(ev *schedulerevent.SchedulerAllocationUpdatesEvent) {
	if len(ev.ExistingAllocations) > 0 {
		// in recovery mode, we only expect existing allocations being reported
		if len(ev.NewAsks) > 0 || len(ev.RejectedAllocations) > 0 || ev.ToReleases != nil {
			log.Logger().Warn("illegal SchedulerAllocationUpdatesEvent,"+
				" only existingAllocations can be set exclusively, other info will be skipped",
				zap.Int("num of existingAllocations", len(ev.ExistingAllocations)),
				zap.Int("num of rejectedAllocations", len(ev.RejectedAllocations)),
				zap.Int("num of newAsk", len(ev.NewAsks)))
		}
		s.recoverExistingAllocations(ev.ExistingAllocations, ev.RMId)
		return
	}

	// Allocations events cannot contain accepted and rejected allocations at the same time.
	// There should never be more than one accepted allocation in the list
	// See cluster_info.processAllocationProposalEvent()
	if len(ev.AcceptedAllocations) > 0 {
		alloc := ev.AcceptedAllocations[0]
		// Update pending resource
		if err := s.confirmAllocationProposal(alloc); err != nil {
			log.Logger().Error("failed to confirm allocation proposal",
				zap.Error(err))
		}
	}

	// Rejects have not updated the node in the cache but have updated the scheduler with
	// outstanding allocations that need to be removed. Can be multiple in one event.
	if len(ev.RejectedAllocations) > 0 {
		for _, alloc := range ev.RejectedAllocations {
			// Update pending resource back
			if err := s.rejectAllocationProposal(alloc); err != nil {
				log.Logger().Error("failed to reject allocation proposal",
					zap.Error(err))
			}
		}
	}

	// When RM asks to remove some allocations, the event will be send to scheduler first, to release pending asks, etc.
	// Then it will be relay to cache to release allocations.
	// The reason to send to scheduler before cache is, we need to clean up asks otherwise new allocations will be created.
	if ev.ToReleases != nil {
		s.processAllocationReleaseByAllocationKey(ev.ToReleases.AllocationAsksToRelease, ev.ToReleases.AllocationsToRelease)
		s.eventHandlers.CacheEventHandler.HandleEvent(cacheevent.NewReleaseAllocationEventFromProto(ev.ToReleases.AllocationsToRelease))
	}

	if len(ev.NewAsks) > 0 {
		rejectedAsks := make([]*si.RejectedAllocationAsk, 0)

		var rmID = ""
		for _, ask := range ev.NewAsks {
			rmID = common.GetRMIdFromPartitionName(ask.PartitionName)
			schedulingAsk := newSchedulingAllocationAsk(ask)
			if err := s.updateSchedulingRequest(schedulingAsk); err != nil {
				rejectedAsks = append(rejectedAsks, &si.RejectedAllocationAsk{
					AllocationKey: schedulingAsk.AskProto.AllocationKey,
					ApplicationID: schedulingAsk.ApplicationID,
					Reason:        err.Error()})
			}
		}

		// Reject asks to RM Proxy
		if len(rejectedAsks) > 0 {
			s.eventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMRejectedAllocationAskEvent{
				RejectedAllocationAsks: rejectedAsks,
				RmID:                   rmID,
			})
		}
	}
}

// Process application adds and removes that have been processed by the cache.
// The cache processes the applications and has already filtered out some apps.
// All apps come from one si.UpdateRequest and thus from one RM.
func (s *Scheduler) processApplicationUpdateEvent(ev *schedulerevent.SchedulerApplicationsUpdateEvent) {
	if len(ev.AddedApplications) > 0 {
		rejectedApps := make([]*si.RejectedApplication, 0)
		acceptedApps := make([]*si.AcceptedApplication, 0)
		var rmID string
		for _, j := range ev.AddedApplications {
			app, ok := j.(*cache.ApplicationInfo)
			// if the cast failed we do not have the correct object, skip it
			if !ok {
				log.Logger().Debug("cast failed unexpected object in event",
					zap.Any("ApplicationInfo", j))
				continue
			}
			rmID = common.GetRMIdFromPartitionName(app.Partition)
			if err := s.addNewApplication(app); err != nil {
				log.Logger().Debug("rejecting application in scheduler",
					zap.String("appID", app.ApplicationID),
					zap.String("partitionName", app.Partition),
					zap.Error(err))
				// update cache
				s.eventHandlers.CacheEventHandler.HandleEvent(
					&cacheevent.RejectedNewApplicationEvent{
						ApplicationID: app.ApplicationID,
						PartitionName: app.Partition,
						Reason:        err.Error(),
					})
				rejectedApps = append(rejectedApps, &si.RejectedApplication{
					ApplicationID: app.ApplicationID,
					Reason:        err.Error(),
				})
				// app is rejected by the scheduler
				err = app.HandleApplicationEvent(cache.RejectApplication)
				if err != nil {
					log.Logger().Debug("cache event handling error returned",
						zap.Error(err))
				}
			} else {
				acceptedApps = append(acceptedApps, &si.AcceptedApplication{
					ApplicationID: app.ApplicationID,
				})
			}
		}
		// notify RM proxy about apps added and rejected
		s.eventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMApplicationUpdateEvent{
			RmID:                 rmID,
			AcceptedApplications: acceptedApps,
			RejectedApplications: rejectedApps,
		})
	}

	if len(ev.RemovedApplications) > 0 {
		for _, app := range ev.RemovedApplications {
			err := s.removeApplication(app)

			if err != nil {
				log.Logger().Error("failed to remove app from partition",
					zap.String("appID", app.ApplicationID),
					zap.String("partitionName", app.PartitionName),
					zap.Error(err))
				continue
			}
			s.eventHandlers.CacheEventHandler.HandleEvent(&cacheevent.RemovedApplicationEvent{ApplicationID: app.ApplicationID, PartitionName: app.PartitionName})
		}
	}
}

func (s *Scheduler) removePartitionsBelongToRM(event *commonevents.RemoveRMPartitionsEvent) {
	s.clusterSchedulingContext.RemoveSchedulingPartitionsByRMId(event.RmID)

	// Send this event to cache
	s.eventHandlers.CacheEventHandler.HandleEvent(event)
}

func (s *Scheduler) processUpdatePartitionConfigsEvent(event *schedulerevent.SchedulerUpdatePartitionsConfigEvent) {
	partitions := make([]*cache.PartitionInfo, 0)
	for _, p := range event.UpdatedPartitions {
		partition, ok := p.(*cache.PartitionInfo)
		if !ok {
			log.Logger().Debug("cast failed unexpected object in partition update event",
				zap.Any("PartitionInfo", p))
		}
		partitions = append(partitions, partition)
	}

	if err := s.clusterSchedulingContext.updateSchedulingPartitions(partitions); err != nil {
		event.ResultChannel <- &commonevents.Result{
			Succeeded: false,
			Reason:    err.Error(),
		}
	} else {
		event.ResultChannel <- &commonevents.Result{
			Succeeded: true,
		}
	}
}

func (s *Scheduler) processDeletePartitionConfigsEvent(event *schedulerevent.SchedulerDeletePartitionsConfigEvent) {
	partitions := make([]*cache.PartitionInfo, 0)
	for _, p := range event.DeletePartitions {
		partition, ok := p.(*cache.PartitionInfo)
		if !ok {
			log.Logger().Debug("cast failed unexpected object in partition delete event",
				zap.Any("PartitionInfo", p))
		}
		partitions = append(partitions, partition)
	}

	if err := s.clusterSchedulingContext.deleteSchedulingPartitions(partitions); err != nil {
		event.ResultChannel <- &commonevents.Result{
			Succeeded: false,
			Reason:    err.Error(),
		}
	} else {
		event.ResultChannel <- &commonevents.Result{
			Succeeded: true,
		}
	}
}

// Add a scheduling node based on the node added to the cache.
func (s *Scheduler) processNodeEvent(event *schedulerevent.SchedulerNodeEvent) {
	// process the node addition (one per event)
	if event.AddedNode != nil {
		nodeInfo, ok := event.AddedNode.(*cache.NodeInfo)
		if !ok {
			log.Logger().Debug("cast failed unexpected object in node add event",
				zap.Any("NodeInfo", event.AddedNode))
		}
		s.clusterSchedulingContext.addSchedulingNode(nodeInfo)
	}
	// process the node deletion (one per event)
	if event.RemovedNode != nil {
		nodeInfo, ok := event.RemovedNode.(*cache.NodeInfo)
		if !ok {
			log.Logger().Debug("cast failed unexpected object in node remove event",
				zap.Any("NodeInfo", event.RemovedNode))
		}
		s.clusterSchedulingContext.removeSchedulingNode(nodeInfo)
	}
	// preempted resources have now been released update the node
	if event.PreemptedNodeResources != nil {
		s.clusterSchedulingContext.releasePreemptedResources(event.PreemptedNodeResources)
	}
	// update node resources
	if event.UpdateNode != nil {
		nodeInfo, ok := event.UpdateNode.(*cache.NodeInfo)
		if !ok {
			log.Logger().Debug("cast failed unexpected object in node update event",
				zap.Any("NodeInfo", event.UpdateNode))
		}
		s.clusterSchedulingContext.updateSchedulingNode(nodeInfo)
	}
}

func (s *Scheduler) handleSchedulerEvent() {
	for {
		ev := <-s.pendingSchedulerEvents
		switch v := ev.(type) {
		case *schedulerevent.SchedulerNodeEvent:
			s.processNodeEvent(v)
		case *schedulerevent.SchedulerAllocationUpdatesEvent:
			s.processAllocationUpdateEvent(v)
		case *schedulerevent.SchedulerApplicationsUpdateEvent:
			s.processApplicationUpdateEvent(v)
		case *commonevents.RemoveRMPartitionsEvent:
			s.removePartitionsBelongToRM(v)
		case *schedulerevent.SchedulerUpdatePartitionsConfigEvent:
			s.processUpdatePartitionConfigsEvent(v)
		case *schedulerevent.SchedulerDeletePartitionsConfigEvent:
			s.processDeletePartitionConfigsEvent(v)
		default:
			log.Logger().Error("Received type is not an acceptable type for Scheduler event.",
				zap.String("Received type", reflect.TypeOf(v).String()))
		}
	}
}

// Visible by tests
func (s *Scheduler) GetClusterSchedulingContext() *ClusterSchedulingContext {
	return s.clusterSchedulingContext
}

// The scheduler for testing which runs nAlloc times the normal schedule routine.
// Visible by tests
func (s *Scheduler) MultiStepSchedule(nAlloc int) {
	for i := 0; i < nAlloc; i++ {
		log.Logger().Debug("Scheduler manual stepping",
			zap.Int("count", i))
		s.schedule()

		// sometimes the smoke tests are failing because they are competing CPU resources.
		// each scheduling cycle, let's sleep for a small amount of time (100ms),
		// this can ensure even CPU is intensive, the main thread can give up some CPU time
		// for other go routines to process, such as event handling routines.
		// Note, this sleep only works in tests.
		time.Sleep(100 * time.Millisecond)
	}
}

// The main scheduling routine.
// Process each partition in the scheduler, walk over each queue and app to check if anything can be scheduled.
func (s *Scheduler) schedule() {
	// schedule each partition defined in the cluster
	for _, psc := range s.clusterSchedulingContext.getPartitionMapClone() {
		// if there are no resources in the partition just skip
		if psc.root.getMaxResource() == nil {
			continue
		}
		// try reservations first: gets back a node ID if the allocation occurs on a node
		// that was not reserved by the app/ask
		alloc := psc.tryReservedAllocate()
		// nothing reserved that can be allocated try normal allocate
		if alloc == nil {
			alloc = psc.tryAllocate()
		}
		// there is an allocation that can be made do the real work in the partition
		if alloc != nil {
			// only pass back a real allocation, reservations are just scheduler side
			// proposal this will return to the scheduler an SchedulerApplicationsUpdateEvent when the
			// is processed by the cache (this can be a reject or accept)
			// nodeID is an empty string in all but reserved alloc cases
			if psc.allocate(alloc) {
				s.eventHandlers.CacheEventHandler.HandleEvent(newSingleAllocationProposal(alloc))
			}
		}
	}
}

// Retrieve the app and node to set the allocating resources on when recovering allocations
func (s *Scheduler) updateAppAllocating(ask *schedulingAllocationAsk, nodeID string) error {
	app := s.clusterSchedulingContext.GetSchedulingApplication(ask.ApplicationID, ask.PartitionName)
	if app == nil {
		return fmt.Errorf("cannot find scheduling application on allocation recovery %s", ask.ApplicationID)
	}
	node := s.clusterSchedulingContext.GetSchedulingNode(nodeID, ask.PartitionName)
	if node == nil {
		return fmt.Errorf("cannot find scheduling node on allocation recovery %s", nodeID)
	}
	log.Logger().Debug("updating allocating for application, queue and node",
		zap.String("allocKey", ask.AskProto.AllocationKey),
		zap.String("nodeID", node.NodeID),
		zap.String("appID", ask.ApplicationID),
		zap.String("queueName", ask.QueueName))
	app.recoverOnNode(node, ask)
	return nil
}
