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
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	siCommon "github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/common"
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
	clusterInfo            *cache.ClusterInfo    // link to the cache object
	preemptionContext      *preemptionContext    // Preemption context
	eventHandlers          handler.EventHandlers // list of event handlers
	pendingSchedulerEvents chan interface{}      // queue for scheduler events
	partitions             map[string]*PartitionSchedulingContext
	needPreemption         bool
	policyGroup            string

	// Event queues
	pendingRmEvents chan interface{}

	sync.RWMutex
}

// FIXME: need better create the constructor
func NewScheduler(clusterInfo *cache.ClusterInfo) *Scheduler {
	s := &Scheduler{}
	s.clusterInfo = clusterInfo
	s.clusterSchedulingContext = NewClusterSchedulingContext()
	s.pendingSchedulerEvents = make(chan interface{}, 1024*1024)
	return s
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
	for _, psc := range s.getPartitionMapClone() {
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
	app := s.GetSchedulingApplication(schedulingAsk.ApplicationID, schedulingAsk.PartitionName)
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
	partition := s.getPartition(allocProposal.PartitionName)
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
		event := &cacheevent.ReleaseAllocationsEvent{AllocationsToRelease: make([]*ReleaseAllocation, 0)}
		event.AllocationsToRelease = append(event.AllocationsToRelease, commonevents.NewReleaseAllocation(allocProposal.UUID, allocProposal.ApplicationID, allocProposal.PartitionName, "allocation confirmation failure (ask removal)", si.AllocationReleaseResponse_STOPPED_BY_RM))

		s.eventHandlers.CacheEventHandler.HandleEvent(event)
	}
	return err
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

func (s *Scheduler) processAllocationAsksToRelease(allocationAsksToRelease []*si.AllocationAskReleaseRequest) {
	// For all Requests
	if len(allocationAsksToRelease) > 0 {
		for _, toRelease := range allocationAsksToRelease {
			schedulingApp := s.GetSchedulingApplication(toRelease.ApplicationID, toRelease.PartitionName)
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
					s.getPartition(toRelease.PartitionName).unReserveUpdate(toRelease.ApplicationID, reservedAsks)
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
		app := s.GetSchedulingApplication(ask.ApplicationID, ask.PartitionName)
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
}

func (s *Scheduler) removePartitionsBelongToRM(event *commonevents.RemoveRMPartitionsEvent) {
	s.RemoveSchedulingPartitionsByRMId(event.RmID)

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

	if err := s.updateSchedulingPartitions(partitions); err != nil {
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

	if err := s.deleteSchedulingPartitions(partitions); err != nil {
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

func (s *Scheduler) handleSchedulerEvent() {
	for {
		ev := <-s.pendingSchedulerEvents
		switch v := ev.(type) {
		case *schedulerevent.SchedulerNodeEvent:
			s.processNodeEvent(v)
		case *schedulerevent.SchedulerAllocationUpdatesEvent:
			s.processAllocationUpdateEvent(v)
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
	for _, psc := range s.getPartitionMapClone() {
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
			// Do allocate and commit
			psc.allocate(alloc)
		}
	}
}

// Retrieve the app and node to set the allocating resources on when recovering allocations
func (s *Scheduler) updateAppAllocating(ask *schedulingAllocationAsk, nodeID string) error {
	app := s.GetSchedulingApplication(ask.ApplicationID, ask.PartitionName)
	if app == nil {
		return fmt.Errorf("cannot find scheduling application on allocation recovery %s", ask.ApplicationID)
	}
	node := s.GetSchedulingNode(nodeID, ask.PartitionName)
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

func (s *Scheduler) handleSchedulerEvents_OriginalCache() {
	for {
		ev := <-s.pendingSchedulerEvents
		switch v := ev.(type) {
		case *cacheevent.AllocationProposalBundleEvent:
			s.processAllocationProposalEvent(v)
		default:
			log.Logger().Error("Received type is not an acceptable type for scheduler event.",
				zap.String("received type", reflect.TypeOf(v).String()))
		}
	}
}

func (s *Scheduler) handleRMEvents() {
	for {
		ev := <-s.pendingRmEvents
		switch v := ev.(type) {
		case *cacheevent.RMUpdateRequestEvent:
			s.processRMUpdateEvent(v)
		case *commonevents.RegisterRMEvent:
			s.processRMRegistrationEvent(v)
		case *commonevents.ConfigUpdateRMEvent:
			s.processRMConfigUpdateEvent(v)
		case *commonevents.RemoveRMPartitionsEvent:
			s.processRemoveRMPartitionsEvent(v)
		default:
			log.Logger().Error("Received type is not an acceptable type for RM event.",
				zap.String("received type", reflect.TypeOf(v).String()))
		}
	}
}

// Implement methods for Cache events
func (s *Scheduler) HandleEvent(ev interface{}) {
	switch v := ev.(type) {
	case *cacheevent.AllocationProposalBundleEvent:
		enqueueAndCheckFull(s.pendingSchedulerEvents, v)
	case *cacheevent.ReleaseAllocationsEvent:
		enqueueAndCheckFull(s.pendingSchedulerEvents, v)
	case *commonevents.RemoveRMPartitionsEvent:
		enqueueAndCheckFull(s.pendingSchedulerEvents, v)
	case *cacheevent.RMUpdateRequestEvent:
		enqueueAndCheckFull(s.pendingRmEvents, v)
	case *commonevents.RegisterRMEvent:
		enqueueAndCheckFull(s.pendingRmEvents, v)
	case *commonevents.ConfigUpdateRMEvent:
		enqueueAndCheckFull(s.pendingRmEvents, v)
	default:
		log.Logger().Error("Received unexpected event type.",
			zap.String("Received event type", reflect.TypeOf(v).String()))
	}
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

// Get the config name.
// Locked call, used outside of the cache.
func (s *Scheduler) GetPolicyGroup() string {
	s.RLock()
	defer s.RUnlock()
	return s.policyGroup
}

// Get the list of partitions.
// Locked call, used outside of the cache.
func (s *Scheduler) ListPartitions() []string {
	s.RLock()
	defer s.RUnlock()
	var partitions []string
	for k := range s.partitions {
		partitions = append(partitions, k)
	}
	return partitions
}

// Get the partition by name.
// Locked call, used outside of the cache.
func (s *Scheduler) GetPartition(name string) *PartitionSchedulingContext {
	s.RLock()
	defer s.RUnlock()
	return s.partitions[name]
}

// Add a new partition to the cluster, locked.
// Called by the configuration update.
func (s *Scheduler) addPartition(name string, partition *PartitionSchedulingContext) {
	s.Lock()
	defer s.Unlock()
	s.partitions[name] = partition
}

// Remove a partition from the cluster, locked.
// Called by the partition itself when the partition is removed by the partition manager.
func (s *Scheduler) removePartition(name string) {
	s.Lock()
	defer s.Unlock()
	delete(s.partitions, name)
}

// Process the application update. Add and remove applications from the partitions.
// Lock free call, all updates occur on the underlying partition which is locked, or via events.
func (s *Scheduler) processApplicationUpdateFromRMUpdate(request *si.UpdateRequest) {
	if len(request.NewApplications) == 0 && len(request.RemoveApplications) == 0 {
		return
	}
	rejectedApps := make([]*si.RejectedApplication, 0)

	for _, app := range request.NewApplications {
		partitionInfo := s.GetPartition(app.PartitionName)
		if partitionInfo == nil {
			msg := fmt.Sprintf("Failed to add application %s to partition %s, partition doesn't exist", app.ApplicationID, app.PartitionName)
			log.Logger().Info(msg)
			rejectedApps = append(rejectedApps, &si.RejectedApplication{
				ApplicationID: app.ApplicationID,
				Reason:        msg,
			})
			continue
		}
		// convert and resolve the user: cache can be set per partition
		ugi, err := partitionInfo.convertUGI(app.Ugi)
		if err != nil {
			rejectedApps = append(rejectedApps, &si.RejectedApplication{
				ApplicationID: app.ApplicationID,
				Reason:        err.Error(),
			})
			continue
		}
		// create a new app object and add it to the partition (partition logs details)
		appInfo := NewSchedulingApp(app.ApplicationID, app.PartitionName, app.QueueName, ugi, app.Tags,
			s.eventHandlers, request.RmID)
		if err = partitionInfo.addSchedulingApplication(appInfo); err != nil {
			rejectedApps = append(rejectedApps, &si.RejectedApplication{
				ApplicationID: app.ApplicationID,
				Reason:        err.Error(),
			})
			continue
		}
	}

	// Respond to RMProxy with already rejected apps if needed
	if len(rejectedApps) > 0 {
		s.eventHandlers.RMProxyEventHandler.HandleEvent(
			&rmevent.RMApplicationUpdateEvent{
				RmID:                 request.RmID,
				AcceptedApplications: make([]*si.AcceptedApplication, 0),
				RejectedApplications: rejectedApps,
			})
	}

	if len(request.RemoveApplications) > 0 {
		metrics.GetSchedulerMetrics().SubTotalApplicationsRunning(len(request.RemoveApplications))
		// ToDO: need to improve this once we have state in YuniKorn for apps.
		metrics.GetSchedulerMetrics().AddTotalApplicationsCompleted(len(request.RemoveApplications))

		for _, app := range request.RemoveApplications {
			s.removeSchedulingApplication(app.ApplicationID, app.PartitionName)
		}
	}
}

// Process the allocation updates. Add and remove allocations for the applications.
// Lock free call, all updates occur on the underlying application which is locked or via events.
func (s *Scheduler) processNewAndReleaseAllocationRequests(request *si.UpdateRequest) {
	// When RM asks to remove some allocations, the event will be send to scheduler first, to release pending asks, etc.
	// Then it will be relay to cache to release allocations.
	// The reason to send to scheduler before cache is, we need to clean up asks otherwise new allocations will be created.
	toReleases := request.Releases
	newAsks := request.Asks

	if toReleases!= nil {
		s.processAllocationAsksToRelease(toReleases.AllocationAsksToRelease)
		// FIXME: move the event NewReleaseAllocationEventFromProto to a normal structure to scheduler
		s.processAllocationReleases(cacheevent.NewReleaseAllocationEventFromProto(toReleases.AllocationsToRelease).AllocationsToRelease)
	}

	if len(newAsks) > 0 {
		rejectedAsks := make([]*si.RejectedAllocationAsk, 0)

		var rmID = ""
		for _, ask := range newAsks {
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

func (s *Scheduler) processNewSchedulableNodes(request *si.UpdateRequest) {
	acceptedNodes := make([]*si.AcceptedNode, 0)
	rejectedNodes := make([]*si.RejectedNode, 0)
	for _, node := range request.NewSchedulableNodes {
		nodeInfo := newSchedulingNode(node)
		partition := s.GetPartition(nodeInfo.Partition)
		if partition == nil {
			msg := fmt.Sprintf("Failed to find partition %s for new node %s", nodeInfo.Partition, node.NodeID)
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
		err := partition.addNewNode(nodeInfo, node.ExistingAllocations)
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
			zap.String("partition", nodeInfo.Partition))
	}

	// inform the RM which nodes have been accepted
	s.eventHandlers.RMProxyEventHandler.HandleEvent(
		&rmevent.RMNodeUpdateEvent{
			RmID:          request.RmID,
			AcceptedNodes: acceptedNodes,
			RejectedNodes: rejectedNodes,
		})
}

// RM may notify us to remove, blacklist or whitelist a node,
// this is to process such actions.
func (s *Scheduler) processNodeActions(request *si.UpdateRequest) {
	for _, update := range request.UpdatedNodes {
		var partition *PartitionSchedulingContext
		if p, ok := update.Attributes[siCommon.NodePartition]; ok {
			partition = s.GetPartition(p)
		} else {
			log.Logger().Debug("node partition not specified",
				zap.String("nodeID", update.NodeID),
				zap.String("nodeAction", update.Action.String()))
			continue
		}

		if partition == nil {
			continue
		}

		if nodeInfo, ok := partition.nodes[update.NodeID]; ok {
			switch update.Action {
			case si.UpdateNodeInfo_UPDATE:
				if sr := update.SchedulableResource; sr != nil {
					newCapacity := resources.NewResourceFromProto(sr)
					nodeInfo.setCapacity(newCapacity)
				}
				if or := update.OccupiedResource; or != nil {
					newOccupied := resources.NewResourceFromProto(or)
					nodeInfo.setOccupiedResource(newOccupied)
				}
				s.eventHandlers.SchedulerEventHandler.HandleEvent(
					&schedulerevent.SchedulerNodeEvent{
						UpdateNode: nodeInfo,
					})
			case si.UpdateNodeInfo_DRAIN_NODE:
				// set the state to not schedulable
				nodeInfo.SetSchedulable(false)
			case si.UpdateNodeInfo_DRAIN_TO_SCHEDULABLE:
				// set the state to schedulable
				nodeInfo.SetSchedulable(true)
			case si.UpdateNodeInfo_DECOMISSION:
				// set the state to not schedulable then tell the partition to clean up
				nodeInfo.SetSchedulable(false)
				partition.removeSchedulingNode(nodeInfo.NodeID)
				// remove the equivalent scheduling node
				s.eventHandlers.SchedulerEventHandler.HandleEvent(
					&schedulerevent.SchedulerNodeEvent{
						RemovedNode: nodeInfo,
					})
			}
		}
	}
}

// Process the node updates: add and remove nodes as needed.
// Lock free call, all updates occur on the underlying node which is locked or via events.
func (s *Scheduler) processNodeUpdate(request *si.UpdateRequest) {
	// Process add node
	if len(request.NewSchedulableNodes) > 0 {
		s.processNewSchedulableNodes(request)
	}

	if len(request.UpdatedNodes) > 0 {
		s.processNodeActions(request)
	}
}

// Process RM event internally. Split in steps that handle specific parts.
// Lock free call, all updates occur in other methods.
func (s *Scheduler) processRMUpdateEvent(event *cacheevent.RMUpdateRequestEvent) {
	// Order of following operations are important,
	// don't change unless carefully thought
	request := event.Request
	// 1) Add / remove app requested by RM.
	s.processApplicationUpdateFromRMUpdate(request)
	// 2) Add new request, release allocation, cancel request
	s.processNewAndReleaseAllocationRequests(request)
	// 3) Add / remove / update Nodes
	s.processNodeUpdate(request)
}

func (s *Scheduler) updateSchedulerConfig(newConfig *configs.SchedulerConfig, rmId string) error {
	updatedPartitions := make([]*PartitionSchedulingContext, 0)
	for _, partitionConf := range newConfig.Partitions {
		updatedPartition, err := newSchedulingPartitionFromConfig(partitionConf, rmId, s.eventHandlers)
		if err != nil {
			log.Logger().Error("failed to initialize partition during config update",
				zap.String("partition", partitionConf.Name),
				zap.Error(err))
			return err
		}
		updatedPartitions = append(updatedPartitions, updatedPartition)
	}

	// Get deleted partitions
	deletedPartitions := make([]string, 0)
	for _, p := range updatedPartitions {
		if _, ok := s.partitions[p.Name]; !ok {
			deletedPartitions = append(deletedPartitions, p.Name)
		}
	}

	// Update partitions first
	if err := s.updateSchedulingPartitions(updatedPartitions); err != nil {
		return err
	}

	// Handle deleted partitions
	if err := s.deleteSchedulingPartitions(deletedPartitions); err != nil {
		return err
	}

	return nil
}

func (s *Scheduler) processRMRegistrationEvent(event *commonevents.RegisterRMEvent) {
	rmId := event.RMRegistrationRequest.RmID
	policyGroup := event.RMRegistrationRequest.PolicyGroup

	s.Lock()
	defer s.Unlock()

	// we should not have any partitions set at this point
	if len(s.partitions) > 0 {
		event.Channel <- &commonevents.Result{
			Succeeded: false,
			Reason:    fmt.Errorf("RM %s has been registered before, active partitions %d", rmId, len(s.partitions)).Error()}
	}

	// load the config this returns a validated configuration
	conf, err := s.loadSchedulerConfigAndNotifyError(rmId, policyGroup, event.Channel)

	if err != nil {
		return
	}

	if err = s.updateSchedulerConfig(conf, rmId); err != nil {
		event.Channel <- &commonevents.Result{
			Succeeded: false,
			Reason:    fmt.Errorf("failed to update scheduler config, err=%s", err.Error()).Error()}
	}

	s.policyGroup = policyGroup
	configs.ConfigContext.Set(policyGroup, conf)

	event.Channel <- &commonevents.Result{Succeeded: true, Reason: ""}
}

func (s *Scheduler) loadSchedulerConfigAndNotifyError(rmId, policyGroup string, resultChannel chan *commonevents.Result) (*configs.SchedulerConfig, error) {
	// load the config this returns a validated configuration
	conf, err := configs.SchedulerConfigLoader(policyGroup)

	if err != nil {
		resultChannel <- &commonevents.Result{
			Succeeded: false,
			Reason:    fmt.Errorf("failed to load scheduler config, err=%s", err.Error()).Error()}
		return nil, err
	}

	// Normalize partition name before return
	for _, p := range conf.Partitions {
		partitionName := common.GetNormalizedPartitionName(p.Name, rmId)
		p.Name = partitionName
	}

	return conf, nil
}

func (s *Scheduler) processRMConfigUpdateEvent(event *commonevents.ConfigUpdateRMEvent) {
	s.Lock()
	defer s.Unlock()

	conf, err := s.loadSchedulerConfigAndNotifyError(event.RmID, s.policyGroup, event.Channel)

	if err != nil {
		return
	}

	if err = s.updateSchedulerConfig(conf, event.RmID); err != nil {
		event.Channel <- &commonevents.Result{
			Succeeded: false,
			Reason:    fmt.Errorf("failed to update scheduler config, err=%s", err.Error()).Error()}
	}

	event.Channel <- &commonevents.Result{Succeeded: true, Reason: ""}
}

// Process an allocation bundle which could contain release and allocation proposals.
// Lock free call, all updates occur on the underlying partition which is locked or via events.
func (s *Scheduler) processAllocationProposalEvent(event *cacheevent.AllocationProposalBundleEvent) {
	// Release if there is anything to release
	if len(event.ReleaseProposals) > 0 {
		s.processAllocationReleases(event.ReleaseProposals)
	}
	// Skip allocation if nothing here.
	if len(event.AllocationProposals) == 0 {
		return
	}

	// we currently only support 1 allocation in the list, reject all but the first
	if len(event.AllocationProposals) != 1 {
		log.Logger().Info("More than 1 allocation proposal rejected all but first",
			zap.Int("allocPropLength", len(event.AllocationProposals)))
	}

	// just process the first the rest is rejected already
	proposal := event.AllocationProposals[0]
	partitionInfo := s.GetPartition(proposal.PartitionName)
	allocInfo, err := partitionInfo.addNewAllocation(proposal)
	if err != nil {
		log.Logger().Error("failed to add new allocation to partition",
			zap.String("partition", partitionInfo.Name),
			zap.String("allocationKey", proposal.AllocationKey),
			zap.Error(err))
		return
	}

	log.Logger().Info("allocation accepted",
		zap.String("appID", proposal.ApplicationID),
		zap.String("queue", proposal.QueueName),
		zap.String("partition", proposal.PartitionName),
		zap.String("allocationKey", proposal.AllocationKey))

	// Send allocation event to RM: rejects are not passed back
	rmID := common.GetRMIdFromPartitionName(proposal.PartitionName)
	s.eventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMNewAllocationsEvent{
		Allocations: []*si.Allocation{allocInfo.AllocationProto},
		RmID:        rmID,
	})
}

// Process the allocations to release.
// Lock free call, all updates occur via events.
func (s *Scheduler) processAllocationReleases(toReleases []*ReleaseAllocation) {
	// Try to release allocations specified in the events
	for _, toReleaseAllocation := range toReleases {
		partitionInfo := s.GetPartition(toReleaseAllocation.PartitionName)
		if partitionInfo == nil {
			log.Logger().Info("Failed to find partition for allocation proposal",
				zap.String("partitionName", toReleaseAllocation.PartitionName))
			continue
		}
		// release the allocation from the partition
		partitionInfo.releaseAllocationsForApplication(toReleaseAllocation)
	}
}

func (s *Scheduler) processRemoveRMPartitionsEvent(event *commonevents.RemoveRMPartitionsEvent) {
	s.Lock()
	defer s.Unlock()

	deletedPartitions := make([]string, 0)
	for partition, partitionContext := range s.partitions {
		if partitionContext.RmID == event.RmID {
			deletedPartitions = append(deletedPartitions, partition)
		}
	}

	// Handle deleted partitions
	if err := s.deleteSchedulingPartitions(deletedPartitions); err != nil {
		// Done, notify channel
		event.Channel <- &commonevents.Result{
			Succeeded: false,
			Reason:    err.Error(),
		}
	}

	// Done, notify channel
	event.Channel <- &commonevents.Result{
		Succeeded: true,
	}
}

func (s *Scheduler) getPartitionMapClone() map[string]*PartitionSchedulingContext {
	s.RLock()
	defer s.RUnlock()

	newMap := make(map[string]*PartitionSchedulingContext)
	for k, v := range s.partitions {
		newMap[k] = v
	}
	return newMap
}

func (s *Scheduler) getPartition(partitionName string) *PartitionSchedulingContext {
	s.RLock()
	defer s.RUnlock()

	return s.partitions[partitionName]
}

// Get the scheduling application based on the ID from the partition.
// Returns nil if the partition or app cannot be found.
// Visible for tests
func (s *Scheduler) GetSchedulingApplication(appID, partitionName string) *SchedulingApplication {
	s.RLock()
	defer s.RUnlock()

	if partition := s.partitions[partitionName]; partition != nil {
		return partition.getApplication(appID)
	}

	return nil
}

// Get the scheduling queue based on the queue path name from the partition.
// Returns nil if the partition or queue cannot be found.
// Visible for tests
func (s *Scheduler) GetSchedulingQueue(queueName string, partitionName string) *SchedulingQueue {
	s.RLock()
	defer s.RUnlock()

	if partition := s.partitions[partitionName]; partition != nil {
		return partition.GetQueue(queueName)
	}

	return nil
}

// Return the list of reservations for the partition.
// Returns nil if the partition cannot be found or an empty map if there are no reservations
// Visible for tests
func (s *Scheduler) GetPartitionReservations(partitionName string) map[string]int {
	s.RLock()
	defer s.RUnlock()

	if partition := s.partitions[partitionName]; partition != nil {
		return partition.getReservations()
	}

	return nil
}

func (s *Scheduler) addSchedulingApplication(schedulingApp *SchedulingApplication) error {
	partitionName := schedulingApp.Partition
	appID := schedulingApp.ApplicationID

	s.Lock()
	defer s.Unlock()

	if partition := s.partitions[partitionName]; partition != nil {
		if err := partition.addSchedulingApplication(schedulingApp); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find partition=%s while adding app=%s", partitionName, appID)
	}

	return nil
}

func (s *Scheduler) removeSchedulingApplication(appID string, partitionName string) []*schedulingAllocation {
	s.Lock()
	defer s.Unlock()

	if partition := s.partitions[partitionName]; partition != nil {
		return partition.removeApplication(appID)
	}

	return nil
}

// Update the scheduler's partition list based on the processed config
// - updates existing partitions and the queues linked
// - add new partitions including queues
// updates and add internally are processed differently outside of this method they are the same.
func (s *Scheduler) updateSchedulingPartitions(partitions []*PartitionSchedulingContext) error {
	s.Lock()
	defer s.Unlock()
	log.Logger().Info("updating scheduler context",
		zap.Int("numOfPartitionsUpdated", len(partitions)))

	// Walk over the updated partitions
	for _, updatedPartition := range partitions {
		s.needPreemption = s.needPreemption || updatedPartition.NeedPreemption()

		partition := s.partitions[updatedPartition.Name]
		if partition != nil {
			log.Logger().Info("updating scheduling partition",
				zap.String("partitionName", updatedPartition.Name))
			// the partition details don't need updating just the queues
			partition.updatePartitionSchedulingContext(updatedPartition)
		} else {
			log.Logger().Info("creating scheduling partition",
				zap.String("partitionName", updatedPartition.Name))
			// create a new partition and add the queues
			go updatedPartition.partitionManager.Run()

			s.partitions[updatedPartition.Name] = updatedPartition
		}
	}
	return nil
}

func (s *Scheduler) RemoveSchedulingPartitionsByRMId(rmID string) {
	s.Lock()
	defer s.Unlock()
	partitionToRemove := make(map[string]bool)

	// Just remove corresponding partitions
	for k, partition := range s.partitions {
		if partition.RmID == rmID {
			partition.partitionManager.stop = true
			partitionToRemove[k] = true
		}
	}

	for partitionName := range partitionToRemove {
		delete(s.partitions, partitionName)
	}
}

// Remove the partition from the scheduler based on a configuration change
// No resources can be used and the underlying partition should not be running
func (s *Scheduler) deleteSchedulingPartitions(partitions []string) error {
	s.Lock()
	defer s.Unlock()

	var err error
	// Walk over the deleted partitions
	for _, deletedPartition := range partitions {
		partition := s.partitions[deletedPartition]
		if partition != nil {
			log.Logger().Info("marking scheduling partition for deletion",
				zap.String("partitionName", deletedPartition))
			partition.partitionManager.Stop()
		} else {
			// collect all errors and keep processing
			if err == nil {
				err = fmt.Errorf("failed to find partition that should have been deleted: %s", deletedPartition)
			} else {
				err = fmt.Errorf("%v, %s", err, deletedPartition)
			}
		}
	}
	return err
}

func (s *Scheduler) NeedPreemption() bool {
	s.RLock()
	defer s.RUnlock()

	return s.needPreemption
}

// Callback from the partition manager to finalise the removal of the partition
func (s *Scheduler) removeSchedulingPartition(partitionName string) {
	s.RLock()
	defer s.RUnlock()

	delete(s.partitions, partitionName)
}

// Get a scheduling node based on its name from the partition.
// Returns nil if the partition or node cannot be found.
// Visible for tests
func (s *Scheduler) GetSchedulingNode(nodeID, partitionName string) *SchedulingNode {
	s.Lock()
	defer s.Unlock()

	partition := s.partitions[partitionName]
	if partition == nil {
		log.Logger().Info("partition not found for scheduling node",
			zap.String("nodeID", nodeID),
			zap.String("partitionName", partitionName))
		return nil
	}
	return partition.getSchedulingNode(nodeID)
}

// Release preempted resources after the cache has been updated.
// This is a lock free call: locks are taken while retrieving the node and when updating the node
func (s *Scheduler) releasePreemptedResources(resources []schedulerevent.PreemptedNodeResource) {
	// no resources to release just return
	if len(resources) == 0 {
		return
	}
	// walk over the list of preempted resources
	for _, nodeRes := range resources {
		node := s.GetSchedulingNode(nodeRes.NodeID, nodeRes.Partition)
		if node == nil {
			log.Logger().Info("scheduling node not found trying to release preempted resources",
				zap.String("nodeID", nodeRes.NodeID),
				zap.String("partitionName", nodeRes.Partition),
				zap.Any("resource", nodeRes.PreemptedRes))
			continue
		}
		node.decPreemptingResource(nodeRes.PreemptedRes)
	}
}
