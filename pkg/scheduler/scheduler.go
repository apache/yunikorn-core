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
	lock                     sync.RWMutex

	// Wait till next try
	// (It is designed to be accessed under a single goroutine, no lock needed).
	// This field has dual purposes:
	// 1) When a request is picky, scheduler will increase this value for every failed retry. So we don't need to
	//    look at picky-requests every time.
	// 2) This also indicate starved requests so preemption can do surgical preemption based on this.
	waitTillNextTry map[string]uint64

	step uint64 // TODO document this, see ask_finder@findMayAllocationFromApplication
}

func NewScheduler(clusterInfo *cache.ClusterInfo) *Scheduler {
	m := &Scheduler{}
	m.clusterInfo = clusterInfo
	m.waitTillNextTry = make(map[string]uint64)
	m.clusterSchedulingContext = NewClusterSchedulingContext()
	m.pendingSchedulerEvents = make(chan interface{}, 1024*1024)
	return m
}

// Start service
func (m *Scheduler) StartService(handlers handler.EventHandlers, manualSchedule bool) {
	m.eventHandlers = handlers

	// Start event handlers
	go m.handleSchedulerEvent()

	// Start resource monitor if necessary (majorly for testing)
	monitor := newNodesResourceUsageMonitor(m)
	monitor.start()

	if !manualSchedule {
		go m.internalSchedule()
		go m.internalPreemption()
	}
}

// Create single allocation
func newSingleAllocationProposal(alloc *SchedulingAllocation) *cacheevent.AllocationProposalBundleEvent {
	return &cacheevent.AllocationProposalBundleEvent{
		AllocationProposals: []*commonevents.AllocationProposal{
			{
				NodeID:            alloc.NodeID,
				ApplicationID:     alloc.SchedulingAsk.ApplicationID,
				QueueName:         alloc.SchedulingAsk.QueueName,
				AllocatedResource: alloc.SchedulingAsk.AllocatedResource,
				AllocationKey:     alloc.SchedulingAsk.AskProto.AllocationKey,
				Priority:          alloc.SchedulingAsk.AskProto.Priority,
				PartitionName:     alloc.SchedulingAsk.PartitionName,
			},
		},
		ReleaseProposals: alloc.Releases,
		PartitionName:    alloc.PartitionName,
	}
}

// Internal start scheduling service
func (m *Scheduler) internalSchedule() {
	for {
		m.singleStepSchedule(16, &preemptionParameters{
			crossQueuePreemption: false,
			blacklistedRequest:   make(map[string]bool),
		})
	}
}

// Internal start preemption service
func (m *Scheduler) internalPreemption() {
	for {
		m.SingleStepPreemption()
		time.Sleep(1000 * time.Millisecond)
	}
}

func (m *Scheduler) updateSchedulingRequest(schedulingAsk *SchedulingAllocationAsk) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	// Get SchedulingApplication
	schedulingApp := m.clusterSchedulingContext.GetSchedulingApplication(schedulingAsk.ApplicationID, schedulingAsk.PartitionName)
	if schedulingApp == nil {
		return fmt.Errorf("cannot find scheduling application %s, for allocation %s", schedulingAsk.ApplicationID, schedulingAsk.AskProto.AllocationKey)
	}

	// found now update the pending requests for the queue that the app is running in
	schedulingAsk.QueueName = schedulingApp.queue.Name
	pendingDelta, err := schedulingApp.Requests.AddAllocationAsk(schedulingAsk)
	if err == nil && !resources.IsZero(pendingDelta) {
		schedulingApp.queue.IncPendingResource(pendingDelta)
	}
	return err
}

func (m *Scheduler) updateSchedulingRequestPendingAskByDelta(allocProposal *commonevents.AllocationProposal, deltaPendingAsk int32) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	// Get SchedulingApplication
	schedulingApp := m.clusterSchedulingContext.GetSchedulingApplication(allocProposal.ApplicationID, allocProposal.PartitionName)
	if schedulingApp == nil {
		return fmt.Errorf("cannot find scheduling application %s, for allocation ID %s", allocProposal.ApplicationID, allocProposal.AllocationKey)
	}

	// found, now update the pending requests for the queues
	pendingDelta, err := schedulingApp.Requests.UpdateAllocationAskRepeat(allocProposal.AllocationKey, deltaPendingAsk)
	if err == nil && !resources.IsZero(pendingDelta) {
		schedulingApp.queue.IncPendingResource(pendingDelta)
	}
	return err
}

// When a new app added, invoked by external
func (m *Scheduler) addNewApplication(info *cache.ApplicationInfo) error {
	schedulingApp := NewSchedulingApplication(info)

	return m.clusterSchedulingContext.AddSchedulingApplication(schedulingApp)
}

func (m *Scheduler) removeApplication(request *si.RemoveApplicationRequest) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, err := m.clusterSchedulingContext.RemoveSchedulingApplication(request.ApplicationID, request.PartitionName); err != nil {
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
func (m *Scheduler) HandleEvent(ev interface{}) {
	enqueueAndCheckFull(m.pendingSchedulerEvents, ev)
}

func (m *Scheduler) processAllocationReleaseByAllocationKey(
	allocationAsksToRelease []*si.AllocationAskReleaseRequest, allocationsToRelease []*si.AllocationReleaseRequest) {
	m.lock.Lock()
	defer m.lock.Unlock()

	// For all Requests
	if len(allocationAsksToRelease) > 0 {
		for _, toRelease := range allocationAsksToRelease {
			schedulingApp := m.clusterSchedulingContext.GetSchedulingApplication(toRelease.ApplicationID, toRelease.PartitionName)
			if schedulingApp != nil {
				delta, _ := schedulingApp.Requests.RemoveAllocationAsk(toRelease.Allocationkey)
				if !resources.IsZero(delta) {
					schedulingApp.queue.IncPendingResource(delta)
				}

				log.Logger().Info("release allocation",
					zap.String("allocation", toRelease.Allocationkey),
					zap.String("appID", toRelease.ApplicationID),
					zap.String("deductPendingResource", delta.String()),
					zap.String("message", toRelease.Message))
			}
		}
	}

	if len(allocationsToRelease) > 0 {
		toReleaseAllocations := make([]*si.ForgotAllocation, len(allocationAsksToRelease))
		for _, toRelease := range allocationsToRelease {
			schedulingApp := m.clusterSchedulingContext.GetSchedulingApplication(toRelease.ApplicationID, toRelease.PartitionName)
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

func (m *Scheduler) recoverExistingAllocations(existingAllocations []*si.Allocation, rmID string) {
	// the recovering of existing allocations looks like a replay of the scheduling process,
	// in general, a scheduling process takes following steps
	//  1) add scheduling info to schedulers cache, including app, pending requests etc
	//  2) Look order partition -> queue -> app
	//  3) For pending asks of a app, scheduler runs scheduling logic to propose a node
	//  4) Process the proposal generated in step 3, update data in cache
	// the recovery is repeating all steps except step 2
	for _, alloc := range existingAllocations {
		log.Logger().Info("recovering allocations for app",
			zap.String("applicationID", alloc.ApplicationID),
			zap.String("nodeID", alloc.NodeID),
			zap.String("queueName", alloc.QueueName),
			zap.String("partition", alloc.PartitionName),
			zap.String("allocationId", alloc.UUID))

		// add scheduling asks
		schedulingAsk := ConvertFromAllocation(alloc, rmID)
		if err := m.updateSchedulingRequest(schedulingAsk); err != nil {
			log.Logger().Warn("failed...", zap.Error(err))
		}

		// handle allocation proposals
		if err := m.updateSchedulingRequestPendingAskByDelta(&commonevents.AllocationProposal{
			NodeID:            alloc.NodeID,
			ApplicationID:     alloc.ApplicationID,
			QueueName:         alloc.QueueName,
			AllocatedResource: resources.NewResourceFromProto(alloc.ResourcePerAlloc),
			AllocationKey:     alloc.AllocationKey,
			Tags:              alloc.AllocationTags,
			Priority:          alloc.Priority,
			PartitionName:     common.GetNormalizedPartitionName(alloc.PartitionName, rmID),
		}, -1); err != nil {
			log.Logger().Error("failed to increase pending ask",
				zap.Error(err))
		}
	}
}

func (m *Scheduler) processAllocationUpdateEvent(ev *schedulerevent.SchedulerAllocationUpdatesEvent) {
	if len(ev.ExistingAllocations) > 0 {
		// in recovery mode, we only expect existing allocations being reported
		if len(ev.NewAsks) > 0 || len(ev.RejectedAllocations) > 0 || ev.ToReleases != nil {
			log.Logger().Warn("illegal SchedulerAllocationUpdatesEvent,"+
				" only existingAllocations can be set exclusively, other info will be skipped",
				zap.Int("num of existingAllocations", len(ev.ExistingAllocations)),
				zap.Int("num of rejectedAllocations", len(ev.RejectedAllocations)),
				zap.Int("num of newAsk", len(ev.NewAsks)))
		}
		m.recoverExistingAllocations(ev.ExistingAllocations, ev.RMId)
		return
	}

	// Allocations events cannot contain accepted and rejected allocations at the same time.
	// There should never be more than one accepted allocation in the list
	// See cluster_info.processAllocationProposalEvent()
	if len(ev.AcceptedAllocations) > 0 {
		alloc := ev.AcceptedAllocations[0]
		// decrease the outstanding allocation resources
		m.clusterSchedulingContext.updateSchedulingNodeAlloc(alloc)
	}

	// Rejects have not updated the node in the cache but have updated the scheduler node with
	// outstanding allocations that need to be removed. Can be multiple in one event.
	if len(ev.RejectedAllocations) > 0 {
		for _, alloc := range ev.RejectedAllocations {
			// Update pending resource back
			if err := m.updateSchedulingRequestPendingAskByDelta(alloc, 1); err != nil {
				log.Logger().Error("failed to increase pending ask",
					zap.Error(err))
			}
			// decrease the outstanding allocation resources
			m.clusterSchedulingContext.updateSchedulingNodeAlloc(alloc)
		}
	}

	// When RM asks to remove some allocations, the event will be send to scheduler first, to release pending asks, etc.
	// Then it will be relay to cache to release allocations.
	// The reason to send to scheduler before cache is, we need to clean up asks otherwise new allocations will be created.
	if ev.ToReleases != nil {
		m.processAllocationReleaseByAllocationKey(ev.ToReleases.AllocationAsksToRelease, ev.ToReleases.AllocationsToRelease)
		m.eventHandlers.CacheEventHandler.HandleEvent(cacheevent.NewReleaseAllocationEventFromProto(ev.ToReleases.AllocationsToRelease))
	}

	if len(ev.NewAsks) > 0 {
		rejectedAsks := make([]*si.RejectedAllocationAsk, 0)

		var rmID = ""
		for _, ask := range ev.NewAsks {
			rmID = common.GetRMIdFromPartitionName(ask.PartitionName)
			schedulingAsk := NewSchedulingAllocationAsk(ask)
			if err := m.updateSchedulingRequest(schedulingAsk); err != nil {
				rejectedAsks = append(rejectedAsks, &si.RejectedAllocationAsk{
					AllocationKey: schedulingAsk.AskProto.AllocationKey,
					ApplicationID: schedulingAsk.ApplicationID,
					Reason:        err.Error()})
			}
		}

		// Reject asks to RM Proxy
		if len(rejectedAsks) > 0 {
			m.eventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMRejectedAllocationAskEvent{
				RejectedAllocationAsks: rejectedAsks,
				RmID:                   rmID,
			})
		}
	}
}

// Process application adds and removes that have been processed by the cache.
// The cache processes the applications and has already filtered out some apps.
// All apps come from one si.UpdateRequest and thus from one RM.
func (m *Scheduler) processApplicationUpdateEvent(ev *schedulerevent.SchedulerApplicationsUpdateEvent) {
	if len(ev.AddedApplications) > 0 {
		rejectedApps := make([]*si.RejectedApplication, 0)
		acceptedApps := make([]*si.AcceptedApplication, 0)
		var rmID string
		for _, j := range ev.AddedApplications {
			app, ok := j.(*cache.ApplicationInfo)
			if !ok {
				log.Logger().Debug("cast failed unexpected object in event",
					zap.Any("ApplicationInfo", j))
			}
			rmID = common.GetRMIdFromPartitionName(app.Partition)
			if err := m.addNewApplication(app); err != nil {
				log.Logger().Debug("rejecting application in scheduler",
					zap.String("appID", app.ApplicationID),
					zap.String("partitionName", app.Partition),
					zap.Error(err))
				// update cache
				m.eventHandlers.CacheEventHandler.HandleEvent(
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
				// app is accepted by scheduler
				err = app.HandleApplicationEvent(cache.AcceptApplication)
				if err != nil {
					log.Logger().Debug("cache event handling error returned",
						zap.Error(err))
				}
			}
		}
		// notify RM proxy about apps added and rejected
		m.eventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMApplicationUpdateEvent{
			RmID:                 rmID,
			AcceptedApplications: acceptedApps,
			RejectedApplications: rejectedApps,
		})
	}

	if len(ev.RemovedApplications) > 0 {
		for _, app := range ev.RemovedApplications {
			err := m.removeApplication(app)

			if err != nil {
				log.Logger().Error("failed to remove app from partition",
					zap.String("appID", app.ApplicationID),
					zap.String("partitionName", app.PartitionName),
					zap.Error(err))
				continue
			}
			m.eventHandlers.CacheEventHandler.HandleEvent(&cacheevent.RemovedApplicationEvent{ApplicationID: app.ApplicationID, PartitionName: app.PartitionName})
		}
	}
}

func (m *Scheduler) removePartitionsBelongToRM(event *commonevents.RemoveRMPartitionsEvent) {
	m.clusterSchedulingContext.RemoveSchedulingPartitionsByRMId(event.RmID)

	// Send this event to cache
	m.eventHandlers.CacheEventHandler.HandleEvent(event)
}

func (m *Scheduler) processUpdatePartitionConfigsEvent(event *schedulerevent.SchedulerUpdatePartitionsConfigEvent) {
	partitions := make([]*cache.PartitionInfo, 0)
	for _, p := range event.UpdatedPartitions {
		partition, ok := p.(*cache.PartitionInfo)
		if !ok {
			log.Logger().Debug("cast failed unexpected object in event",
				zap.Any("PartitionInfo", p))
		}
		partitions = append(partitions, partition)
	}

	if err := m.clusterSchedulingContext.updateSchedulingPartitions(partitions); err != nil {
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

func (m *Scheduler) processDeletePartitionConfigsEvent(event *schedulerevent.SchedulerDeletePartitionsConfigEvent) {
	partitions := make([]*cache.PartitionInfo, 0)
	for _, p := range event.DeletePartitions {
		partition, ok := p.(*cache.PartitionInfo)
		if !ok {
			log.Logger().Debug("cast failed unexpected object in event",
				zap.Any("PartitionInfo", p))
		}
		partitions = append(partitions, partition)
	}

	if err := m.clusterSchedulingContext.deleteSchedulingPartitions(partitions); err != nil {
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
func (m *Scheduler) processNodeEvent(event *schedulerevent.SchedulerNodeEvent) {
	// process the node addition (one per event)
	if event.AddedNode != nil {
		nodeInfo, ok := event.AddedNode.(*cache.NodeInfo)
		if !ok {
			log.Logger().Debug("cast failed unexpected object in event",
				zap.Any("NodeInfo", event.AddedNode))
		}
		m.clusterSchedulingContext.addSchedulingNode(nodeInfo)
	}
	// process the node deletion (one per event)
	if event.RemovedNode != nil {
		nodeInfo, ok := event.RemovedNode.(*cache.NodeInfo)
		if !ok {
			log.Logger().Debug("cast failed unexpected object in event",
				zap.Any("NodeInfo", event.RemovedNode))
		}
		m.clusterSchedulingContext.removeSchedulingNode(nodeInfo)
	}
	// preempted resources have now been released update the node
	if event.PreemptedNodeResources != nil {
		m.clusterSchedulingContext.releasePreemptedResources(event.PreemptedNodeResources)
	}
}

func (m *Scheduler) handleSchedulerEvent() {
	for {
		ev := <-m.pendingSchedulerEvents
		switch v := ev.(type) {
		case *schedulerevent.SchedulerNodeEvent:
			m.processNodeEvent(v)
		case *schedulerevent.SchedulerAllocationUpdatesEvent:
			m.processAllocationUpdateEvent(v)
		case *schedulerevent.SchedulerApplicationsUpdateEvent:
			m.processApplicationUpdateEvent(v)
		case *commonevents.RemoveRMPartitionsEvent:
			m.removePartitionsBelongToRM(v)
		case *schedulerevent.SchedulerUpdatePartitionsConfigEvent:
			m.processUpdatePartitionConfigsEvent(v)
		case *schedulerevent.SchedulerDeletePartitionsConfigEvent:
			m.processDeletePartitionConfigsEvent(v)
		default:
			panic(fmt.Sprintf("%s is not an acceptable type for Scheduler event.", reflect.TypeOf(v).String()))
		}
	}
}

// Visible by tests
func (m *Scheduler) GetClusterSchedulingContext() *ClusterSchedulingContext {
	return m.clusterSchedulingContext
}
