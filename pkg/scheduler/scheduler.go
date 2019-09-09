/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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
    "github.com/cloudera/yunikorn-core/pkg/cache"
    "github.com/cloudera/yunikorn-core/pkg/cache/cacheevent"
    "github.com/cloudera/yunikorn-core/pkg/common"
    "github.com/cloudera/yunikorn-core/pkg/common/commonevents"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/handler"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "github.com/cloudera/yunikorn-core/pkg/metrics"
    "github.com/cloudera/yunikorn-core/pkg/plugins"
    "github.com/cloudera/yunikorn-core/pkg/rmproxy/rmevent"
    "github.com/cloudera/yunikorn-core/pkg/scheduler/schedulerevent"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "go.uber.org/zap"
    "reflect"
    "sync"
    "time"
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

    metrics metrics.CoreSchedulerMetrics // Reference to scheduler metrics

    // Wait till next try
    // (It is designed to be accessed under a single goroutine, no lock needed).
    // This field has dual purposes:
    // 1) When a request is picky, scheduler will increase this value for every failed retry. So we don't need to
    //    look at picky-requests every time.
    // 2) This also indicate starved requests so preemption can do surgical preemption based on this.
    waitTillNextTry map[string]uint64

    step uint64 // TODO document this, see ask_finder@findMayAllocationFromApplication
}

func NewScheduler(clusterInfo *cache.ClusterInfo, metrics metrics.CoreSchedulerMetrics) *Scheduler {
    m := &Scheduler{}
    m.clusterInfo = clusterInfo
    m.waitTillNextTry = make(map[string]uint64)
    m.clusterSchedulingContext = NewClusterSchedulingContext()
    m.pendingSchedulerEvents = make(chan interface{}, 1024*1024)
    m.metrics = metrics

    return m
}

// Start service
func (m *Scheduler) StartService(handlers handler.EventHandlers, manualSchedule bool) {
    m.eventHandlers = handlers

    // Start event handlers
    go m.handleSchedulerEvent()

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
                NodeId:            alloc.NodeId,
                ApplicationId:     alloc.SchedulingAsk.ApplicationId,
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
            blacklistedRequest: make(map[string]bool),
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
    schedulingApp := m.clusterSchedulingContext.GetSchedulingApplication(schedulingAsk.ApplicationId, schedulingAsk.PartitionName)
    if schedulingApp == nil {
        return fmt.Errorf("cannot find scheduling application %s, for allocation %s", schedulingAsk.ApplicationId, schedulingAsk.AskProto.AllocationKey)
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
    schedulingApp := m.clusterSchedulingContext.GetSchedulingApplication(allocProposal.ApplicationId, allocProposal.PartitionName)
    if schedulingApp == nil {
        return fmt.Errorf("cannot find scheduling application %s, for allocation ID %s", allocProposal.ApplicationId, allocProposal.AllocationKey)
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

    if _, err := m.clusterSchedulingContext.RemoveSchedulingApplication(request.ApplicationId, request.PartitionName); err != nil {
        log.Logger().Error("failed to remove apps",
            zap.String("appId", request.ApplicationId),
            zap.String("partitionName", request.PartitionName),
            zap.Error(err))
        return err
    }

    log.Logger().Info("app removed",
        zap.String("appId", request.ApplicationId),
        zap.String("partitionName", request.PartitionName))
    return nil
}

func enqueueAndCheckFull(queue chan interface{}, ev interface{}) {
    select {
    case queue <- ev:
        log.Logger().Debug("enqueued event",
            zap.String("eventType",  reflect.TypeOf(ev).String()),
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

    if allocationAsksToRelease != nil && len(allocationAsksToRelease) > 0 {
        for _, toRelease := range allocationAsksToRelease {
            schedulingApp := m.clusterSchedulingContext.GetSchedulingApplication(toRelease.ApplicationId, toRelease.PartitionName)
            if schedulingApp != nil {
                delta, _ := schedulingApp.Requests.RemoveAllocationAsk(toRelease.Allocationkey)
                if !resources.IsZero(delta) {
                    schedulingApp.queue.IncPendingResource(delta)
                }

                log.Logger().Info("release allocation",
                    zap.String("allocation", toRelease.Allocationkey),
                    zap.String("appId", toRelease.ApplicationId),
                    zap.String("deductPendingResource", delta.String()),
                    zap.String("message", toRelease.Message))
            }
        }
    }

    if allocationsToRelease != nil && len(allocationsToRelease) > 0 {
        toReleaseAllocations :=  make([]*si.ForgotAllocation, len(allocationAsksToRelease))
        for _, toRelease := range allocationsToRelease {
            schedulingApp := m.clusterSchedulingContext.GetSchedulingApplication(toRelease.ApplicationId, toRelease.PartitionName)
            if schedulingApp != nil {
                for _, alloc := range schedulingApp.ApplicationInfo.GetAllAllocations() {
                    if alloc.AllocationProto.Uuid == toRelease.Uuid {
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
        log.Logger().Info(">>>>> processing release allocations")
        if len(toReleaseAllocations) > 0 {
            log.Logger().Info(">>>>> forget allocations", zap.Int("size", len(toReleaseAllocations)))
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

func (m *Scheduler) recoverExistingAllocations(existingAllocations []*si.Allocation, rmId string) {
    // the recovering of existing allocations looks like a replay of the scheduling process,
    // in general, a scheduling process takes following steps
    //  1) add scheduling info to schedulers cache, including app, pending requests etc
    //  2) Look order partition -> queue -> app
    //  3) For pending asks of a app, scheduler runs scheduling logic to propose a node
    //  4) Process the proposal generated in step 3, update data in cache
    // the recovery is repeating all steps except step 2
    for _, alloc := range existingAllocations {
        log.Logger().Info("recovering allocations for app",
            zap.String("applicationId", alloc.ApplicationId),
            zap.String("nodeId", alloc.NodeId),
            zap.String("queueName", alloc.QueueName),
            zap.String("partition", alloc.PartitionName),
            zap.String("allocationId", alloc.Uuid))

        // add scheduling asks
        schedulingAsk := ConvertFromAllocation(alloc, rmId)
        if err := m.updateSchedulingRequest(schedulingAsk); err != nil {
            log.Logger().Warn("failed...", zap.Error(err))
        }

        // handle allocation proposals
        if err := m.updateSchedulingRequestPendingAskByDelta(&commonevents.AllocationProposal{
            NodeId:            alloc.NodeId,
            ApplicationId:     alloc.ApplicationId,
            QueueName:         alloc.QueueName,
            AllocatedResource: resources.NewResourceFromProto(alloc.ResourcePerAlloc),
            AllocationKey:     alloc.AllocationKey,
            Tags:              alloc.AllocationTags,
            Priority:          alloc.Priority,
            PartitionName:     common.GetNormalizedPartitionName(alloc.PartitionName, rmId),
        }, -1); err != nil {
            log.Logger().Error("failed to increase pending ask",
                zap.Error(err))
        }
    }
}

func (m *Scheduler) processAllocationUpdateEvent(ev *schedulerevent.SchedulerAllocationUpdatesEvent) {
    if len(ev.ExistingAllocations) > 0 {
        // in recovery mode, we only expect existing allocations being reported
        if len(ev.NewAsks) > 0 || len(ev.RejectedAllocations) > 0 || ev.ToReleases != nil{
            log.Logger().Warn("illegal SchedulerAllocationUpdatesEvent," +
                " only existingAllocations can be set exclusively, other info will be skipped",
                zap.Int("num of existingAllocations", len(ev.ExistingAllocations)),
                zap.Int("num of rejectedAllocations", len(ev.RejectedAllocations)),
                zap.Int("num of newAsk", len(ev.NewAsks)))
        }
        m.recoverExistingAllocations(ev.ExistingAllocations, ev.RMId)
        return
    }

    if len(ev.RejectedAllocations) > 0 {
        for _, alloc := range ev.RejectedAllocations {
            // Update pending resource back
            if err := m.updateSchedulingRequestPendingAskByDelta(alloc, 1); err != nil {
                log.Logger().Error("failed to increase pending ask",
                    zap.Error(err))
            }
        }
    }

    // When RM asks to remove some allocations, the event will be send to scheduler first, to release pending asks, etc.
    // Then it will be relay to cache to release allocations.
    // The reason to send to scheduler before cache is, we need to clean up asks otherwise new allocations will be created.
    log.Logger().Info(">>>>> processing to release")
    if ev.ToReleases != nil {
        log.Logger().Info(">>>>> to release is not nil", zap.Any("xxx", ev.ToReleases.AllocationAsksToRelease))
        m.processAllocationReleaseByAllocationKey(ev.ToReleases.AllocationAsksToRelease, ev.ToReleases.AllocationsToRelease)
        m.eventHandlers.CacheEventHandler.HandleEvent(cacheevent.NewReleaseAllocationEventFromProto(ev.ToReleases.AllocationsToRelease))
    }

    if len(ev.NewAsks) > 0 {
        rejectedAsks := make([]*si.RejectedAllocationAsk, 0)

        var rmId = ""
        for _, ask := range ev.NewAsks {
            rmId = common.GetRMIdFromPartitionName(ask.PartitionName)
            schedulingAsk := NewSchedulingAllocationAsk(ask)
            if err := m.updateSchedulingRequest(schedulingAsk); err != nil {
                rejectedAsks = append(rejectedAsks, &si.RejectedAllocationAsk{
                    AllocationKey: schedulingAsk.AskProto.AllocationKey,
                    ApplicationId: schedulingAsk.ApplicationId,
                    Reason: err.Error()})
            }
        }

        // Reject asks to RM Proxy
        if len(rejectedAsks) > 0 {
            m.eventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMRejectedAllocationAskEvent{
                RejectedAllocationAsks: rejectedAsks,
                RMId:                   rmId,
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
            app := j.(*cache.ApplicationInfo)
            rmID = common.GetRMIdFromPartitionName(app.Partition)
            if err := m.addNewApplication(app); err != nil {
                log.Logger().Debug("rejecting application in scheduler",
                    zap.String("appId", app.ApplicationId),
                    zap.String("partitionName", app.Partition),
                    zap.Error(err))
                // update cache
                m.eventHandlers.CacheEventHandler.HandleEvent(
                    &cacheevent.RejectedNewApplicationEvent{
                        ApplicationId: app.ApplicationId,
                        PartitionName: app.Partition,
                        Reason: err.Error(),
                    })
                rejectedApps = append(rejectedApps, &si.RejectedApplication{
                    ApplicationId: app.ApplicationId,
                    Reason:        err.Error(),
                })
                // app is rejected by the scheduler
                _ = app.HandleApplicationEvent(cache.RejectApplication)
            } else {
                acceptedApps = append(acceptedApps, &si.AcceptedApplication{
                    ApplicationId: app.ApplicationId,
                })
                // app is accepted by scheduler
                _ = app.HandleApplicationEvent(cache.AcceptApplication)
            }
        }
        // notify RM proxy about apps added and rejected
        m.eventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMApplicationUpdateEvent{
            RMId:                 rmID,
            AcceptedApplications: acceptedApps,
            RejectedApplications: rejectedApps,
        })
    }

    if len(ev.RemovedApplications) > 0 {
        for _, app := range ev.RemovedApplications {
            err := m.removeApplication(app)

            if err != nil {
                log.Logger().Error("failed to remove app from partition",
                    zap.String("appId", app.ApplicationId),
                    zap.String("partitionName", app.PartitionName),
                    zap.Error(err))
                continue
            }
            m.eventHandlers.CacheEventHandler.HandleEvent(&cacheevent.RemovedApplicationEvent{ApplicationId: app.ApplicationId, PartitionName: app.PartitionName})
        }
    }
}

func (m *Scheduler) removePartitionsBelongToRM(event *commonevents.RemoveRMPartitionsEvent) {
    m.clusterSchedulingContext.RemoveSchedulingPartitionsByRMId(event.RmId)

    // Send this event to cache
    m.eventHandlers.CacheEventHandler.HandleEvent(event)
}

func (m *Scheduler) processUpdatePartitionConfigsEvent(event *schedulerevent.SchedulerUpdatePartitionsConfigEvent) {
    partitions := make([]*cache.PartitionInfo, 0)
    for _, p := range event.UpdatedPartitions {
        partitions = append(partitions, p.(*cache.PartitionInfo))
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
        partitions = append(partitions, p.(*cache.PartitionInfo))
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

func (m *Scheduler) handleSchedulerEvent() {
    for {
        ev := <-m.pendingSchedulerEvents
        switch v := ev.(type) {
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
