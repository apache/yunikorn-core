/*
Copyright 2019 The Unity Scheduler Authors

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
    "errors"
    "fmt"
    "github.com/golang/glog"
    "github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/cache"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/cache/cacheevent"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/commonevents"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/handler"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/rmproxy/rmevent"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/scheduler/schedulerevent"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/schedulermetrics"
    "reflect"
    "sync"
    "time"
)

// Responsibility of this class is, get status from SchedulerCache, and
// send allocation / release proposal back to cache.
//
// Scheduler may maintain its local status which is different from SchedulerCache
type Scheduler struct {
    clusterInfo *cache.ClusterInfo

    clusterSchedulingContext *ClusterSchedulingContext

    // Wait till next try
    // (It is designed to be accessed under a single goroutine, no lock needed).
    // This field has dual purposes:
    // 1) When a request is picky, scheduler will increase this value for every failed retry. So we don't need to
    //    look at picky-requests every time.
    // 2) This also indicate starved requests so preemption can do surgical preemption based on this.
    waitTillNextTry map[string]uint64

    EventHandlers handler.EventHandlers

    lock sync.RWMutex

    // Any changes to partition, like add/remove partition
    partitionChangeLock sync.RWMutex

    pendingSchedulerEvents chan interface{}

    // Reference to scheduler metrics
    metrics schedulermetrics.CoreSchedulerMetrics

    // Preemption context
    preemptionContext *preemptionContext

    step uint64
}

func NewScheduler(clusterInfo *cache.ClusterInfo, metrics schedulermetrics.CoreSchedulerMetrics) *Scheduler {
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
    m.EventHandlers = handlers

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
                QueueName:         alloc.SchedulingAsk.AskProto.QueueName,
                AllocatedResource: alloc.SchedulingAsk.AllocatedResource,
                AllocationKey:     alloc.SchedulingAsk.AskProto.AllocationKey,
                Tags:              alloc.SchedulingAsk.AskProto.Tags,
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
        m.singleStepSchedule(16, &preemptionParameters{})
    }
}

// Internal start preemption service
func (m *Scheduler) internalPreemption() {
    for {
        m.SingleStepPreemption()
        time.Sleep(1000 * time.Millisecond)
    }
}

func (m *Scheduler) updateQueuePendingResources(queue *SchedulingQueue, pendingResourceDelta *resources.Resource) {
    for queue != nil {
        queue.IncPendingResource(pendingResourceDelta)
        queue = queue.Parent
    }
}

func (m *Scheduler) updateSchedulingRequest(schedulingAsk *SchedulingAllocationAsk) error {
    m.lock.Lock()
    defer m.lock.Unlock()

    // Get SchedulingApplication
    schedulingApp := m.clusterSchedulingContext.GetSchedulingApplication(schedulingAsk.ApplicationId, schedulingAsk.PartitionName)
    if schedulingApp == nil {
        return errors.New(fmt.Sprintf("Cannot find scheduling-app=%s, for allocation=%s", schedulingAsk.ApplicationId, schedulingAsk.AskProto.AllocationKey))
    }

    // Succeeded
    if pendingDelta, err := schedulingApp.Requests.AddAllocationAsk(schedulingAsk); err == nil {
        m.updateQueuePendingResources(schedulingApp.ParentQueue, pendingDelta)
        return nil
    } else {
        return err
    }
}

func (m *Scheduler) updateSchedulingRequestPendingAskByDelta(allocProposal *commonevents.AllocationProposal, deltaPendingAsk int32) error {
    m.lock.Lock()
    defer m.lock.Unlock()

    // Get SchedulingApplication
    schedulingApp := m.clusterSchedulingContext.GetSchedulingApplication(allocProposal.ApplicationId, allocProposal.PartitionName)
    if schedulingApp == nil {
        return errors.New(fmt.Sprintf("Cannot find scheduling-app=%s, for allocation=%s", allocProposal.ApplicationId, allocProposal.AllocationKey))
    }

    // Succeeded
    if pendingDelta, err := schedulingApp.Requests.UpdateAllocationAskRepeat(allocProposal.AllocationKey, deltaPendingAsk); err == nil {
        m.updateQueuePendingResources(schedulingApp.ParentQueue, pendingDelta)
        return nil
    } else {
        return err
    }
}

// When a new app added, invoked by external
func (m *Scheduler) addNewApplication(info *cache.ApplicationInfo) error {
    schedulingApp := NewSchedulingApplication(info)

    if err := m.clusterSchedulingContext.AddSchedulingApplication(schedulingApp); err != nil {
        return err
    }

    return nil
}

func (m *Scheduler) removeApplication(request *si.RemoveApplicationRequest) error {
    m.lock.Lock()
    defer m.lock.Unlock()

    _, err := m.clusterSchedulingContext.RemoveSchedulingApplication(request.ApplicationId, request.PartitionName)

    if err != nil {
        return err
    }

    glog.V(2).Infof("Removed app=%s from partition=%s", request.ApplicationId, request.PartitionName)
    return nil
}

func enqueueAndCheckFull(queue chan interface{}, ev interface{}) {
    select {
    case queue <- ev:
        glog.V(2).Infof("Enqueued event=%s, current queue size=%d", ev, len(queue))
    default:
        panic(fmt.Sprintf("Failed to enqueue event=%s", reflect.TypeOf(ev).String()))
    }
}

// Implement methods for Scheduler events
func (m *Scheduler) HandleEvent(ev interface{}) {
    enqueueAndCheckFull(m.pendingSchedulerEvents, ev)
}

func (m *Scheduler) processAllocationReleaseByAllocationKey(allocationAsksToRelease []*si.AllocationAskReleaseRequest) {
    if len(allocationAsksToRelease) == 0 {
        return
    }

    m.lock.Lock()
    defer m.lock.Unlock()

    // For all Requests
    for _, toRelease := range allocationAsksToRelease {
        schedulingApp := m.clusterSchedulingContext.GetSchedulingApplication(toRelease.ApplicationId, toRelease.PartitionName)
        if nil != schedulingApp {
            delta, _ := schedulingApp.Requests.RemoveAllocationAsk(toRelease.Allocationkey)
            if delta != nil {
                schedulingQueue := schedulingApp.ParentQueue
                for schedulingQueue != nil {
                    schedulingQueue.IncPendingResource(delta)
                    schedulingQueue = schedulingQueue.Parent
                }
            }

            glog.V(2).Infof("Removed allocation=%s from app=%s, reduced pending resource=%v, message=%s",
                toRelease.Allocationkey, toRelease.ApplicationId, delta, toRelease.Message)
        }
    }
}

func (m *Scheduler) processAllocationUpdateEvent(ev *schedulerevent.SchedulerAllocationUpdatesEvent) {
    if len(ev.RejectedAllocations) > 0 {
        for _, alloc := range ev.RejectedAllocations {
            // Update pending resource back
            if err := m.updateSchedulingRequestPendingAskByDelta(alloc, 1); err != nil {
                glog.V(2).Infof("Issues when increase pending ask for rejected proposal, error=%s", err)
            }
        }
    }

    // When RM asks to remove some allocations, the event will be send to scheduler first, to release pending asks, etc.
    // Then it will be relay to cache to release allocations.
    // The reason to send to scheduler before cache is, we need to clean up asks otherwise new allocations will be created.
    if ev.ToReleases != nil {
        m.processAllocationReleaseByAllocationKey(ev.ToReleases.AllocationAsksToRelease)
        m.EventHandlers.CacheEventHandler.HandleEvent(cacheevent.NewReleaseAllocationEventFromProto(ev.ToReleases.AllocationsToRelease))
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
            m.EventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMRejectedAllocationAskEvent{
                RejectedAllocationAsks: rejectedAsks,
                RMId:                   rmId,
            })
        }
    }
}

func (m *Scheduler) processApplicationUpdateEvent(ev *schedulerevent.SchedulerApplicationsUpdateEvent) {
    if len(ev.AddedApplications) > 0 {
        for _, j := range ev.AddedApplications {
            app := j.(*cache.ApplicationInfo)
            if err := m.addNewApplication(app); err != nil {
                // update cache
                m.EventHandlers.CacheEventHandler.HandleEvent(&cacheevent.RejectedNewApplicationEvent{ApplicationId: app.ApplicationId, Reason: err.Error()})
                // notify RM proxy about rejected apps
                rejectedApps := make([]*si.RejectedApplication, 0)
                m.EventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMApplicationUpdateEvent{
                    RMId:                 common.GetRMIdFromPartitionName(app.Partition),
                    AcceptedApplications: nil,
                    RejectedApplications: append(rejectedApps, &si.RejectedApplication{
                        ApplicationId: app.ApplicationId,
                        Reason:        err.Error(),
                    }),
                })
                // app rejects apps
                app.HandleApplicationEvent(cache.RejectApplication)
            }
            // app is accepted by scheduler
            app.HandleApplicationEvent(cache.AcceptApplication)
        }
    }

    if len(ev.RemovedApplications) > 0 {
        for _, app := range ev.RemovedApplications {
            err := m.removeApplication(app)

            if err != nil {
                glog.V(0).Infof("Failed when remove app=%s, partition=%s, error=%s", app.ApplicationId, app.PartitionName, err)
                continue
            }
            m.EventHandlers.CacheEventHandler.HandleEvent(&cacheevent.RemovedApplicationEvent{ApplicationId: app.ApplicationId, PartitionName: app.PartitionName})
        }
    }
}

func (m *Scheduler) removePartitionsBelongToRM(event *commonevents.RemoveRMPartitionsEvent) {
    m.clusterSchedulingContext.RemoveSchedulingPartitionsByRMId(event.RmId)

    // Send this event to cache
    m.EventHandlers.CacheEventHandler.HandleEvent(event)
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
