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
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/cache"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/cache/cacheevent"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/common"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/common/commonevents"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/common/resources"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/handler"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/rmproxy/rmevent"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/scheduler/schedulerevent"
    "reflect"
    "sync"
)

// Responsibility of this class is, get status from SchedulerCache, and
// send allocation / release proposal back to cache.
//
// Scheduler may maintain its local status which is different from SchedulerCache
type Scheduler struct {
    clusterInfo *cache.ClusterInfo

    clusterSchedulingContext *ClusterSchedulingContext

    // Missed opportunity map
    // It is designed to be accessed under single goroutine, no lock needed.
    missedOpportunities map[string]uint64

    EventHandlers handler.EventHandlers

    lock sync.RWMutex

    // Any changes to partition, like add/remove partition
    partitionChangeLock sync.RWMutex

    pendingSchedulerEvents chan interface{}

    step uint64
}

func NewScheduler(clusterInfo *cache.ClusterInfo) *Scheduler {
    m := &Scheduler{}
    m.clusterInfo = clusterInfo
    m.missedOpportunities = make(map[string]uint64)
    m.clusterSchedulingContext = NewClusterSchedulingContext()
    m.pendingSchedulerEvents = make(chan interface{}, 1024*1024)

    return m
}

// Start service
func (m *Scheduler) StartService(handlers handler.EventHandlers, manualSchedule bool) {
    m.EventHandlers = handlers

    // Start event handlers
    go m.handleSchedulerEvent()

    if !manualSchedule {
        go m.internalSchedule()
    }
}

// Create single allocation
func newSingleAllocationProposal(alloc *SchedulingAllocation) *cacheevent.AllocationProposalBundleEvent {
    return &cacheevent.AllocationProposalBundleEvent{
        AllocationProposals: []*commonevents.AllocationProposal{
            {
                NodeId:            alloc.NodeId,
                JobId:             alloc.SchedulingAsk.JobId,
                QueueName:         alloc.SchedulingAsk.AskProto.QueueName,
                AllocatedResource: alloc.SchedulingAsk.AllocatedResource,
                AllocationKey:     alloc.SchedulingAsk.AskProto.AllocationKey,
                Tags:              alloc.SchedulingAsk.AskProto.Tags,
                Priority:          alloc.SchedulingAsk.AskProto.Priority,
                PartitionName:     alloc.SchedulingAsk.PartitionName,
            },
        },
    }
}

// Visible by tests
func (m *Scheduler) SingleStepSchedule(nAlloc int) {
    m.step++

    // Make sure no change of partitions happen
    m.partitionChangeLock.RLock()
    m.partitionChangeLock.RUnlock()

    for partition, partitionContext := range m.clusterSchedulingContext.partitions {
        // Following steps:
        // - According to resource usage, find next N allocation Requests, N could be
        //   mini-batch because we don't want the process takes too long. And this
        //   runs as single thread.
        // - According to mini-batched allocation request. Try to allocate. This step
        //   can be done as multiple thread.
        // - For asks cannot be assigned, we will do preemption. Again it is done using
        //   single-thread.
        candidates := m.findAllocationAsks(partitionContext, nAlloc, m.step)

        // Try to allocate from candidates, returns allocation proposal as well as failed allocation
        // ask candidates. (For preemption).
        allocations, failedCandidate := m.tryBatchAllocation(partition, candidates)

        // Send allocations to cache, and pending ask.
        confirmedAllocations := make([]*SchedulingAllocation, 0)
        if len(allocations) > 0 {
            for _, alloc := range allocations {
                if alloc == nil {
                    continue
                }

                proposal := newSingleAllocationProposal(alloc)
                err := m.updateSchedulingRequestPendingAskByDelta(proposal.AllocationProposals[0], -1)
                if err == nil {
                    m.EventHandlers.CacheEventHandler.HandleEvent(newSingleAllocationProposal(alloc))
                    confirmedAllocations = append(confirmedAllocations, alloc)
                } else {
                    glog.V(2).Infof("Issues when trying to send proposal, err=%s", err.Error())
                }
            }
        }

        // Update missed opportunities
        m.updateMissedOpportunity(confirmedAllocations, candidates)

        // Do preemption according to failedCandidate
        m.preemptForAllocationAskCandidates(failedCandidate)
    }
}

// Internal start scheduling service
func (m *Scheduler) internalSchedule() {
    for {
        m.SingleStepSchedule(16)
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

    // Get SchedulingJob
    schedulingJob := m.clusterSchedulingContext.GetSchedulingJob(schedulingAsk.JobId, schedulingAsk.PartitionName)
    if schedulingJob == nil {
        return errors.New(fmt.Sprintf("Cannot find scheduling-job=%s, for allocation=%s", schedulingAsk.JobId, schedulingAsk.AskProto.AllocationKey))
    }

    // Succeeded
    if pendingDelta, err := schedulingJob.Requests.AddAllocationAsk(schedulingAsk); err == nil {
        m.updateQueuePendingResources(schedulingJob.ParentQueue, pendingDelta)
        return nil
    } else {
        return err
    }
}

func (m *Scheduler) updateSchedulingRequestPendingAskByDelta(allocProposal *commonevents.AllocationProposal, deltaPendingAsk int32) error {
    m.lock.Lock()
    defer m.lock.Unlock()

    // Get SchedulingJob
    schedulingJob := m.clusterSchedulingContext.GetSchedulingJob(allocProposal.JobId, allocProposal.PartitionName)
    if schedulingJob == nil {
        return errors.New(fmt.Sprintf("Cannot find scheduling-job=%s, for allocation=%s", allocProposal.JobId, allocProposal.AllocationKey))
    }

    // Succeeded
    if pendingDelta, err := schedulingJob.Requests.UpdateAllocationAskRepeat(allocProposal.AllocationKey, deltaPendingAsk); err == nil {
        m.updateQueuePendingResources(schedulingJob.ParentQueue, pendingDelta)
        return nil
    } else {
        return err
    }
}

// When a new job added, invoked by external
func (m *Scheduler) addNewJob(info *cache.JobInfo) error {
    schedulingJob := NewSchedulingJob(info)

    if err := m.clusterSchedulingContext.AddSchedulingJob(schedulingJob); err != nil {
        return err
    }

    return nil
}

func (m *Scheduler) removeJob(request *si.RemoveJobRequest) error {
    m.lock.Lock()
    defer m.lock.Unlock()

    _, err := m.clusterSchedulingContext.RemoveSchedulingJob(request.JobId, request.PartitionName)

    if err != nil {
        return err
    }

    glog.V(2).Infof("Removed job=%s from partition=%s", request.JobId, request.PartitionName)
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
        schedulingJob := m.clusterSchedulingContext.GetSchedulingJob(toRelease.JobId, toRelease.PartitionName)
        if nil != schedulingJob {
            delta, _ := schedulingJob.Requests.RemoveAllocationAsk(toRelease.Allocationkey)
            if delta != nil {
                schedulingQueue := schedulingJob.ParentQueue
                for schedulingQueue != nil {
                    schedulingQueue.IncPendingResource(delta)
                    schedulingQueue = schedulingQueue.Parent
                }
            }

            glog.V(2).Infof("Removed allocation=%s from job=%s, reduced pending resource=%v, message=%s",
                toRelease.Allocationkey, toRelease.JobId, delta, toRelease.Message)
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
                rejectedAsks = append(rejectedAsks, &si.RejectedAllocationAsk{AllocationKey: schedulingAsk.AskProto.AllocationKey, Reason: err.Error()})
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

func (m *Scheduler) processJobUpdateEvent(ev *schedulerevent.SchedulerJobsUpdateEvent) {
    if len(ev.AddedJobs) > 0 {
        for _, j := range ev.AddedJobs {
            job := j.(*cache.JobInfo)
            if err := m.addNewJob(job); err != nil {
                m.EventHandlers.CacheEventHandler.HandleEvent(&cacheevent.RejectedNewJobEvent{JobId: job.JobId, Reason: err.Error()})
            }
        }
    }

    if len(ev.RemovedJobs) > 0 {
        for _, job := range ev.RemovedJobs {
            err := m.removeJob(job)

            if err != nil {
                glog.V(0).Infof("Failed when remove job=%s, partition=%s, error=%s", job.JobId, job.PartitionName, err)
                continue
            }
            m.EventHandlers.CacheEventHandler.HandleEvent(&cacheevent.RemovedJobEvent{JobId: job.JobId, PartitionName: job.PartitionName})
        }
    }
}

func (m *Scheduler) removePartitionsBelongToRM(event *commonevents.RemoveRMPartitionsEvent) {
    m.partitionChangeLock.Lock()
    m.partitionChangeLock.Unlock()

    partitionToRemove := make(map[string]bool)

    // Just remove corresponding partitions
    for k, partition := range m.clusterSchedulingContext.partitions {
        if partition.RmId == event.RmId {
            partitionToRemove[k] = true
        }
    }

    for partitionName := range partitionToRemove {
        m.clusterSchedulingContext.RemoveSchedulingPartition(partitionName)
    }

    // Send this event to cache
    m.EventHandlers.CacheEventHandler.HandleEvent(event)
}

func (m *Scheduler) processUpdatePartitionConfigsEvent(event *schedulerevent.SchedulerUpdatePartitionsConfigEvent) {
    m.partitionChangeLock.Lock()
    m.partitionChangeLock.Unlock()

    m.lock.Lock()
    m.lock.Unlock()

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

func (m *Scheduler) handleSchedulerEvent() {
    for {
        ev := <-m.pendingSchedulerEvents
        switch v := ev.(type) {
        case *schedulerevent.SchedulerAllocationUpdatesEvent:
            m.processAllocationUpdateEvent(v)
        case *schedulerevent.SchedulerJobsUpdateEvent:
            m.processJobUpdateEvent(v)
        case *commonevents.RemoveRMPartitionsEvent:
            m.removePartitionsBelongToRM(v)
        case *schedulerevent.SchedulerUpdatePartitionsConfigEvent:
            m.processUpdatePartitionConfigsEvent(v)
        default:
            panic(fmt.Sprintf("%s is not an acceptable type for Scheduler event.", reflect.TypeOf(v).String()))
        }
    }
}

// Visible by tests
func (m *Scheduler) GetClusterSchedulingContext() *ClusterSchedulingContext {
    return m.clusterSchedulingContext
}
