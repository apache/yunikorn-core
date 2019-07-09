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

package cache

import (
    "fmt"
    "github.com/cloudera/yunikorn-core/pkg/cache/cacheevent"
    "github.com/cloudera/yunikorn-core/pkg/common"
    "github.com/cloudera/yunikorn-core/pkg/common/commonevents"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/handler"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "github.com/cloudera/yunikorn-core/pkg/metrics"
    "github.com/cloudera/yunikorn-core/pkg/rmproxy/rmevent"
    "github.com/cloudera/yunikorn-core/pkg/scheduler/schedulerevent"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "go.uber.org/zap"
    "reflect"
    "sync"
)

type ClusterInfo struct {
    partitions     map[string]*PartitionInfo
    lock           sync.RWMutex
    policyGroup    string

    // Event queues
    pendingRmEvents        chan interface{}
    pendingSchedulerEvents chan interface{}

    // RM Event Handler
    EventHandlers handler.EventHandlers

    // Reference to scheduler metrics
    metrics metrics.CoreSchedulerMetrics
}

func NewClusterInfo() (*ClusterInfo, metrics.CoreSchedulerMetrics) {
    clusterInfo := &ClusterInfo{
        partitions:             make(map[string]*PartitionInfo),
        pendingRmEvents:        make(chan interface{}, 1024*1024),
        pendingSchedulerEvents: make(chan interface{}, 1024*1024),
    }

    clusterInfo.metrics = metrics.GetInstance()

    return clusterInfo, clusterInfo.metrics
}

// Start service
func (m *ClusterInfo) StartService(handlers handler.EventHandlers) {
    m.EventHandlers = handlers

    // Start event handlers
    go m.handleRMEvents()
    go m.handleSchedulerEvents()
}

func (m *ClusterInfo) handleSchedulerEvents() {
    for {
        ev := <-m.pendingSchedulerEvents
        switch v := ev.(type) {
        case *cacheevent.AllocationProposalBundleEvent:
            m.processAllocationProposalEvent(v)
        case *cacheevent.RejectedNewApplicationEvent:
            m.processRejectedApplicationEvent(v)
        case *cacheevent.ReleaseAllocationsEvent:
            m.handleAllocationReleasesRequestEvent(v)
        case *cacheevent.RemovedApplicationEvent:
            m.processRemovedApplication(v)
        case *commonevents.RemoveRMPartitionsEvent:
            m.processRemoveRMPartitionsEvent(v)
        default:
            panic(fmt.Sprintf("%s is not an acceptable type for scheduler event.", reflect.TypeOf(v).String()))
        }
    }
}

func (m *ClusterInfo) handleRMEvents() {
    for {
        ev := <-m.pendingRmEvents
        switch v := ev.(type) {
        case *cacheevent.RMUpdateRequestEvent:
            m.processRMUpdateEvent(v)
        case *commonevents.RegisterRMEvent:
            m.processRMRegistrationEvent(v)
        case *commonevents.ConfigUpdateRMEvent:
            m.processRMConfigUpdateEvent(v)
        default:
            panic(fmt.Sprintf("%s is not an acceptable type for RM event.", reflect.TypeOf(v).String()))
        }
    }
}

func (m *ClusterInfo) GetPartition(name string) *PartitionInfo {
    m.lock.RLock()
    defer m.lock.RUnlock()
    return m.partitions[name]
}

func (m* ClusterInfo) GetTotalPartitionResource(partitionName string) *resources.Resource {
    if p := m.GetPartition(partitionName); p != nil {
        return p.GetTotalPartitionResource()
    }
    return nil
}

func (m *ClusterInfo) addApplicationToPartition(appInfo *ApplicationInfo, failIfExist bool) error {
    m.lock.Lock()
    defer m.lock.Unlock()

    partitionInfo := m.partitions[appInfo.Partition]
    if partitionInfo == nil {
        return fmt.Errorf("failed to add application %s to partition %s, partition doesn't exist", appInfo.ApplicationId, appInfo.Partition)
    }

    return partitionInfo.addNewApplication(appInfo, failIfExist)
}

func (m *ClusterInfo) addPartition(name string, info *PartitionInfo) {
    m.lock.Lock()
    defer m.lock.Unlock()
    info.metrics = m.metrics
    m.partitions[name] = info
}

func (m *ClusterInfo) removePartition(name string) {
    m.lock.Lock()
    defer m.lock.Unlock()
    delete(m.partitions, name)
}

func (m *ClusterInfo) processApplicationUpdateFromRMUpdate(request *si.UpdateRequest) {
    if len(request.NewApplications) > 0 || len(request.RemoveApplications) > 0 {
        addedAppInfos := make([]*ApplicationInfo, 0)
        acceptedApps := make([]*si.AcceptedApplication, 0)
        rejectedApps := make([]*si.RejectedApplication, 0)

        for _, app := range request.NewApplications {
            appInfo := NewApplicationInfo(app.ApplicationId, app.PartitionName, app.QueueName)
            if err := m.addApplicationToPartition(appInfo, true); err != nil {
                m.metrics.IncTotalApplicationsRejected()
                rejectedApps = append(rejectedApps, &si.RejectedApplication{ApplicationId: app.ApplicationId, Reason: err.Error()})
            } else {
                // Update metrics with accepted applications
                m.metrics.IncTotalApplicationsAdded()
                m.metrics.IncTotalApplicationsRunning()
                acceptedApps = append(acceptedApps, &si.AcceptedApplication{ApplicationId: app.ApplicationId})
                addedAppInfos = append(addedAppInfos, appInfo)
            }
        }

        // Respond to RMProxy
        m.EventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMApplicationUpdateEvent{
            RMId:                 request.RmId,
            AcceptedApplications: acceptedApps,
            RejectedApplications: rejectedApps,
        })

        if len(addedAppInfos) > 0 {
            addedAppInfosInterface := make([]interface{}, 0)
            for _, j := range addedAppInfos {
                addedAppInfosInterface = append(addedAppInfosInterface, j)
            }

            // Update metrics with removed applications
            m.metrics.SubTotalApplicationsRunning(len(request.RemoveApplications))
            // ToDO: need to improve this once we have state in YuniKorn for apps.
            m.metrics.AddTotalApplicationsCompleted(len(request.RemoveApplications))

            // Send message to Scheduler
            m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerApplicationsUpdateEvent{
                AddedApplications:   addedAppInfosInterface,
                RemovedApplications: request.RemoveApplications})
        }
    }
}

func (m *ClusterInfo) processNewAndReleaseAllocationRequests(request *si.UpdateRequest) {

    if len(request.Asks) > 0 || request.Releases != nil {
        // Send back to RM
        rejectedAsks := make([]*si.RejectedAllocationAsk, 0)

        // Send to scheduler
        for _, req := range request.Asks {
            allocationKey := req.AllocationKey

            // try to get ApplicationInfo
            partitionContext := m.GetPartition(req.PartitionName)
            if partitionContext == nil {
                msg := fmt.Sprintf("Failed to find partition=%s, for ask %s", req.PartitionName, allocationKey)
                log.Logger.Info(msg)
                rejectedAsks = append(rejectedAsks, &si.RejectedAllocationAsk{
                    AllocationKey: allocationKey,
                    ApplicationId: req.ApplicationId,
                    Reason: msg})
                continue
            }

            // if app info doesn't exist, reject the request
            appInfo := partitionContext.getApplication(req.ApplicationId)
            if appInfo == nil {
                rejectedAsks = append(rejectedAsks,
                    &si.RejectedAllocationAsk{
                        AllocationKey: allocationKey,
                        ApplicationId: req.ApplicationId,
                        Reason:        fmt.Sprintf("Failed to find app=%s for allocation=%s", req.ApplicationId, allocationKey),
                    })
            } else {
                // start to process allocation asks from this app
                // transit app's state to running
                appInfo.HandleApplicationEvent(RunApplication)
            }
        }

        // Reject asks to RM Proxy
        m.EventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMRejectedAllocationAskEvent{
            RMId:                   request.RmId,
            RejectedAllocationAsks: rejectedAsks,
        })

        // Send new asks, added applications, and new release allocation requests to scheduler
        m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerAllocationUpdatesEvent{
            NewAsks:    request.Asks,
            ToReleases: request.Releases,
        })
    }
}

func (m *ClusterInfo) processNodeUpdate(request *si.UpdateRequest) {
    // Process add node
    if len(request.NewSchedulableNodes) > 0 {
        acceptedNodes := make([]*si.AcceptedNode, 0)
        rejectedNodes := make([]*si.RejectedNode, 0)
        for _, node := range request.NewSchedulableNodes {
            nodeInfo, err := NewNodeInfo(node)
            if err != nil {
                errorMessage := fmt.Sprintf("Failed to create node info from request, nodeId=%s, error=%s", node.NodeId, err.Error())
                log.Logger.Info(errorMessage)
                // TODO assess impact of partition metrics (this never hit the partition)
                m.metrics.IncFailedNodes()
                rejectedNodes = append(rejectedNodes, &si.RejectedNode{NodeId: node.NodeId, Reason: errorMessage})
                continue
            }

            if partition := m.GetPartition(nodeInfo.Partition); partition != nil {
                err := partition.addNewNode(nodeInfo, node.ExistingAllocations)
                if err == nil {
                    log.Logger.Info("successfully added node",
                        zap.String("nodeId", node.NodeId),
                        zap.String("partition", nodeInfo.Partition))
                    acceptedNodes = append(acceptedNodes, &si.AcceptedNode{NodeId: node.NodeId})
                    continue
                } else {
                    errorMessage := fmt.Sprintf("Failure while adding new node, rejected the node, error=%s", err)
                    log.Logger.Warn(errorMessage)
                    rejectedNodes = append(rejectedNodes, &si.RejectedNode{NodeId: node.NodeId, Reason: errorMessage})
                    continue
                }
            } else {
                errorMessage := fmt.Sprintf("Failed to find partition=%s for new node=%s", nodeInfo.Partition, node.NodeId)
                log.Logger.Warn(errorMessage)
                // TODO assess impact of partition metrics (this never hit the partition)
                m.metrics.IncFailedNodes()
                rejectedNodes = append(rejectedNodes, &si.RejectedNode{NodeId: node.NodeId, Reason: errorMessage})
                continue
            }
        }

        m.EventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMNodeUpdateEvent{
            RMId:          request.RmId,
            AcceptedNodes: acceptedNodes,
            RejectedNodes: rejectedNodes,
        })
    }
}

// process events internally
func (m *ClusterInfo) processRMUpdateEvent(event *cacheevent.RMUpdateRequestEvent) {
    request := event.Request

    /* Order of following operations are important, don't change unless carefully thought*/

    // Add / remove app requested by RM.
    m.processApplicationUpdateFromRMUpdate(request)

    // Add new request, release allocation, cancel request
    m.processNewAndReleaseAllocationRequests(request)

    // Add / remove Nodes
    m.processNodeUpdate(request)

    m.lock.Lock()
    defer m.lock.Unlock()
}

func (m *ClusterInfo) processRMRegistrationEvent(event *commonevents.RegisterRMEvent) {
    updatedPartitions, err := SetClusterInfoFromConfigFile(m, event.RMRegistrationRequest.RmId, event.RMRegistrationRequest.PolicyGroup)
    if err != nil {
        event.Channel <- &commonevents.Result{Succeeded: false, Reason: err.Error()}
    }

    updatedPartitionsInterfaces := make([]interface{}, 0)
    for _, u := range updatedPartitions {
        updatedPartitionsInterfaces = append(updatedPartitionsInterfaces, u)
    }

    // Keep track of the config, cannot be changed for this RM
    m.policyGroup = event.RMRegistrationRequest.PolicyGroup

    // Send updated partitions to scheduler
    m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerUpdatePartitionsConfigEvent{
        UpdatedPartitions: updatedPartitionsInterfaces,
        ResultChannel:     event.Channel,
    })
}

func (m *ClusterInfo) processRMConfigUpdateEvent(event *commonevents.ConfigUpdateRMEvent) {
    updatedPartitions, deletedPartitions, err := UpdateClusterInfoFromConfigFile(m, event.RmId)
    if err != nil {
        event.Channel <- &commonevents.Result{Succeeded: false, Reason: err.Error()}
        return
    }

    // TODO inconsistent risk. What if cache updated but updating scheduler context failed?

    updatedPartitionsInterfaces := make([]interface{}, 0)
    for _, u := range updatedPartitions {
        updatedPartitionsInterfaces = append(updatedPartitionsInterfaces, u)
    }

    // Send updated partitions to scheduler
    updatePartitionResult := make(chan *commonevents.Result)
    m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerUpdatePartitionsConfigEvent{
        UpdatedPartitions: updatedPartitionsInterfaces,
        ResultChannel:     updatePartitionResult,
    })
    result := <- updatePartitionResult
    if !result.Succeeded {
        event.Channel <- &commonevents.Result{Succeeded: false, Reason: result.Reason}
        return
    }

    deletedPartitionsInterfaces := make([]interface{}, 0)
    for _, u := range deletedPartitions {
        deletedPartitionsInterfaces = append(deletedPartitionsInterfaces, u)
    }

    // Send deleted partitions to the scheduler
    deletePartitionResult := make(chan *commonevents.Result)
    m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerDeletePartitionsConfigEvent{
        DeletePartitions: deletedPartitionsInterfaces,
        ResultChannel:    deletePartitionResult,
    })
    result = <- deletePartitionResult
    if !result.Succeeded {
        event.Channel <- &commonevents.Result{Succeeded: false, Reason: result.Reason}
        return
    }

    // all succeed
    event.Channel <- &commonevents.Result{Succeeded: true}
}

func (m *ClusterInfo) processAllocationProposalEvent(event *cacheevent.AllocationProposalBundleEvent) {
    // Hold write lock of cache
    m.lock.Lock()
    defer m.lock.Unlock()

    partitionInfo := m.partitions[event.PartitionName]

    // Try to release allocations specified in the bundle
    m.processAllocationReleases(event.ReleaseProposals)

    // Skip allocation if nothing here.
    if len(event.AllocationProposals) == 0 {
        return
    }

    // we currently only support 1 allocation in the list, fail if there are more
    if len(event.AllocationProposals) != 1 {
        // Send reject event back to scheduler
        m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerAllocationUpdatesEvent{
            RejectedAllocations: event.AllocationProposals,
        })
    }
    proposal := event.AllocationProposals[0]
    allocInfo, err := partitionInfo.addNewAllocation(proposal)
    if err != nil {
        // Send reject event back to scheduler
        m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerAllocationUpdatesEvent{
            RejectedAllocations: event.AllocationProposals,
        })
    } else {
        rmId := common.GetRMIdFromPartitionName(proposal.PartitionName)

        // Send allocation event to RM.
        m.EventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMNewAllocationsEvent{
            Allocations: []*si.Allocation{allocInfo.AllocationProto},
            RMId:        rmId,
        })
    }
}

func (m *ClusterInfo) processRejectedApplicationEvent(event *cacheevent.RejectedNewApplicationEvent) {
    // rejected apps will not be added into cache, so there is nothing to do here for now.
    // we might revisit this if we further support app states in the cache.
}

func (m *ClusterInfo) notifyRMAllocationReleased(rmId string, released []*AllocationInfo, terminationType si.AllocationReleaseResponse_TerminationType, message string) {
    if len(released) == 0 {
        return
    }

    releaseEvent := &rmevent.RMReleaseAllocationEvent{
        ReleasedAllocations: make([]*si.AllocationReleaseResponse, 0),
        RMId:                rmId,
    }
    for _, alloc := range released {
        releaseEvent.ReleasedAllocations = append(releaseEvent.ReleasedAllocations, &si.AllocationReleaseResponse{
            AllocationUUID:  alloc.AllocationProto.Uuid,
            TerminationType: terminationType,
            Message:         message,
        })
    }

    m.EventHandlers.RMProxyEventHandler.HandleEvent(releaseEvent)
}

func (m *ClusterInfo) processAllocationReleases(toReleases []*commonevents.ReleaseAllocation) {
    if len(toReleases) == 0 {
        return
    }

    for _, toReleaseAllocation := range toReleases {
        if partition := m.partitions[toReleaseAllocation.PartitionName]; partition != nil {
            releasedAllocations := partition.releaseAllocationsForApplication(toReleaseAllocation)
            m.notifyRMAllocationReleased(common.GetRMIdFromPartitionName(toReleaseAllocation.PartitionName), releasedAllocations, toReleaseAllocation.ReleaseType,
                toReleaseAllocation.Message)
        }
    }
}

func (m *ClusterInfo) handleAllocationReleasesRequestEvent(event *cacheevent.ReleaseAllocationsEvent) {
    // Hold write lock of cache
    m.lock.Lock()
    defer m.lock.Unlock()

    m.processAllocationReleases(event.AllocationsToRelease)
}

func (m *ClusterInfo) processRemoveRMPartitionsEvent(event *commonevents.RemoveRMPartitionsEvent) {
    // Hold write lock of cache
    m.lock.Lock()
    defer m.lock.Unlock()

    toRemove := make(map[string]bool)

    for partition, partitionContext := range m.partitions {
        if partitionContext.RMId == event.RmId {
            toRemove[partition] = true
        }
    }

    for partition := range toRemove {
        delete(m.partitions, partition)
    }

    // Done, notify channel
    event.Channel <- &commonevents.Result{
        Succeeded: true,
    }
}

func (m *ClusterInfo) processRemovedApplication(event *cacheevent.RemovedApplicationEvent) {
    // Hold write lock of cache
    m.lock.Lock()
    defer m.lock.Unlock()

    if partition := m.GetPartition(event.PartitionName); partition != nil {
        _, allocations := partition.RemoveApplication(event.ApplicationId)

        m.notifyRMAllocationReleased(common.GetRMIdFromPartitionName(event.PartitionName), allocations, si.AllocationReleaseResponse_STOPPED_BY_RM,
            fmt.Sprintf("App=%s Removed", event.ApplicationId))
    }
}

func enqueueAndCheckFull(queue chan interface{}, ev interface{}) {
    select {
    case queue <- ev:
        log.Logger.Info("enqueued event",
            zap.String("event",  reflect.TypeOf(ev).String()),
            zap.Int("currentQueueSize", len(queue)))
    default:
        log.Logger.Panic("failed to enqueue event",
            zap.String("event", reflect.TypeOf(ev).String()))
    }
}

func (m *ClusterInfo) ListPartitions() []string {
    m.lock.RLock()
    defer m.lock.RUnlock()
    var partitions []string
    for k := range m.partitions {
        partitions = append(partitions, k)
    }
    return partitions
}

// Implement methods for Cache events
func (m *ClusterInfo) HandleEvent(ev interface{}) {
    switch v := ev.(type) {
    case *cacheevent.AllocationProposalBundleEvent:
        enqueueAndCheckFull(m.pendingSchedulerEvents, v)
    case *cacheevent.RejectedNewApplicationEvent:
        enqueueAndCheckFull(m.pendingSchedulerEvents, v)
    case *cacheevent.ReleaseAllocationsEvent:
        enqueueAndCheckFull(m.pendingSchedulerEvents, v)
    case *commonevents.RemoveRMPartitionsEvent:
        enqueueAndCheckFull(m.pendingSchedulerEvents, v)
    case *cacheevent.RemovedApplicationEvent:
        enqueueAndCheckFull(m.pendingSchedulerEvents, v)
    case *cacheevent.RMUpdateRequestEvent:
        enqueueAndCheckFull(m.pendingRmEvents, v)
    case *commonevents.RegisterRMEvent:
        enqueueAndCheckFull(m.pendingRmEvents, v)
    case *commonevents.ConfigUpdateRMEvent:
        enqueueAndCheckFull(m.pendingRmEvents, v)
    default:
        panic(fmt.Sprintf("Received unexpected event type = %s", reflect.TypeOf(v).String()))
    }
}
