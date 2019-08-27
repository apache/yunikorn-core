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
    partitions  map[string]*PartitionInfo
    lock        sync.RWMutex
    policyGroup string

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

func enqueueAndCheckFull(queue chan interface{}, ev interface{}) {
    select {
    case queue <- ev:
        log.Logger.Debug("enqueued event",
            zap.String("eventType", reflect.TypeOf(ev).String()),
            zap.Any("event", ev),
            zap.Int("currentQueueSize", len(queue)))
    default:
        log.Logger.DPanic("failed to enqueue event",
            zap.String("event", reflect.TypeOf(ev).String()))
    }
}

// Get the list of partitions.
// Locked call, used outside of the cache.
func (m *ClusterInfo) ListPartitions() []string {
    m.lock.RLock()
    defer m.lock.RUnlock()
    var partitions []string
    for k := range m.partitions {
        partitions = append(partitions, k)
    }
    return partitions
}

// Get the partition by name.
// Locked call, used outside of the cache.
func (m *ClusterInfo) GetPartition(name string) *PartitionInfo {
    m.lock.RLock()
    defer m.lock.RUnlock()
    return m.partitions[name]
}

// Get the total resources for the partition by name.
// Locked call, used outside of the cache.
func (m *ClusterInfo) GetTotalPartitionResource(partitionName string) *resources.Resource {
    if p := m.GetPartition(partitionName); p != nil {
        return p.GetTotalPartitionResource()
    }
    return nil
}

// Add a new partition to the cluster, locked.
// Called by the configuration update.
func (m *ClusterInfo) addPartition(name string, info *PartitionInfo) {
    m.lock.Lock()
    defer m.lock.Unlock()
    info.metrics = m.metrics
    m.partitions[name] = info
}

// Remove a partition from the cluster, locked.
// Called by the partition itself when the partition is removed by the partition manager.
func (m *ClusterInfo) removePartition(name string) {
    m.lock.Lock()
    defer m.lock.Unlock()
    delete(m.partitions, name)
}

// Process the application update. Add and remove applications from the partitions.
// Lock free call, all updates occur on the underlying partition which is locked, or via events.
func (m *ClusterInfo) processApplicationUpdateFromRMUpdate(request *si.UpdateRequest) {
    if len(request.NewApplications) == 0 && len(request.RemoveApplications) == 0 {
        return
    }
    addedAppInfosInterface := make([]interface{}, 0)
    rejectedApps := make([]*si.RejectedApplication, 0)

    for _, app := range request.NewApplications {
        partitionInfo := m.GetPartition(app.PartitionName)
        if partitionInfo == nil {
            msg := fmt.Sprintf("Failed to add application %s to partition %s, partition doesn't exist", app.ApplicationId, app.PartitionName)
            log.Logger.Info(msg)
            rejectedApps = append(rejectedApps, &si.RejectedApplication{
                ApplicationId: app.ApplicationId,
                Reason:        msg,
            })
            continue
        }
        // convert and resolve the user: cache can be set per partition
        ugi, err := partitionInfo.convertUGI(app.Ugi)
        if err != nil {
            m.metrics.IncTotalApplicationsRejected()
            rejectedApps = append(rejectedApps, &si.RejectedApplication{
                ApplicationId: app.ApplicationId,
                Reason:        err.Error(),
            })
            continue
        }
        // create a new app object and add it to the partition (partition logs details)
        appInfo := NewApplicationInfo(app.ApplicationId, app.PartitionName, app.QueueName, ugi, app.Tags)
        if err := partitionInfo.addNewApplication(appInfo, true); err != nil {
            m.metrics.IncTotalApplicationsRejected()
            rejectedApps = append(rejectedApps, &si.RejectedApplication{
                ApplicationId: app.ApplicationId,
                Reason:        err.Error(),
            })
            continue
        }
        // just add the apps: the scheduler might reject one later
        m.metrics.IncTotalApplicationsAdded()
        addedAppInfosInterface = append(addedAppInfosInterface, appInfo)
    }

    // Respond to RMProxy with already rejected apps if needed
    if len(rejectedApps) > 0 {
        m.EventHandlers.RMProxyEventHandler.HandleEvent(
            &rmevent.RMApplicationUpdateEvent{
                RMId:                 request.RmId,
                AcceptedApplications: make([]*si.AcceptedApplication, 0),
                RejectedApplications: rejectedApps,
            })
    }

    // Update metrics with removed applications
    if len(request.RemoveApplications) > 0 {
        m.metrics.SubTotalApplicationsRunning(len(request.RemoveApplications))
        // ToDO: need to improve this once we have state in YuniKorn for apps.
        m.metrics.AddTotalApplicationsCompleted(len(request.RemoveApplications))
    }
    // Send message to Scheduler if we have anything to process (remove and or add)
    if len(request.RemoveApplications) > 0 || len(addedAppInfosInterface) > 0 {
        m.EventHandlers.SchedulerEventHandler.HandleEvent(
            &schedulerevent.SchedulerApplicationsUpdateEvent{
                AddedApplications:   addedAppInfosInterface,
                RemovedApplications: request.RemoveApplications,
            })
    }
}

// Process the allocation updates. Add and remove allocations for the applications.
// Lock free call, all updates occur on the underlying application which is locked or via events.
func (m *ClusterInfo) processNewAndReleaseAllocationRequests(request *si.UpdateRequest) {
    if len(request.Asks) == 0 && request.Releases == nil {
        return
    }
    // Send rejects back to RM
    rejectedAsks := make([]*si.RejectedAllocationAsk, 0)

    // Send to scheduler
    for _, req := range request.Asks {
        // try to get ApplicationInfo
        partitionInfo := m.GetPartition(req.PartitionName)
        if partitionInfo == nil {
            msg := fmt.Sprintf("Failed to find partition %s, for application %s and allocation %s", req.PartitionName, req.ApplicationId, req.AllocationKey)
            log.Logger.Info(msg)
            rejectedAsks = append(rejectedAsks, &si.RejectedAllocationAsk{
                AllocationKey: req.AllocationKey,
                ApplicationId: req.ApplicationId,
                Reason:        msg,
            })
            continue
        }

        // if app info doesn't exist, reject the request
        appInfo := partitionInfo.getApplication(req.ApplicationId)
        if appInfo == nil {
            msg := fmt.Sprintf("Failed to find applictaion %s, for allocation %s", req.ApplicationId, req.AllocationKey)
            log.Logger.Info(msg)
            rejectedAsks = append(rejectedAsks,
                &si.RejectedAllocationAsk{
                    AllocationKey: req.AllocationKey,
                    ApplicationId: req.ApplicationId,
                    Reason:        msg,
                })
            continue
        }
        // start to process allocation asks from this app
        // transit app's state to running
        err := appInfo.HandleApplicationEvent(RunApplication)
        if err != nil {
            log.Logger.Debug("Application state change failed",
                zap.Error(err))
        }
    }

    // Reject asks returned to RM Proxy for the apps and partitions not found
    if len(rejectedAsks) > 0 {
        m.EventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMRejectedAllocationAskEvent{
            RMId:                   request.RmId,
            RejectedAllocationAsks: rejectedAsks,
        })
    }

    // Send all asks and release allocation requests to scheduler
    m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerAllocationUpdatesEvent{
        NewAsks:    request.Asks,
        ToReleases: request.Releases,
    })
}

// Process the node updates: add and remove nodes as needed.
// Lock free call, all updates occur on the underlying application which is locked or via events.
func (m *ClusterInfo) processNodeUpdate(request *si.UpdateRequest) {
    // Process add node
    if len(request.NewSchedulableNodes) == 0 {
        return
    }
    acceptedNodes := make([]*si.AcceptedNode, 0)
    rejectedNodes := make([]*si.RejectedNode, 0)
    existingAllocations := make([]*si.Allocation, 0)
    for _, node := range request.NewSchedulableNodes {
        nodeInfo, err := NewNodeInfo(node)
        if err != nil {
            msg := fmt.Sprintf("Failed to create node info from request, nodeId %s, error %s", node.NodeId, err.Error())
            log.Logger.Info(msg)
            // TODO assess impact of partition metrics (this never hit the partition)
            m.metrics.IncFailedNodes()
            rejectedNodes = append(rejectedNodes,
                &si.RejectedNode{
                    NodeId: node.NodeId,
                    Reason: msg,
                })
            continue
        }
        partition := m.GetPartition(nodeInfo.Partition)
        if partition == nil {
            msg := fmt.Sprintf("Failed to find partition %s for new node %s", nodeInfo.Partition, node.NodeId)
            log.Logger.Info(msg)
            // TODO assess impact of partition metrics (this never hit the partition)
            m.metrics.IncFailedNodes()
            rejectedNodes = append(rejectedNodes,
                &si.RejectedNode{
                    NodeId: node.NodeId,
                    Reason: msg,
                })
            continue
        }
        err = partition.addNewNode(nodeInfo, node.ExistingAllocations)
        if err != nil {
            msg := fmt.Sprintf("Failure while adding new node, node rejectd with error %s", err.Error())
            log.Logger.Warn(msg)
            rejectedNodes = append(rejectedNodes,
                &si.RejectedNode{
                    NodeId: node.NodeId,
                    Reason: msg,
                })
            continue
        }
        log.Logger.Info("successfully added node",
            zap.String("nodeId", node.NodeId),
            zap.String("partition", nodeInfo.Partition))
        acceptedNodes = append(acceptedNodes, &si.AcceptedNode{NodeId: node.NodeId})
        existingAllocations = append(existingAllocations, node.ExistingAllocations...)
    }

    m.EventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMNodeUpdateEvent{
        RMId:          request.RmId,
        AcceptedNodes: acceptedNodes,
        RejectedNodes: rejectedNodes,
    })

    // notify the scheduler to recover existing allocations
    m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerAllocationUpdatesEvent{
        ExistingAllocations: existingAllocations,
        RMId:                request.RmId,
    })
}

// Process RM event internally. Split in steps that handle specific parts.
// Lock free call, all updates occur in other methods.
func (m *ClusterInfo) processRMUpdateEvent(event *cacheevent.RMUpdateRequestEvent) {
    // Order of following operations are important,
    // don't change unless carefully thought
    request := event.Request
    // 1) Add / remove app requested by RM.
    m.processApplicationUpdateFromRMUpdate(request)
    // 2) Add new request, release allocation, cancel request
    m.processNewAndReleaseAllocationRequests(request)
    // 3) Add / remove Nodes
    m.processNodeUpdate(request)
}

// Process the RM registration
// Updated partitions can not fail on the scheduler side.
// Locking occurs by the methods that are called, this must be lock free.
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

// Process a configuration update.
// The configuration is syntax checked as part of the update of the cluster from the file.
// Updated and deleted partitions can not fail on the scheduler side.
// Locking occurs by the methods that are called, this must be lock free.
func (m *ClusterInfo) processRMConfigUpdateEvent(event *commonevents.ConfigUpdateRMEvent) {
    updatedPartitions, deletedPartitions, err := UpdateClusterInfoFromConfigFile(m, event.RmId)
    if err != nil {
        event.Channel <- &commonevents.Result{Succeeded: false, Reason: err.Error()}
        return
    }

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
    result := <-updatePartitionResult
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
    result = <-deletePartitionResult
    if !result.Succeeded {
        event.Channel <- &commonevents.Result{Succeeded: false, Reason: result.Reason}
        return
    }

    // all succeed
    event.Channel <- &commonevents.Result{Succeeded: true}
}

// Process an allocation bundle which could contain release and allocation proposals.
// Lock free call, all updates occur on the underlying partition which is locked or via events.
func (m *ClusterInfo) processAllocationProposalEvent(event *cacheevent.AllocationProposalBundleEvent) {
    // Release if there is anything to release
    if len(event.ReleaseProposals) > 0 {
        m.processAllocationReleases(event.ReleaseProposals)
    }
    // Skip allocation if nothing here.
    if len(event.AllocationProposals) == 0 {
        return
    }

    // we currently only support 1 allocation in the list, fail if there are more
    if len(event.AllocationProposals) != 1 {
        log.Logger.Info("More than 1 allocation proposal rejected all but first",
            zap.Int("allocPropLength", len(event.AllocationProposals)))
        // Send reject event back to scheduler for all but first
        m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerAllocationUpdatesEvent{
            RejectedAllocations: event.AllocationProposals[1:],
        })
    }
    proposal := event.AllocationProposals[0]
    partitionInfo := m.GetPartition(proposal.PartitionName)
    allocInfo, err := partitionInfo.addNewAllocation(proposal)
    if err != nil {
        // Send reject event back to scheduler
        m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerAllocationUpdatesEvent{
            RejectedAllocations: event.AllocationProposals,
        })
        return
    }
    rmId := common.GetRMIdFromPartitionName(proposal.PartitionName)

    // Send allocation event to RM.
    m.EventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMNewAllocationsEvent{
        Allocations: []*si.Allocation{allocInfo.AllocationProto},
        RMId:        rmId,
    })
}

// Rejected application from the scheduler.
// Cleanup the app from the partition.
// Lock free call, all updates occur in the partition which is locked.
func (m *ClusterInfo) processRejectedApplicationEvent(event *cacheevent.RejectedNewApplicationEvent) {
    if partition := m.GetPartition(event.PartitionName); partition != nil {
        partition.RemoveRejectedApp(event.ApplicationId)
    }
}

// Create a RM update event to notify RM of released allocations
// Lock free call, all updates occur via events.
func (m *ClusterInfo) notifyRMAllocationReleased(rmId string, released []*AllocationInfo, terminationType si.AllocationReleaseResponse_TerminationType, message string) {
    releaseEvent := &rmevent.RMReleaseAllocationEvent{
        ReleasedAllocations: make([]*si.AllocationReleaseResponse, 0),
        RMId:                rmId,
    }
    for _, alloc := range released {
        releaseEvent.ReleasedAllocations = append(releaseEvent.ReleasedAllocations, &si.AllocationReleaseResponse{
            Uuid:            alloc.AllocationProto.Uuid,
            TerminationType: terminationType,
            Message:         message,
        })
    }

    m.EventHandlers.RMProxyEventHandler.HandleEvent(releaseEvent)
}

// Process the allocations to release.
// Lock free call, all updates occur via events.
func (m *ClusterInfo) processAllocationReleases(toReleases []*commonevents.ReleaseAllocation) {
    // Try to release allocations specified in the events
    for _, toReleaseAllocation := range toReleases {
        partitionInfo := m.GetPartition(toReleaseAllocation.PartitionName)
        if partitionInfo == nil {
            log.Logger.Info("Failed to find partition for allocation proposal",
                zap.String("partitionName", toReleaseAllocation.PartitionName))
            continue
        }
        rmID := common.GetRMIdFromPartitionName(toReleaseAllocation.PartitionName)
        // release the allocation from the partition
        releasedAllocations := partitionInfo.releaseAllocationsForApplication(toReleaseAllocation)
        // whatever was released pass it back to the RM
        if len(releasedAllocations) != 0 {
            m.notifyRMAllocationReleased(rmID, releasedAllocations, toReleaseAllocation.ReleaseType, toReleaseAllocation.Message)
        }
    }
}

// Process the allocation release event.
func (m *ClusterInfo) handleAllocationReleasesRequestEvent(event *cacheevent.ReleaseAllocationsEvent) {
    // Release if there is anything to release.
    if len(event.AllocationsToRelease) > 0 {
        m.processAllocationReleases(event.AllocationsToRelease)
    }
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

// Process an application removal send by the RM
// Lock free call, all updates occur in the partition which is locked
func (m *ClusterInfo) processRemovedApplication(event *cacheevent.RemovedApplicationEvent) {
    partitionInfo := m.GetPartition(event.PartitionName)
    if partitionInfo == nil {
        log.Logger.Info("Failed to find partition for allocation proposal",
            zap.String("partitionName", event.PartitionName))
        return
    }
    _, allocations := partitionInfo.RemoveApplication(event.ApplicationId)
    log.Logger.Info("Removed application from partition",
        zap.String("applicationId", event.ApplicationId),
        zap.String("partitionName", event.PartitionName),
        zap.Int("allocationsRemoved", len(allocations)))

    if len(allocations) > 0 {
        rmID := common.GetRMIdFromPartitionName(event.PartitionName)
        m.notifyRMAllocationReleased(rmID, allocations, si.AllocationReleaseResponse_STOPPED_BY_RM,
            fmt.Sprintf("Application %s Removed", event.ApplicationId))
    }
}
