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

package cache

import (
    "errors"
    "fmt"
    "github.com/golang/glog"
    "github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/cache/cacheevent"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/commonevents"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/strings"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/handler"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/rmproxy/rmevent"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/scheduler/schedulerevent"
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
}

func NewClusterInfo() *ClusterInfo {
    clusterInfo := &ClusterInfo{
        partitions:             make(map[string]*PartitionInfo),
        pendingRmEvents:        make(chan interface{}, 1024*1024),
        pendingSchedulerEvents: make(chan interface{}, 1024*1024),
    }

    return clusterInfo
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
            m.processAllocationReleasesRequest(v)
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

func (m *ClusterInfo) addApplicationToPartition(appInfo *ApplicationInfo, failIfExist bool) error {
    m.lock.Lock()
    defer m.lock.Unlock()

    partitionInfo := m.partitions[appInfo.Partition]
    if partitionInfo == nil {
        return errors.New(fmt.Sprintf("failed to add app=%s to partition=%s, partition doesn't exist", appInfo.ApplicationId, appInfo.Partition))
    }

    if j := partitionInfo.applications[appInfo.ApplicationId]; j != nil {
        if failIfExist {
            return errors.New(fmt.Sprintf("App=%s already existed in partition=%s", appInfo.ApplicationId, appInfo.Partition))
        } else {
            return nil
        }
    }

    // check if queue exist, and it is a leaf queue
    // TODO. add acl check
    queue := partitionInfo.getQueue(appInfo.QueueName)
    if queue == nil || !queue.IsLeafQueue() {
        return errors.New(fmt.Sprintf("failed to submit app=%s to queue=%s, partitio=%s, because queue doesn't exist or queue is not leaf queue", appInfo.ApplicationId,
            appInfo.QueueName, appInfo.Partition))
    }

    // All checked, app can be added.
    partitionInfo.applications[appInfo.ApplicationId] = appInfo

    appInfo.LeafQueue = queue

    return nil
}

func (m *ClusterInfo) addPartition(name string, info *PartitionInfo) {
    m.lock.Lock()
    defer m.lock.Unlock()
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
                rejectedApps = append(rejectedApps, &si.RejectedApplication{ApplicationId: app.ApplicationId, Reason: err.Error()})
            } else {
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
                glog.V(2).Infoln(msg)
                rejectedAsks = append(rejectedAsks, &si.RejectedAllocationAsk{
                    AllocationKey: allocationKey,
                    ApplicationId: req.ApplicationId,
                    Reason: msg})
                continue
            }

            // if app info doesn't exist, reject the request
            if partitionContext.getApplication(req.ApplicationId) == nil {
                rejectedAsks = append(rejectedAsks,
                    &si.RejectedAllocationAsk{
                        AllocationKey: allocationKey,
                        ApplicationId: req.ApplicationId,
                        Reason:        fmt.Sprintf("Failed to find app=%s for allocation=%s", req.ApplicationId, allocationKey),
                    })
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
                glog.Warning(errorMessage)
                rejectedNodes = append(rejectedNodes, &si.RejectedNode{NodeId: node.NodeId, Reason: errorMessage})
                continue
            }

            if partition := m.GetPartition(nodeInfo.Partition); partition != nil {
                err := partition.addNewNode(nodeInfo, node.ExistingAllocations)
                if err == nil {
                    glog.V(0).Infof("Successfully added node=%s, partition=%s", node.NodeId, nodeInfo.Partition)
                    acceptedNodes = append(acceptedNodes, &si.AcceptedNode{NodeId: node.NodeId})
                    continue
                } else {
                    errorMessage := fmt.Sprintf("Failures when add new node, removing the node, error=%s", err)
                    glog.Warning(errorMessage)
                    partition.removeNode(node.NodeId)
                    rejectedNodes = append(rejectedNodes, &si.RejectedNode{NodeId: node.NodeId, Reason: errorMessage})
                    continue
                }
            } else {
                errorMessage := fmt.Sprintf("Failed to find partition=%s for new node=%s", nodeInfo.Partition, node.NodeId)
                glog.Warning(errorMessage)
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
    }

    updatedPartitionsInterfaces := make([]interface{}, 0)
    for _, u := range updatedPartitions {
        updatedPartitionsInterfaces = append(updatedPartitionsInterfaces, u)
    }

    // Send updated partitions to scheduler
    m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerUpdatePartitionsConfigEvent{
        UpdatedPartitions: updatedPartitionsInterfaces,
        ResultChannel:     event.Channel,
    })

    deletedPartitionsInterfaces := make([]interface{}, 0)
    for _, u := range deletedPartitions {
        deletedPartitionsInterfaces = append(deletedPartitionsInterfaces, u)
    }

    // Send deleted partitions to the scheduler
    m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerDeletePartitionsConfigEvent{
        DeletePartitions:  deletedPartitionsInterfaces,
        ResultChannel:     event.Channel,
    })
}

func (m *ClusterInfo) processAllocationProposalEvent(event *cacheevent.AllocationProposalBundleEvent) {
    if len(event.AllocationProposals) != 0 && len(event.ReleaseProposals) != 0 {
        panic(fmt.Sprintf("Received event = %s, we now only support #allocation=1 and #release = 0, for every event, please double check.", strings.PrettyPrintStruct(event)))
    }

    // Hold write lock of cache
    m.lock.Lock()
    defer m.lock.Unlock()

    proposal := event.AllocationProposals[0]

    allocInfo, err := m.partitions[proposal.PartitionName].addNewAllocationForSchedulingAllocation(proposal)
    if err != nil {
        // Send reject event back to scheduler
        m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerAllocationUpdatesEvent{
            RejectedAllocations: []*commonevents.AllocationProposal{
                proposal,
            },
        })
    } else {
        rmId := common.GetRMIdFromPartitionName(proposal.PartitionName)

        // Send allocation event to RM.
        m.EventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMNewAllocationsEvent{
            Allocations: []*si.Allocation{allocInfo.AllocationProto},
            RMId:        rmId,
        })

        // Send allocation event to Scheduler
        // TODO
        //m.EventHandlers.SchedulerEventHandler.HandleEvent(&rmevent.RMNewAllocationsEvent{
        //    Allocations: []*si.Allocation{allocInfo.AllocationProto},
        //    RMId:        rmId,
        //})
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

func (m *ClusterInfo) processAllocationReleasesRequest(event *cacheevent.ReleaseAllocationsEvent) {
    if len(event.AllocationsToRelease) == 0 {
        return
    }

    // Hold write lock of cache
    m.lock.Lock()
    defer m.lock.Unlock()

    for _, toReleaseAllocation := range event.AllocationsToRelease {
        if partition := m.partitions[toReleaseAllocation.PartitionName]; partition != nil {
            releasedAllocations := partition.releaseAllocationsForApplication(toReleaseAllocation)
            m.notifyRMAllocationReleased(common.GetRMIdFromPartitionName(toReleaseAllocation.PartitionName), releasedAllocations, toReleaseAllocation.ReleaseType,
                toReleaseAllocation.Message)
        }
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
        glog.V(2).Infof("Enqueued event=%s, current queue size=%d", ev, len(queue))
    default:
        panic(fmt.Sprintf("Failed to enqueue event=%s", reflect.TypeOf(ev).String()))
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
