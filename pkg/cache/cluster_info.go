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

package cache

import (
	"fmt"
	"reflect"
	"sync"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/cache/cacheevent"
	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/commonevents"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/handler"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/schedulerevent"
	siCommon "github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type ClusterInfo struct {
	partitions  map[string]*PartitionInfo
	policyGroup string

	// Event queues
	pendingRmEvents        chan interface{}
	pendingSchedulerEvents chan interface{}

	// RM Event Handler
	EventHandlers handler.EventHandlers

	sync.RWMutex
}

func NewClusterInfo() (info *ClusterInfo) {
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
			m.handleAllocationReleasesRequestEvent(v)
		case *cacheevent.RemovedApplicationEvent:
			m.processRemovedApplication(v)
		case *commonevents.RemoveRMPartitionsEvent:
			m.processRemoveRMPartitionsEvent(v)
		default:
			log.Logger().Error("Received type is not an acceptable type for scheduler event.",
				zap.String("received type", reflect.TypeOf(v).String()))
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
			log.Logger().Error("Received type is not an acceptable type for RM event.",
				zap.String("received type", reflect.TypeOf(v).String()))
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
func (m *ClusterInfo) GetPolicyGroup() string {
	m.RLock()
	defer m.RUnlock()
	return m.policyGroup
}

// Get the list of partitions.
// Locked call, used outside of the cache.
func (m *ClusterInfo) ListPartitions() []string {
	m.RLock()
	defer m.RUnlock()
	var partitions []string
	for k := range m.partitions {
		partitions = append(partitions, k)
	}
	return partitions
}

// Get the partition by name.
// Locked call, used outside of the cache.
func (m *ClusterInfo) GetPartition(name string) *PartitionInfo {
	m.RLock()
	defer m.RUnlock()
	return m.partitions[name]
}

// Add a new partition to the cluster, locked.
// Called by the configuration update.
func (m *ClusterInfo) addPartition(name string, info *PartitionInfo) {
	m.Lock()
	defer m.Unlock()
	m.partitions[name] = info
}

// Remove a partition from the cluster, locked.
// Called by the partition itself when the partition is removed by the partition manager.
func (m *ClusterInfo) removePartition(name string) {
	m.Lock()
	defer m.Unlock()
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
		appInfo := NewApplicationInfo(app.ApplicationID, app.PartitionName, app.QueueName, ugi, app.Tags)
		if err = partitionInfo.addNewApplication(appInfo, true); err != nil {
			rejectedApps = append(rejectedApps, &si.RejectedApplication{
				ApplicationID: app.ApplicationID,
				Reason:        err.Error(),
			})
			continue
		}
		addedAppInfosInterface = append(addedAppInfosInterface, appInfo)
	}

	// Respond to RMProxy with already rejected apps if needed
	if len(rejectedApps) > 0 {
		m.EventHandlers.RMProxyEventHandler.HandleEvent(
			&rmevent.RMApplicationUpdateEvent{
				RmID:                 request.RmID,
				AcceptedApplications: make([]*si.AcceptedApplication, 0),
				RejectedApplications: rejectedApps,
			})
	}

	// Update metrics with removed applications
	if len(request.RemoveApplications) > 0 {
		metrics.GetSchedulerMetrics().SubTotalApplicationsRunning(len(request.RemoveApplications))
		// ToDO: need to improve this once we have state in YuniKorn for apps.
		metrics.GetSchedulerMetrics().AddTotalApplicationsCompleted(len(request.RemoveApplications))
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
			msg := fmt.Sprintf("Failed to find partition %s, for application %s and allocation %s", req.PartitionName, req.ApplicationID, req.AllocationKey)
			log.Logger().Info(msg)
			rejectedAsks = append(rejectedAsks, &si.RejectedAllocationAsk{
				AllocationKey: req.AllocationKey,
				ApplicationID: req.ApplicationID,
				Reason:        msg,
			})
			continue
		}

		// if app info doesn't exist, reject the request
		appInfo := partitionInfo.getApplication(req.ApplicationID)
		if appInfo == nil {
			msg := fmt.Sprintf("Failed to find application %s, for allocation %s", req.ApplicationID, req.AllocationKey)
			log.Logger().Info(msg)
			rejectedAsks = append(rejectedAsks,
				&si.RejectedAllocationAsk{
					AllocationKey: req.AllocationKey,
					ApplicationID: req.ApplicationID,
					Reason:        msg,
				})
			continue
		}
	}

	// Reject asks returned to RM Proxy for the apps and partitions not found
	if len(rejectedAsks) > 0 {
		m.EventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMRejectedAllocationAskEvent{
			RmID:                   request.RmID,
			RejectedAllocationAsks: rejectedAsks,
		})
	}

	// Send all asks and release allocation requests to scheduler
	m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerAllocationUpdatesEvent{
		NewAsks:    request.Asks,
		ToReleases: request.Releases,
	})
}

func (m *ClusterInfo) processNewSchedulableNodes(request *si.UpdateRequest) {
	acceptedNodes := make([]*si.AcceptedNode, 0)
	rejectedNodes := make([]*si.RejectedNode, 0)
	existingAllocations := make([]*si.Allocation, 0)
	for _, node := range request.NewSchedulableNodes {
		nodeInfo := NewNodeInfo(node)
		partition := m.GetPartition(nodeInfo.Partition)
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
		// create the equivalent scheduling node
		m.EventHandlers.SchedulerEventHandler.HandleEvent(
			&schedulerevent.SchedulerNodeEvent{
				AddedNode: nodeInfo,
			})
		acceptedNodes = append(acceptedNodes, &si.AcceptedNode{NodeID: node.NodeID})
		existingAllocations = append(existingAllocations, node.ExistingAllocations...)
	}

	// inform the RM which nodes have been accepted
	m.EventHandlers.RMProxyEventHandler.HandleEvent(
		&rmevent.RMNodeUpdateEvent{
			RmID:          request.RmID,
			AcceptedNodes: acceptedNodes,
			RejectedNodes: rejectedNodes,
		})

	// notify the scheduler to recover existing allocations (only when provided)
	if len(existingAllocations) > 0 {
		m.EventHandlers.SchedulerEventHandler.HandleEvent(
			&schedulerevent.SchedulerAllocationUpdatesEvent{
				ExistingAllocations: existingAllocations,
				RMId:                request.RmID,
			})
	}
}

// RM may notify us to remove, blacklist or whitelist a node,
// this is to process such actions.
func (m *ClusterInfo) processNodeActions(request *si.UpdateRequest) {
	for _, update := range request.UpdatedNodes {
		var partition *PartitionInfo
		if p, ok := update.Attributes[siCommon.NodePartition]; ok {
			partition = m.GetPartition(p)
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
				m.EventHandlers.SchedulerEventHandler.HandleEvent(
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
				released := partition.RemoveNode(nodeInfo.NodeID)
				// notify the shim allocations have been released from node
				if len(released) != 0 {
					m.notifyRMAllocationReleased(partition.RmID, released, si.AllocationReleaseResponse_STOPPED_BY_RM,
						fmt.Sprintf("Node %s Removed", nodeInfo.NodeID))
				}
				// remove the equivalent scheduling node
				m.EventHandlers.SchedulerEventHandler.HandleEvent(
					&schedulerevent.SchedulerNodeEvent{
						RemovedNode: nodeInfo,
					})
			}
		}
	}
}

// Process the node updates: add and remove nodes as needed.
// Lock free call, all updates occur on the underlying node which is locked or via events.
func (m *ClusterInfo) processNodeUpdate(request *si.UpdateRequest) {
	// Process add node
	if len(request.NewSchedulableNodes) > 0 {
		m.processNewSchedulableNodes(request)
	}

	if len(request.UpdatedNodes) > 0 {
		m.processNodeActions(request)
	}
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
	// 3) Add / remove / update Nodes
	m.processNodeUpdate(request)
}

// Process the RM registration
// Updated partitions can not fail on the scheduler side.
// Locking occurs by the methods that are called, this must be lock free.
func (m *ClusterInfo) processRMRegistrationEvent(event *commonevents.RegisterRMEvent) {
	updatedPartitions, err := SetClusterInfoFromConfigFile(m, event.RMRegistrationRequest.RmID, event.RMRegistrationRequest.PolicyGroup)
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
}

// Process a configuration update.
// The configuration is syntax checked as part of the update of the cluster from the file.
// Updated and deleted partitions can not fail on the scheduler side.
// Locking occurs by the methods that are called, this must be lock free.
func (m *ClusterInfo) processRMConfigUpdateEvent(event *commonevents.ConfigUpdateRMEvent) {
	updatedPartitions, deletedPartitions, err := UpdateClusterInfoFromConfigFile(m, event.RmID)
	if err != nil {
		event.Channel <- &commonevents.Result{Succeeded: false, Reason: err.Error()}
		return
	}
	result := m.sendUpdatedPartitionsToScheduler(updatedPartitions)
	if !result.Succeeded {
		event.Channel <- &commonevents.Result{Succeeded: false, Reason: result.Reason}
		return
	}
	result = m.sendDeletedPartitionsToScheduler(deletedPartitions)
	if !result.Succeeded {
		event.Channel <- &commonevents.Result{Succeeded: false, Reason: result.Reason}
		return
	}

	// all succeed
	event.Channel <- &commonevents.Result{Succeeded: true}
}

func(m *ClusterInfo) sendUpdatedPartitionsToScheduler(updatedPartitions []*PartitionInfo) *commonevents.Result {
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
	return result
}

func(m *ClusterInfo) sendDeletedPartitionsToScheduler(deletedPartitions []*PartitionInfo) *commonevents.Result {
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
	result := <-deletePartitionResult
	return result
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

	// we currently only support 1 allocation in the list, reject all but the first
	if len(event.AllocationProposals) != 1 {
		log.Logger().Info("More than 1 allocation proposal rejected all but first",
			zap.Int("allocPropLength", len(event.AllocationProposals)))
		// Send reject event back to scheduler for all but first
		// this can be more than 1
		m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerAllocationUpdatesEvent{
			RejectedAllocations: event.AllocationProposals[1:],
		})
	}
	// just process the first the rest is rejected already
	proposal := event.AllocationProposals[0]
	partitionInfo := m.GetPartition(proposal.PartitionName)
	allocInfo, err := partitionInfo.addNewAllocation(proposal)
	if err != nil {
		log.Logger().Error("failed to add new allocation to partition",
			zap.String("partition", partitionInfo.Name),
			zap.String("allocationKey", proposal.AllocationKey),
			zap.Error(err))
		// Send reject event back to scheduler
		// this could be more than 1 for the handler however here we can only have 1
		m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerAllocationUpdatesEvent{
			RejectedAllocations: event.AllocationProposals[:1],
		})
		return
	}

	log.Logger().Info("allocation accepted",
		zap.String("appID", proposal.ApplicationID),
		zap.String("queue", proposal.QueueName),
		zap.String("partition", proposal.PartitionName),
		zap.String("allocationKey", proposal.AllocationKey))

	// all is confirmed set the UUID in the proposal to pass it back to the scheduler
	// currently used when an ask is removed while allocation is in flight
	proposal.UUID = allocInfo.AllocationProto.UUID
	// Send accept event back to scheduler
	// this must be only 1: the handler will ignore all others
	m.EventHandlers.SchedulerEventHandler.HandleEvent(&schedulerevent.SchedulerAllocationUpdatesEvent{
		AcceptedAllocations: event.AllocationProposals[:1],
	})
	rmID := common.GetRMIdFromPartitionName(proposal.PartitionName)

	// Send allocation event to RM: rejects are not passed back
	m.EventHandlers.RMProxyEventHandler.HandleEvent(&rmevent.RMNewAllocationsEvent{
		Allocations: []*si.Allocation{allocInfo.AllocationProto},
		RmID:        rmID,
	})
}

// Rejected application from the scheduler.
// Cleanup the app from the partition.
// Lock free call, all updates occur in the partition which is locked.
func (m *ClusterInfo) processRejectedApplicationEvent(event *cacheevent.RejectedNewApplicationEvent) {
	if partition := m.GetPartition(event.PartitionName); partition != nil {
		partition.removeRejectedApp(event.ApplicationID)
	}
}

// Release the preempted resources we have released from the scheduling node
func (m *ClusterInfo) notifySchedNodeAllocReleased(infos []*AllocationInfo, partitionName string) {
	// we should only have 1 node but be safe and handle multiple independent nodes
	nodeRes := make([]schedulerevent.PreemptedNodeResource, len(infos))
	for i, info := range infos {
		nodeRes[i] = schedulerevent.PreemptedNodeResource{
			NodeID:       info.AllocationProto.NodeID,
			Partition:    partitionName,
			PreemptedRes: info.AllocatedResource,
		}
	}
	// pass all the preempted resources to the scheduler
	m.EventHandlers.SchedulerEventHandler.HandleEvent(
		&schedulerevent.SchedulerNodeEvent{
			PreemptedNodeResources: nodeRes,
		})
}

// Create a RM update event to notify RM of released allocations
// Lock free call, all updates occur via events.
func (m *ClusterInfo) notifyRMAllocationReleased(rmID string, released []*AllocationInfo, terminationType si.AllocationReleaseResponse_TerminationType, message string) {
	releaseEvent := &rmevent.RMReleaseAllocationEvent{
		ReleasedAllocations: make([]*si.AllocationReleaseResponse, 0),
		RmID:                rmID,
	}
	for _, alloc := range released {
		releaseEvent.ReleasedAllocations = append(releaseEvent.ReleasedAllocations, &si.AllocationReleaseResponse{
			UUID:            alloc.AllocationProto.UUID,
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
			log.Logger().Info("Failed to find partition for allocation proposal",
				zap.String("partitionName", toReleaseAllocation.PartitionName))
			continue
		}
		rmID := common.GetRMIdFromPartitionName(toReleaseAllocation.PartitionName)
		// release the allocation from the partition
		releasedAllocations := partitionInfo.releaseAllocationsForApplication(toReleaseAllocation)
		if len(releasedAllocations) != 0 {
			// if the resources released were preempted update the scheduling node that it is done
			if toReleaseAllocation.ReleaseType == si.AllocationReleaseResponse_PREEMPTED_BY_SCHEDULER {
				m.notifySchedNodeAllocReleased(releasedAllocations, toReleaseAllocation.PartitionName)
			}
			// whatever was released pass it back to the RM
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
	m.Lock()
	defer m.Unlock()

	toRemove := make(map[string]bool)

	for partition, partitionContext := range m.partitions {
		if partitionContext.RmID == event.RmID {
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
		log.Logger().Info("Failed to find partition for allocation proposal",
			zap.String("partitionName", event.PartitionName))
		return
	}
	_, allocations := partitionInfo.RemoveApplication(event.ApplicationID)
	log.Logger().Info("Removed application from partition",
		zap.String("applicationID", event.ApplicationID),
		zap.String("partitionName", event.PartitionName),
		zap.Int("allocationsRemoved", len(allocations)))

	if len(allocations) > 0 {
		rmID := common.GetRMIdFromPartitionName(event.PartitionName)
		m.notifyRMAllocationReleased(rmID, allocations, si.AllocationReleaseResponse_STOPPED_BY_RM,
			fmt.Sprintf("Application %s Removed", event.ApplicationID))
	}
}

func (m *ClusterInfo) UpdateSchedulerConfig(conf *configs.SchedulerConfig) error {
	rmID := ""
	for _, pi := range m.partitions {
		rmID = pi.RmID
		break
	}
	updatedPartitions, deletedPartitions, err := m.applyConfigChanges(conf, rmID)
	if err != nil {
		return err
	}
	if len(updatedPartitions) > 0 {
		result := m.sendUpdatedPartitionsToScheduler(updatedPartitions)
		if !result.Succeeded {
			return fmt.Errorf(result.Reason)
		}
	}
	if len(deletedPartitions) > 0 {
		result2 := m.sendDeletedPartitionsToScheduler(deletedPartitions)
		if !result2.Succeeded {
			return fmt.Errorf(result2.Reason)
		}
	}
	return nil
}

