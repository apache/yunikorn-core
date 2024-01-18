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

package rmproxy

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/handler"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// Gateway to talk to ResourceManager (behind grpc/API of scheduler-interface)
type RMProxy struct {
	EventHandlers handler.EventHandlers
	stop          chan struct{}

	// Internal fields
	pendingRMEvents chan interface{}

	rmIDToCallback map[string]api.ResourceManagerCallback

	sync.RWMutex
}

func (rmp *RMProxy) GetRMEventHandler() handler.EventHandler {
	return rmp
}

func enqueueAndCheckFull(queue chan interface{}, ev interface{}) {
	select {
	case queue <- ev:
		log.Log(log.RMProxy).Debug("enqueue event",
			zap.Stringer("eventType", reflect.TypeOf(ev)),
			zap.Any("event", ev),
			zap.Int("currentQueueSize", len(queue)))
	default:
		log.Log(log.RMProxy).DPanic("failed to enqueue event",
			zap.Stringer("event", reflect.TypeOf(ev)))
	}
}

func (rmp *RMProxy) HandleEvent(ev interface{}) {
	enqueueAndCheckFull(rmp.pendingRMEvents, ev)
}

func NewRMProxy() *RMProxy {
	rm := &RMProxy{
		rmIDToCallback:  make(map[string]api.ResourceManagerCallback),
		pendingRMEvents: make(chan interface{}, 1024*1024),
		stop:            make(chan struct{}),
	}
	return rm
}

func (rmp *RMProxy) StartService(handlers handler.EventHandlers) {
	rmp.EventHandlers = handlers

	go rmp.handleRMEvents()
}

func (rmp *RMProxy) handleUpdateResponseError(rmID string, err error) {
	log.Log(log.RMProxy).Error("failed to handle response",
		zap.String("rmID", rmID),
		zap.Error(err))
}

func (rmp *RMProxy) processAllocationUpdateEvent(event *rmevent.RMNewAllocationsEvent) {
	allocationsCount := len(event.Allocations)
	if allocationsCount != 0 {
		rmp.RLock()
		defer rmp.RUnlock()
		response := &si.AllocationResponse{
			New: event.Allocations,
		}
		rmp.triggerUpdateAllocation(event.RmID, response)
		metrics.GetSchedulerMetrics().AddAllocatedContainers(len(event.Allocations))
	}
	// Done, notify channel
	event.Channel <- &rmevent.Result{
		Succeeded: true,
		Reason:    "no. of allocations: " + strconv.Itoa(allocationsCount),
	}
}

func (rmp *RMProxy) processApplicationUpdateEvent(event *rmevent.RMApplicationUpdateEvent) {
	rmp.RLock()
	defer rmp.RUnlock()
	if len(event.RejectedApplications) == 0 && len(event.AcceptedApplications) == 0 && len(event.UpdatedApplications) == 0 {
		return
	}
	response := &si.ApplicationResponse{
		Rejected: event.RejectedApplications,
		Accepted: event.AcceptedApplications,
		Updated:  event.UpdatedApplications,
	}
	if callback := rmp.rmIDToCallback[event.RmID]; callback != nil {
		if err := callback.UpdateApplication(response); err != nil {
			rmp.handleUpdateResponseError(event.RmID, err)
		}
	} else {
		log.Log(log.RMProxy).DPanic("RM is not registered",
			zap.String("rmID", event.RmID))
	}

	// update app metrics
	if len(event.RejectedApplications) > 0 {
		metrics.GetSchedulerMetrics().AddTotalApplicationsRejected(len(event.RejectedApplications))
	}
	if len(event.AcceptedApplications) > 0 {
		metrics.GetSchedulerMetrics().AddTotalApplicationsAccepted(len(event.AcceptedApplications))
	}
}

func (rmp *RMProxy) processRMReleaseAllocationEvent(event *rmevent.RMReleaseAllocationEvent) {
	allocationsCount := len(event.ReleasedAllocations)
	if allocationsCount != 0 {
		rmp.RLock()
		defer rmp.RUnlock()
		response := &si.AllocationResponse{
			Released: event.ReleasedAllocations,
		}
		rmp.triggerUpdateAllocation(event.RmID, response)
		metrics.GetSchedulerMetrics().AddReleasedContainers(len(event.ReleasedAllocations))
	}

	// Done, notify channel
	event.Channel <- &rmevent.Result{
		Succeeded: true,
		Reason:    "no. of allocations: " + strconv.Itoa(allocationsCount),
	}
}

func (rmp *RMProxy) triggerUpdateAllocation(rmID string, response *si.AllocationResponse) {
	if callback := rmp.rmIDToCallback[rmID]; callback != nil {
		if err := callback.UpdateAllocation(response); err != nil {
			rmp.handleUpdateResponseError(rmID, err)
		}
	} else {
		log.Log(log.RMProxy).DPanic("RM is not registered",
			zap.String("rmID", rmID))
	}
}

func (rmp *RMProxy) processRMReleaseAllocationAskEvent(event *rmevent.RMReleaseAllocationAskEvent) {
	rmp.RLock()
	defer rmp.RUnlock()
	if len(event.ReleasedAllocationAsks) == 0 {
		return
	}
	response := &si.AllocationResponse{
		ReleasedAsks: event.ReleasedAllocationAsks,
	}
	rmp.triggerUpdateAllocation(event.RmID, response)
}

func (rmp *RMProxy) processRMRejectedAllocationAskEvent(event *rmevent.RMRejectedAllocationAskEvent) {
	rmp.RLock()
	defer rmp.RUnlock()
	if len(event.RejectedAllocationAsks) == 0 {
		return
	}
	response := &si.AllocationResponse{
		Rejected: event.RejectedAllocationAsks,
	}
	rmp.triggerUpdateAllocation(event.RmID, response)
	metrics.GetSchedulerMetrics().AddRejectedContainers(len(event.RejectedAllocationAsks))
}

func (rmp *RMProxy) processRMRejectedAllocationEvent(event *rmevent.RMRejectedAllocationEvent) {
	rmp.RLock()
	defer rmp.RUnlock()
	if len(event.RejectedAllocations) == 0 {
		return
	}
	response := &si.AllocationResponse{
		RejectedAllocations: event.RejectedAllocations,
	}
	rmp.triggerUpdateAllocation(event.RmID, response)
	metrics.GetSchedulerMetrics().AddRejectedContainers(len(event.RejectedAllocations))
}

func (rmp *RMProxy) processRMNodeUpdateEvent(event *rmevent.RMNodeUpdateEvent) {
	rmp.RLock()
	defer rmp.RUnlock()
	if len(event.RejectedNodes) == 0 && len(event.AcceptedNodes) == 0 {
		return
	}
	response := &si.NodeResponse{
		Rejected: event.RejectedNodes,
		Accepted: event.AcceptedNodes,
	}

	if callback := rmp.rmIDToCallback[event.RmID]; callback != nil {
		if err := callback.UpdateNode(response); err != nil {
			rmp.handleUpdateResponseError(event.RmID, err)
		}
	} else {
		log.Log(log.RMProxy).DPanic("RM is not registered",
			zap.String("rmID", event.RmID))
	}
}

func (rmp *RMProxy) handleRMEvents() {
	for {
		select {
		case ev := <-rmp.pendingRMEvents:
			switch v := ev.(type) {
			case *rmevent.RMNewAllocationsEvent:
				rmp.processAllocationUpdateEvent(v)
			case *rmevent.RMApplicationUpdateEvent:
				rmp.processApplicationUpdateEvent(v)
			case *rmevent.RMReleaseAllocationEvent:
				rmp.processRMReleaseAllocationEvent(v)
			case *rmevent.RMRejectedAllocationAskEvent:
				rmp.processRMRejectedAllocationAskEvent(v)
			case *rmevent.RMRejectedAllocationEvent:
				rmp.processRMRejectedAllocationEvent(v)
			case *rmevent.RMNodeUpdateEvent:
				rmp.processRMNodeUpdateEvent(v)
			case *rmevent.RMReleaseAllocationAskEvent:
				rmp.processRMReleaseAllocationAskEvent(v)
			default:
				panic(fmt.Sprintf("%s is not an acceptable type for RM event.", reflect.TypeOf(v).String()))
			}
		case <-rmp.stop:
			return
		}
	}
}

func (rmp *RMProxy) RegisterResourceManager(request *si.RegisterResourceManagerRequest, callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error) {
	rmp.Lock()
	defer rmp.Unlock()
	c := make(chan *rmevent.Result)

	// If this is a re-register we need to clean up first
	if rmp.rmIDToCallback[request.RmID] != nil {
		go func() {
			rmp.EventHandlers.SchedulerEventHandler.HandleEvent(
				&rmevent.RMPartitionsRemoveEvent{
					RmID:    request.RmID,
					Channel: c,
				})
		}()

		result := <-c
		close(c)
		if !result.Succeeded {
			return nil, fmt.Errorf("registration of RM failed: %v", result.Reason)
		}
	}

	c = make(chan *rmevent.Result)

	// Add new RM.
	go func() {
		rmp.EventHandlers.SchedulerEventHandler.HandleEvent(
			&rmevent.RMRegistrationEvent{
				Registration: request,
				Channel:      c,
			})
	}()

	// Wait from channel
	result := <-c
	if result.Succeeded {
		rmp.rmIDToCallback[request.RmID] = callback

		// RM callback can optionally implement one or more scheduler plugin interfaces,
		// register scheduler plugin if the callback implements any plugin interface
		plugins.RegisterSchedulerPlugin(callback)

		return &si.RegisterResourceManagerResponse{}, nil
	}
	return nil, fmt.Errorf("registration of RM failed: %v", result.Reason)
}

func (rmp *RMProxy) GetResourceManagerCallback(rmID string) api.ResourceManagerCallback {
	rmp.RLock()
	defer rmp.RUnlock()

	return rmp.rmIDToCallback[rmID]
}

func (rmp *RMProxy) UpdateAllocation(request *si.AllocationRequest) error {
	if rmp.GetResourceManagerCallback(request.RmID) == nil {
		return fmt.Errorf("received AllocationRequest, but RmID=\"%s\" not registered", request.RmID)
	}
	go func() {
		// Update allocations
		if len(request.Allocations) > 0 {
			for _, alloc := range request.Allocations {
				alloc.PartitionName = common.GetNormalizedPartitionName(alloc.PartitionName, request.RmID)
			}
		}

		// Update asks
		if len(request.Asks) > 0 {
			for _, ask := range request.Asks {
				ask.PartitionName = common.GetNormalizedPartitionName(ask.PartitionName, request.RmID)
			}
		}

		// Update releases
		if request.Releases != nil {
			if len(request.Releases.AllocationsToRelease) > 0 {
				for _, rel := range request.Releases.AllocationsToRelease {
					rel.PartitionName = common.GetNormalizedPartitionName(rel.PartitionName, request.RmID)
				}
			}

			if len(request.Releases.AllocationAsksToRelease) > 0 {
				for _, rel := range request.Releases.AllocationAsksToRelease {
					rel.PartitionName = common.GetNormalizedPartitionName(rel.PartitionName, request.RmID)
				}
			}
		}
		rmp.EventHandlers.SchedulerEventHandler.HandleEvent(&rmevent.RMUpdateAllocationEvent{Request: request})
	}()
	return nil
}

func (rmp *RMProxy) UpdateApplication(request *si.ApplicationRequest) error {
	if rmp.GetResourceManagerCallback(request.RmID) == nil {
		return fmt.Errorf("received ApplicationRequest, but RmID=\"%s\" not registered", request.RmID)
	}

	go func() {
		// Update New apps
		if len(request.New) > 0 {
			for _, app := range request.New {
				app.PartitionName = common.GetNormalizedPartitionName(app.PartitionName, request.RmID)
			}
		}
		// Update Remove apps
		if len(request.Remove) > 0 {
			for _, app := range request.Remove {
				app.PartitionName = common.GetNormalizedPartitionName(app.PartitionName, request.RmID)
			}
		}
		rmp.EventHandlers.SchedulerEventHandler.HandleEvent(&rmevent.RMUpdateApplicationEvent{Request: request})
	}()
	return nil
}

func (rmp *RMProxy) UpdateNode(request *si.NodeRequest) error {
	if rmp.GetResourceManagerCallback(request.RmID) == nil {
		return fmt.Errorf("received NodeRequest, but RmID=\"%s\" not registered", request.RmID)
	}

	if len(request.Nodes) > 0 {
		for _, node := range request.Nodes {
			if len(node.GetAttributes()) == 0 {
				node.Attributes = map[string]string{}
			}
			partition := node.Attributes[siCommon.NodePartition]
			node.Attributes[siCommon.NodePartition] = common.GetNormalizedPartitionName(partition, request.RmID)
		}
	}
	rmp.EventHandlers.SchedulerEventHandler.HandleEvent(&rmevent.RMUpdateNodeEvent{Request: request})

	return nil
}

// Triggers scheduler to reload configuration and apply the changes on-the-fly to the scheduler itself.
func (rmp *RMProxy) UpdateConfiguration(request *si.UpdateConfigurationRequest) error {
	rmp.RLock()
	defer rmp.RUnlock()

	c := make(chan *rmevent.Result)
	go func() {
		rmp.EventHandlers.SchedulerEventHandler.HandleEvent(&rmevent.RMConfigUpdateEvent{
			RmID:        request.RmID,
			PolicyGroup: request.PolicyGroup,
			Config:      request.Config,
			ExtraConfig: request.ExtraConfig,
			Channel:     c,
		})
	}()

	// Wait from channel
	result := <-c
	if !result.Succeeded {
		return fmt.Errorf("update of configuration failed: %v", result.Reason)
	}
	return nil
}

func (rmp *RMProxy) Stop() {
	log.Log(log.RMProxy).Info("Stopping RMProxy")
	close(rmp.stop)
}
