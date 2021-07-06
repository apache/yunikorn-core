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
	"sync"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/handler"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/api"
	siCommon "github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// Gateway to talk to ResourceManager (behind grpc/API of scheduler-interface)
type RMProxy struct {
	EventHandlers handler.EventHandlers

	// Internal fields
	pendingRMEvents chan interface{}

	rmIDToCallback map[string]api.ResourceManagerCallback

	// config version is tracked per RM,
	// it is used to determine if configs need to be reloaded
	rmIDToConfigWatcher map[string]*configs.ConfigWatcher

	sync.RWMutex
}

func (rmp *RMProxy) GetRMEventHandler() handler.EventHandler {
	return rmp
}

func enqueueAndCheckFull(queue chan interface{}, ev interface{}) {
	select {
	case queue <- ev:
		log.Logger().Debug("enqueue event",
			zap.Any("event", ev),
			zap.Int("currentQueueSize", len(queue)))
	default:
		log.Logger().Panic("failed to enqueue event",
			zap.String("event", reflect.TypeOf(ev).String()))
	}
}

func (rmp *RMProxy) HandleEvent(ev interface{}) {
	enqueueAndCheckFull(rmp.pendingRMEvents, ev)
}

func NewRMProxy() *RMProxy {
	rm := &RMProxy{
		rmIDToCallback:      make(map[string]api.ResourceManagerCallback),
		rmIDToConfigWatcher: make(map[string]*configs.ConfigWatcher),
		pendingRMEvents:     make(chan interface{}, 1024*1024),
	}
	return rm
}

func (rmp *RMProxy) StartService(handlers handler.EventHandlers) {
	rmp.EventHandlers = handlers

	go rmp.handleRMEvents()
}

func (rmp *RMProxy) handleRMRecvUpdateResponseError(rmID string, err error) {
	log.Logger().Error("failed to handle response",
		zap.String("rmID", rmID),
		zap.Error(err))
}

func (rmp *RMProxy) processUpdateResponse(rmID string, response *si.UpdateResponse) {
	rmp.RLock()
	defer rmp.RUnlock()

	if callback := rmp.rmIDToCallback[rmID]; callback != nil {
		if err := callback.RecvUpdateResponse(response); err != nil {
			rmp.handleRMRecvUpdateResponseError(rmID, err)
		}
	} else {
		log.Logger().DPanic("RM is not registered",
			zap.String("rmID", rmID))
	}
}

func (rmp *RMProxy) processAllocationUpdateEvent(event *rmevent.RMNewAllocationsEvent) {
	if len(event.Allocations) == 0 {
		return
	}
	response := &si.UpdateResponse{
		NewAllocations: event.Allocations,
	}

	rmp.processUpdateResponse(event.RmID, response)
	metrics.GetSchedulerMetrics().AddAllocatedContainers(len(event.Allocations))
}

func (rmp *RMProxy) processApplicationUpdateEvent(event *rmevent.RMApplicationUpdateEvent) {
	if len(event.RejectedApplications) == 0 && len(event.AcceptedApplications) == 0 && len(event.UpdatedApplications) == 0 {
		return
	}
	response := &si.UpdateResponse{
		RejectedApplications: event.RejectedApplications,
		AcceptedApplications: event.AcceptedApplications,
		UpdatedApplications:  event.UpdatedApplications,
	}

	rmp.processUpdateResponse(event.RmID, response)

	// update app metrics
	if len(event.RejectedApplications) > 0 {
		metrics.GetSchedulerMetrics().AddTotalApplicationsRejected(len(event.RejectedApplications))
	}
	if len(event.AcceptedApplications) > 0 {
		metrics.GetSchedulerMetrics().AddTotalApplicationsAccepted(len(event.AcceptedApplications))
	}
}

func (rmp *RMProxy) processRMReleaseAllocationEvent(event *rmevent.RMReleaseAllocationEvent) {
	if len(event.ReleasedAllocations) == 0 {
		return
	}
	response := &si.UpdateResponse{
		ReleasedAllocations: event.ReleasedAllocations,
	}

	rmp.processUpdateResponse(event.RmID, response)
	metrics.GetSchedulerMetrics().AddReleasedContainers(len(event.ReleasedAllocations))
}

func (rmp *RMProxy) processRMReleaseAllocationAskEvent(event *rmevent.RMReleaseAllocationAskEvent) {
	if len(event.ReleasedAllocationAsks) == 0 {
		return
	}
	response := &si.UpdateResponse{
		ReleasedAllocationAsks: event.ReleasedAllocationAsks,
	}
	rmp.processUpdateResponse(event.RmID, response)
}

func (rmp *RMProxy) processUpdatePartitionConfigsEvent(event *rmevent.RMRejectedAllocationAskEvent) {
	if len(event.RejectedAllocationAsks) == 0 {
		return
	}
	response := &si.UpdateResponse{
		RejectedAllocations: event.RejectedAllocationAsks,
	}

	rmp.processUpdateResponse(event.RmID, response)
	metrics.GetSchedulerMetrics().AddRejectedContainers(len(event.RejectedAllocationAsks))
}

func (rmp *RMProxy) processRMNodeUpdateEvent(event *rmevent.RMNodeUpdateEvent) {
	if len(event.RejectedNodes) == 0 && len(event.AcceptedNodes) == 0 {
		return
	}
	response := &si.UpdateResponse{
		RejectedNodes: event.RejectedNodes,
		AcceptedNodes: event.AcceptedNodes,
	}

	rmp.processUpdateResponse(event.RmID, response)
}

func (rmp *RMProxy) handleRMEvents() {
	for {
		ev := <-rmp.pendingRMEvents
		switch v := ev.(type) {
		case *rmevent.RMNewAllocationsEvent:
			rmp.processAllocationUpdateEvent(v)
		case *rmevent.RMApplicationUpdateEvent:
			rmp.processApplicationUpdateEvent(v)
		case *rmevent.RMReleaseAllocationEvent:
			rmp.processRMReleaseAllocationEvent(v)
		case *rmevent.RMRejectedAllocationAskEvent:
			rmp.processUpdatePartitionConfigsEvent(v)
		case *rmevent.RMNodeUpdateEvent:
			rmp.processRMNodeUpdateEvent(v)
		case *rmevent.RMReleaseAllocationAskEvent:
			rmp.processRMReleaseAllocationAskEvent(v)
		default:
			panic(fmt.Sprintf("%s is not an acceptable type for RM event.", reflect.TypeOf(v).String()))
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
		// create a config watcher for this RM
		// config watcher will only be started when a reload is triggered
		// it is configured with a expiration time, and will be auto exit once that reaches
		configWatcher := configs.CreateConfigWatcher(request.RmID, request.PolicyGroup, configs.DefaultConfigWatcherDuration)
		configWatcher.RegisterCallback(&configurationReloader{
			rmID:    request.RmID,
			rmProxy: rmp,
		})
		rmp.rmIDToConfigWatcher[request.RmID] = configWatcher
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

// Do normalize.
func normalizeUpdateRequestByRMId(request *si.UpdateRequest) {
	// Update asks
	if len(request.Asks) > 0 {
		for _, ask := range request.Asks {
			ask.PartitionName = common.GetNormalizedPartitionName(ask.PartitionName, request.RmID)
		}
	}

	// Update Schedulable Nodes
	if len(request.NewSchedulableNodes) > 0 {
		for _, node := range request.NewSchedulableNodes {
			partition := node.Attributes[siCommon.NodePartition]
			node.Attributes[siCommon.NodePartition] = common.GetNormalizedPartitionName(partition, request.RmID)
		}
	}

	// Update New apps
	if len(request.NewApplications) > 0 {
		for _, app := range request.NewApplications {
			app.PartitionName = common.GetNormalizedPartitionName(app.PartitionName, request.RmID)
		}
	}

	// Update Updated nodes
	if len(request.UpdatedNodes) > 0 {
		for _, node := range request.UpdatedNodes {
			partition := node.Attributes[siCommon.NodePartition]
			node.Attributes[siCommon.NodePartition] = common.GetNormalizedPartitionName(partition, request.RmID)
		}
	}

	// Update Remove apps
	if len(request.RemoveApplications) > 0 {
		for _, app := range request.RemoveApplications {
			app.PartitionName = common.GetNormalizedPartitionName(app.PartitionName, request.RmID)
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
}

func (rmp *RMProxy) Update(request *si.UpdateRequest) error {
	if rmp.GetResourceManagerCallback(request.RmID) == nil {
		return fmt.Errorf("received UpdateRequest, but RmID=\"%s\" not registered", request.RmID)
	}

	go func() {
		normalizeUpdateRequestByRMId(request)
		rmp.EventHandlers.SchedulerEventHandler.HandleEvent(&rmevent.RMUpdateRequestEvent{Request: request})
	}()

	return nil
}

// Triggers scheduler to reload configuration and apply the changes on-the-fly to the scheduler itself.
func (rmp *RMProxy) ReloadConfiguration(rmID string) error {
	rmp.RLock()
	defer rmp.RUnlock()

	cw, ok := rmp.rmIDToConfigWatcher[rmID]
	if !ok {
		// if configWatcher is not found for this RM
		return fmt.Errorf("failed to reload configuration, because RM %s is unknown to the scheduler", rmID)
	}
	// ensure configWatcher is running
	// configWatcher is only triggered to run when the reload is called,
	// it might be stopped when reload is done or expires, so it needs to
	// be re-triggered when there is new reload call coming. This is a
	// noop if the config watcher is already running.
	cw.Run()

	return nil
}

// actual configuration reloader
type configurationReloader struct {
	rmID    string
	rmProxy *RMProxy
}

func (cr configurationReloader) DoReloadConfiguration() error {
	c := make(chan *rmevent.Result)
	cr.rmProxy.EventHandlers.SchedulerEventHandler.HandleEvent(
		&rmevent.RMConfigUpdateEvent{
			RmID:    cr.rmID,
			Channel: c,
		})
	result := <-c
	if !result.Succeeded {
		return fmt.Errorf("failed to update configuration for RM %s, result: %v", cr.rmID, result)
	}
	return nil
}
