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
	"github.com/cloudera/yunikorn-core/pkg/common/resources"
	"github.com/cloudera/yunikorn-core/pkg/log"
	"github.com/cloudera/yunikorn-core/pkg/plugins"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
	"go.uber.org/zap"
	"sync"
)

type SchedulingNode struct {
	NodeId   string

	// Private info
	nodeInfo                  *cache.NodeInfo
	allocatingResource        *resources.Resource // resources being allocated
	preemptingResource        *resources.Resource // resources considered for preemption
	cachedAvailable           *resources.Resource // calculated available resources
	needUpdateCachedAvailable bool                // is the calculated available resource up to date?

	// Reservation requests
	// Key is combination of applicationId and requestKey to make it unique
	// Why multiple reservation supported? We can leverage this to do multi-preemption on one node.
	// Or pre-ordering node for autoscaling nodes.
	reservationRequests map[string]*ReservedSchedulingRequest

	// Total reserved resource
	totalReservedResource *resources.Resource

	lock sync.RWMutex
}

func NewSchedulingNode(info *cache.NodeInfo) *SchedulingNode {
	// safe guard against panic
	if info == nil {
		return nil
	}
	return &SchedulingNode{
		nodeInfo:                  info,
		NodeId:                    info.NodeId,
		allocatingResource:        resources.NewResource(),
		preemptingResource:        resources.NewResource(),
		needUpdateCachedAvailable: true,
		reservationRequests: make(map[string]*ReservedSchedulingRequest, 0),
		totalReservedResource:     resources.NewResource(),
	}
}

// Get the allocated resource on this node.
// These resources are just the confirmed allocations (tracked in the cache node).
// This does not lock the cache node as it will take its own lock.
func (sn *SchedulingNode) GetAllocatedResource() *resources.Resource {
	return sn.nodeInfo.GetAllocatedResource()
}

// Get the available resource on this node.
// These resources are confirmed allocations (tracked in the cache node) minus the resources
// currently being allocated but not confirmed in the cache.
// This does not lock the cache node as it will take its own lock.
func (sn *SchedulingNode) getAvailableResource() *resources.Resource {
    sn.lock.Lock()
    defer sn.lock.Unlock()

	if sn.needUpdateCachedAvailable {
		sn.cachedAvailable = sn.nodeInfo.GetAvailableResource()
		sn.cachedAvailable.SubFrom(sn.allocatingResource)
		sn.needUpdateCachedAvailable = false
	}
	return sn.cachedAvailable
}

// Get the resource tagged for allocation on this node.
// These resources are part of unconfirmed allocations.
func (sn *SchedulingNode) getAllocatingResource() *resources.Resource {
	sn.lock.RLock()
	defer sn.lock.RUnlock()

	return sn.allocatingResource
}

// Update the number of resource proposed for allocation on this node
func (sn *SchedulingNode) incAllocatingResource(proposed *resources.Resource) {
	sn.lock.Lock()
	defer sn.lock.Unlock()

	sn.needUpdateCachedAvailable = true
	sn.allocatingResource.AddTo(proposed)
}

// Handle the allocation processing on the scheduler when the cache node is updated.
// This will be invoked when allocation is either rejected or accepted by cache. It
// will decrease allocating resource, and make sure refresh cache at the next round.
func (sn *SchedulingNode) decreaseAllocatingResource(confirmed *resources.Resource) {
	sn.lock.Lock()
	defer sn.lock.Unlock()
	log.Logger().Debug("allocations in progress increased",
		zap.String("nodeId", sn.NodeId),
		zap.Any("confirmed", confirmed))

	sn.needUpdateCachedAvailable = true
	sn.allocatingResource.SubFrom(confirmed)
}

// Get the number of resource tagged for preemption on this node
func (sn *SchedulingNode) getPreemptingResource() *resources.Resource {
	sn.lock.RLock()
	defer sn.lock.RUnlock()

	return sn.preemptingResource
}

// Update the number of resource tagged for preemption on this node
func (sn *SchedulingNode) incPreemptingResource(preempting *resources.Resource) {
	sn.lock.Lock()
	defer sn.lock.Unlock()

    sn.preemptingResource.AddTo(preempting)
}

func (sn *SchedulingNode) handlePreemptionUpdate(preempted *resources.Resource) {
	sn.lock.Lock()
	defer sn.lock.Unlock()
	log.Logger().Debug("preempted resources released",
		zap.String("nodeId", sn.NodeId),
		zap.Any("preempted", preempted))

	sn.preemptingResource.SubFrom(preempted)
}

// Calculate gap score of fit in, 0 means smaller can be fitin in the larger resource.
// fit_in_gap_score = sigma((smaller_i- larger_i)/smaller_i) (i is index of resource type, and smaller_i > larger_i)
func fitInGapScore(larger, smaller *resources.Resource) float64 {
	score := resources.Quantity(0)
	for k, v := range smaller.Resources {
		if v <= 0 {
			// 0 or negative v can fit at anywhere
			continue
		}

		if larger == nil {
			score += 1
		}
		largerV := larger.Resources[k]
		if largerV < 0 {
			largerV = 0
		}

		if v > largerV {
			score += (v - largerV) / v
		}
	}
	return float64(score)
}

// Check and update allocating resources of the scheduling node.
// If the proposed allocation fits in the available resources, taking into account resources marked for
// preemption if applicable, the allocating resources are updated and true is returned.
// If the proposed allocation does not fit false is returned and no changes are made.
// return true when resource can be allocated.
// return false, and a score of how close it is
func (sn *SchedulingNode) CheckAndAllocateResource(delta *resources.Resource, preemptionPhase bool) (bool, float64) {
	sn.lock.Lock()
	defer sn.lock.Unlock()

	available := sn.nodeInfo.GetAvailableResource()
	newAllocating := resources.Add(delta, sn.allocatingResource)

	if preemptionPhase {
        available.AddTo(sn.preemptingResource)
    }
	score := fitInGapScore(available, newAllocating)
    if score == 0 {
        log.Logger().Debug("allocations in progress updated",
			zap.String("nodeId", sn.NodeId),
			zap.Any("total unconfirmed", newAllocating))
		sn.needUpdateCachedAvailable = true
		sn.allocatingResource = newAllocating
		return true, 0
	} else {
		return false, score
	}
}

func (sn* SchedulingNode) UnreserveOnNode(reservationRequest *ReservedSchedulingRequest) bool {
	sn.lock.Lock()
	defer sn.lock.Unlock()

	requestAllocKey := reservationRequest.SchedulingAsk.AskProto.AllocationKey

	if val, ok := sn.reservationRequests[requestAllocKey]; ok {
		_, decSucceeded := val.DecAmount()

		if val.GetAmount() <= 0 {
			delete(sn.reservationRequests, requestAllocKey)
		}

		if decSucceeded {
			sn.totalReservedResource = resources.Sub(sn.totalReservedResource, reservationRequest.SchedulingAsk.AllocatedResource)
			return true
		}
	}

	return false
}

func (sn* SchedulingNode) ReserveOnNode(reservationRequest *ReservedSchedulingRequest) (bool, error) {
	requestAllocKey := reservationRequest.SchedulingAsk.AskProto.AllocationKey
	nodeId := reservationRequest.SchedulingNode.NodeId

	if reservationRequest.GetAmount() != 1 {
		return false, fmt.Errorf("Only allow reserve one allocation at a time allocationKey=%s; node=%s", requestAllocKey, nodeId)
	}

	sn.lock.Lock()
	defer sn.lock.Unlock()

	// TODO: For now, only allow one reservation at a time, however once we have preemption + reservation, or autoscaling inferno node,
	// we can have this relaxed
	if len(sn.reservationRequests) > 0 {
		return false, fmt.Errorf("Failed to reserve request key=%s on node=%s since node is already reserved", requestAllocKey, nodeId)
	}

	key := reservationRequest.GetReservationRequestKey()
	val, exist := sn.reservationRequests[key]

	if !exist {
		reservationRequest := reservationRequest.Clone()
		sn.reservationRequests[key] = reservationRequest
	} else {
		val.IncAmount(reservationRequest.GetAmount())
	}

	sn.totalReservedResource = resources.Add(sn.totalReservedResource, reservationRequest.SchedulingAsk.AllocatedResource)

	return true, nil
}

// Checking pre allocation conditions. The pre-allocation conditions are implemented via plugins
// in the shim. If no plugins are implemented then the check will return true. If multiple plugins
// are implemented the first failure will stop the checks.
// The caller must thus not rely on all plugins being executed.
// This is a lock free call as it does not change the node and multiple predicate checks could be
// run at the same time.
func (sn *SchedulingNode) CheckAllocateConditions(allocId string, reservation bool) bool {
	if !sn.nodeInfo.IsSchedulable() {
		log.Logger().Debug("node is unschedulable",
			zap.String("nodeId", sn.NodeId))
		return false
	}

	// Only check this for regular allocation (not reservation)
	if reservation && len(sn.reservationRequests) > 0 {
		log.Logger().Debug("node is already reserved",
			zap.String("nodeId", sn.NodeId))
		return false
	}

	// Check the predicates plugin (k8shim)
	if plugin := plugins.GetPredicatesPlugin(); plugin != nil {
		log.Logger().Debug("predicates",
			zap.String("allocationId", allocId),
			zap.String("nodeId", sn.NodeId))
		if err := plugin.Predicates(&si.PredicatesArgs{
			AllocationKey: allocId,
			NodeId:        sn.NodeId,
		}); err != nil {
			log.Logger().Debug("running predicates failed",
				zap.String("allocationId", allocId),
				zap.String("nodeId", sn.NodeId),
				zap.Error(err))
			return false
		}
	}
	// must be last return in the list
	return true
}

// Make sure update cached available at the next round
func (sn *SchedulingNode) SetNeedUpdateCachedAvailable() {
	sn.lock.Lock()
	defer sn.lock.Unlock()
	sn.needUpdateCachedAvailable = true
}