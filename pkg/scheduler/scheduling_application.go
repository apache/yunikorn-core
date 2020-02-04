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

package scheduler

import (
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

type SchedulingApplication struct {
	ApplicationInfo *cache.ApplicationInfo

	// Private fields need protection
	queue          *SchedulingQueue                    // queue the application is running in
	allocating     *resources.Resource                 // allocating resource set by the scheduler
	pending        *resources.Resource                 // pending resources from asks for the app
	reservations   map[string]*reservation             // a map of reservations
	requests       map[string]*schedulingAllocationAsk // a map of asks
	sortedRequests []*schedulingAllocationAsk

	sync.RWMutex
}

func newSchedulingApplication(appInfo *cache.ApplicationInfo) *SchedulingApplication {
	return &SchedulingApplication{
		ApplicationInfo: appInfo,
		allocating:      resources.NewResource(),
		pending:         resources.NewResource(),
		requests:        make(map[string]*schedulingAllocationAsk),
		reservations:    make(map[string]*reservation),
	}
}

func (sa *SchedulingApplication) GetSchedulingAllocationAsk(allocationKey string) *schedulingAllocationAsk {
	sa.RLock()
	defer sa.RUnlock()
	return sa.requests[allocationKey]
}

// Return the pending resources for this application
func (sa *SchedulingApplication) GetPendingResource() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return sa.pending
}

// Return the allocating and allocated resources for this application
func (sa *SchedulingApplication) getUnconfirmedAllocated() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return resources.Add(sa.allocating, sa.ApplicationInfo.GetAllocatedResource())
}

func (sa *SchedulingApplication) getAllocatingResource() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return sa.allocating
}

// Increment allocating resource for the app
func (sa *SchedulingApplication) incAllocating(delta *resources.Resource) {
	sa.Lock()
	defer sa.Unlock()
	sa.allocating.AddTo(delta)
}

// Decrement allocating resource for the app
func (sa *SchedulingApplication) decAllocating(delta *resources.Resource) {
	sa.Lock()
	defer sa.Unlock()
	var err error
	sa.allocating, err = resources.SubErrorNegative(sa.allocating, delta)
	if err != nil {
		log.Logger().Warn("Allocating resources went negative",
			zap.Error(err))
	}
}

// Remove one or more allocation asks from this application
func (sa *SchedulingApplication) removeAllocationAsk(allocKey string) *resources.Resource {
	sa.Lock()
	defer sa.Unlock()
	// shortcut no need to do anything
	if len(sa.requests) == 0 {
		return nil
	}
	var deltaPendingResource *resources.Resource = nil
	// when allocation key not specified, cleanup all allocation ask
	if allocKey == "" {
		// Cleanup total pending resource
		deltaPendingResource = resources.Multiply(sa.pending, -1)
		sa.pending = resources.NewResource()
		sa.requests = make(map[string]*schedulingAllocationAsk)
	} else if ask := sa.requests[allocKey]; ask != nil {
		deltaPendingResource = resources.MultiplyBy(ask.AllocatedResource, -float64(ask.getPendingAskRepeat()))
		sa.pending.AddTo(deltaPendingResource)
		delete(sa.requests, allocKey)
	}

	return deltaPendingResource
}

// Add an allocation ask to this application
// If the ask already exist update the existing info
func (sa *SchedulingApplication) addAllocationAsk(ask *schedulingAllocationAsk) (*resources.Resource, error) {
	sa.Lock()
	defer sa.Unlock()
	if ask == nil {
		return nil, fmt.Errorf("ask cannot be nil when added to app %s", sa.ApplicationInfo.ApplicationID)
	}
	if ask.getPendingAskRepeat() == 0 || resources.IsZero(ask.AllocatedResource) {
		return nil, fmt.Errorf("invalid ask added to app %s: %v", sa.ApplicationInfo.ApplicationID, ask)
	}
	delta := resources.Multiply(ask.AllocatedResource, int64(ask.getPendingAskRepeat()))

	var oldAskResource *resources.Resource = nil
	if oldAsk := sa.requests[ask.AskProto.AllocationKey]; oldAsk != nil {
		oldAskResource = resources.Multiply(oldAsk.AllocatedResource, int64(oldAsk.getPendingAskRepeat()))
	}

	delta.SubFrom(oldAskResource)
	sa.requests[ask.AskProto.AllocationKey] = ask

	// Update total pending resource
	sa.pending.AddTo(delta)

	return delta, nil
}

func (sa *SchedulingApplication) updateAllocationAskRepeat(allocKey string, delta int32) (*resources.Resource, error) {
	sa.Lock()
	defer sa.Unlock()
	if ask := sa.requests[allocKey]; ask != nil {
		// updating with delta does error checking internally
		if !ask.addPendingAskRepeat(delta) {
			return nil, fmt.Errorf("ask repaeat not updated resulting repeat less than zero for ask %s on app %s", allocKey, sa.ApplicationInfo.ApplicationID)
		}

		deltaPendingResource := resources.Multiply(ask.AllocatedResource, int64(delta))
		sa.pending.AddTo(deltaPendingResource)

		return deltaPendingResource, nil
	}
	return nil, fmt.Errorf("failed to locate ask with key %s", allocKey)
}

// Return if the application has any reservations.
func (sa *SchedulingApplication) hasReserved() bool {
	sa.RLock()
	defer sa.RUnlock()
	return len(sa.reservations) > 0
}

// Return if the application has the node reserved.
// An empty nodeID is never reserved.
func (sa *SchedulingApplication) isReservedOnNode(nodeID string) bool {
	if nodeID == "" {
		return false
	}
	sa.RLock()
	defer sa.RUnlock()
	for key := range sa.reservations {
		if strings.HasPrefix(key, nodeID) {
			return true
		}
	}
	return false
}

// Reserve the application for this node and ask combination.
// If the reservation fails the function returns false, if the reservation is made it returns true.
// If the node and ask combination was already reserved for the application this is a noop and returns true.
func (sa *SchedulingApplication) reserve(node *schedulingNode, ask *schedulingAllocationAsk) (bool, error) {
	sa.Lock()
	defer sa.Unlock()
	// create the reservation (includes nil checks)
	nodeReservation := newReservation(node, nil, ask)
	if nodeReservation == nil {
		log.Logger().Debug("reservation creation failed unexpectedly",
			zap.String("app", sa.ApplicationInfo.ApplicationID),
			zap.Any("node", node),
			zap.Any("ask", ask))
		return false, fmt.Errorf("reservation creation failed node or ask are nil on appID %s", sa.ApplicationInfo.ApplicationID)
	}
	if !sa.canAskReserve(ask) {
		return false, fmt.Errorf("reservation of ask exceeds pending repeat, pending ask repeat %d", ask.pendingRepeatAsk)
	}
	// check if we can reserve the node before reserving on the app
	if ok, err := node.reserve(sa, ask); !ok {
		return ok, err
	}
	sa.reservations[nodeReservation.getKey()] = nodeReservation
	// reservation added successfully
	return true, nil
}

// unReserve the application for this node and ask combination.
// This first removes the reservation from the node.
// The error is set if the reservation key cannot be generated on the app or node.
// If the reservation does not exist it returns false, if the reservation is removed it returns true.
func (sa *SchedulingApplication) unReserve(node *schedulingNode, ask *schedulingAllocationAsk) (bool, error) {
	sa.Lock()
	defer sa.Unlock()
	resKey := reservationKey(node, nil, ask)
	if resKey == "" {
		log.Logger().Debug("unreserve reservation key create failed unexpectedly",
			zap.String("appID", sa.ApplicationInfo.ApplicationID),
			zap.Any("node", node),
			zap.Any("ask", ask))
		return false, fmt.Errorf("reservation key failed node or ask are nil for appID %s", sa.ApplicationInfo.ApplicationID)
	}
	// find the reservation and then unreserve the node before removing from the app
	if _, found := sa.reservations[resKey]; found {
		if ok, err := node.unReserve(sa, ask); !ok {
			return false, err
		}
		delete(sa.reservations, resKey)
		return true, nil
	}
	// reservation was not found
	log.Logger().Debug("reservation not found while removing from app",
		zap.String("appID", sa.ApplicationInfo.ApplicationID),
		zap.String("nodeID", node.NodeID),
		zap.String("ask", ask.AskProto.AllocationKey))
	return false, nil
}

// Return the allocation reservations on any node.
// The returned array is 0 or more keys into the reservations map.
// No locking must be called while holding the lock
func (sa *SchedulingApplication) isAskReserved(allocKey string) []string {
	reservationKeys := make([]string, 0)
	if allocKey == "" {
		return reservationKeys
	}
	for key := range sa.reservations {
		if strings.HasSuffix(key, allocKey) {
			reservationKeys = append(reservationKeys, key)
		}
	}
	return reservationKeys
}

// Check if the allocation has already been reserved. An ask can reserve multiple nodes if the request has a repeat set
// larger than 1. It can never reserve more than the repeat number of nodes.
// No locking must be called while holding the lock
func (sa *SchedulingApplication) canAskReserve(ask *schedulingAllocationAsk) bool {
	allocKey := ask.AskProto.AllocationKey
	pending := int(ask.getPendingAskRepeat())
	resNumber := sa.isAskReserved(allocKey)
	if len(resNumber) >= pending {
		log.Logger().Debug("reservation exceeds repeats",
			zap.String("askKey", allocKey),
			zap.Int("askPending", pending),
			zap.Int("askReserved", len(resNumber)))
	}
	return pending > len(resNumber)
}

// Locking occurs by the methods that are calling the sort, this must be lock free.
func (sa *SchedulingApplication) sortRequests(ascending bool) {
	sa.sortedRequests = make([]*schedulingAllocationAsk, len(sa.requests))
	idx := 0
	for _, value := range sa.requests {
		sa.sortedRequests[idx] = value
		idx++
	}
	sortAskByPriority(sa.sortedRequests, ascending)
}
