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
	ApplicationInfo      *cache.ApplicationInfo
	Requests             *SchedulingRequests
	MayAllocatedResource *resources.Resource // Maybe allocated, set by scheduler

	// Private fields need protection
	queue        *SchedulingQueue        // queue the application is running in
	reservations map[string]*reservation // a map of reservations

	sync.RWMutex
}

func newSchedulingApplication(appInfo *cache.ApplicationInfo) *SchedulingApplication {
	return &SchedulingApplication{
		ApplicationInfo: appInfo,
		Requests:        NewSchedulingRequests(),
		reservations:    make(map[string]*reservation),
	}
}

// Return if the application has any reservations
func (sa *SchedulingApplication) hasReserved() bool {
	sa.RLock()
	defer sa.RUnlock()
	return len(sa.reservations) > 0
}

// Return if the application has the node reserved
func (sa *SchedulingApplication) isReservedOnNode(nodeID string) bool {
	sa.RLock()
	defer sa.RUnlock()
	for key := range sa.reservations {
		if strings.HasPrefix(key, nodeID) {
			return true
		}
	}
	return false
}

// Reserve the application for this node and ask combination
// If the reservation fails the function returns false, if the reservation is made it returns true
// If the node and ask combination was already reserved for the application this is a noop and returns true
func (sa *SchedulingApplication) reserve(node *schedulingNode, ask *SchedulingAllocationAsk) (bool, error) {
	sa.Lock()
	defer sa.Unlock()
	// create the reservation (includes nil checks)
	reserved := newReservation(node, nil, ask)
	if reserved == nil {
		log.Logger().Debug("reservation creation failed unexpectedly",
			zap.String("app", sa.ApplicationInfo.ApplicationID),
			zap.Any("node", node),
			zap.Any("ask", ask))
		return false, fmt.Errorf("reservation creation failed node or ask are nil on appID %s", sa.ApplicationInfo.ApplicationID)
	}
	// check if we can reserved the node before reserving on the app
	if ok, err := node.reserve(sa, ask); !ok {
		return ok, err
	}
	sa.reservations[reserved.getKey()] = reserved
	// reservation added successfully
	return true, nil
}

// unReserve the application for this node and ask combination.
// This first removes the reservation from the node.
// The error is set if the reservation key cannot be generated on the app or node.
// If the reservation does not exist it returns false, if the reservation is removed it returns true
func (sa *SchedulingApplication) unReserve(node *schedulingNode, ask *SchedulingAllocationAsk) (bool, error) {
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