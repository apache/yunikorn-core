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

	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/common"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type SchedulingNode struct {
	NodeID string

	// Private info
	allocating                  *resources.Resource     // resources being allocated
	preempting                  *resources.Resource     // resources considered for preemption
	reservations                map[string]*reservation // a map of reservations

	Hostname  string
	Rackname  string
	Partition string

	attributes            map[string]string
	totalResource         *resources.Resource
	occupiedResource      *resources.Resource
	allocatedResource     *resources.Resource
	availableResource     *resources.Resource
	availableUpdateNeeded bool
	allocations           map[string]*schedulingAllocation
	schedulable           bool


	sync.RWMutex
}

func newSchedulingNode(proto *si.NewNodeInfo) *SchedulingNode {
	if proto == nil {
		return nil
	}
	m := &SchedulingNode{
		NodeID:                proto.NodeID,
		availableUpdateNeeded: true,
		totalResource:         resources.NewResourceFromProto(proto.SchedulableResource),
		allocatedResource:     resources.NewResource(),
		occupiedResource:      resources.NewResourceFromProto(proto.OccupiedResource),
		allocations:           make(map[string]*schedulingAllocation),
		schedulable:           true,
		allocating:            resources.NewResource(),
		preempting:            resources.NewResource(),
		reservations:          make(map[string]*reservation),
	}

	// initialise available resources
	m.availableResource = resources.Sub(m.totalResource, m.occupiedResource)

	m.initializeAttribute(proto.Attributes)

	return m
}

// Return an array of all reservation keys for the node.
// This will return an empty array if there are no reservations.
// Visible for tests
func (sn *SchedulingNode) GetReservations() []string {
	sn.RLock()
	defer sn.RUnlock()
	keys := make([]string, 0)
	for key := range sn.reservations {
		keys = append(keys, key)
	}
	return keys
}

// FIXME, update this for node update calls.
func (sn *SchedulingNode) updateNodeInfo(newNodeInfo *cache.NodeInfo) {
	sn.Lock()
	defer sn.Unlock()

	sn.nodeInfo = newNodeInfo
}

// Get the available resource on this node.
// These resources are confirmed allocations (tracked in the cache node) minus the resources
// currently being allocated but not confirmed in the cache.
// This does not lock the cache node as it will take its own lock.
func (sn *SchedulingNode) GetAvailableResource() *resources.Resource {
	sn.Lock()
	defer sn.Unlock()
	return sn.availableResource
}

// Get the resource tagged for allocation on this node.
// These resources are part of unconfirmed allocations.
func (sn *SchedulingNode) getAllocatingResource() *resources.Resource {
	sn.RLock()
	defer sn.RUnlock()

	return sn.allocating
}

// Update the number of resource proposed for allocation on this node
func (sn *SchedulingNode) incAllocatingResource(delta *resources.Resource) {
	sn.Lock()
	defer sn.Unlock()

	sn.allocating.AddTo(delta)
}

// Handle the allocation processing on the scheduler when the cache node is updated.
func (sn *SchedulingNode) decAllocatingResource(delta *resources.Resource) {
	sn.Lock()
	defer sn.Unlock()

	var err error
	sn.allocating, err = resources.SubErrorNegative(sn.allocating, delta)
	if err != nil {
		log.Logger().Warn("Allocating resources went negative",
			zap.String("nodeID", sn.NodeID),
			zap.Error(err))
	}
}

// Get the number of resource tagged for preemption on this node
func (sn *SchedulingNode) getPreemptingResource() *resources.Resource {
	sn.RLock()
	defer sn.RUnlock()

	return sn.preempting
}

// Update the number of resource tagged for preemption on this node
func (sn *SchedulingNode) incPreemptingResource(preempting *resources.Resource) {
	sn.Lock()
	defer sn.Unlock()

	sn.preempting.AddTo(preempting)
}

func (sn *SchedulingNode) decPreemptingResource(delta *resources.Resource) {
	sn.Lock()
	defer sn.Unlock()
	var err error
	sn.preempting, err = resources.SubErrorNegative(sn.preempting, delta)
	if err != nil {
		log.Logger().Warn("Preempting resources went negative",
			zap.String("nodeID", sn.NodeID),
			zap.Error(err))
	}
}

// Check and update allocating resources of the scheduling node.
// If the proposed allocation fits in the available resources, taking into account resources marked for
// preemption if applicable, the allocating resources are updated and true is returned.
// If the proposed allocation does not fit false is returned and no changes are made.
func (sn *SchedulingNode) allocateResource(res *resources.Resource, preemptionPhase bool) bool {
	sn.Lock()
	defer sn.Unlock()
	newAllocating := resources.Add(res, sn.allocating)

	availableResource := sn.availableResource.Clone()
	if preemptionPhase {
		availableResource.AddTo(sn.preempting)
	}
	// check if this still fits: it might have changed since pre check
	if resources.FitIn(availableResource, newAllocating) {
		log.Logger().Debug("allocations in progress updated",
			zap.String("nodeID", sn.NodeID),
			zap.Any("total unconfirmed", newAllocating))
		sn.allocating = newAllocating
		return true
	}
	// allocation failed resource did not fit
	return false
}

// Checking pre-conditions in the shim for an allocation.
func (sn *SchedulingNode) preAllocateConditions(allocID string) bool {
	return sn.preConditions(allocID, true)
}

// Checking pre-conditions in the shim for a reservation.
func (sn *SchedulingNode) preReserveConditions(allocID string) bool {
	return sn.preConditions(allocID, false)
}

// The pre conditions are implemented via plugins in the shim. If no plugins are implemented then
// the check will return true. If multiple plugins are implemented the first failure will stop the
// checks.
// The caller must thus not rely on all plugins being executed.
// This is a lock free call as it does not change the node and multiple predicate checks could be
// run at the same time.
func (sn *SchedulingNode) preConditions(allocID string, allocate bool) bool {
	// Check the predicates plugin (k8shim)
	if plugin := plugins.GetPredicatesPlugin(); plugin != nil {
		// checking predicates
		if err := plugin.Predicates(&si.PredicatesArgs{
			AllocationKey: allocID,
			NodeID:        sn.NodeID,
			Allocate:      allocate,
		}); err != nil {
			// running predicates failed
			return false
		}
	}
	// all predicate plugins passed
	return true
}

// Check if the node should be considered as a possible node to allocate on.
//
// This is a lock free call. No updates are made this only performs a pre allocate checks
func (sn *SchedulingNode) preAllocateCheck(res *resources.Resource, resKey string, preemptionPhase bool) error {
	// shortcut if a node is not schedulable
	if !sn.IsSchedulable() {
		log.Logger().Debug("node is unschedulable",
			zap.String("nodeID", sn.NodeID))
		return fmt.Errorf("pre alloc check, node is unschedulable: %s", sn.NodeID)
	}
	// cannot allocate zero or negative resource
	if !resources.StrictlyGreaterThanZero(res) {
		log.Logger().Debug("pre alloc check: requested resource is zero",
			zap.String("nodeID", sn.NodeID))
		return fmt.Errorf("pre alloc check: requested resource is zero: %s", sn.NodeID)
	}
	// check if the node is reserved for this app/alloc
	if sn.isReserved() {
		if !sn.isReservedForApp(resKey) {
			log.Logger().Debug("pre alloc check: node reserved for different app or ask",
				zap.String("nodeID", sn.NodeID),
				zap.String("resKey", resKey))
			return fmt.Errorf("pre alloc check: node %s reserved for different app or ask: %s", sn.NodeID, resKey)
		}
	}

	// check if resources are available
	available := sn.availableResource.Clone()
	if preemptionPhase {
		available.AddTo(sn.getPreemptingResource())
	}
	// remove the unconfirmed resources
	available.SubFrom(sn.getAllocatingResource())
	// check the request fits in what we have calculated
	if !resources.FitIn(available, res) {
		// requested resource is larger than currently available node resources
		return fmt.Errorf("pre alloc check: requested resource %s is larger than currently available %s resource on %s", res.String(), available.String(), sn.NodeID)
	}
	// can allocate, based on resource size
	return nil
}

// Return if the node has been reserved by any application
func (sn *SchedulingNode) isReserved() bool {
	sn.RLock()
	defer sn.RUnlock()
	return len(sn.reservations) > 0
}

// Return true if and only if the node has been reserved by the application
// NOTE: a return value of false does not mean the node is not reserved by a different app
func (sn *SchedulingNode) isReservedForApp(key string) bool {
	if key == "" {
		return false
	}
	sn.RLock()
	defer sn.RUnlock()
	if strings.Contains(key, "|") {
		return sn.reservations[key] != nil
	}
	for resKey := range sn.reservations {
		if strings.HasPrefix(resKey, key) {
			return true
		}
	}
	return false
}

// Reserve the node for this application and ask combination, if not reserved yet.
// The reservation is checked against the node resources.
// If the reservation fails the function returns false, if the reservation is made it returns true.
func (sn *SchedulingNode) reserve(app *SchedulingApplication, ask *schedulingAllocationAsk) error {
	sn.Lock()
	defer sn.Unlock()
	if len(sn.reservations) > 0 {
		return fmt.Errorf("node is already reserved, nodeID %s", sn.NodeID)
	}
	appReservation := newReservation(sn, app, ask, false)
	// this should really not happen just guard against panic
	// either app or ask are nil
	if appReservation == nil {
		log.Logger().Debug("reservation creation failed unexpectedly",
			zap.String("nodeID", sn.NodeID),
			zap.Any("app", app),
			zap.Any("ask", ask))
		return fmt.Errorf("reservation creation failed app or ask are nil on nodeID %s", sn.NodeID)
	}
	// reservation must fit on the empty node
	if !sn.FitInNode(ask.AllocatedResource) {
		log.Logger().Debug("reservation does not fit on the node",
			zap.String("nodeID", sn.NodeID),
			zap.String("appID", app.ApplicationID),
			zap.String("ask", ask.AskProto.AllocationKey),
			zap.String("allocationAsk", ask.AllocatedResource.String()))
		return fmt.Errorf("reservation does not fit on node %s, appID %s, ask %s", sn.NodeID, app.ApplicationID, ask.AllocatedResource.String())
	}
	sn.reservations[appReservation.getKey()] = appReservation
	// reservation added successfully
	return nil
}

// unReserve the node for this application and ask combination
// If the reservation does not exist it returns 0 for reservations removed, if the reservation is removed it returns 1.
// The error is set if the reservation key cannot be generated.
func (sn *SchedulingNode) unReserve(app *SchedulingApplication, ask *schedulingAllocationAsk) (int, error) {
	sn.Lock()
	defer sn.Unlock()
	resKey := reservationKey(nil, app, ask)
	if resKey == "" {
		log.Logger().Debug("unreserve reservation key create failed unexpectedly",
			zap.String("nodeID", sn.NodeID),
			zap.Any("app", app),
			zap.Any("ask", ask))
		return 0, fmt.Errorf("reservation key failed app or ask are nil on nodeID %s", sn.NodeID)
	}
	if _, ok := sn.reservations[resKey]; ok {
		delete(sn.reservations, resKey)
		return 1, nil
	}
	// reservation was not found
	log.Logger().Debug("reservation not found while removing from node",
		zap.String("nodeID", sn.NodeID),
		zap.String("appID", app.ApplicationID),
		zap.String("ask", ask.AskProto.AllocationKey))
	return 0, nil
}

// Remove all reservation made on this node from the app.
// This is an unlocked function, it does not use a copy of the map when calling unReserve. That call will via the app call
// unReserve on the node which is locked and modifies the original map. However deleting an entry from a map while iterating
// over the map is perfectly safe based on the Go Specs.
// It must only be called when removing the node under a partition lock.
// It returns a list of all apps that have been checked on the node regardless of the result of the app unReserve call.
// The corresponding integers show the number of reservations removed for each app entry
func (sn *SchedulingNode) unReserveApps() ([]string, []int) {
	var appReserve []string
	var askRelease []int
	for key, res := range sn.reservations {
		appID := res.appID
		num, err := res.app.unReserveInternal(res.node, res.ask)
		if err != nil {
			log.Logger().Warn("Removal of reservation failed while removing node",
				zap.String("nodeID", sn.NodeID),
				zap.String("reservationKey", key),
				zap.Error(err))
		}
		// pass back the removed asks for each app
		appReserve = append(appReserve, appID)
		askRelease = append(askRelease, num)
	}
	return appReserve, askRelease
}

// Set the attributes and fast access fields.
// Unlocked call: should only be called on create or from test code
func (sn *SchedulingNode) initializeAttribute(newAttributes map[string]string) {
	sn.attributes = newAttributes

	sn.Hostname = sn.attributes[common.HostName]
	sn.Rackname = sn.attributes[common.RackName]
	sn.Partition = sn.attributes[common.NodePartition]
}

// Get an attribute by name. The most used attributes can be directly accessed via the
// fields: HostName, RackName and Partition.
// This is a lock free call. All attributes are considered read only
func (sn *SchedulingNode) GetAttribute(key string) string {
	return sn.attributes[key]
}

// Return the currently allocated resource for the node.
// It returns a cloned object as we do not want to allow modifications to be made to the
// value of the node.
func (sn *SchedulingNode) GetAllocatedResource() *resources.Resource {
	sn.RLock()
	defer sn.RUnlock()

	return sn.allocatedResource
}

func (sn *SchedulingNode) GetCapacity() *resources.Resource {
	sn.RLock()
	defer sn.RUnlock()
	return sn.totalResource.Clone()
}

func (sn *SchedulingNode) setCapacity(newCapacity *resources.Resource) {
	sn.Lock()
	defer sn.Unlock()
	if resources.Equals(sn.totalResource, newCapacity) {
		log.Logger().Debug("skip updating capacity, not changed")
		return
	}
	sn.totalResource = newCapacity
	sn.refreshAvailableResource()
}

func (sn *SchedulingNode) GetOccupiedResource() *resources.Resource {
	sn.RLock()
	defer sn.RUnlock()
	return sn.occupiedResource.Clone()
}

func (sn *SchedulingNode) setOccupiedResource(occupiedResource *resources.Resource) {
	sn.Lock()
	defer sn.Unlock()
	if resources.Equals(sn.occupiedResource, occupiedResource) {
		log.Logger().Debug("skip updating occupiedResource, not changed")
		return
	}
	sn.occupiedResource = occupiedResource
	sn.refreshAvailableResource()
}

// Return the allocation based on the uuid of the allocation.
// returns nil if the allocation is not found
func (sn *SchedulingNode) GetAllocation(uuid string) *schedulingAllocation {
	sn.RLock()
	defer sn.RUnlock()

	return sn.allocations[uuid]
}

// Check if the allocation fits int the nodes resources.
// unlocked call as the totalResource can not be changed
func (sn *SchedulingNode) FitInNode(resRequest *resources.Resource) bool {
	return resources.FitIn(sn.totalResource, resRequest)
}

// Check if the allocation fits in the currently available resources.
func (sn *SchedulingNode) canAllocate(resRequest *resources.Resource) bool {
	sn.RLock()
	defer sn.RUnlock()
	return resources.FitIn(sn.availableResource, resRequest)
}

// Add the allocation to the node.Used resources will increase available will decrease.
// This cannot fail. A nil AllocationInfo makes no changes.
func (sn *SchedulingNode) AddAllocation(alloc *schedulingAllocation) {
	if alloc == nil {
		return
	}
	sn.Lock()
	defer sn.Unlock()

	sn.allocations[alloc.uuid] = alloc
	sn.allocatedResource = resources.Add(sn.allocatedResource, alloc.schedulingAsk.AllocatedResource)
	sn.availableResource = resources.Sub(sn.availableResource, alloc.schedulingAsk.AllocatedResource)
	sn.availableUpdateNeeded = true
}

// Remove the allocation to the node.
// Returns nil if the allocation was not found and no changes are made. If the allocation
// is found the AllocationInfo removed is returned. Used resources will decrease available
// will increase as per the allocation removed.
func (sn *SchedulingNode) RemoveAllocation(uuid string) *schedulingAllocation {
	sn.Lock()
	defer sn.Unlock()

	info := sn.allocations[uuid]
	if info != nil {
		delete(sn.allocations, uuid)
		sn.allocatedResource = resources.Sub(sn.allocatedResource, info.schedulingAsk.AllocatedResource)
		sn.availableResource = resources.Add(sn.availableResource, info.schedulingAsk.AllocatedResource)
		sn.availableUpdateNeeded = true
	}

	return info
}

// Get a copy of the allocations on this node
func (sn *SchedulingNode) GetAllAllocations() []*schedulingAllocation {
	sn.RLock()
	defer sn.RUnlock()

	arr := make([]*schedulingAllocation, 0)
	for _, v := range sn.allocations {
		arr = append(arr, v)
	}

	return arr
}

// Set the node to unschedulable.
// This will cause the node to be skipped during the scheduling cycle.
// Visible for testing only
func (sn *SchedulingNode) SetSchedulable(schedulable bool) {
	sn.Lock()
	defer sn.Unlock()
	sn.schedulable = schedulable
}

// Can this node be used in scheduling.
func (sn *SchedulingNode) IsSchedulable() bool {
	sn.RLock()
	defer sn.RUnlock()
	return sn.schedulable
}

// refresh node available resource based on the latest allocated and occupied resources.
// this call assumes the caller already acquires the lock.
func (sn *SchedulingNode) refreshAvailableResource() {
	sn.availableResource = sn.totalResource.Clone()
	sn.availableResource = resources.Sub(sn.availableResource, sn.allocatedResource)
	sn.availableResource = resources.Sub(sn.availableResource, sn.occupiedResource)
	sn.availableUpdateNeeded = true
}
