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

package objects

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const ReadyFlag = "ready"

type Node struct {
	// Fields for fast access These fields are considered read only.
	// Values should only be set when creating a new node and never changed.
	NodeID    string
	Hostname  string
	Rackname  string
	Partition string

	// Private fields need protection
	attributes        map[string]string
	totalResource     *resources.Resource
	occupiedResource  *resources.Resource
	allocatedResource *resources.Resource
	availableResource *resources.Resource
	allocations       map[string]*Allocation
	schedulable       bool
	ready             bool

	preempting   *resources.Resource     // resources considered for preemption
	reservations map[string]*reservation // a map of reservations
	listeners    []NodeListener          // a list of node listeners

	sync.RWMutex
}

func NewNode(proto *si.NodeInfo) *Node {
	// safe guard against panic
	if proto == nil {
		return nil
	}

	var ready bool
	var err error
	if ready, err = strconv.ParseBool(proto.Attributes[ReadyFlag]); err != nil {
		log.Logger().Error("Could not parse ready flag, assuming true")
		ready = true
	}
	sn := &Node{
		NodeID:            proto.NodeID,
		preempting:        resources.NewResource(),
		reservations:      make(map[string]*reservation),
		totalResource:     resources.NewResourceFromProto(proto.SchedulableResource),
		allocatedResource: resources.NewResource(),
		occupiedResource:  resources.NewResourceFromProto(proto.OccupiedResource),
		allocations:       make(map[string]*Allocation),
		schedulable:       true,
		listeners:         make([]NodeListener, 0),
		ready:             ready,
	}
	// initialise available resources
	sn.availableResource, err = resources.SubErrorNegative(sn.totalResource, sn.occupiedResource)
	if err != nil {
		log.Logger().Error("New node created with no available resources",
			zap.Error(err))
	}

	sn.initializeAttribute(proto.Attributes)

	return sn
}

func (sn *Node) String() string {
	if sn == nil {
		return "node is nil"
	}
	return fmt.Sprintf("NodeID %s, Partition %s, Schedulable %t, Total %s, Allocated %s, #allocations %d",
		sn.NodeID, sn.Partition, sn.schedulable, sn.totalResource, sn.allocatedResource, len(sn.allocations))
}

// Set the attributes and fast access fields.
// Unlocked call: should only be called on create or from test code
func (sn *Node) initializeAttribute(newAttributes map[string]string) {
	sn.attributes = newAttributes

	sn.Hostname = sn.attributes[common.HostName]
	sn.Rackname = sn.attributes[common.RackName]
	sn.Partition = sn.attributes[common.NodePartition]
}

// Get an attribute by name. The most used attributes can be directly accessed via the
// fields: HostName, RackName and Partition.
// This is a lock free call. All attributes are considered read only
func (sn *Node) GetAttribute(key string) string {
	return sn.attributes[key]
}

// Return an array of all reservation keys for the node.
// This will return an empty array if there are no reservations.
// Visible for tests
func (sn *Node) GetReservations() []string {
	sn.RLock()
	defer sn.RUnlock()
	keys := make([]string, 0)
	for key := range sn.reservations {
		keys = append(keys, key)
	}
	return keys
}

func (sn *Node) GetCapacity() *resources.Resource {
	sn.RLock()
	defer sn.RUnlock()
	return sn.totalResource.Clone()
}

func (sn *Node) SetCapacity(newCapacity *resources.Resource) *resources.Resource {
	defer sn.notifyListeners()
	sn.Lock()
	defer sn.Unlock()
	if resources.Equals(sn.totalResource, newCapacity) {
		log.Logger().Debug("skip updating capacity, not changed")
		return nil
	}
	delta := resources.Sub(newCapacity, sn.totalResource)
	sn.totalResource = newCapacity
	sn.refreshAvailableResource()
	return delta
}

func (sn *Node) GetOccupiedResource() *resources.Resource {
	sn.RLock()
	defer sn.RUnlock()
	return sn.occupiedResource.Clone()
}

func (sn *Node) SetOccupiedResource(occupiedResource *resources.Resource) {
	defer sn.notifyListeners()
	sn.Lock()
	defer sn.Unlock()
	if resources.Equals(sn.occupiedResource, occupiedResource) {
		log.Logger().Debug("skip updating occupiedResource, not changed")
		return
	}
	sn.occupiedResource = occupiedResource
	sn.refreshAvailableResource()
}

// refresh node available resource based on the latest total, allocated and occupied resources.
// this call assumes the caller already acquires the lock.
func (sn *Node) refreshAvailableResource() {
	sn.availableResource = sn.totalResource.Clone()
	sn.availableResource.SubFrom(sn.allocatedResource)
	sn.availableResource.SubFrom(sn.occupiedResource)
	// check if any quantity is negative: a nil resource is all 0's
	if !resources.StrictlyGreaterThanOrEquals(sn.availableResource, nil) {
		log.Logger().Warn("Node update triggered over allocated node",
			zap.String("available", sn.availableResource.String()),
			zap.String("total", sn.totalResource.String()),
			zap.String("occupied", sn.occupiedResource.String()),
			zap.String("allocated", sn.allocatedResource.String()))
	}
}

// Return the allocation based on the uuid of the allocation.
// returns nil if the allocation is not found
func (sn *Node) GetAllocation(uuid string) *Allocation {
	sn.RLock()
	defer sn.RUnlock()

	return sn.allocations[uuid]
}

// Get a copy of the allocations on this node
func (sn *Node) GetAllAllocations() []*Allocation {
	sn.RLock()
	defer sn.RUnlock()

	arr := make([]*Allocation, 0)
	for _, v := range sn.allocations {
		arr = append(arr, v)
	}

	return arr
}

// Set the node to unschedulable.
// This will cause the node to be skipped during the scheduling cycle.
// Visible for testing only
func (sn *Node) SetSchedulable(schedulable bool) {
	defer sn.notifyListeners()
	sn.Lock()
	defer sn.Unlock()
	sn.schedulable = schedulable
}

// Can this node be used in scheduling.
func (sn *Node) IsSchedulable() bool {
	sn.RLock()
	defer sn.RUnlock()
	return sn.schedulable
}

// Get the allocated resource on this node.
func (sn *Node) GetAllocatedResource() *resources.Resource {
	sn.RLock()
	defer sn.RUnlock()
	return sn.allocatedResource.Clone()
}

// Get the available resource on this node.
func (sn *Node) GetAvailableResource() *resources.Resource {
	sn.Lock()
	defer sn.Unlock()
	return sn.availableResource.Clone()
}

// Get the utilized resource on this node.
func (sn *Node) GetUtilizedResource() *resources.Resource {
	total := sn.GetCapacity()
	resourceAllocated := sn.GetAllocatedResource()
	utilizedResource := make(map[string]resources.Quantity)

	for name := range resourceAllocated.Resources {
		if total.Resources[name] > 0 {
			utilizedResource[name] = resources.CalculateAbsUsedCapacity(total, resourceAllocated).Resources[name]
		}
	}
	return &resources.Resource{Resources: utilizedResource}
}

func (sn *Node) FitInNode(resRequest *resources.Resource) bool {
	sn.RLock()
	defer sn.RUnlock()
	return sn.totalResource.FitInMaxUndef(resRequest)
}

// Get the number of resource tagged for preemption on this node
func (sn *Node) getPreemptingResource() *resources.Resource {
	sn.RLock()
	defer sn.RUnlock()

	return sn.preempting
}

// Update the number of resource tagged for preemption on this node
func (sn *Node) IncPreemptingResource(preempting *resources.Resource) {
	defer sn.notifyListeners()
	sn.Lock()
	defer sn.Unlock()

	sn.preempting.AddTo(preempting)
}

func (sn *Node) decPreemptingResource(delta *resources.Resource) {
	defer sn.notifyListeners()
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

// Remove the allocation to the node.
// Returns nil if the allocation was not found and no changes are made. If the allocation
// is found the Allocation removed is returned. Used resources will decrease available
// will increase as per the allocation removed.
func (sn *Node) RemoveAllocation(uuid string) *Allocation {
	defer sn.notifyListeners()
	sn.Lock()
	defer sn.Unlock()

	alloc := sn.allocations[uuid]
	if alloc != nil {
		delete(sn.allocations, uuid)
		sn.allocatedResource.SubFrom(alloc.AllocatedResource)
		sn.availableResource.AddTo(alloc.AllocatedResource)
		return alloc
	}

	return nil
}

// Add the allocation to the node. Used resources will increase available will decrease.
// A nil Allocation makes no changes. Pre-empted resources must have been released already.
// Do a sanity check to make sure it still fits in the node and nothing has changed
func (sn *Node) AddAllocation(alloc *Allocation) bool {
	if alloc == nil {
		return false
	}
	defer sn.notifyListeners()
	sn.Lock()
	defer sn.Unlock()
	// check if this still fits: it might have changed since pre check
	res := alloc.AllocatedResource
	if sn.availableResource.FitInMaxUndef(res) {
		sn.allocations[alloc.UUID] = alloc
		sn.allocatedResource.AddTo(res)
		sn.availableResource.SubFrom(res)
		return true
	}
	return false
}

// ReplaceAllocation replaces the placeholder with the real allocation on the node.
// The delta passed in is the difference in resource usage between placeholder and real allocation.
// It should always be a negative value or zero: it is a decrease in usage or no change
func (sn *Node) ReplaceAllocation(uuid string, replace *Allocation, delta *resources.Resource) {
	defer sn.notifyListeners()
	sn.Lock()
	defer sn.Unlock()

	replace.placeholderCreateTime = sn.allocations[uuid].createTime
	delete(sn.allocations, uuid)
	replace.placeholderUsed = true
	sn.allocations[replace.UUID] = replace
	before := sn.allocatedResource.Clone()
	sn.allocatedResource.AddTo(delta)
	if !resources.FitIn(before, sn.allocatedResource) {
		log.Logger().Warn("unexpected increase in node usage after placeholder replacement",
			zap.String("placeholder UUID", uuid),
			zap.String("allocation UUID", replace.UUID),
			zap.String("delta", delta.String()))
	}
}

// Check if the proposed allocation fits in the available resources.
// Taking into account resources marked for preemption if applicable.
// If the proposed allocation does not fit false is returned.
// TODO: remove when updating preemption
func (sn *Node) CanAllocate(res *resources.Resource, preemptionPhase bool) bool {
	err := sn.preAllocateCheck(res, "", preemptionPhase)
	return err == nil
}

// Checking pre-conditions in the shim for an allocation.
func (sn *Node) preAllocateConditions(allocID string) bool {
	return sn.preConditions(allocID, true)
}

// Checking pre-conditions in the shim for a reservation.
func (sn *Node) preReserveConditions(allocID string) bool {
	return sn.preConditions(allocID, false)
}

// The pre conditions are implemented via plugins in the shim. If no plugins are implemented then
// the check will return true. If multiple plugins are implemented the first failure will stop the
// checks.
// The caller must thus not rely on all plugins being executed.
// This is a lock free call as it does not change the node and multiple predicate checks could be
// run at the same time.
func (sn *Node) preConditions(allocID string, allocate bool) bool {
	// Check the predicates plugin (k8shim)
	if plugin := plugins.GetResourceManagerCallbackPlugin(); plugin != nil {
		// checking predicates
		if err := plugin.Predicates(&si.PredicatesArgs{
			AllocationKey: allocID,
			NodeID:        sn.NodeID,
			Allocate:      allocate,
		}); err != nil {
			log.Logger().Debug("running predicates failed",
				zap.String("allocationKey", allocID),
				zap.String("nodeID", sn.NodeID),
				zap.Bool("allocateFlag", allocate),
				zap.Error(err))
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
func (sn *Node) preAllocateCheck(res *resources.Resource, resKey string, preemptionPhase bool) error {
	// cannot allocate zero or negative resource
	if !resources.StrictlyGreaterThanZero(res) {
		log.Logger().Debug("pre alloc check: requested resource is zero",
			zap.String("nodeID", sn.NodeID))
		return fmt.Errorf("pre alloc check: requested resource is zero: %s", sn.NodeID)
	}
	// check if the node is reserved for this app/alloc
	if sn.IsReserved() {
		if !sn.isReservedForApp(resKey) {
			log.Logger().Debug("pre alloc check: node reserved for different app or ask",
				zap.String("nodeID", sn.NodeID),
				zap.String("resKey", resKey))
			return fmt.Errorf("pre alloc check: node %s reserved for different app or ask: %s", sn.NodeID, resKey)
		}
	}

	// check if resources are available
	available := sn.GetAvailableResource()
	if preemptionPhase {
		available.AddTo(sn.getPreemptingResource())
	}
	// check the request fits in what we have calculated
	if !available.FitInMaxUndef(res) {
		// requested resource is larger than currently available node resources
		return fmt.Errorf("pre alloc check: requested resource %s is larger than currently available %s resource on %s", res.String(), available.String(), sn.NodeID)
	}
	// can allocate, based on resource size
	return nil
}

// Return if the node has been reserved by any application
func (sn *Node) IsReserved() bool {
	sn.RLock()
	defer sn.RUnlock()
	return len(sn.reservations) > 0
}

// isReservedForApp returns true if and only if the node has been reserved by the application
// NOTE: a return value of false does not mean the node is not reserved by a different app
func (sn *Node) isReservedForApp(key string) bool {
	if key == "" {
		return false
	}
	sn.RLock()
	defer sn.RUnlock()
	if strings.Contains(key, "|") {
		return sn.reservations[key] != nil
	}
	// make sure matches only for the whole appID
	separator := key + "|"
	for resKey := range sn.reservations {
		if strings.HasPrefix(resKey, separator) {
			return true
		}
	}
	return false
}

// Reserve the node for this application and ask combination, if not reserved yet.
// The reservation is checked against the node resources.
// If the reservation fails the function returns false, if the reservation is made it returns true.
func (sn *Node) Reserve(app *Application, ask *AllocationAsk) error {
	defer sn.notifyListeners()
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
	if !resources.FitIn(sn.totalResource, ask.AllocatedResource) {
		log.Logger().Debug("reservation does not fit on the node",
			zap.String("nodeID", sn.NodeID),
			zap.String("appID", app.ApplicationID),
			zap.String("ask", ask.AllocationKey),
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
func (sn *Node) unReserve(app *Application, ask *AllocationAsk) (int, error) {
	defer sn.notifyListeners()
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
		zap.String("ask", ask.AllocationKey))
	return 0, nil
}

// Remove all reservation made on this node from the app.
// This is an unlocked function, it does not use a copy of the map when calling unReserve. That call will via the app call
// unReserve on the node which is locked and modifies the original map. However deleting an entry from a map while iterating
// over the map is perfectly safe based on the Go Specs.
// It must only be called when removing the node under a partition lock.
// It returns a list of all apps that have been checked on the node regardless of the result of the app unReserve call.
// The corresponding integers show the number of reservations removed for each app entry
func (sn *Node) UnReserveApps() ([]string, []int) {
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

// Gets map of name -> resource usages per type in shares (0 to 1). Can return NaN.
func (sn *Node) GetResourceUsageShares() map[string]float64 {
	sn.RLock()
	defer sn.RUnlock()
	res := make(map[string]float64)
	if sn.totalResource == nil {
		// no resources present, so no usage
		return res
	}
	for k, v := range sn.totalResource.Resources {
		res[k] = float64(1) - (float64(sn.availableResource.Resources[k]) / float64(v))
	}
	return res
}

func (sn *Node) AddListener(listener NodeListener) {
	sn.Lock()
	defer sn.Unlock()
	sn.listeners = append(sn.listeners, listener)
}

func (sn *Node) RemoveListener(listener NodeListener) {
	sn.Lock()
	defer sn.Unlock()

	newListeners := make([]NodeListener, 0)
	for _, entry := range sn.listeners {
		if entry == listener {
			continue
		}
		newListeners = append(newListeners, entry)
	}
	sn.listeners = newListeners
}

// Notifies listeners of changes to this node. This method must not be called while locks are held.
func (sn *Node) notifyListeners() {
	for _, listener := range sn.getListeners() {
		listener.NodeUpdated(sn)
	}
}

func (sn *Node) getListeners() []NodeListener {
	sn.RLock()
	defer sn.RUnlock()
	list := make([]NodeListener, len(sn.listeners))
	copy(list, sn.listeners)
	return list
}

func (sn *Node) IsReady() bool {
	sn.RLock()
	defer sn.RUnlock()
	return sn.ready
}

func (sn *Node) SetReady(ready bool) {
	sn.Lock()
	defer sn.Unlock()
	sn.ready = ready
}
