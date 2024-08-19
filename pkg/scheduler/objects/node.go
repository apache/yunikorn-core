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
	"strings"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/plugins"
	schedEvt "github.com/apache/yunikorn-core/pkg/scheduler/objects/events"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	UnknownInstanceType = "UNKNOWN"
)

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

	reservations map[string]*reservation // a map of reservations
	listeners    []NodeListener          // a list of node listeners
	nodeEvents   *schedEvt.NodeEvents

	locking.RWMutex
}

func NewNode(proto *si.NodeInfo) *Node {
	// safeguard against panic
	if proto == nil {
		return nil
	}

	sn := &Node{
		NodeID:            proto.NodeID,
		reservations:      make(map[string]*reservation),
		totalResource:     resources.NewResourceFromProto(proto.SchedulableResource),
		allocatedResource: resources.NewResource(),
		occupiedResource:  resources.NewResourceFromProto(proto.OccupiedResource),
		allocations:       make(map[string]*Allocation),
		schedulable:       true,
		listeners:         make([]NodeListener, 0),
	}
	sn.nodeEvents = schedEvt.NewNodeEvents(events.GetEventSystem())
	// initialise available resources
	var err error
	sn.availableResource, err = resources.SubErrorNegative(sn.totalResource, sn.occupiedResource)
	if err != nil {
		log.Log(log.SchedNode).Error("New node created with no available resources",
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

	// Avoid passing empty nodeAttributes in initializeAttribute
	if len(sn.attributes) == 0 {
		sn.attributes = map[string]string{}
	}

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

func (sn *Node) GetAttributes() map[string]string {
	return sn.attributes
}

// Get InstanceType of this node.
// This is a lock free call because all attributes are considered read only
func (sn *Node) GetInstanceType() string {
	itype := sn.GetAttribute(common.InstanceType)
	if itype != "" {
		return itype
	}
	return UnknownInstanceType
}

// GetReservationKeys Return an array of all reservation keys for the node.
// This will return an empty array if there are no reservations.
// Visible for tests
func (sn *Node) GetReservationKeys() []string {
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

// SetCapacity changes the node resource capacity and returns the resource delta.
// The delta is positive for an increased capacity and negative for a decrease.
func (sn *Node) SetCapacity(newCapacity *resources.Resource) *resources.Resource {
	var delta *resources.Resource
	defer func() {
		if delta != nil {
			sn.notifyListeners()
		}
	}()
	sn.Lock()
	defer sn.Unlock()
	if resources.Equals(sn.totalResource, newCapacity) {
		log.Log(log.SchedNode).Debug("skip updating capacity, not changed")
		return nil
	}
	delta = resources.Sub(newCapacity, sn.totalResource)
	sn.totalResource = newCapacity
	sn.refreshAvailableResource()
	sn.nodeEvents.SendNodeCapacityChangedEvent(sn.NodeID, sn.totalResource.Clone())
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
		log.Log(log.SchedNode).Debug("skip updating occupiedResource, not changed")
		return
	}
	sn.occupiedResource = occupiedResource
	sn.nodeEvents.SendNodeOccupiedResourceChangedEvent(sn.NodeID, sn.occupiedResource.Clone())
	sn.refreshAvailableResource()
}

// refresh node available resource based on the latest total, allocated and occupied resources.
// this call assumes the caller already acquires the lock.
func (sn *Node) refreshAvailableResource() {
	sn.availableResource = sn.totalResource.Clone()
	sn.availableResource.SubFrom(sn.allocatedResource)
	sn.availableResource.SubFrom(sn.occupiedResource)
	sn.availableResource.Prune()
	// check if any quantity is negative: a nil resource is all 0's
	if !resources.StrictlyGreaterThanOrEquals(sn.availableResource, nil) {
		log.Log(log.SchedNode).Warn("Node update triggered over allocated node",
			zap.Stringer("available", sn.availableResource),
			zap.Stringer("total", sn.totalResource),
			zap.Stringer("occupied", sn.occupiedResource),
			zap.Stringer("allocated", sn.allocatedResource))
	}
}

// Return the allocation based on the allocationKey of the allocation.
// returns nil if the allocation is not found
func (sn *Node) GetAllocation(allocationKey string) *Allocation {
	sn.RLock()
	defer sn.RUnlock()

	return sn.allocations[allocationKey]
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
	sn.nodeEvents.SendNodeSchedulableChangedEvent(sn.NodeID, sn.schedulable)
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

// GetFitInScoreForAvailableResource calculates a fit in score for "res" based on the current
// available resources, avoiding cloning. The caller must ensure that "res" cannot change while this method is running.
func (sn *Node) GetFitInScoreForAvailableResource(res *resources.Resource) float64 {
	sn.RLock()
	defer sn.RUnlock()
	return res.FitInScore(sn.availableResource)
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

// FitInNode checks if the request fits in the node.
// All resources types requested must match the resource types provided by the nodes.
// A request may ask for only a subset of the types, but the node must provide at least the
// resource types requested in a larger or equal quantity as requested.
func (sn *Node) FitInNode(resRequest *resources.Resource) bool {
	sn.RLock()
	defer sn.RUnlock()
	return sn.totalResource.FitIn(resRequest)
}

// Remove the allocation to the node.
// Returns nil if the allocation was not found and no changes are made. If the allocation
// is found the Allocation removed is returned. Used resources will decrease available
// will increase as per the allocation removed.
func (sn *Node) RemoveAllocation(allocationKey string) *Allocation {
	defer sn.notifyListeners()
	sn.Lock()
	defer sn.Unlock()

	alloc := sn.allocations[allocationKey]
	if alloc != nil {
		delete(sn.allocations, allocationKey)
		sn.allocatedResource.SubFrom(alloc.GetAllocatedResource())
		sn.availableResource.AddTo(alloc.GetAllocatedResource())
		sn.allocatedResource.Prune()
		sn.nodeEvents.SendAllocationRemovedEvent(sn.NodeID, alloc.allocationKey, alloc.allocatedResource)
		return alloc
	}

	return nil
}

// TryAddAllocation attempts to add the allocation to the node. Used resources will increase available will decrease.
// A nil Allocation makes no changes. Preempted resources must have been released already.
// Do a sanity check to make sure it still fits in the node and nothing has changed
func (sn *Node) TryAddAllocation(alloc *Allocation) bool {
	return sn.addAllocationInternal(alloc, false)
}

// AddAllocation adds the allocation to the node. Used resources will increase available will decrease.
// A nil Allocation makes no changes. Preempted resources must have been released already.
// Do a sanity check to make sure it still fits in the node and nothing has changed
func (sn *Node) AddAllocation(alloc *Allocation) {
	_ = sn.addAllocationInternal(alloc, true)
}

func (sn *Node) addAllocationInternal(alloc *Allocation, force bool) bool {
	if alloc == nil {
		return false
	}
	result := false
	defer func() {
		// check result to ensure we don't notify listeners unnecessarily
		if result {
			sn.notifyListeners()
		}
	}()

	sn.Lock()
	defer sn.Unlock()
	// check if this still fits: it might have changed since pre-check
	res := alloc.GetAllocatedResource()
	if force || sn.availableResource.FitIn(res) {
		sn.allocations[alloc.GetAllocationKey()] = alloc
		sn.allocatedResource.AddTo(res)
		sn.availableResource.SubFrom(res)
		sn.availableResource.Prune()
		sn.nodeEvents.SendAllocationAddedEvent(sn.NodeID, alloc.allocationKey, res)
		result = true
		return result
	}
	result = false
	return result
}

// ReplaceAllocation replaces the placeholder with the real allocation on the node.
// The delta passed in is the difference in resource usage between placeholder and real allocation.
// It should always be a negative value or zero: it is a decrease in usage or no change
func (sn *Node) ReplaceAllocation(allocationKey string, replace *Allocation, delta *resources.Resource) {
	defer sn.notifyListeners()
	sn.Lock()
	defer sn.Unlock()

	replace.SetPlaceholderCreateTime(sn.allocations[allocationKey].GetCreateTime())
	delete(sn.allocations, allocationKey)
	replace.SetPlaceholderUsed(true)
	sn.allocations[replace.GetAllocationKey()] = replace
	before := sn.allocatedResource.Clone()
	// The allocatedResource and availableResource should be updated in the same way
	sn.allocatedResource.AddTo(delta)
	sn.availableResource.SubFrom(delta)
	sn.availableResource.Prune()
	if !before.FitIn(sn.allocatedResource) {
		log.Log(log.SchedNode).Warn("unexpected increase in node usage after placeholder replacement",
			zap.String("placeholder allocationKey", allocationKey),
			zap.String("allocation allocationKey", replace.GetAllocationKey()),
			zap.Stringer("delta", delta))
	}
}

// CanAllocate checks if the proposed allocation fits in the available resources.
// If the proposed allocation does not fit false is returned.
func (sn *Node) CanAllocate(res *resources.Resource) bool {
	sn.RLock()
	defer sn.RUnlock()
	return sn.availableResource.FitIn(res)
}

// Checking pre-conditions in the shim for an allocation.
func (sn *Node) preAllocateConditions(ask *Allocation) error {
	return sn.preConditions(ask, true)
}

// Checking pre-conditions in the shim for a reservation.
func (sn *Node) preReserveConditions(ask *Allocation) error {
	return sn.preConditions(ask, false)
}

// The pre conditions are implemented via plugins in the shim. If no plugins are implemented then
// the check will return true. If multiple plugins are implemented the first failure will stop the
// checks.
// The caller must thus not rely on all plugins being executed.
// This is a lock free call as it does not change the node and multiple predicate checks could be
// run at the same time.
func (sn *Node) preConditions(ask *Allocation, allocate bool) error {
	// Check the predicates plugin (k8shim)
	allocationKey := ask.GetAllocationKey()
	if plugin := plugins.GetResourceManagerCallbackPlugin(); plugin != nil {
		// checking predicates
		if err := plugin.Predicates(&si.PredicatesArgs{
			AllocationKey: allocationKey,
			NodeID:        sn.NodeID,
			Allocate:      allocate,
		}); err != nil {
			log.Log(log.SchedNode).Debug("running predicates failed",
				zap.String("allocationKey", allocationKey),
				zap.String("nodeID", sn.NodeID),
				zap.Bool("allocateFlag", allocate),
				zap.Error(err))
			// running predicates failed
			msg := err.Error()
			ask.LogAllocationFailure(msg, allocate)
			return err
		}
	}
	// all predicate plugins passed
	return nil
}

// preAllocateCheck checks if the node should be considered as a possible node to allocate on.
// No updates are made this only performs a pre allocate checks
func (sn *Node) preAllocateCheck(res *resources.Resource, resKey string) bool {
	// cannot allocate zero or negative resource
	if !resources.StrictlyGreaterThanZero(res) {
		log.Log(log.SchedNode).Debug("pre alloc check: requested resource is zero",
			zap.String("nodeID", sn.NodeID))
		return false
	}
	// check if the node is reserved for this app/alloc
	if sn.IsReserved() {
		if !sn.isReservedForApp(resKey) {
			log.Log(log.SchedNode).Debug("pre alloc check: node reserved for different app or ask",
				zap.String("nodeID", sn.NodeID),
				zap.String("resKey", resKey))
			return false
		}
	}

	sn.RLock()
	defer sn.RUnlock()
	// returns true/false based on if the request fits in what we have calculated
	return sn.availableResource.FitIn(res)
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
func (sn *Node) Reserve(app *Application, ask *Allocation) error {
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
		log.Log(log.SchedNode).Debug("reservation creation failed unexpectedly",
			zap.String("nodeID", sn.NodeID),
			zap.Any("app", app),
			zap.Any("ask", ask))
		return fmt.Errorf("reservation creation failed app or ask are nil on nodeID %s", sn.NodeID)
	}
	// reservation must fit on the empty node
	if !sn.totalResource.FitIn(ask.GetAllocatedResource()) {
		log.Log(log.SchedNode).Debug("reservation does not fit on the node",
			zap.String("nodeID", sn.NodeID),
			zap.String("appID", app.ApplicationID),
			zap.String("ask", ask.GetAllocationKey()),
			zap.Stringer("allocationAsk", ask.GetAllocatedResource()))
		return fmt.Errorf("reservation does not fit on node %s, appID %s, ask %s", sn.NodeID, app.ApplicationID, ask.GetAllocatedResource().String())
	}
	sn.reservations[appReservation.getKey()] = appReservation
	sn.nodeEvents.SendReservedEvent(sn.NodeID, ask.GetAllocatedResource(), ask.GetAllocationKey())
	// reservation added successfully
	return nil
}

// unReserve the node for this application and ask combination
// If the reservation does not exist it returns 0 for reservations removed, if the reservation is removed it returns 1.
// The error is set if the reservation key cannot be generated.
func (sn *Node) unReserve(app *Application, ask *Allocation) (int, error) {
	defer sn.notifyListeners()
	sn.Lock()
	defer sn.Unlock()
	resKey := reservationKey(nil, app, ask)
	if resKey == "" {
		log.Log(log.SchedNode).Debug("unreserve reservation key create failed unexpectedly",
			zap.String("nodeID", sn.NodeID),
			zap.Any("app", app),
			zap.Any("ask", ask))
		return 0, fmt.Errorf("reservation key failed app or ask are nil on nodeID %s", sn.NodeID)
	}
	if _, ok := sn.reservations[resKey]; ok {
		delete(sn.reservations, resKey)
		sn.nodeEvents.SendUnreservedEvent(sn.NodeID, ask.GetAllocatedResource(), ask.GetAllocationKey())
		return 1, nil
	}
	// reservation was not found
	log.Log(log.SchedNode).Debug("reservation not found while removing from node",
		zap.String("nodeID", sn.NodeID),
		zap.String("appID", app.ApplicationID),
		zap.String("ask", ask.GetAllocationKey()))
	return 0, nil
}

// GetReservations returns all reservation made on this node
func (sn *Node) GetReservations() []*reservation {
	sn.Lock()
	defer sn.Unlock()
	var res []*reservation
	if len(sn.reservations) > 0 {
		for _, r := range sn.reservations {
			res = append(res, r)
		}
	}
	return res
}

// GetResourceUsageShares gets a map of name -> resource usages per type in shares (0 to 1). Can return NaN.
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

func (sn *Node) SendNodeAddedEvent() {
	sn.RLock()
	defer sn.RUnlock()
	sn.nodeEvents.SendNodeAddedEvent(sn.NodeID, sn.totalResource.Clone())
}

func (sn *Node) SendNodeRemovedEvent() {
	sn.nodeEvents.SendNodeRemovedEvent(sn.NodeID)
}
