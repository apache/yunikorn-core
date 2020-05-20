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
	"sync"

	"github.com/apache/incubator-yunikorn-core/pkg/api"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// The node structure used throughout the system
type NodeInfo struct {
	// Fields for fast access These fields are considered read only.
	// Values should only be set when creating a new node and never changed.
	NodeID    string
	Hostname  string
	Rackname  string
	Partition string

	// Private fields need protection
	attributes            map[string]string
	totalResource         *resources.Resource
	occupiedResource      *resources.Resource
	allocatedResource     *resources.Resource
	availableResource     *resources.Resource
	availableUpdateNeeded bool
	allocations           map[string]*AllocationInfo
	schedulable           bool

	sync.RWMutex
}

// Create a new node from the protocol object.
// The object can only be nil if the si.NewNodeInfo is nil, otherwise a valid object is returned.
func NewNodeInfo(proto *si.NewNodeInfo) *NodeInfo {
	if proto == nil {
		return nil
	}
	m := &NodeInfo{
		NodeID:                proto.NodeID,
		availableUpdateNeeded: true,
		totalResource:         resources.NewResourceFromProto(proto.SchedulableResource),
		allocatedResource:     resources.NewResource(),
		occupiedResource:      resources.NewResourceFromProto(proto.OccupiedResource),
		allocations:           make(map[string]*AllocationInfo),
		schedulable:           true,
	}
	// initialise available resources
	m.availableResource = resources.Sub(m.totalResource, m.occupiedResource)

	m.initializeAttribute(proto.Attributes)

	return m
}

// Set the attributes and fast access fields.
// Unlocked call: should only be called on create or from test code
func (ni *NodeInfo) initializeAttribute(newAttributes map[string]string) {
	ni.attributes = newAttributes

	ni.Hostname = ni.attributes[api.HostName]
	ni.Rackname = ni.attributes[api.RackName]
	ni.Partition = ni.attributes[api.NodePartition]
}

// Get an attribute by name. The most used attributes can be directly accessed via the
// fields: HostName, RackName and Partition.
// This is a lock free call. All attributes are considered read only
func (ni *NodeInfo) GetAttribute(key string) string {
	return ni.attributes[key]
}

// Return the currently allocated resource for the node.
// It returns a cloned object as we do not want to allow modifications to be made to the
// value of the node.
func (ni *NodeInfo) GetAllocatedResource() *resources.Resource {
	ni.RLock()
	defer ni.RUnlock()

	return ni.allocatedResource.Clone()
}

// Return the currently available resource for the node.
// It returns a cloned object as we do not want to allow modifications to be made to the
// value of the node.
func (ni *NodeInfo) GetAvailableResource() *resources.Resource {
	ni.RLock()
	defer ni.RUnlock()

	return ni.availableResource.Clone()
}

// Return the fact that the available resource has changed for the node.
// This should only be called by the SchedulingNode when it checks to update its cached value.
// This handles two cases:
// - removal of allocations via events
// - update of the node capacity via events
func (ni *NodeInfo) SyncAvailableResource() bool {
	ni.RLock()
	defer ni.RUnlock()
	if ni.availableUpdateNeeded {
		ni.availableUpdateNeeded = false
		return true
	}
	return false
}

func (ni *NodeInfo) GetCapacity() *resources.Resource {
	ni.RLock()
	defer ni.RUnlock()
	return ni.totalResource.Clone()
}

func (ni *NodeInfo) setCapacity(newCapacity *resources.Resource) {
	ni.Lock()
	defer ni.Unlock()
	if resources.Equals(ni.totalResource, newCapacity) {
		log.Logger().Debug("skip updating capacity, not changed")
		return
	}
	ni.totalResource = newCapacity
	ni.refreshAvailableResource()
}

func (ni *NodeInfo) GetOccupiedResource() *resources.Resource {
	ni.RLock()
	defer ni.RUnlock()
	return ni.occupiedResource.Clone()
}

func (ni *NodeInfo) setOccupiedResource(occupiedResource *resources.Resource) {
	ni.Lock()
	defer ni.Unlock()
	if resources.Equals(ni.occupiedResource, occupiedResource) {
		log.Logger().Debug("skip updating occupiedResource, not changed")
		return
	}
	ni.occupiedResource = occupiedResource
	ni.refreshAvailableResource()
}

// Return the allocation based on the uuid of the allocation.
// returns nil if the allocation is not found
func (ni *NodeInfo) GetAllocation(uuid string) *AllocationInfo {
	ni.RLock()
	defer ni.RUnlock()

	return ni.allocations[uuid]
}

// Check if the allocation fits int the nodes resources.
// unlocked call as the totalResource can not be changed
func (ni *NodeInfo) FitInNode(resRequest *resources.Resource) bool {
	return resources.FitIn(ni.totalResource, resRequest)
}

// Check if the allocation fits in the currently available resources.
func (ni *NodeInfo) canAllocate(resRequest *resources.Resource) bool {
	ni.RLock()
	defer ni.RUnlock()
	return resources.FitIn(ni.availableResource, resRequest)
}

// Add the allocation to the node.Used resources will increase available will decrease.
// This cannot fail. A nil AllocationInfo makes no changes.
func (ni *NodeInfo) AddAllocation(alloc *AllocationInfo) {
	if alloc == nil {
		return
	}
	ni.Lock()
	defer ni.Unlock()

	ni.allocations[alloc.AllocationProto.UUID] = alloc
	ni.allocatedResource.AddTo(alloc.AllocatedResource)
	ni.availableResource.SubFrom(alloc.AllocatedResource)
	ni.availableUpdateNeeded = true
}

// Remove the allocation to the node.
// Returns nil if the allocation was not found and no changes are made. If the allocation
// is found the AllocationInfo removed is returned. Used resources will decrease available
// will increase as per the allocation removed.
func (ni *NodeInfo) RemoveAllocation(uuid string) *AllocationInfo {
	ni.Lock()
	defer ni.Unlock()

	info := ni.allocations[uuid]
	if info != nil {
		delete(ni.allocations, uuid)
		ni.allocatedResource.SubFrom(info.AllocatedResource)
		ni.availableResource.AddTo(info.AllocatedResource)
		ni.availableUpdateNeeded = true
	}

	return info
}

// Get a copy of the allocations on this node
func (ni *NodeInfo) GetAllAllocations() []*AllocationInfo {
	ni.RLock()
	defer ni.RUnlock()

	arr := make([]*AllocationInfo, 0)
	for _, v := range ni.allocations {
		arr = append(arr, v)
	}

	return arr
}

// Set the node to unschedulable.
// This will cause the node to be skipped during the scheduling cycle.
// Visible for testing only
func (ni *NodeInfo) SetSchedulable(schedulable bool) {
	ni.Lock()
	defer ni.Unlock()
	ni.schedulable = schedulable
}

// Can this node be used in scheduling.
func (ni *NodeInfo) IsSchedulable() bool {
	ni.RLock()
	defer ni.RUnlock()
	return ni.schedulable
}

// refresh node available resource based on the latest allocated and occupied resources.
// this call assumes the caller already acquires the lock.
func (ni *NodeInfo) refreshAvailableResource() {
	ni.availableResource = ni.totalResource.Clone()
	ni.availableResource.SubFrom(ni.allocatedResource)
	ni.availableResource.SubFrom(ni.occupiedResource)
	ni.availableUpdateNeeded = true
}
