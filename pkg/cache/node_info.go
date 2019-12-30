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

package cache

import (
	"sync"

	"github.com/cloudera/yunikorn-core/pkg/api"
	"github.com/cloudera/yunikorn-core/pkg/common/resources"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
)

// The node structure used throughout the system
type NodeInfo struct {
	// Fields for fast access These fields are considered read only.
	// Values should only be set when creating a new node and never changed.
	NodeId        string
	Hostname      string
	Rackname      string
	Partition     string
	TotalResource *resources.Resource

	// Private fields need protection
	attributes        map[string]string
	allocatedResource *resources.Resource
	availableResource *resources.Resource
	allocations       map[string]*AllocationInfo
	schedulable       bool

	lock sync.RWMutex
}

// Create a new node from the protocol object.
// The object can only be nil if the si.NewNodeInfo is nil, otherwise a valid object is returned.
func NewNodeInfo(proto *si.NewNodeInfo) *NodeInfo {
	if proto == nil {
		return nil
	}
	m := &NodeInfo{
		NodeId:            proto.NodeId,
		TotalResource:     resources.NewResourceFromProto(proto.SchedulableResource),
		allocatedResource: resources.NewResource(),
		allocations:       make(map[string]*AllocationInfo),
		schedulable:       true,
	}
	m.availableResource = m.TotalResource.Clone()

	m.initializeAttribute(proto.Attributes)

	return m
}

// Set the attributes and fast access fields.
// Unlocked call: should only be called on create or from test code
func (ni *NodeInfo) initializeAttribute(newAttributes map[string]string) {
	ni.attributes = newAttributes

	ni.Hostname = ni.attributes[api.HOSTNAME]
	ni.Rackname = ni.attributes[api.RACKNAME]
	ni.Partition = ni.attributes[api.NODE_PARTITION]
}

// Get an attribute by name. The most used attributes can be directly accessed via the
// fields: Hostname, Rackname and Partition.
// This is a lock free call. All attributes are considered read only
func (ni *NodeInfo) GetAttribute(key string) string {
	return ni.attributes[key]
}

// Return the currently allocated resource for the node.
// It returns a cloned object as we do not want to allow modifications to be made to the
// value of the node.
func (ni *NodeInfo) GetAllocatedResource() *resources.Resource {
	ni.lock.RLock()
	defer ni.lock.RUnlock()

	return ni.allocatedResource.Clone()
}

// Return the currently available resource for the node.
// It returns a cloned object as we do not want to allow modifications to be made to the
// value of the node.
func (ni *NodeInfo) GetAvailableResource() *resources.Resource {
	ni.lock.RLock()
	defer ni.lock.RUnlock()

	return ni.availableResource.Clone()
}

// Return the allocation based on the uuid of the allocation.
// returns nil if the allocation is not found
func (ni *NodeInfo) GetAllocation(uuid string) *AllocationInfo {
	ni.lock.RLock()
	defer ni.lock.RUnlock()

	return ni.allocations[uuid]
}

// Check if the allocation fits in the currently available resources.
func (ni *NodeInfo) CanAllocate(resRequest *resources.Resource) bool {
	ni.lock.RLock()
	defer ni.lock.RUnlock()
	return resources.FitIn(ni.availableResource, resRequest)
}

// Add the allocation to the node.Used resources will increase available will decrease.
// This cannot fail. A nil AllocationInfo makes no changes.
func (ni *NodeInfo) AddAllocation(alloc *AllocationInfo) {
	if alloc == nil {
		return
	}
	ni.lock.Lock()
	defer ni.lock.Unlock()

	ni.allocations[alloc.AllocationProto.Uuid] = alloc
	ni.allocatedResource.AddTo(alloc.AllocatedResource)
	ni.availableResource.SubFrom(alloc.AllocatedResource)
}

// Remove the allocation to the node.
// Returns nil if the allocation was not found and no changes are made. If the allocation
// is found the AllocationInfo removed is returned. Used resources will decrease available
// will increase as per the allocation found.
func (ni *NodeInfo) RemoveAllocation(uuid string) *AllocationInfo {
	ni.lock.Lock()
	defer ni.lock.Unlock()

	info := ni.allocations[uuid]
	if info != nil {
		delete(ni.allocations, uuid)
		ni.allocatedResource.SubFrom(info.AllocatedResource)
		ni.availableResource.AddTo(info.AllocatedResource)
	}

	return info
}

// Get a copy of the allocations on this node
func (ni *NodeInfo) GetAllAllocations() []*AllocationInfo {
	ni.lock.RLock()
	defer ni.lock.RUnlock()

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
	ni.lock.Lock()
	defer ni.lock.Unlock()
	ni.schedulable = schedulable
}

// Can this node be used in scheduling.
func (ni *NodeInfo) IsSchedulable() bool {
	ni.lock.RLock()
	defer ni.lock.RUnlock()
	return ni.schedulable
}
