/*
Copyright 2019 The Unity Scheduler Authors

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
    "github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "sync"
)

/* Related to nodes */
type NodeInfo struct {
    NodeId            string
    TotalResource     *resources.Resource
    allocatedResource *resources.Resource
    availableResource *resources.Resource

    // Fields for fast access
    Hostname  string
    Rackname  string
    Partition string

    // Private fields need protection
    attributes  map[string]string
    allocations map[string]*AllocationInfo
    lock        sync.RWMutex
}

func (m* NodeInfo) GetAllocatedResource() *resources.Resource {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.allocatedResource
}

func (m* NodeInfo) GetAvailableResource() *resources.Resource {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.availableResource
}

func (m *NodeInfo) GetAttribute(key string) string {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.attributes[key]
}

func (m *NodeInfo) GetAllocation(uuid string) *AllocationInfo {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.allocations[uuid]
}

func (m *NodeInfo) initializeAttribute(newAttributes map[string]string) error {
    m.lock.Lock()
    defer m.lock.Unlock()

    m.attributes = newAttributes

    m.refreshLocalVarsByAttributes()

    return nil
}

func (m *NodeInfo) refreshLocalVarsByAttributes() {
    m.Hostname = m.attributes[api.HOSTNAME]
    m.Rackname = m.attributes[api.RACKNAME]
    m.Partition = m.attributes[api.NODE_PARTITION]
}

func NewNodeInfo(proto *si.NewNodeInfo) (*NodeInfo, error) {
    m := &NodeInfo{
        NodeId:            proto.NodeId,
        TotalResource:     resources.NewResourceFromProto(proto.SchedulableResource),
        allocatedResource: resources.NewResource(),
        allocations:       make(map[string]*AllocationInfo, 0),
    }
    m.availableResource = m.TotalResource

    if err := m.initializeAttribute(proto.Attributes); err != nil {
        return nil, err
    }

    return m, nil
}

func (m *NodeInfo) AddAllocation(info *AllocationInfo) {
    m.lock.Lock()
    defer m.lock.Unlock()

    m.allocations[info.AllocationProto.Uuid] = info
    m.allocatedResource = resources.Add(m.allocatedResource, info.AllocatedResource)
    m.availableResource = resources.Sub(m.TotalResource, m.allocatedResource)
}

func (m *NodeInfo) RemoveAllocation(uuid string) *AllocationInfo {
    m.lock.Lock()
    defer m.lock.Unlock()

    var info *AllocationInfo
    if info = m.allocations[uuid]; info != nil {
        delete(m.allocations, uuid)
        m.allocatedResource = resources.Sub(m.allocatedResource, info.AllocatedResource)
        m.availableResource = resources.Sub(m.TotalResource, m.allocatedResource)
    }

    return info
}

func (m *NodeInfo) GetAllAllocations() []*AllocationInfo {
    m.lock.RLock()
    defer m.lock.RUnlock()

    arr := make([]*AllocationInfo, 0)
    for _, v := range m.allocations {
        arr = append(arr, v)
    }

    return arr
}
