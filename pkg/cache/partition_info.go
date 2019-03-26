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
    "errors"
    "fmt"
    "github.com/golang/glog"
    "github.com/satori/go.uuid"
    "github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/cache/cacheevent"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/commonevents"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/webservice/dao"
    "strings"
    "sync"
)

const (
    DEFAULT_PARTITION = "default"
)

/* Related to partitions */
type PartitionInfo struct {
    Name string
    Root *QueueInfo
    RMId string

    // Private fields need protection
    queues       map[string]*QueueInfo // all children
    allocations  map[string]*AllocationInfo
    nodes        map[string]*NodeInfo
    applications map[string]*ApplicationInfo

    // Total node resources
    TotalPartitionResource *resources.Resource

    lock sync.RWMutex
}

func newPartitionInfo(name string) *PartitionInfo {
    p := &PartitionInfo{Name: name}
    p.allocations = make(map[string]*AllocationInfo)
    p.queues = make(map[string]*QueueInfo)
    p.nodes = make(map[string]*NodeInfo)
    p.applications = make(map[string]*ApplicationInfo)
    p.TotalPartitionResource = resources.NewResource()

    return p
}

func (m *PartitionInfo) addNewNode(node *NodeInfo, existingAllocations []*si.Allocation) error {
    m.lock.Lock()
    defer m.lock.Unlock()

    if node := m.nodes[node.NodeId]; node != nil {
        return errors.New(fmt.Sprintf("Same node=%s existed in partition=%s while adding new node, please double check.", node.NodeId, node.Partition))
    }

    m.nodes[node.NodeId] = node

    // Add allocations
    if len(existingAllocations) > 0 {
        for _, alloc := range existingAllocations {
            if _, err := m.addNewAllocationForNodeReportedAllocation(alloc); err != nil {
                return err
            }
        }
    }

    m.TotalPartitionResource = resources.Add(m.TotalPartitionResource, node.TotalResource)
    m.Root.MaxResource = m.TotalPartitionResource

    return nil
}

func (m *PartitionInfo) removeNode(nodeId string) {
    m.lock.Lock()
    defer m.lock.Unlock()

    node := m.nodes[nodeId]
    if node == nil {
        glog.V(2).Infof("Trying to remove node=%s, but it is not part of cache", nodeId)
        return
    }

    for _, alloc := range node.GetAllAllocations() {
        var queue *QueueInfo = nil
        // get app and update
        if app := m.applications[alloc.ApplicationId]; app != nil {
            if alloc := app.RemoveAllocation(alloc.AllocationProto.Uuid); alloc == nil {
                panic(fmt.Sprintf("Failed to get allocation=%s from app=%s when node=%s being removed, this shouldn't happen.", alloc.AllocationProto.Uuid, alloc.ApplicationId,
                    node.NodeId))
            }
            queue = app.LeafQueue
        } else {
            panic(fmt.Sprintf("Failed to getApp=%s when node=%s being removed, this shouldn't happen.", alloc.ApplicationId, node.NodeId))
        }

        // get queue and update
        for queue != nil {
            queue.DecAllocatedResource(alloc.AllocatedResource)
            queue = queue.Parent
        }

        glog.V(2).Infof("Remove allocation=%s from node=%s when node is removing", alloc.AllocationProto.Uuid, node.NodeId)
    }

    m.TotalPartitionResource = resources.Sub(m.TotalPartitionResource, node.TotalResource)
    m.Root.MaxResource = m.TotalPartitionResource

    // Remove node from nodes
    glog.V(0).Infof("Node=%s is removed from cache", node.NodeId)
    delete(m.nodes, nodeId)
}

func (m *PartitionInfo) addNewApplication(info *ApplicationInfo) {
    m.lock.Lock()
    defer m.lock.Unlock()

    m.applications[info.ApplicationId] = info
}

func (m *PartitionInfo) getApplication(appId string) *ApplicationInfo {
    m.lock.RLock()
    m.lock.RUnlock()

    return m.applications[appId]
}

// Visible by tests
func (m *PartitionInfo) GetNode(nodeId string) *NodeInfo {
    m.lock.RLock()
    m.lock.RUnlock()

    return m.nodes[nodeId]
}

// Returns removed allocations
func (m *PartitionInfo) releaseAllocationsForApplication(toRelease *cacheevent.ReleaseAllocation) []*AllocationInfo {
    m.lock.Lock()
    m.lock.Unlock()

    allocationsToRelease := make([]*AllocationInfo, 0)

    // First delete from app
    var queue *QueueInfo = nil
    if app := m.applications[toRelease.ApplicationId]; app != nil {
        // when uuid not specified, remove all allocations from the app
        if toRelease.Uuid == "" {
            for _, alloc := range app.CleanupAllAllocations() {
                allocationsToRelease = append(allocationsToRelease, alloc)
            }
        } else {
            if alloc := app.RemoveAllocation(toRelease.Uuid); alloc != nil {
                allocationsToRelease = append(allocationsToRelease, alloc)
            }
        }
        queue = app.LeafQueue
    }

    if len(allocationsToRelease) == 0 {
        return allocationsToRelease
    }

    // for each allocations to release, update node.
    totalReleasedResource := resources.NewResource()

    for _, alloc := range allocationsToRelease {
        // remove from nodes
        node := m.nodes[alloc.AllocationProto.NodeId]
        if node == nil || node.GetAllocation(alloc.AllocationProto.Uuid) == nil {
            panic(fmt.Sprintf("Failed locate node=%s for allocation=%s", alloc.AllocationProto.NodeId, alloc.AllocationProto.Uuid))
        }
        node.RemoveAllocation(alloc.AllocationProto.Uuid)
        resources.AddTo(totalReleasedResource, alloc.AllocatedResource)
    }

    // Update queues
    for queue != nil {
        queue.DecAllocatedResource(totalReleasedResource)
        queue = queue.Parent
    }

    // Update global allocation list
    for _, alloc := range allocationsToRelease {
        delete(m.allocations, alloc.AllocationProto.Uuid)

    }

    return allocationsToRelease
}

func (m *PartitionInfo) addNewAllocation(alloc *commonevents.AllocationProposal) (*AllocationInfo, error) {
    m.lock.Lock()
    defer m.lock.Unlock()

    // Check if allocation violates any resource restriction, or allocate on a
    // non-existent applications or nodes.
    var node *NodeInfo
    var app *ApplicationInfo
    var queue *QueueInfo
    var ok bool

    if node, ok = m.nodes[alloc.NodeId]; !ok {
        return nil, errors.New(fmt.Sprintf("Failed to find node=%s", alloc.NodeId))
    }

    if app, ok = m.applications[alloc.ApplicationId]; !ok {
        return nil, errors.New(fmt.Sprintf("Failed to find app=%s", alloc.ApplicationId))
    }

    if queue = m.getQueue(alloc.QueueName); queue == nil {
        return nil, errors.New(fmt.Sprintf("Failed to find queue=%s", alloc.QueueName))
    }

    // If new allocation go beyond node's total resource?
    newNodeResource := resources.Add(node.AllocatedResource, alloc.AllocatedResource)
    if !resources.FitIn(node.TotalResource, newNodeResource) {
        return nil, errors.New(fmt.Sprintf("Cannot allocate resource=[%s] from app=%s on "+
            "node=%s because resource exceeded total available, allocated+new=%s, total=%s",
            alloc.AllocatedResource, alloc.ApplicationId, node.NodeId, newNodeResource, node.TotalResource))
    }

    // If new allocation go beyond any of queue's max resource?
    q := queue
    for q != nil {
        newQueueResource := resources.Add(q.AllocatedResource, alloc.AllocatedResource)
        if q.MaxResource != nil && !resources.FitIn(q.MaxResource, newQueueResource) {
            return nil, errors.New(fmt.Sprintf("Cannot allocate resource=[%s] from app=%s on "+
                "queue=%s because resource exceeded total available, allocated+new=%s, total=%s",
                alloc.AllocatedResource, alloc.ApplicationId, queue.Name, newQueueResource, queue.MaxResource))
        }
        q = q.Parent
    }

    // Start allocation
    allocationUuid := m.GetNewAllocationUuid()
    allocation := NewAllocationInfo(allocationUuid, alloc)
    q = queue
    for q != nil {
        q.IncAllocatedResource(allocation.AllocatedResource)
        q = q.Parent
    }

    node.AddAllocation(allocation)

    app.AddAllocation(allocation)

    m.allocations[allocation.AllocationProto.Uuid] = allocation

    return allocation, nil

}

func (m *PartitionInfo) addNewAllocationForNodeReportedAllocation(allocation *si.Allocation) (*AllocationInfo, error) {
    return m.addNewAllocation(&commonevents.AllocationProposal{
        NodeId:            allocation.NodeId,
        ApplicationId:     allocation.ApplicationId,
        QueueName:         allocation.QueueName,
        AllocatedResource: resources.NewResourceFromProto(allocation.ResourcePerAlloc),
        AllocationKey:     allocation.AllocationKey,
        Tags:              allocation.AllocationTags,
        Priority:          allocation.Priority,
    })
}

func (m *PartitionInfo) addNewAllocationForSchedulingAllocation(proposal *commonevents.AllocationProposal) (*AllocationInfo, error) {
    return m.addNewAllocation(proposal)
}

func (m *PartitionInfo) GetNewAllocationUuid() string {
    // Retry to make sure uuid is correct
    for {
        allocationUuid := uuid.NewV4().String()
        if m.allocations[allocationUuid] == nil {
            return allocationUuid
        }
    }
}

func (m *PartitionInfo) RemoveApplication(appId string) (*ApplicationInfo, []*AllocationInfo) {
    m.lock.Lock()
    defer m.lock.Unlock()

    if app := m.applications[appId]; app != nil {
        // Remove app from cache
        delete(m.applications, appId)

        // Total allocated
        totalAppAllocated := app.AllocatedResource

        // Get all allocations
        allocations := app.CleanupAllAllocations()

        // No allocations of the app, just return
        if len(allocations) == 0 {
            return app, allocations
        }

        for _, alloc := range allocations {
            uuid := alloc.AllocationProto.Uuid

            // Remove from partition cache
            if globalAlloc := m.allocations[uuid]; globalAlloc == nil {
                panic(fmt.Sprintf("Failed to find allocation=%s from global cache", uuid))
            } else {
                delete(m.allocations, uuid)
            }

            // Remove from node
            node := m.nodes[alloc.AllocationProto.NodeId]
            if node == nil {
                panic(fmt.Sprintf("Failed to find node=%s for allocation=%s", alloc.AllocationProto.NodeId, uuid))
            }
            if nodeAlloc := node.RemoveAllocation(uuid); nodeAlloc == nil {
                panic(fmt.Sprintf("Failed to find allocation=%s from node=%s", uuid, alloc.AllocationProto.NodeId))
            }
        }

        // Update queues
        queue := app.LeafQueue
        for queue != nil {
            queue.AllocatedResource = resources.Sub(queue.AllocatedResource, totalAppAllocated)
            queue = queue.Parent
        }

        glog.V(2).Infof("Removed app=%s from partition=%s, total-allocated=%s", app.ApplicationId, app.Partition, totalAppAllocated)

        return app, allocations
    }

    return nil, make([]*AllocationInfo, 0)
}

func (m *PartitionInfo) CopyNodeInfos() []*NodeInfo {
    m.lock.RLock()
    defer m.lock.RUnlock()

    out := make([]*NodeInfo, len(m.nodes))

    var i = 0
    for _, v := range m.nodes {
        out[i] = v
        i++
    }

    return out
}

func (m *PartitionInfo) GetQueueInfos() []dao.QueueDAOInfo {
    m.lock.RLock()
    defer m.lock.RUnlock()

    //out := make([]*QueueInfo, len(m.queues))
    var queueInfos []dao.QueueDAOInfo

    info := dao.QueueDAOInfo{}
    info.QueueName = m.Root.Name
    info.Status = "RUNNING"
    info.Capacities = dao.QueueCapacity{
        Capacity:     strings.Trim(m.Root.GuaranteedResource.String(), "map"),
        MaxCapacity:  strings.Trim(m.Root.GuaranteedResource.String(), "map"),
        UsedCapacity: strings.Trim(m.Root.AllocatedResource.String(), "map"),
        AbsUsedCapacity: "20",
    }
    info.ChildQueues = GetChildQueueInfos(m.Root)
    queueInfos = append(queueInfos, info)

    return queueInfos
}

func GetChildQueueInfos(info *QueueInfo) []dao.QueueDAOInfo {
    var infos []dao.QueueDAOInfo
    for _, v := range info.children {
        queue := dao.QueueDAOInfo{}
        queue.QueueName = v.Name
        queue.Status = "RUNNING"
        queue.Capacities = dao.QueueCapacity{
            Capacity:     strings.Trim(v.GuaranteedResource.String(), "map"),
            MaxCapacity:  strings.Trim(v.GuaranteedResource.String(), "map"),
            UsedCapacity: strings.Trim(v.AllocatedResource.String(), "map"),
            AbsUsedCapacity: "20",
        }
        queue.ChildQueues = GetChildQueueInfos(v)
        infos = append(infos, queue)
    }

    return infos
}

func (m *PartitionInfo) GetTotalApplicationCount() int {
    m.lock.RLock()
    defer m.lock.RUnlock()
    return len(m.applications)
}

func (m *PartitionInfo) GetTotalAllocationCount() int {
    m.lock.RLock()
    defer m.lock.RUnlock()
    return len(m.allocations)
}

func (m *PartitionInfo) GetTotalNodeCount() int {
    m.lock.RLock()
    defer m.lock.RUnlock()
    return len(m.nodes)
}

func (m *PartitionInfo) GetApplications() []*ApplicationInfo {
    var appList []*ApplicationInfo
    for _, app := range m.applications {
        appList = append(appList, app)
    }
    return appList
}

func (m *PartitionInfo) initQueues(queue *QueueInfo) {
    if queue != nil {
        m.queues[queue.FullQualifiedPath] = queue
        if queue.children != nil && len(queue.children) > 0 {
            for _, value := range queue.children {
                m.initQueues(value)
            }
        }
    }
}

func (m *PartitionInfo) getQueue(name string) *QueueInfo {
    return m.queues[name]
}