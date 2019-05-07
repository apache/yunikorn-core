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
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/commonevents"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/configs"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/schedulermetrics"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/webservice/dao"
    "strings"
    "sync"
)


/* Related to partitions */
type PartitionInfo struct {
    Name string
    Root *QueueInfo
    RMId string

    // Private fields need protection
    allocations   map[string]*AllocationInfo
    nodes         map[string]*NodeInfo
    applications  map[string]*ApplicationInfo
    state         partitionState
    isPreemptable bool
    // Total node resources
    totalPartitionResource *resources.Resource

    // Reference to scheduler metrics
    metrics schedulermetrics.CoreSchedulerMetrics

    lock sync.RWMutex
}

// Create a new partition from scratch based on a validated configuration.
// If teh configuration did not pass validation and is processed weird errors could occur.
func NewPartitionInfo(partition configs.PartitionConfig, rmId string) (*PartitionInfo, error) {
    p := &PartitionInfo{
        Name: partition.Name,
        RMId: rmId,
        state: active,
    }
    p.allocations = make(map[string]*AllocationInfo)
    p.nodes = make(map[string]*NodeInfo)
    p.applications = make(map[string]*ApplicationInfo)
    p.totalPartitionResource = resources.NewResource()
    glog.V(0).Infof("Creating partition %s for RM %s", p.Name, p.RMId)

    // Setup the queue structure: root first it should be the only queue at this level
    // Add the rest of the queue structure recursively
    queueConf := partition.Queues[0]
    root, err := NewManagedQueue(queueConf, nil)
    err = addQueueInfo(queueConf.Queues, root)
    if err != nil {
        return nil, err
    }
    p.Root = root
    glog.V(0).Infof("Added queue structure to partition %s", p.Name)

    // set preemption needed flag
    p.isPreemptable = partition.Preemption.Enabled

    //TODO add placement rules and users
    return p, nil
}

// Process the config structure and create a queue info tree for this partition
func addQueueInfo(conf []configs.QueueConfig, parent *QueueInfo) error {
    // create the queue at this level
    for _, queueConf := range conf {
        parent, err := NewManagedQueue(queueConf, parent)
        if err != nil {
            return err
        }
        // recursive create the queues below
        if len(queueConf.Queues) > 0 {
            err := addQueueInfo(queueConf.Queues, parent)
            if err != nil {
                return err
            }
        }
    }
    return nil
}

func (pi *PartitionInfo) GetTotalPartitionResource() *resources.Resource {
    pi.lock.RLock()
    defer pi.lock.RUnlock()

    return pi.totalPartitionResource
}

func (pi *PartitionInfo) NeedPreemption() bool {
    return pi.isPreemptable
}

func (pi *PartitionInfo) addNewNode(node *NodeInfo, existingAllocations []*si.Allocation) error {
    pi.lock.Lock()
    defer pi.lock.Unlock()

    if node := pi.nodes[node.NodeId]; node != nil {
        return errors.New(fmt.Sprintf("Same node=%s existed in partition=%s while adding new node, please double check.", node.NodeId, node.Partition))
    }

    pi.nodes[node.NodeId] = node

    // Add allocations
    if len(existingAllocations) > 0 {
        for _, alloc := range existingAllocations {
            if _, err := pi.addNewAllocationForNodeReportedAllocation(alloc); err != nil {
                return err
            }
        }
    }

    pi.totalPartitionResource = resources.Add(pi.totalPartitionResource, node.TotalResource)
    pi.Root.MaxResource = pi.totalPartitionResource

    return nil
}

func (pi *PartitionInfo) removeNode(nodeId string) {
    pi.lock.Lock()
    defer pi.lock.Unlock()

    node := pi.nodes[nodeId]
    if node == nil {
        glog.V(2).Infof("Trying to remove node=%s, but it is not part of cache", nodeId)
        return
    }

    for _, alloc := range node.GetAllAllocations() {
        var queue *QueueInfo = nil
        // get app and update
        if app := pi.applications[alloc.ApplicationId]; app != nil {
            if alloc := app.RemoveAllocation(alloc.AllocationProto.Uuid); alloc == nil {
                panic(fmt.Sprintf("Failed to get allocation=%s from app=%s when node=%s being removed, this shouldn't happen.", alloc.AllocationProto.Uuid, alloc.ApplicationId,
                    node.NodeId))
            }
            queue = app.LeafQueue
        } else {
            panic(fmt.Sprintf("Failed to getApp=%s when node=%s being removed, this shouldn't happen.", alloc.ApplicationId, node.NodeId))
        }

        // we should never have an error, cache is in an inconsistent state if this happens
        if queue != nil {
            if err := queue.DecAllocatedResource(alloc.AllocatedResource); err != nil {
                glog.V(0).Infof("Queue failed to release resources from application %s: %v", alloc.ApplicationId, err)
            }
        }

        glog.V(2).Infof("Remove allocation=%s from node=%s when node is removing", alloc.AllocationProto.Uuid, node.NodeId)
    }

    pi.totalPartitionResource = resources.Sub(pi.totalPartitionResource, node.TotalResource)
    pi.Root.MaxResource = pi.totalPartitionResource

    // Remove node from nodes
    glog.V(0).Infof("Node=%s is removed from cache", node.NodeId)
    delete(pi.nodes, nodeId)
}

func (pi *PartitionInfo) addNewApplication(info *ApplicationInfo) {
    pi.lock.Lock()
    defer pi.lock.Unlock()

    pi.applications[info.ApplicationId] = info
}

func (pi *PartitionInfo) getApplication(appId string) *ApplicationInfo {
    pi.lock.RLock()
    defer pi.lock.RUnlock()

    return pi.applications[appId]
}

// Visible by tests
func (pi *PartitionInfo) GetNode(nodeId string) *NodeInfo {
    pi.lock.RLock()
    defer pi.lock.RUnlock()

    return pi.nodes[nodeId]
}

// Returns removed allocations
func (pi *PartitionInfo) releaseAllocationsForApplication(toRelease *commonevents.ReleaseAllocation) []*AllocationInfo {
    pi.lock.Lock()
    defer pi.lock.Unlock()

    allocationsToRelease := make([]*AllocationInfo, 0)

    // First delete from app
    var queue *QueueInfo = nil
    if app := pi.applications[toRelease.ApplicationId]; app != nil {
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
        node := pi.nodes[alloc.AllocationProto.NodeId]
        if node == nil || node.GetAllocation(alloc.AllocationProto.Uuid) == nil {
            panic(fmt.Sprintf("Failed locate node=%s for allocation=%s", alloc.AllocationProto.NodeId, alloc.AllocationProto.Uuid))
        }
        node.RemoveAllocation(alloc.AllocationProto.Uuid)
        resources.AddTo(totalReleasedResource, alloc.AllocatedResource)
    }

    // this nil check is not really needed as we can only reach here with a queue set, IDE complains without this
    if queue != nil {
        // we should never have an error, cache is in an inconsistent state if this happens
        if err := queue.DecAllocatedResource(totalReleasedResource); err != nil {
            glog.V(0).Infof("Queue failed to release resources for application %s: %v", toRelease.ApplicationId, err)
        }
    }

    // Update global allocation list
    for _, alloc := range allocationsToRelease {
        delete(pi.allocations, alloc.AllocationProto.Uuid)

    }

    return allocationsToRelease
}

func (pi *PartitionInfo) addNewAllocation(alloc *commonevents.AllocationProposal, nodeReported bool) (*AllocationInfo, error) {
    pi.lock.Lock()
    defer pi.lock.Unlock()

    // Check if allocation violates any resource restriction, or allocate on a
    // non-existent applications or nodes.
    var node *NodeInfo
    var app *ApplicationInfo
    var queue *QueueInfo
    var ok bool

    if node, ok = pi.nodes[alloc.NodeId]; !ok {
        pi.metrics.IncScheduledAllocationErrors()
        return nil, errors.New(fmt.Sprintf("Failed to find node=%s", alloc.NodeId))
    }

    if app, ok = pi.applications[alloc.ApplicationId]; !ok {
        pi.metrics.IncScheduledAllocationErrors()
        return nil, errors.New(fmt.Sprintf("Failed to find app=%s", alloc.ApplicationId))
    }

    if queue = pi.getQueue(alloc.QueueName); queue == nil {
        pi.metrics.IncScheduledAllocationErrors()
        return nil, errors.New(fmt.Sprintf("Failed to find queue=%s", alloc.QueueName))
    }

    // If new allocation go beyond node's total resource?
    newNodeResource := resources.Add(node.GetAllocatedResource(), alloc.AllocatedResource)
    if !resources.FitIn(node.TotalResource, newNodeResource) {
        pi.metrics.IncScheduledAllocationFailures()
        return nil, errors.New(fmt.Sprintf("Cannot allocate resource=[%s] from app=%s on "+
            "node=%s because resource exceeded total available, allocated+new=%s, total=%s",
            alloc.AllocatedResource, alloc.ApplicationId, node.NodeId, newNodeResource, node.TotalResource))
    }

    // If new allocation go beyond any of queue's max resource? Only check if when it is allocated instead of node reported.
    if !nodeReported {
        if err := queue.IncAllocatedResource(alloc.AllocatedResource); err != nil {
            pi.metrics.IncScheduledAllocationFailures()
            return nil, fmt.Errorf("Cannot allocate resource from application %s: %v ",
                alloc.ApplicationId, err)
        }
    }

    // Start allocation
    allocationUuid := pi.GetNewAllocationUuid()
    allocation := NewAllocationInfo(allocationUuid, alloc)

    node.AddAllocation(allocation)

    app.AddAllocation(allocation)

    pi.allocations[allocation.AllocationProto.Uuid] = allocation

    pi.metrics.IncScheduledAllocationSuccesses()

    return allocation, nil

}

func (pi *PartitionInfo) addNewAllocationForNodeReportedAllocation(allocation *si.Allocation) (*AllocationInfo, error) {
    return pi.addNewAllocation(&commonevents.AllocationProposal{
        NodeId:            allocation.NodeId,
        ApplicationId:     allocation.ApplicationId,
        QueueName:         allocation.QueueName,
        AllocatedResource: resources.NewResourceFromProto(allocation.ResourcePerAlloc),
        AllocationKey:     allocation.AllocationKey,
        Tags:              allocation.AllocationTags,
        Priority:          allocation.Priority,
    }, true)
}

func (pi *PartitionInfo) addNewAllocationForSchedulingAllocation(allocationProposals []*commonevents.AllocationProposal) (*AllocationInfo, error) {
    if len(allocationProposals) > 1 {
        return nil, errors.New(fmt.Sprintf("Allocation=%v, Now we only support allocate one allocation at one time", allocationProposals))
    }

    return pi.addNewAllocation(allocationProposals[0], false)
}

func (pi *PartitionInfo) GetNewAllocationUuid() string {
    // Retry to make sure uuid is correct
    for {
        allocationUuid := uuid.NewV4().String()
        if pi.allocations[allocationUuid] == nil {
            return allocationUuid
        }
    }
}

func (pi *PartitionInfo) RemoveApplication(appId string) (*ApplicationInfo, []*AllocationInfo) {
    pi.lock.Lock()
    defer pi.lock.Unlock()

    if app := pi.applications[appId]; app != nil {
        // Remove app from cache
        delete(pi.applications, appId)

        // Total allocated
        totalAppAllocated := app.GetAllocatedResource()

        // Get all allocations
        allocations := app.CleanupAllAllocations()

        // No allocations of the app, just return
        if len(allocations) == 0 {
            return app, allocations
        }

        for _, alloc := range allocations {
            uuid := alloc.AllocationProto.Uuid

            // Remove from partition cache
            if globalAlloc := pi.allocations[uuid]; globalAlloc == nil {
                panic(fmt.Sprintf("Failed to find allocation=%s from global cache", uuid))
            } else {
                delete(pi.allocations, uuid)
            }

            // Remove from node
            node := pi.nodes[alloc.AllocationProto.NodeId]
            if node == nil {
                panic(fmt.Sprintf("Failed to find node=%s for allocation=%s", alloc.AllocationProto.NodeId, uuid))
            }
            if nodeAlloc := node.RemoveAllocation(uuid); nodeAlloc == nil {
                panic(fmt.Sprintf("Failed to find allocation=%s from node=%s", uuid, alloc.AllocationProto.NodeId))
            }
        }

        // we should never have an error, cache is in an inconsistent state if this happens
        queue := app.LeafQueue
        if queue != nil {
            if err := queue.DecAllocatedResource(totalAppAllocated); err != nil {
                glog.V(0).Infof("Queue failed to release resources from application %s: %v", app.ApplicationId, err)
            }
        }

        glog.V(2).Infof("Removed app=%s from partition=%s, total-allocated=%s", app.ApplicationId, app.Partition, totalAppAllocated)

        return app, allocations
    }

    return nil, make([]*AllocationInfo, 0)
}

func (pi *PartitionInfo) CopyNodeInfos() []*NodeInfo {
    pi.lock.RLock()
    defer pi.lock.RUnlock()

    out := make([]*NodeInfo, len(pi.nodes))

    var i = 0
    for _, v := range pi.nodes {
        out[i] = v
        i++
    }

    return out
}

func checkAndSetResource(resource *resources.Resource) string {
    if resource != nil {
        return strings.Trim(resource.String(), "map")
    }
    return ""
}

// TODO fix this:
// should only return one element, only a root queue
// remove hard coded values and unknown AbsUsedCapacity
// map status to the draining flag
func (pi *PartitionInfo) GetQueueInfos() []dao.QueueDAOInfo {
    pi.lock.RLock()
    defer pi.lock.RUnlock()

    var queueInfos []dao.QueueDAOInfo

    info := dao.QueueDAOInfo{}
    info.QueueName = pi.Root.Name
    info.Status = "RUNNING"
    info.Capacities = dao.QueueCapacity{
        Capacity:     checkAndSetResource(pi.Root.GuaranteedResource),
        MaxCapacity:  checkAndSetResource(pi.Root.MaxResource),
        UsedCapacity: checkAndSetResource(pi.Root.GetAllocatedResource()),
        AbsUsedCapacity: "20",
    }
    info.ChildQueues = GetChildQueueInfos(pi.Root)
    queueInfos = append(queueInfos, info)

    return queueInfos
}

// TODO fix this:
// should only return one element, only a root queue
// remove hard coded values and unknown AbsUsedCapacity
// map status to the draining flag
func GetChildQueueInfos(info *QueueInfo) []dao.QueueDAOInfo {
    var infos []dao.QueueDAOInfo
    for _, v := range info.children {
        queue := dao.QueueDAOInfo{}
        queue.QueueName = v.Name
        queue.Status = "RUNNING"
        queue.Capacities = dao.QueueCapacity{
            Capacity:     checkAndSetResource(v.GuaranteedResource),
            MaxCapacity:  checkAndSetResource(v.MaxResource),
            UsedCapacity: checkAndSetResource(v.GetAllocatedResource()),
            AbsUsedCapacity: "20",
        }
        queue.ChildQueues = GetChildQueueInfos(v)
        infos = append(infos, queue)
    }

    return infos
}

func (pi *PartitionInfo) GetTotalApplicationCount() int {
    pi.lock.RLock()
    defer pi.lock.RUnlock()
    return len(pi.applications)
}

func (pi *PartitionInfo) GetTotalAllocationCount() int {
    pi.lock.RLock()
    defer pi.lock.RUnlock()
    return len(pi.allocations)
}

func (pi *PartitionInfo) GetTotalNodeCount() int {
    pi.lock.RLock()
    defer pi.lock.RUnlock()
    return len(pi.nodes)
}

func (pi *PartitionInfo) GetApplications() []*ApplicationInfo {
    pi.lock.RLock()
    defer pi.lock.RUnlock()
    var appList []*ApplicationInfo
    for _, app := range pi.applications {
        appList = append(appList, app)
    }
    return appList
}

// Get the queue from the structure based on the fully qualified name.
// The name is not syntax checked and must be valid.
// Returns nil if the queue is not found otherwise the queue object.
//
// NOTE: this is a lock free call. It should only be called holding a PartitionInfo lock or
// a ClusterInfo lock. If access outside is needed a locked version must be introduced.
func (pi *PartitionInfo) getQueue(name string) *QueueInfo {
    // start at the root
    queue := pi.Root
    part := strings.Split(strings.ToLower(name), DOT)
    // short circuit the root queue
    if len(part) == 1 {
        return queue
    }
    // walk over the parts going down towards the requested queue
    for i := 1;  i < len(part); i++ {
        // if child not found break out and return
        if queue = queue.children[part[i]]; queue == nil {
            break
        }
    }
    return queue
}

// Update the queues in the partition based on the reloaded and checked config
func (pi *PartitionInfo) updatePartitionDetails(partition configs.PartitionConfig) error {
    pi.lock.RLock()
    defer pi.lock.RUnlock()
    // update preemption needed flag
    pi.isPreemptable = partition.Preemption.Enabled
    // start at the root: there is only one queue
    queueConf := partition.Queues[0]
    root := pi.getQueue(queueConf.Name)
    err := root.updateQueueProps(queueConf)
    if err != nil {
        return err
    }
    return pi.updateQueues(queueConf.Queues, root)
}

// Update the passed in queues and then do this recursively for the children
func (pi *PartitionInfo) updateQueues(config []configs.QueueConfig, parent *QueueInfo) error {
    // get the name of the passed in queue
    parentPath := parent.GetQueuePath() + DOT
    // keep track of which children we have updated
    visited := map[string]bool{}
    // walk over the queues recursively
    for _, queueConfig := range config {
        pathName :=  parentPath + queueConfig.Name
        queue := pi.getQueue(pathName)
        var err error
        if queue == nil {
            queue, err = NewManagedQueue(queueConfig, parent)
        } else {
            err = queue.updateQueueProps(queueConfig)
        }
        if err != nil {
            return  err
        }
        err = pi.updateQueues(queueConfig.Queues, queue)
        if err != nil {
            return  err
        }
        visited[queueConfig.Name] = true
    }
    // remove all children that were not visited
    for childName, childQueue := range parent.children {
        if !visited[childName] {
            childQueue.MarkQueueForRemoval()
        }
    }
    return nil
}

// Partition states for a partition indirectly leveraged by the scheduler
// - new application can only be assigned in a running state
// - new allocation should only be made in a running state
// - in a stopped state nothing may change on the partition
// NOTE: states could be expanded and should not be referenced directly outside the PartitionInfo
const (
    active partitionState = iota
    deleted
)

type partitionState int

// Is the partition running and can be used in scheduling
func (pi *PartitionInfo) IsRunning() bool {
    return pi.state == active
}

// Is the partition
func (pi *PartitionInfo) IsStopped() bool {
    return pi.state == deleted
}
