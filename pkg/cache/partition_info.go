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
    allocations   map[string]*AllocationInfo   // allocations
    nodes         map[string]*NodeInfo         // nodes registered
    applications  map[string]*ApplicationInfo  // the application list
    state         partitionState               // state of the partition for scheduling
    isPreemptable bool                         // can allocations be preempted
    lock          sync.RWMutex                 // lock for updating the partition
    totalPartitionResource *resources.Resource    // Total node resources
    metrics schedulermetrics.CoreSchedulerMetrics // Reference to scheduler metrics
    partitionConfig *configs.PartitionConfig      // Partition Configs
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

// Add a new node to the partition.
// If a partition is not active a new node can not be added as the partition is about to be removed.
// A new node must be added to the partition before the existing allocations can be processed. This
// means that the partition is updated before the allocations. Failure to add all reported allocations will
// cause the node to be rejected
func (pi *PartitionInfo) addNewNode(node *NodeInfo, existingAllocations []*si.Allocation) error {
    pi.lock.Lock()
    defer pi.lock.Unlock()

    glog.V(4).Infof("Trying to add node %s to partition %s", node.NodeId, pi.Name)

    if pi.IsStopped() {
        return fmt.Errorf("partition %s is stopped cannot add a new node %s", pi.Name, node.NodeId)
    }

    if node := pi.nodes[node.NodeId]; node != nil {
        return fmt.Errorf("partition %s has an existing node %s, node name must be unique", pi.Name, node.NodeId)
    }

    // update the resources available in the cluster
    pi.totalPartitionResource = resources.Add(pi.totalPartitionResource, node.TotalResource)
    pi.Root.MaxResource = pi.totalPartitionResource

    // Node is added to the system to allow processing of the allocations
    pi.nodes[node.NodeId] = node

    // Add allocations that exist on the node when added
    if len(existingAllocations) > 0 {
        glog.V(4).Infof("Adding existing allocations for node %s (%d total)", node.NodeId, len(existingAllocations))
        for _, alloc := range existingAllocations {
            if _, err := pi.addNodeReportedAllocations(alloc); err != nil {
                glog.V(4).Infof("Failure adding existing allocations for node %s (%d total), rejecting node", node.NodeId, len(existingAllocations))
                pi.removeNodeInternal(node.NodeId)
                pi.metrics.IncFailedNodes()
                return err
            }
        }
    }
    // Node is added update the metrics
    pi.metrics.IncActiveNodes()
    glog.V(4).Infof("Added node %s to partition %s", node.NodeId, pi.Name)

    return nil
}

// Wrapper function to convert the reported allocation into an AllocationProposal.
// Used when a new node is added to the partition which already reports existing allocations.
func (pi *PartitionInfo) addNodeReportedAllocations(allocation *si.Allocation) (*AllocationInfo, error) {
    return pi.addNewAllocationInternal(&commonevents.AllocationProposal{
        NodeId:            allocation.NodeId,
        ApplicationId:     allocation.ApplicationId,
        QueueName:         allocation.QueueName,
        AllocatedResource: resources.NewResourceFromProto(allocation.ResourcePerAlloc),
        AllocationKey:     allocation.AllocationKey,
        Tags:              allocation.AllocationTags,
        Priority:          allocation.Priority,
    }, true)
}

// Remove a node from the partition.
// This locks the partition and calls the internal unlocked version.
func (pi *PartitionInfo) removeNode(nodeId string) {
    pi.lock.Lock()
    defer pi.lock.Unlock()

    pi.removeNodeInternal(nodeId)
}

// Remove a node from the partition.
//
// NOTE: this is a lock free call. It should only be called holding the PartitionInfo lock.
// If access outside is needed a locked version must used, see removeNode
func (pi *PartitionInfo) removeNodeInternal(nodeId string) {
    glog.V(4).Infof("Trying to remove node %s from partition %s", nodeId, pi.Name)

    node := pi.nodes[nodeId]
    if node == nil {
        glog.V(0).Infof("Tried to remove node %s from partition %s: node was not found", nodeId, pi.Name)
        return
    }

    // walk over all allocations still registered for this node
    for _, alloc := range node.GetAllAllocations() {
        var queue *QueueInfo = nil
        allocID := alloc.AllocationProto.Uuid
        // since we are not locking the node and or application we could have had an update while processing
        if app := pi.applications[alloc.ApplicationId]; app != nil {
            // check allocations on the node
           if alloc := app.RemoveAllocation(allocID); alloc == nil {
                glog.V(0).Infof("Allocation %s not found for application %s while removing node %s, skipping",
                    allocID, app.ApplicationId, node.NodeId)
                continue
            }
            queue = app.LeafQueue
        } else {
            glog.V(0).Infof("Application %s not found while removing node %s, skipping.", alloc.ApplicationId, node.NodeId)
            continue
        }

        // we should never have an error, cache is in an inconsistent state if this happens
        if queue != nil {
            if err := queue.DecAllocatedResource(alloc.AllocatedResource); err != nil {
                glog.V(0).Infof("Queue failed to release resources from application %s: %v", alloc.ApplicationId, err)
            }
        }

        glog.V(2).Infof("Removed allocation %s from removed node %s", allocID, node.NodeId)
    }

    pi.totalPartitionResource = resources.Sub(pi.totalPartitionResource, node.TotalResource)
    pi.Root.MaxResource = pi.totalPartitionResource

    // Remove node from list of tracked nodes
    delete(pi.nodes, nodeId)
    pi.metrics.DecActiveNodes()

    glog.V(4).Infof("Removed node %s from partition %s", node.NodeId, pi.Name)
}

// Add a new application to the partition.
// A new application should not be part of the partition yet. The failure behaviour is managed by the failIfExist flag.
// If the flag is true the add will fail returning an error. If the flag is false the add will not fail if the application
// exists but no change is made.
func (pi *PartitionInfo) addNewApplication(info *ApplicationInfo, failIfExist bool) error {
    pi.lock.Lock()
    defer pi.lock.Unlock()

    glog.V(4).Infof("Trying to add application %s in queue %s to partition %s", info.ApplicationId, info.QueueName, pi.Name)
    if pi.IsStopped() {
        return fmt.Errorf("partition %s is stopped cannot add a new application %s", pi.Name, info.ApplicationId)
    }

    if app := pi.applications[info.ApplicationId]; app != nil {
        if failIfExist {
            return fmt.Errorf("application %s already exists in partition %s", info.ApplicationId, pi.Name)
        } else {
            glog.V(4).Infof("Application %s already exists in partition %s (ignoring add request)", info.ApplicationId, pi.Name)
            return nil
        }
    }

    // check if queue exist, and it is a leaf queue
    // TODO. add acl check and placement rules
    queue := pi.getQueue(info.QueueName)
    if queue == nil || !queue.IsLeafQueue() {
        pi.metrics.IncTotalApplicationsRejected()
        return fmt.Errorf("failed to submit application %s to queue %s, partition %s, because queue doesn't exist or queue is not leaf queue",
            info.ApplicationId, info.QueueName, pi.Name)
    }

    // All checked, app can be added.
    info.LeafQueue = queue
    pi.applications[info.ApplicationId] = info
    pi.metrics.IncTotalApplicationsAdded()

    glog.V(4).Infof("Added application %s in queue %s to partition %s", info.ApplicationId, info.QueueName, pi.Name)
    return nil
}

// Get the application object for the application ID as tracked by the partition.
// This will return nil if the application is not part of this partition.
func (pi *PartitionInfo) getApplication(appId string) *ApplicationInfo {
    pi.lock.RLock()
    defer pi.lock.RUnlock()

    return pi.applications[appId]
}

// Get the node object for the node ID as tracked by the partition.
// This will return nil if the node is not part of this partition.
// Visible by tests
func (pi *PartitionInfo) GetNode(nodeId string) *NodeInfo {
    pi.lock.RLock()
    defer pi.lock.RUnlock()

    return pi.nodes[nodeId]
}

// Remove one or more allocations for an application.
// Returns all removed allocations.
// If no specific allocation is specified via a uuid all allocations are removed.
func (pi *PartitionInfo) releaseAllocationsForApplication(toRelease *commonevents.ReleaseAllocation) []*AllocationInfo {
    pi.lock.Lock()
    defer pi.lock.Unlock()

    allocationsToRelease := make([]*AllocationInfo, 0)

    glog.V(4).Infof("Trying to remove allocation from partition %s", pi.Name)
    if toRelease == nil {
        glog.V(4).Infof("No allocations removed from partition %s", pi.Name)
        return allocationsToRelease
    }

    // First delete from app
    var queue *QueueInfo = nil
    if app := pi.applications[toRelease.ApplicationId]; app != nil {
        // when uuid not specified, remove all allocations from the app
        if toRelease.Uuid == "" {
            glog.V(4).Infof("Removing all allocations for application %s", app.ApplicationId)
            for _, alloc := range app.CleanupAllAllocations() {
                allocationsToRelease = append(allocationsToRelease, alloc)
            }
        } else {
            glog.V(4).Infof("Removing allocations for application %s with uuid %s", app.ApplicationId, toRelease.Uuid)
            if alloc := app.RemoveAllocation(toRelease.Uuid); alloc != nil {
                allocationsToRelease = append(allocationsToRelease, alloc)
            }
        }
        queue = app.LeafQueue
    }

    // If nothing was released then return now: this can happen if the allocation was not found or the application did not
    // not have any allocations
    if len(allocationsToRelease) == 0 {
        glog.V(4).Infof("No active allocations found to release for application %s", toRelease.ApplicationId)
        return allocationsToRelease
    }

    // for each allocations to release, update node.
    totalReleasedResource := resources.NewResource()

    for _, alloc := range allocationsToRelease {
        // remove allocation from node
        node := pi.nodes[alloc.AllocationProto.NodeId]
        if node == nil || node.GetAllocation(alloc.AllocationProto.Uuid) == nil {
            glog.V(0).Infof("No node found for allocation %v", alloc)
            continue
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

    glog.V(4).Infof("Removed %d allocation from partition %s", len(allocationsToRelease), pi.Name)
    return allocationsToRelease
}

// Add an allocation to the partition/node/application/queue.
// Queue max allocation is not check if the allocation is part of a new node addition (nodeReported == true)
//
// NOTE: this is a lock free call. It should only be called holding the PartitionInfo lock.
// If access outside is needed a locked version must used, see addNewAllocation
func (pi *PartitionInfo) addNewAllocationInternal(alloc *commonevents.AllocationProposal, nodeReported bool) (*AllocationInfo, error) {
    glog.V(4).Infof("Trying to add new allocation to partition %s", pi.Name)

    if pi.IsStopped() {
        return nil, fmt.Errorf("partition %s is stopped cannot add new allocation %s", pi.Name, alloc.AllocationKey)
    }

    // Check if allocation violates any resource restriction, or allocate on a
    // non-existent applications or nodes.
    var node *NodeInfo
    var app *ApplicationInfo
    var queue *QueueInfo
    var ok bool

    if node, ok = pi.nodes[alloc.NodeId]; !ok {
        pi.metrics.IncScheduledAllocationErrors()
        return nil, fmt.Errorf("failed to find node %s", alloc.NodeId)
    }

    if app, ok = pi.applications[alloc.ApplicationId]; !ok {
        pi.metrics.IncScheduledAllocationErrors()
        return nil, fmt.Errorf("failed to find application %s", alloc.ApplicationId)
    }

    if queue = pi.getQueue(alloc.QueueName); queue == nil || !queue.IsLeafQueue() {
        pi.metrics.IncScheduledAllocationErrors()
        return nil, fmt.Errorf("queue does not exist or is not a leaf queue %s", alloc.QueueName)
    }

    // Does the new allocation exceed the node's available resource?
    newNodeResource := resources.Add(node.GetAllocatedResource(), alloc.AllocatedResource)
    if !resources.FitIn(node.TotalResource, newNodeResource) {
        pi.metrics.IncScheduledAllocationFailures()
        return nil, fmt.Errorf("cannot allocate resource [%v] for application %s on "+
            "node %s because request exceeds available resources, used [%v] node limit [%v]",
            alloc.AllocatedResource, alloc.ApplicationId, node.NodeId, newNodeResource, node.TotalResource)    }

    // If new allocation go beyond any of queue's max resource? Only check if when it is allocated instead of node reported.
    if err := queue.IncAllocatedResource(alloc.AllocatedResource, nodeReported); err != nil {
        pi.metrics.IncScheduledAllocationFailures()
        return nil, fmt.Errorf("cannot allocate resource from application %s: %v ",
            alloc.ApplicationId, err)
    }

    // Start allocation
    allocationUuid := pi.GetNewAllocationUuid()
    allocation := NewAllocationInfo(allocationUuid, alloc)

    node.AddAllocation(allocation)

    app.AddAllocation(allocation)

    pi.allocations[allocation.AllocationProto.Uuid] = allocation

    pi.metrics.IncScheduledAllocationSuccesses()
    glog.V(4).Infof("Added new allocation with uuid %s to partition %s", allocationUuid, pi.Name)
    return allocation, nil

}

// Add a new allocation to the partition.
// This is the locked version which calls addNewAllocationInternal() inside a partition lock.
func (pi *PartitionInfo) addNewAllocation(proposal *commonevents.AllocationProposal) (*AllocationInfo, error) {
    pi.lock.Lock()
    defer pi.lock.Unlock()
    return pi.addNewAllocationInternal(proposal, false)
}

// Generate a new uuid for the allocation.
// This is guaranteed to return a unique ID for this partition.
func (pi *PartitionInfo) GetNewAllocationUuid() string {
    // Retry to make sure uuid is correct
    for {
        allocationUuid := uuid.NewV4().String()
        if pi.allocations[allocationUuid] == nil {
            return allocationUuid
        }
    }
}

// Remove the application from the partition.
// This will also release all the allocations for application from the queue and nodes.
func (pi *PartitionInfo) RemoveApplication(appId string) (*ApplicationInfo, []*AllocationInfo) {
    pi.lock.Lock()
    defer pi.lock.Unlock()

    glog.V(4).Infof("Trying to remove application %s from partition %s", appId, pi.Name)

    app := pi.applications[appId]
    if app == nil {
        glog.V(4).Infof("Application %s not found in partition %s", appId, pi.Name)
        return nil, make([]*AllocationInfo, 0)
    }
    // Save the total allocated resources of the application.
    // Might need to base this on calculation of the real removed resources.
    totalAppAllocated := app.GetAllocatedResource()

    // Remove all allocations from the application
    allocations := app.CleanupAllAllocations()

    // Remove all allocations from nodes/queue
    if len(allocations) != 0 {
        for _, alloc := range allocations {
            uuid := alloc.AllocationProto.Uuid
            glog.V(4).Infof("Removing allocation %s for application %s", uuid, appId)
            // Remove from partition cache
            if globalAlloc := pi.allocations[uuid]; globalAlloc == nil {
                glog.V(1).Infof("Application %s references unknown allocation %s: not found in global cache", appId, uuid)
                continue
            } else {
                delete(pi.allocations, uuid)
            }

            // Remove from node
            node := pi.nodes[alloc.AllocationProto.NodeId]
            if node == nil {
                glog.V(1).Infof("Application %s references unknown node in allocation %s: not found in active node list", appId, alloc.AllocationProto.NodeId)
                continue
            }
            if nodeAlloc := node.RemoveAllocation(uuid); nodeAlloc == nil {
                glog.V(1).Infof("Application %s allocation %s not found on node %s", appId, uuid, alloc.AllocationProto.NodeId)
                continue
            }
        }

        // we should never have an error, cache is in an inconsistent state if this happens
        queue := app.LeafQueue
        if queue != nil {
            if err := queue.DecAllocatedResource(totalAppAllocated); err != nil {
                glog.V(1).Infof("Queue failed to release resources from application %s: %v", app.ApplicationId, err)
            }
        }
    }
    // Remove app from cache now that everything is cleaned up
    delete(pi.applications, appId)

    glog.V(4).Infof("Removed application %s from partition %s, resources released %v", app.ApplicationId, app.Partition, totalAppAllocated)

    return app, allocations
}

// Return a copy of all the nodes registers to this partition
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
// NOTE: this is a lock free call. It should only be called holding the PartitionInfo lock.
// If access outside is needed a locked version must be introduced.
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

// Mark the partition  for removal from the system.
// This can be executed multiple times and is only effective the first time.
func (pi *PartitionInfo) MarkPartitionForRemoval() {
    pi.lock.RLock()
    defer pi.lock.RUnlock()
    pi.state = deleted
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
