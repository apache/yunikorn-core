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
    "fmt"
    "github.com/cloudera/yunikorn-core/pkg/common/commonevents"
    "github.com/cloudera/yunikorn-core/pkg/common/configs"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "github.com/cloudera/yunikorn-core/pkg/metrics"
    "github.com/cloudera/yunikorn-core/pkg/webservice/dao"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "github.com/looplab/fsm"
    "github.com/satori/go.uuid"
    "go.uber.org/zap"
    "strings"
    "sync"
    "time"
)


/* Related to partitions */
type PartitionInfo struct {
    Name string
    Root *QueueInfo
    RMId string

    // Private fields need protection
    allocations            map[string]*AllocationInfo   // allocations
    nodes                  map[string]*NodeInfo         // nodes registered
    applications           map[string]*ApplicationInfo  // the application list
    stateMachine           *fsm.FSM                     // the state of the queue for scheduling
    stateTime              time.Time                    // last time the state was updated (needed for cleanup)
    isPreemptable          bool                         // can allocations be preempted
    clusterInfo            *ClusterInfo                 // link back to the cluster info
    lock                   sync.RWMutex                 // lock for updating the partition
    totalPartitionResource *resources.Resource          // Total node resources
    metrics                metrics.CoreSchedulerMetrics // Reference to scheduler metrics
}

// Create a new partition from scratch based on a validated configuration.
// If teh configuration did not pass validation and is processed weird errors could occur.
func NewPartitionInfo(partition configs.PartitionConfig, rmId string, info *ClusterInfo) (*PartitionInfo, error) {
    p := &PartitionInfo{
        Name:         partition.Name,
        RMId:         rmId,
        stateMachine: newObjectState(),
        clusterInfo:  info,
    }
    p.allocations = make(map[string]*AllocationInfo)
    p.nodes = make(map[string]*NodeInfo)
    p.applications = make(map[string]*ApplicationInfo)
    p.totalPartitionResource = resources.NewResource()
    log.Logger.Info("creating partition",
        zap.String("partitionName", p.Name),
        zap.String("rmId", p.RMId))

    // Setup the queue structure: root first it should be the only queue at this level
    // Add the rest of the queue structure recursively
    queueConf := partition.Queues[0]
    root, err := NewManagedQueue(queueConf, nil)
    err = addQueueInfo(queueConf.Queues, root)
    if err != nil {
        return nil, err
    }
    p.Root = root
    log.Logger.Info("queue added",
        zap.String("partitionName", p.Name),
        zap.String("rmId", p.RMId))

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

// Handle the state event for the application.
// The state machine handles the locking.
func (pi *PartitionInfo) HandlePartitionEvent(event SchedulingObjectEvent) error {
    err := pi.stateMachine.Event(event.String(), pi.Name)
    if err == nil {
        pi.stateTime =time.Now()
        return nil
    }
    // handle the same state transition not nil error (limit of fsm).
    if err.Error() == "no transition" {
        return nil
    }
    return err
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

    log.Logger.Info("add node to partition",
        zap.String("nodeId", node.NodeId),
        zap.String("partition", pi.Name))

    if pi.IsDraining() || pi.IsStopped() {
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
        log.Logger.Info("add existing allocations",
            zap.String("nodeId", node.NodeId),
            zap.Int("existingAllocations", len(existingAllocations)))
        for _, alloc := range existingAllocations {
            // if this is to start processing existing allocations for an app,
            // transit app's state from Accepted to Running
            if app, ok := pi.applications[alloc.ApplicationId]; ok {
                log.Logger.Info("", zap.String("state", app.GetApplicationState()))
                if app.GetApplicationState() == Accepted.String() {
                    if err := app.HandleApplicationEvent(RunApplication); err != nil {
                        log.Logger.Warn("unable to handle app event - RunApplication",
                            zap.Error(err))
                    }
                }
            }

            if _, err := pi.addNodeReportedAllocations(alloc); err != nil {
                log.Logger.Info("failed to add existing allocations",
                    zap.String("nodeId", node.NodeId),
                    zap.Int("existingAllocations", len(existingAllocations)))
                pi.removeNodeInternal(node.NodeId)
                pi.metrics.IncFailedNodes()
                return err
            }
        }
    }
    // Node is added update the metrics
    pi.metrics.IncActiveNodes()
    log.Logger.Info("added node to partition",
        zap.String("nodeId", node.NodeId),
        zap.String("partition", pi.Name))

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
func (pi *PartitionInfo) RemoveNode(nodeId string) {
    pi.lock.Lock()
    defer pi.lock.Unlock()

    pi.removeNodeInternal(nodeId)
}

// Remove a node from the partition.
//
// NOTE: this is a lock free call. It should only be called holding the PartitionInfo lock.
// If access outside is needed a locked version must used, see removeNode
func (pi *PartitionInfo) removeNodeInternal(nodeId string) {
    log.Logger.Info("remove node from partition",
        zap.String("nodeId", nodeId),
        zap.String("partition", pi.Name))

    node := pi.nodes[nodeId]
    if node == nil {
        log.Logger.Debug("not was not found",
            zap.String("nodeId", nodeId),
            zap.String("partitionName", pi.Name))
        return
    }

    // walk over all allocations still registered for this node
    for _, alloc := range node.GetAllAllocations() {
        var queue *QueueInfo = nil
        allocID := alloc.AllocationProto.Uuid
        // since we are not locking the node and or application we could have had an update while processing
        if app := pi.applications[alloc.ApplicationId]; app != nil {
            // check allocations on the node
           if alloc := app.removeAllocation(allocID); alloc == nil {
                log.Logger.Info("allocation is not found, skipping removing the node",
                    zap.String("allocationId", allocID),
                    zap.String("appId", app.ApplicationId),
                    zap.String("nodeId", node.NodeId))
                continue
            }
            queue = app.leafQueue
        } else {
            log.Logger.Info("app is not found, skipping removing the node",
                zap.String("appId", alloc.ApplicationId),
                zap.String("nodeId", node.NodeId))
            continue
        }

        // we should never have an error, cache is in an inconsistent state if this happens
        if queue != nil {
            if err := queue.DecAllocatedResource(alloc.AllocatedResource); err != nil {
                log.Logger.Warn("failed to release resources from queue",
                    zap.String("appId", alloc.ApplicationId),
                    zap.Error(err))
            }
        }

        log.Logger.Info("allocation removed",
            zap.String("allocationId", allocID),
            zap.String("nodeId", node.NodeId))
    }

    pi.totalPartitionResource = resources.Sub(pi.totalPartitionResource, node.TotalResource)
    pi.Root.MaxResource = pi.totalPartitionResource

    // Remove node from list of tracked nodes
    delete(pi.nodes, nodeId)
    pi.metrics.DecActiveNodes()

    log.Logger.Info("node removed",
        zap.String("partitionName", pi.Name),
        zap.String("nodeId", node.NodeId))
}

// Add a new application to the partition.
// A new application should not be part of the partition yet. The failure behaviour is managed by the failIfExist flag.
// If the flag is true the add will fail returning an error. If the flag is false the add will not fail if the application
// exists but no change is made.
func (pi *PartitionInfo) addNewApplication(info *ApplicationInfo, failIfExist bool) error {
    pi.lock.Lock()
    defer pi.lock.Unlock()

    log.Logger.Info("adding app to partition",
        zap.String("appId", info.ApplicationId),
        zap.String("queue", info.QueueName),
        zap.String("partitionName", pi.Name))
    if pi.IsDraining() || pi.IsStopped() {
        return fmt.Errorf("partition %s is stopped cannot add a new application %s", pi.Name, info.ApplicationId)
    }

    if app := pi.applications[info.ApplicationId]; app != nil {
        if failIfExist {
            return fmt.Errorf("application %s already exists in partition %s", info.ApplicationId, pi.Name)
        } else {
            log.Logger.Info("app already exists in partition",
                zap.String("appId", info.ApplicationId),
                zap.String("partitionName", pi.Name))
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
    info.leafQueue = queue
    pi.applications[info.ApplicationId] = info
    pi.metrics.IncTotalApplicationsAdded()

    log.Logger.Info("app added to partition",
        zap.String("appId", info.ApplicationId),
        zap.String("partitionName", pi.Name))
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

    log.Logger.Debug("removing allocation from partition",
        zap.String("partitionName", pi.Name))
    if toRelease == nil {
        log.Logger.Debug("no allocations removed from partition",
            zap.String("partitionName", pi.Name))
        return allocationsToRelease
    }

    // First delete from app
    var queue *QueueInfo = nil
    if app := pi.applications[toRelease.ApplicationId]; app != nil {
        // when uuid not specified, remove all allocations from the app
        if toRelease.Uuid == "" {
            log.Logger.Debug("remove all allocations",
                zap.String("appId", app.ApplicationId))
            for _, alloc := range app.removeAllAllocations() {
                allocationsToRelease = append(allocationsToRelease, alloc)
            }
        } else {
            log.Logger.Debug("removing allocations",
                zap.String("appId", app.ApplicationId),
                zap.String("allocationId", toRelease.Uuid))
            if alloc := app.removeAllocation(toRelease.Uuid); alloc != nil {
                allocationsToRelease = append(allocationsToRelease, alloc)
            }
        }
        queue = app.leafQueue
    }

    // If nothing was released then return now: this can happen if the allocation was not found or the application did not
    // not have any allocations
    if len(allocationsToRelease) == 0 {
        log.Logger.Debug("no active allocations found to release",
            zap.String("appId", toRelease.ApplicationId))
        return allocationsToRelease
    }

    // for each allocations to release, update node.
    totalReleasedResource := resources.NewResource()

    for _, alloc := range allocationsToRelease {
        // remove allocation from node
        node := pi.nodes[alloc.AllocationProto.NodeId]
        if node == nil || node.GetAllocation(alloc.AllocationProto.Uuid) == nil {
            log.Logger.Info("node is not found for allocation",
                zap.Any("allocation", alloc))
            continue
        }
        node.RemoveAllocation(alloc.AllocationProto.Uuid)
        resources.AddTo(totalReleasedResource, alloc.AllocatedResource)
    }

    // this nil check is not really needed as we can only reach here with a queue set, IDE complains without this
    if queue != nil {
        // we should never have an error, cache is in an inconsistent state if this happens
        if err := queue.DecAllocatedResource(totalReleasedResource); err != nil {
            log.Logger.Warn("failed to release resources",
                zap.Any("appId", toRelease.ApplicationId),
                zap.Error(err))
        }
    }

    // Update global allocation list
    for _, alloc := range allocationsToRelease {
        delete(pi.allocations, alloc.AllocationProto.Uuid)
    }

    log.Logger.Info("allocation removed",
        zap.Int("numOfAllocationReleased", len(allocationsToRelease)),
        zap.String("partitionName", pi.Name))
    return allocationsToRelease
}

// Add an allocation to the partition/node/application/queue.
// Queue max allocation is not check if the allocation is part of a new node addition (nodeReported == true)
//
// NOTE: this is a lock free call. It should only be called holding the PartitionInfo lock.
// If access outside is needed a locked version must used, see addNewAllocation
func (pi *PartitionInfo) addNewAllocationInternal(alloc *commonevents.AllocationProposal, nodeReported bool) (*AllocationInfo, error) {
    log.Logger.Debug("adding allocation",
        zap.String("partitionName", pi.Name))

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

    app.addAllocation(allocation)

    pi.allocations[allocation.AllocationProto.Uuid] = allocation

    pi.metrics.IncScheduledAllocationSuccesses()

    log.Logger.Debug("added allocation",
        zap.String("allocationUid", allocationUuid),
        zap.String("partitionName", pi.Name))
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

    log.Logger.Debug("removing app from partition",
        zap.String("appId", appId),
        zap.String("partitionName", pi.Name))

    app := pi.applications[appId]
    if app == nil {
        log.Logger.Warn("app not found partition",
            zap.String("appId", appId),
            zap.String("partitionName", pi.Name))
        return nil, make([]*AllocationInfo, 0)
    }
    // Save the total allocated resources of the application.
    // Might need to base this on calculation of the real removed resources.
    totalAppAllocated := app.GetAllocatedResource()

    // Remove all allocations from the application
    allocations := app.removeAllAllocations()

    // Remove all allocations from nodes/queue
    if len(allocations) != 0 {
        for _, alloc := range allocations {
            uuid := alloc.AllocationProto.Uuid
            log.Logger.Warn("removing allocations",
                zap.String("appId", appId),
                zap.String("allocationId", uuid))
            // Remove from partition cache
            if globalAlloc := pi.allocations[uuid]; globalAlloc == nil {
                log.Logger.Warn("unknown allocation: not found in global cache",
                    zap.String("appId", appId),
                    zap.String("allocationId", uuid))
                continue
            } else {
                delete(pi.allocations, uuid)
            }

            // Remove from node
            node := pi.nodes[alloc.AllocationProto.NodeId]
            if node == nil {
                log.Logger.Warn("unknown node: not found in active node list",
                    zap.String("appId", appId),
                    zap.String("nodeId", alloc.AllocationProto.NodeId))
                continue
            }
            if nodeAlloc := node.RemoveAllocation(uuid); nodeAlloc == nil {
                log.Logger.Warn("allocation not found on node",
                    zap.String("appId", appId),
                    zap.String("allocationId", uuid),
                    zap.String("nodeId", alloc.AllocationProto.NodeId))
                continue
            }
        }

        // we should never have an error, cache is in an inconsistent state if this happens
        queue := app.leafQueue
        if queue != nil {
            if err := queue.DecAllocatedResource(totalAppAllocated); err != nil {
                log.Logger.Error("failed to release resources for app",
                    zap.String("appId", app.ApplicationId),
                    zap.Error(err))
            }
        }
    }
    // Remove app from cache now that everything is cleaned up
    delete(pi.applications, appId)

    log.Logger.Info("app removed from partition",
        zap.String("appId", app.ApplicationId),
        zap.String("partitionName", app.Partition),
        zap.Any("resourceReleased", totalAppAllocated))

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
    if err := pi.HandlePartitionEvent(Remove); err != nil {
        log.Logger.Error("failed to mark partition for deletion",
            zap.String("partitionName", pi.Name),
            zap.Error(err))
    }
}

// The partition has been removed from the configuration and must be removed.
// This is the cleanup triggered by the exiting scheduler partition. Just unlinking from the cluster should be enough.
// All other clean up is triggered from the scheduler
func (pi *PartitionInfo) Remove() {
    pi.lock.RLock()
    defer pi.lock.RUnlock()
    // safeguard
    if pi.IsDraining() || pi.IsStopped() {
        pi.clusterInfo.removePartition(pi.Name)
    }
}


// Is the partition marked for deletion and can only handle existing application requests.
// No new applications will be accepted.
func (pi *PartitionInfo) IsDraining() bool {
    return pi.stateMachine.Current() == Draining.String()
}

func (pi *PartitionInfo) IsRunning() bool {
    return pi.stateMachine.Current() == Active.String()
}

func (pi *PartitionInfo) IsStopped() bool {
    return pi.stateMachine.Current() == Stopped.String()
}
