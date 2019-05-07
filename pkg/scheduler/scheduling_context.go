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

package scheduler

import (
    "errors"
    "fmt"
    "github.com/golang/glog"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/cache"
    "sync"
)

type ClusterSchedulingContext struct {
    partitions map[string]*PartitionSchedulingContext

    needPreemption bool

    lock sync.RWMutex
}

func NewClusterSchedulingContext() *ClusterSchedulingContext {
    return &ClusterSchedulingContext{
        partitions: make(map[string]*PartitionSchedulingContext),
    }
}

type PartitionSchedulingContext struct {
    applications map[string]*SchedulingApplication
    queues       map[string]*SchedulingQueue
    Root         *SchedulingQueue
    RmId         string

    lock sync.RWMutex
}

// Create a new partitioning scheduling context
func newPartitionSchedulingContext(rmId string) *PartitionSchedulingContext {
    return &PartitionSchedulingContext{
        applications: make(map[string]*SchedulingApplication),
        queues:       make(map[string]*SchedulingQueue),
        RmId:         rmId,
    }
}

func (csc *ClusterSchedulingContext) getPartitionMapClone() map[string]*PartitionSchedulingContext {
    csc.lock.RLock()
    defer csc.lock.RUnlock()

    newMap := make(map[string]*PartitionSchedulingContext)
    for k,v := range csc.partitions {
        newMap[k] = v
    }
    return newMap
}

func (psc *PartitionSchedulingContext) updatePartitionSchedulingContext(info *cache.PartitionInfo) {
    root := psc.Root
    // update the root queue properties
    root.updateSchedulingQueueProperties(info.Root.Properties)
    // update the rest of the queues recursively
    root.updateSchedulingQueueInfo(info.Root.GetCopyOfChildren(), root)
}

func (psc *PartitionSchedulingContext) AddSchedulingApplication(schedulingApp *SchedulingApplication) error {
    psc.lock.Lock()
    defer psc.lock.Unlock()

    // Add to applications
    appId := schedulingApp.ApplicationInfo.ApplicationId
    if psc.applications[appId] != nil {
        return errors.New(fmt.Sprintf("Adding app=%s to partition=%s, but app already existed.", appId, schedulingApp.ApplicationInfo.Partition))
    }

    psc.applications[appId] = schedulingApp

    // Put app under queue
    schedulingQueue := psc.queues[schedulingApp.ApplicationInfo.QueueName]
    if schedulingQueue == nil {
        return errors.New(fmt.Sprintf("Failed to find queue=%s for app=%s", schedulingApp.ApplicationInfo.QueueName, schedulingApp.ApplicationInfo.ApplicationId))
    }
    schedulingApp.ParentQueue = schedulingQueue
    schedulingQueue.AddSchedulingApplication(schedulingApp)

    return nil
}

func (psc *PartitionSchedulingContext) RemoveSchedulingApplication(appId string, partitionName string) (*SchedulingApplication, error) {
    psc.lock.Lock()
    defer psc.lock.Unlock()

    // Remove from applications map
    if psc.applications[appId] == nil {
        return nil, errors.New(fmt.Sprintf("Removing app=%s to partition=%s, but application is non-existed.", appId, partitionName))
    }
    schedulingApp := psc.applications[appId]
    delete(psc.applications, appId)

    // Remove app under queue
    schedulingQueue := psc.queues[schedulingApp.ApplicationInfo.QueueName]
    if schedulingQueue == nil {
        // This is not normal
        panic(fmt.Sprintf("Failed to find queue=%s for app=%s while removing application", schedulingApp.ApplicationInfo.QueueName, schedulingApp.ApplicationInfo.ApplicationId))
    }
    schedulingApp.ParentQueue = schedulingQueue
    schedulingQueue.RemoveSchedulingApplication(schedulingApp)

    // Update pending resource of queues
    totalPending := schedulingApp.Requests.GetPendingResource()
    queue := schedulingApp.ParentQueue
    for queue != nil {
        queue.DecPendingResource(totalPending)
        queue = queue.Parent
    }

    return schedulingApp, nil
}

// Visible by tests
// TODO need to think about this: maintaining the flat map might not scale with dynamic queues
func (psc *PartitionSchedulingContext) GetQueue(queueName string) *SchedulingQueue {
    psc.lock.RLock()
    defer psc.lock.RUnlock()

    return psc.queues[queueName]
}

func (psc *PartitionSchedulingContext) getApplication(appId string) *SchedulingApplication {
    psc.lock.RLock()
    defer psc.lock.RUnlock()

    return psc.applications[appId]
}

func (csc *ClusterSchedulingContext) GetPartitionSchedulingContext(partitionName string) *PartitionSchedulingContext {
    csc.lock.RLock()
    defer csc.lock.RUnlock()
    return csc.partitions[partitionName]
}

func (csc *ClusterSchedulingContext) GetSchedulingApplication(appId string, partitionName string) *SchedulingApplication {
    csc.lock.RLock()
    defer csc.lock.RUnlock()

    if partition := csc.partitions[partitionName]; partition != nil {
        return partition.getApplication(appId)
    }

    return nil
}

// Visible by tests
func (csc *ClusterSchedulingContext) GetSchedulingQueue(queueName string, partitionName string) *SchedulingQueue {
    csc.lock.RLock()
    defer csc.lock.RUnlock()

    if partition := csc.partitions[partitionName]; partition != nil {
        return partition.GetQueue(queueName)
    }

    return nil
}

func (csc *ClusterSchedulingContext) AddSchedulingApplication(schedulingApp *SchedulingApplication) error {
    partitionName := schedulingApp.ApplicationInfo.Partition
    appId := schedulingApp.ApplicationInfo.ApplicationId

    csc.lock.Lock()
    defer csc.lock.Unlock()

    if partition := csc.partitions[partitionName]; partition != nil {
        if err := partition.AddSchedulingApplication(schedulingApp); err != nil {
            return err
        }
    } else {
        return fmt.Errorf("failed to find partition=%s while adding app=%s", partitionName, appId)
    }

    return nil
}

func (csc *ClusterSchedulingContext) RemoveSchedulingApplication(appId string, partitionName string) (*SchedulingApplication, error) {
    csc.lock.Lock()
    defer csc.lock.Unlock()

    if partition := csc.partitions[partitionName]; partition != nil {
        schedulingApp, err := partition.RemoveSchedulingApplication(appId, partitionName)
        if err != nil {
            return nil, err
        }
        return schedulingApp, nil
    } else {
        return nil, fmt.Errorf("failed to find partition=%s while remove app=%s", partitionName, appId)
    }
}

// Update the scheduler's partition list based on the processed config
// - updates existing partitions and the queues linked
// - add new partitions including queues
// updates and add internally are processed differently outside of this method they are the same.
func (csc *ClusterSchedulingContext) updateSchedulingPartitions(partitions []*cache.PartitionInfo) error {
    csc.lock.Lock()
    defer csc.lock.Unlock()
    glog.V(3).Infof("Updating scheduler context, %d partitions changed", len(partitions))

    // Walk over the updated partitions
    for _, updatedPartition := range partitions {
        csc.needPreemption = csc.needPreemption || updatedPartition.NeedPreemption()

        partition := csc.partitions[updatedPartition.Name]
        if partition != nil {
            glog.V(3).Infof("Updating existing scheduling partition: %s", updatedPartition.Name)
            // the partition details don't need updating just the queues
            partition.updatePartitionSchedulingContext(updatedPartition)
            // redo the flat map as queues might have been added/removed
            partition.queues = make(map[string]*SchedulingQueue)
            partition.Root.GetFlatChildrenQueues(partition.queues)
        } else {
            glog.V(3).Infof("Creating new scheduling partition: %s", updatedPartition.Name)
            // create a new partition and add the queues
            newPartition := newPartitionSchedulingContext(updatedPartition.RMId)
            newPartition.Root = NewSchedulingQueueInfo(updatedPartition.Root, nil)
            newPartition.Root.GetFlatChildrenQueues(newPartition.queues)

            csc.partitions[updatedPartition.Name] = newPartition
        }
    }
    return nil
}

func (csc *ClusterSchedulingContext) RemoveSchedulingPartitionsByRMId(rmId string) {
    csc.lock.Lock()
    defer csc.lock.Unlock()
    partitionToRemove := make(map[string]bool)

    // Just remove corresponding partitions
    for k, partition := range csc.partitions {
        if partition.RmId == rmId {
            partitionToRemove[k] = true
        }
    }

    for partitionName := range partitionToRemove {
        delete(csc.partitions, partitionName)
    }
}

// Remove the partition from the scheduler based on a configuration change
// No resources can be used and the underlying partition should not be running
func (csc *ClusterSchedulingContext) deleteSchedulingPartitions(partitions []*cache.PartitionInfo) error {
    csc.lock.Lock()
    defer csc.lock.Unlock()

    // Walk over the deleted partitions
    for _, updatedPartition := range partitions {
        partition := csc.partitions[updatedPartition.Name]
        if partition != nil {
            glog.V(1).Infof("Marking scheduling partition for deletion: %s", updatedPartition.Name)
            // TODO really clean up
        } else {
            return fmt.Errorf("failed to find partition that should have been deleted ")
        }
    }
    return nil
}

func (csc* ClusterSchedulingContext) NeedPreemption() bool {
    csc.lock.RLock()
    defer csc.lock.RUnlock()

    return csc.needPreemption
}
