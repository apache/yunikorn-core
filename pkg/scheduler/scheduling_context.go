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
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/cache"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common"
    "sync"
)

type ClusterSchedulingContext struct {
    partitions map[string]*PartitionSchedulingContext

    lock sync.RWMutex
}

func NewClusterSchedulingContext() *ClusterSchedulingContext {
    return &ClusterSchedulingContext{partitions: make(map[string]*PartitionSchedulingContext)}
}

type PartitionSchedulingContext struct {
    applications map[string]*SchedulingApplication
    queues       map[string]*SchedulingQueue
    Root         *SchedulingQueue
    RmId         string

    lock sync.RWMutex
}

func (m *PartitionSchedulingContext) AddSchedulingApplication(schedulingApp *SchedulingApplication) error {
    m.lock.Lock()
    defer m.lock.Unlock()

    // Add to applications
    appId := schedulingApp.ApplicationInfo.ApplicationId
    if m.applications[appId] != nil {
        return errors.New(fmt.Sprintf("Adding app=%s to partition=%s, but app already existed.", appId, schedulingApp.ApplicationInfo.Partition))
    }

    m.applications[appId] = schedulingApp

    // Put app under queue
    schedulingQueue := m.queues[schedulingApp.ApplicationInfo.QueueName]
    if schedulingQueue == nil {
        return errors.New(fmt.Sprintf("Failed to find queue=%s for app=%s", schedulingApp.ApplicationInfo.QueueName, schedulingApp.ApplicationInfo.ApplicationId))
    }
    schedulingApp.ParentQueue = schedulingQueue
    schedulingQueue.AddSchedulingApplication(schedulingApp)

    return nil
}

func (m *PartitionSchedulingContext) RemoveSchedulingApplication(appId string, partitionName string) (*SchedulingApplication, error) {
    m.lock.Lock()
    defer m.lock.Unlock()

    // Remove from applications map
    if m.applications[appId] == nil {
        return nil, errors.New(fmt.Sprintf("Removing app=%s to partition=%s, but application is non-existed.", appId, partitionName))
    }
    schedulingApp := m.applications[appId]
    delete(m.applications, appId)

    // Remove app under queue
    schedulingQueue := m.queues[schedulingApp.ApplicationInfo.QueueName]
    if schedulingQueue == nil {
        // This is not normal
        panic(fmt.Sprintf("Failed to find queue=%s for app=%s while removing application", schedulingApp.ApplicationInfo.QueueName, schedulingApp.ApplicationInfo.ApplicationId))
    }
    schedulingApp.ParentQueue = schedulingQueue
    schedulingQueue.RemoveSchedulingApplication(schedulingApp)

    // Update pending resource of queues
    totalPending := schedulingApp.Requests.TotalPendingResource
    queue := schedulingApp.ParentQueue
    for queue != nil {
        queue.DecPendingResource(totalPending)
        queue = queue.Parent
    }

    return schedulingApp, nil
}

// Visible by tests
func (m *PartitionSchedulingContext) GetQueue(queueName string) *SchedulingQueue {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.queues[queueName]
}

func (m *PartitionSchedulingContext) getApplication(appId string) *SchedulingApplication {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.applications[appId]
}

func newPartitionSchedulingContext(rmId string) *PartitionSchedulingContext {
    return &PartitionSchedulingContext{applications: make(map[string]*SchedulingApplication), queues: make(map[string]*SchedulingQueue), RmId: rmId}
}

func (m *ClusterSchedulingContext) GetPartitionSchedulingContext(partitionName string) *PartitionSchedulingContext {
    m.lock.RLock()
    defer m.lock.RUnlock()
    return m.partitions[partitionName]
}

func (m *ClusterSchedulingContext) GetSchedulingApplication(appId string, partitionName string) *SchedulingApplication {
    m.lock.RLock()
    defer m.lock.RUnlock()

    if partition := m.partitions[partitionName]; partition != nil {
        return partition.getApplication(appId)
    }

    return nil
}

// Visible by tests
func (m *ClusterSchedulingContext) GetSchedulingQueue(queueName string, partitionName string) *SchedulingQueue {
    m.lock.RLock()
    defer m.lock.RUnlock()

    if partition := m.partitions[partitionName]; partition != nil {
        return partition.GetQueue(queueName)
    }

    return nil
}

func (m *ClusterSchedulingContext) AddSchedulingApplication(schedulingApp *SchedulingApplication) error {
    partitionName := schedulingApp.ApplicationInfo.Partition
    appId := schedulingApp.ApplicationInfo.ApplicationId

    m.lock.Lock()
    defer m.lock.Unlock()

    if partition := m.partitions[partitionName]; partition != nil {
        if err := partition.AddSchedulingApplication(schedulingApp); err != nil {
            return err
        }
    } else {
        return errors.New(fmt.Sprintf("Failed to find partition=%s while adding app=%s", partitionName, appId))
    }

    return nil
}

func (m *ClusterSchedulingContext) RemoveSchedulingApplication(appId string, partitionName string) (*SchedulingApplication, error) {
    m.lock.Lock()
    defer m.lock.Unlock()

    if partition := m.partitions[partitionName]; partition != nil {
        schedulingApp, err := partition.RemoveSchedulingApplication(appId, partitionName)
        if err != nil {
            return nil, err
        }
        return schedulingApp, nil
    } else {
        return nil, errors.New(fmt.Sprintf("Failed to find partition=%s while remove app=%s", partitionName, appId))
    }
}

func (m *ClusterSchedulingContext) updateSchedulingPartitions(partitions []*cache.PartitionInfo) error {
    m.lock.Lock()
    defer m.lock.Unlock()

    for _, updatedPartition := range partitions {
        if partition := m.partitions[updatedPartition.Name]; partition != nil {
            return errors.New(fmt.Sprintf("Update partition is not supported yet, partition=%s existed.", updatedPartition.Name))
        }
    }

    for _, updatedPartition := range partitions {
        newPartition := newPartitionSchedulingContext(updatedPartition.RMId)
        newPartition.Root = NewSchedulingQueueInfo(updatedPartition.Root)
        newPartition.Root.GetFlatChildrenQueues(newPartition.queues)

        name := common.GetNormalizedPartitionName(updatedPartition.Name, updatedPartition.RMId)
        m.partitions[name] = newPartition
    }

    return nil
}

func (m *ClusterSchedulingContext) RemoveSchedulingPartition(partitionName string) {
    m.lock.Lock()
    defer m.lock.Unlock()
    delete(m.partitions, partitionName)
}
