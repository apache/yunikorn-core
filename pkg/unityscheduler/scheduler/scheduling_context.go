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
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/cache"
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
    jobs   map[string]*SchedulingJob
    queues map[string]*SchedulingQueue
    Root   *SchedulingQueue
    RmId   string

    lock sync.RWMutex
}

func (m *PartitionSchedulingContext) AddSchedulingJob(schedulingJob *SchedulingJob) error {
    m.lock.Lock()
    defer m.lock.Unlock()

    // Add to jobs
    jobId := schedulingJob.JobInfo.JobId
    if m.jobs[jobId] != nil {
        return errors.New(fmt.Sprintf("Adding job=%s to partition=%s, but job already existed.", jobId, schedulingJob.JobInfo.Partition))
    }

    m.jobs[jobId] = schedulingJob

    // Put job under queue
    schedulingQueue := m.queues[schedulingJob.JobInfo.QueueName]
    if schedulingQueue == nil {
        return errors.New(fmt.Sprintf("Failed to find queue=%s for job=%s", schedulingJob.JobInfo.QueueName, schedulingJob.JobInfo.JobId))
    }
    schedulingJob.ParentQueue = schedulingQueue
    schedulingQueue.AddSchedulingJob(schedulingJob)

    return nil
}

func (m *PartitionSchedulingContext) RemoveSchedulingJob(jobId string, partitionName string) (*SchedulingJob, error) {
    m.lock.Lock()
    defer m.lock.Unlock()

    // Remove from jobs map
    if m.jobs[jobId] == nil {
        return nil, errors.New(fmt.Sprintf("Removing job=%s to partition=%s, but job is non-existed.", jobId, partitionName))
    }
    schedulingJob := m.jobs[jobId]
    delete(m.jobs, jobId)

    // Remove job under queue
    schedulingQueue := m.queues[schedulingJob.JobInfo.QueueName]
    if schedulingQueue == nil {
        // This is not normal
        panic(fmt.Sprintf("Failed to find queue=%s for job=%s while removing job", schedulingJob.JobInfo.QueueName, schedulingJob.JobInfo.JobId))
    }
    schedulingJob.ParentQueue = schedulingQueue
    schedulingQueue.RemoveSchedulingJob(schedulingJob)

    // Update pending resource of queues
    totalPending := schedulingJob.Requests.TotalPendingResource
    queue := schedulingJob.ParentQueue
    for queue != nil {
        queue.DecPendingResource(totalPending)
        queue = queue.Parent
    }

    return schedulingJob, nil
}

// Visible by tests
func (m *PartitionSchedulingContext) GetQueue(queueName string) *SchedulingQueue {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.queues[queueName]
}

func (m *PartitionSchedulingContext) getJob(jobId string) *SchedulingJob {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.jobs[jobId]
}

func newPartitionSchedulingContext(rmId string) *PartitionSchedulingContext {
    return &PartitionSchedulingContext{jobs: make(map[string]*SchedulingJob), queues: make(map[string]*SchedulingQueue), RmId: rmId}
}

func (m *ClusterSchedulingContext) GetPartitionSchedulingContext(partitionName string) *PartitionSchedulingContext {
    m.lock.RLock()
    defer m.lock.RUnlock()
    return m.partitions[partitionName]
}

func (m *ClusterSchedulingContext) GetSchedulingJob(jobId string, partitionName string) *SchedulingJob {
    m.lock.RLock()
    defer m.lock.RUnlock()

    if partition := m.partitions[partitionName]; partition != nil {
        return partition.getJob(jobId)
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

func (m *ClusterSchedulingContext) AddSchedulingJob(schedulingJob *SchedulingJob) error {
    partitionName := schedulingJob.JobInfo.Partition
    jobId := schedulingJob.JobInfo.JobId

    m.lock.Lock()
    defer m.lock.Unlock()

    if partition := m.partitions[partitionName]; partition != nil {
        if err := partition.AddSchedulingJob(schedulingJob); err != nil {
            return err
        }
    } else {
        return errors.New(fmt.Sprintf("Failed to find partition=%s while adding job=%s", partitionName, jobId))
    }

    return nil
}

func (m *ClusterSchedulingContext) RemoveSchedulingJob(jobId string, partitionName string) (*SchedulingJob, error) {
    m.lock.Lock()
    defer m.lock.Unlock()

    if partition := m.partitions[partitionName]; partition != nil {
        schedulingJob, err := partition.RemoveSchedulingJob(jobId, partitionName)
        if err != nil {
            return nil, err
        }
        return schedulingJob, nil
    } else {
        return nil, errors.New(fmt.Sprintf("Failed to find partition=%s while remove job=%s", partitionName, jobId))
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

        m.partitions[updatedPartition.Name] = newPartition
    }

    return nil
}

func (m *ClusterSchedulingContext) RemoveSchedulingPartition(partitionName string) {
    m.lock.Lock()
    defer m.lock.Unlock()
    delete(m.partitions, partitionName)
}
