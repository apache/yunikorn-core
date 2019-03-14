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
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/cache"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "sync"
)

// Represents Queue inside Scheduler
type SchedulingQueue struct {
    CachedQueueInfo *cache.QueueInfo

    Name string

    // Maybe allocated, set by scheduler
    MayAllocatedResource *resources.Resource

    // For fairness calculation
    PartitionResource *resources.Resource

    Parent      *SchedulingQueue
    IsLeafQueue bool // Allocation can be directly assigned under leaf queue only

    childrenQueues map[string]*SchedulingQueue // Only for direct children, parent queue only

    jobs map[string]*SchedulingJob // only for leaf queue

    // Total pending resource
    PendingResource *resources.Resource

    // How jobs are sorted (leaf queue only)
    JobSortType SortType

    // How sub queues are sorted (parent queue only)
    QueueSortType SortType

    lock sync.RWMutex
}

func NewSchedulingQueueInfo(cacheQueueInfo *cache.QueueInfo) *SchedulingQueue {
    schedulingQueue := &SchedulingQueue{}
    schedulingQueue.Name = cacheQueueInfo.Name
    schedulingQueue.CachedQueueInfo = cacheQueueInfo
    schedulingQueue.MayAllocatedResource = resources.NewResource()
    schedulingQueue.IsLeafQueue = cacheQueueInfo.IsLeafQueue
    schedulingQueue.childrenQueues = make(map[string]*SchedulingQueue)
    schedulingQueue.jobs = make(map[string]*SchedulingJob)
    schedulingQueue.PendingResource = resources.NewResource()

    // TODO, make them configurable
    if cacheQueueInfo.Properties[cache.JOB_SORT_POLICY] == "fair" {
        schedulingQueue.JobSortType = FAIR_SORT_POLICY
    } else {
        schedulingQueue.JobSortType = FIFO_SORT_POLICY
    }
    schedulingQueue.QueueSortType = FAIR_SORT_POLICY

    for childName, childQueue := range cacheQueueInfo.GetCopyOfChildren() {
        newChildQueue := NewSchedulingQueueInfo(childQueue)
        newChildQueue.Parent = schedulingQueue
        schedulingQueue.childrenQueues[childName] = newChildQueue
    }

    return schedulingQueue
}

// Update pending resource of this queue
func (m *SchedulingQueue) IncPendingResource(delta *resources.Resource) {
    m.lock.Lock()
    defer m.lock.Unlock()

    m.PendingResource = resources.Add(m.PendingResource, delta)
}

// Remove pending resource of this queue
func (m *SchedulingQueue) DecPendingResource(delta *resources.Resource) {
    m.lock.Lock()
    defer m.lock.Unlock()

    m.PendingResource = resources.Sub(m.PendingResource, delta)
}

func (m *SchedulingQueue) AddSchedulingJob(job *SchedulingJob) {
    m.lock.Lock()
    defer m.lock.Unlock()

    m.jobs[job.JobInfo.JobId] = job
}

func (m *SchedulingQueue) RemoveSchedulingJob(job *SchedulingJob) {
    m.lock.Lock()
    defer m.lock.Unlock()

    delete(m.jobs, job.JobInfo.JobId)
}

func (m *SchedulingQueue) GetFlatChildrenQueues(allQueues map[string]*SchedulingQueue) {
    m.lock.RLock()
    defer m.lock.RUnlock()

    if m == nil {
        return
    }

    // add self
    allQueues[m.Name] = m

    for childName, child := range m.childrenQueues {
        allQueues[childName] = child
        child.GetFlatChildrenQueues(allQueues)
    }
}
