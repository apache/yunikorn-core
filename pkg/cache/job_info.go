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
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/common/resources"
    "sync"
    "time"
)

/* Related to Jobs */
type JobInfo struct {
    JobId string

    AllocatedResource *resources.Resource

    Partition string

    QueueName string

    LeafQueue *QueueInfo

    // Private fields need protection
    allocations map[string]*AllocationInfo

    SubmissionTime int64

    lock sync.RWMutex
}

func (m *JobInfo) GetAllocation(uuid string) *AllocationInfo {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.allocations[uuid]
}

func NewJobInfo(jobId string, partition, queueName string) *JobInfo {
    j := &JobInfo{JobId: jobId}
    j.AllocatedResource = resources.NewResource()
    j.allocations = make(map[string]*AllocationInfo)
    j.Partition = partition
    j.QueueName = queueName
    j.SubmissionTime = time.Now().UnixNano()
    return j
}

func (m *JobInfo) NewJobInfo(jobId string) {
}

func (m *JobInfo) AddAllocation(info *AllocationInfo) {
    m.lock.Lock()
    defer m.lock.Unlock()

    m.allocations[info.AllocationProto.Uuid] = info
    m.AllocatedResource = resources.Add(m.AllocatedResource, info.AllocatedResource)
}

func (m *JobInfo) RemoveAllocation(uuid string) *AllocationInfo {
    m.lock.Lock()
    defer m.lock.Unlock()

    alloc := m.allocations[uuid]

    if alloc != nil {
        // When job has the allocation, update map, and update allocated resource of the job
        m.AllocatedResource = resources.Sub(m.AllocatedResource, alloc.AllocatedResource)
        delete(m.allocations, uuid)
        return alloc
    }

    return nil
}

func (m *JobInfo) CleanupAllAllocations() []*AllocationInfo {
    allocationsToRelease := make([]*AllocationInfo, 0)

    m.lock.Lock()
    defer m.lock.Unlock()

    for _, alloc := range m.allocations {
        allocationsToRelease = append(allocationsToRelease, alloc)
    }
    // cleanup allocated resource for job
    m.AllocatedResource = resources.NewResource()
    m.allocations = make(map[string]*AllocationInfo)

    return allocationsToRelease
}
