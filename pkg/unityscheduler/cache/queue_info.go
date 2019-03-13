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
)

const (
    ROOT_QUEUE = "root"

    // How to sort job, valid options are fair / fifo
    JOB_SORT_POLICY = "job.sort.policy"
)

/* Related to queues */
type QueueInfo struct {
    Name string

    // Full qualified path include parents (split by ".")
    FullQualifiedPath string

    // When not set, max = nil
    MaxResource *resources.Resource

    // When not set, Guaranteed == 0
    GuaranteedResource *resources.Resource
    AllocatedResource  *resources.Resource
    Parent             *QueueInfo
    IsLeafQueue        bool // Allocation can be directly assigned under leaf queue only

    // Private fields need protection
    children   map[string]*QueueInfo // Only for direct children
    Properties map[string]string     // this should be treated as immutable

    lock sync.RWMutex
}

func NewQueueInfo(name string, parent *QueueInfo) (*QueueInfo) {
    q := &QueueInfo{Name: name, Parent: parent}
    if parent != nil {
        q.FullQualifiedPath = parent.FullQualifiedPath + "." + name
    }
    q.IsLeafQueue = true
    q.children = make(map[string]*QueueInfo)
    q.AllocatedResource = resources.NewResource()
    return q
}

func (m *QueueInfo) IncAllocatedResource(alloc *resources.Resource) {
    m.lock.Lock()
    defer m.lock.Unlock()
    m.AllocatedResource = resources.Add(m.AllocatedResource, alloc)
}

func (m *QueueInfo) DecAllocatedResource(alloc *resources.Resource) {
    m.lock.Lock()
    defer m.lock.Unlock()
    m.AllocatedResource = resources.Sub(m.AllocatedResource, alloc)
}

func (m *QueueInfo) GetCopyOfChildren() map[string]*QueueInfo {
    m.lock.RLock()
    defer m.lock.RUnlock()

    copy := make(map[string]*QueueInfo)
    for k, v := range m.children {
        copy[k] = v
    }

    return copy
}
