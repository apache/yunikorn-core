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
    "github.com/golang/glog"
    "github.com/looplab/fsm"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "sync"
    "time"
)

/* Related to applications */
type ApplicationInfo struct {
    ApplicationId string

    allocatedResource *resources.Resource

    Partition string

    QueueName string

    LeafQueue *QueueInfo

    // Private fields need protection
    allocations map[string]*AllocationInfo

    // application state machine
    fsm *fsm.FSM

    SubmissionTime int64

    lock sync.RWMutex
}

func (m* ApplicationInfo) GetAllocatedResource() *resources.Resource {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.allocatedResource
}

func (m *ApplicationInfo) GetAllocation(uuid string) *AllocationInfo {
    m.lock.RLock()
    defer m.lock.RUnlock()

    return m.allocations[uuid]
}

func NewApplicationInfo(appId string, partition, queueName string) *ApplicationInfo {
    j := &ApplicationInfo{ApplicationId: appId}
    j.allocatedResource = resources.NewResource()
    j.allocations = make(map[string]*AllocationInfo)
    j.Partition = partition
    j.QueueName = queueName
    j.SubmissionTime = time.Now().UnixNano()
    j.fsm = fsm.NewFSM(
        New.String(), fsm.Events{
            {Name: AcceptApplication.String(), Src: []string{New.String()}, Dst: Accepted.String()},
            {Name: RejectApplication.String(), Src: []string{New.String()}, Dst: Rejected.String()},
            {Name: RunApplication.String(), Src: []string{Accepted.String(), Running.String()}, Dst: Running.String()},
            {Name: CompleteApplication.String(), Src: []string{Running.String()}, Dst: Completed.String()},
        }, fsm.Callbacks{
            "enter_state": func(event *fsm.Event) {
                glog.V(0).Infof(
                    "application %s transited to state %s from %s, on event %s",
                    j.ApplicationId, event.Dst, event.Src, event.Event)
            },
        })
    return j
}

func (m *ApplicationInfo) AddAllocation(info *AllocationInfo) {
    m.lock.Lock()
    defer m.lock.Unlock()

    m.allocations[info.AllocationProto.Uuid] = info
    m.allocatedResource = resources.Add(m.allocatedResource, info.AllocatedResource)
}

func (m *ApplicationInfo) RemoveAllocation(uuid string) *AllocationInfo {
    m.lock.Lock()
    defer m.lock.Unlock()

    alloc := m.allocations[uuid]

    if alloc != nil {
        // When app has the allocation, update map, and update allocated resource of the app
        m.allocatedResource = resources.Sub(m.allocatedResource, alloc.AllocatedResource)
        delete(m.allocations, uuid)
        return alloc
    }

    return nil
}

func (m *ApplicationInfo) CleanupAllAllocations() []*AllocationInfo {
    allocationsToRelease := make([]*AllocationInfo, 0)

    m.lock.Lock()
    defer m.lock.Unlock()

    for _, alloc := range m.allocations {
        allocationsToRelease = append(allocationsToRelease, alloc)
    }
    // cleanup allocated resource for app
    m.allocatedResource = resources.NewResource()
    m.allocations = make(map[string]*AllocationInfo)

    return allocationsToRelease
}

func (m *ApplicationInfo) GetAllAllocations() []*AllocationInfo {
    m.lock.RLock()
    defer m.lock.RUnlock()

    var allocations []*AllocationInfo
    for _, alloc := range m.allocations {
        allocations = append(allocations, alloc)
    }
    return allocations
}

func (m *ApplicationInfo) GetApplicationState() string {
    return m.fsm.Current()
}

func (m *ApplicationInfo) HandleApplicationEvent(event ApplicationEvent) error {
    return m.fsm.Event(event.String())
}
