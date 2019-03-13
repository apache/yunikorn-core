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
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/cache"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/common/resources"
    "sync"
)

type SchedulingNode struct {
    NodeInfo *cache.NodeInfo
    NodeId   string

    // Resource which is allocating (in addition to confirmed, allocated)
    AllocatingResource *resources.Resource

    lock sync.RWMutex
}

func NewSchedulingNode(info *cache.NodeInfo) *SchedulingNode {
    return &SchedulingNode{
        NodeInfo:           info,
        NodeId:             info.NodeId,
        AllocatingResource: resources.NewResource(),
    }
}

func (m *SchedulingNode) CheckAndAllocateResource(delta *resources.Resource) bool {
    m.lock.Lock()
    m.lock.Unlock()
    newAllocating := resources.Add(delta, m.AllocatingResource)

    if resources.FitIn(m.NodeInfo.AvailableResource, newAllocating) {
        m.AllocatingResource = newAllocating
        return true
    }
    return false
}
