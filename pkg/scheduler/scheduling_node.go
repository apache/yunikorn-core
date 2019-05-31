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
    "github.com/golang/glog"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/cache"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/plugins"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/plugins/predicates"
    "sync"
)

type SchedulingNode struct {
    NodeInfo *cache.NodeInfo
    NodeId   string
    RmID     string

    // Resource which is allocating (in addition to confirmed, allocated)
    AllocatingResource      *resources.Resource
    PreemptingResource      *resources.Resource
    CachedAvailableResource *resources.Resource

    lock sync.RWMutex
}

func NewSchedulingNode(info *cache.NodeInfo) *SchedulingNode {
    return &SchedulingNode{
        NodeInfo:                info,
        NodeId:                  info.NodeId,
        AllocatingResource:      resources.NewResource(),
        PreemptingResource:      resources.NewResource(),
        CachedAvailableResource: info.GetAvailableResource(),
        RmID:                    info.RMId,
    }
}

func (m *SchedulingNode) CheckAndAllocateResource(delta *resources.Resource, preemptionPhase bool) bool {
    m.lock.Lock()
    m.lock.Unlock()
    newAllocating := resources.Add(delta, m.AllocatingResource)

    avail := m.CachedAvailableResource
    if preemptionPhase {
        avail = resources.Add(avail, m.PreemptingResource)
    }

    if resources.FitIn(avail, newAllocating) {
        m.AllocatingResource = newAllocating
        return true
    }
    return false
}

func (m *SchedulingNode) CheckAllocateConditions(allocId string) bool {
    // this will call shim function...
    plugin := plugins.GetSchedulerPlugin(plugins.PredicatesPluginName)

    var pp predicates.PredicatePlugin
    switch t := plugin.(type) {
    case predicates.PredicatePlugin:
        pp = t
    default:
        return false
    }

    glog.V(4).Infof("eval predicates for allocation(%s) on node(%s)", allocId, m.NodeId)
    if err := pp.EvalPredicates(allocId, m.NodeId); err != nil {
        glog.V(4).Infof("eval predicates for allocation(%s) on node(%s) failed, reason: %s",
            allocId, m.NodeId, err.Error())
        return false
    }

    return true
}
