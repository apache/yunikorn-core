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

package scheduler

import (
    "github.com/cloudera/scheduler-interface/lib/go/si"
    "github.com/cloudera/yunikorn-core/pkg/cache"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "github.com/cloudera/yunikorn-core/pkg/plugins"
    "go.uber.org/zap"
    "sync"
)

type SchedulingNode struct {
    NodeInfo *cache.NodeInfo
    NodeId   string

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

// Checking pre allocation conditions. The pre-allocation conditions are implemented via plugins in the shim.
// If no plugins are implemented then the check will return true. If multiple plugins are implemented the first failure
// will stop the checks.
// The caller must not rely on all plugins being executed.
func (m *SchedulingNode) CheckAllocateConditions(allocId string) bool {
    // Check the predicates plugin (k8shim)
    if plugin := plugins.GetPredicatesPlugin(); plugin != nil {
        log.Logger.Debug("predicates",
            zap.String("allocationId", allocId),
            zap.String("nodeId", m.NodeId))
        if err := plugin.Predicates(&si.PredicatesArgs{
            AllocationKey: allocId,
            NodeId: m.NodeId,
        }); err != nil {
            log.Logger.Debug("running predicates failed",
                zap.String("allocationId", allocId),
                zap.String("nodeId", m.NodeId),
                zap.Error(err))
            return false
        }
    }

    // Check the volumes plugin (k8shim)
    if plugin := plugins.GetVolumesPlugin(); plugin != nil {
        log.Logger.Debug("check volumes",
            zap.String("allocationId", allocId),
            zap.String("nodeId", m.NodeId))
        if err := plugin.VolumesCheck(allocId, m.NodeId); err != nil {
            log.Logger.Debug("volumes check failed",
                zap.String("allocationId", allocId),
                zap.String("nodeId", m.NodeId),
                zap.Error(err))
            return false
        }
    }
    // must be last return in the list
    return true
}
