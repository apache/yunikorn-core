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
    "github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/commonevents"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
)

/* Related to Allocation */
type AllocationInfo struct {
    // Original protocol
    AllocationProto *si.Allocation

    // Other information
    JobId             string
    AllocatedResource *resources.Resource
}

func NewAllocationInfo(uuid string, alloc *commonevents.AllocationProposal) *AllocationInfo {
    allocation := &AllocationInfo{
        AllocationProto: &si.Allocation{
            AllocationKey:    alloc.AllocationKey,
            AllocationTags:   alloc.Tags,
            Uuid:             uuid,
            ResourcePerAlloc: alloc.AllocatedResource.ToProto(),
            Priority:         alloc.Priority,
            QueueName:        alloc.QueueName,
            NodeId:           alloc.NodeId,
            Partition:        common.GetPartitionNameWithoutClusterId(alloc.PartitionName),
            JobId:            alloc.JobId,
        },
        JobId:             alloc.JobId,
        AllocatedResource: alloc.AllocatedResource,
    }

    return allocation
}
