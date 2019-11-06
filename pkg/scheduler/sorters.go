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
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/metrics"
    "sort"
    "time"
)

// Sort queues, apps, etc.

type SortType int32

const (
    FairSortPolicy        = 0
    FifoSortPolicy        = 1
    MaxAvailableResources = 2
)

func SortQueue(queues []*SchedulingQueue, sortType SortType) {
    if sortType == FairSortPolicy {
        sort.SliceStable(queues, func(i, j int) bool {
            l := queues[i]
            r := queues[j]

            comp := resources.CompUsageRatio(l.ProposingResource, r.ProposingResource, l.CachedQueueInfo.GuaranteedResource)
            return comp < 0
        })
    }
}

func SortApplications(queues []*SchedulingApplication, sortType SortType, globalResource *resources.Resource) {
    if sortType == FairSortPolicy {
        sort.SliceStable(queues, func(i, j int) bool {
            l := queues[i]
            r := queues[j]

            comp := resources.CompUsageRatio(l.MayAllocatedResource, r.MayAllocatedResource, globalResource)
            return comp < 0
        })
    } else if sortType == FifoSortPolicy {
        sort.SliceStable(queues, func(i, j int) bool {
            l := queues[i]
            r := queues[j]
            return l.ApplicationInfo.SubmissionTime < r.ApplicationInfo.SubmissionTime
        })
    }
}

func SortNodes(nodes []*SchedulingNode, sortType SortType) {
    if sortType == MaxAvailableResources {
        sortingStart := time.Now()
        sort.SliceStable(nodes, func(i, j int) bool {
            l := nodes[i]
            r := nodes[j]

            // Sort by available resource, descending order
            return resources.CompUsageShares(l.CachedAvailableResource, r.CachedAvailableResource) > 0
        })
        metrics.GetSchedulerMetrics().ObserveNodeSortingLatency(sortingStart)
    }
}

// Sort nodes here.
func SortAllNodesWithAscendingResource(schedulingNodeList []*SchedulingNode) []*SchedulingNode {

    sortingStart := time.Now()
    sort.SliceStable(schedulingNodeList, func(i, j int) bool {
        l := schedulingNodeList[i]
        r := schedulingNodeList[j]

        // Sort by available resource, ascending order
        return resources.CompUsageShares(r.CachedAvailableResource, l.CachedAvailableResource) > 0
    })

    metrics.GetSchedulerMetrics().ObserveNodeSortingLatency(sortingStart)

    return schedulingNodeList
}
