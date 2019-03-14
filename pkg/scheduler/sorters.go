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
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "sort"
)

// Sort queues, jobs, etc.

type SortType int32

const (
    FAIR_SORT_POLICY = 0
    FIFO_SORT_POLICY = 1
)

func SortQueue(queues []*SchedulingQueue, sortType SortType) {
    if sortType == FAIR_SORT_POLICY {
        sort.SliceStable(queues, func(i, j int) bool {
            l := queues[i]
            r := queues[j]

            comp, _ := resources.CompFairnessRatio(l.MayAllocatedResource, l.CachedQueueInfo.GuaranteedResource, r.MayAllocatedResource, l.CachedQueueInfo.GuaranteedResource)
            return comp < 0
        })
    }
}

func SortJobs(queues []*SchedulingJob, sortType SortType, globalResource *resources.Resource) {
    if sortType == FAIR_SORT_POLICY {
        sort.SliceStable(queues, func(i, j int) bool {
            l := queues[i]
            r := queues[j]

            comp, _ := resources.CompFairnessRatio(l.MayAllocatedResource, globalResource, r.MayAllocatedResource, globalResource)
            return comp < 0
        })
    } else if sortType == FIFO_SORT_POLICY {
        sort.SliceStable(queues, func(i, j int) bool {
            l := queues[i]
            r := queues[j]
            return l.JobInfo.SubmissionTime < r.JobInfo.SubmissionTime
        })
    }
}
