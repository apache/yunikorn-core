/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package scheduler

import (
	"sort"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
)

// Sort type for queues, apps, nodes etc.
type SortType int32

const (
	FairSortPolicy        = 0
	FifoSortPolicy        = 1
	MaxAvailableResources = 2 // node sorting, descending on available resources
	MinAvailableResources = 3 // node sorting, ascending on available resources
)

func sortQueue(queues []*SchedulingQueue, sortType SortType) {
	// TODO add latency metric
	if sortType == FairSortPolicy {
		sort.SliceStable(queues, func(i, j int) bool {
			l := queues[i]
			r := queues[j]
			comp := resources.CompUsageRatioSeparately(l.getAssumeAllocated(), l.QueueInfo.GetGuaranteedResource(),
				r.getAssumeAllocated(), r.QueueInfo.GetGuaranteedResource())
			return comp < 0
		})
	}
}

func sortApplications(apps []*SchedulingApplication, sortType SortType, globalResource *resources.Resource) {
	// TODO add latency metric
	switch sortType {
	case FairSortPolicy:
		// Sort by usage
		sort.SliceStable(apps, func(i, j int) bool {
			l := apps[i]
			r := apps[j]
			return resources.CompUsageRatio(l.getAssumeAllocated(), r.getAssumeAllocated(), globalResource) < 0
		})
	case FifoSortPolicy:
		// Sort by submission time oldest first
		sort.SliceStable(apps, func(i, j int) bool {
			l := apps[i]
			r := apps[j]
			return l.ApplicationInfo.SubmissionTime < r.ApplicationInfo.SubmissionTime
		})

		// fifo := &AppStateAwareFifoPolicy{}
		// fifo.Apply(apps)

		fifo := &BasicFifoPolicy{}
		fifo.Apply(apps)
	}
}

func sortNodes(nodes []*SchedulingNode, sortType SortType) {
	sortingStart := time.Now()
	switch sortType {
	case MaxAvailableResources:
		// Sort by available resource, descending order
		sort.SliceStable(nodes, func(i, j int) bool {
			l := nodes[i]
			r := nodes[j]
			return resources.CompUsageShares(l.GetAvailableResource(), r.GetAvailableResource()) > 0
		})
	case MinAvailableResources:
		// Sort by available resource, ascending order
		sort.SliceStable(nodes, func(i, j int) bool {
			l := nodes[i]
			r := nodes[j]
			return resources.CompUsageShares(r.GetAvailableResource(), l.GetAvailableResource()) > 0
		})
	}
	metrics.GetSchedulerMetrics().ObserveNodeSortingLatency(sortingStart)
}

func sortAskByPriority(requests []*schedulingAllocationAsk, ascending bool) {
	sort.SliceStable(requests, func(i, j int) bool {
		l := requests[i]
		r := requests[j]

		if ascending {
			return l.priority < r.priority
		}
		return l.priority > r.priority
	})
}
