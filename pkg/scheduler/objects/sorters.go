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

package objects

import (
	"cmp"
	"slices"
	"sort"
	"time"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
)

// queueFairMax ties a queue to the fair max its usage is compared against. The sort moves the
// queue and its own fair max as one element, an index into a second list does not follow a swap.
type queueFairMax struct {
	queue   *Queue
	fairMax *resources.Resource
}

func sortQueue(queues []queueFairMax, sortType policies.SortPolicy, considerPriority bool) []*Queue {
	sortingStart := time.Now()
	if sortType == policies.FairSortPolicy {
		if considerPriority {
			sortQueuesByPriorityAndFairness(queues)
		} else {
			sortQueuesByFairnessAndPriority(queues)
		}
	} else {
		if considerPriority {
			sortQueuesByPriority(queues)
		}
	}
	metrics.GetSchedulerMetrics().ObserveQueueSortingLatency(sortingStart)
	sorted := make([]*Queue, 0, len(queues))
	for _, entry := range queues {
		sorted = append(sorted, entry.queue)
	}
	return sorted
}

func sortQueuesByPriority(queues []queueFairMax) {
	sort.SliceStable(queues, func(i, j int) bool {
		l := queues[i].queue
		r := queues[j].queue
		lPriority := l.GetCurrentPriority()
		rPriority := r.GetCurrentPriority()
		return lPriority > rPriority
	})
}

func sortQueuesByPriorityAndFairness(queues []queueFairMax) {
	sort.SliceStable(queues, func(i, j int) bool {
		l := queues[i].queue
		r := queues[j].queue
		lPriority := l.GetCurrentPriority()
		rPriority := r.GetCurrentPriority()
		if lPriority > rPriority {
			return true
		}
		if lPriority < rPriority {
			return false
		}

		comp := resources.CompUsageRatioSeparately(l.GetAllocatedResource(), l.GetGuaranteedResource(), queues[i].fairMax,
			r.GetAllocatedResource(), r.GetGuaranteedResource(), queues[j].fairMax)

		if comp == 0 {
			return resources.StrictlyGreaterThan(resources.Sub(l.GetPendingResource(), r.GetPendingResource()), resources.Zero)
		}
		return comp < 0
	})
}

func sortQueuesByFairnessAndPriority(queues []queueFairMax) {
	sort.SliceStable(queues, func(i, j int) bool {
		l := queues[i].queue
		r := queues[j].queue

		comp := resources.CompUsageRatioSeparately(l.GetAllocatedResource(), l.GetGuaranteedResource(), queues[i].fairMax,
			r.GetAllocatedResource(), r.GetGuaranteedResource(), queues[j].fairMax)
		if comp == 0 {
			lPriority := l.GetCurrentPriority()
			rPriority := r.GetCurrentPriority()
			if lPriority > rPriority {
				return true
			}
			if lPriority < rPriority {
				return false
			}
			return resources.StrictlyGreaterThan(resources.Sub(l.GetPendingResource(), r.GetPendingResource()), resources.Zero)
		}
		return comp < 0
	})
}

// appSortKey ties an application to the sort inputs read from it. The sort moves the app and its
// own inputs as one element, an index into a second list does not follow a swap.
type appSortKey struct {
	app    *Application
	shares []float64
	// a higher priority sorts first, so every compare on priority is reversed
	priority       int32
	submissionTime time.Time
}

func sortApplications(apps map[string]*Application, sortType policies.SortPolicy, considerPriority bool, globalResource *resources.Resource) []*Application {
	sortingStart := time.Now()
	sortedApps := sortFilteredApplications(filterOnPendingResources(apps), sortType, considerPriority, globalResource)
	metrics.GetSchedulerMetrics().ObserveAppSortingLatency(sortingStart)
	return sortedApps
}

// sortFilteredApplications sorts the applications in the order the policy asks for. Each app is
// read once into a key, the keys are sorted, so a comparison never locks an app.
func sortFilteredApplications(apps []*Application, sortType policies.SortPolicy, considerPriority bool, globalResource *resources.Resource) []*Application {
	fair := sortType == policies.FairSortPolicy
	keys := make([]appSortKey, 0, len(apps))
	for _, app := range apps {
		keys = append(keys, app.sortKey(globalResource, fair))
	}
	switch {
	case sortType == policies.FairSortPolicy && considerPriority:
		sortApplicationsByPriorityAndFairness(keys)
	case sortType == policies.FairSortPolicy:
		sortApplicationsByFairnessAndPriority(keys)
	case sortType == policies.FifoSortPolicy && considerPriority:
		sortApplicationsByPriorityAndSubmissionTime(keys)
	case sortType == policies.FifoSortPolicy:
		sortApplicationsBySubmissionTimeAndPriority(keys)
	}
	sorted := make([]*Application, 0, len(keys))
	for _, key := range keys {
		sorted = append(sorted, key.app)
	}
	return sorted
}

func sortApplicationsByFairnessAndPriority(keys []appSortKey) {
	slices.SortStableFunc(keys, func(l, r appSortKey) int {
		if comp := resources.CompareShares(l.shares, r.shares); comp != 0 {
			return comp
		}
		return cmp.Compare(r.priority, l.priority)
	})
}

func sortApplicationsByPriorityAndFairness(keys []appSortKey) {
	slices.SortStableFunc(keys, func(l, r appSortKey) int {
		if comp := cmp.Compare(r.priority, l.priority); comp != 0 {
			return comp
		}
		return resources.CompareShares(l.shares, r.shares)
	})
}

func sortApplicationsBySubmissionTimeAndPriority(keys []appSortKey) {
	slices.SortStableFunc(keys, func(l, r appSortKey) int {
		if comp := l.submissionTime.Compare(r.submissionTime); comp != 0 {
			return comp
		}
		return cmp.Compare(r.priority, l.priority)
	})
}

func sortApplicationsByPriorityAndSubmissionTime(keys []appSortKey) {
	slices.SortStableFunc(keys, func(l, r appSortKey) int {
		if comp := cmp.Compare(r.priority, l.priority); comp != 0 {
			return comp
		}
		return l.submissionTime.Compare(r.submissionTime)
	})
}

func filterOnPendingResources(apps map[string]*Application) []*Application {
	filteredApps := make([]*Application, 0)
	for _, app := range apps {
		// Only look at app when pending-res > 0
		if app.HasPendingResource() {
			filteredApps = append(filteredApps, app)
		}
	}
	return filteredApps
}
