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
	"sort"
	"time"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
)

func sortQueue(queues []*Queue, sortType policies.SortPolicy, considerPriority bool) {
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
}

func sortQueuesByPriority(queues []*Queue) {
	sort.SliceStable(queues, func(i, j int) bool {
		l := queues[i]
		r := queues[j]
		lPriority := l.GetCurrentPriority()
		rPriority := r.GetCurrentPriority()
		return lPriority > rPriority
	})
}

func sortQueuesByPriorityAndFairness(queues []*Queue) {
	sort.SliceStable(queues, func(i, j int) bool {
		l := queues[i]
		r := queues[j]
		lPriority := l.GetCurrentPriority()
		rPriority := r.GetCurrentPriority()
		if lPriority > rPriority {
			return true
		}
		if lPriority < rPriority {
			return false
		}
		comp := resources.CompUsageRatioSeparately(l.GetAllocatedResource(), l.GetGuaranteedResource(),
			r.GetAllocatedResource(), r.GetGuaranteedResource())
		if comp == 0 {
			return resources.StrictlyGreaterThan(resources.Sub(l.GetPendingResource(), r.GetPendingResource()), resources.Zero)
		}
		return comp < 0
	})
}

func sortQueuesByFairnessAndPriority(queues []*Queue) {
	sort.SliceStable(queues, func(i, j int) bool {
		l := queues[i]
		r := queues[j]
		comp := resources.CompUsageRatioSeparately(l.GetAllocatedResource(), l.GetGuaranteedResource(),
			r.GetAllocatedResource(), r.GetGuaranteedResource())
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

func sortApplications(apps map[string]*Application, sortType policies.SortPolicy, considerPriority bool, globalResource *resources.Resource) []*Application {
	sortingStart := time.Now()
	var sortedApps []*Application
	switch sortType {
	case policies.FairSortPolicy:
		sortedApps = filterOnPendingResources(apps)
		if considerPriority {
			sortApplicationsByPriorityAndFairness(sortedApps, globalResource)
		} else {
			sortApplicationsByFairnessAndPriority(sortedApps, globalResource)
		}
	case policies.FifoSortPolicy:
		sortedApps = filterOnPendingResources(apps)
		if considerPriority {
			sortApplicationsByPriorityAndSubmissionTime(sortedApps)
		} else {
			sortApplicationsBySubmissionTimeAndPriority(sortedApps)
		}
	}
	metrics.GetSchedulerMetrics().ObserveAppSortingLatency(sortingStart)
	return sortedApps
}

func sortApplicationsByFairnessAndPriority(sortedApps []*Application, globalResource *resources.Resource) {
	sort.SliceStable(sortedApps, func(i, j int) bool {
		l := sortedApps[i]
		r := sortedApps[j]
		if comp := resources.CompUsageRatio(l.GetAllocatedResource(), r.GetAllocatedResource(), globalResource); comp != 0 {
			return comp < 0
		}
		return l.GetAskMaxPriority() > r.GetAskMaxPriority()
	})
}

func sortApplicationsByPriorityAndFairness(sortedApps []*Application, globalResource *resources.Resource) {
	sort.SliceStable(sortedApps, func(i, j int) bool {
		l := sortedApps[i]
		r := sortedApps[j]
		leftPriority := l.GetAskMaxPriority()
		rightPriority := r.GetAskMaxPriority()
		if leftPriority > rightPriority {
			return true
		}
		if leftPriority < rightPriority {
			return false
		}
		return resources.CompUsageRatio(l.GetAllocatedResource(), r.GetAllocatedResource(), globalResource) < 0
	})
}

func sortApplicationsBySubmissionTimeAndPriority(sortedApps []*Application) {
	sort.SliceStable(sortedApps, func(i, j int) bool {
		l := sortedApps[i]
		r := sortedApps[j]
		if l.SubmissionTime.Before(r.SubmissionTime) {
			return true
		}
		if r.SubmissionTime.Before(l.SubmissionTime) {
			return false
		}
		return l.GetAskMaxPriority() > r.GetAskMaxPriority()
	})
}

func sortApplicationsByPriorityAndSubmissionTime(sortedApps []*Application) {
	sort.SliceStable(sortedApps, func(i, j int) bool {
		l := sortedApps[i]
		r := sortedApps[j]
		leftPriority := l.GetAskMaxPriority()
		rightPriority := r.GetAskMaxPriority()
		if leftPriority > rightPriority {
			return true
		}
		if leftPriority < rightPriority {
			return false
		}
		return l.SubmissionTime.Before(r.SubmissionTime)
	})
}

func filterOnPendingResources(apps map[string]*Application) []*Application {
	filteredApps := make([]*Application, 0)
	for _, app := range apps {
		// Only look at app when pending-res > 0
		if resources.StrictlyGreaterThanZero(app.GetPendingResource()) {
			filteredApps = append(filteredApps, app)
		}
	}
	return filteredApps
}
