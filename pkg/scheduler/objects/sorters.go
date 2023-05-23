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
			return resources.StrictlyGreaterThan(resources.Sub(l.pending, r.pending), resources.Zero)
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
			return resources.StrictlyGreaterThan(resources.Sub(l.pending, r.pending), resources.Zero)
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
	case policies.StateAwarePolicy:
		sortedApps = stateAwareFilter(apps)
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

// This filter only allows one (1) application with a state that is not running in the list of candidates.
// The preference is a state of Starting. If we can not find an app with a starting state we will use an app
// with an Accepted state. However if there is an app with a Starting state even with no pending resource
// requests, no Accepted apps can be scheduled. An app in New state does not have any asks and can never be
// scheduled.
func stateAwareFilter(apps map[string]*Application) []*Application {
	filteredApps := make([]*Application, 0)
	var acceptedApp *Application
	var foundStarting bool
	for _, app := range apps {
		// found a starting app clear out the accepted app (independent of pending resources)
		if app.IsStarting() {
			foundStarting = true
			acceptedApp = nil
		}
		// Now just look at app when pending-res > 0
		if resources.StrictlyGreaterThanZero(app.GetPendingResource()) {
			// filter accepted apps
			if app.IsAccepted() {
				// check if we have not seen a starting app
				// replace the currently tracked accepted app if this is an older one
				if !foundStarting && (acceptedApp == nil || acceptedApp.SubmissionTime.After(app.SubmissionTime)) {
					acceptedApp = app
				}
				continue
			}
			// this is a running or starting app add it to the list
			filteredApps = append(filteredApps, app)
		}
	}
	// just add the accepted app if we need to: apps are not sorted yet
	if acceptedApp != nil {
		filteredApps = append(filteredApps, acceptedApp)
	}
	return filteredApps
}
