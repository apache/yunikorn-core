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

func sortQueue(queues []*Queue, sortType policies.SortPolicy) {
	sortingStart := time.Now()
	if sortType == policies.FairSortPolicy {
		sort.SliceStable(queues, func(i, j int) bool {
			l := queues[i]
			r := queues[j]
			comp := resources.CompUsageRatioSeparately(l.GetAllocatedResource(), l.GetGuaranteedResource(),
				r.GetAllocatedResource(), r.GetGuaranteedResource())
			if comp == 0 {
				return resources.StrictlyGreaterThan(resources.Sub(l.pending, r.pending), resources.Zero)
			}
			return comp < 0
		})
	}
	metrics.GetSchedulerMetrics().ObserveQueueSortingLatency(sortingStart)
}

func sortApplications(apps map[string]*Application, sortType policies.SortPolicy, globalResource *resources.Resource) []*Application {
	sortingStart := time.Now()
	var sortedApps []*Application
	switch sortType {
	case policies.FairSortPolicy:
		sortedApps = filterOnPendingResources(apps)
		// Sort by usage
		sort.SliceStable(sortedApps, func(i, j int) bool {
			l := sortedApps[i]
			r := sortedApps[j]
			return resources.CompUsageRatio(l.GetAllocatedResource(), r.GetAllocatedResource(), globalResource) < 0
		})
	case policies.FifoSortPolicy:
		sortedApps = filterOnPendingResources(apps)
		// Sort by submission time oldest first
		sort.SliceStable(sortedApps, func(i, j int) bool {
			l := sortedApps[i]
			r := sortedApps[j]
			return l.SubmissionTime.Before(r.SubmissionTime)
		})
	case policies.StateAwarePolicy:
		sortedApps = stateAwareFilter(apps)
		// Sort by submission time oldest first
		sort.SliceStable(sortedApps, func(i, j int) bool {
			l := sortedApps[i]
			r := sortedApps[j]
			return l.SubmissionTime.Before(r.SubmissionTime)
		})
	}
	metrics.GetSchedulerMetrics().ObserveAppSortingLatency(sortingStart)
	return sortedApps
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

func sortAskByPriority(requests []*AllocationAsk, ascending bool) {
	sort.SliceStable(requests, func(i, j int) bool {
		l := requests[i]
		r := requests[j]

		if l.GetPriority() == r.GetPriority() {
			return l.GetCreateTime().Before(r.GetCreateTime())
		}

		if ascending {
			return l.GetPriority() < r.GetPriority()
		}
		return l.GetPriority() > r.GetPriority()
	})
}
