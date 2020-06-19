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

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/maps"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

func sortQueue(queues []*SchedulingQueue, sortType policies.SortPolicy) {
	sortingStart := time.Now()
	if sortType == policies.FairSortPolicy {
		sort.SliceStable(queues, func(i, j int) bool {
			l := queues[i]
			r := queues[j]
			comp := resources.CompUsageRatioSeparately(l.getAssumeAllocated(), l.QueueInfo.GetGuaranteedResource(),
				r.getAssumeAllocated(), r.QueueInfo.GetGuaranteedResource())
			if comp == 0 {
				return resources.StrictlyGreaterThan(resources.Sub(l.pending, r.pending), resources.Zero)
			}
			return comp < 0
		})
	}
	metrics.GetSchedulerMetrics().ObserveQueueSortingLatency(sortingStart)
}

func sortApplications(apps map[string]*SchedulingApplication, sortType policies.SortPolicy, globalResource *resources.Resource) []*SchedulingApplication {
	sortingStart := time.Now()
	var sortedApps []*SchedulingApplication
	switch sortType {
	case policies.FairSortPolicy:
		sortedApps = filterOnPendingResources(apps)
		// Sort by usage
		sort.SliceStable(sortedApps, func(i, j int) bool {
			l := sortedApps[i]
			r := sortedApps[j]
			return resources.CompUsageRatio(l.getAssumeAllocated(), r.getAssumeAllocated(), globalResource) < 0
		})
	case policies.FifoSortPolicy:
		sortedApps = filterOnPendingResources(apps)
		// Sort by submission time oldest first
		sort.SliceStable(sortedApps, func(i, j int) bool {
			l := sortedApps[i]
			r := sortedApps[j]
			return l.ApplicationInfo.SubmissionTime < r.ApplicationInfo.SubmissionTime
		})
	case policies.StateAwarePolicy:
		sortedApps = stateAwareFilter(apps)
		// Sort by submission time oldest first
		sort.SliceStable(sortedApps, func(i, j int) bool {
			l := sortedApps[i]
			r := sortedApps[j]
			return l.ApplicationInfo.SubmissionTime < r.ApplicationInfo.SubmissionTime
		})
	}
	metrics.GetSchedulerMetrics().ObserveAppSortingLatency(sortingStart)
	return sortedApps
}

func filterOnPendingResources(apps map[string]*SchedulingApplication) []*SchedulingApplication {
	filteredApps := make([]*SchedulingApplication, 0)
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
func stateAwareFilter(apps map[string]*SchedulingApplication) []*SchedulingApplication {
	filteredApps := make([]*SchedulingApplication, 0)
	var acceptedApp *SchedulingApplication
	var foundStarting bool
	for _, app := range apps {
		// found a starting app clear out the accepted app (independent of pending resources)
		if app.isStarting() {
			foundStarting = true
			acceptedApp = nil
		}
		// Now just look at app when pending-res > 0
		if resources.StrictlyGreaterThanZero(app.GetPendingResource()) {
			// filter accepted apps
			if app.isAccepted() {
				// check if we have not seen a starting app
				// replace the currently tracked accepted app if this is an older one
				if !foundStarting && (acceptedApp == nil || acceptedApp.ApplicationInfo.SubmissionTime > app.ApplicationInfo.SubmissionTime) {
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

func sortNodes(nodes []*SchedulingNode, sortType common.SortingPolicy) {
	sortingStart := time.Now()
	switch sortType {
	case common.FairnessPolicy:
		// Sort by available resource, descending order
		sort.SliceStable(nodes, func(i, j int) bool {
			l := nodes[i]
			r := nodes[j]
			return resources.CompUsageShares(l.GetAvailableResource(), r.GetAvailableResource()) > 0
		})
	case common.BinPackingPolicy:
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

// This interface is used by scheduling queue to sort applications and
// get sorted pending requests from a specific application.
type AppRequestSorter interface {
	// sort applications
	sortApplications(apps []*SchedulingApplication, queueInfo *cache.QueueInfo)
	// get pending requests iterator,
	// there may be different orders or compositions of requests for different implementations.
	getPendingRequestIterator(app *SchedulingApplication) RequestIterator
}

type FairAppRequestSorter struct {
	AppRequestSorter
}

func (as *FairAppRequestSorter) sortApplications(apps []*SchedulingApplication, queueInfo *cache.QueueInfo) {
	// Sort by usage
	sort.SliceStable(apps, func(i, j int) bool {
		l := apps[i]
		r := apps[j]
		return resources.CompUsageRatio(l.getAssumeAllocated(), r.getAssumeAllocated(),
			queueInfo.GetGuaranteedResource()) < 0
	})
}

func (as *FairAppRequestSorter) getPendingRequestIterator(app *SchedulingApplication) RequestIterator {
	return app.requests.GetPendingRequestIterator()
}

type FifoAppRequestSorter struct {
	AppRequestSorter
}

func (as *FifoAppRequestSorter) sortApplications(apps []*SchedulingApplication, queueInfo *cache.QueueInfo) {
	// Sort first by priority, then by create time
	sort.SliceStable(apps, func(i, j int) bool {
		r := apps[j].requests.GetTopPendingPriorityGroup()
		l := apps[i].requests.GetTopPendingPriorityGroup()
		switch {
		case r == nil && l == nil:
			return apps[i].ApplicationInfo.SubmissionTime < apps[j].ApplicationInfo.SubmissionTime
		case r == nil:
			return true
		case l == nil:
			return false
		}

		if l.GetPriority() == r.GetPriority() {
			return l.GetCreateTime().Before(r.GetCreateTime())
		}
		return l.GetPriority() > r.GetPriority()
	})
}

func (as *FifoAppRequestSorter) getPendingRequestIterator(app *SchedulingApplication) RequestIterator {
	topPendingPriorityGroup := app.requests.GetTopPendingPriorityGroup()
	if topPendingPriorityGroup != nil {
		return topPendingPriorityGroup.GetPendingRequestIterator()
	}
	return NewSortedRequestIterator([]maps.MapIterator{})
}

func newAppRequestSorter(sortPolicy policies.SortPolicy) AppRequestSorter {
	switch sortPolicy {
	case policies.FairSortPolicy:
		return &FairAppRequestSorter{}
	case policies.FifoSortPolicy:
		return &FifoAppRequestSorter{}
	default:
		return &FairAppRequestSorter{}
	}
}
