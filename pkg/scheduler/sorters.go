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

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

// Queues can only be sorted on usage.
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

// Sort applications: each sort type can filter the applications before sorting.
// The filtering is dependent on the sorting policy used.
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
			return l.ApplicationInfo.SubmissionTime.Before(r.ApplicationInfo.SubmissionTime)
		})
	case policies.StateAwarePolicy:
		sortedApps = stateAwareFilter(apps)
		// Sort by submission time oldest first
		sort.SliceStable(sortedApps, func(i, j int) bool {
			l := sortedApps[i]
			r := sortedApps[j]
			return l.ApplicationInfo.SubmissionTime.Before(r.ApplicationInfo.SubmissionTime)
		})
	}
	metrics.GetSchedulerMetrics().ObserveAppSortingLatency(sortingStart)
	return sortedApps
}

// Filter applications and only return applications with pending resources
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
				if !foundStarting && (acceptedApp == nil || acceptedApp.ApplicationInfo.SubmissionTime.After(app.ApplicationInfo.SubmissionTime)) {
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

// Sort requests inside an application.
// Requests are filtered and only requests with a pending repeats are returned
func sortRequests(requests map[string]*schedulingAllocationAsk, sortType policies.SortPolicy) []*schedulingAllocationAsk {
	var sortedRequests []*schedulingAllocationAsk
	for _, request := range requests {
		if request.getPendingAskRepeat() == 0 {
			continue
		}
		sortedRequests = append(sortedRequests, request)
	}
	switch sortType {
	case policies.FifoSortPolicy:
		// Sort by create time
		sort.SliceStable(sortedRequests, func(i, j int) bool {
			l := sortedRequests[i]
			r := sortedRequests[j]
			return l.createTime.Before(r.createTime)
		})
	case policies.PriorityPolicy:
		// Sort by priority, then by create time stamp
		sort.SliceStable(sortedRequests, func(i, j int) bool {
			l := sortedRequests[i]
			r := sortedRequests[j]
			return sortRequestsByPriority(l, r)
		})
	}
	return sortedRequests
}

// Sort the requests based on the priority of the request.
// The higher the number the higher the priority.
// Sorting happens in an descending order.
// If the request priority is the same the oldest one is considered to have the higher priority.
func sortRequestsByPriority(l, r *schedulingAllocationAsk) bool {
	if l.priority == r.priority {
		return l.createTime.Before(r.createTime)
	}
	return l.priority > r.priority
}
