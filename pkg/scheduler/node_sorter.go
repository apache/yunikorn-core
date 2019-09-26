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

// Sort nodes here.
func (m *Scheduler) SortAllNodesWithAscendingResource(name string) []*SchedulingNode {
	nodeList := m.clusterInfo.GetPartition(name).CopyNodeInfos()
	if len(nodeList) <= 0 {
		// When we don't have node, do nothing
		return nil
	}

	schedulingNodeList := make([]*SchedulingNode, len(nodeList))
	for idx, v := range nodeList {
		schedulingNodeList[idx] = NewSchedulingNode(v)
	}

	sortingStart := time.Now()
	sort.SliceStable(schedulingNodeList, func(i, j int) bool {
		l := schedulingNodeList[i]
		r := schedulingNodeList[j]

		// Sort by available resource, ascending order
		return resources.CompFairnessRatioAssumesUnitPartition(r.CachedAvailableResource, l.CachedAvailableResource) > 0
	})

	metrics.GetSchedulerMetrics().ObserveNodeSortingLatency(sortingStart)

	return schedulingNodeList
}
