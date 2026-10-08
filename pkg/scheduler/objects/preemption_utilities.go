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
)

var (
	scoreNonOriginator uint64 = 1 << 34
	scoreAllowPreempt  uint64 = 1 << 33
)

func SortAllocationsBasedOnAsk(allocations []*Allocation, total, ask *resources.Resource) {
	sort.SliceStable(allocations, func(i, j int) bool {
		l := allocations[i]
		r := allocations[j]

		scoreLeft := scoreAllocationBasedOnAsk(l, ask)
		scoreRight := scoreAllocationBasedOnAsk(r, ask)
		if scoreLeft != scoreRight {
			return scoreLeft > scoreRight
		}

		// sort based on the priority
		lPriority := l.GetPriority()
		rPriority := r.GetPriority()
		if lPriority < rPriority {
			return true
		}
		if lPriority > rPriority {
			return false
		}

		// sort based on the age (limiting the boundary to hour max)
		lHour := l.GetCreateTime().Truncate(time.Hour)
		rHour := r.GetCreateTime().Truncate(time.Hour)
		if !lHour.Equal(rHour) {
			return lHour.After(rHour)
		}

		// sort based on the allocated resource
		lResource := l.GetAllocatedResource()
		rResource := r.GetAllocatedResource()
		comp := resources.CompUsageRatioSpecificTypes(lResource, rResource, total, ask)
		if comp == -1 {
			return true
		}
		if comp == 1 {
			return false
		}
		return l.GetAllocationKey() < r.GetAllocationKey()
	})
}

// scoreAllocationBasedOnAsk generates a relative score for an allocation based on ask. Higher-scored allocations are considered more likely
// preemption candidates. Opted out pods are considered before originator pods.
func scoreAllocationBasedOnAsk(allocation *Allocation, ask *resources.Resource) uint64 {
	var score uint64 = 0
	if !allocation.IsOriginator() {
		score |= scoreNonOriginator
	}
	if allocation.IsAllowPreemptSelf() {
		score |= scoreAllowPreempt
	}
	score += allocation.GetAllocatedResource().TypeMatching(ask)
	return score
}
