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

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

var (
	scoreNonOriginator uint64 = 1 << 34
	scoreAllowPreempt  uint64 = 1 << 33
)

// SortAllocations Sort allocations based on the following criteria in the specified order:
// 1. By type (regular pods, opted out pods, driver/owner pods),
// 2. By priority (least priority ask placed first),
// 3. By Create time or age of the ask (younger ask placed first),
// 4. By resource (ask with lesser allocated resources placed first)
func SortAllocations(allocations []*Allocation) {
	sort.SliceStable(allocations, func(i, j int) bool {
		l := allocations[i]
		r := allocations[j]

		// sort based on the type
		lAskType := 1         // regular pod
		if l.IsOriginator() { // driver/owner pod
			lAskType = 3
		} else if !l.IsAllowPreemptSelf() { // opted out pod
			lAskType = 2
		}
		rAskType := 1
		if r.IsOriginator() {
			rAskType = 3
		} else if !r.IsAllowPreemptSelf() {
			rAskType = 2
		}
		if lAskType < rAskType {
			return true
		}
		if lAskType > rAskType {
			return false
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

		// sort based on the age
		if !l.GetCreateTime().Equal(r.GetCreateTime()) {
			return l.GetCreateTime().After(r.GetCreateTime())
		}

		// sort based on the allocated resource
		lResource := l.GetAllocatedResource()
		rResource := r.GetAllocatedResource()
		if !resources.Equals(lResource, rResource) {
			delta := resources.Sub(lResource, rResource)
			return !resources.StrictlyGreaterThanZero(delta)
		}
		return true
	})
}

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

func runPredicateChecks(plugin api.ResourceManagerCallback, args *si.PreemptionPredicatesArgs) *predicateCheckResult {
	result := &predicateCheckResult{
		allocationKey:   args.AllocationKey,
		nodeID:          args.NodeID,
		success:         false,
		index:           -1,
		predicateErrors: make(map[string]int32),
	}
	if len(args.PreemptAllocationKeys) == 0 {
		// normal check; there are sufficient resources to run on this node
		if err := plugin.Predicates(&si.PredicatesArgs{
			AllocationKey: args.AllocationKey,
			NodeID:        args.NodeID,
			Allocate:      true,
		}); err == nil {
			result.success = true
			result.index = -1
		} else {
			log.Log(log.SchedPreemption).Debug("Normal predicate check failed",
				zap.String("AllocationKey", args.AllocationKey),
				zap.String("NodeID", args.NodeID),
				zap.Error(err))
			result.predicateErrors[err.Error()]++
		}
	} else if response := plugin.PreemptionPredicates(args); response != nil {
		// preemption check; at least one allocation will need preemption
		result.success = response.GetSuccess()
		if result.success {
			result.index = int(response.GetIndex())
		} else {
			log.Log(log.SchedPreemption).Debug("Preemption predicate check failed",
				zap.String("AllocationKey", args.AllocationKey),
				zap.String("NodeID", args.NodeID))
			result.predicateErrors = response.GetErrorMessage()
		}
	}
	return result
}
