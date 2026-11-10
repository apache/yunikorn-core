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

	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
)

// queuePriorityRank supports default priority policies only. Fences need a
// separately agreed comparison domain; do not silently fall back to another policy.
func (p *Preemptor) queuePriorityRank(a *Allocation) (int64, bool) {
	q := p.queue.GetQueueByAppID(a.GetApplicationID())
	if q == nil {
		return 0, false
	}
	rank := int64(a.GetPriority())
	for q != nil {
		policy, offset := q.GetPriorityPolicyAndOffset()
		if policy != policies.DefaultPriorityPolicy {
			return 0, false
		}
		rank += int64(offset)
		q = q.parent
	}
	return rank, true
}

// tryQueuePriorityStages expands the eligible tier set before running the
// existing node predicates, quota checks, final validation and release path.
// This is a bounded heuristic, not an exhaustive search over victim subsets.
func (p *Preemptor) tryQueuePriorityStages() (*AllocationResult, bool) {
	if _, ok := p.queuePriorityRank(p.ask); !ok {
		return nil, false
	}
	ranks := make(map[string]int64)
	unique := make(map[int64]bool)
	// Include additional victims even when their node cannot host the ask.
	for _, snapshot := range p.allocationsByQueue {
		for _, victim := range snapshot.PotentialVictims {
			rank, ok := p.queuePriorityRank(victim)
			if !ok {
				return nil, false
			}
			ranks[victim.GetAllocationKey()] = rank
			unique[rank] = true
		}
	}
	tiers := make([]int64, 0, len(unique))
	for rank := range unique {
		tiers = append(tiers, rank)
	}
	sort.Slice(tiers, func(i, j int) bool { return tiers[i] < tiers[j] })
	for _, ceiling := range tiers {
		trial := *p
		trial.victimRanks = ranks
		trial.allocationsByQueue = p.duplicateQueueSnapshots()
		trial.allocationsByNode = make(map[string][]*Allocation)
		trial.queueByAlloc = make(map[string]*QueuePreemptionSnapshot)
		for _, q := range trial.allocationsByQueue {
			filtered := make([]*Allocation, 0, len(q.PotentialVictims))
			for _, victim := range q.PotentialVictims {
				if ranks[victim.GetAllocationKey()] > ceiling {
					continue
				}
				filtered = append(filtered, victim)
				trial.queueByAlloc[victim.GetAllocationKey()] = q
			}
			q.PotentialVictims = filtered
		}
		// Keep the original node eligibility filter and within-tier ordering.
		for nodeID, candidates := range p.allocationsByNode {
			for _, victim := range candidates {
				if ranks[victim.GetAllocationKey()] <= ceiling {
					trial.allocationsByNode[nodeID] = append(trial.allocationsByNode[nodeID], victim)
				}
			}
			victims := trial.allocationsByNode[nodeID]
			sort.SliceStable(victims, func(i, j int) bool {
				return ranks[victims[i].GetAllocationKey()] < ranks[victims[j].GetAllocationKey()]
			})
		}
		if result, ok := trial.tryPreemptionCandidates(); ok {
			return result, true
		}
	}
	return nil, false
}
