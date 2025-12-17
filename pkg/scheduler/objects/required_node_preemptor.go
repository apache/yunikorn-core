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
	"github.com/apache/yunikorn-core/pkg/common/resources"
)

type PreemptionContext struct {
	node        *Node
	requiredAsk *Allocation
	allocations []*Allocation
}

type filteringResult struct {
	totalAllocations            int // total number of allocations
	requiredNodeAllocations     int // number of requiredNode (daemon set) allocations that cannot be preempted
	atLeastOneResNotMatched     int // number of allocations where there's no single resource type that would match
	higherPriorityAllocations   int // number of allocations with higher priority
	alreadyPreemptedAllocations int // number of allocations already preempted
	releasedPhAllocations       int // number of ph allocations released
}

func NewRequiredNodePreemptor(node *Node, requiredAsk *Allocation) *PreemptionContext {
	preemptor := &PreemptionContext{
		node:        node,
		requiredAsk: requiredAsk,
		allocations: make([]*Allocation, 0),
	}
	return preemptor
}

func (p *PreemptionContext) filterAllocations() filteringResult {
	var result filteringResult
	yunikornAllocations := p.node.GetYunikornAllocations()
	result.totalAllocations = len(yunikornAllocations)

	for _, allocation := range yunikornAllocations {
		// skip daemon set pods and higher priority allocation
		if allocation.GetRequiredNode() != "" {
			result.requiredNodeAllocations++
			continue
		}

		if allocation.GetPriority() > p.requiredAsk.GetPriority() {
			result.higherPriorityAllocations++
			continue
		}

		// skip if the allocation is already being preempted
		if allocation.IsPreempted() {
			result.alreadyPreemptedAllocations++
			continue
		}

		// at least one of the required ask resource should match, otherwise skip
		if !p.requiredAsk.GetAllocatedResource().MatchAny(allocation.GetAllocatedResource()) {
			result.atLeastOneResNotMatched++
			continue
		}

		// skip placeholder tasks which are marked released
		if allocation.IsReleased() {
			result.releasedPhAllocations++
			continue
		}

		p.allocations = append(p.allocations, allocation)
	}

	return result
}

// sort based on the following criteria in the specified order:
// 1. By type (regular pods, opted out pods, driver/owner pods),
// 2. By priority (least priority ask placed first),
// 3. By Create time or age of the ask (younger ask placed first),
// 4. By resource (ask with lesser allocated resources placed first)
func (p *PreemptionContext) sortAllocations() {
	SortAllocations(p.allocations)
}

func (p *PreemptionContext) GetVictims() []*Allocation {
	var victims []*Allocation
	var currentResource = resources.NewResource()
	for _, allocation := range p.allocations {
		if !resources.StrictlyGreaterThanOrEquals(currentResource, p.requiredAsk.GetAllocatedResource()) {
			currentResource.AddTo(allocation.GetAllocatedResource())
			victims = append(victims, allocation)
		} else {
			break
		}
	}

	// Did we found the useful set of victims?
	if len(victims) > 0 && resources.StrictlyGreaterThanOrEquals(
		resources.Add(currentResource, p.node.GetAvailableResource()), p.requiredAsk.GetAllocatedResource()) {
		return victims
	}
	return nil
}

// for test only
func (p *PreemptionContext) getAllocations() []*Allocation {
	return p.allocations
}
