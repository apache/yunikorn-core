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

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
)

type PreemptionContext struct {
	Node        *Node
	allocations []*AllocationAsk
}

func NewSimplePreemptor(node *Node) *PreemptionContext {
	preemptor := &PreemptionContext{
		Node: node,
	}
	preemptor.filterAllocations()
	preemptor.sortAllocations()
	return preemptor
}

func (p *PreemptionContext) filterAllocations() {
	for _, allocation := range p.Node.GetAllAllocations() {
		// skip daemon set pods
		if allocation.Ask.GetRequiredNode() != "" {
			continue
		}
		p.allocations = append(p.allocations, allocation.Ask)
	}
}

// sort based on the following criteria in the specified order:
// 1. By type (regular pods, opted out pods, driver/owner pods),
// 2. By priority (least priority ask placed first),
// 3. By Create time or age of the ask (younger ask placed first),
// 4. By resource (ask with lesser allocated resources placed first)
func (p *PreemptionContext) sortAllocations() {
	sort.SliceStable(p.allocations, func(i, j int) bool {
		l := p.allocations[i]
		r := p.allocations[j]

		// sort based on the type
		lAskType := 1         // regular pod
		if l.IsOriginator() { // driver/owner pod
			lAskType = 3
		} else if !l.GetAllowPreemption() { // opted out pod
			lAskType = 2
		}
		rAskType := 1
		if r.IsOriginator() {
			rAskType = 3
		} else if !r.GetAllowPreemption() {
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
			log.Logger().Info("step 21")
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

func (p *PreemptionContext) GetVictims(sourceAsk *AllocationAsk) []*AllocationAsk {
	var victims []*AllocationAsk
	requiredResource := resources.Multiply(sourceAsk.GetAllocatedResource(), int64(sourceAsk.GetPendingAskRepeat()))
	currentResource := resources.Zero
	for _, allocation := range p.allocations {
		if !resources.StrictlyGreaterThanOrEquals(currentResource, requiredResource) {
			currentResource.AddTo(allocation.GetAllocatedResource())
			victims = append(victims, allocation)
		} else {
			break
		}
	}

	// Did we found the meaning full set of victims?
	if len(victims) > 0 && resources.StrictlyGreaterThanOrEquals(
		resources.Add(currentResource, p.Node.GetAvailableResource()), requiredResource) {
		return victims
	}
	return nil
}

// for test only
func (p *PreemptionContext) setAllocations(allocations []*AllocationAsk) {
	p.allocations = allocations
}

// for test only
func (p *PreemptionContext) getAllocations() []*AllocationAsk {
	return p.allocations
}
