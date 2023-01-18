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
	"sync"

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

type PreemptionContext struct {
	node        *Node
	requiredAsk *AllocationAsk
	allocations []*Allocation

	sync.RWMutex
}

func NewRequiredNodePreemptor(node *Node, requiredAsk *AllocationAsk) *PreemptionContext {
	preemptor := &PreemptionContext{
		node:        node,
		requiredAsk: requiredAsk,
		allocations: make([]*Allocation, 0),
	}
	return preemptor
}

func (p *PreemptionContext) filterAllocations() {
	p.Lock()
	defer p.Unlock()
	for _, allocation := range p.node.GetAllAllocations() {
		// skip daemon set pods and higher priority allocation
		if allocation.GetAsk().GetRequiredNode() != "" || allocation.GetPriority() > p.requiredAsk.GetPriority() {
			continue
		}

		// skip if the allocation is already being preempted
		if allocation.IsPreempted() {
			continue
		}

		// atleast one of the required ask resource should match, otherwise skip
		includeAllocation := false
		for k := range p.requiredAsk.GetAllocatedResource().Resources {
			if _, ok := allocation.GetAsk().GetAllocatedResource().Resources[k]; ok {
				includeAllocation = true
				break
			}
		}
		if includeAllocation {
			p.allocations = append(p.allocations, allocation)
		}
	}
}

// sort based on the following criteria in the specified order:
// 1. By type (regular pods, opted out pods, driver/owner pods),
// 2. By priority (least priority ask placed first),
// 3. By Create time or age of the ask (younger ask placed first),
// 4. By resource (ask with lesser allocated resources placed first)
func (p *PreemptionContext) sortAllocations() {
	p.Lock()
	defer p.Unlock()
	sort.SliceStable(p.allocations, func(i, j int) bool {
		l := p.allocations[i].GetAsk()
		r := p.allocations[j].GetAsk()

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

func (p *PreemptionContext) GetVictims() []*Allocation {
	p.RLock()
	defer p.RUnlock()
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
	p.RLock()
	defer p.RUnlock()
	return p.allocations
}
