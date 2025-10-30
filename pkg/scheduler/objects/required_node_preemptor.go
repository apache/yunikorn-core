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

func NewRequiredNodePreemptor(node *Node, requiredAsk *Allocation) *PreemptionContext {
	preemptor := &PreemptionContext{
		node:        node,
		requiredAsk: requiredAsk,
		allocations: make([]*Allocation, 0),
	}
	return preemptor
}

func (p *PreemptionContext) filterAllocations() {
	for _, allocation := range p.node.GetYunikornAllocations() {
		// skip daemon set pods and higher priority allocation
		if allocation.GetRequiredNode() != "" || allocation.GetPriority() > p.requiredAsk.GetPriority() {
			continue
		}

		// skip if the allocation is already being preempted
		if allocation.IsPreempted() {
			continue
		}

		// at least one of the required ask resource should match, otherwise skip
		if !p.requiredAsk.GetAllocatedResource().MatchAny(allocation.GetAllocatedResource()) {
			continue
		}
		p.allocations = append(p.allocations, allocation)
	}
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
