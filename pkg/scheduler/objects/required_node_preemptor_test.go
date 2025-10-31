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
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestFilterAllocations(t *testing.T) {
	node := NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 100}},
		},
	})

	// case 1: allocations are available but none of its resources are matching with ds request ask, hence no allocations considered
	requiredAsk := createAllocationAsk("ask12", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5}))
	p := NewRequiredNodePreemptor(node, requiredAsk)
	asks := prepareAllocationAsks(t, node)
	p.filterAllocations()
	filteredAllocations := p.getAllocations()

	// allocations are not even considered as there is no match. of course, no victims
	assert.Equal(t, len(filteredAllocations), 0)
	removeAllocationAsks(node, asks)

	// case 2: allocations are available but priority is higher than ds request priority, hence no allocations considered
	requiredAsk1 := createAllocationAsk("ask12", "app1", true, true, 1,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p1 := NewRequiredNodePreemptor(node, requiredAsk1)
	asks = prepareAllocationAsks(t, node)
	p1.filterAllocations()
	filteredAllocations = p.getAllocations()

	// allocations are not even considered as there is no match. of course, no victims
	assert.Equal(t, len(filteredAllocations), 0)
	removeAllocationAsks(node, asks)

	// case 3: victims are available as there are allocations with lower priority and resource match
	requiredAsk2 := createAllocationAsk("ask12", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p2 := NewRequiredNodePreemptor(node, requiredAsk2)
	asks = prepareAllocationAsks(t, node)
	p2.filterAllocations()
	filteredAllocations = p2.getAllocations()
	assert.Equal(t, len(filteredAllocations), 10)
	removeAllocationAsks(node, asks)

	// case 4: allocation has preempted
	requiredAsk3 := createAllocationAsk("ask12", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p3 := NewRequiredNodePreemptor(node, requiredAsk3)
	asks = prepareAllocationAsks(t, node)
	p3.filterAllocations()
	p3.sortAllocations()

	victims := p3.GetVictims()
	for _, victim := range victims {
		victim.MarkPreempted()
	}
	p3.filterAllocations()
	filteredAllocations = p3.getAllocations()
	assert.Equal(t, len(filteredAllocations), 19)
	removeAllocationAsks(node, asks)
}

func TestGetVictims(t *testing.T) {
	node := NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 100}},
		},
	})

	// case 1: victims are available and its resources are matching with ds request ask
	requiredAsk := createAllocationAsk("ask11", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25}))

	p := NewRequiredNodePreemptor(node, requiredAsk)
	asks := prepareAllocationAsks(t, node)
	p.filterAllocations()
	p.sortAllocations()
	victims := p.GetVictims()
	assert.Equal(t, len(victims), 4)
	assert.Equal(t, victims[0].GetAllocationKey(), "ask5")
	assert.Equal(t, resources.Equals(victims[0].GetAllocatedResource(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})), true)
	assert.Equal(t, victims[1].GetAllocationKey(), "ask1")
	assert.Equal(t, resources.Equals(victims[1].GetAllocatedResource(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})), true)
	assert.Equal(t, victims[2].GetAllocationKey(), "ask4")
	assert.Equal(t, resources.Equals(victims[2].GetAllocatedResource(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})), true)
	assert.Equal(t, victims[3].GetAllocationKey(), "ask2")
	assert.Equal(t, resources.Equals(victims[3].GetAllocatedResource(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8})), true)
	removeAllocationAsks(node, asks)

	// case 2: victims are available and its resources are matching with ds request ask (but with different quantity)
	requiredAsk2 := createAllocationAsk("ask13", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p2 := NewRequiredNodePreemptor(node, requiredAsk2)
	asks = prepareAllocationAsks(t, node)
	p2.filterAllocations()
	p2.sortAllocations()
	victims2 := p2.GetVictims()
	assert.Equal(t, len(victims2), 1)
	removeAllocationAsks(node, asks)

	// case 3: allocations are available and its resources are matching partially with ds request ask (because of different resource types), hence no victims
	requiredAsk3 := createAllocationAsk("ask13", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 5}))
	p3 := NewRequiredNodePreemptor(node, requiredAsk3)
	asks = prepareAllocationAsks(t, node)
	p3.filterAllocations()
	filteredAllocations := p3.getAllocations()

	// allocations are considered as there is partial match
	assert.Equal(t, len(filteredAllocations), 10)
	p3.sortAllocations()

	// allocations are available but no exact match for choosing victims
	victims3 := p3.GetVictims()
	assert.Equal(t, len(victims3), 0)
	removeAllocationAsks(node, asks)
}
