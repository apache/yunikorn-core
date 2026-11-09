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

	rootQ, err := createRootQueue(map[string]string{"first": "100"})
	assert.NilError(t, err)
	defaultQ, err := createManagedQueueGuaranteed(rootQ, "default", true, map[string]string{"vcores": "3"}, nil, nil)
	assert.NilError(t, err)

	app := newApplication("app1", "default", "root.default")
	app.SetQueue(defaultQ)

	// case 1: allocations are available but none of its resources are matching with ds request ask, hence no allocations considered
	requiredAsk := createAllocationAsk("ask12", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5}))
	p := NewRequiredNodePreemptor(node, requiredAsk, app)
	asks := prepareAllocationAsks(t, node)
	result := p.filterAllocations()
	verifyFilterResult(t, 10, 0, 10, 0, 0, 0, result)
	filteredAllocations := p.getAllocations()

	// allocations are not even considered as there is no match. of course, no victims
	assert.Equal(t, len(filteredAllocations), 0)
	removeAllocationAsks(node, asks)

	// case 2: allocations are available but priority is higher than ds request priority, hence no allocations considered
	requiredAsk1 := createAllocationAsk("ask12", "app1", true, true, 1,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p1 := NewRequiredNodePreemptor(node, requiredAsk1, app)
	asks = prepareAllocationAsks(t, node)
	result = p1.filterAllocations()
	verifyFilterResult(t, 10, 0, 0, 10, 0, 0, result)
	filteredAllocations = p.getAllocations()

	// allocations are not even considered as there is no match. of course, no victims
	assert.Equal(t, len(filteredAllocations), 0)
	removeAllocationAsks(node, asks)

	// case 3: victims are available as there are allocations with lower priority and resource match
	requiredAsk2 := createAllocationAsk("ask12", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p2 := NewRequiredNodePreemptor(node, requiredAsk2, app)
	asks = prepareAllocationAsks(t, node)
	result = p2.filterAllocations()
	verifyFilterResult(t, 10, 0, 0, 0, 0, 0, result)
	filteredAllocations = p2.getAllocations()
	assert.Equal(t, len(filteredAllocations), 10)
	removeAllocationAsks(node, asks)

	// case 4: allocation has been preempted
	requiredAsk3 := createAllocationAsk("ask12", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p3 := NewRequiredNodePreemptor(node, requiredAsk3, app)
	asks = prepareAllocationAsks(t, node)
	err = node.GetAllocation("ask5").MarkPreempted() // "ask5" would be the first and only victim without this
	assert.NilError(t, err)
	result = p3.filterAllocations()
	p3.sortAllocations()

	verifyFilterResult(t, 10, 0, 0, 0, 1, 0, result)
	filteredAllocations = p3.getAllocations()
	assert.Equal(t, len(filteredAllocations), 9) // "ask5" is no longer considered as a victim
	removeAllocationAsks(node, asks)

	// case 5: existing required node allocation
	requiredAsk4 := createAllocationAsk("ask12", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p4 := NewRequiredNodePreemptor(node, requiredAsk4, app)
	results := prepareAllocationAsks(t, node)
	results[8].requiredNode = "node-3"

	result = p4.filterAllocations()
	verifyFilterResult(t, 10, 1, 0, 0, 0, 0, result)
	filteredAllocations = p4.getAllocations()
	assert.Equal(t, len(filteredAllocations), 9)
	removeAllocationAsks(node, asks)

	// case 6: release ph allocation
	p5 := NewRequiredNodePreemptor(node, requiredAsk2, app)
	results = prepareAllocationAsks(t, node)
	results[9].released = true

	result = p5.filterAllocations()
	verifyFilterResult(t, 10, 0, 0, 0, 0, 1, result)
	filteredAllocations = p5.getAllocations()
	assert.Equal(t, len(filteredAllocations), 9)
	removeAllocationAsks(node, asks)
}

func TestGetVictims(t *testing.T) {
	rootQ, err := createRootQueue(map[string]string{"first": "100"})
	assert.NilError(t, err)
	defaultQ, err := createManagedQueueGuaranteed(rootQ, "default", true, map[string]string{"vcores": "3"}, nil, nil)
	assert.NilError(t, err)

	app := newApplication("app1", "default", "root.default")
	app.SetQueue(defaultQ)

	newNodeWithAsks := func(nodeID string, res map[string]*si.Quantity) (*Node, []*Allocation) {
		n := NewNode(&si.NodeInfo{
			NodeID:              nodeID,
			Attributes:          nil,
			SchedulableResource: &si.Resource{Resources: res},
		})
		asks := prepareAllocationAsks(t, n)
		return n, asks
	}

	node, _ := newNodeWithAsks("node", map[string]*si.Quantity{"first": {Value: 76}})
	nodeReleased, asksReleased := newNodeWithAsks("node-released", map[string]*si.Quantity{"first": {Value: 76}})
	assert.NilError(t, asksReleased[1].SetReleased(true))
	nodeWithAvail, _ := newNodeWithAsks("node-avail", map[string]*si.Quantity{"first": {Value: 100}})

	nodeMulti := NewNode(&si.NodeInfo{
		NodeID:              "node-multi",
		Attributes:          nil,
		SchedulableResource: &si.Resource{Resources: map[string]*si.Quantity{"first": {Value: 10}, "second": {Value: 5}}},
	})
	alloc1 := createAllocation("alloc1", "app1", nodeMulti.NodeID, true, false, 1, false,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	alloc2 := createAllocation("alloc2", "app1", nodeMulti.NodeID, true, false, 2, false,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	alloc3 := createAllocation("alloc3", "app1", nodeMulti.NodeID, true, false, 3, false,
		resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5}))
	for _, alloc := range []*Allocation{alloc1, alloc2, alloc3} {
		assert.Assert(t, nodeMulti.TryAddAllocation(alloc))
	}

	tests := []struct {
		name            string
		node            *Node
		requiredAsk     *Allocation
		expectedVictims []string
	}{
		{
			name: "victims are available and matching ask",
			node: node,
			requiredAsk: createAllocationAsk("ask11", "app1", true, true, 20,
				resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25})),
			expectedVictims: []string{"ask5", "ask4", "ask2", "ask1"},
		},
		{
			name: "victims are available with different quantity",
			node: node,
			requiredAsk: createAllocationAsk("ask13", "app1", true, true, 20,
				resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})),
			expectedVictims: []string{"ask5"},
		},
		{
			name: "allocations match partially with ds request ask, hence no victims",
			node: node,
			requiredAsk: createAllocationAsk("ask13", "app1", true, true, 20,
				resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 5})),
			expectedVictims: []string{},
		},
		{
			name: "victim chosen earlier released in the meantime",
			node: nodeReleased,
			requiredAsk: createAllocationAsk("ask14", "app1", true, true, 20,
				resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25})),
			expectedVictims: []string{"ask5", "ask4", "ask1", "ask3"},
		},
		{
			name: "node already has partial available resource",
			node: nodeWithAvail,
			requiredAsk: createAllocationAsk("ask11", "app1", true, true, 20,
				resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25})),
			expectedVictims: []string{"ask5"},
		},
		{
			name: "skip allocations not contributing to remaining shortfall in multi-resource preemption",
			node: nodeMulti,
			requiredAsk: createAllocationAsk("ask16", "app1", true, true, 20,
				resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 5})),
			expectedVictims: []string{"alloc1", "alloc3"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := NewRequiredNodePreemptor(tc.node, tc.requiredAsk, app)
			p.filterAllocations()
			p.sortAllocations()
			victims := p.pruneVictims(p.allocations)
			assert.Equal(t, len(victims), len(tc.expectedVictims))
			for i, key := range tc.expectedVictims {
				assert.Equal(t, victims[i].GetAllocationKey(), key)
			}
		})
	}
}

func TestSortAllocations(t *testing.T) {
	// case 1: victims are compared by their share of the node capacity, not by their raw quantities
	node := NewNode(&si.NodeInfo{
		NodeID: "node1",
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 100}, "second": {Value: 10}},
		},
	})
	for _, alloc := range []*Allocation{
		createAllocation("filler", "app1", node.NodeID, true, false, 30, false,
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 85, "second": 7})),
		createAllocation("alloc-a", "app1", node.NodeID, true, false, 10, false,
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 1})),
		createAllocation("alloc-b", "app1", node.NodeID, true, false, 10, false,
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 2})),
	} {
		assert.Assert(t, node.TryAddAllocation(alloc))
	}
	requiredAsk := createAllocationAsk("ds1", "app2", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 1}))
	p := NewRequiredNodePreemptor(node, requiredAsk, nil)
	p.filterAllocations()
	p.sortAllocations()
	allocations := p.getAllocations()
	assert.Equal(t, len(allocations), 2)
	assert.Equal(t, allocations[0].GetAllocationKey(), "alloc-a")
	assert.Equal(t, allocations[1].GetAllocationKey(), "alloc-b")

	// case 2: only the resource types the node is short of are considered, a pod holding none of them goes last
	node = NewNode(&si.NodeInfo{
		NodeID: "node2",
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 10}, "pods": {Value: 10}},
		},
	})
	for _, alloc := range []*Allocation{
		createAllocation("best-effort", "app1", node.NodeID, true, false, 10, false,
			resources.NewResourceFromMap(map[string]resources.Quantity{"pods": 1})),
		createAllocation("busy", "app1", node.NodeID, true, false, 10, false,
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 1})),
	} {
		assert.Assert(t, node.TryAddAllocation(alloc))
	}
	requiredAsk = createAllocationAsk("ds2", "app2", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "pods": 1}))
	p = NewRequiredNodePreemptor(node, requiredAsk, nil)
	p.filterAllocations()
	p.sortAllocations()
	allocations = p.getAllocations()
	assert.Equal(t, len(allocations), 2)
	assert.Equal(t, allocations[0].GetAllocationKey(), "busy")
	assert.Equal(t, allocations[1].GetAllocationKey(), "best-effort")
}

func verifyFilterResult(t *testing.T, totalAllocations, requiredNodeAllocations, resourceNotEnough, higherPriorityAllocations, alreadyPreemptedAllocations int, releasedPhAllocations int, result filteringResult) {
	t.Helper()
	assert.Equal(t, totalAllocations, result.totalAllocations)
	assert.Equal(t, requiredNodeAllocations, result.requiredNodeAllocations)
	assert.Equal(t, resourceNotEnough, result.atLeastOneResNotMatched)
	assert.Equal(t, higherPriorityAllocations, result.higherPriorityAllocations)
	assert.Equal(t, alreadyPreemptedAllocations, result.alreadyPreemptedAllocations)
	assert.Equal(t, releasedPhAllocations, result.releasedPhAllocations)
}
