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
  "strconv"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestQuotaChangeGetPreemptableResource(t *testing.T) {
	parent, err := NewConfiguredQueue(configs.QueueConfig{
		Name:   "parent",
		Parent: true,
	}, nil, false, nil)
	assert.NilError(t, err)

	leaf, err := NewConfiguredQueue(configs.QueueConfig{
		Name: "leaf",
	}, parent, false, nil)
	assert.NilError(t, err)

	testCases := []struct {
		name             string
		queue            *Queue
		parentGuaranteed *resources.Resource
		maxResource      *resources.Resource
		usedResource     *resources.Resource
		preemptable      *resources.Resource
	}{
		{"nil max and nil used", leaf, nil, nil, nil, nil},
		{"nil max", leaf, nil, nil, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), nil},
		{"nil used", leaf, nil, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), nil, nil},
		{"used below max", leaf, nil, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500}), nil},
		{"used above max", leaf, nil, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1500}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500})},
		{"used above max, below max, equals max in specific res type and also with extra res types", leaf, nil, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000, "cpu": 10, "gpu": 10}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1500, "cpu": 10, "gpu": 9}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500})},
		{"used res type but max undefined", leaf, nil, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 150}), nil},
		{"parent guaranteed set, lower than leaf's preemptable resources", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1200}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1500}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300})},
		{"parent guaranteed set, lower than leaf's preemptable resources - extra res types ", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1200}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1500, "cpu": 10, "gpu": 9}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300})},
		{"parent guaranteed set, higher than leaf's preemptable resources", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 900}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1500}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500})},
		{"parent guaranteed set, higher than leaf's preemptable resources - extra res types", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 900}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1500, "cpu": 10, "gpu": 9}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500})},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.queue.parent.guaranteedResource = tc.parentGuaranteed
			tc.queue.maxResource = tc.maxResource
			tc.queue.IncAllocatedResource(tc.usedResource, false)
			preemptor := NewQuotaPreemptor(tc.queue)
			preemptor.setPreemptableResources()
			assert.Equal(t, resources.Equals(preemptor.preemptableResource, tc.preemptable), true)

			// reset
			err = tc.queue.DecAllocatedResource(tc.usedResource)
			assert.NilError(t, err)
		})
	}
}

func TestQuotaChangeFilterVictims(t *testing.T) {
	leaf, err := NewConfiguredQueue(configs.QueueConfig{
		Name: "leaf",
	}, nil, false, nil)
	assert.NilError(t, err)

	node := NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 100}},
		},
	})
	testCases := []struct {
		name                     string
		queue                    *Queue
		preemptableResource      *resources.Resource
		irrelevantAllocations    []bool
		expectedAllocationsCount int
	}{
		{"nil preemptable resource", leaf, nil, []bool{false, false, false}, 0},
		{"not even single res type in preemptable resource matches", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100}), []bool{false, false, false}, 0},
		{"res type in preemptable resource matches", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100}), []bool{false, false, false}, 10},
		{"irrelevant - required node allocations", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100}), []bool{true, false, false}, 8},
		{"irrelevant - already preempted allocations", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100}), []bool{false, true, false}, 8},
		{"irrelevant - already released allocations", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100}), []bool{false, false, true}, 8},
		{"combine irrelevant allocations", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100}), []bool{true, true, true}, 4},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			asks := prepareAllocationAsks(t, node)
			assignAllocationsToQueue(asks, leaf)
			if tc.irrelevantAllocations[0] {
				asks[0].SetRequiredNode("node2")
				asks[1].SetRequiredNode("node2")
			}
			if tc.irrelevantAllocations[1] {
				err = asks[2].MarkPreempted()
				assert.NilError(t, err)
				err = asks[3].MarkPreempted()
				assert.NilError(t, err)
			}
			if tc.irrelevantAllocations[2] {
				err = asks[4].SetReleased(true)
				assert.NilError(t, err)
				err = asks[5].SetReleased(true)
				assert.NilError(t, err)
			}
			preemptor := NewQuotaPreemptor(tc.queue)
			preemptor.preemptableResource = tc.preemptableResource
			preemptor.filterAllocations()
			assert.Equal(t, len(preemptor.allocations), tc.expectedAllocationsCount)
			removeAllocationAsks(node, asks)
			resetQueue(leaf)
		})
	}
}

func TestQuotaChangeTryPreemption(t *testing.T) {
	events.Init()
	eventSystem := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	eventSystem.StartServiceWithPublisher(false)
	leaf, err := NewConfiguredQueue(configs.QueueConfig{
		Name: "leaf",
	}, nil, false, nil)
	assert.NilError(t, err)

	node := NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 200}},
		},
	})

	suitableVictims := make([]*Allocation, 0)
	notSuitableVictims := make([]*Allocation, 0)
	oversizedVictims := make([]*Allocation, 0)
	overflowVictims := make([]*Allocation, 0)
	shortfallVictims := make([]*Allocation, 0)

	suitableVictims = append(suitableVictims, createVictim(t, "ask1", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims = append(suitableVictims, createVictim(t, "ask2", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))

	oversizedVictims = append(oversizedVictims, createVictim(t, "ask21", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 9})))
	oversizedVictims = append(oversizedVictims, createVictim(t, "ask3", node, 3, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11})))

	overflowVictims = append(overflowVictims, createVictim(t, "ask4", node, 3, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})))
	overflowVictims = append(overflowVictims, createVictim(t, "ask41", node, 2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 6})))
	overflowVictims = append(overflowVictims, createVictim(t, "ask42", node, 1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 9})))

	shortfallVictims = append(shortfallVictims, createVictim(t, "ask5", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})))
	shortfallVictims = append(shortfallVictims, createVictim(t, "ask51", node, 3, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 6})))
	shortfallVictims = append(shortfallVictims, createVictim(t, "ask52", node, 2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})))
	shortfallVictims = append(shortfallVictims, createVictim(t, "ask53", node, 1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 4})))

	notSuitableVictims = append(notSuitableVictims, createVictim(t, "ask6", node, 3, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11})))

	oldMax := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 20})
	newMax := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	preemptable := newMax
	guaranteed := preemptable
	lowerGuaranteed := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	testCases := []struct {
		name                 string
		queue                *Queue
		oldMax               *resources.Resource
		newMax               *resources.Resource
		guaranteed           *resources.Resource
		preemptableResource  *resources.Resource
		victims              []*Allocation
		claimedResource      *resources.Resource
		totalExpectedVictims int
		expectedVictimsCount int
	}{
		{"no victims available", leaf, oldMax, newMax, nil, preemptable, []*Allocation{}, nil, 0, 0},
		{"suitable victims available", leaf, oldMax, newMax, nil, preemptable, suitableVictims, preemptable, 2, 1},
		{"victims available but none is suitable ", leaf, oldMax, newMax, nil, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1}), notSuitableVictims, nil, 1, 0},
		{"skip over sized victims", leaf, oldMax, newMax, nil, preemptable, oversizedVictims, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 9}), 2, 1},
		{"guaranteed not set", leaf, oldMax, newMax, nil, preemptable, overflowVictims, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}), 3, 1},
		{"guaranteed set but lower than max", leaf, oldMax, newMax, lowerGuaranteed, preemptable, overflowVictims, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}), 3, 1},
		{"best effort - guaranteed set and equals max", leaf, oldMax, newMax, guaranteed, preemptable, shortfallVictims, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 9}), 4, 2},
		{"best effort - guaranteed set, max not set earlier but now", leaf, nil, newMax, guaranteed, preemptable, shortfallVictims, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 9}), 4, 2},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			leaf.maxResource = tc.oldMax
			leaf.guaranteedResource = tc.guaranteed
			asks := tc.victims
			assignAllocationsToQueue(asks, leaf)
			leaf.maxResource = tc.newMax
			leaf.guaranteedResource = tc.guaranteed
			preemptor := NewQuotaPreemptor(tc.queue)
			preemptor.allocations = asks
			preemptor.tryPreemption()
			assert.Equal(t, len(preemptor.allocations), tc.totalExpectedVictims)
			var victimsCount int
			for _, a := range asks {
				if a.IsPreempted() {
					victimsCount++
				}
			}
			assert.Equal(t, victimsCount, tc.expectedVictimsCount)

			time.Sleep(500 * time.Millisecond)
			assertQuotaPreemptionEvent(t, tc.totalExpectedVictims, "Quota Preemption results summary: preemptable resources: "+tc.preemptableResource.String()+", claimed resources: "+tc.claimedResource.String()+", selected victims: "+strconv.Itoa(tc.totalExpectedVictims)+", preempted victims: "+strconv.Itoa(tc.expectedVictimsCount), eventSystem.Store.CollectEvents())
			removeAllocationAsks(node, asks)
			resetQueue(leaf)

			// clear the events sent later after earlier collection
			_ = eventSystem.Store.CollectEvents()
		})
	}
}

func TestQuotaChangeTryPreemptionWithDifferentResTypes(t *testing.T) {
	leaf, err := NewConfiguredQueue(configs.QueueConfig{
		Name: "leaf",
	}, nil, false, nil)
	assert.NilError(t, err)

	node := NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 100}, "second": {Value: 200}},
		},
	})

	suitableVictims := make([]*Allocation, 0)
	overflowVictims := make([]*Allocation, 0)
	oversizedVictims := make([]*Allocation, 0)

	suitableVictims = append(suitableVictims, createVictim(t, "ask1", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})))
	suitableVictims = append(suitableVictims, createVictim(t, "ask2", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})))

	oversizedVictims = append(oversizedVictims, createVictim(t, "ask21", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 9, "second": 10})))
	oversizedVictims = append(oversizedVictims, createVictim(t, "ask3", node, 3, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11, "second": 10})))

	overflowVictims = append(overflowVictims, createVictim(t, "ask4", node, 3, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 10})))
	overflowVictims = append(overflowVictims, createVictim(t, "ask41", node, 2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 6, "second": 10})))
	overflowVictims = append(overflowVictims, createVictim(t, "ask42", node, 1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 9, "second": 10})))

	oldMax := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 20})
	newMax := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	newMaxWithNewResTypes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "third": 10})
	newMaxWithRemovedResTypes := resources.NewResourceFromMap(map[string]resources.Quantity{"second": 10})
	lowerGuaranteed := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	lowerGuaranteedWithNewResTypes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "fourth": 5})

	type test struct {
		allocs               []*Allocation
		totalExpectedVictims int
		expectedVictimsCount int
	}

	testCases := []struct {
		name       string
		queue      *Queue
		oldMax     *resources.Resource
		newMax     *resources.Resource
		guaranteed *resources.Resource
		victims    []test
	}{
		{"oversized victims available with extra resource types", leaf, oldMax, newMax, nil,
			[]test{
				{oversizedVictims, 2, 1},
			},
		},
		{"suitable victims available with extra resource types other than defined in max", leaf, oldMax, newMax, nil,
			[]test{
				{suitableVictims, 2, 1},
			},
		},
		{"suitable victims available with extra resource types other than defined in max", leaf, nil, newMax, nil,
			[]test{
				{suitableVictims, 2, 1},
			},
		},
		{"suitable victims available with extra resource types other than defined in guaranteed", leaf, nil, newMax, lowerGuaranteed,
			[]test{
				{suitableVictims, 2, 1},
			},
		},
		{"suitable victims available - different res types, adding new res type in max", leaf, oldMax, newMaxWithNewResTypes, nil,
			[]test{
				{suitableVictims, 2, 1},
			},
		},
		{"suitable victims available - different res types, removing existing res type from max", leaf, oldMax, newMaxWithRemovedResTypes, nil,
			[]test{
				{suitableVictims, 2, 1},
			},
		},
		{"overflow victims available with extra resource types other than defined in guaranteed and vice versa", leaf, oldMax, newMax, lowerGuaranteedWithNewResTypes,
			[]test{
				{overflowVictims, 3, 1},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, v := range tc.victims {
				leaf.maxResource = tc.oldMax
				leaf.guaranteedResource = tc.guaranteed
				asks := v.allocs
				assignAllocationsToQueue(asks, leaf)
				leaf.maxResource = tc.newMax
				leaf.guaranteedResource = tc.guaranteed
				preemptor := NewQuotaPreemptor(tc.queue)
				preemptor.tryPreemption()
				assert.Equal(t, len(preemptor.allocations), v.totalExpectedVictims)
				var victimsCount int
				for _, a := range asks {
					if a.IsPreempted() {
						victimsCount++
					}
				}
				assert.Equal(t, victimsCount, v.expectedVictimsCount)
				removeAllocationAsks(node, asks)
				resetQueue(leaf)
			}
		})
	}
}

// TestQuotaChangeGetChildQueuesPreemptableResource Test child queues distribution from parent's preemptable resources under different circumstances
// Queue Structure:
// parent
//
//	leaf 1 (Guaranteed set for this hierarchy)
//		leaf11
//			leaf111
//		leaf12
//	leaf2 (Guaranteed not set for this hierarchy)
//		leaf21
//			leaf211
//		leaf22
//	leaf3 (No usage)
//	leaf4 (Guaranteed set but equals usage)
func TestQuotaChangeGetChildQueuesPreemptableResource(t *testing.T) {
	parentConfig := configs.QueueConfig{Name: "parent", Parent: true}
	parent, err := NewConfiguredQueue(parentConfig, nil, false, nil)
	assert.NilError(t, err)

	leaf111, leaf12, leaf211, leaf22, leaf4 := createQueueSetups(t, parent, configs.Resources{Guaranteed: map[string]string{"first": "10"}}, configs.Resources{})

	parent.GetChildQueue("leaf1").allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})
	parent.GetChildQueue("leaf2").allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 80})
	leaf4.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	parent.GetChildQueue("leaf1").GetChildQueue("leaf11").allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 30})
	parent.GetChildQueue("leaf1").GetChildQueue("leaf12").allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 20})
	parent.GetChildQueue("leaf2").GetChildQueue("leaf21").allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})
	leaf22.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 30})
	leaf111.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 30})
	leaf211.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})

	testCases := []struct {
		name              string
		parentQueue       *Queue
		parentPreemptable *resources.Resource
		leaf111PRes       *resources.Resource
		leaf12PRes        *resources.Resource
		leaf211PRes       *resources.Resource
		leaf22PRes        *resources.Resource
	}{
		{"normal preemptable resources  - normal distribution", parent, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 22}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 41}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 24})},

		{"twice the preemptable resources - twice the normal distribution", parent, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 200}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 44}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 22}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 83}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 49})},

		{"half the preemptable resources - half the normal distribution", parent, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 20}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 12})},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			childQueues := make(map[*Queue]*QuotaPreemptionContext)
			getChildQueuesPreemptableResource(tc.parentQueue, tc.parentPreemptable, childQueues)
			assert.Equal(t, len(childQueues), 4)
			assert.Equal(t, resources.Equals(childQueues[leaf111].preemptableResource, tc.leaf111PRes), true)
			assert.Equal(t, resources.Equals(childQueues[leaf12].preemptableResource, tc.leaf12PRes), true)
			assert.Equal(t, resources.Equals(childQueues[leaf211].preemptableResource, tc.leaf211PRes), true)
			assert.Equal(t, resources.Equals(childQueues[leaf22].preemptableResource, tc.leaf22PRes), true)
			if _, ok := childQueues[parent.GetChildQueue("leaf3")]; ok {
				t.Fatal("leaf 3 queue exists")
			}
			if _, ok := childQueues[parent.GetChildQueue("leaf4")]; ok {
				t.Fatal("leaf 4 queue exists")
			}
		})
	}
}

// TestQuotaChangeGetChildQueuesPreemptableResourceWithDifferentResTypes Test child queues distribution from parent's preemptable resources under different circumstances with different resource types
// Queue Structure:
// parent
//
//	leaf 1 (Guaranteed set for this hierarchy)
//		leaf11
//			leaf111
//		leaf12
//	leaf2 (Guaranteed not set for this hierarchy)
//		leaf21
//			leaf211
//		leaf22
//	leaf3 (No usage)
//	leaf4 (Guaranteed set but equals usage)
func TestQuotaChangeGetChildQueuesPreemptableResourceWithDifferentResTypes(t *testing.T) {
	parentConfig := configs.QueueConfig{Name: "parent", Parent: true}
	parent, err := NewConfiguredQueue(parentConfig, nil, false, nil)
	assert.NilError(t, err)

	leaf111, leaf12, leaf211, leaf22, leaf4 := createQueueSetups(t, parent, configs.Resources{Guaranteed: map[string]string{"first": "10", "third": "10"}}, configs.Resources{})

	parent.GetChildQueue("leaf1").allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "second": 50})
	parent.GetChildQueue("leaf2").allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 80})
	leaf4.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})
	parent.GetChildQueue("leaf1").GetChildQueue("leaf11").allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 30, "second": 30})
	parent.GetChildQueue("leaf1").GetChildQueue("leaf12").allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 20, "second": 20})
	parent.GetChildQueue("leaf2").GetChildQueue("leaf21").allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})
	leaf22.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 30})
	leaf111.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 30, "second": 30})
	leaf211.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})

	testCases := []struct {
		name              string
		parentQueue       *Queue
		parentPreemptable *resources.Resource
		leaf111PRes       *resources.Resource
		leaf12PRes        *resources.Resource
		leaf211PRes       *resources.Resource
		leaf22PRes        *resources.Resource
	}{
		{"normal preemptable resources  - normal distribution", parent, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "fourth": 100}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 22}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 41}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 24})},

		{"twice the preemptable resources - twice the normal distribution", parent, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 200, "fourth": 100}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 44}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 22}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 83}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 49})},

		{"half the preemptable resources - half the normal distribution", parent, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "fourth": 100}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 20}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 12})},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			childQueues := make(map[*Queue]*QuotaPreemptionContext)
			getChildQueuesPreemptableResource(tc.parentQueue, tc.parentPreemptable, childQueues)
			assert.Equal(t, len(childQueues), 4)
			assert.Equal(t, resources.Equals(childQueues[leaf111].preemptableResource, tc.leaf111PRes), true)
			assert.Equal(t, resources.Equals(childQueues[leaf12].preemptableResource, tc.leaf12PRes), true)
			assert.Equal(t, resources.Equals(childQueues[leaf211].preemptableResource, tc.leaf211PRes), true)
			assert.Equal(t, resources.Equals(childQueues[leaf22].preemptableResource, tc.leaf22PRes), true)
			if _, ok := childQueues[parent.GetChildQueue("leaf3")]; ok {
				t.Fatal("leaf 3 queue exists")
			}
			if _, ok := childQueues[parent.GetChildQueue("leaf4")]; ok {
				t.Fatal("leaf 4 queue exists")
			}
		})
	}
}

func TestQuotaChangeTryPreemptionForParentQueue(t *testing.T) {
	events.Init()
	eventSystem := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	eventSystem.StartServiceWithPublisher(false)
	node := NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 500}},
		},
	})
	parent, err := NewConfiguredQueue(configs.QueueConfig{Name: "parent", Parent: true}, nil, false, nil)
	assert.NilError(t, err)
	parent1, err := NewConfiguredQueue(configs.QueueConfig{Name: "parent1", Parent: true}, nil, false, nil)
	assert.NilError(t, err)
	parent2, err := NewConfiguredQueue(configs.QueueConfig{Name: "parent2", Parent: true}, nil, false, nil)
	assert.NilError(t, err)

	leaf111G, leaf12G, leaf211G, leaf22G, leaf4G := createQueueSetups(t, parent, configs.Resources{Guaranteed: map[string]string{"first": "10"}}, configs.Resources{})
	leaf111, leaf12, leaf211, leaf22, leaf4 := createQueueSetups(t, parent1, configs.Resources{}, configs.Resources{})
	leaf111WithParentG, leaf12WithParentG, leaf211WithParentG, leaf22WithParentG, leaf4WithParentG := createQueueSetups(t, parent2, configs.Resources{}, configs.Resources{})

	var suitableVictims, notSuitableVictims, suitableVictims1, suitableVictims2, suitableVictims3 []*Allocation
	suitableVictims = append(suitableVictims, createVictim(t, "ask1", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims = append(suitableVictims, createVictim(t, "ask2", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims = append(suitableVictims, createVictim(t, "ask3", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	notSuitableVictims = append(notSuitableVictims, createVictim(t, "ask3_1", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11})))

	leafGVictims, leafGNotSuitableVictims, leafVictims, leafVictimsWithParentG := make(map[*Queue][]*Allocation), make(map[*Queue][]*Allocation), make(map[*Queue][]*Allocation), make(map[*Queue][]*Allocation)
	leafGVictims[leaf111G] = suitableVictims
	leafGNotSuitableVictims[leaf111G] = notSuitableVictims
	leafVictims[leaf111] = suitableVictims

	suitableVictims1 = append(suitableVictims1, createVictim(t, "ask4", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims1 = append(suitableVictims1, createVictim(t, "ask5", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	leafGVictims[leaf12G] = suitableVictims1
	leafVictims[leaf12] = suitableVictims1

	suitableVictims2 = append(suitableVictims2, createVictim(t, "ask6", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims2 = append(suitableVictims2, createVictim(t, "ask7", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims2 = append(suitableVictims2, createVictim(t, "ask8", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims2 = append(suitableVictims2, createVictim(t, "ask9", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims2 = append(suitableVictims2, createVictim(t, "ask10", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	leafGVictims[leaf211G] = suitableVictims2
	leafVictims[leaf211] = suitableVictims2

	suitableVictims3 = append(suitableVictims3, createVictim(t, "ask11", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims3 = append(suitableVictims3, createVictim(t, "ask12", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims3 = append(suitableVictims3, createVictim(t, "ask13", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	leafGVictims[leaf22G] = suitableVictims3
	leafVictims[leaf22] = suitableVictims3

	leafGVictims[leaf4G] = []*Allocation{createVictim(t, "ask14", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))}
	leafVictims[leaf4] = []*Allocation{createVictim(t, "ask14", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))}

	leafVictimsWithParentG[leaf111WithParentG] = []*Allocation{createVictim(t, "ask15", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 13})),
		createVictim(t, "ask16", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 12}))}
	leafVictimsWithParentG[leaf12WithParentG] = []*Allocation{createVictim(t, "ask17", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 13})),
		createVictim(t, "ask18", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 12}))}
	leafVictimsWithParentG[leaf211WithParentG] = []*Allocation{createVictim(t, "ask19", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 13})),
		createVictim(t, "ask20", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 12}))}
	leafVictimsWithParentG[leaf22WithParentG] = []*Allocation{createVictim(t, "ask21", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 13})),
		createVictim(t, "ask22", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 12}))}
	leafVictimsWithParentG[leaf4WithParentG] = []*Allocation{createVictim(t, "ask23", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 13})),
		createVictim(t, "ask24", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 12}))}
	oldMax := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 130})
	newMax := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	oldMax1 := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 150})

	testCases := []struct {
		name             string
		queue            *Queue
		oldMax           *resources.Resource
		newMax           *resources.Resource
		victims          map[*Queue][]*Allocation
		claimedResources *resources.Resource
		totalVictims     int
		expectedVictims  int
	}{
		{"Guaranteed set on one side of queue hierarchy - suitable victims available", parent, oldMax, newMax, leafGVictims, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 110}), 13, 11},
		{"Guaranteed set on one side of queue hierarchy - victims available but none suitable", parent, oldMax, newMax, leafGNotSuitableVictims, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 110}), 1, 0},
		{"Guaranteed set not set on any queue - suitable victims available", parent1, oldMax, newMax, leafVictims, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 90}), 14, 9},
		{"Guaranteed set only on parent queue but not on any child queues underneath - suitable victims available", parent2, oldMax1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 20}), leafVictimsWithParentG, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 60}), 10, 5},
		{"Guaranteed set only on parent queue but not on any child queues underneath - suitable victims available", parent2, oldMax1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}), leafVictimsWithParentG, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 60}), 10, 5},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.queue.maxResource = tc.oldMax
			for q, v := range tc.victims {
				assignAllocationsToQueue(v, q)
			}
			tc.queue.maxResource = tc.newMax
			tc.queue.guaranteedResource = tc.newMax
			preemptableResource := resources.SubOnlyExisting(tc.newMax, tc.queue.allocatedResource)
			preemptor := NewQuotaPreemptor(tc.queue)
			preemptor.tryPreemption()
			victimsCount := 0
			for _, asks := range tc.victims {
				for _, a := range asks {
					if a.IsPreempted() {
						victimsCount++
					}
				}
			}
			assert.Equal(t, victimsCount, tc.expectedVictims)
			time.Sleep(500 * time.Millisecond)
			assertQuotaPreemptionEvent(t, tc.expectedVictims, "Quota Preemption results summary: preemptable resources: "+resources.Multiply(preemptableResource, -1).String()+", claimed resources: "+tc.claimedResources.String()+", selected victims: "+strconv.Itoa(tc.totalVictims)+", preempted victims: "+strconv.Itoa(tc.expectedVictims), eventSystem.Store.CollectEvents())
			for _, v := range tc.victims {
				removeAllocationAsks(node, v)
			}
			resetQueue(tc.queue)

			// clear the events sent later after earlier collection
			_ = eventSystem.Store.CollectEvents()
		})
	}
}

// TestTryPreemptionInternal tests tryPreemptionInternal which filters, sorts, and preempts victims
// based on the preemptable resource that must be set before calling this function.
func TestTryPreemptionInternal(t *testing.T) {
	node := NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 500}},
		},
	})

	leaf, err := NewConfiguredQueue(configs.QueueConfig{
		Name: "leaf",
	}, nil, false, nil)
	assert.NilError(t, err)

	testCases := []struct {
		name                 string
		preemptableResource  *resources.Resource
		maxResource          *resources.Resource
		guaranteedResource   *resources.Resource
		victims              []*Allocation
		expectedPreemptedKeys []string
	}{
		{
			name:                "nil preemptable resource - no victims preempted",
			preemptableResource: nil,
			maxResource:         resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}),
			guaranteedResource:  nil,
			victims: []*Allocation{
				createVictim(t, "a1", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})),
			},
			expectedPreemptedKeys: []string{},
		},
		{
			name:                "empty victim list - nothing to preempt",
			preemptableResource: resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}),
			maxResource:         resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}),
			guaranteedResource:  nil,
			victims:             []*Allocation{},
			expectedPreemptedKeys: []string{},
		},
		{
			name:                "single victim fits preemptable resource - preempted",
			preemptableResource: resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}),
			maxResource:         resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}),
			guaranteedResource:  nil,
			victims: []*Allocation{
				createVictim(t, "b1", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})),
			},
			expectedPreemptedKeys: []string{"b1"},
		},
		{
			name:                "victim resource exceeds preemptable - skipped",
			preemptableResource: resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}),
			maxResource:         resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}),
			guaranteedResource:  nil,
			victims: []*Allocation{
				createVictim(t, "c1", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})),
			},
			expectedPreemptedKeys: []string{},
		},
		{
			// After sorting by resource share (smaller first): d2(5/20=0.25) < d3(10/20=0.5) == d1(10/20=0.5).
			// d3 and d1 have equal share; sort is stable but the comparator returns true for both
			// directions on equal elements, so d3 ends up before d1 in practice.
			// Iteration: d2(total=5) kept, d3(total=15, ==preemptable) kept, d1(total=25 > 15) skipped.
			name:                "multiple victims - only enough to cover preemptable resource preempted",
			preemptableResource: resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15}),
			maxResource:         resources.NewResourceFromMap(map[string]resources.Quantity{"first": 20}),
			guaranteedResource:  nil,
			victims: []*Allocation{
				createVictim(t, "d1", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})),
				createVictim(t, "d2", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})),
				createVictim(t, "d3", node, 3, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})),
			},
			expectedPreemptedKeys: []string{"d2", "d3"},
		},
		{
			// e1 has a required node and is excluded by filterAllocations; only e2 passes the filter.
			name:                "required-node allocations skipped during filter",
			preemptableResource: resources.NewResourceFromMap(map[string]resources.Quantity{"first": 20}),
			maxResource:         resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}),
			guaranteedResource:  nil,
			victims: func() []*Allocation {
				a := createVictim(t, "e1", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
				a.SetRequiredNode("node")
				b := createVictim(t, "e2", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
				return []*Allocation{a, b}
			}(),
			expectedPreemptedKeys: []string{"e2"},
		},
		{
			// f1 is already preempted before the call so it is excluded by filterAllocations;
			// f2 is newly preempted by tryPreemptionInternal. Both are reported as preempted
			// because f1 retains its pre-existing preempted state throughout.
			name:                "already preempted allocations skipped during filter",
			preemptableResource: resources.NewResourceFromMap(map[string]resources.Quantity{"first": 20}),
			maxResource:         resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}),
			guaranteedResource:  nil,
			victims: func() []*Allocation {
				a := createVictim(t, "f1", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
				err := a.MarkPreempted()
				assert.NilError(t, err)
				b := createVictim(t, "f2", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
				return []*Allocation{a, b}
			}(),
			expectedPreemptedKeys: []string{"f1", "f2"},
		},
		{
			// g1 is released before the call so it is excluded by filterAllocations;
			// only g2 is preempted by tryPreemptionInternal.
			name:                "already released allocations skipped during filter",
			preemptableResource: resources.NewResourceFromMap(map[string]resources.Quantity{"first": 20}),
			maxResource:         resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}),
			guaranteedResource:  nil,
			victims: func() []*Allocation {
				a := createVictim(t, "g1", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
				err := a.SetReleased(true)
				assert.NilError(t, err)
				b := createVictim(t, "g2", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
				return []*Allocation{a, b}
			}(),
			expectedPreemptedKeys: []string{"g2"},
		},
		{
			// Best-effort mode (guaranteed==max): all victims have distinct shares,
			// sorted ascending: h4(2/10=0.2) < h1(3/10=0.3) < h3(5/10=0.5) < h2(6/10=0.6).
			// Iteration: h4(total=2) kept, h1(total=5) kept, h3(total=10, ==preemptable) kept,
			// h2(total=16 > 10) skipped.
			name:                "best-effort mode: guaranteed equals max - preempt as close as possible",
			preemptableResource: resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}),
			maxResource:         resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}),
			guaranteedResource:  resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}),
			victims: []*Allocation{
				createVictim(t, "h1", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 3})),
				createVictim(t, "h2", node, 3, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 6})),
				createVictim(t, "h3", node, 2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})),
				createVictim(t, "h4", node, 1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2})),
			},
			expectedPreemptedKeys: []string{"h4", "h1", "h3"},
		},
		{
			name:                "no matching resource type in preemptable - no victims filtered",
			preemptableResource: resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100}),
			maxResource:         resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}),
			guaranteedResource:  nil,
			victims: []*Allocation{
				createVictim(t, "i1", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})),
			},
			expectedPreemptedKeys: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			leaf.maxResource = tc.maxResource
			leaf.guaranteedResource = tc.guaranteedResource
			asks := tc.victims
			assignAllocationsToQueue(asks, leaf)

			preemptor := NewQuotaPreemptor(leaf)
			preemptor.preemptableResource = tc.preemptableResource

			preemptor.tryPreemptionInternal()

			preemptedKeys := make([]string, 0)
			for _, a := range asks {
				if a.IsPreempted() {
					preemptedKeys = append(preemptedKeys, a.GetAllocationKey())
				}
			}
			sort.Strings(preemptedKeys)
			expectedKeys := append([]string{}, tc.expectedPreemptedKeys...)
			sort.Strings(expectedKeys)
			assert.DeepEqual(t, preemptedKeys, expectedKeys)

			removeAllocationAsks(node, asks)
			resetQueue(leaf)
		})
 }
}

func assertQuotaPreemptionEvent(t *testing.T, victims int, results string, records []*si.EventRecord) {
	recordsLen := len(records)
	if victims > 0 {
		assert.Equal(t, si.EventRecord_QUEUE, records[recordsLen-1].Type)
		assert.Equal(t, si.EventRecord_SET, records[recordsLen-1].EventChangeType)
		assert.Equal(t, si.EventRecord_QUEUE_PREEMPTION, records[recordsLen-1].EventChangeDetail)
		assert.Equal(t, results, records[recordsLen-1].Message)
	} else {
		assert.Assert(t, !strings.Contains(records[len(records)-1].Message, "Quota Preemption results summary"))
	}
}

// createQueueSetups Creates a queue hierarchy
// Queue Structure:
// parent
//
//	leaf 1 (Guaranteed set/or not set for this hierarchy)
//		leaf11
//			leaf111
//		leaf12
//	leaf2 (Guaranteed not set for this hierarchy)
//		leaf21
//			leaf211
//		leaf22
//	leaf3
//	leaf4
func createQueueSetups(t *testing.T, parent *Queue, leafResG configs.Resources, leafRes configs.Resources) (*Queue, *Queue, *Queue, *Queue, *Queue) {
	leaf1, err := NewConfiguredQueue(configs.QueueConfig{Name: "leaf1", Parent: true, Resources: leafResG}, parent, false, nil)
	assert.NilError(t, err)

	leaf2, err := NewConfiguredQueue(configs.QueueConfig{Name: "leaf2", Parent: true, Resources: leafRes}, parent, false, nil)
	assert.NilError(t, err)

	_, err = NewConfiguredQueue(configs.QueueConfig{Name: "leaf3", Resources: leafRes}, parent, false, nil)
	assert.NilError(t, err)

	leaf4, err := NewConfiguredQueue(configs.QueueConfig{Name: "leaf4", Resources: leafResG}, parent, false, nil)
	assert.NilError(t, err)

	leaf11, err := NewConfiguredQueue(configs.QueueConfig{Name: "leaf11", Parent: true, Resources: leafResG}, leaf1, false, nil)
	assert.NilError(t, err)

	leaf12, err := NewConfiguredQueue(configs.QueueConfig{Name: "leaf12", Resources: leafResG}, leaf1, false, nil)
	assert.NilError(t, err)

	leaf21, err := NewConfiguredQueue(configs.QueueConfig{Name: "leaf21", Parent: true, Resources: leafRes}, leaf2, false, nil)
	assert.NilError(t, err)

	leaf22, err := NewConfiguredQueue(configs.QueueConfig{Name: "leaf22", Resources: leafRes}, leaf2, false, nil)
	assert.NilError(t, err)

	leaf111, err := NewConfiguredQueue(configs.QueueConfig{Name: "leaf111", Resources: leafResG}, leaf11, false, nil)
	assert.NilError(t, err)

	leaf211, err := NewConfiguredQueue(configs.QueueConfig{Name: "leaf211", Resources: leafRes}, leaf21, false, nil)
	assert.NilError(t, err)

	return leaf111, leaf12, leaf211, leaf22, leaf4
}

func createVictim(t *testing.T, allocKey string, node *Node, adjustment int, allocRes *resources.Resource) *Allocation {
	createTime := time.Now()
	allocation := createAllocation(allocKey, "app1", node.NodeID, true, false, 10, false, allocRes)
	allocation.createTime = createTime.Add(-time.Minute * time.Duration(adjustment))
	assert.Assert(t, node.TryAddAllocation(allocation))
	return allocation
}
