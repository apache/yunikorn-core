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

	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestQuotaChangeCheckPreconditions(t *testing.T) {
	parentConfig := configs.QueueConfig{
		Name:   "parent",
		Parent: true,
		Resources: configs.Resources{
			Max: map[string]string{"memory": "1000"},
		},
	}
	parent, err := NewConfiguredQueue(parentConfig, nil, false, nil)
	assert.NilError(t, err)
	parent.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 2000, "cpu": 2000})

	leafRes := configs.Resources{
		Max: map[string]string{"memory": "1000"},
	}
	leaf, err := NewConfiguredQueue(configs.QueueConfig{
		Name:      "leaf",
		Resources: leafRes,
	}, parent, false, nil)
	assert.NilError(t, err)

	dynamicLeaf, err := NewConfiguredQueue(configs.QueueConfig{
		Name:      "dynamic-leaf",
		Resources: leafRes,
	}, parent, false, nil)
	assert.NilError(t, err)
	dynamicLeaf.isManaged = false

	alreadyPreemptionRunning, err := NewConfiguredQueue(configs.QueueConfig{
		Name:      "leaf-already-preemption-running",
		Resources: leafRes,
	}, parent, false, nil)
	assert.NilError(t, err)

	usageExceededMaxQueue, err := NewConfiguredQueue(configs.QueueConfig{
		Name:      "leaf-usage-exceeded-max",
		Resources: leafRes,
	}, parent, false, nil)
	assert.NilError(t, err)
	usageExceededMaxQueue.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 2000, "cpu": 2000})

	usageEqualsMaxQueue, err := NewConfiguredQueue(configs.QueueConfig{
		Name:      "leaf-usage-equals-max",
		Resources: leafRes,
	}, parent, false, nil)
	assert.NilError(t, err)
	usageEqualsMaxQueue.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000, "cpu": 1000})

	usageNotMatchingMaxQueue, err := NewConfiguredQueue(configs.QueueConfig{
		Name: "leaf-usage-res-not-matching-max-res",
		Resources: configs.Resources{
			Max: map[string]string{"cpu": "1000"},
		},
	}, parent, false, nil)
	assert.NilError(t, err)
	usageNotMatchingMaxQueue.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000})

	testCases := []struct {
		name               string
		queue              *Queue
		preemptionRunning  bool
		preconditionResult bool
	}{
		{"parent queue", parent, false, true},
		{"leaf queue", leaf, false, false},
		{"dynamic leaf queue", dynamicLeaf, false, false},
		{"leaf queue, usage exceeded max resources", usageExceededMaxQueue, false, true},
		{"leaf queue, usage equals max resources", usageEqualsMaxQueue, false, false},
		{"leaf queue, already preemption process started or running", alreadyPreemptionRunning, true, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.queue.MarkQuotaChangePreemptionRunning(tc.preemptionRunning)
			preemptor := NewQuotaChangePreemptor(tc.queue)
			assert.Equal(t, preemptor.CheckPreconditions(), tc.preconditionResult)
		})
	}
	// Since parent's leaf queue "leaf-already-preemption-running" is running, parent preconditions passed earlier should fail now
	preemptor := NewQuotaChangePreemptor(parent)
	assert.Equal(t, preemptor.CheckPreconditions(), false)
}

func TestQuotaChangeGetPreemptableResource(t *testing.T) {
	leaf, err := NewConfiguredQueue(configs.QueueConfig{
		Name: "leaf",
	}, nil, false, nil)
	assert.NilError(t, err)

	testCases := []struct {
		name         string
		queue        *Queue
		maxResource  *resources.Resource
		usedResource *resources.Resource
		preemptable  *resources.Resource
	}{
		{"nil max and nil used", leaf, nil, nil, nil},
		{"nil max", leaf, nil, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), nil},
		{"nil used", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), nil, nil},
		{"used below max", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500}), nil},
		{"used above max", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1500}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500})},
		{"used above max, below max, equals max in specific res type and also with extra res types", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000, "cpu": 10, "gpu": 10}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1500, "cpu": 10, "gpu": 9}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500})},
		{"used res type but max undefined", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 150}), nil},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.queue.maxResource = tc.maxResource
			tc.queue.allocatedResource = tc.usedResource
			preemptor := NewQuotaChangePreemptor(tc.queue)
			assert.Equal(t, resources.Equals(preemptor.getPreemptableResources(), tc.preemptable), true)
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
			preemptor := NewQuotaChangePreemptor(tc.queue)
			preemptor.preemptableResource = tc.preemptableResource
			allocations := preemptor.filterAllocations()
			assert.Equal(t, len(allocations), tc.expectedAllocationsCount)
			removeAllocationAsks(node, asks)
			resetQueue(leaf)
		})
	}
}

func TestQuotaChangeTryPreemption(t *testing.T) {
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

	suitableVictims := make([]*Allocation, 0)
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
		totalExpectedVictims int
		expectedVictimsCount int
	}{
		{"no victims available", leaf, oldMax, newMax, nil, preemptable, []*Allocation{}, 0, 0},
		{"suitable victims available", leaf, oldMax, newMax, nil, preemptable, suitableVictims, 2, 1},
		{"skip over sized victims", leaf, oldMax, newMax, nil, preemptable, oversizedVictims, 2, 1},
		{"guaranteed not set, victims total resource might go over the requirement a bit", leaf, oldMax, newMax, nil, preemptable, overflowVictims, 3, 2},
		{"guaranteed set but lower than max, victims total resource might go over the requirement a bit", leaf, oldMax, newMax, lowerGuaranteed, preemptable, overflowVictims, 3, 2},
		{"best effort - guaranteed set and equals max, victims total resource might fall below the requirement a bit", leaf, oldMax, newMax, guaranteed, preemptable, shortfallVictims, 4, 2},
		{"best effort - guaranteed set, max not set earlier but now, victims total resource might fall below the requirement a bit", leaf, nil, newMax, guaranteed, preemptable, shortfallVictims, 4, 2},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			leaf.maxResource = tc.oldMax
			leaf.guaranteedResource = tc.guaranteed
			asks := tc.victims
			assignAllocationsToQueue(asks, leaf)
			leaf.maxResource = tc.newMax
			leaf.guaranteedResource = tc.guaranteed
			preemptor := NewQuotaChangePreemptor(tc.queue)
			preemptor.allocations = asks
			preemptor.tryPreemption()
			assert.Equal(t, len(preemptor.getVictims()), tc.totalExpectedVictims)
			var victimsCount int
			for _, a := range asks {
				if a.IsPreempted() {
					victimsCount++
				}
			}
			assert.Equal(t, victimsCount, tc.expectedVictimsCount)
			removeAllocationAsks(node, asks)
			resetQueue(leaf)
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
				{overflowVictims, 3, 2},
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
				preemptor := NewQuotaChangePreemptor(tc.queue)
				preemptor.tryPreemption()
				assert.Equal(t, len(preemptor.getVictims()), v.totalExpectedVictims)
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
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 42}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25})},

		{"twice the preemptable resources - twice the normal distribution", parent, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 200}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 45}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 22}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 83}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})},

		{"half the preemptable resources - half the normal distribution", parent, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 6}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 21}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 12})},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			childQueues := make(map[*Queue]*resources.Resource)
			getChildQueuesPreemptableResource(tc.parentQueue, tc.parentPreemptable, childQueues)
			assert.Equal(t, len(childQueues), 4)
			assert.Equal(t, resources.Equals(childQueues[leaf111], tc.leaf111PRes), true)
			assert.Equal(t, resources.Equals(childQueues[leaf12], tc.leaf12PRes), true)
			assert.Equal(t, resources.Equals(childQueues[leaf211], tc.leaf211PRes), true)
			assert.Equal(t, resources.Equals(childQueues[leaf22], tc.leaf22PRes), true)
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
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 42}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25})},

		{"twice the preemptable resources - twice the normal distribution", parent, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 200, "fourth": 100}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 45}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 22}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 83}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50})},

		{"half the preemptable resources - half the normal distribution", parent, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 50, "fourth": 100}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 6}),
			resources.NewResourceFromMap(map[string]resources.Quantity{"first": 21}), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 12})},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			childQueues := make(map[*Queue]*resources.Resource)
			getChildQueuesPreemptableResource(tc.parentQueue, tc.parentPreemptable, childQueues)
			assert.Equal(t, len(childQueues), 4)
			assert.Equal(t, resources.Equals(childQueues[leaf111], tc.leaf111PRes), true)
			assert.Equal(t, resources.Equals(childQueues[leaf12], tc.leaf12PRes), true)
			assert.Equal(t, resources.Equals(childQueues[leaf211], tc.leaf211PRes), true)
			assert.Equal(t, resources.Equals(childQueues[leaf22], tc.leaf22PRes), true)
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
	node := NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 200}},
		},
	})

	parentConfig := configs.QueueConfig{Name: "parent", Parent: true}
	parent, err := NewConfiguredQueue(parentConfig, nil, false, nil)
	assert.NilError(t, err)

	parentConfig1 := configs.QueueConfig{Name: "parent1", Parent: true}
	parent1, err := NewConfiguredQueue(parentConfig1, nil, false, nil)
	assert.NilError(t, err)

	leaf111G, leaf12G, leaf211G, leaf22G, leaf4G := createQueueSetups(t, parent, configs.Resources{Guaranteed: map[string]string{"first": "10"}}, configs.Resources{})
	leaf111, leaf12, leaf211, leaf22, leaf4 := createQueueSetups(t, parent1, configs.Resources{}, configs.Resources{})

	suitableVictims := make([]*Allocation, 0)
	suitableVictims = append(suitableVictims, createVictim(t, "ask1", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims = append(suitableVictims, createVictim(t, "ask2", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims = append(suitableVictims, createVictim(t, "ask3", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))

	leafGVictims := make(map[*Queue][]*Allocation)
	leafGVictims[leaf111G] = suitableVictims
	leafVictims := make(map[*Queue][]*Allocation)
	leafVictims[leaf111] = suitableVictims

	suitableVictims1 := make([]*Allocation, 0)
	suitableVictims1 = append(suitableVictims1, createVictim(t, "ask4", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims1 = append(suitableVictims1, createVictim(t, "ask5", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	leafGVictims[leaf12G] = suitableVictims1
	leafVictims[leaf12] = suitableVictims1

	suitableVictims2 := make([]*Allocation, 0)
	suitableVictims2 = append(suitableVictims2, createVictim(t, "ask6", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims2 = append(suitableVictims2, createVictim(t, "ask7", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims2 = append(suitableVictims2, createVictim(t, "ask8", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims2 = append(suitableVictims2, createVictim(t, "ask9", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims2 = append(suitableVictims2, createVictim(t, "ask10", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	leafGVictims[leaf211G] = suitableVictims2
	leafVictims[leaf211] = suitableVictims2

	suitableVictims3 := make([]*Allocation, 0)
	suitableVictims3 = append(suitableVictims3, createVictim(t, "ask11", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims3 = append(suitableVictims3, createVictim(t, "ask12", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))
	suitableVictims3 = append(suitableVictims3, createVictim(t, "ask13", node, 4, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})))

	leafGVictims[leaf22G] = suitableVictims3
	leafVictims[leaf22] = suitableVictims3

	v := createVictim(t, "ask14", node, 5, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	leafGVictims[leaf4G] = []*Allocation{v}
	leafVictims[leaf4] = []*Allocation{v}

	expectedGVictims := make(map[*Queue]int)
	expectedGVictims[leaf111G] = 2
	expectedGVictims[leaf12G] = 1
	expectedGVictims[leaf211G] = 5
	expectedGVictims[leaf22G] = 3

	expectedVictims := make(map[*Queue]int)
	expectedVictims[leaf111] = 3
	expectedVictims[leaf12] = 2
	expectedVictims[leaf211] = 5
	expectedVictims[leaf22] = 3

	oldMax := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 130})
	newMax := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})

	testCases := []struct {
		name            string
		queue           *Queue
		oldMax          *resources.Resource
		newMax          *resources.Resource
		victims         map[*Queue][]*Allocation
		expectedVictims map[*Queue]int
	}{
		{"Guaranteed set on one side of queue hierarchy - suitable victims available", parent, oldMax, newMax, leafGVictims, expectedGVictims},
		{"Guaranteed set not set on any queue - suitable victims available", parent1, oldMax, newMax, leafVictims, expectedVictims},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.queue.maxResource = tc.oldMax
			for q, v := range tc.victims {
				assignAllocationsToQueue(v, q)
			}
			tc.queue.maxResource = tc.newMax
			preemptor := NewQuotaChangePreemptor(tc.queue)
			preemptor.tryPreemption()
			for q, asks := range tc.victims {
				var victimsCount int
				for _, a := range asks {
					if a.IsPreempted() {
						victimsCount++
					}
				}
				assert.Equal(t, victimsCount, tc.expectedVictims[q])
			}
			for _, v := range tc.victims {
				removeAllocationAsks(node, v)
			}
			resetQueue(tc.queue)
		})
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
