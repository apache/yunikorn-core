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
	evtMock "github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	schedEvt "github.com/apache/yunikorn-core/pkg/scheduler/objects/events"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestFilterAllocationsForeign(t *testing.T) {
	for _, tc := range []struct {
		name         string
		foreignType  string
		priority     int32
		requiredNode string
		preempted    bool
		released     bool
		resourceName string
		eligible     bool
		expected     filteringResult
	}{
		{name: "foreign lower priority", foreignType: siCommon.AllocTypeDefault, priority: 9, eligible: true},
		{name: "foreign equal priority", foreignType: siCommon.AllocTypeDefault, priority: 10,
			expected: filteringResult{higherPriorityAllocations: 1}},
		{name: "foreign higher priority", foreignType: siCommon.AllocTypeDefault, priority: 11,
			expected: filteringResult{higherPriorityAllocations: 1}},
		{name: "static pod", foreignType: siCommon.AllocTypeStatic, priority: 9},
		{name: "foreign daemon set", foreignType: siCommon.AllocTypeDefault, priority: 9, requiredNode: "node",
			expected: filteringResult{requiredNodeAllocations: 1}},
		{name: "foreign already preempted", foreignType: siCommon.AllocTypeDefault, priority: 9, preempted: true,
			expected: filteringResult{alreadyPreemptedAllocations: 1}},
		{name: "foreign released", foreignType: siCommon.AllocTypeDefault, priority: 9, released: true,
			expected: filteringResult{releasedPhAllocations: 1}},
		{name: "foreign nonmatching resource", foreignType: siCommon.AllocTypeDefault, priority: 9, resourceName: "second",
			expected: filteringResult{atLeastOneResNotMatched: 1}},
		{name: "managed lower priority", priority: 9, eligible: true},
		{name: "managed equal priority", priority: 10, eligible: true},
		{name: "managed higher priority", priority: 11,
			expected: filteringResult{higherPriorityAllocations: 1}},
		{name: "managed daemon set", priority: 9, requiredNode: "node",
			expected: filteringResult{requiredNodeAllocations: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := newNode("node", map[string]resources.Quantity{"first": 10, "second": 10})
			resourceName := tc.resourceName
			if resourceName == "" {
				resourceName = "first"
			}
			tags := map[string]string{}
			appID := "app"
			if tc.foreignType != "" {
				tags[siCommon.Foreign] = tc.foreignType
				appID = ""
			}
			allocation := NewAllocationFromSI(&si.Allocation{
				AllocationKey:    "victim",
				ApplicationID:    appID,
				NodeID:           node.NodeID,
				Priority:         tc.priority,
				AllocationTags:   tags,
				ResourcePerAlloc: resources.NewResourceFromMap(map[string]resources.Quantity{resourceName: 5}).ToProto(),
			})
			allocation.requiredNode = tc.requiredNode
			allocation.released = tc.released
			if tc.preempted {
				assert.NilError(t, allocation.MarkPreempted())
			}
			assert.Assert(t, node.TryAddAllocation(allocation))
			requiredAsk := newAllocationAskPriority("ask", "app", resources.NewResourceFromMap(
				map[string]resources.Quantity{"first": 5}), 10)
			p := NewRequiredNodePreemptor(node, requiredAsk, nil)

			result := p.filterAllocations()

			tc.expected.totalAllocations = 1
			assert.DeepEqual(t, result, tc.expected)
			if tc.eligible {
				assert.Equal(t, len(p.allocations), 1)
				assert.Assert(t, p.allocations[0] == allocation)
			} else {
				assert.Equal(t, len(p.allocations), 0)
			}
		})
	}
}

func TestRequiredNodePreemptionForeignVictims(t *testing.T) {
	for _, managedVictims := range []int{0, 1, 2} {
		name := []string{"foreign only", "mixed", "managed only"}[managedVictims]
		t.Run(name, func(t *testing.T) {
			node := newNode("node", map[string]resources.Quantity{"first": 10})
			appQueueMapping := NewAppQueueMapping()
			rootQ, err := createRootQueue(map[string]string{"first": "10"})
			assert.NilError(t, err)
			queue, err := createManagedQueueWithAppQueueMapping(rootQ, "default", false,
				map[string]string{"first": "10"}, appQueueMapping)
			assert.NilError(t, err)
			app := newApplication("app", "default", "root.default")
			app.SetQueue(queue)
			appQueueMapping.AddAppQueueMapping(app.ApplicationID, queue)
			var releaseEvents []*rmevent.RMReleaseAllocationEvent
			app.rmEventHandler = &mockAppEventHandler{callback: func(ev interface{}) {
				if releaseEvent, ok := ev.(*rmevent.RMReleaseAllocationEvent); ok {
					releaseEvents = append(releaseEvents, releaseEvent)
				}
			}}
			eventSystem := evtMock.NewEventSystem()
			victimResource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
			victims := make(map[string]*Allocation)
			for i, key := range []string{"victim-1", "victim-2"} {
				var victim *Allocation
				if i < managedVictims {
					victim = newAllocationWithKey(key, app.ApplicationID, node.NodeID, victimResource)
				} else {
					victim = newForeignAllocation(key, node.NodeID, victimResource)
				}
				victim.askEvents = schedEvt.NewAskEvents(eventSystem)
				assert.Assert(t, node.TryAddAllocation(victim))
				victims[key] = victim
			}
			requiredAsk := newAllocationAskPriority("ask", app.ApplicationID,
				resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}), 10)
			requiredAsk.requiredNode = node.NodeID

			NewRequiredNodePreemptor(node, requiredAsk, app).tryPreemption()

			assert.Assert(t, requiredAsk.HasTriggeredPreemption())
			assert.Equal(t, len(releaseEvents), 1)
			assert.Equal(t, len(releaseEvents[0].ReleasedAllocations), 2)
			for _, release := range releaseEvents[0].ReleasedAllocations {
				victim := victims[release.AllocationKey]
				assert.Assert(t, victim != nil)
				assert.Assert(t, victim.IsPreempted())
				assert.Equal(t, release.ApplicationID, victim.GetApplicationID())
				assert.Equal(t, release.TerminationType, si.TerminationType_PREEMPTED_BY_SCHEDULER)
				delete(victims, release.AllocationKey)
			}
			assert.Equal(t, len(victims), 0)
			expectedPreempting := resources.NewResourceFromMap(map[string]resources.Quantity{
				"first": resources.Quantity(managedVictims * 5),
			})
			assert.Assert(t, resources.Equals(queue.GetPreemptingResource(), expectedPreempting))
			assert.Assert(t, resources.Equals(rootQ.GetPreemptingResource(), expectedPreempting))
			assert.Equal(t, len(eventSystem.Events), managedVictims)
		})
	}
}

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
	node := NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 76}},
		},
	})

	rootQ, err := createRootQueue(map[string]string{"first": "100"})
	assert.NilError(t, err)
	defaultQ, err := createManagedQueueGuaranteed(rootQ, "default", true, map[string]string{"vcores": "3"}, nil, nil)
	assert.NilError(t, err)

	app := newApplication("app1", "default", "root.default")
	app.SetQueue(defaultQ)

	// case 1: victims are available and its resources are matching with ds request ask
	requiredAsk := createAllocationAsk("ask11", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25}))

	p := NewRequiredNodePreemptor(node, requiredAsk, app)
	asks := prepareAllocationAsks(t, node)
	p.filterAllocations()
	p.sortAllocations()
	prunedVictims := p.pruneVictims(p.allocations)
	assert.Equal(t, len(prunedVictims), 4)
	assert.Equal(t, prunedVictims[0].GetAllocationKey(), "ask5")
	assert.Equal(t, resources.Equals(prunedVictims[0].GetAllocatedResource(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})), true)
	assert.Equal(t, prunedVictims[1].GetAllocationKey(), "ask4")
	assert.Equal(t, resources.Equals(prunedVictims[1].GetAllocatedResource(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})), true)
	assert.Equal(t, prunedVictims[2].GetAllocationKey(), "ask2")
	assert.Equal(t, resources.Equals(prunedVictims[2].GetAllocatedResource(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8})), true)
	assert.Equal(t, prunedVictims[3].GetAllocationKey(), "ask1")
	assert.Equal(t, resources.Equals(prunedVictims[3].GetAllocatedResource(), resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})), true)
	removeAllocationAsks(node, asks)

	// case 2: victims are available and its resources are matching with ds request ask (but with different quantity)
	requiredAsk2 := createAllocationAsk("ask13", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p2 := NewRequiredNodePreemptor(node, requiredAsk2, app)
	asks = prepareAllocationAsks(t, node)
	p2.filterAllocations()
	p2.sortAllocations()
	prunedVictims2 := p2.pruneVictims(p2.allocations)
	assert.Equal(t, len(prunedVictims2), 1)
	removeAllocationAsks(node, asks)

	// case 3: allocations are available and its resources are matching partially with ds request ask (because of different resource types), hence no victims
	requiredAsk3 := createAllocationAsk("ask13", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 5}))
	p3 := NewRequiredNodePreemptor(node, requiredAsk3, app)
	asks = prepareAllocationAsks(t, node)
	p3.filterAllocations()
	filteredAllocations := p3.getAllocations()

	// allocations are considered as there is partial match
	assert.Equal(t, len(filteredAllocations), 10)
	p3.sortAllocations()

	// allocations are available but no exact match for choosing victims
	prunedVictims3 := p3.pruneVictims(p3.allocations)
	assert.Equal(t, len(prunedVictims3), 0)
	removeAllocationAsks(node, asks)

	// case 4: victim chosen earlier released in the meantime
	requiredAsk4 := createAllocationAsk("ask14", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25}))
	p4 := NewRequiredNodePreemptor(node, requiredAsk4, app)
	asks = prepareAllocationAsks(t, node)
	p4.filterAllocations()
	p4.sortAllocations()
	err = asks[1].SetReleased(true)
	assert.NilError(t, err)
	prunedVictims4 := p4.pruneVictims(p4.allocations)
	assert.Equal(t, len(prunedVictims4), 4)
	removeAllocationAsks(node, asks)

	// case 5: node already has partial available resource (24 free out of 100), so asking for 25 only requires 1 victim (ask5: 5) instead of 4
	nodeWithAvail := NewNode(&si.NodeInfo{
		NodeID:     "node-avail",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 100}},
		},
	})
	p5 := NewRequiredNodePreemptor(nodeWithAvail, requiredAsk, app)
	asks = prepareAllocationAsks(t, nodeWithAvail)
	p5.filterAllocations()
	p5.sortAllocations()
	victims5 := p5.pruneVictims(p5.allocations)
	assert.Equal(t, len(victims5), 1)
	assert.Equal(t, victims5[0].GetAllocationKey(), "ask5")
	removeAllocationAsks(nodeWithAvail, asks)
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
