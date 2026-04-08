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

	"github.com/apache/yunikorn-core/pkg/common/resources"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func createAllocationAsk(allocationKey string, app string, allowPreemption bool, isOriginator bool, priority int32, res *resources.Resource) *Allocation {
	tags := map[string]string{}
	siAsk := &si.Allocation{
		AllocationKey:    allocationKey,
		ApplicationID:    app,
		PartitionName:    "default",
		Priority:         priority,
		ResourcePerAlloc: res.ToProto(),
		Originator:       isOriginator,
		PreemptionPolicy: &si.PreemptionPolicy{AllowPreemptSelf: allowPreemption, AllowPreemptOther: true},
		AllocationTags:   tags,
	}
	ask := NewAllocationFromSI(siAsk)
	return ask
}

func createAllocation(allocationKey string, app string, nodeID string, allowPreemption bool, isOriginator bool, priority int32, requiredNode bool, res *resources.Resource) *Allocation {
	tags := map[string]string{}
	siAsk := &si.Allocation{
		AllocationKey:    allocationKey,
		ApplicationID:    app,
		PartitionName:    "default",
		Priority:         priority,
		ResourcePerAlloc: res.ToProto(),
		Originator:       isOriginator,
		PreemptionPolicy: &si.PreemptionPolicy{AllowPreemptSelf: allowPreemption, AllowPreemptOther: true},
		AllocationTags:   tags,
		NodeID:           nodeID,
	}
	if requiredNode {
		tags[siCommon.DomainYuniKorn+siCommon.KeyRequiredNode] = nodeID
	}
	ask := NewAllocationFromSI(siAsk)
	return ask
}

func prepareAllocationAsks(t *testing.T, node *Node) []*Allocation {
	createTime := time.Now()

	result := make([]*Allocation, 0)

	// regular pods
	alloc1 := createAllocation("ask1", "app1", node.NodeID, true, false, 10, false,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	assert.Assert(t, node.TryAddAllocation(alloc1))
	result = append(result, alloc1)

	alloc2 := createAllocation("ask2", "app1", node.NodeID, true, false, 10, false,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8}))
	alloc2.createTime = createTime
	assert.Assert(t, node.TryAddAllocation(alloc2))
	result = append(result, alloc2)

	alloc3 := createAllocation("ask3", "app1", node.NodeID, true, false, 15, false,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	assert.Assert(t, node.TryAddAllocation(alloc3))
	result = append(result, alloc3)

	alloc4 := createAllocation("ask4", "app1", node.NodeID, true, false, 10, false,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	alloc4.createTime = createTime
	assert.Assert(t, node.TryAddAllocation(alloc4))
	result = append(result, alloc4)

	alloc5 := createAllocation("ask5", "app1", node.NodeID, true, false, 5, false,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	assert.Assert(t, node.TryAddAllocation(alloc5))
	result = append(result, alloc5)

	// opted out pods
	alloc6 := createAllocation("ask6", "app1", node.NodeID, false, false, 10, false,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	assert.Assert(t, node.TryAddAllocation(alloc6))
	result = append(result, alloc6)

	alloc7 := createAllocation("ask7", "app1", node.NodeID, false, false, 10, false,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8}))
	alloc7.createTime = createTime
	assert.Assert(t, node.TryAddAllocation(alloc7))
	result = append(result, alloc7)

	alloc8 := createAllocation("ask8", "app1", node.NodeID, false, false, 15, false,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	assert.Assert(t, node.TryAddAllocation(alloc8))
	result = append(result, alloc8)

	// driver/owner pods
	alloc9 := createAllocation("ask9", "app1", node.NodeID, false, true, 10, false,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	alloc9.createTime = createTime
	assert.Assert(t, node.TryAddAllocation(alloc9))
	result = append(result, alloc9)

	alloc10 := createAllocation("ask10", "app1", node.NodeID, true, true, 5, false,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	assert.Assert(t, node.TryAddAllocation(alloc10))
	result = append(result, alloc10)

	return result
}

func removeAllocationAsks(node *Node, asks []*Allocation) {
	for _, ask := range asks {
		node.RemoveAllocation(ask.GetAllocationKey())
		ask.preempted = false
	}
}

func assignAllocationsToQueue(allocations []*Allocation, queue *Queue) {
	for _, allocation := range allocations {
		var app *Application
		var ok bool
		if _, ok = queue.applications[allocation.applicationID]; !ok {
			app = newApplication(allocation.applicationID, "default", queue.QueuePath)
			app.SetQueue(queue)
			queue.applications[allocation.applicationID] = app
		} else {
			app = queue.applications[allocation.applicationID]
		}
		app.AddAllocation(allocation)
		queue.IncAllocatedResource(allocation.GetAllocatedResource(), false)
	}
}

func resetQueue(queue *Queue) {
	queue.applications = make(map[string]*Application)
	queue.maxResource = nil
	queue.allocatedResource = nil
	queue.guaranteedResource = nil
	queue.isQuotaPreemptionRunning = false
	queue.preemptingResource = nil
}

// regular pods
// ask1: pri - 10, create time - 1, res - 10
// ask2: pri - 10, create time - 2, res - 8
// ask3: pri - 15, create time - 3, res - 10
// ask4: pri - 10, create time - 2, res - 5
// ask5: pri - 5, create time - 4, res - 5

// opted out pods
// ask6: pri - 10, create time - 5, res - 10
// ask7: pri - 10, create time - 2, res - 8
// ask8: pri - 15, create time - 6, res - 10

// driver/owner pods
// ask9: pri - 10, create time - 2, res - 5
// ask10: pri - 5, create time - 4, res - 5

// original asks order: 6, 7, 8, 9, 10, 1, 2, 3, 4, 5
// expected sorted asks o/p: 5, 1, 4, 2, 3, 6, 7, 8, 10, 9
func TestSortAllocations(t *testing.T) {
	node := NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 100}},
		},
	})

	asks := prepareAllocationAsks(t, node)
	SortAllocations(asks)
	sortedAsks := asks

	// assert regular pods
	assert.Equal(t, sortedAsks[0].GetAllocationKey(), "ask5")
	assert.Equal(t, sortedAsks[1].GetAllocationKey(), "ask1")
	assert.Equal(t, sortedAsks[2].GetAllocationKey(), "ask4")
	assert.Equal(t, sortedAsks[3].GetAllocationKey(), "ask2")
	assert.Equal(t, sortedAsks[4].GetAllocationKey(), "ask3")

	// assert opted out pods
	assert.Equal(t, sortedAsks[5].GetAllocationKey(), "ask6")
	assert.Equal(t, sortedAsks[6].GetAllocationKey(), "ask7")
	assert.Equal(t, sortedAsks[7].GetAllocationKey(), "ask8")

	// assert driver/owner pods
	assert.Equal(t, sortedAsks[8].GetAllocationKey(), "ask10")
	assert.Equal(t, sortedAsks[9].GetAllocationKey(), "ask9")

	removeAllocationAsks(node, asks)
}

func TestSortAllocationsBasedOnAsk(t *testing.T) {
	node := NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 100}, "second": {Value: 100}, "third": {Value: 100}, "fourth": {Value: 100},
				"extra_a": {Value: 100}, "extra_b": {Value: 100}, "extra_c": {Value: 100}, "extra_d": {Value: 100}, "extra_e": {Value: 100},
				"extra_f": {Value: 100}, "extra_g": {Value: 100}, "extra_h": {Value: 100}, "extra_i": {Value: 100}, "extra_j": {Value: 100}},
		},
	})

	type vStruct struct {
		key       string
		allocated *resources.Resource
	}

	// Victims for res types matching comparison
	res1 := vStruct{"ask1", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 13})}
	res2 := vStruct{"ask2", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 12, "second": 10})}
	res3 := vStruct{"ask3", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11, "second": 10, "third": 10})}
	res4 := vStruct{"ask4", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10, "third": 10, "fourth": 10})}
	askWithExtraRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "extra_a": 10, "extra_b": 10})

	// Victims for allocated resources comparison
	res5 := vStruct{"ask4", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 13, "extra_c": 10, "extra_d": 10})}
	res6 := vStruct{"ask3", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 12, "extra_e": 10, "extra_f": 10})}
	res7 := vStruct{"ask2", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11, "extra_g": 10, "extra_h": 10})}
	res8 := vStruct{"ask1", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "extra_i": 10, "extra_j": 10})}
	res9 := vStruct{"ask4", resources.NewResourceFromMap(map[string]resources.Quantity{"first": -13, "extra_c": 10, "extra_d": 10})}

	res10 := vStruct{"ask2", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 10})}
	res11 := vStruct{"ask1", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 5})}
	res12 := vStruct{"ask1", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})}
	res13 := vStruct{"ask2", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})}
	askWithExtraRes1 := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 1})

	total := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "second": 100, "third": 100, "fourth": 100})

	testCases := []struct {
		name            string
		victims         []vStruct
		total           *resources.Resource
		ask             *resources.Resource
		expectedVictims []string
	}{
		{"100% matching victims, final order should be based on matching % in decreasing order. ask extra res should not create any impact",
			[]vStruct{res1}, total, askWithExtraRes, []string{"ask1"}},
		{"50% matching victims, final order should be based on matching % in decreasing order. ask extra res should not create any impact",
			[]vStruct{res2, res1}, total, askWithExtraRes, []string{"ask1", "ask2"}},
		{"33% matching victims, final order should be based on matching % in decreasing order. ask extra res should not create any impact",
			[]vStruct{res3, res2, res1}, total, askWithExtraRes, []string{"ask1", "ask2", "ask3"}},
		{"25% matching victims, final order should be based on matching % in decreasing order. ask extra res should not create any impact",
			[]vStruct{res4, res3, res2, res1}, total, askWithExtraRes, []string{"ask1", "ask2", "ask3", "ask4"}},
		{"ordering based on matching res types, non matching res types does not matter",
			[]vStruct{res5, res6, res7, res8}, total, askWithExtraRes, []string{"ask1", "ask2", "ask3", "ask4"}},
		{"negative value resource, ordering based on matching res types, non matching res types does not matter",
			[]vStruct{res6, res7, res8, res9}, total, askWithExtraRes, []string{"ask4", "ask1", "ask2", "ask3"}},
		{"first victim one res type bigger and another smaller and vice versa on second victim",
			[]vStruct{res10, res11}, total, askWithExtraRes1, []string{"ask1", "ask2"}},
		{"first victim one res type bigger and another smaller and vice versa on second victim",
			[]vStruct{res13, res12}, total, askWithExtraRes1, []string{"ask1", "ask2"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var victimAllocations []*Allocation
			for _, vRes := range tc.victims {
				// Except victim used resources and ask resources, have all other criteria same for all victims
				victim := createAllocation(vRes.key, "app1", node.NodeID, true, false, 10, false,
					vRes.allocated)
				assert.Assert(t, node.TryAddAllocation(victim))
				victimAllocations = append(victimAllocations, victim)
			}
			SortAllocationsBasedOnAsk(victimAllocations, tc.total, tc.ask)
			for index, sortedAsk := range victimAllocations {
				assert.Equal(t, sortedAsk.GetAllocationKey(), tc.expectedVictims[index])
			}
			removeAllocationAsks(node, victimAllocations)
		})
	}
}
