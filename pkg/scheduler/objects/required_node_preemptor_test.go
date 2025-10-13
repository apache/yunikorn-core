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
	}
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

	requiredAsk := createAllocationAsk("ask", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))

	p := NewRequiredNodePreemptor(node, requiredAsk)
	asks := prepareAllocationAsks(t, node)
	p.filterAllocations()
	p.sortAllocations()
	sortedAsks := p.getAllocations()

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
	result := p.filterAllocations()
	verifyFilterResult(t, 10, 0, 10, 0, 0, result)
	filteredAllocations := p.getAllocations()

	// allocations are not even considered as there is no match. of course, no victims
	assert.Equal(t, len(filteredAllocations), 0)
	removeAllocationAsks(node, asks)

	// case 2: allocations are available but priority is higher than ds request priority, hence no allocations considered
	requiredAsk1 := createAllocationAsk("ask12", "app1", true, true, 1,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p1 := NewRequiredNodePreemptor(node, requiredAsk1)
	asks = prepareAllocationAsks(t, node)
	result = p1.filterAllocations()
	verifyFilterResult(t, 10, 0, 0, 10, 0, result)
	filteredAllocations = p.getAllocations()

	// allocations are not even considered as there is no match. of course, no victims
	assert.Equal(t, len(filteredAllocations), 0)
	removeAllocationAsks(node, asks)

	// case 3: victims are available as there are allocations with lower priority and resource match
	requiredAsk2 := createAllocationAsk("ask12", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p2 := NewRequiredNodePreemptor(node, requiredAsk2)
	asks = prepareAllocationAsks(t, node)
	result = p2.filterAllocations()
	verifyFilterResult(t, 10, 0, 0, 0, 0, result)
	filteredAllocations = p2.getAllocations()
	assert.Equal(t, len(filteredAllocations), 10)
	removeAllocationAsks(node, asks)

	// case 4: allocation has been preempted
	requiredAsk3 := createAllocationAsk("ask12", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p3 := NewRequiredNodePreemptor(node, requiredAsk3)
	asks = prepareAllocationAsks(t, node)
	node.GetAllocation("ask5").MarkPreempted() // "ask5" would be the first and only victim without this
	result = p3.filterAllocations()
	p3.sortAllocations()

	verifyFilterResult(t, 10, 0, 0, 0, 1, result)
	filteredAllocations = p3.getAllocations()
	assert.Equal(t, len(filteredAllocations), 9) // "ask5" is no longer considered as a victim
	removeAllocationAsks(node, asks)

	// case 5: existing required node allocation
	requiredAsk4 := createAllocationAsk("ask12", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p4 := NewRequiredNodePreemptor(node, requiredAsk4)
	_ = prepareAllocationAsks(t, node)
	allocReqNode := createAllocation("ask11", "app1", node.NodeID, true, true, 5, true,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	assert.Assert(t, node.TryAddAllocation(allocReqNode))

	result = p4.filterAllocations()
	verifyFilterResult(t, 11, 1, 0, 0, 0, result)
	filteredAllocations = p4.getAllocations()
	assert.Equal(t, len(filteredAllocations), 10)
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

func verifyFilterResult(t *testing.T, totalAllocations, requiredNodeAllocations, resourceNotEnough, higherPriorityAllocations, alreadyPreemptedAllocations int, result filteringResult) {
	t.Helper()
	assert.Equal(t, totalAllocations, result.totalAllocations)
	assert.Equal(t, requiredNodeAllocations, result.requiredNodeAllocations)
	assert.Equal(t, resourceNotEnough, result.resourceNotEnough)
	assert.Equal(t, higherPriorityAllocations, result.higherPriorityAllocations)
	assert.Equal(t, alreadyPreemptedAllocations, result.alreadyPreemptedAllocations)
}
