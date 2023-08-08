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
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func createAllocationAsk(allocationKey string, app string, allowPreemption bool, isOriginator bool, priority int32, res *resources.Resource) *AllocationAsk {
	tags := map[string]string{}
	siAsk := &si.AllocationAsk{
		AllocationKey:    allocationKey,
		ApplicationID:    app,
		PartitionName:    "default",
		Priority:         priority,
		ResourceAsk:      res.ToProto(),
		Originator:       isOriginator,
		PreemptionPolicy: &si.PreemptionPolicy{AllowPreemptSelf: allowPreemption, AllowPreemptOther: true},
		Tags:             tags,
	}
	ask := NewAllocationAskFromSI(siAsk)
	return ask
}

func prepareAllocationAsks(node *Node) {
	createTime := time.Now()

	// regular pods
	ask1 := createAllocationAsk("ask1", "app1", true, false, 10,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	node.AddAllocation(NewAllocation("1", node.NodeID, ask1))

	ask2 := createAllocationAsk("ask2", "app1", true, false, 10,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8}))
	ask2.createTime = createTime
	node.AddAllocation(NewAllocation("2", node.NodeID, ask2))

	ask3 := createAllocationAsk("ask3", "app1", true, false, 15,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	node.AddAllocation(NewAllocation("3", node.NodeID, ask3))

	ask4 := createAllocationAsk("ask4", "app1", true, false, 10,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask4.createTime = createTime
	node.AddAllocation(NewAllocation("4", node.NodeID, ask4))

	ask5 := createAllocationAsk("ask5", "app1", true, false, 5,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	node.AddAllocation(NewAllocation("5", node.NodeID, ask5))

	// opted out pods
	ask6 := createAllocationAsk("ask6", "app1", false, false, 10,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	node.AddAllocation(NewAllocation("6", node.NodeID, ask6))

	ask7 := createAllocationAsk("ask7", "app1", false, false, 10,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8}))
	ask7.createTime = createTime
	node.AddAllocation(NewAllocation("7", node.NodeID, ask7))

	ask8 := createAllocationAsk("ask8", "app1", false, false, 15,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	node.AddAllocation(NewAllocation("8", node.NodeID, ask8))

	// driver/owner pods
	ask9 := createAllocationAsk("ask9", "app1", false, true, 10,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask9.createTime = createTime
	node.AddAllocation(NewAllocation("9", node.NodeID, ask9))

	ask10 := createAllocationAsk("ask10", "app1", true, true, 5,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	node.AddAllocation(NewAllocation("10", node.NodeID, ask10))
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
	requiredAsk.pendingAskRepeat = 5

	p := NewRequiredNodePreemptor(node, requiredAsk)
	prepareAllocationAsks(node)
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
	prepareAllocationAsks(node)
	p.filterAllocations()
	filteredAllocations := p.getAllocations()

	// allocations are not even considered as there is no match. of course, no victims
	assert.Equal(t, len(filteredAllocations), 0)

	// case 2: allocations are available but priority is higher than ds request priority, hence no allocations considered
	requiredAsk1 := createAllocationAsk("ask12", "app1", true, true, 1,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p1 := NewRequiredNodePreemptor(node, requiredAsk1)
	prepareAllocationAsks(node)
	p1.filterAllocations()
	filteredAllocations = p.getAllocations()

	// allocations are not even considered as there is no match. of course, no victims
	assert.Equal(t, len(filteredAllocations), 0)

	// case 3: victims are available as there are allocations with lower priority and resource match
	requiredAsk2 := createAllocationAsk("ask12", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p2 := NewRequiredNodePreemptor(node, requiredAsk2)
	prepareAllocationAsks(node)
	p2.filterAllocations()
	filteredAllocations = p2.getAllocations()
	assert.Equal(t, len(filteredAllocations), 10)

	// case 4: allocation has preempted
	requiredAsk3 := createAllocationAsk("ask12", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p3 := NewRequiredNodePreemptor(node, requiredAsk3)
	prepareAllocationAsks(node)
	p3.filterAllocations()
	p3.sortAllocations()

	victims := p3.GetVictims()
	for _, victim := range victims {
		victim.MarkPreempted()
	}
	p3.filterAllocations()
	filteredAllocations = p3.getAllocations()
	assert.Equal(t, len(filteredAllocations), 19)
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
	prepareAllocationAsks(node)
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

	// case 2: victims are available and its resources are matching with ds request ask (but with different quantity)
	requiredAsk2 := createAllocationAsk("ask13", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	p2 := NewRequiredNodePreemptor(node, requiredAsk2)
	prepareAllocationAsks(node)
	p2.filterAllocations()
	p2.sortAllocations()
	victims2 := p2.GetVictims()
	assert.Equal(t, len(victims2), 1)

	// case 3: allocations are available and its resources are matching partially with ds request ask (because of different resource types), hence no victims
	requiredAsk3 := createAllocationAsk("ask13", "app1", true, true, 20,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 5}))
	p3 := NewRequiredNodePreemptor(node, requiredAsk3)
	prepareAllocationAsks(node)
	p3.filterAllocations()
	filteredAllocations := p3.getAllocations()

	// allocations are considered as there is partial match
	assert.Equal(t, len(filteredAllocations), 10)
	p3.sortAllocations()

	// allocations are available but no exact match for choosing victims
	victims3 := p3.GetVictims()
	assert.Equal(t, len(victims3), 0)
}
