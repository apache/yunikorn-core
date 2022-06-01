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

	"gotest.tools/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	interfaceCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	si "github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func createAllocationAsk(allocationKey string, app string, allowPreemption string, isOriginator bool, priority int32, res *resources.Resource) *AllocationAsk {
	tags := map[string]string{}
	tags[interfaceCommon.DomainYuniKorn+interfaceCommon.KeyAllowPreemption] = allowPreemption
	siAsk := &si.AllocationAsk{
		AllocationKey: allocationKey,
		ApplicationID: app,
		PartitionName: "default",
		Priority: &si.Priority{
			Priority: &si.Priority_PriorityValue{PriorityValue: priority},
		},
		ResourceAsk: res.ToProto(),
		Originator:  isOriginator,
		Tags:        tags,
	}
	ask := NewAllocationAsk(siAsk)
	return ask
}

func prepareAllocationAsks(p *PreemptionContext) {
	createTime := time.Now()

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

	// regular pods
	ask1 := createAllocationAsk("ask1", "app1", "true", false, 10,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))

	ask2 := createAllocationAsk("ask2", "app1", "true", false, 10,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8}))
	ask2.SetCreateTime(createTime)

	ask3 := createAllocationAsk("ask3", "app1", "true", false, 15,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))

	ask4 := createAllocationAsk("ask4", "app1", "true", false, 10,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask4.SetCreateTime(createTime)

	ask5 := createAllocationAsk("ask5", "app1", "true", false, 5,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))

	// opted out pods
	ask6 := createAllocationAsk("ask6", "app1", "false", false, 10,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))

	ask7 := createAllocationAsk("ask7", "app1", "false", false, 10,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8}))
	ask7.SetCreateTime(createTime)

	ask8 := createAllocationAsk("ask8", "app1", "false", false, 15,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))

	// driver/owner pods
	ask9 := createAllocationAsk("ask9", "app1", "false", true, 10,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask9.SetCreateTime(createTime)

	ask10 := createAllocationAsk("ask10", "app1", "true", true, 5,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))

	var asks []*AllocationAsk
	asks = append(asks, ask6, ask7, ask8, ask9, ask10, ask1, ask2, ask3, ask4, ask5)
	p.setAllocations(asks)
}

func TestSortAllocations(t *testing.T) {
	node := NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 100}},
		},
	})
	p := NewSimplePreemptor(node)
	prepareAllocationAsks(p)
	p.sortAllocations()
	sortedAsks := p.getAllocations()

	// assert regular pods
	assert.Equal(t, sortedAsks[0].AllocationKey, "ask5")
	assert.Equal(t, sortedAsks[1].AllocationKey, "ask1")
	assert.Equal(t, sortedAsks[2].AllocationKey, "ask4")
	assert.Equal(t, sortedAsks[3].AllocationKey, "ask2")
	assert.Equal(t, sortedAsks[4].AllocationKey, "ask3")

	// assert opted out pods
	assert.Equal(t, sortedAsks[5].AllocationKey, "ask6")
	assert.Equal(t, sortedAsks[6].AllocationKey, "ask7")
	assert.Equal(t, sortedAsks[7].AllocationKey, "ask8")

	// assert driver/owner pods
	assert.Equal(t, sortedAsks[8].AllocationKey, "ask10")
	assert.Equal(t, sortedAsks[9].AllocationKey, "ask9")
}

func TestGetVictims(t *testing.T) {
	node := NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"first": {Value: 100}},
		},
	})
	p := NewSimplePreemptor(node)
	prepareAllocationAsks(p)
	p.sortAllocations()

	requiredAsk := createAllocationAsk("ask10", "app1", "true", true, 5,
		resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	requiredAsk.SetPendingAskRepeat(5)

	victims := p.GetVictims(requiredAsk)
	assert.Equal(t, len(victims), 4)
	assert.Equal(t, victims[0].AllocationKey, "ask5")
	assert.Equal(t, resources.Equals(victims[0].AllocatedResource, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})), true)
	assert.Equal(t, victims[1].AllocationKey, "ask1")
	assert.Equal(t, resources.Equals(victims[1].AllocatedResource, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})), true)
	assert.Equal(t, victims[2].AllocationKey, "ask4")
	assert.Equal(t, resources.Equals(victims[2].AllocatedResource, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})), true)
	assert.Equal(t, victims[3].AllocationKey, "ask2")
	assert.Equal(t, resources.Equals(victims[3].AllocatedResource, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8})), true)
}
