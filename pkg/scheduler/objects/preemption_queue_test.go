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
)

var smallestRes = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
var childRes = resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5})
var smallestResPlusChildRes = resources.Add(smallestRes, childRes)
var smallestResDouble = resources.Multiply(smallestRes, 2)
var smallestResDoublePlusChildRes = resources.Add(resources.Multiply(smallestRes, 2), childRes)
var smallestResMultiplyByZero = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0})
var smallestResMultiplyByMinusOne = resources.Multiply(smallestRes, -1)
var smallestResMultiplyByMinusOnePlusChildRes = resources.Add(resources.Multiply(smallestRes, -1), childRes)
var childResMultiplyByZero = resources.NewResourceFromMap(map[string]resources.Quantity{"second": 0})
var childResMultiplyByMinusOne = resources.Multiply(childRes, -1)
var childResDouble = resources.Multiply(childRes, 2)
var smallestResDoublePlusChildResDouble = resources.Add(resources.Multiply(smallestRes, 2), childResDouble)
var smallestResPlusChildResDouble = resources.Add(smallestRes, childResDouble)
var smallestResMultiplyByZeroPluschildResMultiplyByZero = resources.Add(smallestResMultiplyByZero, childResMultiplyByZero)
var smallestResMultiplyByZeroPluschildResMultiplyByMinusOne = resources.Add(smallestResMultiplyByZero, childResMultiplyByMinusOne)

type resource struct {
	root, parent, childQ1, childQ2 *resources.Resource
}

type assertMessage struct {
	rootR, parentR, childQ1R, childQ2R string
}

type res struct {
	guaranteed, allocated, remaining resource
	assertMessages                   assertMessage
}

func TestGetPreemptableResource(t *testing.T) {
	// no guaranteed and no usage. so nothing to preempt
	rootQ, err := createRootQueue(map[string]string{"first": "20"})
	assert.NilError(t, err)
	var parentQ, childQ1, childQ2 *Queue
	parentQ, err = createManagedQueue(rootQ, "parent", true, map[string]string{"first": "15"})
	assert.NilError(t, err)
	childQ1, err = createManagedQueue(parentQ, "child1", false, map[string]string{"first": "10"})
	assert.NilError(t, err)
	childQ2, err = createManagedQueue(parentQ, "child2", false, map[string]string{"first": "5"})
	assert.NilError(t, err)

	rootPreemptable, pPreemptable, cPreemptable1, cPreemptable2 := getPreemptableResource(rootQ, parentQ, childQ1, childQ2)
	assert.Assert(t, rootPreemptable.IsEmpty(), "nothing to preempt as no guaranteed set and no usage")
	assert.Assert(t, pPreemptable.IsEmpty(), "nothing to preempt as no guaranteed set and no usage")
	assert.Assert(t, cPreemptable1.IsEmpty(), "nothing to preempt as no guaranteed set and no usage")
	assert.Assert(t, cPreemptable2.IsEmpty(), "nothing to preempt as no guaranteed set and no usage")

	// guaranteed set but no usage. so nothing to preempt
	// clean start for the snapshot: whole hierarchy with guarantee
	smallestRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})

	// clean start for the snapshot: all set guaranteed - new tests
	// add usage to parent + root: use all guaranteed at parent level
	// add usage to child2: use double than guaranteed
	parentQ.guaranteedResource = smallestRes
	rootQ.allocatedResource = resources.Multiply(smallestRes, 2)
	parentQ.allocatedResource = resources.Multiply(smallestRes, 2)
	childQ2.allocatedResource = resources.Multiply(smallestRes, 2)
	rootPreemptable, pPreemptable, cPreemptable1, cPreemptable2 = getPreemptableResource(rootQ, parentQ, childQ1, childQ2)
	assert.Assert(t, resources.Equals(rootPreemptable, resources.Multiply(smallestRes, 2)), "usage is equal to guaranteed in root queue. so nothing to preempt")
	assert.Assert(t, resources.Equals(pPreemptable, smallestRes), "usage has exceeded twice than guaranteed in parent queue. preemtable resource should be equal to guaranteed res")
	assert.Assert(t, resources.IsZero(cPreemptable1), "nothing to preempt as no usage in child1 queue")
	assert.Assert(t, resources.Equals(cPreemptable2, smallestRes), "usage has exceeded twice than guaranteed in child2 queue. preemtable resource should be equal to guaranteed res")

	rootQ.guaranteedResource = resources.Multiply(smallestRes, 2)
	parentQ.guaranteedResource = smallestRes
	childQ2.guaranteedResource = smallestRes
	rootQ.allocatedResource = nil
	parentQ.allocatedResource = nil
	childQ2.allocatedResource = nil
	childRes := resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5})
	childQ1.guaranteedResource = childRes
	rootPreemptable, pPreemptable, cPreemptable1, cPreemptable2 = getPreemptableResource(rootQ, parentQ, childQ1, childQ2)
	assert.Assert(t, rootPreemptable.IsEmpty(), "nothing to preempt as no usage")
	assert.Assert(t, pPreemptable.IsEmpty(), "nothing to preempt as no usage")
	assert.Assert(t, cPreemptable1.IsEmpty(), "nothing to preempt as no usage")
	assert.Assert(t, cPreemptable2.IsEmpty(), "nothing to preempt as no usage")

	// clean start for the snapshot: all set guaranteed
	// add usage to parent + root: use all guaranteed at parent level
	// add usage to child2: use double than guaranteed
	rootQ.guaranteedResource = resources.Multiply(smallestRes, 2)
	parentQ.guaranteedResource = smallestRes
	childQ2.guaranteedResource = smallestRes
	rootQ.allocatedResource = resources.Multiply(smallestRes, 2)
	parentQ.allocatedResource = resources.Multiply(smallestRes, 2)
	childQ2.allocatedResource = resources.Multiply(smallestRes, 2)
	rootPreemptable, pPreemptable, cPreemptable1, cPreemptable2 = getPreemptableResource(rootQ, parentQ, childQ1, childQ2)
	assert.Assert(t, resources.IsZero(rootPreemptable), "usage is equal to guaranteed in root queue. so nothing to preempt")
	assert.Assert(t, resources.Equals(pPreemptable, smallestRes), "usage has exceeded twice than guaranteed in parent queue. preemtable resource should be equal to guaranteed res")
	assert.Assert(t, resources.IsZero(cPreemptable1), "nothing to preempt as no usage in child1 queue")
	assert.Assert(t, resources.Equals(cPreemptable2, smallestRes), "usage has exceeded twice than guaranteed in child2 queue. preemtable resource should be equal to guaranteed res")

	// clean start for the snapshot: all set guaranteed
	// add usage for all: use exactly guaranteed at parent and child level
	// parent guarantee used for one type child guarantee used for second type
	bothRes := resources.Multiply(smallestRes, 2)
	bothRes.AddTo(childRes)
	rootQ.allocatedResource = bothRes
	bothRes = resources.Add(smallestRes, childRes)
	parentQ.allocatedResource = bothRes
	childQ1.allocatedResource = childRes
	childQ2.allocatedResource = smallestRes
	rootPreemptable, pPreemptable, cPreemptable1, cPreemptable2 = getPreemptableResource(rootQ, parentQ, childQ1, childQ2)
	assert.Assert(t, resources.Equals(rootPreemptable, resources.Multiply(childRes, 1)), "usage in root queue has extra resource type not defined as part of guaranteed res. So, that extra resource type should be preempted")
	assert.Assert(t, resources.Equals(pPreemptable, resources.Multiply(childRes, 1)), "usage in parent1 queue has extra resource type not defined as part of guaranteed res. So, that extra resource type should be preempted")
	assert.Assert(t, resources.IsZero(cPreemptable1), "usage in child1 queue is equal to guaranteed res. so nothing to preempt")
	assert.Assert(t, resources.IsZero(cPreemptable2), "nothing to preempt as no usage in child2 queue")

	// clean start for the snapshot: all set guaranteed
	// add usage for root + parent: use exactly guaranteed at parent and child level
	// add usage to child1: use double than guaranteed
	// parent guarantee used for one type child guarantee used for second type
	bothRes = resources.Multiply(smallestRes, 2)
	bothRes.AddTo(resources.Multiply(childRes, 2))
	rootQ.allocatedResource = bothRes
	bothRes = resources.Add(smallestRes, resources.Multiply(childRes, 2))
	parentQ.allocatedResource = bothRes
	childQ1.allocatedResource = resources.Multiply(childRes, 2)
	rootPreemptable, pPreemptable, cPreemptable1, cPreemptable2 = getPreemptableResource(rootQ, parentQ, childQ1, childQ2)
	assert.Assert(t, resources.Equals(rootPreemptable, resources.Multiply(childRes, 2)), "usage in root queue has extra resource type not defined as part of guaranteed res. So, that extra resource type should be preempted")
	assert.Assert(t, resources.Equals(pPreemptable, resources.Multiply(childRes, 2)), "usage in parent1 queue has extra resource type not defined as part of guaranteed res. So, that extra resource type should be preempted")
	assert.Assert(t, resources.Equals(cPreemptable1, childRes), "usage has exceeded twice than guaranteed in child1 queue. cPreemptable1 resource should be equal to guaranteed res")
	assert.Assert(t, resources.IsZero(cPreemptable2), "nothing to preempt as no usage in child2 queue")
}

func TestGetRemainingGuaranteedResource(t *testing.T) {
	rootQ, parentQ, childQ1, childQ2, childQ3 := setup(t)
	guaranteed1 := resource{resources.Multiply(smallestRes, 2), smallestRes, childRes, smallestRes}
	guaranteed2 := resource{resources.Multiply(smallestRes, 2), smallestResPlusChildRes, childRes, smallestRes}
	allocated1 := resource{nil, nil, nil, nil}
	allocated2 := resource{smallestResDouble, smallestResDouble, nil, smallestRes}
	allocated3 := resource{smallestResDoublePlusChildRes, smallestResPlusChildRes, childRes, smallestRes}
	allocated4 := resource{smallestResDoublePlusChildResDouble, smallestResPlusChildResDouble, childResDouble, smallestRes}
	allocated5 := resource{smallestResDoublePlusChildResDouble, smallestResPlusChildResDouble, childResDouble, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2})}
	assertMessage1 := "guaranteed set, but no usage. so all guaranteed should be in remaining"
	assertMessage2 := "guaranteed set, but no usage. so all guaranteed + parent remaining guaranteed should be in remaining"
	assertMessage3 := "guaranteed set, but no usage. so all its guaranteed (because it is lesser than parent's guaranteed) should be in remaining"
	assertMessage4 := "guaranteed set, used completely. so all guaranteed should be in remaining"
	assertMessage5 := "guaranteed set, used double than guaranteed. so remaining should be in -ve"
	assertMessage6 := "guaranteed set, but no usage. However remaining should include its all guaranteed + parent remaining guaranteed"
	assertMessage7 := "guaranteed set, used all guaranteed. remaining should be based on ask queue"
	assertMessage8 := "guaranteed set, used completely. usage also has extra resource types. However, no remaining"
	assertMessage9 := "guaranteed set, used completely. so, no remaining"
	assertMessage10 := "guaranteed set, but no usage. Still, no remaining in guaranteed because of its parent queue"
	assertMessage11 := "guaranteed set, one resource type used completely. usage also has another resource types which is used bit more. remaining should have zero for one resource type and -ve for another"
	assertMessage12 := "guaranteed set, used bit lesser. parent's usage also has extra resource types. remaining should be based on ask queue"

	assertMessageStruct1 := assertMessage{assertMessage1, assertMessage1, assertMessage1, assertMessage1}
	assertMessageStruct2 := assertMessage{assertMessage1, assertMessage1, assertMessage2, assertMessage3}
	assertMessageStruct3 := assertMessage{assertMessage4, assertMessage5, assertMessage6, assertMessage7}
	assertMessageStruct4 := assertMessage{assertMessage8, assertMessage8, assertMessage9, assertMessage10}
	assertMessageStruct5 := assertMessage{assertMessage8, assertMessage8, assertMessage5, assertMessage10}
	assertMessageStruct6 := assertMessage{assertMessage8, assertMessage11, assertMessage5, assertMessage12}

	// guaranteed set only for queue at specific levels but no usage.
	// so remaining for queues without guaranteed quota inherits from parent queue based on min perm calculation
	res1 := res{guaranteed: resource{resources.Multiply(smallestRes, 2), nil, childRes, nil},
		allocated:      allocated1,
		remaining:      resource{smallestResDouble, smallestResDouble, smallestResDoublePlusChildRes, smallestResDouble},
		assertMessages: assertMessageStruct1,
	}
	var tests = []struct {
		testName  string
		askQueue  *Queue
		resources []res
	}{
		{testName: "UnderGuaranteedChildQueue_Under_OverGuaranteedParentQueue_Does_Not_Have_Higher_Precedence_When_AskQueue_Is_Different_From_UnderGuaranteedChildQueue",
			// ask queue diverged from root itself, not sharing any common queue path with potential victim queue paths
			askQueue: childQ3,
			resources: []res{
				res1,
				// guaranteed set but no usage. so nothing to preempt
				// clean start for the snapshot: whole hierarchy with guarantee
				{guaranteed: guaranteed1, allocated: allocated1, remaining: resource{smallestResDouble, smallestRes, smallestResPlusChildRes, smallestRes}, assertMessages: assertMessageStruct2},
				// clean start for the snapshot: all set guaranteed
				// add usage to parent + root + child2 : use all guaranteed set
				// child2 remaining behaviour changes based on the ask queue. Its remaining is min permissive of its own values and parent or ancestor values.
				{guaranteed: guaranteed1, allocated: allocated2, remaining: resource{smallestResMultiplyByZero, smallestResMultiplyByMinusOne, smallestResMultiplyByMinusOnePlusChildRes, smallestResMultiplyByMinusOne}, assertMessages: assertMessageStruct3},
				// clean start for the snapshot: all set guaranteed
				// add usage for all: use exactly guaranteed at parent and child1 level
				// parent guarantee used for one type child guarantee used for second type
				{guaranteed: guaranteed1, allocated: allocated3, remaining: resource{smallestResMultiplyByZero, smallestResMultiplyByZero, smallestResMultiplyByZeroPluschildResMultiplyByZero, smallestResMultiplyByZero}, assertMessages: assertMessageStruct4},
				// clean start for the snapshot: all set guaranteed
				// add usage for root + parent: use exactly guaranteed at parent and child2 level
				// add usage to child1: use double than guaranteed
				// parent guarantee used for one type child guarantee used for second type
				{guaranteed: guaranteed1, allocated: allocated4, remaining: resource{smallestResMultiplyByZero, smallestResMultiplyByZero, smallestResMultiplyByZeroPluschildResMultiplyByMinusOne, smallestResMultiplyByZero}, assertMessages: assertMessageStruct5},
				// clean start for the snapshot: all set guaranteed
				// add usage for root + parent: use exactly guaranteed for one resource and over guaranteed for another resource at parent level
				// add usage to child1: use double than guaranteed
				// add usage to child2: use lesser than guaranteed.
				// child2 remaining behaviour changes based on the ask queue. Its remaining is min permissive of its own values and parent or ancestor values.
				{guaranteed: guaranteed2, allocated: allocated5, remaining: resource{smallestResMultiplyByZero, smallestResMultiplyByZeroPluschildResMultiplyByMinusOne, smallestResMultiplyByZeroPluschildResMultiplyByMinusOne, smallestResMultiplyByZeroPluschildResMultiplyByMinusOne}, assertMessages: assertMessageStruct6},
			},
		},
		{testName: "UnderGuaranteedChildQueue_Under_OverGuaranteedParentQueue_Has_Higher_Precedence_When_AskQueue_Is_Same_As_UnderGuaranteedChildQueue",
			// ask queue shares common queue path with potential victim queue paths
			askQueue: childQ2,
			resources: []res{
				res1,
				// guaranteed set but no usage. so nothing to preempt
				// clean start for the snapshot: whole hierarchy with guarantee
				{guaranteed: guaranteed1, allocated: allocated1, remaining: resource{smallestResDouble, nil, childRes, smallestRes}, assertMessages: assertMessageStruct2},
				// clean start for the snapshot: all set guaranteed
				// add usage to parent + root + child2 : use all guaranteed set
				// child2 remaining behaviour changes based on the ask queue. When ask queue is child2, its own values has higher precedence over the parent or ancestor for common resource types.
				// for extra resources available in parent or ancestor, it can simply inherit.
				{guaranteed: guaranteed1, allocated: allocated2, remaining: resource{smallestResMultiplyByZero, nil, childRes, smallestResMultiplyByZero}, assertMessages: assertMessageStruct3},
				// clean start for the snapshot: all set guaranteed
				// add usage for all: use exactly guaranteed at parent and child1 level
				// parent guarantee used for one type child guarantee used for second type
				{guaranteed: guaranteed1, allocated: allocated3, remaining: resource{smallestResMultiplyByZero, nil, childResMultiplyByZero, smallestResMultiplyByZero}, assertMessages: assertMessageStruct4},
				// clean start for the snapshot: all set guaranteed
				// add usage for root + parent: use exactly guaranteed at parent and child2 level
				// add usage to child1: use double than guaranteed
				// parent guarantee used for one type child guarantee used for second type
				{guaranteed: guaranteed1, allocated: allocated4, remaining: resource{smallestResMultiplyByZero, nil, childResMultiplyByMinusOne, smallestResMultiplyByZero}, assertMessages: assertMessageStruct5},
				// clean start for the snapshot: all set guaranteed
				// add usage for root + parent: use exactly guaranteed for one resource and over guaranteed for another resource at parent level
				// add usage to child1: use double than guaranteed
				// add usage to child2: use lesser than guaranteed.
				// child2 remaining behaviour changes based on the ask queue. Its own values has higher precedence over the parent or ancestor for common resource types.
				// for extra resources available in parent or ancestor, it can simply inherit.
				{guaranteed: guaranteed2, allocated: allocated5, remaining: resource{smallestResMultiplyByZero, nil, childResMultiplyByMinusOne, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 3})}, assertMessages: assertMessageStruct6},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			for _, res1 := range tt.resources {
				assertRemaining(t, rootQ, parentQ, childQ2, childQ1, tt.askQueue, res1)
			}
		})
	}
}

func setup(t *testing.T) (rootQ, parentQ, childQ1, childQ2, childQ3 *Queue) {
	rootQ, err := createRootQueue(map[string]string{})
	assert.NilError(t, err)
	var parent1Q *Queue
	parentQ, err = createManagedQueue(rootQ, "parent", true, map[string]string{})
	assert.NilError(t, err)
	parent1Q, err = createManagedQueue(rootQ, "parent1", true, map[string]string{})
	assert.NilError(t, err)
	childQ1, err = createManagedQueue(parentQ, "child1", false, map[string]string{})
	assert.NilError(t, err)
	childQ2, err = createManagedQueue(parentQ, "child2", false, map[string]string{})
	assert.NilError(t, err)
	childQ3, err = createManagedQueue(parent1Q, "child3", false, map[string]string{})
	assert.NilError(t, err)
	return
}

func assertRemaining(t *testing.T, rootQ *Queue, parentQ *Queue, childQ2 *Queue, childQ1 *Queue, askQueue *Queue, res1 res) {
	var rootRemaining, pRemaining, cRemaining1, cRemaining2 *resources.Resource
	resetQueueResources(rootQ, parentQ, childQ1, childQ2)
	rootQ.guaranteedResource = res1.guaranteed.root
	parentQ.guaranteedResource = res1.guaranteed.parent
	childQ1.guaranteedResource = res1.guaranteed.childQ1
	childQ2.guaranteedResource = res1.guaranteed.childQ2
	rootQ.allocatedResource = res1.allocated.root
	parentQ.allocatedResource = res1.allocated.parent
	childQ1.allocatedResource = res1.allocated.childQ1
	childQ2.allocatedResource = res1.allocated.childQ2
	qpsRoot, qpsParent, qpsChild1, qpsChild2 := createQPSCache(rootQ, parentQ, childQ1, childQ2, askQueue)
	rootRemaining = qpsRoot.GetRemainingGuaranteedResource()
	pRemaining = qpsParent.GetRemainingGuaranteedResource()
	cRemaining1 = qpsChild1.GetRemainingGuaranteedResource()
	cRemaining2 = qpsChild2.GetRemainingGuaranteedResource()
	assert.Assert(t, resources.DeepEquals(rootRemaining, res1.remaining.root), res1.assertMessages.rootR)
	assert.Assert(t, resources.DeepEquals(pRemaining, res1.remaining.parent), res1.assertMessages.parentR)
	assert.Assert(t, resources.DeepEquals(cRemaining1, res1.remaining.childQ1), res1.assertMessages.childQ1R)
	assert.Assert(t, resources.DeepEquals(cRemaining2, res1.remaining.childQ2), res1.assertMessages.childQ2R)
}

func resetQueueResources(rootQ *Queue, parentQ *Queue, childQ1 *Queue, childQ2 *Queue) {
	rootQ.guaranteedResource = nil
	parentQ.guaranteedResource = nil
	childQ1.guaranteedResource = nil
	childQ2.guaranteedResource = nil
	rootQ.allocatedResource = nil
	parentQ.allocatedResource = nil
	childQ1.allocatedResource = nil
	childQ2.allocatedResource = nil
	rootQ.preemptingResource = nil
	parentQ.preemptingResource = nil
	childQ1.preemptingResource = nil
	childQ2.preemptingResource = nil
}

func createQPSCache(rootQ *Queue, parentQ *Queue, childQ1 *Queue, childQ2 *Queue, askQueue *Queue) (*QueuePreemptionSnapshot, *QueuePreemptionSnapshot, *QueuePreemptionSnapshot, *QueuePreemptionSnapshot) {
	cache := make(map[string]*QueuePreemptionSnapshot)
	askQueuePath := ""
	if askQueue != nil {
		askQueuePath = askQueue.QueuePath
		askQueue.createPreemptionSnapshot(cache, askQueue.QueuePath)
		c := askQueue
		// set the ask queue for all queues in the ask queue hierarchy
		for c.parent != nil {
			cache[c.QueuePath].AskQueue = cache[askQueue.QueuePath]
			c = c.parent
		}
	}
	qpsRoot := rootQ.createPreemptionSnapshot(cache, askQueuePath)
	qpsParent := parentQ.createPreemptionSnapshot(cache, askQueuePath)
	qpsChild1 := childQ1.createPreemptionSnapshot(cache, askQueuePath)
	qpsChild2 := childQ2.createPreemptionSnapshot(cache, askQueuePath)
	return qpsRoot, qpsParent, qpsChild1, qpsChild2
}

func getPreemptableResource(rootQ *Queue, parentQ *Queue, childQ1 *Queue, childQ2 *Queue) (*resources.Resource, *resources.Resource, *resources.Resource, *resources.Resource) {
	qpsRoot, qpsParent, qpsChild1, qpsChild2 := createQPSCache(rootQ, parentQ, childQ1, childQ2, nil)
	rootRemaining := qpsRoot.GetPreemptableResource()
	pRemaining := qpsParent.GetPreemptableResource()
	cRemaining1 := qpsChild1.GetPreemptableResource()
	cRemaining2 := qpsChild2.GetPreemptableResource()
	return rootRemaining, pRemaining, cRemaining1, cRemaining2
}
