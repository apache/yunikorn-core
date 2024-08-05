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
	// no guaranteed and no usage. so no remaining
	rootQ, parentQ, childQ1, childQ2, childQ3 := setup(t)
	smallestRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	childRes := resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5})
	expectedSmallestRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 3})
	expectedSmallestRes1 := smallestRes.Clone()
	expectedSmallestRes1.MultiplyTo(float64(0))
	var tests = []struct {
		testName          string
		askQueue          *Queue
		childQ2Remaining  *resources.Resource
		childQ2Remaining1 *resources.Resource
	}{
		{"UnderGuaranteedChildQueue_Under_OverGuaranteedParentQueue_Does_Not_Have_Higher_Precedence_When_AskQueue_Is_Different_From_UnderGuaranteedChildQueue", childQ3,
			resources.Multiply(smallestRes, -1), resources.Add(expectedSmallestRes1, resources.Multiply(childRes, -1))},
		{"UnderGuaranteedChildQueue_Under_OverGuaranteedParentQueue_Has_Higher_Precedence_When_AskQueue_Is_Same_As_UnderGuaranteedChildQueue", childQ2,
			resources.Multiply(smallestRes, 0), resources.Add(expectedSmallestRes, resources.Multiply(childRes, -1))},
	}
	for _, tt := range tests {
		var rootRemaining, pRemaining, cRemaining1, cRemaining2 *resources.Resource
		resetQueueResources(rootQ, parentQ, childQ1, childQ2)
		rootRemaining, pRemaining, cRemaining1, cRemaining2 = setAndGetRemainingGuaranteed(rootQ, parentQ, childQ1, childQ2, map[string]*resources.Resource{rootQ.QueuePath: nil, parentQ.QueuePath: nil, childQ1.QueuePath: nil, childQ2.QueuePath: nil}, tt.askQueue)
		assertZeroRemaining(t, rootRemaining, pRemaining, cRemaining1, cRemaining2)

		// no guaranteed and no usage, but max res set. so min of guaranteed and max should be remaining
		smallestRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
		rootQ.maxResource = resources.Multiply(smallestRes, 4)
		parentQ.maxResource = resources.Multiply(smallestRes, 2)
		childQ1.maxResource = smallestRes
		childQ2.maxResource = smallestRes
		rootRemaining, pRemaining, cRemaining1, cRemaining2 = setAndGetRemainingGuaranteed(rootQ, parentQ, childQ1, childQ2, map[string]*resources.Resource{rootQ.QueuePath: nil, parentQ.QueuePath: nil, childQ1.QueuePath: nil, childQ2.QueuePath: nil}, tt.askQueue)
		assertZeroRemaining(t, rootRemaining, pRemaining, cRemaining1, cRemaining2)

		// guaranteed set only for queue at specific levels but no usage.
		// so remaining for queues without guaranteed quota inherits from parent queue based on min perm calculation
		rootRemaining, pRemaining, cRemaining1, cRemaining2 = setAndGetRemainingGuaranteed(rootQ, parentQ, childQ1, childQ2, map[string]*resources.Resource{rootQ.QueuePath: resources.Multiply(smallestRes, 2), parentQ.QueuePath: nil, childQ1.QueuePath: childRes, childQ2.QueuePath: nil}, tt.askQueue)
		assert.Assert(t, resources.Equals(rootRemaining, resources.Multiply(smallestRes, 2)), "guaranteed set, but no usage. so all guaranteed should be in remaining")
		assert.Assert(t, resources.Equals(pRemaining, resources.Multiply(smallestRes, 2)), "guaranteed not set, also no usage. However, parent's remaining should be used")
		assert.Assert(t, resources.Equals(cRemaining1, resources.Add(resources.Multiply(smallestRes, 2), childRes)), "guaranteed not set, also no usage. However, parent's remaining should be used")
		assert.Assert(t, resources.Equals(cRemaining2, resources.Multiply(smallestRes, 2)), "guaranteed not set, also no usage. However, parent's remaining should be used")

		// guaranteed set but no usage. so nothing to preempt
		// clean start for the snapshot: whole hierarchy with guarantee
		queueRes := map[string]*resources.Resource{rootQ.QueuePath: resources.Multiply(smallestRes, 2), parentQ.QueuePath: smallestRes, childQ1.QueuePath: childRes, childQ2.QueuePath: smallestRes}
		rootRemaining, pRemaining, cRemaining1, cRemaining2 = setAndGetRemainingGuaranteed(rootQ, parentQ, childQ1, childQ2, queueRes, tt.askQueue)
		assert.Assert(t, resources.Equals(rootRemaining, resources.Multiply(smallestRes, 2)), "guaranteed set, but no usage. so all guaranteed should be in remaining")
		assert.Assert(t, resources.Equals(pRemaining, smallestRes), "guaranteed set, but no usage. so all guaranteed should be in remaining")
		assert.Assert(t, resources.Equals(cRemaining1, resources.Add(smallestRes, childRes)), "guaranteed set, but no usage. so all guaranteed + parent remaining guaranteed should be in remaining")
		assert.Assert(t, resources.Equals(cRemaining2, smallestRes), "guaranteed set, but no usage. so all its guaranteed (because it is lesser than parent's guaranteed) should be in remaining")

		// clean start for the snapshot: all set guaranteed
		// add usage to parent + root: use all guaranteed at parent level
		// add usage to child2: use all guaranteed set
		// child2 remaining behaviour changes based on the ask queue.
		// When ask queue is child2, its own values has higher precedence over the parent or ancestor for common resource types.
		// for extra resources available in parent or ancestor, it can simply inherit.
		// When ask queue is child3 (diverged from very earlier branch, not sharing any common queue path), its remaining is min permissive of its own values and parent or ancestor values.
		rootQ.allocatedResource = resources.Multiply(smallestRes, 2)
		parentQ.allocatedResource = resources.Multiply(smallestRes, 2)
		childQ2.allocatedResource = resources.Multiply(smallestRes, 1)
		rootRemaining, pRemaining, cRemaining1, cRemaining2 = setAndGetRemainingGuaranteed(rootQ, parentQ, childQ1, childQ2, queueRes, tt.askQueue)
		assert.Assert(t, resources.IsZero(rootRemaining), "guaranteed set, used completely. so all guaranteed should be in remaining")
		assert.Assert(t, resources.Equals(pRemaining, resources.Multiply(smallestRes, -1)), "guaranteed set, used double than guaranteed. so remaining should be in -ve")
		assert.Assert(t, resources.Equals(cRemaining1, resources.Add(resources.Multiply(smallestRes, -1), childRes)), "guaranteed set, but no usage. However remaining should include its all guaranteed + parent remaining guaranteed")
		assert.Assert(t, resources.Equals(cRemaining2, tt.childQ2Remaining), "guaranteed set, used all guaranteed. remaining should be based on ask queue")

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
		rootRemaining, pRemaining, cRemaining1, cRemaining2 = setAndGetRemainingGuaranteed(rootQ, parentQ, childQ1, childQ2, queueRes, tt.askQueue)
		assert.Assert(t, resources.IsZero(rootRemaining), "guaranteed set, used completely. usage also has extra resource types. However, no remaining")
		assert.Assert(t, resources.IsZero(pRemaining), "guaranteed set, used completely. usage also has extra resource types. However, no remaining")
		assert.Assert(t, resources.IsZero(cRemaining1), "guaranteed set, used completely. so, no remaining")
		assert.Assert(t, resources.IsZero(cRemaining2), "guaranteed set, but no usage. Still, no remaining in guaranteed because of its parent queue")

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
		childQ2.allocatedResource = smallestRes
		rootRemaining, pRemaining, cRemaining1, cRemaining2 = setAndGetRemainingGuaranteed(rootQ, parentQ, childQ1, childQ2, queueRes, tt.askQueue)
		assert.Assert(t, resources.IsZero(rootRemaining), "guaranteed set, used completely. usage also has extra resource types. However, no remaining")
		assert.Assert(t, resources.IsZero(pRemaining), "guaranteed set, used completely. usage also has extra resource types. However, no remaining")
		assert.Assert(t, resources.Equals(cRemaining1, resources.Multiply(childRes, -1)), "guaranteed set, used double than guaranteed. so remaining should be in -ve")
		assert.Assert(t, resources.IsZero(cRemaining2), "guaranteed set, but no usage. Still, no remaining in guaranteed because of its parent queue")

		// clean start for the snapshot: all set guaranteed
		// add usage for root + parent: use exactly guaranteed for one resource and over guaranteed for another resource at parent level
		// add usage to child1: use double than guaranteed
		// add usage to child2: use lesser than guaranteed.
		// child2 remaining behaviour changes based on the ask queue.
		// When ask queue is child2, its own values has higher precedence over the parent or ancestor for common resource types.
		// for extra resources available in parent or ancestor, it can simply inherit.
		// When ask queue is child3 (diverged from very earlier branch, not sharing any common queue path), its remaining is min permissive of its own values and parent or ancestor values.
		childQ2.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2})
		rootRemaining, pRemaining, cRemaining1, cRemaining2 = setAndGetRemainingGuaranteed(rootQ, parentQ, childQ1, childQ2, map[string]*resources.Resource{rootQ.QueuePath: resources.Multiply(smallestRes, 2), parentQ.QueuePath: resources.Add(smallestRes, resources.Multiply(childRes, 1)), childQ1.QueuePath: childRes, childQ2.QueuePath: smallestRes}, tt.askQueue)
		assert.Assert(t, resources.IsZero(rootRemaining), "guaranteed set, used completely. usage also has extra resource types. However, no remaining")
		assert.Assert(t, resources.DeepEquals(pRemaining, resources.Add(expectedSmallestRes1, resources.Multiply(childRes, -1))), "guaranteed set, one resource type used completely. usage also has another resource types which is used bit more. remaining should have zero for one resource type and -ve for another")
		assert.Assert(t, resources.DeepEquals(cRemaining1, resources.Add(expectedSmallestRes1, resources.Multiply(childRes, -1))), "guaranteed set, used double than guaranteed. so remaining should be in -ve")
		assert.Assert(t, resources.DeepEquals(cRemaining2, tt.childQ2Remaining1), "guaranteed set, used bit lesser. parent's usage also has extra resource types. remaining should be based on ask queue")
	}
}

func setup(t *testing.T) (*Queue, *Queue, *Queue, *Queue, *Queue) {
	rootQ, err := createRootQueue(map[string]string{})
	assert.NilError(t, err)
	var parentQ, childQ1, childQ2, parent1Q, childQ3 *Queue
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
	return rootQ, parentQ, childQ1, childQ2, childQ3
}
func assertZeroRemaining(t *testing.T, rootRemaining *resources.Resource, pRemaining *resources.Resource, cRemaining1 *resources.Resource, cRemaining2 *resources.Resource) {
	assert.Assert(t, resources.IsZero(rootRemaining), "guaranteed and max res not set, so no remaining")
	assert.Assert(t, resources.IsZero(pRemaining), "guaranteed and max res not set, so no remaining")
	assert.Assert(t, resources.IsZero(cRemaining1), "guaranteed and max res not set, so no remaining")
	assert.Assert(t, resources.IsZero(cRemaining2), "guaranteed and max res not set, so no remaining")
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

func setAndGetRemainingGuaranteed(rootQ *Queue, parentQ *Queue, childQ1 *Queue, childQ2 *Queue, queueRes map[string]*resources.Resource, askQueue *Queue) (*resources.Resource, *resources.Resource, *resources.Resource, *resources.Resource) {
	rootQ.guaranteedResource = queueRes[rootQ.QueuePath]
	parentQ.guaranteedResource = queueRes[parentQ.QueuePath]
	childQ2.guaranteedResource = queueRes[childQ2.QueuePath]
	childQ1.guaranteedResource = queueRes[childQ1.QueuePath]
	qpsRoot, qpsParent, qpsChild1, qpsChild2 := createQPSCache(rootQ, parentQ, childQ1, childQ2, askQueue)
	rootRemaining := qpsRoot.GetRemainingGuaranteedResource()
	pRemaining := qpsParent.GetRemainingGuaranteedResource()
	cRemaining1 := qpsChild1.GetRemainingGuaranteedResource()
	cRemaining2 := qpsChild2.GetRemainingGuaranteedResource()
	return rootRemaining, pRemaining, cRemaining1, cRemaining2
}

func getPreemptableResource(rootQ *Queue, parentQ *Queue, childQ1 *Queue, childQ2 *Queue) (*resources.Resource, *resources.Resource, *resources.Resource, *resources.Resource) {
	qpsRoot, qpsParent, qpsChild1, qpsChild2 := createQPSCache(rootQ, parentQ, childQ1, childQ2, nil)
	rootRemaining := qpsRoot.GetPreemptableResource()
	pRemaining := qpsParent.GetPreemptableResource()
	cRemaining1 := qpsChild1.GetPreemptableResource()
	cRemaining2 := qpsChild2.GetPreemptableResource()
	return rootRemaining, pRemaining, cRemaining1, cRemaining2
}
