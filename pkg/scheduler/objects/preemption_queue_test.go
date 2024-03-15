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
	var parentQ, childQ, childQ2 *Queue
	parentQ, err = createManagedQueue(rootQ, "parent", true, map[string]string{"first": "15"})
	assert.NilError(t, err)
	childQ, err = createManagedQueue(parentQ, "child1", false, map[string]string{"first": "10"})
	assert.NilError(t, err)
	childQ2, err = createManagedQueue(parentQ, "child2", false, map[string]string{"first": "5"})
	assert.NilError(t, err)
	cache := make(map[string]*QueuePreemptionSnapshot)
	qpsRoot := rootQ.createPreemptionSnapshot(cache)
	rootPreemptable := qpsRoot.GetPreemptableResource()
	qpsParent := parentQ.createPreemptionSnapshot(cache)
	pPreemptable := qpsParent.GetPreemptableResource()
	qpsChild1 := childQ.createPreemptionSnapshot(cache)
	preemptable := qpsChild1.GetPreemptableResource()
	qpsChild2 := childQ2.createPreemptionSnapshot(cache)
	preemptable2 := qpsChild2.GetPreemptableResource()
	assert.Equal(t, len(cache), 4, "expected 4 entries in snapsht cache")
	assert.Assert(t, rootPreemptable.IsEmpty(), "nothing to preempt as no guaranteed set and no usage")
	assert.Assert(t, pPreemptable.IsEmpty(), "nothing to preempt as no guaranteed set and no usage")
	assert.Assert(t, preemptable.IsEmpty(), "nothing to preempt as no guaranteed set and no usage")
	assert.Assert(t, preemptable2.IsEmpty(), "nothing to preempt as no guaranteed set and no usage")

	// guaranteed set but no usage. so nothing to preempt
	// clean start for the snapshot: whole hierarchy with guarantee
	smallestRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	rootQ.guaranteedResource = resources.Multiply(smallestRes, 2)
	parentQ.guaranteedResource = smallestRes
	childQ2.guaranteedResource = smallestRes
	childRes := resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5})
	childQ.guaranteedResource = childRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsRoot = rootQ.createPreemptionSnapshot(cache)
	rootPreemptable = qpsRoot.GetPreemptableResource()
	qpsParent = parentQ.createPreemptionSnapshot(cache)
	pPreemptable = qpsParent.GetPreemptableResource()
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	bothRes := resources.Add(smallestRes, childRes)
	preemptable = qpsChild1.GetPreemptableResource()
	qpsChild2 = childQ2.createPreemptionSnapshot(cache)
	preemptable2 = qpsChild2.GetPreemptableResource()
	assert.Assert(t, rootPreemptable.IsEmpty(), "nothing to preempt as no usage")
	assert.Assert(t, pPreemptable.IsEmpty(), "nothing to preempt as no usage")
	assert.Assert(t, preemptable.IsEmpty(), "nothing to preempt as no usage")
	assert.Assert(t, preemptable2.IsEmpty(), "nothing to preempt as no usage")

	// clean start for the snapshot: all set guaranteed
	// add usage to parent + root: use all guaranteed at parent level
	// add usage to child2: use double than guaranteed
	rootQ.allocatedResource = resources.Multiply(smallestRes, 2)
	parentQ.allocatedResource = resources.Multiply(smallestRes, 2)
	childQ2.allocatedResource = resources.Multiply(smallestRes, 2)
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsRoot = rootQ.createPreemptionSnapshot(cache)
	rootPreemptable = qpsRoot.GetPreemptableResource()
	qpsParent = parentQ.createPreemptionSnapshot(cache)
	pPreemptable = qpsParent.GetPreemptableResource()
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	preemptable = qpsChild1.GetPreemptableResource()
	qpsChild2 = childQ2.createPreemptionSnapshot(cache)
	preemptable2 = qpsChild2.GetPreemptableResource()
	assert.Assert(t, resources.IsZero(rootPreemptable), "usage is equal to guaranteed in root queue. so nothing to preempt")
	assert.Assert(t, resources.Equals(pPreemptable, smallestRes), "usage has exceeded twice than guaranteed in parent queue. preemtable resource should be equal to guaranteed res")
	assert.Assert(t, resources.IsZero(preemptable), "nothing to preempt as no usage in child1 queue")
	assert.Assert(t, resources.Equals(preemptable2, smallestRes), "usage has exceeded twice than guaranteed in child2 queue. preemtable resource should be equal to guaranteed res")

	// clean start for the snapshot: all set guaranteed
	// add usage for all: use exactly guaranteed at parent and child level
	// parent guarantee used for one type child guarantee used for second type
	bothRes = resources.Multiply(smallestRes, 2)
	bothRes.AddTo(childRes)
	rootQ.allocatedResource = bothRes
	bothRes = resources.Add(smallestRes, childRes)
	parentQ.allocatedResource = bothRes
	childQ.allocatedResource = childRes
	childQ2.allocatedResource = smallestRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsRoot = rootQ.createPreemptionSnapshot(cache)
	rootPreemptable = qpsRoot.GetPreemptableResource()
	qpsParent = parentQ.createPreemptionSnapshot(cache)
	pPreemptable = qpsParent.GetPreemptableResource()
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	preemptable = qpsChild1.GetPreemptableResource()
	qpsChild2 = childQ2.createPreemptionSnapshot(cache)
	preemptable2 = qpsChild2.GetPreemptableResource()
	assert.Assert(t, resources.Equals(rootPreemptable, resources.Multiply(childRes, 1)), "usage in root queue has extra resource type not defined as part of guaranteed res. So, that extra resource type should be preempted")
	assert.Assert(t, resources.Equals(pPreemptable, resources.Multiply(childRes, 1)), "usage in parent1 queue has extra resource type not defined as part of guaranteed res. So, that extra resource type should be preempted")
	assert.Assert(t, resources.IsZero(preemptable), "usage in child1 queue is equal to guaranteed res. so nothing to preempt")
	assert.Assert(t, resources.IsZero(preemptable2), "nothing to preempt as no usage in child2 queue")

	// clean start for the snapshot: all set guaranteed
	// add usage for root + parent: use exactly guaranteed at parent and child level
	// add usage to child1: use double than guaranteed
	// parent guarantee used for one type child guarantee used for second type
	bothRes = resources.Multiply(smallestRes, 2)
	bothRes.AddTo(resources.Multiply(childRes, 2))
	rootQ.allocatedResource = bothRes
	bothRes = resources.Add(smallestRes, resources.Multiply(childRes, 2))
	parentQ.allocatedResource = bothRes
	childQ.allocatedResource = resources.Multiply(childRes, 2)
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsRoot = rootQ.createPreemptionSnapshot(cache)
	rootPreemptable = qpsRoot.GetPreemptableResource()
	qpsParent = parentQ.createPreemptionSnapshot(cache)
	pPreemptable = qpsParent.GetPreemptableResource()
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	preemptable = qpsChild1.GetPreemptableResource()
	qpsChild2 = childQ2.createPreemptionSnapshot(cache)
	preemptable2 = qpsChild2.GetPreemptableResource()
	assert.Assert(t, resources.Equals(rootPreemptable, resources.Multiply(childRes, 2)), "usage in root queue has extra resource type not defined as part of guaranteed res. So, that extra resource type should be preempted")
	assert.Assert(t, resources.Equals(pPreemptable, resources.Multiply(childRes, 2)), "usage in parent1 queue has extra resource type not defined as part of guaranteed res. So, that extra resource type should be preempted")
	assert.Assert(t, resources.Equals(preemptable, childRes), "usage has exceeded twice than guaranteed in child1 queue. preemptable resource should be equal to guaranteed res")
	assert.Assert(t, resources.IsZero(preemptable2), "nothing to preempt as no usage in child2 queue")
}

func TestGetRemainingGuaranteed3(t *testing.T) {

	// no guaranteed and no usage. so nothing to preempt
	rootQ, err := createRootQueue(map[string]string{})
	assert.NilError(t, err)
	var parentQ, childQ, childQ2 *Queue
	parentQ, err = createManagedQueueGuaranteed(rootQ, "parent", true, nil, map[string]string{"first": "10"})
	assert.NilError(t, err)
	childQ, err = createManagedQueueGuaranteed(parentQ, "child1", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ2, err = createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)
	cache := make(map[string]*QueuePreemptionSnapshot)
	smallestRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})

	// clean start for the snapshot: all set guaranteed
	// add usage to parent + root: use all guaranteed at parent level
	// add usage to child2: use double than guaranteed
	rootQ.allocatedResource = resources.Multiply(smallestRes, 2)
	parentQ.allocatedResource = resources.Multiply(smallestRes, 2)
	childQ.allocatedResource = resources.Multiply(smallestRes, 2)
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsRoot := rootQ.createPreemptionSnapshot(cache)
	rootRemaining := qpsRoot.GetRemainingGuaranteed()
	qpsParent := parentQ.createPreemptionSnapshot(cache)
	pRemaining := qpsParent.GetRemainingGuaranteed()
	qpsChild1 := childQ.createPreemptionSnapshot(cache)
	remaining := qpsChild1.GetRemainingGuaranteed()
	qpsChild2 := childQ2.createPreemptionSnapshot(cache)
	remaining2 := qpsChild2.GetRemainingGuaranteed()

	assert.Assert(t, resources.IsZero(rootRemaining), "guaranteed set, used completely. so all guaranteed should be in remaining")
	assert.Assert(t, resources.IsZero(pRemaining), "guaranteed set, used double than guaranteed. so remaining should be in -ve")
	assert.Assert(t, resources.Equals(remaining, resources.Multiply(smallestRes, -1)), "guaranteed set, but no usage. However remaining should include its all guaranteed + parent remaining guaranteed")
	assert.Assert(t, resources.IsZero(remaining2), "guaranteed set, used double than guaranteed. so remaining should be in -ve")
}

func TestGetRemainingGuaranteed1(t *testing.T) {
	// no guaranteed and no usage. so nothing to preempt
	rootQ, err := createRootQueue(nil)
	assert.NilError(t, err)
	var parentQ, childQ, childQ2 *Queue
	parentQ, err = createManagedQueue(rootQ, "parent", true, nil)
	assert.NilError(t, err)
	childQ, err = createManagedQueue(parentQ, "child1", false, nil)
	assert.NilError(t, err)
	childQ2, err = createManagedQueue(parentQ, "child2", false, nil)
	assert.NilError(t, err)
	cache := make(map[string]*QueuePreemptionSnapshot)
	qpsRoot := rootQ.createPreemptionSnapshot(cache)
	rootRemaining := qpsRoot.GetRemainingGuaranteed()
	qpsParent := parentQ.createPreemptionSnapshot(cache)
	pRemaining := qpsParent.GetRemainingGuaranteed()
	qpsChild1 := childQ.createPreemptionSnapshot(cache)
	remaining := qpsChild1.GetRemainingGuaranteed()
	qpsChild2 := childQ2.createPreemptionSnapshot(cache)
	remaining2 := qpsChild2.GetRemainingGuaranteed()
	assert.Equal(t, len(cache), 4, "expected 4 entries in snapshot cache")
	assert.Assert(t, rootRemaining.IsEmpty(), "guaranteed not set, so nothing should be in remaining")
	assert.Assert(t, pRemaining.IsEmpty(), "guaranteed not set, so nothing should be in remaining")
	assert.Assert(t, remaining.IsEmpty(), "guaranteed not set, so nothing should be in remaininge")
	assert.Assert(t, remaining2.IsEmpty(), "guaranteed not set, so nothing should be in remaining")

	// guaranteed set but no usage. so nothing to preempt
	// clean start for the snapshot: whole hierarchy with guarantee
	smallestRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	rootQ.guaranteedResource = resources.Multiply(smallestRes, 2)
	parentQ.guaranteedResource = smallestRes
	childQ2.guaranteedResource = smallestRes
	childRes := resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5})
	bothRes := resources.Add(smallestRes, childRes)
	childQ.guaranteedResource = childRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsRoot = rootQ.createPreemptionSnapshot(cache)
	rootRemaining = qpsRoot.GetRemainingGuaranteed()
	qpsParent = parentQ.createPreemptionSnapshot(cache)
	pRemaining = qpsParent.GetRemainingGuaranteed()
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	remaining = qpsChild1.GetRemainingGuaranteed()
	qpsChild2 = childQ2.createPreemptionSnapshot(cache)
	remaining2 = qpsChild2.GetRemainingGuaranteed()
	assert.Assert(t, resources.Equals(rootRemaining, resources.Multiply(smallestRes, 2)), "guaranteed set, but no usage. so all guaranteed should be in remaining")
	assert.Assert(t, resources.Equals(pRemaining, smallestRes), "guaranteed set, but no usage. so all guaranteed should be in remaining")
	assert.Assert(t, resources.Equals(remaining, resources.Add(smallestRes, childRes)), "guaranteed set, but no usage. so all guaranteed + parent remaining guaranteed should be in remaining")
	assert.Assert(t, resources.Equals(remaining2, smallestRes), "guaranteed set, but no usage. so all guaranteed should be in remaining")

	// clean start for the snapshot: all set guaranteed
	// add usage to parent + root: use all guaranteed at parent level
	// add usage to child2: use double than guaranteed
	rootQ.allocatedResource = resources.Multiply(smallestRes, 2)
	parentQ.allocatedResource = resources.Multiply(smallestRes, 2)
	childQ2.allocatedResource = resources.Multiply(smallestRes, 2)
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsRoot = rootQ.createPreemptionSnapshot(cache)
	rootRemaining = qpsRoot.GetRemainingGuaranteed()
	qpsParent = parentQ.createPreemptionSnapshot(cache)
	pRemaining = qpsParent.GetRemainingGuaranteed()
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	remaining = qpsChild1.GetRemainingGuaranteed()
	qpsChild2 = childQ2.createPreemptionSnapshot(cache)
	remaining2 = qpsChild2.GetRemainingGuaranteed()
	assert.Assert(t, resources.IsZero(rootRemaining), "guaranteed set, used completely. so all guaranteed should be in remaining")
	assert.Assert(t, resources.Equals(pRemaining, resources.Multiply(smallestRes, -1)), "guaranteed set, used double than guaranteed. so remaining should be in -ve")
	assert.Assert(t, resources.Equals(remaining, resources.Add(resources.Multiply(smallestRes, -1), childRes)), "guaranteed set, but no usage. However remaining should include its all guaranteed + parent remaining guaranteed")
	assert.Assert(t, resources.Equals(remaining2, resources.Multiply(smallestRes, -1)), "guaranteed set, used double than guaranteed. so remaining should be in -ve")

	// clean start for the snapshot: all set guaranteed
	// add usage for all: use exactly guaranteed at parent and child level
	// parent guarantee used for one type child guarantee used for second type
	bothRes = resources.Multiply(smallestRes, 2)
	bothRes.AddTo(childRes)
	rootQ.allocatedResource = bothRes
	bothRes = resources.Add(smallestRes, childRes)
	parentQ.allocatedResource = bothRes
	childQ.allocatedResource = childRes
	childQ2.allocatedResource = smallestRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsRoot = rootQ.createPreemptionSnapshot(cache)
	rootRemaining = qpsRoot.GetRemainingGuaranteed()
	qpsParent = parentQ.createPreemptionSnapshot(cache)
	pRemaining = qpsParent.GetRemainingGuaranteed()
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	remaining = qpsChild1.GetRemainingGuaranteed()
	qpsChild2 = childQ2.createPreemptionSnapshot(cache)
	remaining2 = qpsChild2.GetRemainingGuaranteed()
	assert.Assert(t, resources.IsZero(rootRemaining), "guaranteed set, used completely. usage also has extra resource types. However, no remaining")
	assert.Assert(t, resources.IsZero(pRemaining), "guaranteed set, used completely. usage also has extra resource types. However, no remaining")
	assert.Assert(t, resources.IsZero(remaining), "guaranteed set, used completely. so, no remaining")
	assert.Assert(t, resources.IsZero(remaining2), "guaranteed set, but no usage. Still, no remaining in guaranteed because of its parent queue")

	// clean start for the snapshot: all set guaranteed
	// add usage for root + parent: use exactly guaranteed at parent and child level
	// add usage to child1: use double than guaranteed
	// parent guarantee used for one type child guarantee used for second type
	bothRes = resources.Multiply(smallestRes, 2)
	bothRes.AddTo(resources.Multiply(childRes, 2))
	rootQ.allocatedResource = bothRes
	bothRes = resources.Add(smallestRes, resources.Multiply(childRes, 2))
	parentQ.allocatedResource = bothRes
	childQ.allocatedResource = resources.Multiply(childRes, 2)
	childQ2.allocatedResource = smallestRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsRoot = rootQ.createPreemptionSnapshot(cache)
	rootRemaining = qpsRoot.GetRemainingGuaranteed()
	qpsParent = parentQ.createPreemptionSnapshot(cache)
	pRemaining = qpsParent.GetRemainingGuaranteed()
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	remaining = qpsChild1.GetRemainingGuaranteed()
	qpsChild2 = childQ2.createPreemptionSnapshot(cache)
	remaining2 = qpsChild2.GetRemainingGuaranteed()
	assert.Assert(t, resources.IsZero(rootRemaining), "guaranteed set, used completely. usage also has extra resource types. However, no remaining")
	assert.Assert(t, resources.IsZero(pRemaining), "guaranteed set, used completely. usage also has extra resource types. However, no remaining")
	assert.Assert(t, resources.Equals(remaining, resources.Multiply(childRes, -1)), "guaranteed set, used double than guaranteed. so remaining should be in -ve")
	assert.Assert(t, resources.IsZero(remaining2), "guaranteed set, but no usage. Still, no remaining in guaranteed because of its parent queue")
}
