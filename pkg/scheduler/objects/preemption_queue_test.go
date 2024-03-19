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
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"testing"

	"gotest.tools/v3/assert"
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
	rootQ.guaranteedResource = resources.Multiply(smallestRes, 2)
	parentQ.guaranteedResource = smallestRes
	childQ2.guaranteedResource = smallestRes
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
	// no guaranteed and no usage. so nothing to preempt
	//maxRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	rootQ, err := createRootQueue(map[string]string{"first": "20"})
	assert.NilError(t, err)
	var parentQ, childQ1, childQ2 *Queue
	parentQ, err = createManagedQueue(rootQ, "parent", true, map[string]string{"first": "10"})
	assert.NilError(t, err)
	childQ1, err = createManagedQueue(parentQ, "child1", false, map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ2, err = createManagedQueue(parentQ, "child2", false, map[string]string{"first": "5"})
	assert.NilError(t, err)
	rootRemaining, pRemaining, cRemaining1, cRemaining2 := getRemainingGuaranteed(rootQ, parentQ, childQ1, childQ2)
	assert.Assert(t, resources.IsZero(rootRemaining), "guaranteed not set, so no remaining")
	assert.Assert(t, resources.IsZero(pRemaining), "guaranteed not set, so no remaining")
	assert.Assert(t, resources.IsZero(cRemaining1), "guaranteed not set, so no remaining")
	assert.Assert(t, resources.IsZero(cRemaining2), "guaranteed not set, so no remaining")

	// guaranteed set but no usage. so nothing to preempt
	// clean start for the snapshot: whole hierarchy with guarantee
	smallestRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	rootQ.guaranteedResource = resources.Multiply(smallestRes, 2)
	parentQ.guaranteedResource = smallestRes
	childQ2.guaranteedResource = smallestRes
	childRes := resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5})
	childQ1.guaranteedResource = childRes
	rootRemaining, pRemaining, cRemaining1, cRemaining2 = getRemainingGuaranteed(rootQ, parentQ, childQ1, childQ2)
	assert.Assert(t, resources.Equals(rootRemaining, resources.Multiply(smallestRes, 2)), "guaranteed set, but no usage. so all guaranteed should be in cRemaining1")
	assert.Assert(t, resources.Equals(pRemaining, smallestRes), "guaranteed set, but no usage. so all guaranteed should be in cRemaining1")
	assert.Assert(t, resources.Equals(cRemaining1, resources.Add(smallestRes, childRes)), "guaranteed set, but no usage. so all guaranteed + parent cRemaining1 guaranteed should be in cRemaining1")
	assert.Assert(t, resources.Equals(cRemaining2, smallestRes), "guaranteed set, but no usage. so all guaranteed should be in cRemaining1")

	// clean start for the snapshot: all set guaranteed
	// add usage to parent + root: use all guaranteed at parent level
	// add usage to child2: use double than guaranteed
	rootQ.allocatedResource = resources.Multiply(smallestRes, 2)
	parentQ.allocatedResource = resources.Multiply(smallestRes, 2)
	childQ2.allocatedResource = resources.Multiply(smallestRes, 2)
	rootRemaining, pRemaining, cRemaining1, cRemaining2 = getRemainingGuaranteed(rootQ, parentQ, childQ1, childQ2)
	assert.Assert(t, resources.IsZero(rootRemaining), "guaranteed set, used completely. so all guaranteed should be in cRemaining1")
	assert.Assert(t, resources.Equals(pRemaining, resources.Multiply(smallestRes, -1)), "guaranteed set, used double than guaranteed. so cRemaining1 should be in -ve")
	assert.Assert(t, resources.Equals(cRemaining1, resources.Add(resources.Multiply(smallestRes, -1), childRes)), "guaranteed set, but no usage. However cRemaining1 should include its all guaranteed + parent cRemaining1 guaranteed")
	assert.Assert(t, resources.Equals(cRemaining2, resources.Multiply(smallestRes, -1)), "guaranteed set, used double than guaranteed. so cRemaining1 should be in -ve")

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
	rootRemaining, pRemaining, cRemaining1, cRemaining2 = getRemainingGuaranteed(rootQ, parentQ, childQ1, childQ2)
	assert.Assert(t, resources.IsZero(rootRemaining), "guaranteed set, used completely. usage also has extra resource types. However, no cRemaining1")
	assert.Assert(t, resources.IsZero(pRemaining), "guaranteed set, used completely. usage also has extra resource types. However, no cRemaining1")
	assert.Assert(t, resources.IsZero(cRemaining1), "guaranteed set, used completely. so, no cRemaining1")
	assert.Assert(t, resources.IsZero(cRemaining2), "guaranteed set, but no usage. Still, no cRemaining1 in guaranteed because of its parent queue")

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
	rootRemaining, pRemaining, cRemaining1, cRemaining2 = getRemainingGuaranteed(rootQ, parentQ, childQ1, childQ2)
	assert.Assert(t, resources.IsZero(rootRemaining), "guaranteed set, used completely. usage also has extra resource types. However, no cRemaining1")
	assert.Assert(t, resources.IsZero(pRemaining), "guaranteed set, used completely. usage also has extra resource types. However, no cRemaining1")
	assert.Assert(t, resources.Equals(cRemaining1, resources.Multiply(childRes, -1)), "guaranteed set, used double than guaranteed. so cRemaining1 should be in -ve")
	assert.Assert(t, resources.IsZero(cRemaining2), "guaranteed set, but no usage. Still, no cRemaining1 in guaranteed because of its parent queue")
}

func createQPSCache(rootQ *Queue, parentQ *Queue, childQ1 *Queue, childQ2 *Queue) (*QueuePreemptionSnapshot, *QueuePreemptionSnapshot, *QueuePreemptionSnapshot, *QueuePreemptionSnapshot) {
	cache := make(map[string]*QueuePreemptionSnapshot)
	qpsRoot := rootQ.createPreemptionSnapshot(cache)
	qpsParent := parentQ.createPreemptionSnapshot(cache)
	qpsChild1 := childQ1.createPreemptionSnapshot(cache)
	qpsChild2 := childQ2.createPreemptionSnapshot(cache)
	return qpsRoot, qpsParent, qpsChild1, qpsChild2
}

func getRemainingGuaranteed(rootQ *Queue, parentQ *Queue, childQ1 *Queue, childQ2 *Queue) (*resources.Resource, *resources.Resource, *resources.Resource, *resources.Resource) {
	qpsRoot, qpsParent, qpsChild1, qpsChild2 := createQPSCache(rootQ, parentQ, childQ1, childQ2)
	rootRemaining := qpsRoot.GetRemainingGuaranteedResource()
	pRemaining := qpsParent.GetRemainingGuaranteedResource()
	cRemaining1 := qpsChild1.GetRemainingGuaranteedResource()
	cRemaining2 := qpsChild2.GetRemainingGuaranteedResource()
	return rootRemaining, pRemaining, cRemaining1, cRemaining2
}

func getPreemptableResource(rootQ *Queue, parentQ *Queue, childQ1 *Queue, childQ2 *Queue) (*resources.Resource, *resources.Resource, *resources.Resource, *resources.Resource) {
	qpsRoot, qpsParent, qpsChild1, qpsChild2 := createQPSCache(rootQ, parentQ, childQ1, childQ2)
	rootRemaining := qpsRoot.GetPreemptableResource()
	pRemaining := qpsParent.GetPreemptableResource()
	cRemaining1 := qpsChild1.GetPreemptableResource()
	cRemaining2 := qpsChild2.GetPreemptableResource()
	return rootRemaining, pRemaining, cRemaining1, cRemaining2
}
