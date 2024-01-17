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

func TestGetGuaranteedResource(t *testing.T) {
	rootQ, err := createRootQueue(map[string]string{"first": "20"})
	assert.NilError(t, err)
	var parentQ, childQ *Queue
	parentQ, err = createManagedQueue(rootQ, "parent", true, nil)
	assert.NilError(t, err)
	childQ, err = createManagedQueue(parentQ, "child1", false, nil)
	assert.NilError(t, err)
	cache := make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 := childQ.createPreemptionSnapshot(cache)
	assert.Equal(t, len(cache), 3, "expected 3 entries in snapsht cache")
	res := qpsChild1.GetGuaranteedResource()
	assert.Assert(t, res.IsEmpty(), "expected empty resource")

	// clean start for the snapshot: root and parent set
	smallestRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	rootQ.guaranteedResource = resources.Multiply(smallestRes, 2)
	parentQ.guaranteedResource = smallestRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	res = qpsChild1.GetGuaranteedResource()
	assert.Assert(t, resources.Equals(res, smallestRes), "expected smallest resource to be set")

	// clean start for the snapshot: whole hierarchy
	childRes := resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5})
	childQ.guaranteedResource = childRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	res = qpsChild1.GetGuaranteedResource()
	assert.Assert(t, resources.Equals(res, resources.Add(smallestRes, childRes)), "expected combined resource to be set")
}

func TestGetMaxResource(t *testing.T) {
	rootQ, err := createRootQueue(nil)
	assert.NilError(t, err)
	var parentQ, childQ *Queue
	parentQ, err = createManagedQueue(rootQ, "parent", true, nil)
	assert.NilError(t, err)
	childQ, err = createManagedQueue(parentQ, "child1", false, nil)
	assert.NilError(t, err)
	cache := make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 := childQ.createPreemptionSnapshot(cache)
	assert.Equal(t, len(cache), 3, "expected 3 entries in snapsht cache")
	res := qpsChild1.GetMaxResource()
	assert.Assert(t, res.IsEmpty(), "expected empty resource")

	// clean start for the snapshot: root and parent set
	smallestRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	rootQ.maxResource = resources.Multiply(smallestRes, 2)
	parentQ.maxResource = smallestRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	res = qpsChild1.GetMaxResource()
	assert.Assert(t, resources.Equals(res, smallestRes), "expected smallest resource to be set")

	// clean start for the snapshot: whole hierarchy
	childRes := resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5})
	childQ.maxResource = childRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	res = qpsChild1.GetMaxResource()
	assert.Assert(t, resources.Equals(res, resources.Add(smallestRes, childRes)), "expected combined resource to be set")
}

func TestFitGuaranteedResource(t *testing.T) {
	rootQ, err := createRootQueue(map[string]string{"first": "20"})
	assert.NilError(t, err)
	var parentQ, childQ *Queue
	parentQ, err = createManagedQueue(rootQ, "parent", true, map[string]string{"first": "15"})
	assert.NilError(t, err)
	childQ, err = createManagedQueue(parentQ, "child1", false, map[string]string{"first": "10"})
	assert.NilError(t, err)
	cache := make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 := childQ.createPreemptionSnapshot(cache)
	assert.Equal(t, len(cache), 3, "expected 3 entries in snapsht cache")
	assert.Assert(t, !qpsChild1.IsWithinGuaranteedResource(), "no guaranteed should never be within guaranteed")
	assert.Assert(t, qpsChild1.IsAtOrAboveGuaranteedResource(), "no guaranteed should always be above guaranteed")

	// clean start for the snapshot: whole hierarchy with guarantee
	smallestRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	rootQ.guaranteedResource = resources.Multiply(smallestRes, 2)
	parentQ.guaranteedResource = smallestRes
	childRes := resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5})
	childQ.guaranteedResource = childRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	assert.Assert(t, qpsChild1.IsWithinGuaranteedResource(), "no usage guaranteed set should be within guaranteed")
	assert.Assert(t, !qpsChild1.IsAtOrAboveGuaranteedResource(), "no usage guaranteed set should not be above guaranteed")

	// clean start for the snapshot: guaranteed set as before
	// add usage to parent + root: use all guaranteed at root level, more than guaranteed at parent
	// no usage at the child
	rootQ.allocatedResource = resources.Multiply(smallestRes, 2)
	parentQ.allocatedResource = resources.Multiply(smallestRes, 2)
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	assert.Assert(t, !qpsChild1.IsWithinGuaranteedResource(), "more than guarantee is used by parent")
	assert.Assert(t, qpsChild1.IsAtOrAboveGuaranteedResource(), "more than guarantee is used by parent")

	// clean start for the snapshot: all set guaranteed
	// add usage for all: use exactly guaranteed at parent and child level
	rootQ.allocatedResource = resources.Multiply(smallestRes, 2)
	parentQ.allocatedResource = smallestRes
	childQ.allocatedResource = childRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	assert.Assert(t, qpsChild1.IsWithinGuaranteedResource(), "usage equals guaranteed should be within")
	assert.Assert(t, qpsChild1.IsAtOrAboveGuaranteedResource(), "usage equals guaranteed should be at")

	// clean start for the snapshot: all set guaranteed
	// add usage for all: use exactly guaranteed at parent and child level
	// usage contains type not guaranteed at parent and root
	rootQ.allocatedResource = resources.Multiply(smallestRes, 2)
	rootQ.allocatedResource.AddTo(childRes)
	parentQ.allocatedResource = resources.Add(smallestRes, childRes)
	childQ.allocatedResource = childRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	assert.Assert(t, !qpsChild1.IsWithinGuaranteedResource(), "usage equals guaranteed should be within")
	assert.Assert(t, qpsChild1.IsAtOrAboveGuaranteedResource(), "usage equals guaranteed should be at")
}

func TestGetRemainingGuaranteed(t *testing.T) {
	rootQ, err := createRootQueue(map[string]string{"first": "20"})
	assert.NilError(t, err)
	var parentQ, childQ *Queue
	parentQ, err = createManagedQueue(rootQ, "parent", true, map[string]string{"first": "15"})
	assert.NilError(t, err)
	childQ, err = createManagedQueue(parentQ, "child1", false, map[string]string{"first": "10"})
	assert.NilError(t, err)
	cache := make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 := childQ.createPreemptionSnapshot(cache)
	assert.Equal(t, len(cache), 3, "expected 3 entries in snapsht cache")
	remaining := qpsChild1.GetRemainingGuaranteed()
	assert.Assert(t, !remaining.IsEmpty(), "no remaining should always have no remaining left")

	// clean start for the snapshot: whole hierarchy with guarantee
	smallestRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	rootQ.guaranteedResource = resources.Multiply(smallestRes, 2)
	parentQ.guaranteedResource = smallestRes
	childRes := resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5})
	childQ.guaranteedResource = childRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	bothRes := resources.Add(smallestRes, childRes)
	remaining = qpsChild1.GetRemainingGuaranteed()
	assert.Assert(t, resources.Equals(bothRes, remaining), "no usage remaining remaining should be smallest + child")

	// clean start for the snapshot: all set guaranteed
	// add usage to parent + root: use all guaranteed at parent level
	rootQ.allocatedResource = resources.Multiply(smallestRes, 2)
	parentQ.allocatedResource = resources.Multiply(smallestRes, 2)
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	remaining = qpsChild1.GetRemainingGuaranteed()
	assert.Assert(t, !resources.Equals(childRes, remaining), "guarantee is used at parent should be child only")

	// clean start for the snapshot: all set guaranteed
	// add usage for all: use exactly guaranteed at parent and child level
	// parent guarantee used for one type child guarantee used for second type
	bothRes = resources.Multiply(smallestRes, 2)
	bothRes.AddTo(childRes)
	rootQ.allocatedResource = bothRes
	bothRes = resources.Add(smallestRes, childRes)
	parentQ.allocatedResource = bothRes
	childQ.allocatedResource = childRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	remaining = qpsChild1.GetRemainingGuaranteed()
	assert.Assert(t, !resources.IsZero(remaining), "no resource should be left")

	// clean start for the snapshot: all set guaranteed
	// add usage for all: use exactly less than guaranteed at parent for one resource after preemption
	bothRes = resources.Multiply(smallestRes, 2)
	bothRes.AddTo(childRes)
	rootQ.allocatedResource = bothRes
	rootQ.preemptingResource = smallestRes
	bothRes = resources.Add(smallestRes, childRes)
	parentQ.allocatedResource = bothRes
	parentQ.preemptingResource = smallestRes
	childQ.allocatedResource = childRes
	cache = make(map[string]*QueuePreemptionSnapshot)
	qpsChild1 = childQ.createPreemptionSnapshot(cache)
	remaining = qpsChild1.GetRemainingGuaranteed()
	assert.Assert(t, !resources.Equals(smallestRes, remaining), "preempting resource should be left")
}
