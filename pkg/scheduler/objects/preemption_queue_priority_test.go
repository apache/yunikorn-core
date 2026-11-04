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
	"fmt"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
)

type queuePriorityCase struct {
	name     string
	enabled  bool
	ask      int64
	protect5 bool
	twoNodes bool
	want     []string
}

func TestQueuePriorityVictimStages(t *testing.T) {
	cases := []queuePriorityCase{
		{"baseline_newest_grade2", false, 1, false, false, []string{"grade2"}},
		{"grade5_only", true, 1, false, false, []string{"grade5"}},
		{"additional_victim_on_unschedulable_node", true, 1, false, false, []string{"grade5"}},
		{"request_priority_fence", true, 1, false, false, nil},
		{"victim_priority_fence", true, 1, false, false, nil},
		{"parent_offsets", true, 1, false, false, []string{"grade5"}},
		{"expand_to_grade4", true, 2, false, false, []string{"grade5", "grade4"}},
		{"expand_to_grade3", true, 3, false, false, []string{"grade5", "grade4", "grade3"}},
		{"grade2_last", true, 4, false, false, []string{"grade5", "grade4", "grade3", "grade2"}},
		{"protect_grade5_guarantee", true, 1, true, false, []string{"grade4"}},
		{"multinode_skip_useless_grade5", true, 2, false, true, []string{"grade4"}},
		{"impossible_request_no_deletion", true, 5, false, false, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { runQueuePriorityCase(t, tc) })
	}
}

func runQueuePriorityCase(t *testing.T, tc queuePriorityCase) {
	t.Helper()

	mapping := NewAppQueueMapping()
	root, err := createRootQueue(map[string]string{"first": "20", "memory": "100"})
	assert.NilError(t, err)
	pool, err := createManagedQueueGuaranteed(root, "pool", true, map[string]string{"first": "20", "memory": "100"}, map[string]string{"first": "20", "memory": "100"}, mapping)
	assert.NilError(t, err)
	high, err := createManagedQueueGuaranteed(pool, "grade1", false, map[string]string{"first": "20", "memory": "100"}, map[string]string{"first": fmt.Sprint(tc.ask)}, mapping)
	assert.NilError(t, err)
	high.priorityOffset = 500
	if tc.enabled {
		high.properties[configs.PreemptionVictimPolicy] = configs.PreemptionVictimQueuePriority
		high.UpdateQueueProperties(nil)
		high.priorityOffset = 500
	}
	if tc.name == "request_priority_fence" {
		high.priorityPolicy = policies.FencePriorityPolicy
	}
	n1 := newNode("n1", map[string]resources.Quantity{"first": 4, "memory": 100})
	nodes := []*Node{n1}
	if tc.twoNodes {
		n1 = newNode("n1", map[string]resources.Quantity{"first": 2, "memory": 100})
		nodes = []*Node{n1, newNode("n2", map[string]resources.Quantity{"first": 2, "memory": 100})}
	}
	victims := map[string]*Allocation{}
	for grade := 5; grade >= 2; grade-- {
		if tc.twoNodes && grade < 4 {
			continue
		}
		name := fmt.Sprintf("grade%d", grade)
		g := map[string]string{}
		if grade == 5 && tc.protect5 {
			g["first"] = "1"
		}
		var q *Queue
		q, err = createManagedQueueGuaranteed(pool, name, tc.name == "parent_offsets", map[string]string{"first": "20", "memory": "100"}, g, mapping)
		assert.NilError(t, err)
		q.priorityOffset = int32((6 - grade) * 100)
		if tc.name == "victim_priority_fence" && grade == 5 {
			q.priorityPolicy = policies.FencePriorityPolicy
		}
		if tc.name == "parent_offsets" {
			q, err = createManagedQueueGuaranteed(q, "jobs", false, map[string]string{"first": "20", "memory": "100"}, g, mapping)
			assert.NilError(t, err)
		}
		app := newApplication(name, "default", q.QueuePath)
		app.SetQueue(q)
		q.AddApplication(app)
		mapping.AddAppQueueMapping(name, q)
		node := n1
		amount := int64(1)
		if tc.twoNodes && grade == 4 {
			node = nodes[1]
			amount = 2
		}
		a := newAllocationWithKey(name, name, node.NodeID, resources.NewResourceFromMap(map[string]resources.Quantity{"first": resources.Quantity(amount), "memory": 1}))
		a.createTime = time.Now().Add(-time.Duration(grade) * time.Minute)
		assert.NilError(t, app.AddAllocation(a))
		assert.Assert(t, node.TryAddAllocation(a))
		assert.NilError(t, q.TryIncAllocatedResource(a.GetAllocatedResource()))
		victims[name] = a
	}
	if tc.name == "additional_victim_on_unschedulable_node" {
		n1.SetSchedulable(false)
		nodes = append(nodes, newNode("n2", map[string]resources.Quantity{"first": 4, "memory": 100}))
	}
	var foreign *Allocation
	if tc.twoNodes {
		foreign = newAllocationWithKey("foreign-driver", "foreign", n1.NodeID, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1}))
		assert.Assert(t, n1.TryAddAllocation(foreign))
	}
	app, ask, err := creatApp2(high, map[string]resources.Quantity{"first": resources.Quantity(tc.ask), "memory": 1}, "request", mapping)
	assert.NilError(t, err)
	headroom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 20, "memory": 100})
	nodesTried := true
	if tc.name == "additional_victim_on_unschedulable_node" {
		headroom = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0})
		nodesTried = false
	}
	preemptor := NewPreemptor(app, headroom, 0, ask, getNodeIteratorFn(nodes...)(), nodesTried)
	result, ok := preemptor.TryPreemption()
	assertQueuePriorityResult(t, tc, result, ok, victims, foreign)
}

func assertQueuePriorityResult(t *testing.T, tc queuePriorityCase, result *AllocationResult, ok bool, victims map[string]*Allocation, foreign *Allocation) {
	t.Helper()
	if tc.want == nil {
		assert.Assert(t, !ok)
		assert.Assert(t, result == nil)
		for _, a := range victims {
			assert.Assert(t, !a.IsPreempted())
		}
		return
	}
	assert.Assert(t, ok)
	assert.Assert(t, result != nil)
	expected := map[string]bool{}
	for _, name := range tc.want {
		expected[name] = true
	}
	for name, a := range victims {
		assert.Equal(t, a.IsPreempted(), expected[name], name)
	}
	if foreign != nil {
		assert.Assert(t, !foreign.IsPreempted())
		assert.Equal(t, result.NodeID, "n2")
	}
	t.Logf("victims=%v node=%s foreign_driver_preserved=%v", tc.want, result.NodeID, foreign == nil || !foreign.IsPreempted())
}

func TestQueuePriorityVictimPolicyInheritanceAndReload(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err)
	assert.Assert(t, !root.usesQueuePriorityVictims())
	parent, err := createManagedQueueWithProps(root, "parent", true, nil, map[string]string{configs.PreemptionVictimPolicy: configs.PreemptionVictimQueuePriority})
	assert.NilError(t, err)
	child, err := createManagedQueue(parent, "child", false, nil)
	assert.NilError(t, err)
	assert.Assert(t, child.usesQueuePriorityVictims())
	override, err := createManagedQueueWithProps(parent, "override", false, nil, map[string]string{configs.PreemptionVictimPolicy: configs.PreemptionVictimDefault})
	assert.NilError(t, err)
	assert.Assert(t, !override.usesQueuePriorityVictims())
	// Removing the parent's option must restore the default after normal reload.
	_, err = parent.ApplyConf(configs.QueueConfig{Name: "parent", Parent: true})
	assert.NilError(t, err)
	parent.MergeParentProperties()
	parent.UpdateQueueProperties(nil)
	_, err = child.ApplyConf(configs.QueueConfig{Name: "child"})
	assert.NilError(t, err)
	child.MergeParentProperties()
	child.UpdateQueueProperties(nil)
	assert.Assert(t, !child.usesQueuePriorityVictims())
}
