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
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/mock"
	"github.com/apache/yunikorn-core/pkg/plugins"
)

// const appID3 = "app-3"
const alloc = "alloc"
const node1 = "node1"

func TestCheckPreconditions(t *testing.T) {
	node := newNode("node1", map[string]resources.Quantity{"first": 5})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ, err := createManagedQueue(rootQ, "child", false, map[string]string{"first": "5"})
	assert.NilError(t, err)
	app := newApplication(appID1, "default", "root.child")
	app.SetQueue(childQ)
	childQ.applications[appID1] = app
	ask := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask.allowPreemptOther = true
	ask.createTime = time.Now().Add(-1 * time.Minute)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err)
	preemptor := NewPreemptor(app, resources.NewResource(), 30*time.Second, ask, iterator(), false)

	// success case
	assert.Assert(t, preemptor.CheckPreconditions(), "preconditions failed")
	ask.preemptCheckTime = time.Now().Add(-1 * time.Minute)

	// verify ask which opted-out of preempting others is disqualified
	ask.allowPreemptOther = false
	assert.Assert(t, !preemptor.CheckPreconditions(), "preconditions succeeded when ask doesn't allow preempt other")
	ask.allowPreemptOther = true
	assert.Assert(t, preemptor.CheckPreconditions(), "preconditions failed")
	ask.preemptCheckTime = time.Now().Add(-1 * time.Minute)

	// verify previously triggered preemption disqualifies ask
	ask.MarkTriggeredPreemption()
	assert.Assert(t, !preemptor.CheckPreconditions(), "preconditions succeeded when ask has already triggered preemption")
	ask.preemptionTriggered = false
	assert.Assert(t, preemptor.CheckPreconditions(), "preconditions failed")
	ask.preemptCheckTime = time.Now().Add(-1 * time.Minute)

	// verify that ask requiring a specific node is disqualified
	ask.SetRequiredNode("node1")
	assert.Assert(t, !preemptor.CheckPreconditions(), "preconditions succeeded with ask requiring a specific node")
	ask.SetRequiredNode("")
	assert.Assert(t, preemptor.CheckPreconditions(), "preconditions failed")
	ask.preemptCheckTime = time.Now().Add(-1 * time.Minute)

	// verify that recently created ask is disqualified
	ask.createTime = time.Now()
	assert.Assert(t, !preemptor.CheckPreconditions(), "preconditions succeeded with newly-created ask")
	ask.createTime = time.Now().Add(-1 * time.Minute)
	assert.Assert(t, preemptor.CheckPreconditions(), "preconditions failed")
	ask.preemptCheckTime = time.Now().Add(-1 * time.Minute)

	// verify that recently checked ask is disqualified
	ask.preemptCheckTime = time.Now()
	assert.Assert(t, !preemptor.CheckPreconditions(), "preconditions succeeded with recently tried ask")
	ask.preemptCheckTime = time.Now().Add(-1 * time.Minute)
	assert.Assert(t, preemptor.CheckPreconditions(), "preconditions failed")
	assert.Assert(t, !preemptor.CheckPreconditions(), "preconditions succeeded on successive run")
}

func TestCheckPreemptionQueueGuarantees(t *testing.T) {
	node := newNode("node1", map[string]resources.Quantity{"first": 20})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"first": "20"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "20"}, map[string]string{"first": "10"})
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
	assert.NilError(t, err)
	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	app1.AddAllocation(NewAllocation("node1", ask1))
	app1.AddAllocation(NewAllocation("node1", ask2))
	assert.NilError(t, childQ1.IncAllocatedResource(ask1.GetAllocatedResource(), false))
	assert.NilError(t, childQ1.IncAllocatedResource(ask2.GetAllocatedResource(), false))
	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	childQ2.incPendingResource(ask3.GetAllocatedResource())
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// positive case
	assert.Assert(t, preemptor.checkPreemptionQueueGuarantees(), "queue guarantees fail")

	// verify too large of a resource will not succeed
	ask3.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25})
	assert.Assert(t, !preemptor.checkPreemptionQueueGuarantees(), "queue guarantees did not fail")
}

func TestCheckPreemptionQueueGuaranteesWithNoGuaranteedResources(t *testing.T) {
	var tests = []struct {
		testName         string
		expected         bool
		parentGuaranteed map[string]string
		childGuaranteed  map[string]string
	}{
		{"NoGuaranteed", false, map[string]string{}, map[string]string{}},
		{"ParentGuaranteed", true, map[string]string{"first": "10"}, map[string]string{}},
		{"ChildGuaranteed", true, map[string]string{}, map[string]string{"first": "5"}},
		{"BothGuaranteed", true, map[string]string{"first": "10"}, map[string]string{"first": "5"}},
	}
	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			node := newNode("node1", map[string]resources.Quantity{"first": 20})
			iterator := getNodeIteratorFn(node)
			rootQ, err := createRootQueue(map[string]string{"first": "20"})
			assert.NilError(t, err)
			parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "20"}, tt.parentGuaranteed)
			assert.NilError(t, err)
			childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
			assert.NilError(t, err)
			childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, map[string]string{"first": "10"}, tt.childGuaranteed)
			assert.NilError(t, err)
			app1 := newApplication(appID1, "default", "root.parent.child1")
			app1.SetQueue(childQ1)
			childQ1.applications[appID1] = app1
			ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
			assert.NilError(t, app1.AddAllocationAsk(ask1))
			ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
			assert.NilError(t, app1.AddAllocationAsk(ask2))
			app1.AddAllocation(NewAllocation("node1", ask1))
			app1.AddAllocation(NewAllocation("node1", ask2))
			assert.NilError(t, childQ1.IncAllocatedResource(ask1.GetAllocatedResource(), false))
			assert.NilError(t, childQ1.IncAllocatedResource(ask2.GetAllocatedResource(), false))
			app2 := newApplication(appID2, "default", "root.parent.child2")
			app2.SetQueue(childQ2)
			childQ2.applications[appID2] = app2
			ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
			assert.NilError(t, app2.AddAllocationAsk(ask3))
			childQ2.incPendingResource(ask3.GetAllocatedResource())
			headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
			preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)
			assert.Equal(t, tt.expected, preemptor.checkPreemptionQueueGuarantees(), "unexpected result")
		})
	}
}

func TestTryPreemption(t *testing.T) {
	node := newNode("node1", map[string]resources.Quantity{"first": 10, "pods": 5})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"first": "20", "pods": "5"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "20"}, map[string]string{"first": "10"})
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
	assert.NilError(t, err)
	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "pods": 1}))
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "pods": 1}))
	ask2.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	alloc1 := NewAllocation("node1", ask1)
	app1.AddAllocation(alloc1)
	assert.Check(t, node.AddAllocation(alloc1), "node alloc1 failed")
	alloc2 := NewAllocation("node1", ask2)
	assert.Check(t, node.AddAllocation(alloc2), "node alloc2 failed")
	node.AddAllocation(alloc2)
	assert.NilError(t, childQ1.IncAllocatedResource(ask1.GetAllocatedResource(), false))
	assert.NilError(t, childQ1.IncAllocatedResource(ask2.GetAllocatedResource(), false))
	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "pods": 1}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	childQ2.incPendingResource(ask3.GetAllocatedResource())
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{
		mock.NewPreemption(true, "alloc3", "node1", []string{"alloc1"}, 0, 0),
	}
	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	alloc, ok := preemptor.TryPreemption()
	assert.NilError(t, plugin.GetPredicateError())
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", alloc.allocationKey, "wrong alloc")
	assert.Check(t, alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, !alloc2.IsPreempted(), "alloc2 not preempted")
}

// TestTryPreemptionOnNode Test try preemption on node with simple queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue path in 2 levels. victim and preemptor queue are siblings.
// Ask (Preemptor) resource type matches with all resource types of the victim. But Guaranteed set only on specific resource type. 2 Victims are available, but 1 should be preempted because further preemption would make usage go below the guaranteed quota
// Setup:
// Nodes are Node1 and Node2. Nodes are full. No space to accommodate the ask.
// root.parent. Guaranteed set on parent, first: 10
// root.parent.child1. Guaranteed set, first: 5. 2 Allocations (belongs to single app) are running. Each Allocation usage is first:5, pods: 1. Total usage is first:10, pods: 2.
// root.parent.child2. Guaranteed set, first: 5. Ask of first:5 is waiting for resources.
// 1 Allocation on root.parent.child1 should be preempted to free up resources for ask arrived in root.parent.child2.
func TestTryPreemptionOnNode(t *testing.T) {
	t.SkipNow()
	node1 := newNode("node1", map[string]resources.Quantity{"first": 5, "pods": 1})
	node2 := newNode("node2", map[string]resources.Quantity{"first": 5, "pods": 1})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(nil)
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "20"}, map[string]string{"first": "10"})
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
	assert.NilError(t, err)
	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "pods": 1}))
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "pods": 1}))
	ask2.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	alloc1 := NewAllocation("node1", ask1)
	app1.AddAllocation(alloc1)
	assert.Check(t, node1.AddAllocation(alloc1), "node alloc1 failed")
	alloc2 := NewAllocation("node1", ask2)
	app1.AddAllocation(alloc2)
	assert.Check(t, node2.AddAllocation(alloc2), "node alloc2 failed")
	assert.NilError(t, childQ1.IncAllocatedResource(ask1.GetAllocatedResource(), false))
	assert.NilError(t, childQ1.IncAllocatedResource(ask2.GetAllocatedResource(), false))
	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "pods": 1}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	alloc, ok := preemptor.TryPreemption()
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", alloc.allocationKey, "wrong alloc")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
}

// TestTryPreemptionOnQueue Test try preemption on queue with simple queue hierarchy. Since Node has enough resources to accomodate, preemption happens because of queue resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue path in 2 levels. victim and preemptor queue are siblings.
// Ask (Preemptor) resource type matches with all resource types of the victim. But Guaranteed set only on specific resource type. 2 Victims are available, but 1 should be preempted because further preemption would make usage go below the guaranteed quota
// Setup:
// Nodes are Node1 and Node2. Node has enough space to accommodate the new ask.
// root.parent. Guaranteed set on parent, first: 10
// root.parent.child1. Guaranteed set, first: 5. 2 Allocations (belongs to single app) are running. Each Allocation usage is first:5, pods: 1. Total usage is first:10, pods: 2.
// root.parent.child2. Guaranteed set, first: 5. Ask of first:5 is waiting for resources.
// 1 Allocation on root.parent.child1 should be preempted to free up resources for ask arrived in root.parent.child2.
func TestTryPreemptionOnQueue(t *testing.T) {
	t.SkipNow()
	node1 := newNode("node1", map[string]resources.Quantity{"first": 10, "pods": 2})
	node2 := newNode("node2", map[string]resources.Quantity{"first": 10, "pods": 2})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(nil)
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "10"}, nil)
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)
	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "pods": 1}))
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "pods": 1}))
	ask2.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	alloc1 := NewAllocation("node1", ask1)
	app1.AddAllocation(alloc1)
	assert.Check(t, node1.AddAllocation(alloc1), "node alloc1 failed")
	alloc2 := NewAllocation("node1", ask2)
	app1.AddAllocation(alloc2)
	assert.Check(t, node2.AddAllocation(alloc2), "node alloc2 failed")
	assert.NilError(t, childQ1.IncAllocatedResource(ask1.GetAllocatedResource(), false))
	assert.NilError(t, childQ1.IncAllocatedResource(ask2.GetAllocatedResource(), false))
	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "pods": 1}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	alloc, ok := preemptor.TryPreemption()
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", alloc.allocationKey, "wrong alloc")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
}

// TestTryPreemptionWithVictimsAvailableButInsufficientResource Test try preemption on queue with simple queue hierarchy. Since Node has enough resources to accomodate, preemption happens because of queue resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue path. victim and preemptor queue are siblings.
// Ask (Preemptor) resource type matches with 1 resource type of the victim. Guaranteed also set on specific resource type. 2 Victims are available, but total resource usage is lesser than ask requirement.
// Setup:
// Nodes are Node1 and Node2. Node has enough space to accommodate the new ask.
// root.parent.Max set on parent, first: 8
// root.parent.child1. Guaranteed not set. 2 Allocations (belongs to single app) are running. Each Allocation usage is first:2, pods: 1. Total usage is first:4, pods: 2.
// root.parent.child2. Guaranteed set, first: 5. Ask of first:5 is waiting for resources.
// 2 Allocation on root.parent.child1 has been found and considered as victims. Since victims total resource usage (first: 4) is lesser than ask requirment (first: 5), preemption won't help. Hence, victims are dropped.
func TestTryPreemptionWithVictimsAvailableButInsufficientResource(t *testing.T) {
	t.SkipNow()
	node1 := newNode(node1, map[string]resources.Quantity{"first": 10, "pods": 2})
	node2 := newNode("node2", map[string]resources.Quantity{"first": 10, "pods": 2})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(map[string]string{"first": "20", "pods": "5"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "8"}, nil)
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, nil)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)
	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "pods": 1}))
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "pods": 1}))
	ask2.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	alloc1 := NewAllocation("node1", ask1)
	app1.AddAllocation(alloc1)
	assert.Check(t, node1.AddAllocation(alloc1), "node alloc1 failed")
	alloc2 := NewAllocation("node1", ask2)
	app1.AddAllocation(alloc2)
	assert.Check(t, node2.AddAllocation(alloc2), "node alloc2 failed")
	assert.NilError(t, childQ1.IncAllocatedResource(ask1.GetAllocatedResource(), false))
	assert.NilError(t, childQ1.IncAllocatedResource(ask2.GetAllocatedResource(), false))
	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	alloc, ok := preemptor.TryPreemption()
	var expectedAlloc *Allocation
	assert.Equal(t, ok, false, "no victims found")
	assert.Equal(t, alloc, expectedAlloc, "wrong alloc")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, !alloc2.IsPreempted(), "alloc2 not preempted")
}

// TestTryPreemptionWithVictimsAvailableOnDifferentNodesButInsufficientResource Test try preemption on queue with simple queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue path. victim and preemptor queue are siblings.
// Ask (Preemptor) resource type matches with 1 resource type of the victim. Guaranteed also set on specific resource type. 2 Victims are available, but total resource usage is lesser than ask requirement.
// Setup:
// Nodes are Node1 and Node2. Both Nodes are almost full. No space to accommodate the ask.
// root.parent.Max set on parent, first: 6
// root.parent.child1. Guaranteed not set. 2 Allocations (belongs to single app) are running. Each Allocation usage is first:2, pods: 1. Total usage is first:4, pods: 2.
// root.parent.child2. Guaranteed set, first: 5. Ask of first:5 is waiting for resources.
// 2 Allocation on root.parent.child1 has been found and considered as victims. Since victims total resource usage (first: 4) is lesser than ask requirment (first: 5), preemption won't help. Hence, victims are dropped.
func TestTryPreemptionWithVictimsAvailableOnDifferentNodesButInsufficientResource(t *testing.T) {
	t.SkipNow()
	node1 := newNode(node1, map[string]resources.Quantity{"first": 5, "pods": 1})
	node2 := newNode("node2", map[string]resources.Quantity{"first": 5, "pods": 1})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(nil)
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "6"}, nil)
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, nil)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)
	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "pods": 1}))
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "pods": 1}))
	ask2.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	alloc1 := NewAllocation("node1", ask1)
	app1.AddAllocation(alloc1)
	assert.Check(t, node1.AddAllocation(alloc1), "node alloc1 failed")
	alloc2 := NewAllocation("node2", ask2)
	app1.AddAllocation(alloc2)
	assert.Check(t, node2.AddAllocation(alloc2), "node alloc2 failed")
	assert.NilError(t, childQ1.IncAllocatedResource(ask1.GetAllocatedResource(), false))
	assert.NilError(t, childQ1.IncAllocatedResource(ask2.GetAllocatedResource(), false))
	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{
		mock.NewPreemption(true, "alloc3", "node1", []string{"alloc1"}, 0, 0),
		mock.NewPreemption(true, "alloc3", "node2", []string{"alloc2"}, 0, 0),
	}

	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	alloc, ok := preemptor.TryPreemption()
	var expectedAlloc *Allocation
	assert.Equal(t, ok, false, "no victims found")
	assert.Equal(t, alloc, expectedAlloc, "wrong alloc")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, !alloc2.IsPreempted(), "alloc2 not preempted")
}

// TestTryPreemptionWithVictimsAvailableButOnDifferentNodes Test try preemption on queue with simple queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue path. victim and preemptor queue are siblings.
// Ask (Preemptor) resource type matches with 1 resource type of the victim. Guaranteed also set on specific resource type. 2 Victims are available, but total resource usage is lesser than ask requirement.
// Setup:
// Nodes are Node1 and Node2. Both Nodes are almost full. No space to accommodate the ask. Node 2 won't even fit because max capacity itself is lesser than ask requirement. Hence, Node 1 only is eligible.
// root.parent.Max set on parent, first: 6
// root.parent.child1. Guaranteed not set. 2 Allocations (belongs to single app) are running. 1st Allocation usage is first:4, pods: 1. 2nd Allocation usage is first:2, pods: 1. Total usage is first:6, pods: 2.
// root.parent.child2. Guaranteed set, first: 5. Ask of first:5 is waiting for resources.
// 2 Allocation on root.parent.child1 has been found and considered as victims and preempted to free up resources for ask.
func TestTryPreemptionWithVictimsAvailableButOnDifferentNodes(t *testing.T) {
	node1 := newNode(node1, map[string]resources.Quantity{"first": 5, "pods": 1})
	node2 := newNode("node2", map[string]resources.Quantity{"first": 4, "pods": 1})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(nil)
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "6"}, nil)
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, nil)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)
	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 4, "pods": 1}))
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "pods": 1}))
	ask2.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	alloc1 := NewAllocation("node1", ask1)
	app1.AddAllocation(alloc1)
	assert.Check(t, node1.AddAllocation(alloc1), "node alloc1 failed")
	alloc2 := NewAllocation("node2", ask2)
	app1.AddAllocation(alloc2)
	assert.Check(t, node2.AddAllocation(alloc2), "node alloc2 failed")
	assert.NilError(t, childQ1.IncAllocatedResource(ask1.GetAllocatedResource(), false))
	assert.NilError(t, childQ1.IncAllocatedResource(ask2.GetAllocatedResource(), false))
	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{
		mock.NewPreemption(true, "alloc3", "node1", []string{"alloc1"}, 0, 0),
		mock.NewPreemption(true, "alloc3", "node2", []string{"alloc2"}, 0, 0),
	}

	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	alloc, ok := preemptor.TryPreemption()
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", alloc.allocationKey, "wrong alloc")
	assert.Check(t, alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
}

// TestTryPreemptionOnQueueWithVictimsOnDifferentNodes Test try preemption on queue with simple queue hierarchy. Since Node has enough resources to accomodate, preemption happens because of queue resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue paths. victim and preemptor queue are siblings.
// Ask (Preemptor) resource type matches with all resource types of the victim. Guaranteed set only on that specific resource type.
// Setup:
// Nodes are Node1 and Node2. Node has enough space to accommodate the new ask.
// root.parent. Max set on parent, first: 18
// root.parent.child1. Guaranteed not set. 2 Allocations (belongs to single app) are running on node1 and node2. Each Allocation usage is first:5. Total usage is first:10.
// root.parent.child2. Guaranteed set 5. Ask of first:5 is waiting for resources.
// root.parent.child3. Guaranteed not set. 1 Allocation is running on node2. Total usage is first:5.
// Preemption options are 1. 2 Alloc running on Node 2 but on child 1 and child 3 queues.  2. 2 Alloc running on Node 2 and child 1 queue. 3. All three 3 allocs.
// option 1 >> option 2 >> option 3. In option 3, preempting third allocation is unnecessary, should avoid this option.
// Either option 1 or option2 is fine, but not option 3.
func TestTryPreemptionOnQueueWithVictimsOnDifferentNodes(t *testing.T) {
	t.SkipNow()
	node1 := newNode(node1, map[string]resources.Quantity{"first": 30})
	node2 := newNode("node2", map[string]resources.Quantity{"first": 30})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(nil)
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "18"}, nil)
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, nil)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "15"})
	assert.NilError(t, err)
	childQ3, err := createManagedQueueGuaranteed(parentQ, "child3", false, nil, nil)
	assert.NilError(t, err)
	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask1.createTime = time.Now().Add(-2 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask2.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	alloc1 := NewAllocation("node1", ask1)
	app1.AddAllocation(alloc1)
	assert.Check(t, node1.AddAllocation(alloc1), "node alloc1 failed")
	alloc2 := NewAllocation("node2", ask2)
	app1.AddAllocation(alloc2)
	assert.Check(t, node2.AddAllocation(alloc2), "node alloc2 failed")

	assert.NilError(t, childQ1.IncAllocatedResource(ask1.GetAllocatedResource(), false))
	assert.NilError(t, childQ1.IncAllocatedResource(ask2.GetAllocatedResource(), false))

	app3 := newApplication(appID3, "default", "root.parent.child3")
	app3.SetQueue(childQ3)
	childQ3.applications[appID3] = app3

	ask4 := newAllocationAsk("alloc4", appID3, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask4.createTime = time.Now()
	assert.NilError(t, app3.AddAllocationAsk(ask4))

	alloc4 := NewAllocation("node2", ask4)
	app3.AddAllocation(alloc4)
	assert.Check(t, node2.AddAllocation(alloc4), "node alloc2 failed")
	assert.NilError(t, childQ3.IncAllocatedResource(ask4.GetAllocatedResource(), false))

	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	allocs := map[string]string{}
	allocs["alloc3"] = "node2"

	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, nil)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	alloc, ok := preemptor.TryPreemption()
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", alloc.allocationKey, "wrong alloc")
	assert.Equal(t, "node2", alloc.nodeID, "wrong alloc")
	assert.Equal(t, "node1", alloc1.nodeID, "wrong alloc")
	assert.Equal(t, "node2", alloc2.nodeID, "wrong alloc")
	assert.Equal(t, "node2", alloc4.nodeID, "wrong alloc")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Check(t, alloc4.IsPreempted(), "alloc2 not preempted")
}

// TestTryPreemptionOnQueueWithVictimsAvailableButLowerPriority Test try preemption on queue with simple queue hierarchy. Since Node has enough resources to accomodate, preemption happens because of queue resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue paths. victim and preemptor queue are siblings.
// Ask (Preemptor) resource type matches with all resource types of the victim. Guaranteed set only on that specific resource type.
// Setup:
// Nodes are Node1 and Node2. Node has enough space to accommodate the new ask.
// root.parent. Max set on parent, first: 18
// root.parent.child1. Guaranteed not set. 2 Allocations (belongs to single app) are running on node1 and node2. Each Allocation usage is first:5. Total usage is first:10.
// root.parent.child2. Guaranteed set 5. Ask of first:5 is waiting for resources.
// root.parent.child3. Guaranteed not set. 1 Allocation is running on node2. Total usage is first:5.
// High priority ask should not be touched and remaining 2 allocs should be preempted to free up resources
func TestTryPreemptionOnQueueWithVictimsAvailableButLowerPriority(t *testing.T) {
	node1 := newNode(node1, map[string]resources.Quantity{"first": 30})
	node2 := newNode("node2", map[string]resources.Quantity{"first": 30})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(nil)
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "18"}, nil)
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, nil)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "15"})
	assert.NilError(t, err)
	childQ3, err := createManagedQueueGuaranteed(parentQ, "child3", false, nil, nil)
	assert.NilError(t, err)
	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask1.createTime = time.Now().Add(-2 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))

	// High priority ask, should not be considered as victim
	ask2 := newAllocationAskPriority("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}), 1000)
	ask2.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	alloc1 := NewAllocation("node1", ask1)
	app1.AddAllocation(alloc1)
	assert.Check(t, node1.AddAllocation(alloc1), "node alloc1 failed")
	alloc2 := NewAllocation("node2", ask2)
	app1.AddAllocation(alloc2)
	assert.Check(t, node2.AddAllocation(alloc2), "node alloc2 failed")

	assert.NilError(t, childQ1.IncAllocatedResource(ask1.GetAllocatedResource(), false))
	assert.NilError(t, childQ1.IncAllocatedResource(ask2.GetAllocatedResource(), false))

	app3 := newApplication(appID3, "default", "root.parent.child3")
	app3.SetQueue(childQ3)
	childQ3.applications[appID3] = app3

	ask4 := newAllocationAsk("alloc4", appID3, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask4.createTime = time.Now()
	assert.NilError(t, app3.AddAllocationAsk(ask4))

	alloc4 := NewAllocation("node2", ask4)
	app3.AddAllocation(alloc4)
	assert.Check(t, node2.AddAllocation(alloc4), "node alloc2 failed")
	assert.NilError(t, childQ3.IncAllocatedResource(ask4.GetAllocatedResource(), false))

	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	allocs := map[string]string{}
	allocs["alloc3"] = "node2"

	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, nil)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	alloc, ok := preemptor.TryPreemption()
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", alloc.allocationKey, "wrong alloc")
	assert.Equal(t, "node2", alloc.nodeID, "wrong alloc")
	assert.Equal(t, "node1", alloc1.nodeID, "wrong alloc")
	assert.Equal(t, "node2", alloc2.nodeID, "wrong alloc")
	assert.Equal(t, "node2", alloc4.nodeID, "wrong alloc")
	assert.Check(t, alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, !alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Check(t, alloc4.IsPreempted(), "alloc2 not preempted")
}

// TestPreemptionWithAskResTypesDifferedFromGuaranteedSetOnPreemptorSide Test try preemption with 2 level queue hierarchy.
// Guaranteed set only on preemptor queue path, but not on the victim queue path.
// Ask (Preemptor) resource type matches with one of the victim's resource types.  Still, needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node has enough space to accommodate the new ask.
// root.parent.parent1.child1. Guaranteed set on root.parent.parent1, vcores: 1. Ask of vcores: 1 is waiting for resources.
// root.parent.parent2.child2. 2 Allocations (belongs to single app) are running. Each Allocation usage is vcores:1, mem: 200. Total usage is vcores:2, mem: 400
// root.parent.parent2.child3. No usage, no guaranteed set
// 1 Allocation on root.parent.parent1.child2 should be preempted to free up resources for ask arrived in root.parent.parent1.child1.
func TestPreemptionWithAskResTypesDifferedFromGuaranteedSetOnPreemptorSide(t *testing.T) {
	t.SkipNow()
	node := newNode(node1, map[string]resources.Quantity{"vcores": 3, "mem": 400})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(nil)
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"vcores": "2"}, nil)
	assert.NilError(t, err)
	parentQ1, err := createManagedQueueGuaranteed(parentQ, "parent1", true, nil, map[string]string{"vcores": "1"})
	assert.NilError(t, err)
	parentQ2, err := createManagedQueueGuaranteed(parentQ, "parent2", true, nil, nil)
	assert.NilError(t, err)

	childQ1, err := createManagedQueueGuaranteed(parentQ1, "child1", false, nil, nil)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ2, "child2", false, nil, nil)
	assert.NilError(t, err)
	_, err = createManagedQueueGuaranteed(parentQ2, "child3", false, nil, nil)
	assert.NilError(t, err)

	app1 := newApplication(appID1, "default", "root.parent.parent2.child2")
	app1.SetQueue(childQ2)
	childQ2.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 200}))
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 200}))
	ask2.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	alloc1 := NewAllocation(node1, ask1)
	app1.AddAllocation(alloc1)
	assert.Check(t, node.AddAllocation(alloc1), "node alloc1 failed")
	alloc2 := NewAllocation(node1, ask2)
	app1.AddAllocation(alloc2)
	assert.Check(t, node.AddAllocation(alloc2), "node alloc2 failed")
	assert.NilError(t, childQ2.IncAllocatedResource(ask1.GetAllocatedResource(), false))
	assert.NilError(t, childQ2.IncAllocatedResource(ask2.GetAllocatedResource(), false))
	app2 := newApplication(appID2, "default", "root.parent.parent1.child1")
	app2.SetQueue(childQ1)
	childQ1.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	alloc, ok := preemptor.TryPreemption()
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", alloc.allocationKey, "wrong alloc")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
}

// TestPreemptionWithAskResTypesDifferedFromGuaranteedSetOnVictimAndPreemptorSides Test try preemption with 2 level queue hierarchy.
// Guaranteed set on both victim queue path and preemptor queue path.
// Ask (Preemptor) resource type matches with one of the victim's resource types.  Still, needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node has enough space to accommodate the new ask.
// root.parent.parent1.child1. Guaranteed set on parent1, vcores: 2. Ask of vcores: 1 is waiting for resources.
// root.parent.parent2.child2. Guaranteed set on parent2, vcores: 1. 6 Allocations (belongs to two diff apps) are running. 3 Allocation's usage is vcores:1, mem: 100. 3 more allocations usage is mem: 100. Total usage is vcores:3, mem: 600
// root.parent.parent2.child3. No usage, no guaranteed set
// 2 Allocations of each vcores:1, mem: 100 running on root.parent.parent1.child2 should be preempted to free up resources for ask arrived in root.parent.parent1.child1.
// 3rd allocation of vcores:1, mem: 100 should not be touched as preempting the same would make usage goes below the guaranteed set on root.parent.parent2.child2.
// All remaining three allocation of each mem: 100 should not be touched at all as there is no matching resource type between these allocs and ask resource types.
func TestPreemptionWithAskResTypesDifferedFromGuaranteedSetOnVictimAndPreemptorSides(t *testing.T) {
	t.SkipNow()
	node := newNode(node1, map[string]resources.Quantity{"vcores": 5, "mem": 700})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(nil)
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"vcores": "3"}, nil)
	assert.NilError(t, err)
	parentQ1, err := createManagedQueueGuaranteed(parentQ, "parent1", true, nil, nil)
	assert.NilError(t, err)
	parentQ2, err := createManagedQueueGuaranteed(parentQ, "parent2", true, nil, nil)
	assert.NilError(t, err)

	childQ1, err := createManagedQueueGuaranteed(parentQ1, "child1", false, nil, map[string]string{"vcores": "2"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ2, "child2", false, nil, map[string]string{"vcores": "1"})
	assert.NilError(t, err)
	_, err = createManagedQueueGuaranteed(parentQ2, "child3", false, nil, nil)
	assert.NilError(t, err)

	app1 := newApplication(appID1, "default", "root.parent.parent2.child2")
	app1.SetQueue(childQ2)
	childQ2.applications[appID1] = app1
	app2 := newApplication(appID2, "default", "root.parent.parent2.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	app3 := newApplication(appID3, "default", "root.parent.parent2.child2")
	app3.SetQueue(childQ2)
	childQ2.applications[appID3] = app3

	for i := 5; i < 8; i++ {
		askN := newAllocationAsk(alloc+strconv.Itoa(i), appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"mem": 100}))
		askN.createTime = time.Now().Add(-2 * time.Minute)
		assert.NilError(t, app1.AddAllocationAsk(askN))
		allocN := NewAllocation(node1, askN)
		app1.AddAllocation(allocN)
		assert.Check(t, node.AddAllocation(allocN), "node alloc1 failed")
	}

	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 100}))
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	ask2 := newAllocationAsk("alloc2", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 100}))
	ask2.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 100}))
	ask3.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask3))
	alloc1 := NewAllocation(node1, ask1)
	app1.AddAllocation(alloc1)
	assert.Check(t, node.AddAllocation(alloc1), "node alloc1 failed")
	alloc2 := NewAllocation(node1, ask2)
	app2.AddAllocation(alloc2)
	assert.Check(t, node.AddAllocation(alloc2), "node alloc2 failed")
	alloc3 := NewAllocation(node1, ask3)
	app3.AddAllocation(alloc3)
	assert.Check(t, node.AddAllocation(alloc3), "node alloc3 failed")

	for i := 5; i < 8; i++ {
		assert.NilError(t, childQ2.IncAllocatedResource(resources.NewResourceFromMap(map[string]resources.Quantity{"mem": 100}), false))
	}
	assert.NilError(t, childQ2.IncAllocatedResource(ask1.GetAllocatedResource(), false))
	assert.NilError(t, childQ2.IncAllocatedResource(ask2.GetAllocatedResource(), false))
	assert.NilError(t, childQ2.IncAllocatedResource(ask3.GetAllocatedResource(), false))

	app4 := newApplication("app-4", "default", "root.parent.parent1.child1")
	app4.SetQueue(childQ1)
	ask4 := newAllocationAsk("alloc4", "app-4", resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2}))
	assert.NilError(t, app4.AddAllocationAsk(ask4))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app4, headRoom, 30*time.Second, ask4, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{mock.NewPreemption(true, "alloc4", node1, []string{"alloc3", "alloc2"}, 1, 1)}
	allocs := map[string]string{}
	allocs["alloc4"] = node1
	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	alloc, ok := preemptor.TryPreemption()
	assert.NilError(t, plugin.GetPredicateError())
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc4", alloc.allocationKey, "wrong alloc")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 not preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Check(t, alloc3.IsPreempted(), "alloc3 preempted")
}

// TestPreemptionWithAskResTypesSameAsGuaranteedSetOnPreemptorSide Test try preemption with 2 level queue hierarchy.
// Guaranteed set only on preemptor queue path.
// Ask (Preemptor) resource type matches with one of the victim's resource types.  Still, needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node has enough space to accommodate the new ask.
// root.parent.parent1.child1. Guaranteed set on parent1, vcores: 2. Ask of vcores: 2, mem: 200 is waiting for resources.
// root.parent.parent2.child2. Guaranteed set on parent2, vcores: 1. 6 Allocations (belongs to two diff apps) are running. 3 Allocation's usage is vcores:1. 3 more allocations usage gpu: 100. Total usage is vcores:3, gpu: 300
// root.parent.parent2.child3. No usage, no guaranteed set
// 3 Allocations of each vcores:1 running on root.parent.parent1.child2 could be preempted to free up resources for ask arrived in root.parent.parent1.child1.
// but last allocation should not be touched as preempting the same would make usage goes above the guaranteed set on preemptor or ask queue root.parent.parent2.child1.
// All remaining three allocation of each mem: 100 should not be touched at all as there is no matching resource type between these allocs and ask resource types.
func TestPreemptionWithAskResTypesSameAsGuaranteedSetOnPreemptorSide(t *testing.T) {
	t.SkipNow()
	node := newNode(node1, map[string]resources.Quantity{"vcores": 5, "gpu": 300, "mem": 200})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(nil)
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, nil, nil)
	assert.NilError(t, err)
	parentQ1, err := createManagedQueueGuaranteed(parentQ, "parent1", true, nil, nil)
	assert.NilError(t, err)
	parentQ2, err := createManagedQueueGuaranteed(parentQ, "parent2", true, nil, nil)
	assert.NilError(t, err)

	childQ1, err := createManagedQueueGuaranteed(parentQ1, "child1", false, nil, map[string]string{"vcores": "2"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ2, "child2", false, nil, nil)
	assert.NilError(t, err)
	_, err = createManagedQueueGuaranteed(parentQ2, "child3", false, nil, nil)
	assert.NilError(t, err)

	app1 := newApplication(appID1, "default", "root.parent.parent2.child2")
	app1.SetQueue(childQ2)
	childQ2.applications[appID1] = app1
	app2 := newApplication(appID2, "default", "root.parent.parent2.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	app3 := newApplication(appID3, "default", "root.parent.parent2.child2")
	app3.SetQueue(childQ2)
	childQ2.applications[appID3] = app3

	for i := 5; i < 8; i++ {
		askN := newAllocationAsk(alloc+strconv.Itoa(i), appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100}))
		askN.createTime = time.Now().Add(-2 * time.Minute)
		assert.NilError(t, app1.AddAllocationAsk(askN))
		allocN := NewAllocation(node1, askN)
		app1.AddAllocation(allocN)
		assert.Check(t, node.AddAllocation(allocN), "node alloc1 failed")
	}

	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	ask2 := newAllocationAsk("alloc2", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	ask2.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	ask3.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask3))
	alloc1 := NewAllocation(node1, ask1)
	app1.AddAllocation(alloc1)
	assert.Check(t, node.AddAllocation(alloc1), "node alloc1 failed")
	alloc2 := NewAllocation(node1, ask2)
	app2.AddAllocation(alloc2)
	assert.Check(t, node.AddAllocation(alloc2), "node alloc2 failed")
	alloc3 := NewAllocation(node1, ask3)
	app3.AddAllocation(alloc3)
	assert.Check(t, node.AddAllocation(alloc3), "node alloc3 failed")

	for i := 5; i < 8; i++ {
		assert.NilError(t, childQ2.IncAllocatedResource(resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100}), false))
	}
	assert.NilError(t, childQ2.IncAllocatedResource(ask1.GetAllocatedResource(), false))
	assert.NilError(t, childQ2.IncAllocatedResource(ask2.GetAllocatedResource(), false))
	assert.NilError(t, childQ2.IncAllocatedResource(ask3.GetAllocatedResource(), false))

	app4 := newApplication("app-4", "default", "root.parent.parent1.child1")
	app4.SetQueue(childQ1)
	ask4 := newAllocationAsk("alloc4", "app-4", resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2, "mem": 200}))
	assert.NilError(t, app4.AddAllocationAsk(ask4))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app4, headRoom, 30*time.Second, ask4, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{mock.NewPreemption(true, "alloc4", node1, []string{"alloc3", "alloc2"}, 1, 1)}
	allocs := map[string]string{}
	allocs["alloc4"] = node1
	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	alloc, ok := preemptor.TryPreemption()
	assert.NilError(t, plugin.GetPredicateError())
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc4", alloc.allocationKey, "wrong alloc")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 not preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Check(t, alloc3.IsPreempted(), "alloc3 preempted")
}

// TestPreemptionWithAskResTypesSameAsGuaranteedSetOnVictimSide Test try preemption with 2 level queue hierarchy.
// Guaranteed set on both victim queue path and preemptor queue path.
// Ask (Preemptor) resource type matches with one of the victim's resource types.  Still, needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node has enough space to accommodate the new ask.
// root.parent.parent1.child1. Guaranteed set on parent1, vcores: 2. Ask of vcores: 2, mem: 200 is waiting for resources.
// root.parent.parent2.child2. Guaranteed set on parent2, vcores: 1. 6 Allocations (belongs to two diff apps) are running. 3 Allocation's usage is vcores:1. 3 more allocations usage gpu: 100. Total usage is vcores:3, gpu: 300
// root.parent.parent2.child3. No usage, no guaranteed set
// 2 Allocations of each vcores:1 running on root.parent.parent1.child2 could be preempted to free up resources for ask arrived in root.parent.parent1.child1.
// 3rd allocation of vcores:1 should not be touched as preempting the same would make usage goes below the guaranteed set on root.parent.parent2.child2.
// All remaining three allocation of each mem: 100 should not be touched at all as there is no matching resource type between these allocs and ask resource types.
func TestPreemptionWithAskResTypesSameAsGuaranteedSetOnVictimAndPreemptorSides(t *testing.T) {
	t.SkipNow()
	node := newNode(node1, map[string]resources.Quantity{"vcores": 5, "gpu": 700, "mem": 200})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(nil)
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, nil, nil)
	assert.NilError(t, err)
	parentQ1, err := createManagedQueueGuaranteed(parentQ, "parent1", true, nil, nil)
	assert.NilError(t, err)
	parentQ2, err := createManagedQueueGuaranteed(parentQ, "parent2", true, nil, nil)
	assert.NilError(t, err)

	childQ1, err := createManagedQueueGuaranteed(parentQ1, "child1", false, nil, map[string]string{"vcores": "2"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ2, "child2", false, nil, map[string]string{"vcores": "1"})
	assert.NilError(t, err)
	_, err = createManagedQueueGuaranteed(parentQ2, "child3", false, nil, nil)
	assert.NilError(t, err)

	app1 := newApplication(appID1, "default", "root.parent.parent2.child2")
	app1.SetQueue(childQ2)
	childQ2.applications[appID1] = app1
	app2 := newApplication(appID2, "default", "root.parent.parent2.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	app3 := newApplication(appID3, "default", "root.parent.parent2.child2")
	app3.SetQueue(childQ2)
	childQ2.applications[appID3] = app3

	for i := 5; i < 8; i++ {
		askN := newAllocationAsk(alloc+strconv.Itoa(i), appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100}))
		askN.createTime = time.Now().Add(-2 * time.Minute)
		assert.NilError(t, app1.AddAllocationAsk(askN))
		allocN := NewAllocation(node1, askN)
		app1.AddAllocation(allocN)
		assert.Check(t, node.AddAllocation(allocN), "node alloc1 failed")
	}

	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	ask2 := newAllocationAsk("alloc2", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	ask2.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	ask3.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask3))
	alloc1 := NewAllocation(node1, ask1)
	app1.AddAllocation(alloc1)
	assert.Check(t, node.AddAllocation(alloc1), "node alloc1 failed")
	alloc2 := NewAllocation(node1, ask2)
	app2.AddAllocation(alloc2)
	assert.Check(t, node.AddAllocation(alloc2), "node alloc2 failed")
	alloc3 := NewAllocation(node1, ask3)
	app3.AddAllocation(alloc3)
	assert.Check(t, node.AddAllocation(alloc3), "node alloc3 failed")

	for i := 5; i < 8; i++ {
		assert.NilError(t, childQ2.IncAllocatedResource(resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100}), false))
	}

	assert.NilError(t, childQ2.IncAllocatedResource(ask1.GetAllocatedResource(), false))
	assert.NilError(t, childQ2.IncAllocatedResource(ask2.GetAllocatedResource(), false))
	assert.NilError(t, childQ2.IncAllocatedResource(ask3.GetAllocatedResource(), false))

	app4 := newApplication("app-4", "default", "root.parent.parent1.child1")
	app4.SetQueue(childQ1)
	ask4 := newAllocationAsk("alloc4", "app-4", resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2, "mem": 200}))
	assert.NilError(t, app4.AddAllocationAsk(ask4))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app4, headRoom, 30*time.Second, ask4, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{mock.NewPreemption(true, "alloc4", node1, []string{"alloc3", "alloc2"}, 1, 1)}
	allocs := map[string]string{}
	allocs["alloc4"] = node1
	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	alloc, ok := preemptor.TryPreemption()
	assert.NilError(t, plugin.GetPredicateError())
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc4", alloc.allocationKey, "wrong alloc")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 not preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Check(t, alloc3.IsPreempted(), "alloc3 preempted")
}

func TestSolutionScoring(t *testing.T) {
	singleAlloc := scoreMap(nodeID1, []bool{false}, []bool{true})
	singleOriginator := scoreMap(nodeID1, []bool{true}, []bool{true})
	singleNoPreempt := scoreMap(nodeID1, []bool{false}, []bool{false})
	dual := scoreMap(nodeID1, []bool{true, false}, []bool{true, false})

	var none *predicateCheckResult = nil
	assert.Equal(t, scoreUnfit, none.getSolutionScore(singleAlloc), "wrong score for nil")

	missing := &predicateCheckResult{nodeID: "missing", success: true, index: 0}
	assert.Equal(t, scoreUnfit, missing.getSolutionScore(singleAlloc), "wrong score for missing")

	failure := &predicateCheckResult{nodeID: nodeID1, success: false, index: -1}
	assert.Equal(t, scoreUnfit, failure.getSolutionScore(singleAlloc), "wrong score for failure")

	overrun := &predicateCheckResult{nodeID: nodeID1, success: true, index: 1}
	assert.Equal(t, scoreUnfit, overrun.getSolutionScore(singleAlloc), "wrong score for overrun")

	noOp := &predicateCheckResult{nodeID: nodeID1, success: true, index: -1}
	assert.Equal(t, uint64(0), noOp.getSolutionScore(singleAlloc), "wrong score for noop")

	ideal := &predicateCheckResult{nodeID: nodeID1, success: true, index: 0}
	assert.Equal(t, uint64(1), ideal.getSolutionScore(singleAlloc), "wrong score for ideal")

	originator := &predicateCheckResult{nodeID: nodeID1, success: true, index: 0}
	assert.Equal(t, scoreOriginator+1, originator.getSolutionScore(singleOriginator), "wrong score for originator")

	noPreempt := &predicateCheckResult{nodeID: nodeID1, success: true, index: 0}
	assert.Equal(t, scoreNoPreempt+1, noPreempt.getSolutionScore(singleNoPreempt), "wrong score for no-preempt")

	both := &predicateCheckResult{nodeID: nodeID1, success: true, index: 1}
	assert.Equal(t, scoreOriginator+scoreNoPreempt+2, both.getSolutionScore(dual), "wrong score for both")

	assert.Check(t, noOp.betterThan(none, singleAlloc), "noop should be better than nil")
	assert.Check(t, noOp.betterThan(ideal, singleAlloc), "noop should be better than ideal")
}

func scoreMap(nodeID string, orig, self []bool) map[string][]*Allocation {
	alloc := make([]*Allocation, 0)
	for i := range orig {
		alloc = append(alloc, allocForScore(orig[i], self[i]))
	}
	return map[string][]*Allocation{nodeID: alloc}
}

func allocForScore(originator bool, allowPreemptSelf bool) *Allocation {
	ask := NewAllocationAsk("alloc1", appID1, resources.NewResource())
	ask.originator = originator
	ask.allowPreemptSelf = allowPreemptSelf
	return NewAllocation(nodeID1, ask)
}
