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
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	evtMock "github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-core/pkg/mock"
	"github.com/apache/yunikorn-core/pkg/plugins"
	schedEvt "github.com/apache/yunikorn-core/pkg/scheduler/objects/events"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const alloc = "alloc"

func creatApp1(
	childQ1 *Queue,
	node1 *Node,
	node2 *Node,
	app1Rec map[string]resources.Quantity,
) (*Allocation, *Allocation, error) {
	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1

	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(app1Rec))
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	if err := app1.AddAllocationAsk(ask1); err != nil {
		return nil, nil, err
	}
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(app1Rec))
	ask2.createTime = time.Now()
	if err := app1.AddAllocationAsk(ask2); err != nil {
		return nil, nil, err
	}

	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(app1Rec))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	if !node1.TryAddAllocation(alloc1) {
		return nil, nil, fmt.Errorf("node alloc1 failed")
	}
	var alloc2 *Allocation
	if node2 != nil {
		alloc2 = newAllocationWithKey("alloc2", appID1, nodeID2, resources.NewResourceFromMap(app1Rec))
		alloc2.createTime = ask2.createTime
		app1.AddAllocation(alloc2)
		if !node2.TryAddAllocation(alloc2) {
			return nil, nil, fmt.Errorf("node alloc2 failed")
		}
	} else {
		alloc2 = newAllocationWithKey("alloc2", appID1, nodeID1, resources.NewResourceFromMap(app1Rec))
		alloc2.createTime = ask2.createTime
		if !node1.TryAddAllocation(alloc2) {
			return nil, nil, fmt.Errorf("node alloc2 failed")
		}
	}

	if err := childQ1.TryIncAllocatedResource(ask1.GetAllocatedResource()); err != nil {
		return nil, nil, err
	}
	if err := childQ1.TryIncAllocatedResource(ask2.GetAllocatedResource()); err != nil {
		return nil, nil, err
	}

	return alloc1, alloc2, nil
}

func creatApp2(
	childQ2 *Queue,
	app2Res map[string]resources.Quantity,
	allocID string,
) (*Application, *Allocation, error) {
	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	ask3 := newAllocationAsk(allocID, appID2, resources.NewResourceFromMap(app2Res))
	if err := app2.AddAllocationAsk(ask3); err != nil {
		return nil, nil, err
	}

	return app2, ask3, nil
}

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
	getNode := func(nodeID string) *Node {
		return node
	}
	preemptionAttemptsRemaining := 1
	result := app.tryAllocate(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2}), true, 1*time.Second, &preemptionAttemptsRemaining, iterator, iterator, getNode)
	assert.Check(t, result == nil, "unexpected result")
	assertAllocationLog(t, ask)
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

	alloc1, alloc2, err := creatApp1(childQ1, node, nil, map[string]resources.Quantity{"first": 5})
	assert.NilError(t, err)
	assert.Assert(t, alloc1 != nil, "alloc1 should not be nil")
	assert.Assert(t, alloc2 != nil, "alloc2 should not be nil")

	app2, ask3, err := creatApp2(childQ2, map[string]resources.Quantity{"first": 5}, "alloc3")
	assert.NilError(t, err)

	childQ2.incPendingResource(ask3.GetAllocatedResource())
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// positive case
	assert.Assert(t, preemptor.checkPreemptionQueueGuarantees(), "queue guarantees fail")

	// verify too large of a resource will not succeed
	ask3.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 25})
	assert.Assert(t, !preemptor.checkPreemptionQueueGuarantees(), "queue guarantees did not fail")
	result, _ := preemptor.TryPreemption()
	assert.Assert(t, result == nil, "no result")
	assert.Equal(t, ask3.GetAllocationLog()[0].Message, common.PreemptionDoesNotGuarantee)
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
			parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{}, tt.parentGuaranteed)
			assert.NilError(t, err)
			childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, map[string]string{}, map[string]string{"first": "5"})
			assert.NilError(t, err)
			childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, map[string]string{}, tt.childGuaranteed)
			assert.NilError(t, err)

			alloc1, alloc2, err := creatApp1(childQ1, node, nil, map[string]resources.Quantity{"first": 5})
			assert.NilError(t, err)
			assert.Assert(t, alloc1 != nil, "alloc1 should not be nil")
			assert.Assert(t, alloc2 != nil, "alloc2 should not be nil")

			app2, ask3, err := creatApp2(childQ2, map[string]resources.Quantity{"first": 5}, "alloc3")
			assert.NilError(t, err)

			childQ2.incPendingResource(ask3.GetAllocatedResource())
			headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
			preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)
			result, ok := preemptor.TryPreemption()
			assert.Equal(t, tt.expected, ok, "unexpected resultType")
			if tt.expected {
				assert.Equal(t, result != nil, true, "unexpected resultType")
				assert.Equal(t, len(ask3.GetAllocationLog()), 0)
			} else {
				assert.Equal(t, result == nil, true, "unexpected resultType")
				assert.Equal(t, len(ask3.GetAllocationLog()), 1)
				assert.Equal(t, ask3.GetAllocationLog()[0].Message, common.PreemptionDoesNotGuarantee)
			}
		})
	}
}

func TestTryPreemption(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 10, "pods": 5})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"first": "20", "pods": "5"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "20"}, map[string]string{"first": "10"})
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
	assert.NilError(t, err)

	alloc1, alloc2, err := creatApp1(childQ1, node, nil, map[string]resources.Quantity{"first": 5, "pods": 1})
	assert.NilError(t, err)

	app2, ask3, err := creatApp2(childQ2, map[string]resources.Quantity{"first": 5, "pods": 1}, "alloc3")
	assert.NilError(t, err)
	childQ2.incPendingResource(ask3.GetAllocatedResource())

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{
		mock.NewPreemption(true, "alloc3", nodeID1, []string{"alloc1"}, 0, 0),
	}
	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.NilError(t, plugin.GetPredicateError())
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Check(t, alloc1.IsPreempted(), "alloc1 not preempted")
	assert.Check(t, !alloc2.IsPreempted(), "alloc2 preempted")
	assert.Equal(t, len(ask3.GetAllocationLog()), 0)
}

func TestTryPreemption_SendEvent(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"first": 10, "pods": 5})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"first": "20", "pods": "5"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "20"}, map[string]string{"first": "10"})
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
	assert.NilError(t, err)

	alloc1, alloc2, err := creatApp1(childQ1, node, nil, map[string]resources.Quantity{"first": 5, "pods": 1})
	assert.NilError(t, err)

	eventSystem := evtMock.NewEventSystem()
	events := schedEvt.NewAskEvents(eventSystem)
	alloc1.askEvents = events

	app2, ask3, err := creatApp2(childQ2, map[string]resources.Quantity{"first": 5, "pods": 1}, "alloc3")
	assert.NilError(t, err)
	childQ2.incPendingResource(ask3.GetAllocatedResource())

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{
		mock.NewPreemption(true, "alloc3", nodeID1, []string{"alloc1"}, 0, 0),
	}
	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.NilError(t, plugin.GetPredicateError())
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Check(t, alloc1.IsPreempted(), "alloc1 not preempted")
	assert.Check(t, !alloc2.IsPreempted(), "alloc2 preempted")
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, alloc1.applicationID, event.ReferenceID)
	assert.Equal(t, alloc1.allocationKey, event.ObjectID)
	assert.Equal(t, si.EventRecord_NONE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, si.EventRecord_REQUEST, event.Type)
	assert.Equal(t, fmt.Sprintf("Preempted by %s from application %s in %s", "alloc3", appID2, "root.parent.child2"), event.Message)
	assert.Equal(t, len(ask3.GetAllocationLog()), 0)
}

// TestTryPreemptionOnNode Test try preemption on node with simple queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue path in 2 levels. victim and preemptor queue are siblings.
// Request (Preemptor) resource type matches with all resource types of the victim. But Guaranteed set only on specific resource type. 2 Victims are available, but 1 should be preempted because further preemption would make usage go below the guaranteed quota
// Setup:
// Nodes are Node1 and Node2. Nodes are full. No space to accommodate the ask.
// root.parent. Guaranteed set on parent, first: 10
// root.parent.child1. Guaranteed set, first: 5. 2 Allocations (belongs to single app) are running. Each Allocation usage is first:5, pods: 1. Total usage is first:10, pods: 2.
// root.parent.child2. Guaranteed set, first: 5. Request of first:5 is waiting for resources.
// 1 Allocation on root.parent.child1 should be preempted to free up resources for ask arrived in root.parent.child2.
func TestTryPreemptionOnNode(t *testing.T) {
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 5, "pods": 1})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 5, "pods": 1})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(map[string]string{"first": "10", "pods": "2"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "20"}, map[string]string{"first": "10"})
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
	assert.NilError(t, err)

	alloc1, alloc2, err := creatApp1(childQ1, node1, node2, map[string]resources.Quantity{"first": 5, "pods": 1})
	assert.NilError(t, err)

	app2, ask3, err := creatApp2(childQ2, map[string]resources.Quantity{"first": 5, "pods": 1}, "alloc3")
	assert.NilError(t, err)

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{
		mock.NewPreemption(true, "alloc3", nodeID2, []string{"alloc2"}, 0, 0),
	}
	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID2, result.NodeID, "wrong node")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Equal(t, len(ask3.GetAllocationLog()), 0)
}

// TestTryPreemption_NodeWithCapacityLesserThanAsk Test try preemption on node whose capacity is lesser than ask resource requirements with simple queue hierarchy. Since Node won't accommodate the ask even after preempting all allocations, there is no use in considering the node.
// Guaranteed and Max resource set on both victim queue path and preemptor queue path in 2 levels. victim and preemptor queue are siblings.
// Request (Preemptor) resource type matches with all resource types of the victim. But Guaranteed set only on specific resource type. 2 Victims are available, but 1 should be preempted because further preemption would make usage go below the guaranteed quota
// Setup:
// Nodes are Node1 and Node2. Nodes are full. No space to accommodate the ask.
// root.parent. Guaranteed set on parent, first: 10
// root.parent.child1. Guaranteed set, first: 5. 2 Allocations (belongs to single app) are running. Each Allocation usage is first:5, pods: 1. Total usage is first:10, pods: 2.
// root.parent.child2. Guaranteed set, first: 6. Request of first:6 is waiting for resources.
// Nome of the node would be considered for preemption as ask requirements is higher than the node capacity. Hence, no results.
func TestTryPreemption_NodeWithCapacityLesserThanAsk(t *testing.T) {
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 5, "pods": 1})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 5, "pods": 1})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(map[string]string{"first": "10", "pods": "2"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "20"}, map[string]string{"first": "10"})
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, map[string]string{"first": "10"}, map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, map[string]string{"first": "10"}, map[string]string{"first": "6"})
	assert.NilError(t, err)

	alloc1, alloc2, err := creatApp1(childQ1, node1, node2, map[string]resources.Quantity{"first": 5, "pods": 1})
	assert.NilError(t, err)

	app2, ask3, err := creatApp2(childQ2, map[string]resources.Quantity{"first": 6, "pods": 1}, "alloc3")
	assert.NilError(t, err)

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)
	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result == nil, "unexpected result")
	assert.Equal(t, ok, false, "no victims found")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, !alloc2.IsPreempted(), "alloc2 preempted")
	assert.Equal(t, len(ask3.GetAllocationLog()), 0)
}

// TestTryPreemptionOnNodeWithOGParentAndUGPreemptor Test try preemption on node with simple queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue path in 2 levels. victim and preemptor queue are siblings.
// Parent is over guaranteed whereas preemptor is under guaranteed with pending pods. Parent is over guaranteed because of another child.
// Setup:
// Nodes are Node1 and Node2. Nodes are full. No space to accommodate the ask.
// root.parent. Guaranteed set on parent, first: 2. Usage is first: 6. So over guaranteed.
// root.parent.child1. No Guaranteed set. Usage is first: 6. 6 Allocations (belongs to single app) are running. Each Allocation usage is first:1. Total usage is first:6.
// root.parent.child2. Guaranteed set, first: 1. Ask of first:1 is waiting for resources.
// 1 Allocation on root.parent.child1 should be preempted to free up resources for ask arrived in root.parent.child2.
func TestTryPreemptionOnNodeWithOGParentAndUGPreemptor(t *testing.T) {
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 3, "pods": 1})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 3, "pods": 1})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(map[string]string{"first": "6", "pods": "2"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "20"}, map[string]string{"first": "2"})
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, nil)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, map[string]string{"first": "10"}, map[string]string{"first": "1"})
	assert.NilError(t, err)
	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1

	for i := 1; i <= 6; i++ {
		ask1 := newAllocationAsk("alloc"+strconv.Itoa(i), appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1}))
		ask1.createTime = time.Now().Add(time.Duration(i*-1) * time.Minute)
		assert.NilError(t, app1.AddAllocationAsk(ask1))
		if i%2 == 0 {
			alloc1 := newAllocationWithKey(ask1.allocationKey, appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1}))
			alloc1.createTime = ask1.createTime
			app1.AddAllocation(alloc1)
			assert.Check(t, node1.TryAddAllocation(alloc1), "node alloc1 failed")
			assert.NilError(t, childQ1.TryIncAllocatedResource(ask1.GetAllocatedResource()))
		} else {
			alloc1 := newAllocationWithKey(ask1.allocationKey, appID1, nodeID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1}))
			alloc1.createTime = ask1.createTime
			app1.AddAllocation(alloc1)
			assert.Check(t, node2.TryAddAllocation(alloc1), "node alloc1 failed")
			assert.NilError(t, childQ1.TryIncAllocatedResource(ask1.GetAllocatedResource()))
		}
	}

	app2, ask3, err := creatApp2(childQ2, map[string]resources.Quantity{"first": 1}, "alloc7")
	assert.NilError(t, err)

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{
		mock.NewPreemption(true, "alloc7", nodeID2, []string{"alloc1"}, 0, 0),
	}
	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc7", result.Request.allocationKey, "wrong alloc")
	assert.Equal(t, nodeID2, result.NodeID, "wrong node")
	assert.Check(t, node2.GetAllocation("alloc1").IsPreempted(), "alloc1 preempted")
	assert.Equal(t, len(ask3.GetAllocationLog()), 0)
}

// TestTryPreemptionOnQueue Test try preemption on queue with simple queue hierarchy. Since Node has enough resources to accomodate, preemption happens because of queue resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue path in 2 levels. victim and preemptor queue are siblings.
// Request (Preemptor) resource type matches with all resource types of the victim. But Guaranteed set only on specific resource type. 2 Victims are available, but 1 should be preempted because further preemption would make usage go below the guaranteed quota
// Setup:
// Nodes are Node1 and Node2. Node has enough space to accommodate the new ask.
// root.parent. Guaranteed set on parent, first: 10
// root.parent.child1. Guaranteed set, first: 5. 2 Allocations (belongs to single app) are running. Each Allocation usage is first:5, pods: 1. Total usage is first:10, pods: 2.
// root.parent.child2. Guaranteed set, first: 5. Request of first:5 is waiting for resources.
// 1 Allocation on root.parent.child1 should be preempted to free up resources for ask arrived in root.parent.child2.
func TestTryPreemptionOnQueue(t *testing.T) {
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 10, "pods": 2})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 10, "pods": 2})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(map[string]string{"first": "20", "pods": "4"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "10"}, nil)
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)

	alloc1, alloc2, err := creatApp1(childQ1, node1, node2, map[string]resources.Quantity{"first": 5, "pods": 1})
	assert.NilError(t, err)

	app2, ask3, err := creatApp2(childQ2, map[string]resources.Quantity{"first": 5, "pods": 1}, "alloc3")
	assert.NilError(t, err)

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	allocs := map[string]string{}
	allocs["alloc3"] = nodeID2

	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, nil)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID2, result.NodeID, "wrong node")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Equal(t, len(ask3.GetAllocationLog()), 0)
}

// TestTryPreemption_VictimsAvailable_InsufficientResource Test try preemption on queue with simple queue hierarchy. Since Node has enough resources to accomodate, preemption happens because of queue resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue path. victim and preemptor queue are siblings.
// Request (Preemptor) resource type matches with 1 resource type of the victim. Guaranteed also set on specific resource type. 2 Victims are available, but total resource usage is lesser than ask requirement.
// Setup:
// Nodes are Node1 and Node2. Node has enough space to accommodate the new ask.
// root.parent.Max set on parent, first: 8
// root.parent.child1. Guaranteed not set. 2 Allocations (belongs to single app) are running. Each Allocation usage is first:2, pods: 1. Total usage is first:4, pods: 2.
// root.parent.child2. Guaranteed set, first: 5. Request of first:5 is waiting for resources.
// 2 Allocation on root.parent.child1 has been found and considered as victims. Since victims total resource usage (first: 4) is lesser than ask requirment (first: 5), preemption won't help. Hence, victims are dropped.
func TestTryPreemption_VictimsAvailable_InsufficientResource(t *testing.T) {
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 10, "pods": 2})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 10, "pods": 2})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(map[string]string{"first": "20", "pods": "4"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "8"}, nil)
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, nil)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)

	alloc1, alloc2, err := creatApp1(childQ1, node1, node2, map[string]resources.Quantity{"first": 2, "pods": 1})
	assert.NilError(t, err)

	app2, ask3, err := creatApp2(childQ2, map[string]resources.Quantity{"first": 5}, "alloc3")
	assert.NilError(t, err)

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result == nil, "unexpected result")
	assert.Equal(t, ok, false, "no victims found")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, !alloc2.IsPreempted(), "alloc2 preempted")
	log := ask3.GetAllocationLog()
	assert.Equal(t, log[0].Message, common.PreemptionShortfall)
}

// TestTryPreemption_VictimsOnDifferentNodes_InsufficientResource Test try preemption on queue with simple queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue path. victim and preemptor queue are siblings.
// Request (Preemptor) resource type matches with 1 resource type of the victim. Guaranteed also set on specific resource type. 2 Victims are available, but total resource usage is lesser than ask requirement.
// Setup:
// Nodes are Node1 and Node2. Both Nodes are almost full. No space to accommodate the ask.
// root.parent.Max set on parent, first: 6
// root.parent.child1. Guaranteed not set. 2 Allocations (belongs to single app) are running. Each Allocation usage is first:2, pods: 1. Total usage is first:4, pods: 2.
// root.parent.child2. Guaranteed set, first: 5. Request of first:5 is waiting for resources.
// 2 Allocation on root.parent.child1 has been found and considered as victims. Since victims total resource usage (first: 4) is lesser than ask requirment (first: 5), preemption won't help. Hence, victims are dropped.
func TestTryPreemption_VictimsOnDifferentNodes_InsufficientResource(t *testing.T) {
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 5, "pods": 1})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 5, "pods": 1})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(map[string]string{"first": "10", "pods": "2"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "6"}, nil)
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, nil)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "5"})
	assert.NilError(t, err)

	alloc1, alloc2, err := creatApp1(childQ1, node1, node2, map[string]resources.Quantity{"first": 2, "pods": 1})
	assert.NilError(t, err)

	app2, ask3, err := creatApp2(childQ2, map[string]resources.Quantity{"first": 5}, "alloc3")
	assert.NilError(t, err)

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{
		mock.NewPreemption(true, "alloc3", nodeID1, []string{"alloc1"}, 0, 0),
		mock.NewPreemption(true, "alloc3", nodeID2, []string{"alloc2"}, 0, 0),
	}

	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result == nil, "unexpected result")
	assert.Equal(t, ok, false, "no victims found")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, !alloc2.IsPreempted(), "alloc2 preempted")
	log := ask3.GetAllocationLog()
	assert.Equal(t, log[0].Message, common.PreemptionShortfall)
}

// TestTryPreemption_VictimsAvailableOnDifferentNodes Test try preemption on queue with simple queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue path. victim and preemptor queue are siblings.
// Request (Preemptor) resource type matches with 1 resource type of the victim. Guaranteed also set on specific resource type. 2 Victims are available, but total resource usage is lesser than ask requirement.
// Setup:
// Nodes are Node1 and Node2. Both Nodes are almost full. No space to accommodate the ask. Node 2 won't even fit because max capacity itself is lesser than ask requirement. Hence, Node 1 only is eligible.
// root.parent.Max set on parent, first: 6
// root.parent.child1. Guaranteed not set. 2 Allocations (belongs to single app) are running. 1st Allocation usage is first:4, pods: 1. 2nd Allocation usage is first:2, pods: 1. Total usage is first:6, pods: 2.
// root.parent.child2. Guaranteed set, first: 5. Request of first:5 is waiting for resources.
// 2 Allocation on root.parent.child1 has been found and considered as victims and preempted to free up resources for ask.
func TestTryPreemption_VictimsAvailableOnDifferentNodes(t *testing.T) {
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 5, "pods": 1})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 4, "pods": 1})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(map[string]string{"first": "9", "pods": "2"})
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
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 4, "pods": 1}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node1.TryAddAllocation(alloc1), "node alloc1 failed")
	alloc2 := newAllocationWithKey("alloc2", appID1, nodeID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "pods": 1}))
	alloc2.createTime = ask2.createTime
	app1.AddAllocation(alloc2)
	assert.Check(t, node2.TryAddAllocation(alloc2), "node alloc2 failed")
	assert.NilError(t, childQ1.TryIncAllocatedResource(ask1.GetAllocatedResource()))
	assert.NilError(t, childQ1.TryIncAllocatedResource(ask2.GetAllocatedResource()))

	app2, ask3, err := creatApp2(childQ2, map[string]resources.Quantity{"first": 5}, "alloc3")
	assert.NilError(t, err)

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{
		mock.NewPreemption(true, "alloc3", nodeID1, []string{"alloc1"}, 0, 0),
		mock.NewPreemption(true, "alloc3", nodeID2, []string{"alloc2"}, 0, 0),
	}
	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result == nil, "unexpected result")
	assert.Equal(t, ok, false, "no victims found")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, !alloc2.IsPreempted(), "alloc2 preempted")
	log := ask3.GetAllocationLog()
	assert.Equal(t, log[0].Message, common.PreemptionShortfall)
}

// TestTryPreemption_OnQueue_VictimsOnDifferentNodes Test try preemption on queue with simple queue hierarchy. Since Node has enough resources to accomodate, preemption happens because of queue resource constraint.xw
// Guaranteed and Max resource set on both victim queue path and preemptor queue paths. victim and preemptor queue are siblings.
// Request (Preemptor) resource type matches with all resource types of the victim. Guaranteed set only on that specific resource type.
// Setup:
// Nodes are Node1 and Node2. Node has enough space to accommodate the new ask.
// root.parent. Max set on parent, first: 18
// root.parent.child1. Guaranteed not set. 2 Allocations (belongs to single app) are running on node1 and node2. Each Allocation usage is first:5. Total usage is first:10.
// root.parent.child2. Guaranteed set 5. Request of first:5 is waiting for resources.
// root.parent.child3. Guaranteed not set. 1 Allocation is running on node2. Total usage is first:5.
// Preemption options are 1. 2 Alloc running on Node 2 but on child 1 and child 3 queues.  2. 2 Alloc running on Node 2 and child 1 queue. 3. All three 3 allocs.
// option 1 >> option 2 >> option 3. In option 3, preempting third allocation is unnecessary, should avoid this option.
// Either option 1 or option2 is fine, but not option 3.
func TestTryPreemption_OnQueue_VictimsOnDifferentNodes(t *testing.T) {
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 30})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 30})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(map[string]string{"first": "60"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"first": "18"}, nil)
	assert.NilError(t, err)
	childQ1, err := createManagedQueueGuaranteed(parentQ, "child1", false, nil, nil)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ, "child2", false, nil, map[string]string{"first": "15"})
	assert.NilError(t, err)
	childQ3, err := createManagedQueueGuaranteed(parentQ, "child3", false, nil, nil)
	assert.NilError(t, err)

	alloc1, alloc2, err := creatApp1(childQ1, node1, node2, map[string]resources.Quantity{"first": 5})
	assert.NilError(t, err)

	app3 := newApplication(appID3, "default", "root.parent.child3")
	app3.SetQueue(childQ3)
	childQ3.applications[appID3] = app3

	ask4 := newAllocationAsk("alloc4", appID3, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask4.createTime = time.Now()
	assert.NilError(t, app3.AddAllocationAsk(ask4))

	alloc4 := newAllocationWithKey("alloc4", appID3, nodeID2, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	alloc4.createTime = ask4.createTime
	app3.AddAllocation(alloc4)
	assert.Check(t, node2.TryAddAllocation(alloc4), "node alloc2 failed")
	assert.NilError(t, childQ3.TryIncAllocatedResource(ask4.GetAllocatedResource()))

	app2, ask3, err := creatApp2(childQ2, map[string]resources.Quantity{"first": 10}, "alloc3")
	assert.NilError(t, err)

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	allocs := map[string]string{}
	allocs["alloc3"] = nodeID2

	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, nil)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID2, result.NodeID, "wrong alloc")
	assert.Equal(t, nodeID1, alloc1.nodeID, "wrong node")
	assert.Equal(t, nodeID2, alloc2.nodeID, "wrong node")
	assert.Equal(t, nodeID2, alloc4.nodeID, "wrong node")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Check(t, alloc4.IsPreempted(), "alloc2 not preempted")
	assert.Equal(t, len(ask3.GetAllocationLog()), 0)
}

// TestTryPreemption_OnQueue_VictimsAvailable_LowerPriority Test try preemption on queue with simple queue hierarchy. Since Node has enough resources to accomodate, preemption happens because of queue resource constraint.
// Guaranteed and Max resource set on both victim queue path and preemptor queue paths. victim and preemptor queue are siblings.
// Request (Preemptor) resource type matches with all resource types of the victim. Guaranteed set only on that specific resource type.
// Setup:
// Nodes are Node1 and Node2. Node has enough space to accommodate the new ask.
// root.parent. Max set on parent, first: 18
// root.parent.child1. Guaranteed not set. 2 Allocations (belongs to single app) are running on node1 and node2. Each Allocation usage is first:5. Total usage is first:10.
// root.parent.child2. Guaranteed set 5. Request of first:5 is waiting for resources.
// root.parent.child3. Guaranteed not set. 1 Allocation is running on node2. Total usage is first:5.
// High priority ask should not be touched and remaining 2 allocs should be preempted to free up resources
func TestTryPreemption_OnQueue_VictimsAvailable_LowerPriority(t *testing.T) {
	node1 := newNode(nodeID1, map[string]resources.Quantity{"first": 30})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"first": 30})
	iterator := getNodeIteratorFn(node1, node2)
	rootQ, err := createRootQueue(map[string]string{"first": "60"})
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
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask2.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node1.TryAddAllocation(alloc1), "node alloc1 failed")
	alloc2 := newAllocationWithKey("alloc2", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	alloc2.createTime = ask2.createTime
	app1.AddAllocation(alloc2)
	assert.Check(t, node1.TryAddAllocation(alloc2), "node alloc2 failed")

	assert.NilError(t, childQ1.TryIncAllocatedResource(ask1.GetAllocatedResource()))
	assert.NilError(t, childQ1.TryIncAllocatedResource(ask2.GetAllocatedResource()))

	app3 := newApplication(appID3, "default", "root.parent.child3")
	app3.SetQueue(childQ3)
	childQ3.applications[appID3] = app3

	ask4 := newAllocationAskPriority("alloc4", appID3, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}), 1000)
	ask4.createTime = time.Now()
	assert.NilError(t, app3.AddAllocationAsk(ask4))

	alloc4 := newAllocationAll("alloc4", appID3, nodeID2, "", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}), false, 1000)
	alloc4.createTime = ask4.createTime
	app3.AddAllocation(alloc4)
	assert.Check(t, node2.TryAddAllocation(alloc4), "node alloc2 failed")
	assert.NilError(t, childQ3.TryIncAllocatedResource(ask4.GetAllocatedResource()))

	app2, ask3, err := creatApp2(childQ2, map[string]resources.Quantity{"first": 10}, "alloc3")
	assert.NilError(t, err)

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	allocs := map[string]string{}
	allocs["alloc3"] = nodeID1

	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, nil)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc1.nodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc2.nodeID, "wrong node")
	assert.Equal(t, nodeID2, alloc4.nodeID, "wrong node")
	assert.Check(t, alloc1.IsPreempted(), "alloc1 not preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Check(t, !alloc4.IsPreempted(), "alloc4 preempted")
	assert.Equal(t, len(ask3.GetAllocationLog()), 0)
}

// TestTryPreemption_AskResTypesDifferent_GuaranteedSetOnPreemptorSide Test try preemption with 2 level queue hierarchy.
// Guaranteed set only on preemptor queue path, but not on the victim queue path.
// Request (Preemptor) resource type matches with one of the victim's resource types.  Still, needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node has enough space to accommodate the new ask.
// root.parent.parent1.child1. Guaranteed set on root.parent.parent1, vcores: 1. Request of vcores: 1 is waiting for resources.
// root.parent.parent2.child2. 2 Allocations (belongs to single app) are running. Each Allocation usage is vcores:1, mem: 200. Total usage is vcores:2, mem: 400
// root.parent.parent2.child3. No usage, no guaranteed set
// 1 Allocation on root.parent.parent1.child2 should be preempted to free up resources for ask arrived in root.parent.parent1.child1.
func TestTryPreemption_AskResTypesDifferent_GuaranteedSetOnPreemptorSide(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"vcores": 3, "mem": 400})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"vcores": "3", "mem": "400"})
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
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 200}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node.TryAddAllocation(alloc1), "node alloc1 failed")
	alloc2 := newAllocationWithKey("alloc2", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 200}))
	alloc2.createTime = ask2.createTime
	app1.AddAllocation(alloc2)
	assert.Check(t, node.TryAddAllocation(alloc2), "node alloc2 failed")
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask1.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask2.GetAllocatedResource()))
	app2 := newApplication(appID2, "default", "root.parent.parent1.child1")
	app2.SetQueue(childQ1)
	childQ1.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	allocs := map[string]string{}
	allocs["alloc3"] = nodeID1

	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, nil)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc2.nodeID, "wrong node")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Equal(t, len(ask3.GetAllocationLog()), 0)
}

// TestTryPreemption_OnNode_AskResTypesDifferent_GuaranteedSetOnPreemptorSide Test try preemption with 2 level queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Guaranteed set only on preemptor queue path, but not on the victim queue path.
// Request (Preemptor) resource type matches with one of the victim's resource types.  Still, needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node is full, doesn't enough space to accommodate the ask.
// root.parent.parent1.child1. Guaranteed set on root.parent.parent1, vcores: 1. Request of vcores: 1 is waiting for resources.
// root.parent.parent2.child2. 2 Allocations (belongs to single app) are running. Each Allocation usage is vcores:1, mem: 200. Total usage is vcores:2, mem: 400
// root.parent.parent2.child3. No usage, no guaranteed set
// 1 Allocation on root.parent.parent1.child2 should be preempted to free up resources for ask arrived in root.parent.parent1.child1.
func TestTryPreemption_OnNode_AskResTypesDifferent_GuaranteedSetOnPreemptorSide(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"vcores": 2, "mem": 400})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"vcores": "2", "mem": "400"})
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
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 200}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node.TryAddAllocation(alloc1), "node alloc1 failed")
	alloc2 := newAllocationWithKey("alloc2", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 200}))
	alloc2.createTime = ask2.createTime
	app1.AddAllocation(alloc2)
	assert.Check(t, node.TryAddAllocation(alloc2), "node alloc2 failed")
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask1.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask2.GetAllocatedResource()))
	app2 := newApplication(appID2, "default", "root.parent.parent1.child1")
	app2.SetQueue(childQ1)
	childQ1.applications[appID2] = app2
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{mock.NewPreemption(true, "alloc3", nodeID1, []string{"alloc2"}, 0, 0)}
	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc2.nodeID, "wrong node")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Equal(t, len(ask3.GetAllocationLog()), 0)
}

// TestTryPreemption_AskResTypesDifferent_GuaranteedSetOnVictimAndPreemptorSides Test try preemption with 2 level queue hierarchy.
// Guaranteed set on both victim queue path and preemptor queue path.
// Request (Preemptor) resource type matches with one of the victim's resource types.  Still, needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node has enough space to accommodate the new ask.
// root.parent.parent1.child1. Guaranteed set on parent1, vcores: 2. Request of vcores: 1 is waiting for resources.
// root.parent.parent2.child2. Guaranteed set on parent2, vcores: 1. 6 Allocations (belongs to two diff apps) are running. 3 Allocation's usage is vcores:1, mem: 100. 3 more allocations usage is mem: 100. Total usage is vcores:3, mem: 600
// root.parent.parent2.child3. No usage, no guaranteed set
// 2 Allocations of each vcores:1, mem: 100 running on root.parent.parent1.child2 should be preempted to free up resources for ask arrived in root.parent.parent1.child1.
// 3rd allocation of vcores:1, mem: 100 should not be touched as preempting the same would make usage goes below the guaranteed set on root.parent.parent2.child2.
// All remaining three allocation of each mem: 100 should not be touched at all as there is no matching resource type between these allocs and ask resource types.
//
//nolint:funlen
func TestTryPreemption_AskResTypesDifferent_GuaranteedSetOnVictimAndPreemptorSides(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"vcores": 5, "mem": 700})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"vcores": "5", "mem": "700"})
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
	app1, app2, app3 := createVictimApplications(childQ2)
	for i := 5; i < 8; i++ {
		askN := newAllocationAsk(alloc+strconv.Itoa(i), appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"mem": 100}))
		askN.createTime = time.Now().Add(-2 * time.Minute)
		assert.NilError(t, app1.AddAllocationAsk(askN))
		allocN := newAllocationWithKey(askN.allocationKey, appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"mem": 100}))
		allocN.createTime = askN.createTime
		app1.AddAllocation(allocN)
		assert.Check(t, node.TryAddAllocation(allocN), "node alloc1 failed")
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
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 100}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node.TryAddAllocation(alloc1), "node alloc1 failed")
	alloc2 := newAllocationWithKey("alloc2", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 100}))
	alloc2.createTime = ask2.createTime
	app2.AddAllocation(alloc2)
	assert.Check(t, node.TryAddAllocation(alloc2), "node alloc2 failed")
	alloc3 := newAllocationWithKey("alloc3", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 100}))
	alloc3.createTime = ask3.createTime
	app3.AddAllocation(alloc3)
	assert.Check(t, node.TryAddAllocation(alloc3), "node alloc3 failed")

	for i := 5; i < 8; i++ {
		assert.NilError(t, childQ2.TryIncAllocatedResource(resources.NewResourceFromMap(map[string]resources.Quantity{"mem": 100})))
	}
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask1.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask2.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask3.GetAllocatedResource()))

	app4 := newApplication("app-4", "default", "root.parent.parent1.child1")
	app4.SetQueue(childQ1)
	ask4 := newAllocationAsk("alloc4", "app-4", resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2}))
	assert.NilError(t, app4.AddAllocationAsk(ask4))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app4, headRoom, 30*time.Second, ask4, iterator(), false)

	// register predicate handler
	allocs := map[string]string{}
	allocs["alloc4"] = nodeID1
	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, nil)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.NilError(t, plugin.GetPredicateError())
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc4", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc2.nodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc3.nodeID, "wrong node")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Check(t, alloc3.IsPreempted(), "alloc3 not preempted")
	assert.Equal(t, len(ask4.GetAllocationLog()), 0)
}

// TestTryPreemption_OnNode_AskResTypesDifferent_GuaranteedSetOnVictimAndPreemptorSides Test try preemption with 2 level queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Guaranteed set on both victim queue path and preemptor queue path.
// Request (Preemptor) resource type matches with one of the victim's resource types.  Still, needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node doesn't have space. Not able to accommodate the ask.
// root.parent.parent1.child1. Guaranteed set on parent1, vcores: 2. Request of vcores: 1 is waiting for resources.
// root.parent.parent2.child2. Guaranteed set on parent2, vcores: 1. 6 Allocations (belongs to two diff apps) are running. 3 Allocation's usage is vcores:1, mem: 100. 3 more allocations usage is mem: 100. Total usage is vcores:3, mem: 600
// root.parent.parent2.child3. No usage, no guaranteed set
// 2 Allocations of each vcores:1, mem: 100 running on root.parent.parent1.child2 should be preempted to free up resources for ask arrived in root.parent.parent1.child1.
// 3rd allocation of vcores:1, mem: 100 should not be touched as preempting the same would make usage goes below the guaranteed set on root.parent.parent2.child2.
// All remaining three allocation of each mem: 100 should not be touched at all as there is no matching resource type between these allocs and ask resource types.
func TestTryPreemption_OnNode_AskResTypesDifferent_GuaranteedSetOnVictimAndPreemptorSides(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"vcores": 3, "mem": 600})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"vcores": "3", "mem": "600"})
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
	app1, app2, app3 := createVictimApplications(childQ2)
	for i := 5; i < 8; i++ {
		askN := newAllocationAsk(alloc+strconv.Itoa(i), appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"mem": 100}))
		askN.createTime = time.Now().Add(-2 * time.Minute)
		assert.NilError(t, app1.AddAllocationAsk(askN))
		allocN := newAllocationWithKey(askN.allocationKey, appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"mem": 100}))
		allocN.createTime = askN.createTime
		app1.AddAllocation(allocN)
		assert.Check(t, node.TryAddAllocation(allocN), "node alloc1 failed")
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
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 100}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node.TryAddAllocation(alloc1), "node alloc1 failed")
	alloc2 := newAllocationWithKey("alloc2", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 100}))
	alloc2.createTime = ask2.createTime
	app2.AddAllocation(alloc2)
	assert.Check(t, node.TryAddAllocation(alloc2), "node alloc2 failed")
	alloc3 := newAllocationWithKey("alloc3", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1, "mem": 100}))
	alloc3.createTime = ask3.createTime
	app3.AddAllocation(alloc3)
	assert.Check(t, node.TryAddAllocation(alloc3), "node alloc3 failed")

	for i := 5; i < 8; i++ {
		assert.NilError(t, childQ2.TryIncAllocatedResource(resources.NewResourceFromMap(map[string]resources.Quantity{"mem": 100})))
	}
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask1.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask2.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask3.GetAllocatedResource()))

	app4 := newApplication("app-4", "default", "root.parent.parent1.child1")
	app4.SetQueue(childQ1)
	ask4 := newAllocationAsk("alloc4", "app-4", resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2}))
	assert.NilError(t, app4.AddAllocationAsk(ask4))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app4, headRoom, 30*time.Second, ask4, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{mock.NewPreemption(true, "alloc4", nodeID1, []string{"alloc3", "alloc2"}, 1, 1)}
	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.NilError(t, plugin.GetPredicateError())
	assert.Assert(t, result != nil, "no result")
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc4", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc2.nodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc3.nodeID, "wrong node")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Check(t, alloc3.IsPreempted(), "alloc3 not preempted")
	assert.Equal(t, len(ask4.GetAllocationLog()), 0)
}

func createVictimApplications(childQ2 *Queue) (*Application, *Application, *Application) {
	app1 := newApplication(appID1, "default", "root.parent.parent2.child2")
	app1.SetQueue(childQ2)
	childQ2.applications[appID1] = app1
	app2 := newApplication(appID2, "default", "root.parent.parent2.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2
	app3 := newApplication(appID3, "default", "root.parent.parent2.child2")
	app3.SetQueue(childQ2)
	childQ2.applications[appID3] = app3
	return app1, app2, app3
}

// TestTryPreemption_AskResTypesSame_GuaranteedSetOnPreemptorSide Test try preemption with 2 level queue hierarchy.  Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Guaranteed set only on preemptor queue path.
// Request (Preemptor) resource type matches with one of the victim's resource types.  Still, needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node has enough space to accommodate the new ask.
// root.parent.parent1.child1. Guaranteed set on parent1, vcores: 2. Request of vcores: 2, mem: 200 is waiting for resources.
// root.parent.parent2.child2. Guaranteed set on parent2, vcores: 1. 6 Allocations (belongs to two diff apps) are running. 3 Allocation's usage is vcores:1. 3 more allocations usage gpu: 100. Total usage is vcores:3, gpu: 300
// root.parent.parent2.child3. No usage, no guaranteed set
// 3 Allocations of each vcores:1 running on root.parent.parent1.child2 could be preempted to free up resources for ask arrived in root.parent.parent1.child1.
// but last allocation should not be touched as preempting the same would make usage goes above the guaranteed set on preemptor or ask queue root.parent.parent2.child1.
// All remaining three allocation of each mem: 100 should not be touched at all as there is no matching resource type between these allocs and ask resource types.
//
//nolint:funlen
func TestTryPreemption_AskResTypesSame_GuaranteedSetOnPreemptorSide(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"vcores": 5, "gpu": 300, "mem": 200})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"vcores": "5", "gpu": "300", "mem": "200"})
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
	app1, app2, app3 := createVictimApplications(childQ2)
	for i := 5; i < 8; i++ {
		askN := newAllocationAsk(alloc+strconv.Itoa(i), appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100}))
		askN.createTime = time.Now().Add(-2 * time.Minute)
		assert.NilError(t, app1.AddAllocationAsk(askN))
		allocN := newAllocationWithKey(askN.allocationKey, appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100}))
		allocN.createTime = askN.createTime
		app1.AddAllocation(allocN)
		assert.Check(t, node.TryAddAllocation(allocN), "node alloc1 failed")
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
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node.TryAddAllocation(alloc1), "node alloc1 failed")
	alloc2 := newAllocationWithKey("alloc2", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc2.createTime = ask2.createTime
	app2.AddAllocation(alloc2)
	assert.Check(t, node.TryAddAllocation(alloc2), "node alloc2 failed")
	alloc3 := newAllocationWithKey("alloc3", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc3.createTime = ask3.createTime
	app3.AddAllocation(alloc3)
	assert.Check(t, node.TryAddAllocation(alloc3), "node alloc3 failed")

	for i := 5; i < 8; i++ {
		assert.NilError(t, childQ2.TryIncAllocatedResource(resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100})))
	}
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask1.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask2.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask3.GetAllocatedResource()))

	app4 := newApplication("app-4", "default", "root.parent.parent1.child1")
	app4.SetQueue(childQ1)
	ask4 := newAllocationAsk("alloc4", "app-4", resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2, "mem": 200}))
	assert.NilError(t, app4.AddAllocationAsk(ask4))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app4, headRoom, 30*time.Second, ask4, iterator(), false)

	// register predicate handler
	allocs := map[string]string{}
	allocs["alloc4"] = nodeID1
	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, nil)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.NilError(t, plugin.GetPredicateError())
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc4", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc2.nodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc3.nodeID, "wrong node")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Check(t, alloc3.IsPreempted(), "alloc3 not preempted")
	assert.Equal(t, len(ask4.GetAllocationLog()), 0)
}

// TestTryPreemption_OnNode_AskResTypesSame_GuaranteedSetOnPreemptorSide Test try preemption with 2 level queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Guaranteed set only on preemptor queue path.
// Request (Preemptor) resource type matches with one of the victim's resource types.  Still, needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node is full. Doesn't have space to accomodate the ask.
// root.parent.parent1.child1. Guaranteed set on parent1, vcores: 2. Request of vcores: 2, mem: 200 is waiting for resources.
// root.parent.parent2.child2. Guaranteed set on parent2, vcores: 1. 6 Allocations (belongs to two diff apps) are running. 3 Allocation's usage is vcores:1. 3 more allocations usage gpu: 100. Total usage is vcores:3, gpu: 300
// root.parent.parent2.child3. No usage, no guaranteed set
// 3 Allocations of each vcores:1 running on root.parent.parent1.child2 could be preempted to free up resources for ask arrived in root.parent.parent1.child1.
// but last allocation should not be touched as preempting the same would make usage goes above the guaranteed set on preemptor or ask queue root.parent.parent2.child1.
// All remaining three allocation of each mem: 100 should not be touched at all as there is no matching resource type between these allocs and ask resource types.
func TestTryPreemption_OnNode_AskResTypesSame_GuaranteedSetOnPreemptorSide(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"vcores": 3, "gpu": 300, "mem": 200})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"vcores": "3", "gpu": "300", "mem": "200"})
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
	app1, app2, app3 := createVictimApplications(childQ2)
	for i := 5; i < 8; i++ {
		askN := newAllocationAsk(alloc+strconv.Itoa(i), appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100}))
		askN.createTime = time.Now().Add(-2 * time.Minute)
		assert.NilError(t, app1.AddAllocationAsk(askN))
		allocN := newAllocationWithKey(askN.allocationKey, appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100}))
		allocN.createTime = askN.createTime
		app1.AddAllocation(allocN)
		assert.Check(t, node.TryAddAllocation(allocN), "node alloc1 failed")
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
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node.TryAddAllocation(alloc1), "node alloc1 failed")
	alloc2 := newAllocationWithKey("alloc2", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc2.createTime = ask2.createTime
	app2.AddAllocation(alloc2)
	assert.Check(t, node.TryAddAllocation(alloc2), "node alloc2 failed")
	alloc3 := newAllocationWithKey("alloc3", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc3.createTime = ask3.createTime
	app3.AddAllocation(alloc3)
	assert.Check(t, node.TryAddAllocation(alloc3), "node alloc3 failed")

	for i := 5; i < 8; i++ {
		assert.NilError(t, childQ2.TryIncAllocatedResource(resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100})))
	}
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask1.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask2.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask3.GetAllocatedResource()))

	app4 := newApplication("app-4", "default", "root.parent.parent1.child1")
	app4.SetQueue(childQ1)
	ask4 := newAllocationAsk("alloc4", "app-4", resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2, "mem": 200}))
	assert.NilError(t, app4.AddAllocationAsk(ask4))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app4, headRoom, 30*time.Second, ask4, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{mock.NewPreemption(true, "alloc4", nodeID1, []string{"alloc3", "alloc2"}, 1, 1)}
	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.NilError(t, plugin.GetPredicateError())
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc4", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc2.nodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc3.nodeID, "wrong node")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Check(t, alloc3.IsPreempted(), "alloc3 not preempted")
	assert.Equal(t, len(ask4.GetAllocationLog()), 0)
}

// TestTryPreemption_AskResTypesSame_GuaranteedSetOnVictimAndPreemptorSides Test try preemption with 2 level queue hierarchy.
// Guaranteed set on both victim queue path and preemptor queue path.
// Request (Preemptor) resource type matches with one of the victim's resource types.  Still, needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node has enough space to accommodate the new ask.
// root.parent.parent1.child1. Guaranteed set on parent1, vcores: 2. Request of vcores: 2, mem: 200 is waiting for resources.
// root.parent.parent2.child2. Guaranteed set on parent2, vcores: 1. 6 Allocations (belongs to two diff apps) are running. 3 Allocation's usage is vcores:1. 3 more allocations usage gpu: 100. Total usage is vcores:3, gpu: 300
// root.parent.parent2.child3. No usage, no guaranteed set
// 2 Allocations of each vcores:1 running on root.parent.parent1.child2 could be preempted to free up resources for ask arrived in root.parent.parent1.child1.
// 3rd allocation of vcores:1 should not be touched as preempting the same would make usage goes below the guaranteed set on root.parent.parent2.child2.
// All remaining three allocation of each mem: 100 should not be touched at all as there is no matching resource type between these allocs and ask resource types.
//
//nolint:funlen
func TestTryPreemption_AskResTypesSame_GuaranteedSetOnVictimAndPreemptorSides(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"vcores": 5, "gpu": 700, "mem": 200})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"vcores": "5", "gpu": "700", "mem": "200"})
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
	app1, app2, app3 := createVictimApplications(childQ2)
	for i := 5; i < 8; i++ {
		askN := newAllocationAsk(alloc+strconv.Itoa(i), appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100}))
		askN.createTime = time.Now().Add(-2 * time.Minute)
		assert.NilError(t, app1.AddAllocationAsk(askN))
		allocN := newAllocationWithKey(askN.allocationKey, appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100}))
		allocN.createTime = askN.createTime
		app1.AddAllocation(allocN)
		assert.Check(t, node.TryAddAllocation(allocN), "node alloc1 failed")
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
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node.TryAddAllocation(alloc1), "node alloc1 failed")
	alloc2 := newAllocationWithKey("alloc2", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc2.createTime = ask2.createTime
	app2.AddAllocation(alloc2)
	assert.Check(t, node.TryAddAllocation(alloc2), "node alloc2 failed")
	alloc3 := newAllocationWithKey("alloc3", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc3.createTime = ask3.createTime
	app3.AddAllocation(alloc3)
	assert.Check(t, node.TryAddAllocation(alloc3), "node alloc3 failed")

	for i := 5; i < 8; i++ {
		assert.NilError(t, childQ2.TryIncAllocatedResource(resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100})))
	}

	assert.NilError(t, childQ2.TryIncAllocatedResource(ask1.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask2.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask3.GetAllocatedResource()))

	app4 := newApplication("app-4", "default", "root.parent.parent1.child1")
	app4.SetQueue(childQ1)
	ask4 := newAllocationAsk("alloc4", "app-4", resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2, "mem": 200}))
	assert.NilError(t, app4.AddAllocationAsk(ask4))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app4, headRoom, 30*time.Second, ask4, iterator(), false)

	// register predicate handler
	allocs := map[string]string{}
	allocs["alloc4"] = nodeID1
	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, nil)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.NilError(t, plugin.GetPredicateError())
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc4", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc2.nodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc3.nodeID, "wrong node")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Check(t, alloc3.IsPreempted(), "alloc3 not preempted")
	assert.Equal(t, len(ask4.GetAllocationLog()), 0)
}

// TestTryPreemption_OnNode_AskResTypesSame_GuaranteedSetOnVictimAndPreemptorSides Test try preemption with 2 level queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Guaranteed set on both victim queue path and preemptor queue path.
// Request (Preemptor) resource type matches with one of the victim's resource types.  Still, needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node is full. Doesn't have enough space to accomodate the ask.
// root.parent.parent1.child1. Guaranteed set on parent1, vcores: 2. Request of vcores: 2, mem: 200 is waiting for resources.
// root.parent.parent2.child2. Guaranteed set on parent2, vcores: 1. 6 Allocations (belongs to two diff apps) are running. 3 Allocation's usage is vcores:1. 3 more allocations usage gpu: 100. Total usage is vcores:3, gpu: 300
// root.parent.parent2.child3. No usage, no guaranteed set
// 2 Allocations of each vcores:1 running on root.parent.parent1.child2 could be preempted to free up resources for ask arrived in root.parent.parent1.child1.
// 3rd allocation of vcores:1 should not be touched as preempting the same would make usage goes below the guaranteed set on root.parent.parent2.child2.
// All remaining three allocation of each mem: 100 should not be touched at all as there is no matching resource type between these allocs and ask resource types.
func TestTryPreemption_OnNode_AskResTypesSame_GuaranteedSetOnVictimAndPreemptorSides(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"vcores": 3, "gpu": 700, "mem": 200})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"vcores": "3", "gpu": "700", "mem": "200"})
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
	app1, app2, app3 := createVictimApplications(childQ2)
	for i := 5; i < 8; i++ {
		askN := newAllocationAsk(alloc+strconv.Itoa(i), appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100}))
		askN.createTime = time.Now().Add(-2 * time.Minute)
		assert.NilError(t, app1.AddAllocationAsk(askN))
		allocN := newAllocationWithKey(askN.allocationKey, appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100}))
		allocN.createTime = askN.createTime
		app1.AddAllocation(allocN)
		assert.Check(t, node.TryAddAllocation(allocN), "node alloc1 failed")
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
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node.TryAddAllocation(alloc1), "node alloc1 failed")
	alloc2 := newAllocationWithKey("alloc2", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc2.createTime = ask2.createTime
	app2.AddAllocation(alloc2)
	assert.Check(t, node.TryAddAllocation(alloc2), "node alloc2 failed")
	alloc3 := newAllocationWithKey("alloc3", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc3.createTime = ask3.createTime
	app3.AddAllocation(alloc3)
	assert.Check(t, node.TryAddAllocation(alloc3), "node alloc3 failed")

	for i := 5; i < 8; i++ {
		assert.NilError(t, childQ2.TryIncAllocatedResource(resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 100})))
	}

	assert.NilError(t, childQ2.TryIncAllocatedResource(ask1.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask2.GetAllocatedResource()))
	assert.NilError(t, childQ2.TryIncAllocatedResource(ask3.GetAllocatedResource()))

	app4 := newApplication("app-4", "default", "root.parent.parent1.child1")
	app4.SetQueue(childQ1)
	ask4 := newAllocationAsk("alloc4", "app-4", resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2, "mem": 200}))
	assert.NilError(t, app4.AddAllocationAsk(ask4))
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app4, headRoom, 30*time.Second, ask4, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{mock.NewPreemption(true, "alloc4", nodeID1, []string{"alloc3", "alloc2"}, 1, 1)}
	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.NilError(t, plugin.GetPredicateError())
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc4", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc2.nodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc3.nodeID, "wrong node")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
	assert.Check(t, alloc3.IsPreempted(), "alloc3 not preempted")
	assert.Equal(t, len(ask4.GetAllocationLog()), 0)
}

// TestTryPreemption_OnNode_UGParent_With_UGPreemptorChild_GNotSetOnVictimChild_As_Siblings Test try preemption with 2 level queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Under guaranteed parent with 2 child queues. Victim Child1 queue doesn't have guaranteed set. Under guaranteed Preemptor Child2 queue is starving for resources.
// Ask (Preemptor) resource type matches with the victim's resource type. Needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node is full, doesn't enough space to accommodate the ask.
// root.parent.parent1.child1. Guaranteed not set directly, but set on root.parent.parent1, vcores: 10. 2 Allocations (belongs to two diff apps) are running. Each Allocation usage is vcores:1. Total usage is vcores:2.
// root.parent.parent1.child2. Guaranteed set, vcores: 1. Ask of vcores: 1 is waiting for resources.
// root.parent.parent2.child3. No usage, no guaranteed set
// 1 Allocation on root.parent.parent1.child1 should be preempted to free up resources for ask arrived in root.parent.parent1.child2. Also, it won't lead to preemption storm or loop.
func TestTryPreemption_OnNode_UGParent_With_UGPreemptorChild_GNotSetOnVictimChild_As_Siblings(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"vcores": 2})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"vcores": "25"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"vcores": "20"}, nil)
	assert.NilError(t, err)
	parentQ1, err := createManagedQueueGuaranteed(parentQ, "parent1", true, nil, map[string]string{"vcores": "10"})
	assert.NilError(t, err)
	parentQ2, err := createManagedQueueGuaranteed(parentQ, "parent2", true, nil, nil)
	assert.NilError(t, err)

	childQ1, err := createManagedQueueGuaranteed(parentQ1, "child1", false, nil, nil)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ1, "child2", false, nil, map[string]string{"vcores": "1"})
	assert.NilError(t, err)
	_, err = createManagedQueueGuaranteed(parentQ2, "child3", false, nil, nil)
	assert.NilError(t, err)

	app1 := newApplication(appID1, "default", "root.parent.parent1.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	ask1.createTime = time.Now().Add(-2 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node.TryAddAllocation(alloc1), "node alloc1 failed")
	assert.NilError(t, childQ1.TryIncAllocatedResource(ask1.GetAllocatedResource()))

	app2 := newApplication(appID2, "default", "root.parent.parent1.child1")
	app2.SetQueue(childQ2)
	childQ1.applications[appID2] = app2
	ask2 := newAllocationAsk("alloc2", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	ask2.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app2.AddAllocationAsk(ask2))
	alloc2 := newAllocationWithKey("alloc2", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc2.createTime = ask2.createTime
	app2.AddAllocation(alloc2)
	assert.Check(t, node.TryAddAllocation(alloc2), "node alloc2 failed")
	assert.NilError(t, childQ1.TryIncAllocatedResource(ask2.GetAllocatedResource()))

	app3 := newApplication(appID3, "default", "root.parent.parent1.child2")
	app3.SetQueue(childQ2)
	childQ2.applications[appID3] = app3
	ask3 := newAllocationAsk("alloc3", appID3, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	assert.NilError(t, app3.AddAllocationAsk(ask3))

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app3, headRoom, 30*time.Second, ask3, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{mock.NewPreemption(true, "alloc3", nodeID1, []string{"alloc2"}, 0, 0)}
	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc2.nodeID, "wrong node")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
}

// TestTryPreemption_OnNode_UGParent_With_GNotSetOnBothChilds Test try preemption with 2 level queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Under guaranteed parent with 2 child queues. Victim Child1 queue doesn't have guaranteed set. Preemptor Child2 queue doesn't have guaranteed set.
// Ask (Preemptor) resource type matches with the victim's resource type. Needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node is full, doesn't enough space to accommodate the ask.
// root.parent.parent1.child1. Guaranteed not set directly, but set on root.parent.parent1, vcores: 10. 2 Allocations (belongs to two diff apps) are running. Each Allocation usage is vcores:1. Total usage is vcores:2.
// root.parent.parent1.child2. Guaranteed not set directly, but set on root.parent.parent1, vcores: 10. Ask of vcores: 1 is waiting for resources.
// root.parent.parent2.child3. No usage, no guaranteed set
// 1 Allocation on root.parent.parent1.child1 should not be preempted to free up resources for ask arrived in root.parent.parent1.child2 because it could lead to preemption storm or loop.
func TestTryPreemption_OnNode_UGParent_With_GNotSetOnBothChilds(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"vcores": 2})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"vcores": "25"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"vcores": "20"}, nil)
	assert.NilError(t, err)
	parentQ1, err := createManagedQueueGuaranteed(parentQ, "parent1", true, nil, map[string]string{"vcores": "10"})
	assert.NilError(t, err)
	parentQ2, err := createManagedQueueGuaranteed(parentQ, "parent2", true, nil, nil)
	assert.NilError(t, err)

	childQ1, err := createManagedQueueGuaranteed(parentQ1, "child1", false, nil, nil)
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ1, "child2", false, nil, nil)
	assert.NilError(t, err)
	_, err = createManagedQueueGuaranteed(parentQ2, "child3", false, nil, nil)
	assert.NilError(t, err)

	app1 := newApplication(appID1, "default", "root.parent.parent1.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	ask1.createTime = time.Now().Add(-2 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node.TryAddAllocation(alloc1), "node alloc1 failed")
	assert.NilError(t, childQ1.TryIncAllocatedResource(ask1.GetAllocatedResource()))

	app2 := newApplication(appID2, "default", "root.parent.parent1.child1")
	app2.SetQueue(childQ2)
	childQ1.applications[appID2] = app2
	ask2 := newAllocationAsk("alloc2", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	ask2.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app2.AddAllocationAsk(ask2))
	alloc2 := newAllocationWithKey("alloc2", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc2.createTime = ask2.createTime
	app2.AddAllocation(alloc2)
	assert.Check(t, node.TryAddAllocation(alloc2), "node alloc2 failed")
	assert.NilError(t, childQ1.TryIncAllocatedResource(ask2.GetAllocatedResource()))

	app3 := newApplication(appID3, "default", "root.parent.parent1.child2")
	app3.SetQueue(childQ2)
	childQ2.applications[appID3] = app3
	ask3 := newAllocationAsk("alloc3", appID3, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	assert.NilError(t, app3.AddAllocationAsk(ask3))

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app3, headRoom, 30*time.Second, ask3, iterator(), false)

	// register predicate handler
	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, nil)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result == nil, "preemption is yielding results")
	assert.Assert(t, !ok, "victims found")
}

// TestTryPreemption_OnNode_UGParent_With_UGPreemptorChild_OGVictimChild_As_Siblings Test try preemption with 2 level queue hierarchy. Since Node doesn't have enough resources to accomodate, preemption happens because of node resource constraint.
// Under guaranteed parent with 2 child queues. Over Guaranteed Victim Child1 queue has guaranteed set. Under Guaranteed Preemptor Child2 queue is starving for resources.
// Ask (Preemptor) resource type matches with the victim's resource type. Needs to be preempted because matching resource type has been configured as guaranteed.
// Setup:
// Nodes are Node1. Node is full, doesn't enough space to accommodate the ask.
// root.parent.parent1.child1. Guaranteed of vcores:1 set directly , but also set on root.parent.parent1, vcores: 10. 2 Allocations (belongs to two diff apps) are running. Each Allocation usage is vcores:1. Total usage is vcores:2.
// root.parent.parent1.child2. Guaranteed of vcores:1 set directly, but also set on root.parent.parent1, vcores: 10. Ask of vcores: 1 is waiting for resources.
// root.parent.parent2.child3. No usage, no guaranteed set
// 1 Allocation on root.parent.parent1.child1 should be preempted to free up resources for ask arrived in root.parent.parent1.child2. Also, it won't lead to preemption storm or loop.
func TestTryPreemption_OnNode_UGParent_With_UGPreemptorChild_OGVictimChild_As_Siblings(t *testing.T) {
	node := newNode(nodeID1, map[string]resources.Quantity{"vcores": 2})
	iterator := getNodeIteratorFn(node)
	rootQ, err := createRootQueue(map[string]string{"vcores": "25"})
	assert.NilError(t, err)
	parentQ, err := createManagedQueueGuaranteed(rootQ, "parent", true, map[string]string{"vcores": "20"}, nil)
	assert.NilError(t, err)
	parentQ1, err := createManagedQueueGuaranteed(parentQ, "parent1", true, nil, map[string]string{"vcores": "10"})
	assert.NilError(t, err)
	parentQ2, err := createManagedQueueGuaranteed(parentQ, "parent2", true, nil, nil)
	assert.NilError(t, err)

	childQ1, err := createManagedQueueGuaranteed(parentQ1, "child1", false, nil, map[string]string{"vcores": "1"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueGuaranteed(parentQ1, "child2", false, nil, map[string]string{"vcores": "1"})
	assert.NilError(t, err)
	_, err = createManagedQueueGuaranteed(parentQ2, "child3", false, nil, nil)
	assert.NilError(t, err)

	app1 := newApplication(appID1, "default", "root.parent.parent1.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	ask1.createTime = time.Now().Add(-2 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node.TryAddAllocation(alloc1), "node alloc1 failed")
	assert.NilError(t, childQ1.TryIncAllocatedResource(ask1.GetAllocatedResource()))

	app2 := newApplication(appID2, "default", "root.parent.parent1.child1")
	app2.SetQueue(childQ2)
	childQ1.applications[appID2] = app2
	ask2 := newAllocationAsk("alloc2", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	ask2.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app2.AddAllocationAsk(ask2))
	alloc2 := newAllocationWithKey("alloc2", appID2, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	alloc2.createTime = ask2.createTime
	app2.AddAllocation(alloc2)
	assert.Check(t, node.TryAddAllocation(alloc2), "node alloc2 failed")
	assert.NilError(t, childQ1.TryIncAllocatedResource(ask2.GetAllocatedResource()))

	app3 := newApplication(appID3, "default", "root.parent.parent1.child2")
	app3.SetQueue(childQ2)
	childQ2.applications[appID3] = app3
	ask3 := newAllocationAsk("alloc3", appID3, resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 1}))
	assert.NilError(t, app3.AddAllocationAsk(ask3))

	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"vcores": 2})
	preemptor := NewPreemptor(app3, headRoom, 30*time.Second, ask3, iterator(), false)

	// register predicate handler
	preemptions := []mock.Preemption{mock.NewPreemption(true, "alloc3", nodeID1, []string{"alloc2"}, 0, 0)}
	plugin := mock.NewPreemptionPredicatePlugin(nil, nil, preemptions)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	result, ok := preemptor.TryPreemption()
	assert.Assert(t, result != nil, "no result")
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID1, result.NodeID, "wrong node")
	assert.Equal(t, nodeID1, alloc2.nodeID, "wrong node")
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 not preempted")
}

func allocForScore(originator bool, allowPreemptSelf bool) *Allocation {
	return NewAllocationFromSI(&si.Allocation{
		AllocationKey:    "alloc1",
		ApplicationID:    appID1,
		Originator:       originator,
		NodeID:           nodeID1,
		PreemptionPolicy: &si.PreemptionPolicy{AllowPreemptSelf: allowPreemptSelf},
	})
}
