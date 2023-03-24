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
	"errors"
	"fmt"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestCheckPreconditions(t *testing.T) {
	node := newNode("node1", map[string]resources.Quantity{"first": 5})
	nodes := []*Node{node}
	iterator := func() NodeIterator { return NewDefaultNodeIterator(nodes) }
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
	nodes := []*Node{node}
	iterator := func() NodeIterator { return NewDefaultNodeIterator(nodes) }
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
	app1.AddAllocation(NewAllocation("alloc1", "node1", ask1))
	app1.AddAllocation(NewAllocation("alloc2", "node1", ask2))
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

func TestTryPreemption(t *testing.T) {
	node := newNode("node1", map[string]resources.Quantity{"first": 10})
	nodes := []*Node{node}
	iterator := func() NodeIterator { return NewDefaultNodeIterator(nodes) }
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
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))
	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5}))
	ask2.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask2))
	alloc1 := NewAllocation("alloc1", "node1", ask1)
	app1.AddAllocation(alloc1)
	assert.Check(t, node.AddAllocation(alloc1), "node alloc1 failed")
	alloc2 := NewAllocation("alloc2", "node1", ask2)
	assert.Check(t, node.AddAllocation(alloc2), "node alloc2 failed")
	node.AddAllocation(alloc2)
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

	// register predicate handler
	pluginErr := &errHolder{}
	plugin := &mockPredicates{
		errHolder:    pluginErr,
		reservations: nil,
		allocations:  nil,
		preemptions: []mockPreemption{{
			expectedAllocationKey:  "alloc3",
			expectedNodeID:         "node1",
			expectedAllocationKeys: []string{"alloc1"},
			expectedStartIndex:     0,
			success:                true,
			index:                  0,
		}},
	}
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	alloc, ok := preemptor.TryPreemption()
	assert.NilError(t, pluginErr.err)
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", alloc.allocationKey, "wrong alloc")
	assert.Check(t, alloc1.IsPreempted(), "alloc1 preempted")
	assert.Check(t, !alloc2.IsPreempted(), "alloc2 not preempted")
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
	return NewAllocation("alloc1", nodeID1, ask)
}

type mockPreemption struct {
	expectedAllocationKey  string
	expectedNodeID         string
	expectedAllocationKeys []string
	expectedStartIndex     int32
	success                bool
	index                  int32
}

var _ api.ResourceManagerCallback = &mockPredicates{}

type errHolder struct {
	err error
}

type mockPredicates struct {
	reservations map[string]string
	allocations  map[string]string
	preemptions  []mockPreemption
	errHolder    *errHolder
	api.ResourceManagerCallback
}

func (m mockPredicates) Predicates(args *si.PredicatesArgs) error {
	if args.Allocate {
		nodeID, ok := m.allocations[args.AllocationKey]
		if !ok {
			return errors.New("no allocation found")
		}
		if nodeID != args.NodeID {
			return errors.New("wrong node")
		}
		return nil
	} else {
		nodeID, ok := m.reservations[args.AllocationKey]
		if !ok {
			return errors.New("no allocation found")
		}
		if nodeID != args.NodeID {
			return errors.New("wrong node")
		}
		return nil
	}
}

func (m mockPredicates) PreemptionPredicates(args *si.PreemptionPredicatesArgs) *si.PreemptionPredicatesResponse {
	result := &si.PreemptionPredicatesResponse{
		Success: false,
		Index:   -1,
	}
	for _, preemption := range m.preemptions {
		if preemption.expectedAllocationKey != args.AllocationKey {
			continue
		}
		if preemption.expectedNodeID != args.NodeID {
			continue
		}
		if preemption.expectedStartIndex != args.StartIndex {
			m.errHolder.err = fmt.Errorf("unexpected start index exepected=%d, actual=%d, allocationKey=%s",
				preemption.expectedStartIndex, args.StartIndex, args.AllocationKey)
			return result
		}
		if len(preemption.expectedAllocationKeys) != len(args.PreemptAllocationKeys) {
			m.errHolder.err = fmt.Errorf("unexpected alloc key length expected=%d, actual=%d, allocationKey=%s",
				len(preemption.expectedAllocationKeys), len(args.PreemptAllocationKeys), args.AllocationKey)
			return result
		}
		for idx, key := range preemption.expectedAllocationKeys {
			if args.PreemptAllocationKeys[idx] != key {
				m.errHolder.err = fmt.Errorf("unexpected preempt alloc key expected=%s, actual=%s, index=%d, allocationKey=%s",
					args.PreemptAllocationKeys[idx], key, idx, args.AllocationKey)
				return result
			}
		}
		result.Success = preemption.success
		result.Index = preemption.index
		return result
	}
	m.errHolder.err = fmt.Errorf("mo match found allocationKey=%s, nodeID=%s", args.AllocationKey, args.NodeID)
	return result
}

func (m mockPredicates) UpdateAllocation(_ *si.AllocationResponse) error {
	// no implementation
	return nil
}

func (m mockPredicates) UpdateApplication(_ *si.ApplicationResponse) error {
	// no implementation
	return nil
}

func (m mockPredicates) UpdateNode(_ *si.NodeResponse) error {
	// no implementation
	return nil
}

func (m mockPredicates) SendEvent(_ []*si.EventRecord) {
	// no implementation
}

func (m mockPredicates) UpdateContainerSchedulingState(_ *si.UpdateContainerSchedulingStateRequest) {
	// no implementation
}
