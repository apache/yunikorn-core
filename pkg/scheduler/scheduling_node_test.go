/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
    "github.com/cloudera/yunikorn-core/pkg/cache"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "github.com/stretchr/testify/assert"
    "testing"
)

func newNode(nodeId string, totalMap map[string]resources.Quantity) *SchedulingNode {
    // leverage the cache test code
    totalRes := resources.NewResourceFromMap(totalMap)
    nodeInfo := cache.NewNodeForTest(nodeId, totalRes)
    return NewSchedulingNode(nodeInfo)
}

// Create a new node for testing only.
func TestNewSchedulingNode(t *testing.T) {
    node := NewSchedulingNode(nil)
    if node != nil {
        t.Errorf("nil input should not return node %v", node)
    }

    // this is just a wrapper so we can use it to test the real new
    res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 100, "second": 100})
    node = newNode("node-1", res.Resources)
    if node == nil || node.NodeId != "node-1" {
        t.Fatalf("node create failed which should not have %v", node)
    }
    // check the resource info all zero
    if !resources.IsZero(node.allocatingResource) && !resources.IsZero(node.preemptingResource) {
        t.Errorf("node resources should all be zero found: %v and %v", node.allocatingResource,  node.preemptingResource)
    }
    if !node.needUpdateCachedAvailable {
        t.Error("node available resource dirty should be set for new node")
    }
    if !resources.Equals(node.getAvailableResource(), res) {
        t.Errorf("node available resource not set to cached value got: %v", node.getAvailableResource())
    }
    if node.needUpdateCachedAvailable {
        t.Error("node available resource dirty should be cleared after getAvailableResource call")
    }
}

func TestCheckConditions(t *testing.T) {
    ask := NewSchedulingAllocationAsk(&si.AllocationAsk{
        AllocationKey: "test",
        ResourceAsk: &si.Resource{
            Resources: map[string]*si.Quantity{
                "memory": {Value: 100},
                "vcore":  {Value: 100},
            },
        },
        MaxAllocations: 10,
        ApplicationId:  "app-1",
    })

    node := newNode("node-1", map[string]resources.Quantity{"memory": 100, "vcore": 100})
    if node == nil || node.NodeId != "node-1" {
        t.Fatalf("node create failed which should not have %v", node)
    }

    // Check if we can allocate on scheduling node (no plugins)
    if !node.CheckAllocateConditions(ask, false) {
        t.Error("node with scheduling set to true no plugins should allow allocation")
    }

    // Check if we can allocate on non scheduling node (no plugins)
    node.nodeInfo.SetSchedulable(false)
    if node.CheckAllocateConditions(ask, false) {
        t.Error("node with scheduling set to false should not allow allocation")
    }

    // Create another ask, which beyond node's resource, it should be rejected
    ask = NewSchedulingAllocationAsk(&si.AllocationAsk{
        AllocationKey: "test",
        ResourceAsk: &si.Resource{
            Resources: map[string]*si.Quantity{
                "memory": {Value: 110},
                "vcore":  {Value: 100},
            },
        },
        MaxAllocations: 10,
        ApplicationId:  "app-1",
    })

    node.nodeInfo.SetSchedulable(true)
    if node.CheckAllocateConditions(ask, false) {
        t.Error("node with smaller than request's resource should not allow allocation ever")
    }
    //TODO add mock for plugin to extend tests
}

func TestCheckAllocate(t *testing.T) {
    node := newNode("node-1", map[string]resources.Quantity{"first": 10})
    if node == nil || node.NodeId != "node-1" {
        t.Fatalf("node create failed which should not have %v", node)
    }
    if !node.needUpdateCachedAvailable {
        t.Error("node available resource dirty should be set for new node")
    }
    // normal alloc check dirty flag
    node.getAvailableResource() // unset the dirty flag
    res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
    if ok, _ := node.CheckAndAllocateResource(res, false); !ok {
        t.Error("node should have accepted allocation")
    }
    if !node.needUpdateCachedAvailable {
        t.Error("node available resource dirty should be set after CheckAndAllocateResource")
    }
    // add one that pushes node over its size
    res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 6})
    if ok, _ := node.CheckAndAllocateResource(res, false); ok {
        t.Error("node should have rejected allocation (oversize)")
    }

    // check if preempting adds to available
    node = newNode("node-1", map[string]resources.Quantity{"first": 5})
    if node == nil || node.NodeId != "node-1" {
        t.Fatalf("node create failed which should not have %v", node)
    }
    node.incPreemptingResource(resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
    // preemption alloc
    if ok, _ := node.CheckAndAllocateResource(res, true); !ok {
        t.Error("node with scheduling set to false should not allow allocation")
    }
}

func TestAllocatingResources(t *testing.T) {
    node := newNode("node-1", map[string]resources.Quantity{"first": 100})
    if node == nil || node.NodeId != "node-1" {
        t.Fatalf("node create failed which should not have %v", node)
    }

    allocRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
    node.incAllocatingResource(allocRes)
    node.incAllocatingResource(allocRes)
    expect := resources.Multiply(allocRes, 2)
    nodeAlloc := node.getAllocatingResource()
    if !resources.Equals(nodeAlloc, expect) {
        t.Errorf("allocating resources not set, expected %v got %v", expect, nodeAlloc)
    }
    // release one
    node.decreaseAllocatingResource(allocRes)
    nodeAlloc = node.getAllocatingResource()
    if !resources.Equals(nodeAlloc, allocRes) {
        t.Errorf("allocating resources not decremented, expected %v got %v", expect, nodeAlloc)
    }
    // release allocating: should be back to zero
    node.decreaseAllocatingResource(allocRes)
    nodeAlloc = node.getAllocatingResource()
    if !resources.IsZero(nodeAlloc) {
        t.Errorf("allocating resources not zero but %v", nodeAlloc)
    }
}

func TestPreemptingResources(t *testing.T) {
    node := newNode("node-1", map[string]resources.Quantity{"first": 10})
    if node == nil || node.NodeId != "node-1" {
        t.Fatalf("node create failed which should not have %v", node)
    }

    preemptRes := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
    node.incPreemptingResource(preemptRes)
    node.incPreemptingResource(preemptRes)
    expect := resources.Multiply(preemptRes, 2)
    nodePreempt := node.getPreemptingResource()
    if !resources.Equals(nodePreempt, expect) {
        t.Errorf("preempting resources not set, expected %v got %v", expect, nodePreempt)
    }
    // release one preemption
    node.handlePreemptionUpdate(preemptRes)
    nodePreempt = node.getPreemptingResource()
    if !resources.Equals(nodePreempt, preemptRes) {
        t.Errorf("preempting resources not decremented, expected %v got %v", preemptRes, nodePreempt)
    }
    // release preemption: should be back to zero
    node.handlePreemptionUpdate(preemptRes)
    nodePreempt = node.getPreemptingResource()
    if !resources.IsZero(nodePreempt) {
        t.Errorf("preempting resources not zero but %v", nodePreempt)
    }
}

func TestAvailableDirty(t *testing.T) {
    node := newNode("node-1", map[string]resources.Quantity{"first": 10})
    if node == nil || node.NodeId != "node-1" {
        t.Fatalf("node create failed which should not have %v", node)
    }
    node.getAvailableResource()
    if node.needUpdateCachedAvailable {
        t.Fatal("node available resource dirty should not be set after getAvailableResource")
    }

    res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
    node.incAllocatingResource(res)
    if !node.needUpdateCachedAvailable {
        t.Error("node available resource dirty should be set after incAllocatingResource")
    }
    node.getAvailableResource()

    node.decreaseAllocatingResource(res)
    if !node.needUpdateCachedAvailable {
        t.Error("node available resource dirty should be set after decreaseAllocatingResource")
    }
}

func TestFitInGapScore(t *testing.T) {
    // Fit
    smaller := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
    larger := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 11})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Fit (negative always fit)
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": -1})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Fit (negative always fit)
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": -1})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": -2})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0, "second": 0})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0, "second": 0})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 0})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 0})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 1})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 1})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 1})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 1})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 1})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 2})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 1})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 2})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"second": 1})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 2})
    assert.True(t, fitInGapScore(larger, smaller) == 0)

    // Cannot fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0})
    assert.True(t, fitInGapScore(larger, smaller) > 0)

    // Cannot fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
    assert.True(t, fitInGapScore(larger, smaller) > 0)

    // Cannot fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{})
    assert.True(t, fitInGapScore(larger, smaller) > 0)

    // Cannot fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 1})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 0, "second": 2})
    assert.True(t, fitInGapScore(larger, smaller) > 0)

    // Cannot fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 1})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 2})
    assert.True(t, fitInGapScore(larger, smaller) > 0)

    // Cannot fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 1})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1, "second": 1})
    assert.True(t, fitInGapScore(larger, smaller) > 0)

    // Cannot fit
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 2, "second": 1})
    larger = resources.NewResourceFromMap(map[string]resources.Quantity{"second": 1})
    assert.True(t, fitInGapScore(larger, smaller) > 0)

    // Following are relative score to see which one is more fit than the other

    // All resource types are treated same
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})
    largerA := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5, "second": 10})
    largerB := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 5})
    assert.Equal(t, fitInGapScore(largerA, smaller), fitInGapScore(largerB, smaller))

    // All resource types are treated same (proportional)
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 100})
    largerA = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8, "second": 100})
    largerB = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 80})
    assert.Equal(t, fitInGapScore(largerA, smaller), fitInGapScore(largerB, smaller))

    // missing types
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})
    largerA = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8})
    largerB = resources.NewResourceFromMap(map[string]resources.Quantity{"second": 8})
    assert.Equal(t, fitInGapScore(largerA, smaller), fitInGapScore(largerB, smaller))

    // missing types with proportional
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 100})
    largerA = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8})
    largerB = resources.NewResourceFromMap(map[string]resources.Quantity{"second": 80})
    assert.Equal(t, fitInGapScore(largerA, smaller), fitInGapScore(largerB, smaller))

    // A is more fit than B (smaller)
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})
    largerA = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8, "second": 10})
    largerB = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 9, "second": 10})
    assert.True(t, fitInGapScore(largerA, smaller) > fitInGapScore(largerB, smaller))

    // A is more fit than B (smaller)
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})
    largerA = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8, "second": 9})
    largerB = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 9, "second": 9})
    assert.True(t, fitInGapScore(largerA, smaller) > fitInGapScore(largerB, smaller))

    // A is more fit than B (smaller)
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 10})
    largerA = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8, "second": 8})
    largerB = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 9, "second": 9})
    assert.True(t, fitInGapScore(largerA, smaller) > fitInGapScore(largerB, smaller))

    // A is more fit than B (smaller)
    smaller = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 100})
    largerA = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 8, "second": 70})
    largerB = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 7, "second": 90})
    assert.True(t, fitInGapScore(largerA, smaller) > fitInGapScore(largerB, smaller))
}
