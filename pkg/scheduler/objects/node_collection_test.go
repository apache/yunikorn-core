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

	"github.com/google/uuid"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
)

func TestNewNodeCollection(t *testing.T) {
	nc := NewNodeCollection("test")
	bc, ok := nc.(*baseNodeCollection)
	if !ok {
		t.Fatal("NewNodeCollection should return baseNodeCollection")
	}
	assert.Equal(t, "test", bc.Partition, "node collection should have matching partition")

	if nc.GetNodeSortingPolicy().PolicyType().String() != policies.FairSortPolicy.String() {
		t.Errorf("Node default Policy: want %s, got %s", nc.GetNodeSortingPolicy().PolicyType().String(), policies.FairSortPolicy.String())
	}
}

func initBaseCollection() *baseNodeCollection {
	return NewNodeCollection("test").(*baseNodeCollection)
}

func initNode(name string) *Node {
	return newNode(name, make(map[string]resources.Quantity))
}

func TestNodeCollection_AddNode(t *testing.T) {
	var err error
	nc := initBaseCollection()
	err = nc.AddNode(nil)
	if err == nil {
		t.Fatal("nil node add did not return error")
	}
	assert.Equal(t, 0, len(nc.nodes), "nil node should not be added")
	node := initNode("test1")
	err = nc.AddNode(node)
	assert.NilError(t, err, "test node add failed unexpected")
	assert.Equal(t, len(nc.nodes), 1, "node list not correct")
	// add the same node nothing changes
	err = nc.AddNode(node)
	if err == nil {
		t.Fatal("add same test node worked unexpected")
	}
	assert.Equal(t, len(nc.nodes), 1, "node list not correct")
}

func TestNodeCollection_RemoveNode(t *testing.T) {
	var err error
	nc := initBaseCollection()
	err = nc.AddNode(initNode("test1"))
	assert.NilError(t, err, "test node add failed unexpected")
	assert.Equal(t, 1, len(nc.nodes), "node list not correct")

	// remove non existing node
	_ = nc.RemoveNode("")
	assert.Equal(t, 1, len(nc.nodes), "nil node should not remove anything")
	_ = nc.RemoveNode("does not exist")
	assert.Equal(t, 1, len(nc.nodes), "non existing node was removed")

	_ = nc.RemoveNode("test1")
	assert.Equal(t, 0, len(nc.nodes), "node was not removed")
}

func TestNodeCollection_GetNode(t *testing.T) {
	var err error
	nc := initBaseCollection()
	err = nc.AddNode(initNode("test1"))
	assert.NilError(t, err, "test node add failed unexpected")
	node := nc.GetNode("test1")
	if node == nil {
		t.Fatal("requested node not found")
	}
	assert.Equal(t, "test1", node.NodeID, "wrong node returned")

	node = nc.GetNode("missing")
	if node != nil {
		t.Fatal("get node returned unexpected value")
	}
}

func TestBaseNodeCollection_GetNodeCount(t *testing.T) {
	var err error
	nc := initBaseCollection()
	assert.Equal(t, 0, nc.GetNodeCount(), "node count for empty collection should be zero")
	err = nc.AddNode(initNode("test1"))
	assert.NilError(t, err, "test node add failed unexpected")
	assert.Equal(t, 1, nc.GetNodeCount(), "node count should include added node")
}

func TestNodeCollection_GetNodes(t *testing.T) {
	var err error
	nc := initBaseCollection()
	nodes := nc.GetNodes()
	assert.Equal(t, 0, len(nodes), "list should have been empty")

	node := initNode("test1")
	node.SetSchedulable(false)
	err = nc.AddNode(node)
	assert.NilError(t, err, "test node add failed unexpected")

	nodes = nc.GetNodes()
	assert.Equal(t, 1, len(nodes), "list is missing node")
}

func TestSetNodeSortingPolicy(t *testing.T) {
	// vcore, memory
	defaultCapicity := [2]int64{10, 15}
	var nodesInfo = []struct {
		nodeID         string
		allocatedVcore int64
		allocatedMem   int64
	}{
		{"node-04", 8, 10},
		{"node-02", 4, 10},
		{"node-01", 2, 10},
		{"node-03", 6, 10},
	}

	order := make(map[string][]string, 2)
	order[policies.FairnessPolicy.String()] = []string{nodesInfo[2].nodeID, nodesInfo[1].nodeID, nodesInfo[3].nodeID, nodesInfo[0].nodeID}
	order[policies.BinPackingPolicy.String()] = []string{nodesInfo[0].nodeID, nodesInfo[3].nodeID, nodesInfo[1].nodeID, nodesInfo[2].nodeID}
	var tests = []struct {
		name       string
		input      string
		nodesOrder []string
	}{
		{"Set unkown node sorting policy", "greedy", order[policies.FairnessPolicy.String()]},
		{"Set default node sorting policy", "", order[policies.FairnessPolicy.String()]},
		{"Set binpacking node sorting policy", policies.BinPackingPolicy.String(), order[policies.BinPackingPolicy.String()]},
		{"Set fair node sorting policy", policies.FairnessPolicy.String(), order[policies.FairnessPolicy.String()]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := NewNodeCollection("test")
			for id := 0; id < len(nodesInfo); id++ {
				node := newNode(nodesInfo[id].nodeID, map[string]resources.Quantity{"vcore": resources.Quantity(defaultCapicity[0]), "memory": resources.Quantity(defaultCapicity[1])})
				res := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": resources.Quantity(nodesInfo[id].allocatedVcore), "memory": resources.Quantity(nodesInfo[id].allocatedMem)})
				alloc := newAllocation(fmt.Sprintf("test-app-%d", id+1), uuid.NewString(), fmt.Sprintf("test-%d", id+1), "root.default", res)
				if ok := node.AddAllocation(alloc); !ok {
					t.Error("Allocation error happen in node.")
				}

				if err := nc.AddNode(node); err != nil {
					t.Errorf("AddNode error:%s", err.Error())
				}
			}

			conf := configs.PartitionConfig{
				Name: "test",
				Queues: []configs.QueueConfig{
					{
						Name:      "root",
						Parent:    true,
						SubmitACL: "*",
						Queues: []configs.QueueConfig{
							{
								Name:   "default",
								Parent: false,
								Queues: nil,
							},
						},
					},
				},
				PlacementRules: nil,
				Limits:         nil,
				NodeSortPolicy: configs.NodeSortingPolicy{
					Type: tt.input,
				},
			}

			nc.SetNodeSortingPolicy(NewNodeSortingPolicy(conf.NodeSortPolicy.Type, conf.NodeSortPolicy.ResourceWeights))
			iter := nc.GetNodeIterator()
			for id := 0; id < len(tt.nodesOrder); id++ {
				if n := iter.Next(); n.NodeID != tt.nodesOrder[id] {
					t.Errorf("%s: NodeID wanted %s, but it got %s.", nc.GetNodeSortingPolicy().PolicyType().String(), tt.nodesOrder[id], n.NodeID)
				}
			}
		})
	}
}

func TestGetNodeSortingPolicy(t *testing.T) {
	nodeNames := []string{"node-1", "node-2", "node-3", "node-4"}
	revertNodeNames := []string{"node-4", "node-3", "node-2", "node-1"}

	var tests = []struct {
		name            string
		input           string
		want            string
		exceptNodeOrder []string
	}{
		{"Default policy", "", policies.FairnessPolicy.String(), nodeNames},
		{"Binpacking policy", policies.BinPackingPolicy.String(), policies.BinPackingPolicy.String(), revertNodeNames},
		{"Fair policy", policies.FairnessPolicy.String(), policies.FairnessPolicy.String(), nodeNames},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := NewNodeCollection("test")
			for id := 1; id < len(nodeNames)+1; id++ {
				node := newNode(nodeNames[id-1], map[string]resources.Quantity{"vcore": resources.Quantity(6)})
				res := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": resources.Quantity(id)})
				alloc := newAllocation(fmt.Sprintf("test-app-%d", id+1), uuid.NewString(), fmt.Sprintf("test-%d", id), "root.default", res)
				node.AddAllocation(alloc)

				if err := nc.AddNode(node); err != nil {
					t.Errorf("AddNode error:%s", err.Error())
				}
			}

			conf := configs.PartitionConfig{
				Name: "test",
				Queues: []configs.QueueConfig{
					{
						Name:      "root",
						Parent:    true,
						SubmitACL: "*",
						Queues: []configs.QueueConfig{
							{
								Name:   "default",
								Parent: false,
								Queues: nil,
							},
						},
					},
				},
				PlacementRules: nil,
				Limits:         nil,
				NodeSortPolicy: configs.NodeSortingPolicy{
					Type: tt.input,
				},
			}

			nc.SetNodeSortingPolicy(NewNodeSortingPolicy(conf.NodeSortPolicy.Type, conf.NodeSortPolicy.ResourceWeights))
			if ans := nc.GetNodeSortingPolicy().PolicyType().String(); ans != tt.want {
				t.Errorf("got %s, want %s", ans, tt.want)
			}

			// Checking thes nodes order in iterator is after setting node policy with Default weight{vcore:1, memory:1}.
			iter := nc.GetNodeIterator()
			for index := 0; iter.HasNext(); index++ {
				if index >= len(tt.exceptNodeOrder) {
					t.Error("Wrong length of nodes in node iterator.")
				}

				n := iter.Next()
				if n.NodeID != tt.exceptNodeOrder[index] {
					t.Errorf("Policy: %s, got %s, want %s", nc.GetNodeSortingPolicy().PolicyType().String(), n.NodeID, tt.exceptNodeOrder[index])
				}
			}
		})
	}
}

func TestGetFullNodeIterator(t *testing.T) {
	nc := NewNodeCollection("test")
	for i := 1; i <= 4; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		node := newNode(nodeName, map[string]resources.Quantity{"vcore": resources.Quantity(10)})
		if i%2 == 0 {
			appName := fmt.Sprintf("app-%02d", i)
			allocName := fmt.Sprintf("alloc-%02d", i)
			app := newApplication(appName, "default", "root.test")
			ask := newAllocationAsk(allocName, appName, resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": resources.Quantity(i)}))
			if err := node.Reserve(app, ask); err != nil {
				t.Error("Reserving failed.")
			}
		} else {
			res := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": resources.Quantity(i)})
			alloc := newAllocation(fmt.Sprintf("test-app-%d", i), uuid.NewString(), fmt.Sprintf("test-%d", i), "root.default", res)
			if ok := node.AddAllocation(alloc); !ok {
				t.Error("Allocation error in node.")
			}
		}
		if err := nc.AddNode(node); err != nil {
			t.Error("Adding another node into BC failed.")
		}
	}
	nodes := make([]*Node, 0)
	for iterator := nc.GetFullNodeIterator(); iterator.HasNext(); {
		nodes = append(nodes, iterator.Next())
	}
	assert.Equal(t, len(nodes), 4, "wrong length")
	assert.Equal(t, nodes[0].NodeID, "node-2", "wrong node 0")
	assert.Equal(t, nodes[1].NodeID, "node-4", "wrong node 1")
	assert.Equal(t, nodes[2].NodeID, "node-1", "wrong node 2")
	assert.Equal(t, nodes[3].NodeID, "node-3", "wrong node 3")
}

func TestGetNodeIterator(t *testing.T) {
	var tests = []struct {
		name         string
		reserved     []bool
		wantWithFair []int
	}{
		{"All nodes are available", []bool{false, false, false, false}, []int{1, 2, 3, 4}},
		{"Some nodes are reserved", []bool{false, true, false, true}, []int{1, 3}},
		{"All nodes are reserved", []bool{true, true, true, true}, []int{}},
	}

	for _, tt := range tests {
		t.Run("There are reserved nodes in an instance of node collection.", func(t *testing.T) {
			nc := NewNodeCollection("test")

			// Initialization of nodes and application
			for i := 1; i < len(tt.reserved)+1; i++ {
				nodeName := fmt.Sprintf("node-%d", i)
				node := newNode(nodeName, map[string]resources.Quantity{"vcore": resources.Quantity(10)})
				if tt.reserved[i-1] {
					appName := fmt.Sprintf("app-%02d", i)
					allocName := fmt.Sprintf("alloc-%02d", i)
					app := newApplication(appName, "default", "root.test")
					ask := newAllocationAsk(allocName, appName, resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": resources.Quantity(i)}))
					if err := node.Reserve(app, ask); err != nil {
						t.Error("Reserving failed.")
					}
				} else {
					res := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": resources.Quantity(i)})
					alloc := newAllocation(fmt.Sprintf("test-app-%d", i), uuid.NewString(), fmt.Sprintf("test-%d", i), "root.default", res)
					if ok := node.AddAllocation(alloc); !ok {
						t.Error("Allocation error happen in node.")
					}
				}

				if err := nc.AddNode(node); err != nil {
					t.Error("Adding another node into BC failed.")
				}
			}

			// Check order of avialble nodes
			if iter := nc.GetNodeIterator(); len(tt.wantWithFair) == 0 {
				if iter != nil {
					t.Error("Wrong count of nodes in the node collection instance.")
				}
			} else {
				NodeSortingPolicy := []string{policies.FairnessPolicy.String(), policies.BinPackingPolicy.String()}

				// Fair policy
				nc.SetNodeSortingPolicy(NewNodeSortingPolicy(NodeSortingPolicy[0], nil))
				if ans := nc.GetNodeSortingPolicy().PolicyType().String(); ans != NodeSortingPolicy[0] {
					t.Errorf("got %s, want %s", ans, NodeSortingPolicy[0])
				}

				for index := 0; iter.HasNext(); index++ {
					if index >= len(tt.wantWithFair) {
						t.Errorf("Want length of nodes: %d, Get length of nodes: %d", index, len(tt.wantWithFair))
					}

					n := iter.Next()
					if want := fmt.Sprintf("node-%d", tt.wantWithFair[index]); n.NodeID != want {
						t.Errorf("%s with %s, Want %s, got %s.", tt.name, NodeSortingPolicy[0], want, n.NodeID)
					}
				}

				// Binpacking policy
				nc.SetNodeSortingPolicy(NewNodeSortingPolicy(NodeSortingPolicy[1], nil))
				if ans := nc.GetNodeSortingPolicy().PolicyType().String(); ans != NodeSortingPolicy[1] {
					t.Errorf("got %s, want %s", ans, NodeSortingPolicy[1])
				}

				iter = nc.GetNodeIterator()
				DescreasingIndex := len(tt.wantWithFair) - 1
				for index := 0; iter.HasNext(); index++ {
					if index >= len(tt.wantWithFair) {
						t.Errorf("Want length of nodes: %d, Get length of nodes: %d", index, len(tt.wantWithFair))
					}
					n := iter.Next()
					if want := fmt.Sprintf("node-%d", tt.wantWithFair[DescreasingIndex]); n.NodeID != want {
						t.Errorf("%s with %s, want %s, got %s.", tt.name, NodeSortingPolicy[1], want, n.NodeID)
					}
					DescreasingIndex--
				}
			}
		})
	}
}
