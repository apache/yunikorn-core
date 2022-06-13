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
	"gotest.tools/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

func TestNewNodeCollection(t *testing.T) {
	nc := NewNodeCollection("test")
	bc, ok := nc.(*baseNodeCollection)
	if !ok {
		t.Fatal("NewNodeCollection should return baseNodeCollection")
	}
	assert.Equal(t, "test", bc.Partition, "node collection should have matching partition")
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
	weights := map[string]float64{
		"vcore":  2.0,
		"memory": 3.0,
	}

	var nodesInfo = []struct {
		nodeID         string
		vcore          int64
		mem            int64
		allocatedVcore int64
		allocatedMem   int64
	}{
		{"node-01", 250, 1000, 120, 530},
		{"node-02", 500, 750, 380, 250},
		{"node-03", 750, 500, 500, 200},
		{"node-04", 1000, 250, 800, 200},
	}
	/*
	*  node\policy			fair																		binpacking(1-fair)
	*	1					[2*(120/250)+3*(530/1000)]/5=(0.96+1.59)/5=2.55/5=0.51		(3)				0.39					(2)
	*	2					[2*(380/500)+3*(250/750)]/5=(1.52+1)/5=2.52/5=0.502			(1)				0.498					(4)
	*	3					[2*(500/750)+3*(200/500)]/5=(1.333+1.2)/5=2.533/5=0.5066	(2)				0.4934					(3)
	*	4					[2*(800/1000)+3*(200/250)]/5=(1.6+2.4)/5=4/5=0.8			(4)				0.2						(1)
	 */

	// Yunikorn support fair and binpacking policy, nil shows when nc policy unsets.
	order := make(map[string][]string, 3)
	order["nil"] = []string{nodesInfo[0].nodeID, nodesInfo[1].nodeID, nodesInfo[2].nodeID, nodesInfo[3].nodeID}
	order["fair"] = []string{nodesInfo[1].nodeID, nodesInfo[2].nodeID, nodesInfo[0].nodeID, nodesInfo[3].nodeID}
	order["binpacking"] = []string{nodesInfo[3].nodeID, nodesInfo[0].nodeID, nodesInfo[2].nodeID, nodesInfo[1].nodeID}

	var tests = []struct {
		name   string
		before string
		after  string
	}{
		{"Set a no exsiting policy and it should be set to fair", "nil", "unknown"},
		{"Initialized policy set fair", "nil", "fair"},
		{"Initialized policy set binpacking", "nil", "binpacking"},
		{"Change fair with binpacking", "fair", "binpacking"},
		{"Change binpacking with fair", "binpacking", "fair"},
	}

	for _, tt := range tests {
		testname := fmt.Sprintf("%s:%s %s", tt.name, tt.before, tt.after)
		t.Run(testname, func(t *testing.T) {
			nc := NewNodeCollection("test")
			for _, nodeInfo := range nodesInfo {
				// Initialize allocation and add it to node
				node := newNode(nodeInfo.nodeID, map[string]resources.Quantity{"vcore": resources.Quantity(nodeInfo.vcore), "memory": resources.Quantity(nodeInfo.mem)})
				res := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": resources.Quantity(nodeInfo.allocatedVcore), "memory": resources.Quantity(nodeInfo.allocatedMem)})
				alloc := newAllocation("test-app-1", uuid.NewString(), "test1", "root.default", res)
				node.AddAllocation(alloc)

				// Add node to nc and make sure this operation is valid.
				err := nc.AddNode(node)
				if err != nil {
					t.Errorf("AddNode error:%s", err.Error())
				}
			}

			// nil pass, but we need to check fair and binpacking policy setting is correct.
			if tt.before != "nil" {
				policy := NewNodeSortingPolicy(tt.before, weights)
				nc.SetNodeSortingPolicy(policy)
				if policy.PolicyType().String() != tt.before {
					t.Errorf("Got %s, want %s", policy.PolicyType().String(), tt.before)
				}
			}

			// Check the order when policy is unset or set with fair or binpacking.
			nodeIterator := nc.GetNodeIterator()
			for index := 0; nodeIterator.HasNext(); index++ {
				node := nodeIterator.Next()
				if ansOrder := order[tt.before]; ansOrder[index] != node.NodeID {
					t.Errorf("%s policy, got %s, except %s", tt.before, node.NodeID, ansOrder[index])
				}
			}

			// Policy setting check that is set with fair or binpacking.
			policy := NewNodeSortingPolicy(tt.after, weights)
			nc.SetNodeSortingPolicy(policy)
			if tt.after == "unknown" {
				if policy.PolicyType().String() != "fair" {
					t.Errorf("Got %s, want %s", policy.PolicyType().String(), "fair")
				}
			} else if policy.PolicyType().String() != tt.after {
				t.Errorf("Got %s, want %s", policy.PolicyType().String(), tt.after)
			}

			// Compare node order after setting node sorting policy which contains fair or binpacking.
			nodeIterator = nc.GetNodeIterator()
			for index := 0; nodeIterator.HasNext(); index++ {
				node := nodeIterator.Next()
				var ansOrder []string
				if tt.after == "unknown" {
					ansOrder = order["fair"]
				} else {
					ansOrder = order[tt.after]
				}

				if ansOrder[index] != node.NodeID {
					t.Errorf("%s policy, got %s, except %s", tt.after, node.NodeID, ansOrder[index])
				}
			}
		})
	}
}

func TestGetNodeSortingPolicy(t *testing.T) {
	weights := map[string]float64{
		"memory": 1.0,
	}

	var nodesInfo = []struct {
		nodeID       string
		mem          int64
		allocatedMem int64
	}{
		{"node-01", 1000, 250},
		{"node-02", 1000, 500},
		{"node-03", 1000, 750},
		{"node-04", 1000, 1000},
	}

	order := make(map[string][]string, 3)
	order["nil"] = []string{nodesInfo[0].nodeID, nodesInfo[1].nodeID, nodesInfo[2].nodeID, nodesInfo[3].nodeID}
	order["fair"] = []string{nodesInfo[0].nodeID, nodesInfo[1].nodeID, nodesInfo[2].nodeID, nodesInfo[3].nodeID}
	order["binpacking"] = []string{nodesInfo[3].nodeID, nodesInfo[2].nodeID, nodesInfo[1].nodeID, nodesInfo[0].nodeID}

	var tests = []struct {
		name   string
		before string
		after  string
	}{
		{"Set fair policy and what's policy that node_collection returns", "nil", "fair"},
		{"Set binpacking policy and what's policy that node_collection returns", "nil", "binpacking"},
	}

	for _, tt := range tests {
		testname := fmt.Sprintf("%s:%s %s", tt.name, tt.before, tt.after)
		t.Run(testname, func(t *testing.T) {
			nc := NewNodeCollection("test")
			if ans := nc.GetNodeSortingPolicy(); ans != nil {
				t.Errorf("Instance policy from NewNodeCollection should be nil: Got %s", ans.PolicyType().String())
			}

			for _, nodeInfo := range nodesInfo {
				// Initialize allocation and add it to node.
				node := newNode(nodeInfo.nodeID, map[string]resources.Quantity{"memory": resources.Quantity(nodeInfo.mem)})
				res := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": resources.Quantity(nodeInfo.allocatedMem)})
				alloc := newAllocation("test-app-1", uuid.NewString(), "test1", "root.default", res)
				node.AddAllocation(alloc)

				// Add node to nc and make sure this operation is valid.
				err := nc.AddNode(node)
				if err != nil {
					t.Errorf("AddNode error:%s", err.Error())
				}
			}

			var policy NodeSortingPolicy
			if tt.before != "nil" {
				policy = NewNodeSortingPolicy(tt.before, weights)
				nc.SetNodeSortingPolicy(policy)
				if ans := nc.GetNodeSortingPolicy(); policy.PolicyType() != ans.PolicyType() {
					t.Errorf("Set initialization nodeSortingPolicy: Got %s, want %s", policy.PolicyType().String(), ans.PolicyType().String())
				}
			}

			// Check the order when policy is unset or sets with fair or binpacking.
			nodeIterator := nc.GetNodeIterator()
			for index := 0; nodeIterator.HasNext(); index++ {
				node := nodeIterator.Next()
				if ansOrder := order[tt.before]; ansOrder[index] != node.NodeID {
					t.Errorf("%s policy, got %s, except %s", tt.before, node.NodeID, ansOrder[index])
				}
			}

			policy = NewNodeSortingPolicy(tt.after, weights)
			nc.SetNodeSortingPolicy(policy)
			if ans := nc.GetNodeSortingPolicy(); policy.PolicyType() != ans.PolicyType() {
				t.Errorf("Got %s, want %s", policy.PolicyType().String(), ans.PolicyType().String())
			}

			// Check the order when policy set with fair or binpacking.
			nodeIterator = nc.GetNodeIterator()
			for index := 0; nodeIterator.HasNext(); index++ {
				node := nodeIterator.Next()
				if ansOrder := order[tt.after]; ansOrder[index] != node.NodeID {
					t.Errorf("%s policy, got %s, except %s", tt.after, node.NodeID, ansOrder[index])
				}
			}
		})
	}
}

func TestGetNodeIterator(t *testing.T) {
	// A empty baseCollection belonging to the partition, called "test".
	bc := initBaseCollection()
	var nc NodeCollection = bc
	var iter NodeIterator

	// Case 1: There are not any nodes in BC.
	if iter = nc.GetNodeIterator(); iter != nil {
		t.Error("There aren't any nodes, BC should return nil")
	}

	// Case 2: There is a unreserved node in BC
	node := newNode("node-1", map[string]resources.Quantity{"vcore": 10})
	node.AddListener(bc)
	if err := nc.AddNode(node); err != nil {
		t.Error("Adding a node into BC failed.")
	}

	if iter = nc.GetNodeIterator(); iter == nil {
		t.Error("Node iterator should not be nil.")
	} else {
		bcIter := iter.(*defaultNodeIterator)
		if size := bcIter.size; size <= 0 || size > 1 {
			t.Error("Wrong size of iter elements")
		}

		tmp := iter.Next()
		if tmp.NodeID != "node-1" {
			t.Errorf("A wrong node in node iterator is %s", tmp.NodeID)
		}
	}

	// Case 3: One node is reserved and the other one is not.
	node2 := newNode("node-2", map[string]resources.Quantity{"vcore": 5})
	node2.AddListener(bc)

	if err := nc.AddNode(node2); err != nil {
		t.Error("Adding another node into BC failed.")
	}

	app := newApplication("app-01", "default", "root.test")
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 5})
	ask := newAllocationAsk("alloc-01", "app-01", res)

	if err := node.Reserve(app, ask); err != nil {
		t.Error("Reserving failed.")
	}

	if iter = nc.GetNodeIterator(); iter == nil {
		t.Error("Node iterator should contain a node and it should not be nil")
	} else {
		bcIter := iter.(*defaultNodeIterator)
		if size := bcIter.size; size <= 0 || size > 1 {
			t.Error("Wrong size of iter elements")
		}

		tmp := iter.Next()
		if tmp.NodeID != "node-2" {
			t.Errorf("A wrong node in node iterator is %s", tmp.NodeID)
		}
	}

	// Case 4: All nodes are reserved
	app2 := newApplication("app-02", "default", "root.test")
	ask2 := newAllocationAsk("alloc-02", "app-02", res)
	if err := node2.Reserve(app2, ask2); err != nil {
		t.Error("Reserving failed.")
	}

	if iter = nc.GetNodeIterator(); iter != nil {
		t.Error("All nodes are reserved. Node iterator should be nil")
	}
}
