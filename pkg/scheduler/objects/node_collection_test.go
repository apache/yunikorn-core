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

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
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

func TestBaseNodeCollection_GetSchedulableNodes(t *testing.T) {
	var err error
	nc := initBaseCollection()
	nodes := nc.GetSchedulableNodes(false)
	assert.Equal(t, 0, len(nodes), "list should have been empty")

	node := initNode("test1")
	node.SetSchedulable(true)
	err = nc.AddNode(node)
	assert.NilError(t, err, "test node add failed unexpected")

	nodes = nc.GetSchedulableNodes(false)
	assert.Equal(t, 1, len(nodes), "list is missing schedulable node")

	node.reservations["test"] = nil // fake a reservation
	nodes = nc.GetSchedulableNodes(false)
	assert.Equal(t, 1, len(nodes), "list is missing reserved node")

	nodes = nc.GetSchedulableNodes(true)
	assert.Equal(t, 0, len(nodes), "list is returning reserved node")
}
