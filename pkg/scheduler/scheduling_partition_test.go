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

package scheduler

import (
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
)

func newTestPartition() (*partitionSchedulingContext, error) {
	rootSched, err := createRootQueue()
	if err != nil {
		return nil, err
	}
	rootInfo := rootSched.CachedQueueInfo
	info := &cache.PartitionInfo{
		Name: "default",
		Root: rootInfo,
		RmID: "test",
	}

	return newPartitionSchedulingContext(info, rootSched), nil
}

func TestNewPartition(t *testing.T) {
	partition := newPartitionSchedulingContext(nil, nil)
	if partition != nil {
		t.Fatal("nil input should not have returned partition")
	}
	partition = newPartitionSchedulingContext(&cache.PartitionInfo{}, nil)
	if partition != nil {
		t.Fatal("nil root queue should not have returned partition")
	}

	_, err := newTestPartition()
	if err != nil {
		t.Errorf("test partition create failed with error: %v ", err)
	}
}

func TestAddNode(t *testing.T) {
	partition, err := newTestPartition()
	if err != nil {
		t.Fatalf("test partition create failed with error: %v ", err)
	}
	partition.addSchedulingNode(nil)
	assert.Equal(t, 0, len(partition.nodes), "nil node should not be added")
	node := cache.NewNodeForTest("test1", resources.NewResource())
	partition.addSchedulingNode(node)
	assert.Equal(t, 1, len(partition.nodes), "node list not correct")
	// add the same node nothing changes
	partition.addSchedulingNode(node)
	assert.Equal(t, 1, len(partition.nodes), "node list not correct")
	node = cache.NewNodeForTest("test2", resources.NewResource())
	partition.addSchedulingNode(node)
	assert.Equal(t, 2, len(partition.nodes), "node list not correct")
}

func TestRemoveNode(t *testing.T) {
	partition, err := newTestPartition()
	if err != nil {
		t.Fatalf("test partition create failed with error: %v ", err)
	}
	partition.addSchedulingNode(cache.NewNodeForTest("test", resources.NewResource()))
	assert.Equal(t, 1, len(partition.nodes), "node list not correct")

	// remove non existing node
	partition.removeSchedulingNode("")
	assert.Equal(t, 1, len(partition.nodes), "nil node should not remove anything")
	partition.removeSchedulingNode("does not exist")
	assert.Equal(t, 1, len(partition.nodes), "non existing node was removed")

	partition.removeSchedulingNode("test")
	assert.Equal(t, 0, len(partition.nodes), "node was not removed")
}

func TestGetNodes(t *testing.T) {
	partition, err := newTestPartition()
	if err != nil {
		t.Fatalf("test partition create failed with error: %v ", err)
	}

	nodes := partition.getSchedulableNodes()
	assert.Equal(t, 0, len(nodes), "list should have been empty")

	node1 := "node-1"
	partition.addSchedulingNode(cache.NewNodeForTest(node1, resources.NewResource()))
	node2 := "node-2"
	partition.addSchedulingNode(cache.NewNodeForTest(node2, resources.NewResource()))
	// add one node that will not be returned in the node list
	node3 := "notScheduling"
	node := cache.NewNodeForTest(node3, resources.NewResource())
	node.SetSchedulable(false)
	partition.addSchedulingNode(node)
	node4 := "reserved"
	node = cache.NewNodeForTest(node4, resources.NewResource())
	partition.addSchedulingNode(node)
	// get individual nodes
	schedNode := partition.getSchedulingNode("")
	if schedNode != nil {
		t.Errorf("existing node returned for nil name: %v", schedNode)
	}
	schedNode = partition.getSchedulingNode("does not exist")
	if schedNode != nil {
		t.Errorf("existing node returned for non existing name: %v", schedNode)
	}
	schedNode = partition.getSchedulingNode(node3)
	if schedNode == nil || schedNode.NodeID != node3 {
		t.Error("failed to retrieve existing non scheduling node")
	}
	schedNode = partition.getSchedulingNode(node4)
	schedNode.reservations["app-1|alloc-1"] = &reservation{"", "app-1", "alloc-1"}
	if schedNode == nil || schedNode.NodeID != node4 {
		t.Error("failed to retrieve existing reserved node")
	}

	assert.Equal(t, 4, len(partition.nodes), "node list not correct")
	nodes = partition.getSchedulableNodes()
	// returned list should be only two long
	assert.Equal(t, 2, len(nodes), "node list not filtered")
	// map iteration is random so don't know which we get first
	for _, schedNode = range nodes {
		if schedNode.NodeID != node1 && schedNode.NodeID != node2 {
			t.Fatalf("unexpected node returned in list: %s", node.NodeID)
		}
	}
	nodes = partition.getSchedulingNodes(false)
	// returned list should be 3 long: no reserved filter
	assert.Equal(t, 3, len(nodes), "node list was incorrectly filtered")
	// check if we have all nodes: since there is a backing map we cannot have duplicates
	for _, schedNode = range nodes {
		if schedNode.NodeID == node3 {
			t.Fatalf("unexpected node returned in list: %s", node.NodeID)
		}
	}
}
