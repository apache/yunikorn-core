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
    "gotest.tools/assert"
    "testing"
)

func newTestPartition() (*PartitionSchedulingContext, error) {
    rootSched, err := createRootQueue()
    if err != nil {
        return nil, err
    }
    rootInfo := rootSched.CachedQueueInfo
    info := &cache.PartitionInfo{
        Name: "default",
        Root: rootInfo,
        RMId: "test",
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

    partition, err := newTestPartition()
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

    nodes := partition.getSchedulingNodes()
    assert.Equal(t, 0, len(nodes), "list should have been empty")

    partition.addSchedulingNode(cache.NewNodeForTest("test1", resources.NewResource()))
    partition.addSchedulingNode(cache.NewNodeForTest("test2", resources.NewResource()))
    // add one node that will not be returned in the node list
    node := cache.NewNodeForTest("test-filtered", resources.NewResource())
    node.SetSchedulable(false)
    partition.addSchedulingNode(node)
    assert.Equal(t, 3, len(partition.nodes), "node list not correct")
    nodes = partition.getSchedulingNodes()
    // returned list should be only two long
    assert.Equal(t, 2, len(nodes), "node list not filtered")
    // map iteration is random so don't know which we get first
    if !(nodes[0].NodeId == "test1" && nodes[1].NodeId == "test2") &&
        !(nodes[0].NodeId == "test2" && nodes[1].NodeId == "test1") {
        t.Errorf("nodes not returend as expected %v", nodes)
    }

    // get individual nodes
    schedNode := partition.getSchedulingNode("")
    if schedNode != nil {
        t.Errorf("existing node returned for nil name: %v", schedNode)
    }
    schedNode = partition.getSchedulingNode("does not exist")
    if schedNode != nil {
        t.Errorf("existing node returned for non existing name: %v", schedNode)
    }
    schedNode = partition.getSchedulingNode("test-filtered")
    if schedNode == nil || schedNode.NodeId != "test-filtered" {
        t.Error("failed to retrieve existing node")
    }
}
