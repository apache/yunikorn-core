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
	"sync"
	"time"

	"github.com/google/btree"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
)

type NodeCollection interface {
	AddNode(node *Node) error
	RemoveNode(nodeID string) *Node
	GetNode(nodeID string) *Node
	GetNodeCount() int
	GetNodes() []*Node
	GetNodeIterator() NodeIterator
	GetFullNodeIterator() NodeIterator
	SetNodeSortingPolicy(policy NodeSortingPolicy)
	GetNodeSortingPolicy() NodeSortingPolicy
}

type nodeRef struct {
	node      *Node   // node reference
	nodeScore float64 // node score
}

func (nr nodeRef) Less(than btree.Item) bool {
	other, ok := than.(nodeRef)
	if !ok {
		return false
	}
	if nr.nodeScore < other.nodeScore {
		return true
	}
	if other.nodeScore < nr.nodeScore {
		return false
	}
	return nr.node.NodeID < other.node.NodeID
}

type baseNodeCollection struct {
	Partition string // partition used with this collection

	// Private fields need protection
	nsp         NodeSortingPolicy   // node sorting policy
	nodes       map[string]*nodeRef // nodes assigned to this collection
	sortedNodes *btree.BTree        // nodes sorted by score

	sync.RWMutex
}

func (nc *baseNodeCollection) scoreNode(node *Node) float64 {
	if nc.nsp == nil {
		return 0
	}
	return nc.nsp.ScoreNode(node)
}

// Add a node to the collection by nodeID.
func (nc *baseNodeCollection) AddNode(node *Node) error {
	nc.Lock()
	defer nc.Unlock()

	if node == nil {
		return fmt.Errorf("node cannot be nil")
	}
	if nc.nodes[node.NodeID] != nil {
		return fmt.Errorf("partition %s has an existing node %s, node name must be unique", nc.Partition, node.NodeID)
	}
	// Node can be added to the system to allow processing of the allocations
	node.AddListener(nc)
	nref := nodeRef{
		node:      node,
		nodeScore: nc.scoreNode(node),
	}
	nc.nodes[node.NodeID] = &nref
	nc.sortedNodes.ReplaceOrInsert(nref)
	return nil
}

// Remove a node from the collection by nodeID.
func (nc *baseNodeCollection) RemoveNode(nodeID string) *Node {
	nc.Lock()
	defer nc.Unlock()
	nref := nc.nodes[nodeID]
	if nref == nil {
		log.Logger().Debug("node was not found, node already removed",
			zap.String("nodeID", nodeID),
			zap.String("partition", nc.Partition))
		return nil
	}

	// Remove node from list of tracked nodes
	nc.sortedNodes.Delete(*nref)
	delete(nc.nodes, nodeID)
	nref.node.RemoveListener(nc)

	return nref.node
}

// Get a node from the collection by nodeID.
func (nc *baseNodeCollection) GetNode(nodeID string) *Node {
	nc.RLock()
	defer nc.RUnlock()
	nref := nc.nodes[nodeID]
	if nref == nil {
		return nil
	}
	return nref.node
}

// Get the count of nodes
func (nc *baseNodeCollection) GetNodeCount() int {
	nc.RLock()
	defer nc.RUnlock()
	return len(nc.nodes)
}

// Return a list of nodes.
func (nc *baseNodeCollection) GetNodes() []*Node {
	nc.RLock()
	defer nc.RUnlock()
	nodes := make([]*Node, 0, len(nc.nodes))
	for _, nref := range nc.nodes {
		nodes = append(nodes, nref.node)
	}
	return nodes
}

// Create an ordered node iterator for unreserved nodes based on the sort policy set for this collection.
// The iterator is nil if there are no unreserved nodes available.
func (nc *baseNodeCollection) GetNodeIterator() NodeIterator {
	return nc.getNodeIteratorInternal(func(node *Node) bool {
		return !node.IsReserved()
	})
}

// Create an ordered node iterator for all nodes based on the sort policy set for this collection.
// The iterator is nil if there are no nodes available.
func (nc *baseNodeCollection) GetFullNodeIterator() NodeIterator {
	return nc.getNodeIteratorInternal(func(node *Node) bool {
		return true
	})
}

func (nc *baseNodeCollection) getNodeIteratorInternal(allow func(*Node) bool) NodeIterator {
	sortingStart := time.Now()
	tree := nc.cloneSortedNodes()

	length := tree.Len()
	if length == 0 {
		return nil
	}

	nodes := make([]*Node, 0, length)
	tree.Ascend(func(item btree.Item) bool {
		node := item.(nodeRef).node
		if allow(node) {
			nodes = append(nodes, node)
		}
		return true
	})
	metrics.GetSchedulerMetrics().ObserveNodeSortingLatency(sortingStart)

	if len(nodes) == 0 {
		return nil
	}

	return NewDefaultNodeIterator(nodes)
}

func (nc *baseNodeCollection) cloneSortedNodes() *btree.BTree {
	nc.Lock()
	defer nc.Unlock()

	return nc.sortedNodes.Clone()
}

// Sets the node sorting policy.
func (nc *baseNodeCollection) SetNodeSortingPolicy(policy NodeSortingPolicy) {
	nc.Lock()
	defer nc.Unlock()
	nc.nsp = policy

	// sortedNodes must be rebuilt since sort ordering is different
	nc.sortedNodes.Clear(false)
	for _, nref := range nc.nodes {
		node := nref.node
		nref.nodeScore = nc.scoreNode(node)
		nc.sortedNodes.ReplaceOrInsert(*nref)
	}
}

// Gets the node sorting policy.
func (nc *baseNodeCollection) GetNodeSortingPolicy() NodeSortingPolicy {
	nc.RLock()
	defer nc.RUnlock()
	return nc.nsp
}

// Callback method triggered when a node is updated.
func (nc *baseNodeCollection) NodeUpdated(node *Node) {
	nc.Lock()
	defer nc.Unlock()

	nref := nc.nodes[node.NodeID]
	if nref == nil {
		return
	}

	updatedScore := nc.scoreNode(node)
	if nref.nodeScore != updatedScore {
		nc.sortedNodes.Delete(*nref)
		nref.nodeScore = nc.scoreNode(node)
		nc.sortedNodes.ReplaceOrInsert(*nref)
	}
}

// Create a new collection for the given partition.
func NewNodeCollection(partition string) NodeCollection {
	return &baseNodeCollection{
		Partition:   partition,
		nsp:         NewNodeSortingPolicy(policies.FairSortPolicy.String(), nil),
		nodes:       make(map[string]*nodeRef),
		sortedNodes: btree.New(7), // Degree=7 here is experimentally the most efficient for up to around 5k nodes
	}
}
