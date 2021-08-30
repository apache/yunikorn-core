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

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
)

type NodeCollection interface {
	AddNode(node *Node) error
	RemoveNode(nodeID string) *Node
	GetNode(nodeID string) *Node
	GetNodeCount() int
	GetNodes() []*Node
	GetSchedulableNodes(excludeReserved bool) []*Node
	GetSchedulableNodeIterator() NodeIterator
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
	activeNodes *btree.BTree        // nodes which are active

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
	if node.IsSchedulable() && !node.IsReserved() {
		nc.activeNodes.ReplaceOrInsert(nref)
	}
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
	nc.activeNodes.Delete(*nref)
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

// Return a list of schedulable nodes (optionally excluding reserved as well).
func (nc *baseNodeCollection) GetSchedulableNodes(excludeReserved bool) []*Node {
	nc.RLock()
	defer nc.RUnlock()
	nodes := make([]*Node, 0)
	for _, nref := range nc.nodes {
		node := nref.node
		// filter out the nodes that are not scheduling
		if excludeReserved && node.IsReserved() {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes
}

// Create a node iterator for the schedulable nodes based on the policy set for this partition.
// The iterator is nil if there are no schedulable nodes available.
func (nc *baseNodeCollection) GetSchedulableNodeIterator() NodeIterator {
	sortingStart := time.Now()
	tree := nc.cloneActiveNodes()

	length := tree.Len()
	if length == 0 {
		return nil
	}

	nodes := make([]*Node, 0, length)
	tree.Ascend(func(item btree.Item) bool {
		nodes = append(nodes, item.(nodeRef).node)
		return true
	})
	metrics.GetSchedulerMetrics().ObserveNodeSortingLatency(sortingStart)
	return NewDefaultNodeIterator(nodes)
}

func (nc *baseNodeCollection) cloneActiveNodes() *btree.BTree {
	nc.Lock()
	defer nc.Unlock()

	return nc.activeNodes.Clone()
}

// Sets the node sorting policy.
func (nc *baseNodeCollection) SetNodeSortingPolicy(policy NodeSortingPolicy) {
	nc.Lock()
	defer nc.Unlock()
	nc.nsp = policy

	// activeNodes must be rebuilt since sort ordering is different
	nc.activeNodes.Clear(false)
	for _, nref := range nc.nodes {
		node := nref.node
		nref.nodeScore = nc.scoreNode(node)
		if !node.IsSchedulable() || node.IsReserved() {
			continue
		}
		nc.activeNodes.ReplaceOrInsert(*nref)
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

	nc.activeNodes.Delete(*nref)
	nref.nodeScore = nc.scoreNode(node)
	if !node.IsSchedulable() || node.IsReserved() {
		return
	}
	nc.activeNodes.ReplaceOrInsert(*nref)
}

// Create a new collection for the given partition.
func NewNodeCollection(partition string) NodeCollection {
	return &baseNodeCollection{
		Partition:   partition,
		nsp:         nil,
		nodes:       make(map[string]*nodeRef),
		activeNodes: btree.New(7), // Degree=7 here is experimentally the most efficient for up to around 5k nodes
	}
}
