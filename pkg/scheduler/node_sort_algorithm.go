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
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/google/btree"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

const (
	DefaultNodeSortingAlgorithmName     = "default"
	IncrementalNodeSortingAlgorithmName = "incremental"
)

func init() {
	var DefaultNodeSortingAlgorithmMaker common.NewNodeSortingAlgorithmFunc = NewDefaultNodeSortingAlgorithm
	var IncrementalNodeSortingAlgorithmMaker common.NewNodeSortingAlgorithmFunc = NewIncrementalNodeSortingAlgorithm
	common.RegisterNodeSortingAlgorithmMaker(DefaultNodeSortingAlgorithmName, DefaultNodeSortingAlgorithmMaker)
	common.RegisterNodeSortingAlgorithmMaker(IncrementalNodeSortingAlgorithmName, IncrementalNodeSortingAlgorithmMaker)
}

// NodeSortingAlgorithm is an interface which can provide iterator of sorted nodes
// for the scheduler according to specified request.
type NodeSortingAlgorithm interface {
	init(psc *partitionSchedulingContext)
	getNodeIterator(request *schedulingAllocationAsk) NodeIterator
}

// NodeScore is a struct with node name and score.
type NodeScore struct {
	Node      *SchedulingNode
	Score     int64
	SubScores map[string]int64
}

type DefaultNodeSortingAlgorithm struct {
	psc *partitionSchedulingContext
}

func NewDefaultNodeSortingAlgorithm(conf map[string]interface{}) (interface{}, error) {
	return &DefaultNodeSortingAlgorithm{}, nil
}

func (dnsa *DefaultNodeSortingAlgorithm) init(psc *partitionSchedulingContext) {
	dnsa.psc = psc
}

func (dnsa *DefaultNodeSortingAlgorithm) getNodeIterator(request *schedulingAllocationAsk) NodeIterator {
	validDynamicScorerConfigs := dnsa.psc.nodeEvaluator.GetValidDynamicScorerConfigs(request)
	// prepare node scores
	nodes := dnsa.psc.getSchedulableNodes()
	nodeScores := make([]*NodeScore, len(nodes))
	for i, node := range nodes {
		score, subScores := dnsa.psc.nodeEvaluator.CalculateScore(validDynamicScorerConfigs, node)
		nodeScores[i] = &NodeScore{
			Node:      node,
			Score:     score,
			SubScores: subScores,
		}
	}
	// sort by node sorting policy
	switch dnsa.psc.partition.GetNodeSortingPolicy() {
	case common.FairnessPolicy:
		sort.SliceStable(nodeScores, func(i, j int) bool {
			return nodeScores[i].Score > nodeScores[j].Score
		})
	case common.BinPackingPolicy:
		sort.SliceStable(nodeScores, func(i, j int) bool {
			return nodeScores[i].Score < nodeScores[j].Score
		})
	}
	// get iterator for sorted nodes
	sortedNodes := make([]*SchedulingNode, len(nodeScores))
	for i, ns := range nodeScores {
		sortedNodes[i] = ns.Node
	}
	return NewDefaultNodeIterator(sortedNodes)
}

type IncrementalNodeSortingAlgorithm struct {
	policy       common.SortingPolicy
	evaluator    NodeEvaluator
	nodesManager *IncrementalNodesManager
	updatedNodes map[string]*SchedulingNode
	sync.RWMutex
}

// Snapshot of scheduling node used for sorting nodes based on btree,
// cachedScore is involved in the comparison between btree items,
// which should be unchangeable so that sorter can keep and manage a stable btree.
type NodeSnapshot struct {
	node            *SchedulingNode
	cachedScore     int64
	cachedSubScores map[string]int64
}

// Comparing cachedAvailableResource and nodeId (to make sure all nodes are independent items in btree)
func (ns *NodeSnapshot) Less(than btree.Item) bool {
	if ns.cachedScore != than.(*NodeSnapshot).cachedScore {
		return ns.cachedScore < than.(*NodeSnapshot).cachedScore
	}
	return strings.Compare(ns.node.NodeID, than.(*NodeSnapshot).node.NodeID) < 0
}

type IncrementalNodesManager struct {
	nodes    map[string]*NodeSnapshot
	nodeTree *btree.BTree
}

func NewIncrementalNodesManager() *IncrementalNodesManager {
	return &IncrementalNodesManager{
		nodes:    make(map[string]*NodeSnapshot),
		nodeTree: btree.New(32),
	}
}

func (inm *IncrementalNodesManager) clone() *IncrementalNodesManager {
	return &IncrementalNodesManager{
		nodes:    inm.nodes,
		nodeTree: inm.nodeTree.Clone(),
	}
}

func (inm *IncrementalNodesManager) addNode(node *SchedulingNode, score int64, subScores map[string]int64) {
	// add node
	nodeSnapshot := &NodeSnapshot{
		node:            node,
		cachedScore:     score,
		cachedSubScores: subScores,
	}
	inm.nodeTree.ReplaceOrInsert(nodeSnapshot)
	inm.nodes[node.NodeID] = nodeSnapshot
}

func (inm *IncrementalNodesManager) removeNode(nodeID string) *NodeSnapshot {
	if nodeSnapshot, ok := inm.nodes[nodeID]; ok {
		inm.nodeTree.Delete(nodeSnapshot)
		delete(inm.nodes, nodeID)
		return nodeSnapshot
	}
	return nil
}

func (inm *IncrementalNodesManager) updateNode(node *SchedulingNode, score int64, subScores map[string]int64) {
	// remove node first, then add it again
	inm.removeNode(node.NodeID)
	inm.addNode(node, score, subScores)
}

func (inm *IncrementalNodesManager) getSortedNodes(policy common.SortingPolicy) []*SchedulingNode {
	sortedNodes := make([]*SchedulingNode, inm.nodeTree.Len())
	var i = 0
	switch policy {
	case common.FairnessPolicy:
		inm.nodeTree.Descend(func(item btree.Item) bool {
			node := item.(*NodeSnapshot).node
			if node.isReserved() {
				return true
			}
			sortedNodes[i] = node
			i++
			return true
		})
	case common.BinPackingPolicy:
		inm.nodeTree.Ascend(func(item btree.Item) bool {
			node := item.(*NodeSnapshot).node
			if node.isReserved() {
				return true
			}
			sortedNodes[i] = node
			i++
			return true
		})
	}
	return sortedNodes[:i]
}

func NewIncrementalNodeSortingAlgorithm(conf map[string]interface{}) (interface{}, error) {
	return &IncrementalNodeSortingAlgorithm{}, nil
}

func (insa *IncrementalNodeSortingAlgorithm) HandleSubjectEvent(subjectType SubjectType, event interface{}) {
	insa.Lock()
	defer insa.Unlock()
	if subjectType == NodeSubject {
		switch v := event.(type) {
		case *NodeSubjectAddEvent:
			score, subScores := insa.evaluator.CalculateStaticScore(v.node)
			insa.nodesManager.addNode(v.node, score, subScores)
		case *NodeSubjectRemoveEvent:
			insa.nodesManager.removeNode(v.nodeID)
		case *NodeSubjectResourceUpdateEvent:
			insa.updatedNodes[v.node.NodeID] = v.node
			log.Logger().Debug("Node resource updated", zap.String("nodeID", v.node.NodeID))
		default:
			panic(fmt.Sprintf("%s is not an acceptable type for node subject event.", reflect.TypeOf(v).String()))
		}
	}
}

func (insa *IncrementalNodeSortingAlgorithm) init(psc *partitionSchedulingContext) {
	insa.Lock()
	defer insa.Unlock()
	insa.policy = psc.partition.GetNodeSortingPolicy()
	insa.evaluator = psc.nodeEvaluator
	insa.nodesManager = NewIncrementalNodesManager()
	insa.updatedNodes = make(map[string]*SchedulingNode)
	psc.subjectManager.RegisterObserver(NodeSubject, insa)
}

func (insa *IncrementalNodeSortingAlgorithm) getNodeIterator(request *schedulingAllocationAsk) NodeIterator {
	insa.Lock()
	defer insa.Unlock()
	// update node
	testMap := make(map[string]int64)
	for _, node := range insa.updatedNodes {
		score, subScores := insa.evaluator.CalculateStaticScore(node)
		insa.nodesManager.updateNode(node, score, subScores)
		testMap[node.NodeID] = score
		delete(insa.updatedNodes, node.NodeID)
	}
	// resort nodes with non-zero dynamic score
	var nodesManager *IncrementalNodesManager
	validDynamicScorerConfigs := insa.evaluator.GetValidDynamicScorerConfigs(request)
	if len(validDynamicScorerConfigs) > 0 {
		nodesManager = insa.nodesManager.clone()
		// prepare update nodes
		updateNodes := make([]*NodeSnapshot, 0)
		for _, nodeSnapshot := range nodesManager.nodes {
			score, subScores := insa.evaluator.CalculateDynamicScore(validDynamicScorerConfigs, nodeSnapshot.node)
			if score != 0 {
				mergedSubScores := make(map[string]int64)
				for subScorer, subScore := range nodeSnapshot.cachedSubScores {
					mergedSubScores[subScorer] = subScore
				}
				for subScorer, subScore := range subScores {
					mergedSubScores[subScorer] = subScore
				}
				updateNodes = append(updateNodes, &NodeSnapshot{
					node:            nodeSnapshot.node,
					cachedScore:     nodeSnapshot.cachedScore + score,
					cachedSubScores: mergedSubScores,
				})
			}
		}
		//TODO: If too many nodes have non-zero dynamic score,
		// maybe we should considering other sorting approaches.
		for _, updatedNodeSnapshot := range updateNodes {
			nodesManager.updateNode(updatedNodeSnapshot.node,
				updatedNodeSnapshot.cachedScore, updatedNodeSnapshot.cachedSubScores)
		}
	} else {
		nodesManager = insa.nodesManager
	}
	// return iterator of sorted nodes
	nodes := nodesManager.getSortedNodes(insa.policy)
	return NewDefaultNodeIterator(nodes)
}
