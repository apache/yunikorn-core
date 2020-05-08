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
	"strconv"
	"testing"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
)

func createTestPartitionContext(t *testing.T, nodeEvaluator *configs.NodeEvaluatorConfig,
	nodeSortingAlgorithm *configs.NodeSortingAlgorithmConfig) *partitionSchedulingContext {
	rootSched, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue")
	info := &cache.PartitionInfo{
		Name: "default",
		Root: rootSched.QueueInfo,
		RmID: "test",
	}
	if nodeEvaluator != nil {
		info.SetNodeEvaluator(nodeEvaluator)
	}
	if nodeSortingAlgorithm != nil {
		info.SetNodeSortingAlgorithm(nodeSortingAlgorithm)
	}
	partitionCtx := newPartitionSchedulingContext(info, rootSched)
	return partitionCtx
}

func initTestSchedulingNodes(partitionCtx *partitionSchedulingContext, numNodes int,
	totalRes *resources.Resource, setAllocatingResFunc func(i int) *resources.Resource) {
	for i := 0; i < numNodes; i++ {
		nodeID := "node-" + strconv.Itoa(i+1)
		partitionCtx.addSchedulingNode(cache.NewNodeForTest(nodeID, totalRes))
		schedulingNode := partitionCtx.getSchedulingNode(nodeID)
		schedulingNode.allocating = setAllocatingResFunc(i)
		schedulingNode.nodeResourceUpdated()
		// notify algorithm immediately
		partitionCtx.subjectManager.NotifyEvent(NodeSubject, &NodeSubjectResourceUpdateEvent{node: schedulingNode})
	}
}

func testNodeSortingAlgorithm(t *testing.T, nodeEvaluator *configs.NodeEvaluatorConfig,
	nodeSortingAlgorithm *configs.NodeSortingAlgorithmConfig, numNodes int, totalRes *resources.Resource,
	setAllocatingResFunc func(i int) *resources.Resource,
	request *schedulingAllocationAsk, expectedSortedNodeIDs []string) {
	// init partition context
	partitionCtx := createTestPartitionContext(t, nodeEvaluator, nodeSortingAlgorithm)
	// init scheduling nodes
	initTestSchedulingNodes(partitionCtx, numNodes, totalRes, setAllocatingResFunc)
	// verify nodes are sorted as expected
	time.Sleep(time.Millisecond)
	nodeIt := partitionCtx.nodeSortingAlgorithm.getNodeIterator(request)
	sortedNodeIDs := make([]string, 0)
	for nodeIt.HasNext() {
		node := nodeIt.Next()
		sortedNodeIDs = append(sortedNodeIDs, node.NodeID)
	}
	assert.DeepEqual(t, sortedNodeIDs, expectedSortedNodeIDs)
}

func getSuccessiveTestNodeIDs(startIndex, endIndex int) []string {
	nodeIDs := make([]string, 0)
	if startIndex >= endIndex {
		for i := startIndex; i >= endIndex; i-- {
			nodeIDs = append(nodeIDs, "node-"+strconv.Itoa(i))
		}
	} else {
		for i := startIndex; i <= endIndex; i++ {
			nodeIDs = append(nodeIDs, "node-"+strconv.Itoa(i))
		}
	}
	return nodeIDs
}

type TestCase struct {
	description           string
	nodeEvaluator         *configs.NodeEvaluatorConfig
	numNodes              int
	totalRes              *resources.Resource
	setAllocatingResFunc  func(i int) *resources.Resource
	request               *schedulingAllocationAsk
	expectedSortedNodeIDs []string
}

func getTestAlgorithmConfigs(t *testing.T) []*configs.NodeSortingAlgorithmConfig {
	defaultAlgorithm, err := common.GetNodeSortingAlgorithm(DefaultNodeSortingAlgorithmName, nil)
	assert.NilError(t, err)
	incrementalAlgorithm, err := common.GetNodeSortingAlgorithm(IncrementalNodeSortingAlgorithmName, nil)
	assert.NilError(t, err)
	return []*configs.NodeSortingAlgorithmConfig{
		{
			Name:      DefaultNodeSortingAlgorithmName,
			Algorithm: defaultAlgorithm,
		},
		{
			Name:      IncrementalNodeSortingAlgorithmName,
			Algorithm: incrementalAlgorithm,
		},
	}
}

func getTestName(testCase *TestCase, algorithmConfig *configs.NodeSortingAlgorithmConfig) string {
	var nodeScorerNames string
	if testCase.nodeEvaluator != nil {
		for _, sc := range testCase.nodeEvaluator.ScorerConfigs {
			if nodeScorerNames != "" {
				nodeScorerNames += ","
			}
			nodeScorerNames += sc.ScorerName
		}
	}
	return fmt.Sprintf("Algorithm:%s/Scorers:%s/Desc:%s",
		algorithmConfig.Name, nodeScorerNames, testCase.description)
}

func testNodeSortingAlgorithms(t *testing.T, testCases []*TestCase) {
	algorithms := getTestAlgorithmConfigs(t)
	for _, algorithm := range algorithms {
		for _, testCase := range testCases {
			testName := getTestName(testCase, algorithm)
			t.Run(testName, func(t *testing.T) {
				testNodeSortingAlgorithm(t, testCase.nodeEvaluator, algorithm,
					testCase.numNodes, testCase.totalRes, testCase.setAllocatingResFunc,
					testCase.request, testCase.expectedSortedNodeIDs)
			})
		}
	}
}

func TestAlgorithmWithDominantResourceUsageScorer(t *testing.T) {
	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 1000, "vcore": 1000})
	dominantResourceScorerConfig := &configs.NodeScorerConfig{
		ScorerName: DominantResourceUsageScorerName,
		Weight:     1,
		Conf:       map[string]interface{}{"resourceNames": []string{"memory", "vcore"}},
	}
	testCases := []*TestCase{
		// allocating memory is increasing as nodeID grows,
		// nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description: "allocating memory is increasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{dominantResourceScorerConfig},
			},
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocating memory is decreasing as nodeID grows,
		// nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description: "allocating memory is decreasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{dominantResourceScorerConfig},
			},
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		// allocating vcore is increasing as nodeID grows,
		// nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description: "allocating vcore is increasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{dominantResourceScorerConfig},
			},
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"vcore": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocating vcore is decreasing as nodeID grows,
		// nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description: "allocating vcore is decreasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{dominantResourceScorerConfig},
			},
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"vcore": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
	}
	testNodeSortingAlgorithms(t, testCases)
}

func TestAlgorithmWithResourceUsageScorer(t *testing.T) {
	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 1000, "vcore": 1000})
	memoryResourceUsageScorerConfig := &configs.NodeScorerConfig{
		ScorerName: ResourceUsageScorerName,
		Weight:     1,
		Conf:       map[string]interface{}{"resourceName": "memory"},
	}
	vcoreResourceUsageScorerConfig := &configs.NodeScorerConfig{
		ScorerName: ResourceUsageScorerName,
		Weight:     1,
		Conf:       map[string]interface{}{"resourceName": "vcore"},
	}
	testCases := []*TestCase{
		// allocating memory is increasing as nodeID grows,
		// nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description: "allocating memory is increasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{memoryResourceUsageScorerConfig},
			},
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocating memory is decreasing as nodeID grows,
		// nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description: "allocating memory is decreasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{memoryResourceUsageScorerConfig},
			},
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		// allocating vcore is increasing as nodeID grows,
		// nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description: "allocating vcore is increasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{vcoreResourceUsageScorerConfig},
			},
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"vcore": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocating vcore is decreasing as nodeID grows,
		// nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description: "allocating vcore is decreasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{vcoreResourceUsageScorerConfig},
			},
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"vcore": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
	}
	testNodeSortingAlgorithms(t, testCases)
}

func TestAlgorithmWithScarceResourceUsageScorer(t *testing.T) {
	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 1000, "vcore": 1000, "gpu": 1000})
	scarceResourceUsageScorerConfig := &configs.NodeScorerConfig{
		ScorerName: ScarceResourceUsageScorerName,
		Weight:     1,
		Conf:       map[string]interface{}{"scarceResourceName": "gpu"},
	}
	testCases := []*TestCase{
		// allocating gpu is increasing as nodeID grows,
		// request gpu resource then nodes should be sorted in ascending order: [node-1, node-2, ..., node-10]
		{
			description: "allocating memory is increasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{scarceResourceUsageScorerConfig},
			},
			request: newAllocationAsk("alloc-1", "app-1",
				resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1, "vcore": 1, "gpu": 1})),
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"gpu": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocating gpu is decreasing as nodeID grows,
		// request gpu resource then nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		{
			description: "allocating memory is decreasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{scarceResourceUsageScorerConfig},
			},
			request: newAllocationAsk("alloc-1", "app-1",
				resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1, "vcore": 1, "gpu": 1})),
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"gpu": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
	}
	testNodeSortingAlgorithms(t, testCases)
}

func TestAlgorithmWithMixedResourceUsageScorers(t *testing.T) {
	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 1000, "vcore": 1000, "gpu": 1000})
	dominantResourceScorerConfig := &configs.NodeScorerConfig{
		ScorerName: DominantResourceUsageScorerName,
		Weight:     1,
		Conf:       map[string]interface{}{"resourceNames": []string{"memory", "vcore"}},
	}
	scarceResourceUsageScorerConfig := &configs.NodeScorerConfig{
		ScorerName: ScarceResourceUsageScorerName,
		Weight:     3,
		Conf:       map[string]interface{}{"scarceResourceName": "gpu"},
	}
	testCases := []*TestCase{
		// allocating memory is increasing and allocating gpu is decreasing as nodeID grows,
		// request gpu resource then nodes should be sorted in descending order: [node-10, node-9, ..., node-1]
		// since weight of scarce_resource_scorer is greater than that of dominant_resource_scorer
		{
			description: "allocating memory is increasing and allocating gpu is decreasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{
					dominantResourceScorerConfig, scarceResourceUsageScorerConfig},
			},
			request: newAllocationAsk("alloc-1", "app-1",
				resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1, "vcore": 1, "gpu": 1})),
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(i), "gpu": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(10, 1),
		},
		// allocating memory is decreasing and allocating gpu is increasing as nodeID grows,
		// request gpu resource then nodes should be sorted in increasing order: [node-1, node-2, ..., node-10]
		// since weight of scarce_resource_scorer is greater than that of dominant_resource_scorer
		{
			description: "allocating memory is decreasing and allocating gpu is increasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{
					dominantResourceScorerConfig, scarceResourceUsageScorerConfig},
			},
			request: newAllocationAsk("alloc-1", "app-1",
				resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1, "vcore": 1, "gpu": 1})),
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(1000 - i), "gpu": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: getSuccessiveTestNodeIDs(1, 10),
		},
		// allocating memory is decreasing as nodeID grows,
		// the latter half of nodes have available gpu resource which is increasing as nodeID grows,
		// request gpu resource then nodes should be sorted like this:
		// [node-6, node-7, ..., node-10, node-5, node-4, ... node-1]
		// since weight of scarce_resource_scorer is greater than that of dominant_resource_scorer
		{
			description: "allocating memory is decreasing, the latter half of nodes have available gpu resource " +
				"which is increasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{
					dominantResourceScorerConfig, scarceResourceUsageScorerConfig},
			},
			request: newAllocationAsk("alloc-1", "app-1",
				resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1, "vcore": 1, "gpu": 1})),
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				if i < 5 {
					return resources.NewResourceFromMap(
						map[string]resources.Quantity{"memory": resources.Quantity(1000 - i), "gpu": resources.Quantity(1000)})
				}
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(1000 - i), "gpu": resources.Quantity(i)})
			},
			expectedSortedNodeIDs: append(getSuccessiveTestNodeIDs(6, 10),
				getSuccessiveTestNodeIDs(5, 1)...),
		},
		// allocating memory is increasing as nodeID grows,
		// the latter half of nodes have available gpu resource which is decreasing as nodeID grows,
		// request gpu resource then nodes should be sorted like this:
		// [node-10, node-9, ..., node-6, node-1, node-2, ... node-5]
		// since weight of scarce_resource_scorer is greater than that of dominant_resource_scorer
		{
			description: "allocating memory is increasing, the latter half of nodes have available gpu resource " +
				"which is decreasing as nodeID grows",
			nodeEvaluator: &configs.NodeEvaluatorConfig{
				ScorerConfigs: []*configs.NodeScorerConfig{
					dominantResourceScorerConfig, scarceResourceUsageScorerConfig},
			},
			request: newAllocationAsk("alloc-1", "app-1",
				resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1, "vcore": 1, "gpu": 1})),
			numNodes: 10,
			totalRes: totalRes,
			setAllocatingResFunc: func(i int) *resources.Resource {
				if i < 5 {
					return resources.NewResourceFromMap(
						map[string]resources.Quantity{"memory": resources.Quantity(i), "gpu": resources.Quantity(1000)})
				}
				return resources.NewResourceFromMap(
					map[string]resources.Quantity{"memory": resources.Quantity(i), "gpu": resources.Quantity(1000 - i)})
			},
			expectedSortedNodeIDs: append(getSuccessiveTestNodeIDs(10, 6),
				getSuccessiveTestNodeIDs(1, 5)...),
		},
	}
	testNodeSortingAlgorithms(t, testCases)
}
