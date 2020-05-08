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

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
)

func TestDefaultNodeEvaluator(t *testing.T) {
	// default node evaluator without specified configs
	defualtNodeEvaluator := NewDefaultNodeEvaluator(nil)
	// default node evaluator with specified config: dominant_resource_usage
	dominantResourceScorerConfig := &configs.NodeScorerConfig{
		ScorerName: DominantResourceUsageScorerName,
		Weight:     1,
		Conf:       map[string]interface{}{"resourceNames": []string{"memory", "vcore"}},
	}
	nodeEvaluatorWithDominantScorer := NewDefaultNodeEvaluator(&configs.NodeEvaluatorConfig{
		ScorerConfigs: []*configs.NodeScorerConfig{dominantResourceScorerConfig},
	})
	// default node evaluator with specified config: dominant_resource_usage and scarce_resource_usage
	scarceResourceScorerConfig := &configs.NodeScorerConfig{
		ScorerName: ScarceResourceUsageScorerName,
		Weight:     1,
		Conf:       map[string]interface{}{"scarceResourceName": "gpu"},
	}
	nodeEvaluatorWithDominantAndScarceScorers := NewDefaultNodeEvaluator(&configs.NodeEvaluatorConfig{
		ScorerConfigs: []*configs.NodeScorerConfig{dominantResourceScorerConfig, scarceResourceScorerConfig},
	})
	// Test cases for node with total resource: <memory:100, vcore:100, gpu:100>
	node := newNode("node-1", map[string]resources.Quantity{"memory": 100, "vcore": 100, "gpu": 100})
	// node available resource: <memory:30, vcore:20, gpu:50>
	node.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 70, "vcore": 80, "gpu": 50})
	score, _ := defualtNodeEvaluator.(NodeEvaluator).CalculateScore(nil, node)
	assert.Equal(t, score, MaxNodeScore*2/10)
	score, _ = nodeEvaluatorWithDominantScorer.(NodeEvaluator).CalculateScore(nil, node)
	assert.Equal(t, score, MaxNodeScore*2/10)
	request := newAllocationAsk("alloc-1", "app-1",
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1, "vcore": 1, "gpu": 1}))
	score, _ = nodeEvaluatorWithDominantAndScarceScorers.(NodeEvaluator).CalculateScore(
		nodeEvaluatorWithDominantAndScarceScorers.GetValidDynamicScorerConfigs(request), node)
	assert.Equal(t, score, MaxNodeScore*2/10+MaxNodeScore*5/10)
}
