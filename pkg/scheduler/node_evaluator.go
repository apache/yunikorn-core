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
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

const (
	// MaxNodeScore is the maximum score a Scorer is expected to return.
	MaxNodeScore int64 = 1000
)

// interface of node evaluator which can give all-round scores
// including a static score (only depends on the node)
// and a dynamic score (depends on not only the node but also the request)
// for specified node.
type NodeEvaluator interface {
	GetValidDynamicScorerConfigs(request *schedulingAllocationAsk) []*configs.NodeScorerConfig
	CalculateStaticScore(node *SchedulingNode) (int64, map[string]int64)
	CalculateDynamicScore(validDynamicScorers []*configs.NodeScorerConfig, node *SchedulingNode) (int64, map[string]int64)
	CalculateScore(validDynamicScorers []*configs.NodeScorerConfig, node *SchedulingNode) (int64, map[string]int64)
}

type DefaultNodeEvaluator struct {
	staticScorerConfigs  []*configs.NodeScorerConfig
	dynamicScorerConfigs []*configs.NodeScorerConfig
}

func NewDefaultNodeEvaluator(nodeEvaluatorConfig *configs.NodeEvaluatorConfig) NodeEvaluator {
	var scorerConfigs []*configs.NodeScorerConfig
	if nodeEvaluatorConfig == nil || len(nodeEvaluatorConfig.ScorerConfigs) == 0 {
		scorerConfigs = getDefaultNodeScorerConfigs()
		log.Logger().Info("using default scorer configs since node evaluator has not been configured")
	} else {
		scorerConfigs = nodeEvaluatorConfig.ScorerConfigs
	}
	staticScorerConfigs := make([]*configs.NodeScorerConfig, 0)
	dynamicScorerConfigs := make([]*configs.NodeScorerConfig, 0)
	for _, scorerConfig := range scorerConfigs {
		scorerOrFactory := getScorerOrFactory(scorerConfig)
		if scorerOrFactory != nil {
			switch scorerOrFactory.(type) {
			case NodeScorer:
				staticScorerConfigs = append(staticScorerConfigs, scorerConfig)
			case NodeScorerFactory:
				dynamicScorerConfigs = append(dynamicScorerConfigs, scorerConfig)
			default:
			}
		}
	}
	log.Logger().Info("node evaluator initialized",
		zap.Any("staticScorerConfigs", staticScorerConfigs),
		zap.Any("dynamicScorerConfigs", dynamicScorerConfigs))
	return &DefaultNodeEvaluator{
		staticScorerConfigs:  staticScorerConfigs,
		dynamicScorerConfigs: dynamicScorerConfigs,
	}
}

func getScorer(nsc *configs.NodeScorerConfig) NodeScorer {
	scorerOrFactory := getScorerOrFactory(nsc)
	if scorerOrFactory != nil {
		if scorer, ok := scorerOrFactory.(NodeScorer); ok {
			return scorer
		}
	}
	return nil
}

func getScorerOrFactory(nsc *configs.NodeScorerConfig) interface{} {
	// scorer (or scorer factory) should always be initialized when validating the conf,
	// but for tests, that process may be skipped so that we should initialize scorer here just in case.
	if nsc.Scorer == nil {
		log.Logger().Info("scorer should be initialized before, initializing now...", zap.Any("config", nsc))
		scorer, err := common.GetNodeScorerOrFactory(nsc.ScorerName, nsc.Conf)
		if err != nil {
			log.Logger().Error("failed to get scorer or scorer factory", zap.Error(err))
			return nil
		}
		nsc.Scorer = scorer
	}
	return nsc.Scorer
}

func (dne *DefaultNodeEvaluator) GetValidDynamicScorerConfigs(request *schedulingAllocationAsk) []*configs.NodeScorerConfig {
	validScorerConfigs := make([]*configs.NodeScorerConfig, 0)
	for _, dsc := range dne.dynamicScorerConfigs {
		scorerOrFactory := getScorerOrFactory(dsc)
		if scorerOrFactory != nil {
			if nodeScorerFactory, ok := scorerOrFactory.(NodeScorerFactory); ok {
				dynamicNodeScorer := nodeScorerFactory.NewNodeScorer(request)
				if dynamicNodeScorer != nil {
					validScorerConfigs = append(validScorerConfigs, &configs.NodeScorerConfig{
						ScorerName: dsc.ScorerName,
						Weight:     dsc.Weight,
						Scorer:     dynamicNodeScorer,
					})
				}
			}
		}
	}
	return validScorerConfigs
}

func (dne *DefaultNodeEvaluator) CalculateStaticScore(node *SchedulingNode) (int64, map[string]int64) {
	var score int64
	subScores := make(map[string]int64)
	for _, ssc := range dne.staticScorerConfigs {
		scorer := getScorer(ssc)
		if scorer != nil {
			subScore := scorer.Score(node)
			subScores[ssc.ScorerName] = subScore
			score += subScore * ssc.Weight
		}
	}
	return score, subScores
}

func (dne *DefaultNodeEvaluator) CalculateDynamicScore(validDynamicScorerConfigs []*configs.NodeScorerConfig, node *SchedulingNode) (int64, map[string]int64) {
	var score int64
	subScores := make(map[string]int64)
	for _, vsc := range validDynamicScorerConfigs {
		scorer := getScorer(vsc)
		if scorer != nil {
			subScore := scorer.Score(node)
			subScores[vsc.ScorerName] = subScore
			score += subScore * vsc.Weight
		}
	}
	return score, subScores
}

func (dne *DefaultNodeEvaluator) CalculateScore(validDynamicScorerConfigs []*configs.NodeScorerConfig,
	node *SchedulingNode) (int64, map[string]int64) {
	score, subScores := dne.CalculateStaticScore(node)
	for _, vsc := range validDynamicScorerConfigs {
		scorer := getScorer(vsc)
		if scorer != nil {
			subScore := scorer.Score(node)
			subScores[vsc.ScorerName] = subScore
			score += subScore * vsc.Weight
		}
	}
	return score, subScores
}
