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
	"math"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

type NodeSortingPolicy interface {
	PolicyType() policies.SortingPolicy
	ScoreNode(node *Node) float64
	ResourceWeights() map[string]float64
}

type binPackingNodeSortingPolicy struct {
	resourceWeights map[string]float64
}

type fairnessNodeSortingPolicy struct {
	resourceWeights map[string]float64
}

func (binPackingNodeSortingPolicy) PolicyType() policies.SortingPolicy {
	return policies.BinPackingPolicy
}

func (fairnessNodeSortingPolicy) PolicyType() policies.SortingPolicy {
	return policies.FairnessPolicy
}

func absResourceUsage(node *Node, weights *map[string]float64) float64 {
	totalWeight := float64(0)
	usage := float64(0)

	shares := node.GetResourceUsageShares()
	for k, v := range shares {
		weight, found := (*weights)[k]
		if !found || weight == float64(0) {
			continue
		}
		if math.IsNaN(v) {
			continue
		}
		usage += v * weight
		totalWeight += weight
	}

	var result float64

	if totalWeight == float64(0) {
		result = float64(0)
	} else {
		result = usage / totalWeight
	}
	return result
}

func (p binPackingNodeSortingPolicy) ScoreNode(node *Node) float64 {
	// choose most loaded node first (score == percentage free)
	return float64(1) - absResourceUsage(node, &p.resourceWeights)
}

func (p fairnessNodeSortingPolicy) ScoreNode(node *Node) float64 {
	// choose least loaded node first (score == percentage used)
	return absResourceUsage(node, &p.resourceWeights)
}

func cloneWeights(source map[string]float64) map[string]float64 {
	weights := make(map[string]float64, len(source))
	for k, v := range source {
		weights[k] = v
	}
	return weights
}

func (p binPackingNodeSortingPolicy) ResourceWeights() map[string]float64 {
	return cloneWeights(p.resourceWeights)
}

func (p fairnessNodeSortingPolicy) ResourceWeights() map[string]float64 {
	return cloneWeights(p.resourceWeights)
}

// Return a default set of resource weights if not otherwise specified.
func defaultResourceWeights() map[string]float64 {
	weights := make(map[string]float64)
	weights["vcore"] = 1.0
	weights["memory"] = 1.0
	return weights
}

func NewNodeSortingPolicy(policyType string, resourceWeights map[string]float64) NodeSortingPolicy {
	pType, err := policies.SortingPolicyFromString(policyType)
	if err != nil {
		log.Logger().Debug("node sorting policy defaulted to 'undefined'",
			zap.Error(err))
	}
	weights := resourceWeights
	if len(weights) == 0 {
		weights = defaultResourceWeights()
	}

	var sp NodeSortingPolicy
	switch pType {
	case policies.BinPackingPolicy:
		sp = binPackingNodeSortingPolicy{
			resourceWeights: weights,
		}
	case policies.FairnessPolicy:
		sp = fairnessNodeSortingPolicy{
			resourceWeights: weights,
		}
	default:
		sp = fairnessNodeSortingPolicy{
			resourceWeights: weights,
		}
	}

	log.Logger().Debug("new node sorting policy added",
		zap.String("type", pType.String()), zap.Any("resourceWeights", weights))
	return sp
}
