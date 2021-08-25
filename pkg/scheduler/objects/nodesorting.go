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
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

type NodeSortingPolicy interface {
	PolicyType() policies.SortingPolicy
	ScoreNode(node *Node) float64
}

type binPackingNodeSortingPolicy struct{}
type fairnessNodeSortingPolicy struct{}

func (binPackingNodeSortingPolicy) PolicyType() policies.SortingPolicy {
	return policies.BinPackingPolicy
}

func (fairnessNodeSortingPolicy) PolicyType() policies.SortingPolicy {
	return policies.FairnessPolicy
}

func (binPackingNodeSortingPolicy) ScoreNode(node *Node) float64 {
	// choose most loaded node first
	return resources.LargestUsageShare(node.GetAvailableResource())
}

func (fairnessNodeSortingPolicy) ScoreNode(node *Node) float64 {
	// choose least loaded node first
	return -resources.LargestUsageShare(node.GetAvailableResource())
}

func NewNodeSortingPolicy(policyType string) NodeSortingPolicy {
	pType, err := policies.SortingPolicyFromString(policyType)
	if err != nil {
		log.Logger().Debug("node sorting policy defaulted to 'undefined'",
			zap.Error(err))
	}
	var sp NodeSortingPolicy
	switch pType {
	case policies.BinPackingPolicy:
		sp = binPackingNodeSortingPolicy{}
	case policies.FairnessPolicy:
		sp = fairnessNodeSortingPolicy{}
	default:
		sp = fairnessNodeSortingPolicy{}
	}

	log.Logger().Debug("new node sorting policy added",
		zap.String("type", pType.String()))
	return sp
}
