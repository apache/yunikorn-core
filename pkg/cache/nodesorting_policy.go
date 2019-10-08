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

package cache

import (
	"github.com/cloudera/yunikorn-core/pkg/common/configs"
	"github.com/cloudera/yunikorn-core/pkg/log"
	"go.uber.org/zap"
)


type NodeSortingPolicy struct {
	PolicyType string
}

func NewNodeSortingPolicy(policy configs.NodeSortingPolicy) *NodeSortingPolicy {
	sp := &NodeSortingPolicy{
		PolicyType: policy.Type,
	}

	log.Logger().Debug("new node sorting policy added",
		zap.String("type", sp.PolicyType))
	return sp
}

func NewNodeDefaultSortingPolicy(policyType string) *NodeSortingPolicy {
	sp := &NodeSortingPolicy{
		PolicyType: policyType,
	}

	log.Logger().Debug("new node sorting policy added",
		zap.String("type", sp.PolicyType))
	return sp
}
