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
	"fmt"
	"github.com/cloudera/yunikorn-core/pkg/common/configs"
	"github.com/cloudera/yunikorn-core/pkg/log"
	"go.uber.org/zap"
)


type NodeSortingPolicy struct {
	PolicyType SortingPolicy
}

type SortingPolicy int

const (
	BinPackingPolicy = iota
	FairnessPolicy
	Default
	Undefined
)

func (nsp SortingPolicy) String() string {
	return [...]string{"binpacking", "fair", "default"}[nsp]
}
func FromString(str string) (SortingPolicy, error) {
	switch str {
	case "binpacking":
		return BinPackingPolicy, nil
	case "fair":
		return FairnessPolicy, nil
	case "default":
		return Default, nil
	default:
		return Undefined, fmt.Errorf("undefined policy %s", str)
	}
}

func NewNodeSortingPolicy(policy configs.NodeSortingPolicy) *NodeSortingPolicy {
	pType, _ := FromString(policy.Type)
	sp := &NodeSortingPolicy{
		PolicyType : pType,
	}

	log.Logger().Debug("new node sorting policy added",
		zap.String("type", policy.Type))
	return sp
}

func NewNodeDefaultSortingPolicy(policyType string) *NodeSortingPolicy {
	pType, _ := FromString(policyType)
	sp := &NodeSortingPolicy{
		PolicyType: pType,
	}

	log.Logger().Debug("new node sorting policy added",
		zap.String("type", policyType))
	return sp
}
