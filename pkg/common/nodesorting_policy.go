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

package common

import (
	"fmt"
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
	Undefined
)

func (nsp SortingPolicy) String() string {
	return [...]string{"binpacking", "fair", "default"}[nsp]
}
func FromString(str string) (SortingPolicy, error) {
	switch str {
	// fair is the default policy when not set
	case "fair", "":
		return FairnessPolicy, nil
	case "binpacking":
		return BinPackingPolicy, nil
	default:
		return Undefined, fmt.Errorf("undefined policy %s", str)
	}
}

func NewNodeSortingPolicy(policyType string) *NodeSortingPolicy {
	pType, _ := FromString(policyType)
	sp := &NodeSortingPolicy{
		PolicyType: pType,
	}

	log.Logger().Debug("new node sorting policy added",
		zap.String("type", policyType))
	return sp
}
