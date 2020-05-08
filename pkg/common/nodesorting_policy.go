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

package common

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
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
	return [...]string{"binpacking", "fair", "undefined"}[nsp]
}

func FromString(str string) (SortingPolicy, error) {
	switch str {
	// fair is the default policy when not set
	case "fair", "":
		return FairnessPolicy, nil
	case "binpacking":
		return BinPackingPolicy, nil
	default:
		return Undefined, fmt.Errorf("undefined policy: %s", str)
	}
}

func NewNodeSortingPolicy(policyType string) *NodeSortingPolicy {
	pType, err := FromString(policyType)
	if err != nil {
		log.Logger().Debug("node sorting policy defaulted to 'undefined'",
			zap.Error(err))
	}
	sp := &NodeSortingPolicy{
		PolicyType: pType,
	}

	log.Logger().Debug("new node sorting policy added",
		zap.String("type", pType.String()))
	return sp
}

type NewNodeScorerFunc func(conf map[string]interface{}) (interface{}, error)
type NewNodeSortingAlgorithmFunc func(conf map[string]interface{}) (interface{}, error)

var (
	NodeScorerMakers           = make(map[string]NewNodeScorerFunc)
	NodeSortingAlgorithmMakers = make(map[string]NewNodeSortingAlgorithmFunc)
)

func RegisterNodeScorerMaker(name string, newNodeScorerFunc NewNodeScorerFunc) {
	NodeScorerMakers[name] = newNodeScorerFunc
}

func RegisterNodeSortingAlgorithmMaker(name string, newNodeSortingAlgorithmFunc NewNodeSortingAlgorithmFunc) {
	NodeSortingAlgorithmMakers[name] = newNodeSortingAlgorithmFunc
}

func GetNodeScorerOrFactory(name string, conf map[string]interface{}) (interface{}, error) {
	scorerMaker := NodeScorerMakers[name]
	if scorerMaker == nil {
		return nil, fmt.Errorf("node scorer '%s' not found", name)
	}
	scorer, err := scorerMaker(conf)
	if err != nil {
		return nil, err
	}
	return scorer, nil
}

func GetNodeSortingAlgorithm(name string, conf map[string]interface{}) (interface{}, error) {
	nodeSortingAlgoMaker := NodeSortingAlgorithmMakers[name]
	if nodeSortingAlgoMaker == nil {
		return nil, fmt.Errorf("node sorting algorithm '%s' not found", name)
	}
	algorithm, err := nodeSortingAlgoMaker(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize node sorting algorithm %s: %s",
			name, err.Error())
	}
	return algorithm, nil
}
