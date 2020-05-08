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

	"github.com/apache/incubator-yunikorn-core/pkg/common"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
)

const (
	ResourceUsageScorerName         = "resource_usage"
	DominantResourceUsageScorerName = "dominant_resource_usage"
	ScarceResourceUsageScorerName   = "scarce_resource_usage"
)

func init() {
	var ResourceUsageScorerMaker common.NewNodeScorerFunc = NewResourceUsageScorer
	var DominantResourceUsageScorerMaker common.NewNodeScorerFunc = NewDominantResourceUsageScorer
	var ScarceResourceUsageScorerMaker common.NewNodeScorerFunc = NewScarceResourceUsageScorerFactory
	common.RegisterNodeScorerMaker(ResourceUsageScorerName, ResourceUsageScorerMaker)
	common.RegisterNodeScorerMaker(DominantResourceUsageScorerName, DominantResourceUsageScorerMaker)
	common.RegisterNodeScorerMaker(ScarceResourceUsageScorerName, ScarceResourceUsageScorerMaker)
}

// NodeScorer is a common scorer interface to give a sub-score for specified node
type NodeScorer interface {
	Score(node *SchedulingNode) int64
}

// NodeScorerFactory is a scorer factory interface which can generate
// new node scorer according to specified request.
type NodeScorerFactory interface {
	NewNodeScorer(request *schedulingAllocationAsk) NodeScorer
}

type ResourceUsageScorer struct {
	resourceName string
}

func parseRequiredString(conf map[string]interface{}, fieldName string) (string, error) {
	if fieldValueObj, ok := conf[fieldName]; ok {
		if fieldValue, ok := fieldValueObj.(string); ok {
			if fieldValue == "" {
				return "", fmt.Errorf("%s should not be empty string", fieldName)
			}
			return fieldValue, nil
		}
		return "", fmt.Errorf("%s should be a string", fieldName)
	}
	return "", fmt.Errorf("required %s is not configured", fieldName)
}

func parseStrings(conf map[string]interface{}, fieldName string) ([]string, error) {
	if resourceNamesObj, ok := conf[fieldName]; ok {
		if resourceNames, ok := resourceNamesObj.([]string); ok {
			return resourceNames, nil
		}
		return nil, fmt.Errorf("%s should be strings", fieldName)
	}
	return nil, nil
}

func NewResourceUsageScorer(conf map[string]interface{}) (interface{}, error) {
	resourceName, err := parseRequiredString(conf, "resourceName")
	if err != nil {
		return nil, err
	}
	return &ResourceUsageScorer{resourceName: resourceName}, nil
}

func (rus *ResourceUsageScorer) Score(node *SchedulingNode) int64 {
	avail := node.GetAvailableResource()
	total := node.nodeInfo.GetCapacity()
	if availResQuantity, ok := avail.Resources[rus.resourceName]; ok {
		totalResQuantity := total.Resources[rus.resourceName]
		return MaxNodeScore * int64(availResQuantity) / int64(totalResQuantity)
	}
	return 0
}

type DominantResourceUsageScorer struct {
	resourceNames []string
}

func NewDominantResourceUsageScorer(conf map[string]interface{}) (interface{}, error) {
	resourceNames, err := parseStrings(conf, "resourceNames")
	if err != nil {
		return nil, err
	}
	return &DominantResourceUsageScorer{resourceNames: resourceNames}, nil
}

func (drus *DominantResourceUsageScorer) Score(node *SchedulingNode) int64 {
	avail := node.GetAvailableResource()
	total := node.nodeInfo.GetCapacity()
	var curScore, minScore int64
	resourceNames := drus.resourceNames
	// if resource names are not configured, use all resource names the node has.
	if resourceNames == nil {
		resourceNames = make([]string, 0)
		for resourceName := range avail.Resources {
			resourceNames = append(resourceNames, resourceName)
		}
	}

	for i, resourceName := range resourceNames {
		if availResQuantity, ok := avail.Resources[resourceName]; ok {
			totalResQuantity := total.Resources[resourceName]
			if totalResQuantity == 0 {
				curScore = 0
			} else {
				curScore = MaxNodeScore * int64(availResQuantity) / int64(totalResQuantity)
			}
		} else {
			curScore = 0
		}
		if i == 0 || curScore < minScore {
			minScore = curScore
		}
	}
	return minScore
}

type ScarceResourceUsageScorerFactory struct {
	scarceResourceName string
}

type ScarceResourceUsageScorer struct {
	scarceResourceName string
	requiredQuantity   resources.Quantity
}

func NewScarceResourceUsageScorerFactory(conf map[string]interface{}) (interface{}, error) {
	scarceResourceName, err := parseRequiredString(conf, "scarceResourceName")
	if err != nil {
		return nil, err
	}
	if scarceResourceName == "" {
		return nil, fmt.Errorf("required scarceResourceName not configured")
	}
	return &ScarceResourceUsageScorerFactory{scarceResourceName: scarceResourceName}, nil
}

func (srsf *ScarceResourceUsageScorerFactory) NewNodeScorer(request *schedulingAllocationAsk) NodeScorer {
	if request == nil {
		return nil
	}
	// return new scorer if this request contains the specified scarce resource
	if requiredQuantity, ok := request.AllocatedResource.Resources[srsf.scarceResourceName]; ok {
		return &ScarceResourceUsageScorer{
			scarceResourceName: srsf.scarceResourceName,
			requiredQuantity:   requiredQuantity,
		}
	}
	// return nil if this request doesn't need the specified scarce resource
	return nil
}

func (srs *ScarceResourceUsageScorer) Score(node *SchedulingNode) int64 {
	quantity := node.GetAvailableResource().Resources[srs.scarceResourceName]
	if quantity >= srs.requiredQuantity {
		totalQuantity := node.nodeInfo.GetCapacity().Resources[srs.scarceResourceName]
		return MaxNodeScore * int64(quantity) / int64(totalQuantity)
	}
	return 0
}
