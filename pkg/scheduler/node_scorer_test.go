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

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"

	"gotest.tools/assert"
)

func TestDominantResourceUsageScorer(t *testing.T) {
	// Test case expected to fail: invalid conf format
	_, err := NewDominantResourceUsageScorer(map[string]interface{}{"resourceNames": ""})
	assert.Error(t, err, "resourceNames should be strings")
	// Test scorers: without or with specified resourceNames
	nilConfScorer, err := NewDominantResourceUsageScorer(nil)
	assert.NilError(t, err, "failed to create new dominant scorer")
	emptyConfScorer, err := NewDominantResourceUsageScorer(map[string]interface{}{})
	assert.NilError(t, err, "failed to create new dominant scorer")
	memoryScorer, err := NewDominantResourceUsageScorer(map[string]interface{}{"resourceNames": []string{"memory"}})
	assert.NilError(t, err, "failed to create new dominant scorer")
	vcoreScorer, err := NewDominantResourceUsageScorer(map[string]interface{}{"resourceNames": []string{"vcore"}})
	assert.NilError(t, err, "failed to create new dominant scorer")
	nonExistScorer, err := NewDominantResourceUsageScorer(map[string]interface{}{"resourceNames": []string{"gpu"}})
	assert.NilError(t, err, "failed to create new dominant scorer")
	// Test cases for node with total resource: <memory:100, vcore:100>
	node := newNode("node-1", map[string]resources.Quantity{"memory": 100, "vcore": 100})
	// node available resource: <memory:30, vcore:20>
	node.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 70, "vcore": 80})
	assert.Equal(t, nilConfScorer.(NodeScorer).Score(node), MaxNodeScore*2/10)
	assert.Equal(t, emptyConfScorer.(NodeScorer).Score(node), MaxNodeScore*2/10)
	assert.Equal(t, memoryScorer.(NodeScorer).Score(node), MaxNodeScore*3/10)
	assert.Equal(t, vcoreScorer.(NodeScorer).Score(node), MaxNodeScore*2/10)
	assert.Equal(t, nonExistScorer.(NodeScorer).Score(node), int64(0))
	// node available resource: <memory:10, vcore:20>
	node.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 90, "vcore": 80})
	node.nodeResourceUpdated()
	assert.Equal(t, nilConfScorer.(NodeScorer).Score(node), MaxNodeScore/10)
	assert.Equal(t, emptyConfScorer.(NodeScorer).Score(node), MaxNodeScore/10)
	assert.Equal(t, memoryScorer.(NodeScorer).Score(node), MaxNodeScore/10)
	assert.Equal(t, vcoreScorer.(NodeScorer).Score(node), MaxNodeScore*2/10)
	assert.Equal(t, nonExistScorer.(NodeScorer).Score(node), int64(0))
}

func TestResourceUsageScorer(t *testing.T) {
	// Test case expected to fail: nil/empty/invalid conf
	_, err := NewResourceUsageScorer(nil)
	assert.Error(t, err, "required resourceName is not configured")
	_, err = NewResourceUsageScorer(map[string]interface{}{})
	assert.Error(t, err, "required resourceName is not configured")
	_, err = NewResourceUsageScorer(map[string]interface{}{"resourceName": ""})
	assert.Error(t, err, "resourceName should not be empty string")
	// Test scorers with specified resourceName
	memoryScorer, err := NewResourceUsageScorer(map[string]interface{}{"resourceName": "memory"})
	assert.NilError(t, err, "failed to create new dominant scorer")
	vcoreScorer, err := NewResourceUsageScorer(map[string]interface{}{"resourceName": "vcore"})
	assert.NilError(t, err, "failed to create new dominant scorer")
	nonExistScorer, err := NewResourceUsageScorer(map[string]interface{}{"resourceName": "gpu"})
	assert.NilError(t, err, "failed to create new dominant scorer")
	// Test cases for node with total resource: <memory:100, vcore:100>
	node := newNode("node-1", map[string]resources.Quantity{"memory": 100, "vcore": 100})
	// node available resource: <memory:30, vcore:20>
	node.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 70, "vcore": 80})
	assert.Equal(t, memoryScorer.(NodeScorer).Score(node), MaxNodeScore*3/10)
	assert.Equal(t, vcoreScorer.(NodeScorer).Score(node), MaxNodeScore*2/10)
	assert.Equal(t, nonExistScorer.(NodeScorer).Score(node), int64(0))
	// node available resource: <memory:10, vcore:20>
	node.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 90, "vcore": 80})
	node.nodeResourceUpdated()
	assert.Equal(t, memoryScorer.(NodeScorer).Score(node), MaxNodeScore/10)
	assert.Equal(t, vcoreScorer.(NodeScorer).Score(node), MaxNodeScore*2/10)
	assert.Equal(t, nonExistScorer.(NodeScorer).Score(node), int64(0))
}

func TestScarceResourceUsageScorer(t *testing.T) {
	// Test case expected to fail: nil/empty/invalid conf
	_, err := NewScarceResourceUsageScorerFactory(nil)
	assert.Error(t, err, "required scarceResourceName is not configured")
	_, err = NewScarceResourceUsageScorerFactory(map[string]interface{}{})
	assert.Error(t, err, "required scarceResourceName is not configured")
	_, err = NewScarceResourceUsageScorerFactory(map[string]interface{}{"scarceResourceName": ""})
	assert.Error(t, err, "scarceResourceName should not be empty string")
	// Test scorer factories with specified resourceName
	gpuScorerFactory, err := NewScarceResourceUsageScorerFactory(map[string]interface{}{"scarceResourceName": "gpu"})
	assert.NilError(t, err, "failed to create new dominant scorer")
	assert.Assert(t, gpuScorerFactory != nil)
	nonExistScorerFactory, err := NewScarceResourceUsageScorerFactory(map[string]interface{}{"scarceResourceName": "non-exist"})
	assert.NilError(t, err, "failed to create new dominant scorer")
	assert.Assert(t, nonExistScorerFactory != nil)
	// Test scorers
	gpuScorerForNilRequest := gpuScorerFactory.(NodeScorerFactory).NewNodeScorer(nil)
	assert.Assert(t, gpuScorerForNilRequest == nil, "scorer should be nil if request is not satisfied")
	unsatisfiedRequest := newAllocationAsk("alloc-1", "app-1",
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1, "vcore": 1}))
	gpuScorerForUnsatisfiedRequest := gpuScorerFactory.(NodeScorerFactory).NewNodeScorer(unsatisfiedRequest)
	assert.Assert(t, gpuScorerForUnsatisfiedRequest == nil, "scorer should be nil if request is not satisfied")
	satisfiedRequest := newAllocationAsk("alloc-1", "app-1",
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1, "vcore": 1, "gpu": 1}))
	gpuScorerForSatisfiedRequest := gpuScorerFactory.(NodeScorerFactory).NewNodeScorer(satisfiedRequest)
	assert.Assert(t, gpuScorerForSatisfiedRequest != nil, "scorer should be nil if request is satisfied")

	// Test cases for node with total resource: <memory:100, vcore:100, gpu:100>
	node := newNode("node-1", map[string]resources.Quantity{"memory": 100, "vcore": 100, "gpu": 100})
	// node available resource: <memory:30, vcore:20, gpu:50>
	node.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 70, "vcore": 80, "gpu": 50})
	assert.Equal(t, gpuScorerForSatisfiedRequest.(NodeScorer).Score(node), MaxNodeScore*5/10)
	// node available resource: <memory:10, vcore:20, gpu:5>
	node.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 90, "vcore": 80, "gpu": 95})
	node.nodeResourceUpdated()
	assert.Equal(t, gpuScorerForSatisfiedRequest.(NodeScorer).Score(node), MaxNodeScore*5/100)
}
