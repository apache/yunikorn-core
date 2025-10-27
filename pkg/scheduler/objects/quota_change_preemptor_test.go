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
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
)

func TestQuotaChangeCheckPreconditions(t *testing.T) {
	parentConfig := configs.QueueConfig{
		Name:   "parent",
		Parent: true,
		Resources: configs.Resources{
			Max: map[string]string{"memory": "1000"},
		},
	}
	parent, err := NewConfiguredQueue(parentConfig, nil, false, nil)
	assert.NilError(t, err)

	leafRes := configs.Resources{
		Max: map[string]string{"memory": "1000"},
	}
	leaf, err := NewConfiguredQueue(configs.QueueConfig{
		Name:      "leaf",
		Resources: leafRes,
	}, parent, false, nil)
	assert.NilError(t, err)

	dynamicLeaf, err := NewConfiguredQueue(configs.QueueConfig{
		Name:      "dynamic-leaf",
		Resources: leafRes,
	}, parent, false, nil)
	assert.NilError(t, err)
	dynamicLeaf.isManaged = false

	alreadyPreemptionRunning, err := NewConfiguredQueue(configs.QueueConfig{
		Name:      "leaf-already-preemption-running",
		Resources: leafRes,
	}, parent, false, nil)
	assert.NilError(t, err)
	alreadyPreemptionRunning.MarkQuotaChangePreemptionRunning()

	alreadyTriggeredPreemption, err := NewConfiguredQueue(configs.QueueConfig{
		Name:      "leaf-already-triggerred-running",
		Resources: leafRes,
	}, parent, false, nil)
	assert.NilError(t, err)
	alreadyTriggeredPreemption.MarkTriggerredQuotaChangePreemption()

	usageExceededMaxQueue, err := NewConfiguredQueue(configs.QueueConfig{
		Name:      "leaf-usage-exceeded-max",
		Resources: leafRes,
	}, parent, false, nil)
	assert.NilError(t, err)
	usageExceededMaxQueue.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 2000})

	testCases := []struct {
		name               string
		queue              *Queue
		preconditionResult bool
	}{
		{"parent queue", parent, false},
		{"leaf queue", leaf, false},
		{"dynamic leaf queue", dynamicLeaf, false},
		{"leaf queue, already preemption process started or running", alreadyPreemptionRunning, false},
		{"leaf queue, already triggerred preemption", alreadyTriggeredPreemption, false},
		{"leaf queue, usage exceeded max resources", usageExceededMaxQueue, true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			preemptor := NewQuotaChangePreemptor(tc.queue)
			assert.Equal(t, preemptor.CheckPreconditions(), tc.preconditionResult)
			if tc.preconditionResult {
				preemptor.tryPreemption()
				assert.Equal(t, tc.queue.HasTriggerredQuotaChangePreemption(), true)
				assert.Equal(t, tc.queue.IsQuotaChangePreemptionRunning(), true)
			}
		})
	}
}

func TestQuotaChangeGetPreemptableResource(t *testing.T) {
	leaf, err := NewConfiguredQueue(configs.QueueConfig{
		Name: "leaf",
	}, nil, false)
	assert.NilError(t, err)

	testCases := []struct {
		name         string
		queue        *Queue
		maxResource  *resources.Resource
		usedResource *resources.Resource
		preemptable  *resources.Resource
	}{
		{"nil max and nil used", leaf, nil, nil, nil},
		{"nil max", leaf, nil, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), nil},
		{"nil used", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), nil, nil},
		{"used below max", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500}), nil},
		{"used above max", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1500}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500})},
		{"used above max in specific res type", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000, "cpu": 10}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1500}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500})},
		{"used above max and below max in specific res type", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000, "cpu": 10}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1500, "cpu": 10}), resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500})},
		{"used res type but max undefined", leaf, resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000}), resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 150}), nil},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.queue.maxResource = tc.maxResource
			tc.queue.allocatedResource = tc.usedResource
			preemptor := NewQuotaChangePreemptor(tc.queue)
			assert.Equal(t, resources.Equals(preemptor.GetPreemptableResources(), tc.preemptable), true)
		})
	}
}
