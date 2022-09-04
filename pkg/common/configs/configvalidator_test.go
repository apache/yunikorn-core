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

package configs

import (
	"testing"

	"gotest.tools/assert"
)

func TestCheckResourceConfigurationsForQueue(t *testing.T) {
	negativeResourceMap := map[string]string{"memory": "-50", "vcores": "33"}
	resourceMapWithSyntaxError := map[string]string{"memory": "ten", "vcores": ""}
	higherResourceMap := map[string]string{"memory": "50", "vcores": "33"}
	lowerResourceMap := map[string]string{"memory": "10", "vcores": "30"}
	testCases := []struct {
		name          string
		current       QueueConfig
		errorExpected bool
	}{
		{"Negative guaranteed resource", QueueConfig{
			Resources: Resources{
				Guaranteed: negativeResourceMap,
			},
		}, true},
		{"Negative max resource", QueueConfig{
			Resources: Resources{
				Max: negativeResourceMap,
			},
		}, true},
		{"Nil guaranteed resource", QueueConfig{
			Resources: Resources{
				Max: lowerResourceMap,
			},
		}, false},
		{"Nil max resource", QueueConfig{
			Resources: Resources{
				Guaranteed: lowerResourceMap,
			},
		}, false},
		{"Syntax error in guaranteed resource", QueueConfig{
			Resources: Resources{
				Guaranteed: resourceMapWithSyntaxError,
			},
		}, true},
		{"Syntax error in max resource", QueueConfig{
			Resources: Resources{
				Max: resourceMapWithSyntaxError,
			},
		}, true},
		{"Higher guaranteed resource in child queues", QueueConfig{
			Resources: Resources{
				Guaranteed: lowerResourceMap,
			},
			Queues: []QueueConfig{{
				Resources: Resources{
					Guaranteed: higherResourceMap,
				},
			}},
		}, true},
		{"Higher sum of guaranteed resource in child queues than the parent's guaranteed", QueueConfig{
			Resources: Resources{
				Max:        higherResourceMap,
				Guaranteed: lowerResourceMap,
			},
			Queues: []QueueConfig{{
				Resources: Resources{
					Max:        lowerResourceMap,
					Guaranteed: lowerResourceMap,
				},
			}, {
				Resources: Resources{
					Max:        lowerResourceMap,
					Guaranteed: lowerResourceMap,
				},
			}},
		}, true},
		{"Higher max resource in child queues", QueueConfig{
			Resources: Resources{
				Max: lowerResourceMap,
			},
			Queues: []QueueConfig{{
				Resources: Resources{
					Max: higherResourceMap,
				},
			}},
		}, true},
		{"Higher guaranteed than max resource", QueueConfig{
			Resources: Resources{
				Max:        lowerResourceMap,
				Guaranteed: higherResourceMap,
			},
		}, true},
		{"Valid configuration",
			QueueConfig{
				Resources: Resources{
					Max:        higherResourceMap,
					Guaranteed: lowerResourceMap,
				},
			},
			false},
		{"One level skipped while setting max resource",
			createQueueWithSkippedMaxRes(),
			true},
		{"Sum of child guaranteed higher than parent max",
			createQueueWithSumGuaranteedHigherThanParentMax(),
			true},
		{"One level skipped while setting guaranteed resource",
			createQueueWithSkippedGuaranteedRes(),
			true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := checkQueueResource(tc.current, nil)
			if tc.errorExpected {
				assert.Assert(t, err != nil, "An error is expected")
			} else {
				assert.NilError(t, err, "No error is expected")
			}
		})
	}
}

func TestCheckQueueMaxApplicationsForQueue(t *testing.T) {
	testCases := []struct {
		name          string
		current       QueueConfig
		errorExpected bool
	}{
		{"Parent maxRunningApps must be larger than child maxRunningApps",
			createQueueWithMaxApplication([4]int64{1, 2, 3, 4}),
			true},
		{"Valid maxApplication settings: Parent maxRunningApps larger than child maxRunningApps",
			createQueueWithMaxApplication([4]int64{4, 3, 2, 1}),
			false},
		{"Valid maxApplication settings: Parent maxRunningApps can be 0",
			createQueueWithMaxApplication([4]int64{0, 3, 2, 1}),
			false},
		{"InValid maxApplication settings: child maxRunningApps cannot be 0",
			createQueueWithMaxApplication([4]int64{4, 3, 2, 0}),
			true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkQueueMaxApplications(tc.current)
			if tc.errorExpected {
				assert.Assert(t, err != nil, "An error is expected")
			} else {
				assert.NilError(t, err, "No error is expected")
			}
		})
	}
}

func createQueueWithSkippedMaxRes() QueueConfig {
	child1MaxMap := map[string]string{"memory": "150"}
	parentMaxMap := map[string]string{"memory": "100"}
	child1 := QueueConfig{
		Resources: Resources{
			Max: child1MaxMap,
		},
		Name: "child1",
	}
	parent1 := QueueConfig{
		Queues: []QueueConfig{child1},
		Name:   "parent1",
	}
	parent := QueueConfig{
		Resources: Resources{
			Max: parentMaxMap,
		},
		Queues: []QueueConfig{parent1},
		Name:   "parent",
	}
	root := QueueConfig{
		Queues: []QueueConfig{parent},
		Name:   RootQueue,
	}
	return root
}

func createQueueWithSumGuaranteedHigherThanParentMax() QueueConfig {
	child1GuaranteedMap := map[string]string{"memory": "50"}
	child2GuaranteedMap := map[string]string{"memory": "40"}
	parentMaxMap := map[string]string{"memory": "100"}
	parent1MaxMap := map[string]string{"memory": "80"}
	child1 := QueueConfig{
		Resources: Resources{
			Guaranteed: child1GuaranteedMap,
		},
		Name: "child1",
	}
	child2 := QueueConfig{
		Resources: Resources{
			Guaranteed: child2GuaranteedMap,
		},
		Name: "child1",
	}
	parent1 := QueueConfig{
		Queues: []QueueConfig{child1, child2},
		Name:   "parent1",
		Resources: Resources{
			Max: parent1MaxMap,
		},
	}
	parent := QueueConfig{
		Resources: Resources{
			Max: parentMaxMap,
		},
		Queues: []QueueConfig{parent1},
		Name:   "parent",
	}
	root := QueueConfig{
		Queues: []QueueConfig{parent},
		Name:   RootQueue,
	}
	return root
}

func createQueueWithSkippedGuaranteedRes() QueueConfig {
	child1MaxMap := map[string]string{"memory": "150"}
	parentMaxMap := map[string]string{"memory": "100"}
	child1 := QueueConfig{
		Resources: Resources{
			Guaranteed: child1MaxMap,
		},
		Name: "child1",
	}
	parent1 := QueueConfig{
		Queues: []QueueConfig{child1},
		Name:   "parent1",
	}
	parent := QueueConfig{
		Resources: Resources{
			Guaranteed: parentMaxMap,
		},
		Queues: []QueueConfig{parent1},
		Name:   "parent",
	}
	root := QueueConfig{
		Queues: []QueueConfig{parent},
		Name:   RootQueue,
	}
	return root
}

func createQueueWithMaxApplication(maxApplication [4]int64) QueueConfig {
	child1MaxMap := map[string]string{"memory": "50"}
	parentMaxMap := map[string]string{"memory": "100"}
	child1 := QueueConfig{
		Resources: Resources{
			Guaranteed: child1MaxMap,
		},
		Name:            "child1",
		MaxApplications: maxApplication[3],
	}
	parent1 := QueueConfig{
		Queues:          []QueueConfig{child1},
		Name:            "parent1",
		MaxApplications: maxApplication[2],
	}
	parent := QueueConfig{
		Resources: Resources{
			Guaranteed: parentMaxMap,
		},
		Queues:          []QueueConfig{parent1},
		Name:            "parent",
		MaxApplications: maxApplication[1],
	}
	root := QueueConfig{
		Queues:          []QueueConfig{parent},
		Name:            RootQueue,
		MaxApplications: maxApplication[0],
	}
	return root
}
