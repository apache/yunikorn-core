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
	"strings"
	"testing"

	"gotest.tools/assert"
)

func TestCheckResourceConfigurationsForQueue(t *testing.T) {
	negativeResourceMap := map[string]string{"memory": "-50", "vcores": "33"}
	resourceMapWithSyntaxError := map[string]string{"memory": "ten", "vcores": ""}
	zeroResourceMap := map[string]string{"memory": "0", "vcores": "0"}
	higherResourceMap := map[string]string{"memory": "50", "vcores": "33"}
	lowerResourceMap := map[string]string{"memory": "10", "vcores": "3"}
	testCases := []struct {
		name             string
		current          QueueConfig
		errorExpected    bool
		expectedErrorMsg string
	}{
		{"Negative guaranteed resource", QueueConfig{
			Resources: Resources{
				Guaranteed: negativeResourceMap,
			},
		},
			true,
			"invalid guaranteed resource map[memory:-50 vcores:33] for queue , cannot be negative"},
		{"Negative max resource", QueueConfig{
			Resources: Resources{
				Max: negativeResourceMap,
			},
		}, true, "invalid max resource map[memory:-50 vcores:33] for queue , cannot be negative"},
		{"Nil guaranteed resource", QueueConfig{
			Resources: Resources{
				Max: lowerResourceMap,
			},
		}, false, ""},
		{"Nil max resource", QueueConfig{
			Resources: Resources{
				Guaranteed: lowerResourceMap,
			},
		}, false, ""},
		{"Syntax error in guaranteed resource", QueueConfig{
			Resources: Resources{
				Guaranteed: resourceMapWithSyntaxError,
			},
		}, true, "invalid syntax"},
		{"Syntax error in max resource", QueueConfig{
			Resources: Resources{
				Max: resourceMapWithSyntaxError,
			},
		}, true, "invalid syntax"},
		{"Higher guaranteed resource in child queues", QueueConfig{
			Resources: Resources{
				Guaranteed: lowerResourceMap,
			},
			Queues: []QueueConfig{{
				Resources: Resources{
					Guaranteed: higherResourceMap,
				},
			}},
		}, true, "queue  has guaranteed-resources (map[memory:10 vcores:3]) smaller than " +
			"sum of children guaranteed resources (map[memory:50 vcores:33])"},
		{"Zero max resource", QueueConfig{
			Resources: Resources{
				Max: zeroResourceMap,
			},
		}, true, "max resource total cannot be 0"},
		{"Higher max resource in child queues", QueueConfig{
			Resources: Resources{
				Max: lowerResourceMap,
			},
			Queues: []QueueConfig{{
				Resources: Resources{
					Max: higherResourceMap,
				},
			}},
		}, true, "queue  has max resources (map[memory:50 vcores:33]) set larger than " +
			"parent's max resources (map[memory:10 vcores:3])"},
		{"Higher guaranteed than max resource", QueueConfig{
			Resources: Resources{
				Max:        lowerResourceMap,
				Guaranteed: higherResourceMap,
			},
		}, true,
			"queue  has max resources (map[memory:10 vcores:3]) set smaller than guaranteed resources (map[memory:50 vcores:33])"},
		{"Valid configuration",
			QueueConfig{
				Resources: Resources{
					Max:        higherResourceMap,
					Guaranteed: lowerResourceMap,
				},
			},
			false,
			""},
		{"One level skipped while setting max resource",
			createQueueWithSkippedMaxRes(),
			true,
			"queue child1 has max resources (map[memory:150]) set larger than parent's max resources (map[memory:100])"},
		{"Sum of child guaranteed higher than parent max",
			createQueueWithSumGuaranteedHigherThanParentMax(),
			true,
			"queue parent1 has max resources (map[memory:80]) set smaller than guaranteed resources (map[memory:90])"},
		{"One level skipped while setting guaranteed resource",
			createQueueWithSkippedGuaranteedRes(),
			true,
			"queue parent1 has max resources (map[memory:150]) set larger than parent's max resources (map[memory:100])"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkResourceConfigurationsForQueue(tc.current, nil, nil)
			if tc.errorExpected {
				assert.Assert(t, err != nil, "An error is expected")
				assert.Assert(t, strings.Contains(err.Error(), tc.expectedErrorMsg), "Unexpected error message")
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
