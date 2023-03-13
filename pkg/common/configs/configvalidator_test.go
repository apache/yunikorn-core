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

	"gotest.tools/v3/assert"
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
			createQueueWithMaxApplication([4]uint64{1, 2, 3, 4}),
			true},
		{"Valid maxApplication settings: Parent maxRunningApps larger than child maxRunningApps",
			createQueueWithMaxApplication([4]uint64{4, 3, 2, 1}),
			false},
		{"Valid maxApplication settings: Parent maxRunningApps can be 0",
			createQueueWithMaxApplication([4]uint64{0, 3, 2, 1}),
			false},
		{"InValid maxApplication settings: child maxRunningApps cannot be 0",
			createQueueWithMaxApplication([4]uint64{4, 3, 2, 0}),
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

func TestGetLongestPlacementPath(t *testing.T) {
	staticPaths := getLongestPlacementPaths(createPlacementRules())
	assert.Equal(t, 2, len(staticPaths))

	path0 := staticPaths[0]
	assert.Equal(t, "root.users", path0.path)
	assert.Equal(t, 0, path0.ruleNo)
	assert.Equal(t, "user->tag->fixed", path0.ruleChain)
	assert.Equal(t, false, path0.create)
	path1 := staticPaths[1]
	assert.Equal(t, "root.admins.dev", path1.path)
	assert.Equal(t, 1, path1.ruleNo)
	assert.Equal(t, "fixed->fixed", path1.ruleChain)
	assert.Equal(t, true, path1.create)
}

func TestCheckQueueHierarchyForPlacement(t *testing.T) {
	queues := createQueueConfig()
	parts := strings.Split(strings.ToLower("root.users"), DOT)
	result := checkQueueHierarchyForPlacement(parts, false, queues)
	assert.Equal(t, checkOK, result)

	parts = strings.Split(strings.ToLower("root.users.dev"), DOT)
	result = checkQueueHierarchyForPlacement(parts, true, queues)
	assert.Equal(t, checkOK, result)

	queues[0].Queues[0].Parent = false
	result = checkQueueHierarchyForPlacement(parts, false, queues)
	assert.Equal(t, queueNotParent, result)

	queues[0].Queues[0].Parent = true
	result = checkQueueHierarchyForPlacement(parts, false, queues)
	assert.Equal(t, nonExistingQueue, result)
}

func TestCheckPlacementRules(t *testing.T) {
	conf := &PartitionConfig{
		PlacementRules: createPlacementRules(),
		Queues:         createQueueConfig(),
	}

	err := checkPlacementRules(conf)
	assert.NilError(t, err)

	conf.Queues[0].Queues[0].Parent = false
	err = checkPlacementRules(conf)
	assert.ErrorContains(t, err, "placement rule no. #0 (user->tag->fixed) references a queue (root.users) which is a leaf")

	conf.Queues[0].Queues[0].Parent = true
	conf.PlacementRules[1].Create = false
	err = checkPlacementRules(conf)
	assert.ErrorContains(t, err, "placement rule no. #1 (fixed->fixed) references non-existing queues (root.admins.dev) and create is 'false'")
}

func createQueueConfig() []QueueConfig {
	return []QueueConfig{
		{
			Name:   "root",
			Parent: true,
			Queues: []QueueConfig{
				{
					Name:   "users",
					Parent: true,
				},
				{
					Name:   "admins",
					Parent: true,
				},
			},
		},
	}
}

func createPlacementRules() []PlacementRule {
	return []PlacementRule{
		{
			Name: "user",
			Parent: &PlacementRule{
				Name:  "tag",
				Value: "namespace",
				Parent: &PlacementRule{
					Name:  "fixed",
					Value: "root.users",
				},
			},
		},
		{
			Name:   "fixed",
			Value:  "dev",
			Create: true,
			Parent: &PlacementRule{
				Name:  "fixed",
				Value: "admins",
			},
		},
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

func createQueueWithMaxApplication(maxApplication [4]uint64) QueueConfig {
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
