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
		current          *QueueConfig
		parent           *QueueConfig
		errorExpected    bool
		expectedErrorMsg string
	}{
		{"Negative guaranteed resource", &QueueConfig{
			Resources: Resources{
				Guaranteed: negativeResourceMap,
			},
		}, nil, true, "cannot be negative"},
		{"Negative max resource", &QueueConfig{
			Resources: Resources{
				Max: negativeResourceMap,
			},
		}, nil, true, "cannot be negative"},
		{"Nil guaranteed resource", &QueueConfig{
			Resources: Resources{
				Max: lowerResourceMap,
			},
		}, nil, false, ""},
		{"Nil max resource", &QueueConfig{
			Resources: Resources{
				Guaranteed: lowerResourceMap,
			},
		}, nil, false, ""},
		{"Syntax error in guaranteed resource", &QueueConfig{
			Resources: Resources{
				Guaranteed: resourceMapWithSyntaxError,
			},
		}, nil, true, "invalid syntax"},
		{"Syntax error in max resource", &QueueConfig{
			Resources: Resources{
				Max: resourceMapWithSyntaxError,
			},
		}, nil, true, "invalid syntax"},
		{"Higher guaranteed resource in child queues", &QueueConfig{
			Resources: Resources{
				Guaranteed: lowerResourceMap,
			},
			Queues: []QueueConfig{{
				Resources: Resources{
					Guaranteed: higherResourceMap,
				},
			}},
		}, nil, true, "smaller than sum of children guaranteed resources"},
		{"Zero max resource", &QueueConfig{
			Resources: Resources{
				Max: zeroResourceMap,
			},
		}, nil, true, "max resource total canno be 0"},
		{"Higher max resource in child queues", &QueueConfig{
			Resources: Resources{
				Max: higherResourceMap,
			},
		}, &QueueConfig{
			Resources: Resources{
				Max: lowerResourceMap,
			},
		}, true, "larger than parent's max resources"},
		{"Higher guaranteed than max resource", &QueueConfig{
			Resources: Resources{
				Max:        lowerResourceMap,
				Guaranteed: higherResourceMap,
			},
		}, nil, true, "smaller than guaranteed resources"},
		{"Valid configuration", &QueueConfig{
			Resources: Resources{
				Max:        higherResourceMap,
				Guaranteed: lowerResourceMap,
			},
		}, nil, false, ""},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkResourceConfigurationsForQueue(tc.current, tc.parent)
			if tc.errorExpected {
				assert.Assert(t, err != nil, "An error is expected")
				assert.Assert(t, strings.Contains(err.Error(), tc.expectedErrorMsg), "Unexpected error message")
			} else {
				assert.NilError(t, err, "No error is expected")
			}
		})
	}
}
