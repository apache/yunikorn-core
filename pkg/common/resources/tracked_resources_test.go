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

package resources

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func CheckLenOfTrackedResource(res *TrackedResource, expected int) (bool, string) {
	if got := len(res.TrackedResourceMap); expected == 0 && (res == nil || got != expected) {
		return false, fmt.Sprintf("input with empty and nil should be a empty tracked resource: Expected %d, got %d", expected, got)
	}
	if got := len(res.TrackedResourceMap); got != expected {
		return false, fmt.Sprintf("Length of tracked resources is wrong: Expected %d, got %d", expected, got)
	}
	return true, ""
}

func CheckResourceValueOfTrackedResource(res *TrackedResource, expected map[string]map[string]int64) (bool, string) {
	for instanceType, resources := range expected {
		for key, value := range resources {
			if got := res.TrackedResourceMap[instanceType][key]; got != value {
				return false, fmt.Sprintf("instance type %s, resource %s, expected %d, got %d", instanceType, key, value, got)
			}
		}
	}
	return true, ""
}

func TestNewTrackedResourceFromMap(t *testing.T) {
	type outputs struct {
		length           int
		trackedResources map[string]map[string]int64
	}
	var tests = []struct {
		caseName string
		input    map[string]map[string]int64
		expected outputs
	}{
		{
			"nil",
			nil,
			outputs{0, map[string]map[string]int64{}},
		},
		{
			"empty",
			map[string]map[string]int64{},
			outputs{0, map[string]map[string]int64{}},
		},
		{
			"tracked resources of one instance type",
			map[string]map[string]int64{"instanceType1": {"first": 1}},
			outputs{1, map[string]map[string]int64{"instanceType1": {"first": 1}}},
		},
		{
			"tracked resources of two instance type",
			map[string]map[string]int64{"instanceType1": {"first": 0}, "instanceType2": {"second": -1}},
			outputs{2, map[string]map[string]int64{"instanceType1": {"first": 0}, "instanceType2": {"second": -1}}},
		},
		{
			"Multiple tracked resources for one instance type",
			map[string]map[string]int64{"instanceType1": {"first": 1, "second": -2, "third": 3}},
			outputs{1, map[string]map[string]int64{"instanceType1": {"first": 1, "second": -2, "third": 3}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			res := NewTrackedResourceFromMap(tt.input)
			if ok, err := CheckLenOfTrackedResource(res, tt.expected.length); !ok {
				t.Error(err)
			} else {
				if ok, err := CheckResourceValueOfTrackedResource(res, tt.expected.trackedResources); !ok {
					t.Error(err)
				}
			}
		})
	}
}

func TestTrackedResourceClone(t *testing.T) {
	var tests = []struct {
		caseName string
		input    map[string]map[string]int64
	}{
		{
			"Nil check",
			nil,
		},
		{
			"No Resources in TrackedResources",
			map[string]map[string]int64{},
		},
		{
			"Proper TrackedResource mappings",
			map[string]map[string]int64{"instanceType1": {"first": 1, "second": -2}, "instanceType2": {"second": -2, "third": 3}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			original := NewTrackedResourceFromMap(tt.input)
			cloned := original.Clone()
			if original == cloned {
				t.Errorf("cloned trackedResources pointers are equal, should not be")
			}
			if !reflect.DeepEqual(original.TrackedResourceMap, cloned.TrackedResourceMap) {
				t.Errorf("cloned trackedResources are not equal: %v / %v", original, cloned)
			}
		})
	}
}

// TestTrackedResourceAggregateTrackedResource tests the AggregateTrackedResource function.
// The modifications to the TrackedResource depend on time.Now(), making it challenging
// to construct an expected TrackedResource for verification.
// To address this, we use bindTime passed in and set it to a known time before "now."
// This provides a reasonably accurate prediction range, considering only whole seconds of the difference.
// The likelihood of a test failure due to this should be extremely low.
func TestTrackedResourceAggregateTrackedResource(t *testing.T) {
	type inputs struct {
		trackedResource map[string]map[string]int64
		instType        string
		otherResource   map[string]Quantity
		bindTime        time.Time
	}

	var tests = []struct {
		caseName                string
		input                   inputs
		expectedTrackedResource map[string]map[string]int64
	}{
		{
			"Resource to be aggregated Nil Check",
			inputs{
				map[string]map[string]int64{"instanceType1": {"first": 1, "second": -2}},
				"instanceType1",
				nil,
				time.Now().Add(-time.Minute),
			},
			map[string]map[string]int64{"instanceType1": {"first": 1, "second": -2}},
		},
		{
			"Resource to be aggregated is Empty",
			inputs{
				map[string]map[string]int64{"instanceType1": {"first": 1, "second": -2}},
				"instanceType1",
				map[string]Quantity{},
				time.Now().Add(-time.Minute),
			},
			map[string]map[string]int64{"instanceType1": {"first": 1, "second": -2}},
		},
		{
			"TrackedResource is empty",
			inputs{
				map[string]map[string]int64{},
				"instanceType1",
				map[string]Quantity{"first": 1, "second": 2},
				time.Now().Add(-time.Minute),
			},
			map[string]map[string]int64{"instanceType1": {"first": 60, "second": 120}},
		},
		{
			"With Negative Values Involved",
			inputs{
				map[string]map[string]int64{"instanceType1": {"first": 1, "second": -2}},
				"instanceType1",
				map[string]Quantity{"first": 1, "second": 2},
				time.Now().Add(-time.Minute),
			},
			map[string]map[string]int64{"instanceType1": {"first": 61, "second": 118}},
		},
		{
			"Multiple Instance Types",
			inputs{
				map[string]map[string]int64{"instanceType1": {"first": 1, "second": 2}, "instanceType2": {"third": 3, "four": 4}},
				"instanceType2",
				map[string]Quantity{"first": 1, "second": 2},
				time.Now().Add(-time.Minute),
			},
			map[string]map[string]int64{"instanceType1": {"first": 1, "second": 2}, "instanceType2": {"first": 60, "second": 120, "third": 3, "four": 4}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			original := NewTrackedResourceFromMap(tt.input.trackedResource)
			original.AggregateTrackedResource(tt.input.instType, &Resource{tt.input.otherResource}, tt.input.bindTime)
			expected := NewTrackedResourceFromMap(tt.expectedTrackedResource)

			if !reflect.DeepEqual(original.TrackedResourceMap, expected.TrackedResourceMap) {
				t.Errorf("trackedResources are not equal, original trackedResource after aggrigation: %v / expected: %v", original, expected)
			}
		})
	}
}
