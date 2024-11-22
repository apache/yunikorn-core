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
	"sort"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func CheckTrackedResource(res *TrackedResource, trMap map[string]map[string]Quantity) error {
	if len(res.TrackedResourceMap) != len(trMap) {
		return fmt.Errorf("input with empty and nil should be a empty tracked resource: Expected %d, got %d", len(trMap), len(res.TrackedResourceMap))
	}
	for instanceType, expect := range trMap {
		trackedRes := res.TrackedResourceMap[instanceType]
		expectedRes := NewResourceFromMap(expect)
		if !Equals(trackedRes, expectedRes) {
			return fmt.Errorf("instance type %s, expected %s, got %s", instanceType, trackedRes, expectedRes)
		}
	}
	return nil
}

func TestNewTrackedResourceFromMap(t *testing.T) {
	var tests = []struct {
		caseName string
		input    map[string]map[string]Quantity
		trMap map[string]map[string]Quantity
	}{
		{
			"nil",
			nil,
			map[string]map[string]Quantity{},
		},
		{
			"empty",
			map[string]map[string]Quantity{},
			map[string]map[string]Quantity{}},
		{
			"tracked resources of one instance type",
			map[string]map[string]Quantity{"instanceType1": {"first": 1}},
			map[string]map[string]Quantity{"instanceType1": {"first": 1}},
		},
		{
			"tracked resources of two instance type",
			map[string]map[string]Quantity{"instanceType1": {"first": 0}, "instanceType2": {"second": -1}},
			map[string]map[string]Quantity{"instanceType1": {"first": 0}, "instanceType2": {"second": -1}},
		},
		{
			"Multiple tracked resources for one instance type",
			map[string]map[string]Quantity{"instanceType1": {"first": 1, "second": -2, "third": 3}},
			map[string]map[string]Quantity{"instanceType1": {"first": 1, "second": -2, "third": 3}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			res := NewTrackedResourceFromMap(tt.input)
			assert.NilError(t, CheckTrackedResource(res, tt.trMap))
		})
	}
}

func TestTrackedResourceClone(t *testing.T) {
	var tests = []struct {
		caseName string
		input    map[string]map[string]Quantity
	}{
		{
			"Nil check",
			nil,
		},
		{
			"No Resources in TrackedResources",
			map[string]map[string]Quantity{},
		},
		{
			"Proper TrackedResource mappings",
			map[string]map[string]Quantity{"instanceType1": {"first": 1, "second": -2}, "instanceType2": {"second": -2, "third": 3}},
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

	// case: tracked resource is nil
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("Clone panic on nil TrackedResource")
		}
	}()
	tr := (*TrackedResource)(nil)
	cloned := tr.Clone()
	assert.Assert(t, cloned == nil)
}

// TestTrackedResourceAggregateTrackedResource tests the AggregateTrackedResource function.
// The modifications to the TrackedResource depend on time.Now(), making it challenging
// to construct an expected TrackedResource for verification.
// To address this, we use bindTime passed in and set it to a known time before "now."
// This provides a reasonably accurate prediction range, considering only whole seconds of the difference.
// The likelihood of a test failure due to this should be extremely low.
func TestTrackedResourceAggregateTrackedResource(t *testing.T) {
	type inputs struct {
		trackedResource map[string]map[string]Quantity
		instType        string
		otherResource   map[string]Quantity
		bindTime        time.Time
	}

	var tests = []struct {
		caseName                string
		input                   inputs
		expectedTrackedResource map[string]map[string]Quantity
	}{
		{
			"Resource to be aggregated Nil Check",
			inputs{
				map[string]map[string]Quantity{"instanceType1": {"first": 1, "second": -2}},
				"instanceType1",
				nil,
				time.Now().Add(-time.Minute),
			},
			map[string]map[string]Quantity{"instanceType1": {"first": 1, "second": -2}},
		},
		{
			"Resource to be aggregated is Empty",
			inputs{
				map[string]map[string]Quantity{"instanceType1": {"first": 1, "second": -2}},
				"instanceType1",
				map[string]Quantity{},
				time.Now().Add(-time.Minute),
			},
			map[string]map[string]Quantity{"instanceType1": {"first": 1, "second": -2}},
		},
		{
			"TrackedResource is empty",
			inputs{
				map[string]map[string]Quantity{},
				"instanceType1",
				map[string]Quantity{"first": 1, "second": 2},
				time.Now().Add(-time.Minute),
			},
			map[string]map[string]Quantity{"instanceType1": {"first": 60, "second": 120}},
		},
		{
			"With Negative Values Involved",
			inputs{
				map[string]map[string]Quantity{"instanceType1": {"first": 1, "second": -2}},
				"instanceType1",
				map[string]Quantity{"first": 1, "second": 2},
				time.Now().Add(-time.Minute),
			},
			map[string]map[string]Quantity{"instanceType1": {"first": 61, "second": 118}},
		},
		{
			"Multiple Instance Types",
			inputs{
				map[string]map[string]Quantity{"instanceType1": {"first": 1, "second": 2}, "instanceType2": {"third": 3, "four": 4}},
				"instanceType2",
				map[string]Quantity{"first": 1, "second": 2},
				time.Now().Add(-time.Minute),
			},
			map[string]map[string]Quantity{"instanceType1": {"first": 1, "second": 2}, "instanceType2": {"first": 60, "second": 120, "third": 3, "four": 4}},
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

	// case: resource is nil
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("Panic on nil map for new TrackedResource")
		}
	}()
	tr := NewTrackedResourceFromMap(nil)
	tr.AggregateTrackedResource("instanceType1", nil, time.Now().Add(-time.Minute))
	assert.Assert(t, tr.TrackedResourceMap != nil && len(tr.TrackedResourceMap) == 0)
}

func TestEqualsTracked(t *testing.T) {
	var tests = []struct {
		caseName string
		base    map[string]map[string]Quantity
		compare map[string]map[string]int64
		expected bool
	}{
		{"nil inputs", nil, nil, true},
		{"both empty", map[string]map[string]Quantity{}, map[string]map[string]int64{}, true},
		{"empty tracked nil dao", map[string]map[string]Quantity{}, nil, true},
		{"empty tracked",
			map[string]map[string]Quantity{},
			map[string]map[string]int64{"first": {"val": 0}},
			false,
		},
		{"same keys different values",
			map[string]map[string]Quantity{"first": {"val": 10}},
			map[string]map[string]int64{"first": {"val": 0}},
			false,
		},
		{"different instance type",
			map[string]map[string]Quantity{"first": {"val": 10}},
			map[string]map[string]int64{"second": {"val": 10}},
			false},
		{"different resource type",
			map[string]map[string]Quantity{"first": {"val": 10}},
			map[string]map[string]int64{"first": {"value": 10}},
			false},
		{"different resource count",
			map[string]map[string]Quantity{"first": {"val": 10}},
			map[string]map[string]int64{"first": {"val": 10, "sum": 7}},
			false},
		{"same keys and values",
			map[string]map[string]Quantity{"x": {"val": 10, "sum": 7}},
			map[string]map[string]int64{"x": {"val": 10, "sum": 7}},
			true},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			var base, compare *TrackedResource
			if tt.base != nil {
				base = NewTrackedResourceFromMap(tt.base)
			}

			assert.Equal(t, base.EqualsDAO(tt.compare), tt.expected, "Equal result should be %v instead of %v, left %v, right %v", tt.expected, base, compare)
		})
	}
}

func TestTrackedResourceString(t *testing.T) {
	sortTrackedResourceString := func(s string) string {
		s = strings.TrimPrefix(s, "TrackedResource{")
		s = strings.TrimSuffix(s, "}")
		parts := strings.Split(s, ",")
		sort.Strings(parts)
		return "TrackedResource{" + strings.Join(parts, ",") + "}"
	}

	// case: empty tracked resource
	tr1 := NewTrackedResource()
	expected := "TrackedResource{}"
	assert.Equal(t, sortTrackedResourceString(expected), sortTrackedResourceString(tr1.String()))

	// case: tracked resource with one instance type and one resource
	tr2 := NewTrackedResourceFromMap(map[string]map[string]Quantity{
		"instanceType1": {
			"cpu": 10,
		},
	})
	expected = "TrackedResource{instanceType1:cpu=10}"
	assert.Equal(t, sortTrackedResourceString(expected), sortTrackedResourceString(tr2.String()))

	// case: tracked resource with multiple instance types and resource
	tr3 := NewTrackedResourceFromMap(map[string]map[string]Quantity{
		"instanceType1": {
			"cpu":    10,
			"memory": 20,
		},
		"instanceType2": {
			"memory": 15,
		},
	})
	expected = "TrackedResource{instanceType1:cpu=10,instanceType1:memory=20,instanceType2:memory=15}"
	assert.Equal(t, sortTrackedResourceString(expected), sortTrackedResourceString(tr3.String()))

	defer func() {
		if r := recover(); r != nil {
			t.Fatal("String panic on nil TrackedResource")
		}
	}()
	tr := (*TrackedResource)(nil)
	str := tr.String()
	assert.Equal(t, str, "TrackedResource{}")
}
