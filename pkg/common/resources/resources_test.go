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
	"math"
	"reflect"
	"testing"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"

	"gotest.tools/assert"
)

func TestNewResourceFromConf(t *testing.T) {
    type expectedvalues struct {
        resourceExist bool
        resourceTypes int
        resources     string
    }
    var tests = []struct {
        caseName string
        input    map[string]string
        expected expectedvalues
    }{
        {"resource with nil input", nil, expectedvalues{true, 0, ""}},
        {"resource without any resource mappings", make(map[string]string), expectedvalues{true, 0, ""}},
        {"resource with zero value", map[string]string{"zero": "0"}, expectedvalues{true, 1, "map[zero:0]"}},
        {"normal multipliers with \"M\"", map[string]string{"memory": "10M"}, expectedvalues{true, 1, "map[memory:10000000]"}},
        {"normal multipliers with \"Mi\"", map[string]string{"memory": "10Mi"}, expectedvalues{true, 1, "map[memory:10485760]"}},
        {"vcore multipliers with \"k\"", map[string]string{"vcore": "10k"}, expectedvalues{true, 1, "map[vcore:10000000]"}},
        {"vcore multipliers", map[string]string{"vcore": "10"}, expectedvalues{true, 1, "map[vcore:10000]"}},
        {"vcore multipliers with \"m\"", map[string]string{"vcore": "10m"}, expectedvalues{true, 1, "map[vcore:10]"}},
        {"failure case: parse error", map[string]string{"fail": "xx"}, expectedvalues{false, 0, ""}},
        {"negative resource", map[string]string{"memory": "-15"}, expectedvalues{false, 0, ""}},
        {"\"milli\" used for anything other than vcore", map[string]string{"memory": "10m"}, expectedvalues{false, 0, ""}},
    }
    for _, tt := range tests {
        t.Run(tt.caseName, func(t *testing.T) {
            if original, err := NewResourceFromConf(tt.input); tt.expected.resourceExist {
                if err != nil || len(original.Resources) != tt.expected.resourceTypes {
                    t.Errorf("Expected err nil, numbers of resource types %v. Got err %v, numbers of resource types %v", tt.expected.resourceTypes, err, len(original.Resources))
                }
                if len(tt.expected.resources) > 0 && tt.expected.resources != original.String() {
                    t.Errorf("Resource value:got %s, expected %s", original.String(), tt.expected.resources)
                }
            } else {
                if err == nil || original != nil {
                    t.Errorf("new resource create should have returned error. got err %v, res %v", err, original)
                }
            }
        })
    }
}

func TestCloneNil(t *testing.T) {
	// make sure we're nil safe IDE will complain about the non nil check
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil resource in clone test")
		}
	}()
	var empty *Resource
	if empty.Clone() != nil {
		t.Fatalf("clone of nil resource should be nil")
	}
}

func TestClone(t *testing.T) {
    var tests = []struct {
        caseName string
        input    map[string]Quantity
    }{
        {"resource without any resource mappings", map[string]Quantity{}},
        {"resource mappings", map[string]Quantity{"first": 1, "second": -2, "third": 3}},
        {"set a resource value to 0", map[string]Quantity{"first": 1, "zero": 0, "third": 3}},
    }
    for _, tt := range tests {
        t.Run(tt.caseName, func(t *testing.T) {
            original := NewResourceFromMap(tt.input)
            cloned := original.Clone()
            if original == cloned {
                t.Errorf("cloned resources pointers are equal, should not be")
            }
            if !reflect.DeepEqual(original.Resources, cloned.Resources) {
                t.Errorf("cloned resources are not equal: %v / %v", original, cloned)
            }
        })
    }
}

func TestEquals(t *testing.T) {
    type inputs struct {
        base    map[string]Quantity
        compare map[string]Quantity
    }
    var tests = []struct {
        caseName string
        input    inputs
        expected bool
    }{
        {"simple cases (nil checks)", inputs{nil, nil}, true},
        {"simple cases (nil checks)", inputs{map[string]Quantity{}, nil}, false},
        {"simple cases (nil checks)", inputs{map[string]Quantity{}, map[string]Quantity{"zero": 0}}, true},
        {"Empty mapping and resource containing zero value are same", inputs{map[string]Quantity{}, map[string]Quantity{"zero": 0}}, true},
        {"Same key and different resource value", inputs{map[string]Quantity{"first": 10}, map[string]Quantity{"first": 0}}, false},
        {"Different key and same resource value", inputs{map[string]Quantity{"first": 10}, map[string]Quantity{"second": 10}}, false},
        {"Same key and same resource value", inputs{map[string]Quantity{"first": 10}, map[string]Quantity{"first": 10}}, true},
        {"Different types of keys", inputs{map[string]Quantity{"first": 1}, map[string]Quantity{"first": 1, "second": 1}}, false},
    }
    for _, tt := range tests {
        t.Run(tt.caseName, func(t *testing.T) {
            var base *Resource
            var compare *Resource
            if base = nil; tt.input.base != nil {
                base = NewResourceFromMap(tt.input.base)
            }
            if compare = nil; tt.input.compare != nil {
                compare = NewResourceFromMap(tt.input.compare)
            }

            if result := Equals(base, compare); result != tt.expected {
                t.Errorf("Equal result should be %v instead of %v, left %v, right %v", tt.expected, result, base, compare)
            }
            if result := Equals(compare, base); result != tt.expected {
                t.Errorf("Equal result should be %v instead of %v, left %v, right %v", tt.expected, result, compare, base)
            }
        })
    }
}

func TestIsZero(t *testing.T) {
	// simple cases (nil checks)
	if !IsZero(nil) {
		t.Errorf("nil resource should be zero")
	}
	base := NewResourceFromMap(map[string]Quantity{})
	if !IsZero(base) {
		t.Errorf("no resource entries should be zero")
	}

	// set resource values
	base = NewResourceFromMap(map[string]Quantity{"zero": 0})
	if !IsZero(base) {
		t.Errorf("only zero resources should be zero")
	}
	base = NewResourceFromMap(map[string]Quantity{"first": 10})
	if IsZero(base) {
		t.Errorf("set resource should be non zero")
	}
}

func TestStrictlyGreaterThanZero(t *testing.T) {
    var tests = []struct {
        caseName string
        input    map[string]Quantity
        expected bool
    }{
        {"nil resource should be smaller than zero", nil, false},
        {"no resource entries should be smaller than zero", map[string]Quantity{}, false},
        {"only zero resources should be smaller than zero", map[string]Quantity{"zero": 0}, false},
        {"only positive resource should be greater than zero", map[string]Quantity{"first": 10}, true},
        {"negative resource should be smaller than zero", map[string]Quantity{"first": -1}, false},
        {"multiple resources containing negative resources should be smaller than zero", map[string]Quantity{"first": -1, "second": 1, "third": -1}, false},
    }
    for _, tt := range tests {
        t.Run(tt.caseName, func(t *testing.T) {
            var base *Resource
            if tt.input != nil {
                base = NewResourceFromMap(tt.input)
            }
            if result := StrictlyGreaterThanZero(base); result != tt.expected {
                t.Errorf("StrictlyGreaterThanZero: got %v, expected %v", result, tt.expected)
            }
        })
    }
}

func TestStrictlyGreaterThan(t *testing.T) {
    type inputs struct {
        larger  map[string]Quantity
        smaller map[string]Quantity
        sameRef bool
    }
    type outputs struct {
        larger  bool
        smaller bool
    }
    var tests = []struct {
        caseName string
        input    inputs
        expected outputs
    }{
        {"Nil check", inputs{nil, nil, false}, outputs{false, false}},
        {"Empty resources", inputs{map[string]Quantity{}, map[string]Quantity{}, false}, outputs{false, false}},
        {"An empty resource and a zero resource", inputs{map[string]Quantity{"zero": 0}, map[string]Quantity{}, false}, outputs{false, false}},
        {"Zero resources", inputs{map[string]Quantity{"zero": 0}, map[string]Quantity{"zero": 0}, false}, outputs{false, false}},
        {"An empty resource and a negative resource", inputs{map[string]Quantity{}, map[string]Quantity{"first": -1}, false}, outputs{true, false}},
        {"Positive resource and same references", inputs{map[string]Quantity{"first": 10}, nil, true}, outputs{false, false}},
        {"Positive numbers", inputs{map[string]Quantity{"first": 10}, map[string]Quantity{"first": 1}, false}, outputs{true, false}},
        {"Different resources", inputs{map[string]Quantity{"first": 10}, map[string]Quantity{"second": 1}, false}, outputs{false, false}},
        {"Mutiple resources and a single resource", inputs{map[string]Quantity{"first": 10, "second": 10}, map[string]Quantity{"first": 1}, false}, outputs{true, false}},
        {"Negative resource is smaller than not set", inputs{map[string]Quantity{"first": 10, "second": -1}, map[string]Quantity{"first": 10}, false}, outputs{false, true}},
        {"Negative resource is smaller than not set", inputs{map[string]Quantity{"first": -1}, map[string]Quantity{}, false}, outputs{false, true}},
    }
    for _, tt := range tests {
        t.Run(tt.caseName, func(t *testing.T) {
            var compare, base *Resource
            if compare = nil; tt.input.larger != nil {
                compare = NewResourceFromMap(tt.input.larger)
            }
            if base = nil; tt.input.sameRef {
                base = compare
            } else {
                base = NewResourceFromMap(tt.input.smaller)
            }
            if result := StrictlyGreaterThan(compare, base); result != tt.expected.larger {
                t.Errorf("comapre %v, base %v, got %v, expeceted %v", compare, base, result, tt.expected.larger)
            }
            if result := StrictlyGreaterThan(base, compare); result != tt.expected.smaller {
                t.Errorf("base %v, compare %v, got %v, expeceted %v", base, compare, result, tt.expected.smaller)
            }
        })
    }
}

func TestStrictlyGreaterThanOrEquals(t *testing.T) {
	// simple case (nil checks)
	if !StrictlyGreaterThanOrEquals(nil, nil) {
		t.Errorf("nil resources should be greater or equal")
	}
	// zero or empty resource values
	larger := NewResourceFromMap(map[string]Quantity{})
	smaller := NewResourceFromMap(map[string]Quantity{})
	if !StrictlyGreaterThanOrEquals(larger, smaller) {
		t.Errorf("no resource entries should be greater or equal")
	}
	larger = NewResourceFromMap(map[string]Quantity{"zero": 0})
	if !StrictlyGreaterThanOrEquals(larger, smaller) {
		t.Errorf("zero resources should be greater or equal empty")
	}
	smaller = NewResourceFromMap(map[string]Quantity{"other": 0})
	if !StrictlyGreaterThanOrEquals(larger, smaller) {
		t.Errorf("only zero resource (both objects) should be greater or equal")
	}

	// set resource values
	larger = NewResourceFromMap(map[string]Quantity{"first": 10})
	if !StrictlyGreaterThanOrEquals(larger, smaller) {
		t.Errorf("set resources %v should be greater than zero %v", larger, smaller)
	}
	smaller = NewResourceFromMap(map[string]Quantity{"second": 10})
	if StrictlyGreaterThanOrEquals(larger, smaller) {
		t.Errorf("different resource set should be not be greater or equal %v and %v", larger, smaller)
	}
	// negative resource is smaller than not set
	larger = NewResourceFromMap(map[string]Quantity{"first": 1, "second": -1})
	smaller = NewResourceFromMap(map[string]Quantity{"first": 1})
	if StrictlyGreaterThanOrEquals(larger, smaller) {
		t.Errorf("negative %v returned as larger compared to not set %v", larger, smaller)
	}
	if !StrictlyGreaterThanOrEquals(smaller, larger) {
		t.Errorf("negative %v returned as larger compared to not set %v", smaller, larger)
	}
}

func TestComponentWiseMin(t *testing.T) {
	// simple case (nil checks)
	result := ComponentWiseMin(nil, Zero)
	if len(result.Resources) != 0 {
		t.Errorf("nil resource (left) did not return zero resource: %v", result)
	}
	result = ComponentWiseMin(Zero, nil)
	if len(result.Resources) != 0 {
		t.Errorf("nil resource (right) did not return zero resource: %v", result)
	}

	// empty resources
	res1 := NewResourceFromMap(map[string]Quantity{})
	result = ComponentWiseMin(res1, res1)
	if len(result.Resources) != 0 {
		t.Errorf("empty resource did not return zero resource: %v", result)
	}

	// set resource value in first resource
	res2 := NewResourceFromMap(map[string]Quantity{"zero": 0})
	result = ComponentWiseMin(res2, res1)
	if len(result.Resources) != 1 {
		t.Errorf("set resource did not return (zero key): %v", result)
	}
	if _, ok := result.Resources["zero"]; !ok {
		t.Errorf("resource key not set (zero): %v", result)
	}
	// set resource value in second resource
	result = ComponentWiseMin(res1, res2)
	if len(result.Resources) != 1 {
		t.Errorf("set resource did not return (zero key): %v", result)
	}
	if _, ok := result.Resources["zero"]; !ok {
		t.Errorf("resource key not set (zero): %v", result)
	}

	// resource with value set: different in each values are wiped
	res1 = NewResourceFromMap(map[string]Quantity{"first": 5})
	res2 = NewResourceFromMap(map[string]Quantity{"second": 10})
	result = ComponentWiseMin(res1, res2)
	if result == nil || len(result.Resources) != 2 {
		t.Fatalf("set resource should be greater than zero: %v", result)
	}
	if value, ok := result.Resources["first"]; !ok || value != 0 {
		t.Errorf("resource key not set expected %v got %v", res1, result)
	}
	if value, ok := result.Resources["second"]; !ok || value != 0 {
		t.Errorf("resource key not set expected %v got %v", res2, result)
	}

	// same resource set value from res1 returned
	res2 = NewResourceFromMap(map[string]Quantity{"first": 10})
	result = ComponentWiseMin(res1, res2)
	if result == nil || len(result.Resources) != 1 {
		t.Fatalf("set resource should be greater than zero: %v", result)
	}
	if value, ok := result.Resources["first"]; !ok || value != 5 {
		t.Errorf("resource key not set expected %v got %v", res1, result)
	}
	result = ComponentWiseMin(res2, res1)
	if result == nil || len(result.Resources) != 1 {
		t.Fatalf("set resource should be greater than zero: %v", result)
	}
	if value, ok := result.Resources["first"]; !ok || value != 5 {
		t.Errorf("resource key not set expected %v got %v", res1, result)
	}

	// negative resources set: one with pos value other not defined
	res1 = NewResourceFromMap(map[string]Quantity{"first": -5, "second": -5})
	result = ComponentWiseMin(res1, res2)
	if result == nil || len(result.Resources) != 2 {
		t.Fatalf("set resource should be greater than zero: %v", result)
	}
	if value, ok := result.Resources["first"]; !ok || value != -5 {
		t.Errorf("resource key not set expected %v got %v", res1, result)
	}
	if value, ok := result.Resources["second"]; !ok || value != -5 {
		t.Errorf("resource key not set expected %v got %v", res1, result)
	}
}

func TestComponentWiseMinPermissive(t *testing.T) {
	smallerRes := NewResourceFromMap(map[string]Quantity{"first": 5, "second": 15, "third": 6})
	higherRes := NewResourceFromMap(map[string]Quantity{"first": 7, "second": 10, "forth": 6})
	expected := NewResourceFromMap(map[string]Quantity{"first": 5, "second": 10, "third": 6, "forth": 6})

	testCases := []struct {
		name     string
		res1     *Resource
		res2     *Resource
		expected *Resource
	}{
		{"Both resources nil", nil, nil, nil},
		{"First resource nil", nil, smallerRes, smallerRes},
		{"Second resource nil", smallerRes, nil, smallerRes},
		{"First resource smaller than the second", smallerRes, higherRes, expected},
		{"Second resource smaller than the first", higherRes, smallerRes, expected},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ComponentWiseMinPermissive(tc.res1, tc.res2)
			assert.DeepEqual(t, result, tc.expected)
		})
	}
}

func TestComponentWiseMax(t *testing.T) {
	// simple case (nil checks)
	result := ComponentWiseMax(nil, Zero)
	if result == nil || len(result.Resources) != 0 {
		t.Errorf("nil resource (left) did not return zero resource: %v", result)
	}
	result = ComponentWiseMax(Zero, nil)
	if result == nil || len(result.Resources) != 0 {
		t.Errorf("nil resource (right) did not return zero resource: %v", result)
	}

	// empty resources
	res1 := NewResourceFromMap(map[string]Quantity{})
	result = ComponentWiseMax(res1, res1)
	if result == nil || len(result.Resources) != 0 {
		t.Errorf("empty resource did not return zero resource: %v", result)
	}

	// set resource value in first resource
	res2 := NewResourceFromMap(map[string]Quantity{"zero": 0})
	result = ComponentWiseMax(res2, res1)
	if result == nil || len(result.Resources) != 1 {
		t.Fatalf("set resource did not return (zero key): %v", result)
	}
	if _, ok := result.Resources["zero"]; !ok {
		t.Errorf("resource key not set (zero): %v", result)
	}
	// set resource value in second resource
	result = ComponentWiseMax(res1, res2)
	if result == nil || len(result.Resources) != 1 {
		t.Fatalf("set resource did not return (zero key): %v", result)
	}
	if _, ok := result.Resources["zero"]; !ok {
		t.Errorf("resource key not set (zero) resource: %v", result)
	}

	// resource with value set: different in each values are merged
	res1 = NewResourceFromMap(map[string]Quantity{"first": 5})
	res2 = NewResourceFromMap(map[string]Quantity{"second": 10})
	result = ComponentWiseMax(res1, res2)
	if result == nil || len(result.Resources) != 2 {
		t.Fatalf("set resource should be greater than zero: %v", result)
	}
	if value, ok := result.Resources["first"]; !ok || value != 5 {
		t.Errorf("resource key not set expected %v got %v", res1, result)
	}
	if value, ok := result.Resources["second"]; !ok || value != 10 {
		t.Errorf("resource key not set expected %v got %v", res2, result)
	}

	// same resource set value from res2 returned
	res2 = NewResourceFromMap(map[string]Quantity{"first": 10})
	result = ComponentWiseMax(res1, res2)
	if result == nil || len(result.Resources) != 1 {
		t.Fatalf("set resource should be greater than zero: %v", result)
	}
	if value, ok := result.Resources["first"]; !ok || value != 10 {
		t.Errorf("resource key not set expected %v got %v", res2, result)
	}
	result = ComponentWiseMax(res2, res1)
	if result == nil || len(result.Resources) != 1 {
		t.Fatalf("set resource should be greater than zero: %v", result)
	}
	if value, ok := result.Resources["first"]; !ok || value != 10 {
		t.Errorf("resource key not set expected %v got %v", res2, result)
	}

	// negative resources set: one with pos value other not defined
	res1 = NewResourceFromMap(map[string]Quantity{"first": -5, "second": -5})
	result = ComponentWiseMax(res1, res2)
	if result == nil || len(result.Resources) != 2 {
		t.Fatalf("set resource should be greater than zero: %v", result)
	}
	if value, ok := result.Resources["first"]; !ok || value != 10 {
		t.Errorf("resource key not set expected %v got %v", res1, result)
	}
	if value, ok := result.Resources["second"]; !ok || value != 0 {
		t.Errorf("resource key not set expected %v got %v", res1, result)
	}
}

func TestToProtoNil(t *testing.T) {
	// make sure we're nil safe IDE will complain about the non nil check
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil resource in proto test")
		}
	}()
	var empty *Resource
	proto := empty.ToProto()
	if proto == nil || len(proto.Resources) != 0 {
		t.Fatalf("to proto on nil resource should have given non nil object")
	}
}

func TestResourceProto(t *testing.T) {
	empty := NewResourceFromProto(nil)
	if empty == nil || len(empty.Resources) != 0 {
		t.Errorf("nil proto gave non empty resource: %v", empty)
	}
	// simple resource with no values to proto
	res1 := NewResourceFromMap(map[string]Quantity{})
	toProto := res1.ToProto()
	if len(toProto.Resources) != 0 {
		t.Fatalf("empty resource to proto conversion failed: %v", toProto)
	}
	// convert back to resource
	res2 := NewResourceFromProto(toProto)
	// res1 and res2 content must be equal
	if !reflect.DeepEqual(res1.Resources, res2.Resources) {
		t.Errorf("resource to proto and back to resource does not give same resources: original %v after %v", res1, res2)
	}

	// simple resource with values to proto
	res1 = NewResourceFromMap(map[string]Quantity{"first": 5, "second": -5})
	toProto = res1.ToProto()
	if len(toProto.Resources) != 2 {
		t.Fatalf("resource to proto conversion failed: %v", toProto)
	}
	// convert back to resource
	res2 = NewResourceFromProto(toProto)
	// res1 and res2 content must be equal
	if !reflect.DeepEqual(res1.Resources, res2.Resources) {
		t.Errorf("resource to proto and back to resource does not give same resources: original %v after %v", res1, res2)
	}

	// resource with zero set values to proto
	res1 = NewResourceFromMap(map[string]Quantity{"first": 5, "second": 0, "third": -5})
	toProto = res1.ToProto()
	if len(toProto.Resources) != 3 {
		t.Fatalf("resource to proto conversion failed: %v", toProto)
	}
	// convert back to resource
	res2 = NewResourceFromProto(toProto)
	// res1 and res2 content must be equal
	if !reflect.DeepEqual(res1.Resources, res2.Resources) {
		t.Errorf("resource to proto and back to resource does not give same resources: original %v after %v", res1, res2)
	}
}

func TestMultiplyBy(t *testing.T) {
	// simple case (nil checks)
	result := MultiplyBy(nil, 0)
	if result == nil || len(result.Resources) != 0 {
		t.Errorf("nil resource (left) did not return zero resource: %v", result)
	}
	// simple case empty resource
	base := NewResourceFromMap(map[string]Quantity{})
	result = MultiplyBy(base, 1)
	if len(result.Resources) != 0 {
		t.Errorf("empty resource did not return zero resource: %v", result)
	}

	// zero multiply factor
	base = NewResourceFromMap(map[string]Quantity{"first": 5})
	result = MultiplyBy(base, 0)
	if len(result.Resources) != 0 {
		t.Errorf("zero factor did not return correct number of resource values: %v", result)
	}

	// fractional multiplier
	// positive multiplication factor
	base = NewResourceFromMap(map[string]Quantity{"first": 5, "second": -5})
	result = MultiplyBy(base, 1.9)
	if len(result.Resources) != 2 {
		t.Errorf("positive factor did not return correct number of resource values: %v", result)
	}
	if val, ok := result.Resources["first"]; !ok || val != 9 {
		t.Errorf("positive factor did not set first value correctly: %v", result)
	}
	if val, ok := result.Resources["second"]; !ok || val != -9 {
		t.Errorf("positive factor did not set second value correctly: %v", result)
	}
	// negative multiplication factor
	base = NewResourceFromMap(map[string]Quantity{"first": 5, "second": -5})
	result = MultiplyBy(base, -1.9)
	if len(result.Resources) != 2 {
		t.Errorf("negative factor did not return correct number of resource values: %v", result)
	}
	if val, ok := result.Resources["first"]; !ok || val != -9 {
		t.Errorf("negative factor did not set first value correctly: %v", result)
	}
	if val, ok := result.Resources["second"]; !ok || val != 9 {
		t.Errorf("negative factor did not set second value correctly: %v", result)
	}
}

func TestMultiply(t *testing.T) {
	// simple case (nil checks)
	result := Multiply(nil, 0)
	if result == nil || len(result.Resources) != 0 {
		t.Errorf("nil resource (left) did not return zero resource: %v", result)
	}
	// simple case empty resource
	base := NewResourceFromMap(map[string]Quantity{})
	result = Multiply(base, 1)
	if len(result.Resources) != 0 {
		t.Errorf("empty resource did not return zero resource: %v", result)
	}

	// zero multiply factor
	base = NewResourceFromMap(map[string]Quantity{"first": 5})
	result = Multiply(base, 0)
	if len(result.Resources) != 0 {
		t.Errorf("zero factor did not return no resource values: %v", result)
	}

	// positive multiplication factor
	base = NewResourceFromMap(map[string]Quantity{"first": 5, "second": -5})
	result = Multiply(base, 2)
	if len(result.Resources) != 2 {
		t.Errorf("positive factor did not return correct number of resource values: %v", result)
	}
	if val, ok := result.Resources["first"]; !ok || val != 10 {
		t.Errorf("positive factor did not set first value correctly: %v", result)
	}
	if val, ok := result.Resources["second"]; !ok || val != -10 {
		t.Errorf("positive factor did not set second value correctly: %v", result)
	}
	// negative multiplication factor
	base = NewResourceFromMap(map[string]Quantity{"first": 5, "second": -5})
	result = Multiply(base, -2)
	if len(result.Resources) != 2 {
		t.Errorf("negative factor did not return correct number of resource values: %v", result)
	}
	if val, ok := result.Resources["first"]; !ok || val != -10 {
		t.Errorf("negative factor did not set first value correctly: %v", result)
	}
	if val, ok := result.Resources["second"]; !ok || val != 10 {
		t.Errorf("negative factor did not set second value correctly: %v", result)
	}
}

func TestMultiplyToNil(t *testing.T) {
	// make sure we're nil safe IDE will complain about the non nil check
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil resource in multiplyto test")
		}
	}()
	var empty *Resource
	empty.MultiplyTo(0)
}

func TestMultiplyTo(t *testing.T) {
	// simple case empty resource
	base := NewResourceFromMap(map[string]Quantity{})
	base.MultiplyTo(1)
	if len(base.Resources) != 0 {
		t.Errorf("empty resource did not return zero resource: %v", base)
	}

	// zero multiply factor
	base = NewResourceFromMap(map[string]Quantity{"first": 5})
	base.MultiplyTo(0)
	if len(base.Resources) != 1 {
		t.Errorf("zero factor did not return correct number of resource values: %v", base)
	}
	if val, ok := base.Resources["first"]; !ok || val != 0 {
		t.Errorf("zero factor did not set value to zero: %v", base)
	}

	// fractional multiplier
	// positive multiplication factor
	base = NewResourceFromMap(map[string]Quantity{"first": 5, "second": -5})
	base.MultiplyTo(1.9)
	if len(base.Resources) != 2 {
		t.Errorf("positive factor did not return correct number of resource values: %v", base)
	}
	if val, ok := base.Resources["first"]; !ok || val != 9 {
		t.Errorf("positive factor did not set first value correctly: %v", base)
	}
	if val, ok := base.Resources["second"]; !ok || val != -9 {
		t.Errorf("positive factor did not set second value correctly: %v", base)
	}
	// negative multiplication factor
	base = NewResourceFromMap(map[string]Quantity{"first": 5, "second": -5})
	base.MultiplyTo(-1.9)
	if len(base.Resources) != 2 {
		t.Errorf("negative factor did not return correct number of resource values: %v", base)
	}
	if val, ok := base.Resources["first"]; !ok || val != -9 {
		t.Errorf("negative factor did not set first value correctly: %v", base)
	}
	if val, ok := base.Resources["second"]; !ok || val != 9 {
		t.Errorf("negative factor did not set second value correctly: %v", base)
	}
}

func TestWrapSafe(t *testing.T) {
	// additions and subtract use the same code
	if addVal(math.MaxInt64, 1) != math.MaxInt64 {
		t.Error("MaxInt64 + 1 != MaxInt64")
	}
	if addVal(math.MinInt64, -1) != math.MinInt64 {
		t.Error("MinInt64 + (-1) != MinInt64")
	}
	if addVal(10, 10) != 20 {
		t.Error("10 + 10 != 20")
	}
	if addVal(10, -20) != -10 {
		t.Error("10 + (-20) != -10")
	}
	if addVal(-10, 20) != 10 {
		t.Error("-10 + 20 != 10")
	}
	if addVal(-20, 10) != -10 {
		t.Error("-20 + 10 != -10")
	}
	if addVal(20, -10) != 10 {
		t.Error("20 + (-10) != 10")
	}
	// subtract special cases
	if subVal(math.MinInt64, 1) != math.MinInt64 {
		t.Error("MinInt64 - 1 != MinInt64")
	}
	if subVal(math.MaxInt64, -1) != math.MaxInt64 {
		t.Error("MaxInt64 - (-1) != MaxInt64")
	}

	// multiplications
	if mulVal(0, 0) != 0 {
		t.Error("0 * 0 != 0")
	}
	if mulVal(math.MaxInt64, -1) != math.MinInt64+1 {
		t.Error("MaxInt64 * -1 != MinInt64 + 1")
	}
	if mulVal(math.MinInt64, -1) != math.MaxInt64 {
		t.Error("MinInt64 * -1 != MaxInt64")
	}
	if mulVal(100000000000000000, -2000) != math.MinInt64 {
		t.Error("100000000000000000 * -2000 != MinInt64")
	}
	// strange one this returns -4 without checks
	if mulVal(math.MaxInt64/2, 4) != math.MaxInt64 {
		t.Error("math.MaxInt64/2 * 4 != MaxInt64")
	}

	// large base
	if mulValRatio(math.MaxInt64, 2) != math.MaxInt64 {
		t.Error("float MaxInt64 * 2 != MaxInt64")
	}
	// small base
	if mulValRatio(math.MinInt64, 2) != math.MinInt64 {
		t.Error("float MinInt64 * 2 != MinInt64")
	}
}

func TestAdd(t *testing.T) {
	// simple case (nil checks)
	result := Add(nil, nil)
	if result == nil || len(result.Resources) != 0 {
		t.Errorf("add nil resources did not return zero resource: %v", result)
	}
	// empty resources
	left := Zero
	result = Add(left, nil)
	if result == nil || len(result.Resources) != 0 || result == left {
		t.Errorf("add Zero resource (right) did not return cloned resource: %v", result)
	}

	// simple empty resources
	res1 := NewResourceFromMap(map[string]Quantity{"a": 5})
	result = Add(left, res1)
	if result == nil || len(result.Resources) != 1 || result.Resources["a"] != 5 {
		t.Errorf("add simple resource did not return cloned input resource: %v", result)
	}

	// complex case: just checking the resource merge, values check is secondary
	res1 = &Resource{Resources: map[string]Quantity{"a": 0, "b": 1}}
	res2 := &Resource{Resources: map[string]Quantity{"a": 1, "c": 0, "d": -1}}
	res3 := Add(res1, res2)

	expected := map[string]Quantity{"a": 1, "b": 1, "c": 0, "d": -1}
	if !reflect.DeepEqual(res3.Resources, expected) {
		t.Errorf("add failed expected %v, actual %v", expected, res3.Resources)
	}
}

func TestAddToNil(t *testing.T) {
	// make sure we're nil safe IDE will complain about the non nil check
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil resource in addto test")
		}
	}()
	var empty *Resource
	empty.AddTo(NewResource())
}

func TestAddTo(t *testing.T) {
	// simple case (nil checks) & empty resources
	base := NewResource()
	base.AddTo(nil)
	if len(base.Resources) != 0 {
		t.Errorf("addto nil resource modified base resource: %v", base)
	}

	// simple resources
	res1 := NewResourceFromMap(map[string]Quantity{"a": 5})
	base.AddTo(res1)
	if len(base.Resources) != 1 && base.Resources["a"] != 5 {
		t.Errorf("addto simple resource did not return merged input resource: %v", base)
	}

	// complex case: just checking the resource merge, values check is secondary
	base = &Resource{Resources: map[string]Quantity{"a": 0, "b": 1}}
	res1 = &Resource{Resources: map[string]Quantity{"a": 1, "c": 0, "d": -1}}
	base.AddTo(res1)

	expected := map[string]Quantity{"a": 1, "b": 1, "c": 0, "d": -1}
	if !reflect.DeepEqual(base.Resources, expected) {
		t.Errorf("addto failed expected %v, actual %v", expected, base.Resources)
	}
}

func TestSub(t *testing.T) {
	// simple case (nil checks)
	result := Sub(nil, nil)
	if result == nil || len(result.Resources) != 0 {
		t.Errorf("sub nil resources did not return zero resource: %v", result)
	}
	// empty resources
	left := NewResource()
	result = Sub(left, nil)
	if result == nil || len(result.Resources) != 0 || result == left {
		t.Errorf("sub Zero resource (right) did not return cloned resource: %v", result)
	}

	// simple empty resources
	res1 := NewResourceFromMap(map[string]Quantity{"a": 5})
	result = Sub(left, res1)
	if result == nil || len(result.Resources) != 1 || result.Resources["a"] != -5 {
		t.Errorf("sub simple resource did not return correct resource: %v", result)
	}

	// complex case: just checking the resource merge, values check is secondary
	res1 = &Resource{Resources: map[string]Quantity{"a": 0, "b": 1}}
	res2 := &Resource{Resources: map[string]Quantity{"a": 1, "c": 0, "d": -1}}
	res3 := Sub(res1, res2)

	expected := map[string]Quantity{"a": -1, "b": 1, "c": 0, "d": 1}
	if !reflect.DeepEqual(res3.Resources, expected) {
		t.Errorf("Add failed expected %v, actual %v", expected, res3.Resources)
	}
}

func TestSubFromNil(t *testing.T) {
	// make sure we're nil safe IDE will complain about the non nil check
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil resource in subfrom test")
		}
	}()
	var empty *Resource
	empty.SubFrom(NewResource())
}

func TestSubFrom(t *testing.T) {
	// simple case (nil checks) & empty resources
	base := NewResource()
	base.SubFrom(nil)
	if len(base.Resources) != 0 {
		t.Errorf("subfrom nil resource modified base resource: %v", base)
	}

	// simple resources
	res1 := NewResourceFromMap(map[string]Quantity{"a": 5})
	base.SubFrom(res1)
	if len(base.Resources) != 1 && base.Resources["a"] != -5 {
		t.Errorf("subfrom simple resource did not return merged input resource: %v", base)
	}

	// complex case: just checking the resource merge, values check is secondary
	base = &Resource{Resources: map[string]Quantity{"a": 0, "b": 1}}
	res1 = &Resource{Resources: map[string]Quantity{"a": 1, "c": 0, "d": -1}}
	base.SubFrom(res1)

	expected := map[string]Quantity{"a": -1, "b": 1, "c": 0, "d": 1}
	if !reflect.DeepEqual(base.Resources, expected) {
		t.Errorf("subfrom failed expected %v, actual %v", expected, base.Resources)
	}
}

func TestSubEliminateNegative(t *testing.T) {
	// simple case (nil checks)
	result := SubEliminateNegative(nil, nil)
	if result == nil || len(result.Resources) != 0 {
		t.Errorf("sub nil resources did not return zero resource: %v", result)
	}
	// empty resources
	left := NewResource()
	result = SubEliminateNegative(left, nil)
	if result == nil || len(result.Resources) != 0 || result == left {
		t.Errorf("sub Zero resource (right) did not return cloned resource: %v", result)
	}

	// simple empty resources
	res1 := NewResourceFromMap(map[string]Quantity{"a": 5})
	result = SubEliminateNegative(left, res1)
	if result == nil || len(result.Resources) != 1 || result.Resources["a"] != 0 {
		t.Errorf("sub simple resource did not return correct resource: %v", result)
	}

	// complex case: just checking the resource merge, values check is secondary
	res1 = &Resource{Resources: map[string]Quantity{"a": 0, "b": 1}}
	res2 := &Resource{Resources: map[string]Quantity{"a": 1, "c": 0, "d": -1}}
	res3 := SubEliminateNegative(res1, res2)

	expected := map[string]Quantity{"a": 0, "b": 1, "c": 0, "d": 1}
	if !reflect.DeepEqual(res3.Resources, expected) {
		t.Errorf("sub failed expected %v, actual %v", expected, res3.Resources)
	}
}

func TestSubErrorNegative(t *testing.T) {
	// simple case (nil checks)
	result, err := SubErrorNegative(nil, nil)
	if err != nil || result == nil || len(result.Resources) != 0 {
		t.Errorf("sub nil resources did not return zero resource: %v, %v", result, err)
	}
	// empty resources
	left := NewResource()
	result, err = SubErrorNegative(left, nil)
	if err != nil || result == nil || len(result.Resources) != 0 || result == left {
		t.Errorf("sub Zero resource (right) did not return cloned resource: %v, %v", result, err)
	}

	// simple empty resources
	res1 := NewResourceFromMap(map[string]Quantity{"a": 5})
	result, err = SubErrorNegative(left, res1)
	if err == nil || result == nil || len(result.Resources) != 1 || result.Resources["a"] != 0 {
		t.Errorf("sub simple resource did not return correct resource or no error: %v, %v", result, err)
	}

	// complex case: just checking the resource merge, values check is secondary
	res1 = &Resource{Resources: map[string]Quantity{"a": 0, "b": 1}}
	res2 := &Resource{Resources: map[string]Quantity{"a": 1, "c": 1, "d": -1}}
	result, err = SubErrorNegative(res1, res2)
	if err == nil {
		t.Errorf("sub should have set error message and did not: %v", result)
	}

	expected := map[string]Quantity{"a": 0, "b": 1, "c": 0, "d": 1}
	if !reflect.DeepEqual(result.Resources, expected) {
		t.Errorf("sub failed expected %v, actual %v", expected, result.Resources)
	}
}

func TestEqualsOrEmpty(t *testing.T) {
	var tests = []struct {
		left, right *Resource
		want        bool
	}{
		{nil, nil, true},
		{nil, NewResourceFromMap(map[string]Quantity{"a": 0, "b": 1}), false},
		{NewResourceFromMap(map[string]Quantity{"a": 0, "b": 1}), nil, false},
		{nil, NewResource(), true},
		{NewResource(), nil, true},
		{NewResourceFromMap(map[string]Quantity{"a": 0, "b": 1}), NewResourceFromMap(map[string]Quantity{"a": 0, "b": 1}), true},
		{NewResourceFromMap(map[string]Quantity{"a": 0, "c": 1}), NewResourceFromMap(map[string]Quantity{"a": 0, "d": 3}), false},
	}

	for _, tt := range tests {
		if got := EqualsOrEmpty(tt.left, tt.right); got != tt.want {
			t.Errorf("got %v, want %v", got, tt.want)
		}
	}
}

func TestFitIn(t *testing.T) {
	// simple case (nil checks)
	empty := NewResource()
	if !FitIn(nil, empty) {
		t.Error("fitin empty in nil resource failed")
	}
	if !FitIn(empty, nil) {
		t.Error("fitin nil in empty resource failed")
	}
	// zero set resources
	smaller := &Resource{Resources: map[string]Quantity{"a": 1}}
	if FitIn(empty, smaller) {
		t.Errorf("fitin resource with value %v should not fit in empty", smaller)
	}

	// simple resources, same type
	larger := NewResourceFromMap(map[string]Quantity{"a": 5})
	if !FitIn(larger, smaller) {
		t.Errorf("fitin smaller resource with value %v should fit in larger %v", smaller, larger)
	}

	// check undefined in larger
	larger = &Resource{Resources: map[string]Quantity{"not-in-smaller": 1}}
	assert.Assert(t, !FitIn(larger, smaller), "different type in smaller %v should not fit in larger %v", smaller, larger)

	// check undefined in smaller
	smaller = &Resource{Resources: map[string]Quantity{"not-in-larger": 1}}
	assert.Assert(t, !FitIn(larger, smaller), "different type in smaller %v should not fit in larger %v", smaller, larger)

	// complex case: just checking the resource merge with negative values, positive is already tested above
	larger = &Resource{Resources: map[string]Quantity{"a": -10}}
	smaller = &Resource{Resources: map[string]Quantity{"a": 0, "b": -10}}
	assert.Assert(t, FitIn(larger, smaller), "fitin smaller resource with zero or neg value %v should fit in larger %v", smaller, larger)

	larger = &Resource{Resources: map[string]Quantity{"a": -5}}
	smaller = &Resource{Resources: map[string]Quantity{"a": 0, "b": 10}}
	assert.Assert(t, !FitIn(larger, smaller), "fitin smaller resource with value %v should not fit in larger %v", smaller, larger)
}

// simple cases (nil checks)
func TestFinInNil(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("FitIn panic on nil resource")
		}
	}()
	var res *Resource
	// ignore nil check from IDE we really want to do this
	assert.Assert(t, res.FitInMaxUndef(nil), "fitin nil should fit in nil (skip undefined)")
	assert.Assert(t, res.FitInMaxUndef(NewResource()), "fitin nil should fit in empty resource failed (skip undefined)")
	smaller := &Resource{Resources: map[string]Quantity{"a": 1}}
	assert.Assert(t, res.FitInMaxUndef(smaller), "defined resource should fit in nil resource (skip undefined)")
}

func TestFitInSkip(t *testing.T) {
	larger := NewResource()
	// zero set resources
	smaller := &Resource{Resources: map[string]Quantity{"a": 1}}
	assert.Assert(t, larger.FitInMaxUndef(smaller), "defined resource %v should fit in empty (skip undefined)", smaller)

	larger = NewResourceFromMap(map[string]Quantity{"a": 5})
	assert.Assert(t, larger.FitInMaxUndef(smaller), "fitin smaller resource with value %v should fit in larger %v (skip undefined)", smaller, larger)

	// check undefined in larger
	larger = &Resource{Resources: map[string]Quantity{"not-in-smaller": 1}}
	assert.Assert(t, larger.FitInMaxUndef(smaller), "different type in smaller %v should fit in larger %v (skip undefined)", smaller, larger)

	// check undefined in smaller
	smaller = &Resource{Resources: map[string]Quantity{"not-in-larger": 1}}
	assert.Assert(t, larger.FitInMaxUndef(smaller), "different type in smaller %v should fit in larger %v (skip undefined)", smaller, larger)

	// complex case: just checking the resource merge, values check is secondary
	larger = &Resource{Resources: map[string]Quantity{"a": -10}}
	smaller = &Resource{Resources: map[string]Quantity{"a": 0, "b": -10}}
	assert.Assert(t, larger.FitInMaxUndef(smaller), "fitin smaller resource with zero or neg values %v should fit in larger %v (skip undefined)", smaller, larger)

	larger = &Resource{Resources: map[string]Quantity{"a": -5}}
	smaller = &Resource{Resources: map[string]Quantity{"a": 0, "b": 10}}
	assert.Assert(t, larger.FitInMaxUndef(smaller), "fitin smaller resource with value %v should fit in larger %v (skip undefined)", smaller, larger)
}

func TestGetShares(t *testing.T) {
	// simple cases nil or empty resources
	shares := getShares(nil, nil)
	if len(shares) > 0 {
		t.Error("nil resources gave shares list longer than 0")
	}
	res := NewResource()
	shares = getShares(res, nil)
	if len(shares) > 0 {
		t.Error("empty resource with total nil gave shares list longer than 0")
	}
	total := NewResource()
	shares = getShares(res, total)
	if len(shares) > 0 {
		t.Error("empty resources gave shares list longer than 0")
	}

	// simple case nil or empty total resource
	res = &Resource{Resources: map[string]Quantity{"zero": 0}}
	shares = getShares(res, nil)
	if len(shares) != 1 && shares[0] != 0 {
		t.Errorf("incorrect share with zero valued resource: %v", shares)
	}
	res = &Resource{Resources: map[string]Quantity{"large": 5, "zero": 0, "small": -5}}
	expected := []float64{-5, 0, 5}
	shares = getShares(res, nil)
	if len(shares) != 3 || !reflect.DeepEqual(shares, expected) {
		t.Errorf("incorrect shares with negative valued resource, expected %v got: %v", expected, shares)
	}
	total = NewResource()
	expected = []float64{-5, 0, 5}
	shares = getShares(res, total)
	if len(shares) != 3 || !reflect.DeepEqual(shares, expected) {
		t.Errorf("incorrect shares with zero valued resource, expected %v got: %v", expected, shares)
	}

	// total resource set same as usage (including signs)
	total = &Resource{Resources: map[string]Quantity{"large": 5, "zero": 0, "small": -5}}
	expected = []float64{0, 1, 1}
	shares = getShares(res, total)
	if len(shares) != 3 || !reflect.DeepEqual(shares, expected) {
		t.Errorf("incorrect shares with same resource, expected %v got: %v", expected, shares)
	}

	// negative share gets set to 0
	res = &Resource{Resources: map[string]Quantity{"large": 5, "zero": 0, "small": -5}}
	total = &Resource{Resources: map[string]Quantity{"large": 10, "zero": 10, "small": 10}}
	expected = []float64{-0.5, 0, 0.5}
	shares = getShares(res, total)
	if len(shares) != 3 || !reflect.DeepEqual(shares, expected) {
		t.Errorf("incorrect shares negative share not set to 0, expected %v got: %v", expected, shares)
	}

	// resource quantity larger than total
	res = &Resource{Resources: map[string]Quantity{"large": 10, "small": 15}}
	total = &Resource{Resources: map[string]Quantity{"large": 15, "small": 10}}
	expected = []float64{10.0 / 15.0, 1.5}
	shares = getShares(res, total)
	if len(shares) != 2 || !reflect.DeepEqual(shares, expected) {
		t.Errorf("incorrect shares larger than total, expected %v got: %v", expected, shares)
	}
	// resource quantity not in total
	res = &Resource{Resources: map[string]Quantity{"large": 5, "notintotal": 10}}
	total = &Resource{Resources: map[string]Quantity{"large": 15}}
	expected = []float64{5.0 / 15.0, 10}
	shares = getShares(res, total)
	if len(shares) != 2 || !reflect.DeepEqual(shares, expected) {
		t.Errorf("incorrect shares not in total, expected %v got: %v", expected, shares)
	}
}

func TestCompareShares(t *testing.T) {
	// simple cases nil or empty shares
	comp := compareShares(nil, nil)
	if comp != 0 {
		t.Error("nil shares not equal")
	}
	left := make([]float64, 0)
	right := make([]float64, 0)
	comp = compareShares(left, right)
	if comp != 0 {
		t.Error("empty shares not equal")
	}
	// simple case same shares
	left = []float64{0, 5}
	comp = compareShares(left, left)
	if comp != 0 {
		t.Error("same shares are not equal")
	}
	// highest same, less shares on one side, zero values
	left = []float64{0, 10.0}
	right = []float64{10.0}
	comp = compareShares(left, right)
	if comp != 0 {
		t.Error("same shares are not equal")
	}
	left, right = right, left
	comp = compareShares(left, right)
	if comp != 0 {
		t.Error("same shares are not equal")
	}

	// highest is same, less shares on one side, positive values
	left = []float64{1, 10}
	right = []float64{10}
	comp = compareShares(left, right)
	if comp != 1 {
		t.Errorf("left should have been larger: left %v, right %v", left, right)
	}
	left, right = right, left
	comp = compareShares(left, right)
	if comp != -1 {
		t.Errorf("right should have been larger: left %v, right %v", left, right)
	}

	// highest is same, less shares on one side, negative values
	left = []float64{-10, 10}
	right = []float64{10}
	comp = compareShares(left, right)
	if comp != -1 {
		t.Errorf("left should have been smaller: left %v, right %v", left, right)
	}
	left, right = right, left
	comp = compareShares(left, right)
	if comp != 1 {
		t.Errorf("right should have been smaller: left %v, right %v", left, right)
	}

	// highest is smaller, less shares on one side, values are not important
	left = []float64{0, 10}
	right = []float64{5}
	comp = compareShares(left, right)
	if comp != 1 {
		t.Errorf("left should have been larger: left %v, right %v", left, right)
	}
	left, right = right, left
	comp = compareShares(left, right)
	if comp != -1 {
		t.Errorf("right should have been larger: left %v, right %v", left, right)
	}

	// highest is +Inf, less shares on one side, zeros before -Inf value
	left = []float64{math.Inf(-1), 0, 0, math.Inf(1)}
	right = []float64{math.Inf(1)}
	comp = compareShares(left, right)
	if comp != -1 {
		t.Errorf("left should have been smaller: left %v, right %v", left, right)
	}
	left, right = right, left
	comp = compareShares(left, right)
	if comp != 1 {
		t.Errorf("right should have been smaller: left %v, right %v", left, right)
	}

	// longer list of values (does not cover any new case)
	left = []float64{-100, -10, 0, 1.1, 2.2, 3.3, 5, math.Inf(1)}
	right = []float64{-99.99, -10, 0, 1.1, 2.2, 3.3, 5, math.Inf(1)}
	comp = compareShares(left, right)
	if comp != -1 {
		t.Errorf("left should have been smaller: left %v, right %v", left, right)
	}
	left, right = right, left
	comp = compareShares(left, right)
	if comp != 1 {
		t.Errorf("right should have been smaller: left %v, right %v", left, right)
	}
}

// This tests just the special code in the FairnessRatio.
// This does not check the share calculation see TestGetShares for that.
func TestFairnessRatio(t *testing.T) {
	// simple case all empty or nil behaviour
	left := NewResource()
	right := NewResource()
	total := NewResource()
	fairRatio := FairnessRatio(left, right, total)
	if fairRatio != 1 {
		t.Errorf("zero resources should return 1, %f", fairRatio)
	}
	// right is zero should give +Inf or -Inf
	total = &Resource{Resources: map[string]Quantity{"first": 10}}
	left = &Resource{Resources: map[string]Quantity{"first": 1}}
	fairRatio = FairnessRatio(left, right, total)
	if !math.IsInf(fairRatio, 1) {
		t.Errorf("positive left, zero right resources should return +Inf got: %f", fairRatio)
	}
	left = &Resource{Resources: map[string]Quantity{"first": -1}}
	fairRatio = FairnessRatio(left, right, total)
	if !math.IsInf(fairRatio, -1) {
		t.Errorf("negative left, zero right resources should return -Inf got: %f", fairRatio)
	}

	// largest possible cluster: all resources used by left gives MaxInt or MinInt
	total = &Resource{Resources: map[string]Quantity{"first": math.MaxInt64}}
	left = &Resource{Resources: map[string]Quantity{"first": math.MaxInt64}}
	right = &Resource{Resources: map[string]Quantity{"first": 1}}
	fairRatio = FairnessRatio(left, right, total)
	if fairRatio != math.MaxInt64 {
		t.Errorf("maximum quantaties on left, 1 on right should get MaxInt got: %f", fairRatio)
	}
	right = &Resource{Resources: map[string]Quantity{"first": -1}}
	fairRatio = FairnessRatio(left, right, total)
	if fairRatio != math.MinInt64 {
		t.Errorf("maximum quantaties on left, -1 on right should get MinInt got: %f", fairRatio)
	}

	// normal cluster size (left > right)
	total = &Resource{Resources: map[string]Quantity{"first": 100}}
	left = &Resource{Resources: map[string]Quantity{"first": 90}}
	right = &Resource{Resources: map[string]Quantity{"first": 1}}
	fairRatio = FairnessRatio(left, right, total)
	if fairRatio != 90 {
		t.Errorf("expected ratio 90 got: %f", fairRatio)
	}
	right = &Resource{Resources: map[string]Quantity{"first": -1}}
	fairRatio = FairnessRatio(left, right, total)
	if fairRatio != -90 {
		t.Errorf("expected ratio -90 got: %f", fairRatio)
	}
	// normal cluster size (right > left)
	total = &Resource{Resources: map[string]Quantity{"first": 100}}
	left = &Resource{Resources: map[string]Quantity{"first": 1}}
	right = &Resource{Resources: map[string]Quantity{"first": 90}}
	expectedRatio := (1.0 / 100.0) / (90.0 / 100.0)
	fairRatio = FairnessRatio(left, right, total)
	if fairRatio != expectedRatio {
		t.Errorf("expected ratio 90 got: %f", fairRatio)
	}
	left = &Resource{Resources: map[string]Quantity{"first": -1}}
	fairRatio = FairnessRatio(left, right, total)
	if fairRatio != -expectedRatio {
		t.Errorf("expected ratio -90 got: %f", fairRatio)
	}
}

// This tests just to cover code in the CompUsageRatio, CompUsageRatioSeparately and CompUsageShare.
// This does not check the share calculation and share comparison see TestGetShares and TestCompShares for that.
func TestCompUsage(t *testing.T) {
	// simple case all empty or nil behaviour
	left := NewResource()
	right := NewResource()
	if CompUsageShares(left, right) != 0 {
		t.Error("empty resources not equal usage")
	}
	total := NewResource()
	if CompUsageRatio(left, right, total) != 0 {
		t.Error("empty resources not equal share ratio")
	}
	// left larger than right
	left = &Resource{Resources: map[string]Quantity{"first": 50, "second": 50, "third": 50}}
	right = &Resource{Resources: map[string]Quantity{"first": 10, "second": 10, "third": 10}}
	if CompUsageShares(left, right) != 1 {
		t.Errorf("left resources should have been larger left %v, right %v", left, right)
	}
	total = &Resource{Resources: map[string]Quantity{"first": 100, "second": 100, "third": 100}}
	if CompUsageRatio(left, right, total) != 1 {
		t.Errorf("left resources ratio should have been larger left %v, right %v", left, right)
	}
	// swap for a smaller than outcome
	left, right = right, left
	if CompUsageShares(left, right) != -1 {
		t.Errorf("right resources should have been larger left %v, right %v", left, right)
	}
	if CompUsageRatio(left, right, total) != -1 {
		t.Errorf("right resources ratio should have been larger left %v, right %v", left, right)
	}

	// test for CompUsageRatioSeparately
	left = &Resource{Resources: map[string]Quantity{"first": 50, "second": 50, "third": 50}}
	right = &Resource{Resources: map[string]Quantity{"first": 10, "second": 10, "third": 10}}
	leftTotal := &Resource{Resources: map[string]Quantity{"first": 100, "second": 100, "third": 100}}
	rightTotal := leftTotal
	if CompUsageRatioSeparately(left, leftTotal, right, rightTotal) != 1 {
		t.Errorf("left resources ratio should have been larger left %v, left-total %v right %v right-total %v",
			left, total, right, rightTotal)
	}
	rightTotal = &Resource{Resources: map[string]Quantity{"first": 10, "second": 10, "third": 10}}
	if CompUsageRatioSeparately(left, leftTotal, right, rightTotal) != -1 {
		t.Errorf("right resources ratio should have been larger left %v, left-total %v right %v right-total %v",
			left, rightTotal, right, total)
	}
}

func TestFitInScoreNil(t *testing.T) {
	// make sure we're nil safe IDE will complain about the non nil check
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil resource in fitinscore test")
		}
	}()
	var empty *Resource
	// nil dereference
	if empty.FitInScore(nil) != 0 {
		t.Error("FitInScore on nil receiver failed")
	}
	fit := NewResourceFromMap(map[string]Quantity{"first": 0})
	if empty.FitInScore(fit) != 1 {
		t.Error("FitInScore on nil receiver failed")
	}
}

func TestFitInScore(t *testing.T) {
	fit := NewResourceFromMap(map[string]Quantity{"first": 0})
	empty := NewResource()
	assert.Equal(t, empty.FitInScore(nil), 0.0, "FitInScore with nil input failed")
	// zero checks
	assert.Equal(t, empty.FitInScore(Zero), 0.0, "FitInScore on zero resource failed")
	assert.Equal(t, empty.FitInScore(fit), 0.0, "FitInScore on resource with zero quantities failed")
	// simple score checks
	fit = NewResourceFromMap(map[string]Quantity{"first": 10})
	assert.Equal(t, empty.FitInScore(fit), 1.0, "FitInScore on resource with one quantity failed")
	fit = NewResourceFromMap(map[string]Quantity{"first": 10, "second": 10})
	assert.Equal(t, empty.FitInScore(fit), 2.0, "FitInScore on resource with two quantities failed")
	fit = NewResourceFromMap(map[string]Quantity{"first": -10})
	assert.Equal(t, empty.FitInScore(fit), 0.0, "FitInScore on resource with negative quantity failed")
	// fit checks, non empty receiver with one quantity
	res := NewResourceFromMap(map[string]Quantity{"first": 10})
	fit = NewResourceFromMap(map[string]Quantity{"first": 10})
	assert.Equal(t, res.FitInScore(fit), 0.0, "FitInScore on exact resource failed")
	fit = NewResourceFromMap(map[string]Quantity{"first": -10})
	assert.Equal(t, res.FitInScore(fit), 0.0, "FitInScore on negative quantity resource failed")
	fit = NewResourceFromMap(map[string]Quantity{"first": 5})
	assert.Equal(t, res.FitInScore(fit), 0.0, "FitInScore on smaller resource failed")
	fit = NewResourceFromMap(map[string]Quantity{"first": 5, "second": 10})
	assert.Equal(t, res.FitInScore(fit), 1.0, "FitInScore on resource with undefined fit quantity failed")
	fit = NewResourceFromMap(map[string]Quantity{"first": 100})
	assert.Equal(t, res.FitInScore(fit), 0.9, "FitInScore on one larger value failed")
	fit = NewResourceFromMap(map[string]Quantity{"first": 20, "second": 100})
	assert.Equal(t, res.FitInScore(fit), 1.5, "FitInScore on resource with defined and undefined quantity failed")
	// fit checks, non empty receiver with multiple quantities
	res = NewResourceFromMap(map[string]Quantity{"first": 10, "second": 10})
	fit = NewResourceFromMap(map[string]Quantity{"first": 100})
	assert.Equal(t, res.FitInScore(fit), 0.9, "FitInScore on larger resource missing quantity failed")
	fit = NewResourceFromMap(map[string]Quantity{"first": 1, "second": 1})
	assert.Equal(t, res.FitInScore(fit), 0.0, "FitInScore on smaller resource multiple quantities failed")
	fit = NewResourceFromMap(map[string]Quantity{"first": 10, "second": 10})
	assert.Equal(t, res.FitInScore(fit), 0.0, "FitInScore on exact resource multiple quantities failed")
	fit = NewResourceFromMap(map[string]Quantity{"first": 100, "second": 100})
	assert.Equal(t, res.FitInScore(fit), 1.8, "FitInScore on larger resource with defined quantities failed")
	// fit checks, non empty receiver with one negative quantity
	res = NewResourceFromMap(map[string]Quantity{"first": -1})
	fit = NewResourceFromMap(map[string]Quantity{"first": 1})
	assert.Equal(t, res.FitInScore(fit), 1.0, "FitInScore on negative receiver quantity failed")
	// fit checks, non empty receiver with negative quantities
	res = NewResourceFromMap(map[string]Quantity{"first": -10, "second": -10})
	fit = NewResourceFromMap(map[string]Quantity{"first": 1, "second": 1})
	assert.Equal(t, res.FitInScore(fit), 2.0, "FitInScore on resource with multiple negative quantities failed")
}

func TestCalculateAbsUsedCapacity(t *testing.T) {
	zeroResource := NewResourceFromMap(map[string]Quantity{"memory": 0, "vcores": 0})
	resourceSet := NewResourceFromMap(map[string]Quantity{"memory": 2048, "vcores": 3})
	usageSet := NewResourceFromMap(map[string]Quantity{"memory": 1024, "vcores": 1})
	partialResource := NewResourceFromMap(map[string]Quantity{"memory": 1024})
	emptyResource := NewResource()

	tests := map[string]struct {
		capacity, used, expected *Resource
	}{
		"nil resource, nil usage": {
			expected: emptyResource,
		},
		"zero resource, nil usage": {
			capacity: zeroResource,
			expected: emptyResource,
		},
		"resource set, nil usage": {
			capacity: resourceSet,
			expected: emptyResource,
		},
		"resource set, zero usage": {
			capacity: resourceSet,
			used:     zeroResource,
			expected: zeroResource,
		},
		"resource set, usage set": {
			capacity: resourceSet,
			used:     usageSet,
			expected: NewResourceFromMap(map[string]Quantity{"memory": 50, "vcores": 33}),
		},
		"partial resource set, usage set": {
			capacity: partialResource,
			used:     usageSet,
			expected: NewResourceFromMap(map[string]Quantity{"memory": 100}),
		},
		"resource set, partial usage set": {
			capacity: resourceSet,
			used:     partialResource,
			expected: NewResourceFromMap(map[string]Quantity{"memory": 50}),
		},
		"positive overflow": {
			capacity: NewResourceFromMap(map[string]Quantity{"memory": 10}),
			used:     NewResourceFromMap(map[string]Quantity{"memory": math.MaxInt64}),
			expected: NewResourceFromMap(map[string]Quantity{"memory": math.MaxInt64}),
		},
		"negative overflow": {
			capacity: NewResourceFromMap(map[string]Quantity{"memory": 10}),
			used:     NewResourceFromMap(map[string]Quantity{"memory": math.MinInt64}),
			expected: NewResourceFromMap(map[string]Quantity{"memory": math.MinInt64}),
		},
		"zero resource, non zero used": {
			capacity: zeroResource,
			used:     usageSet,
			expected: NewResourceFromMap(map[string]Quantity{"memory": math.MaxInt64, "vcores": math.MaxInt64}),
		},
	}
	for _, test := range tests {
		absCapacity := CalculateAbsUsedCapacity(test.capacity, test.used)
		assert.DeepEqual(t, test.expected, absCapacity)
	}
}

func TestNewResourceFromString(t *testing.T) {
	tests := map[string]struct {
		jsonRes  string
		fail     bool
		expected *Resource
	}{
		"empty string": {
			jsonRes:  "",
			fail:     false,
			expected: nil,
		},
		"empty json": {
			jsonRes:  "{\"resources\":{}}",
			fail:     false,
			expected: NewResource(),
		},
		"not a map": {
			jsonRes:  "{\"resources\":{error}}",
			fail:     true,
			expected: nil,
		},
		"illegal json": {
			jsonRes:  "{\"resources\":{\"missing curly bracket\":{\"value\":5}}",
			fail:     true,
			expected: nil,
		},
		"illegal value": {
			jsonRes:  "{\"resources\":{\"invalid\":{\"value\":\"error\"}}}",
			fail:     true,
			expected: nil,
		},
		"simple": {
			jsonRes:  "{\"resources\":{\"valid\":{\"value\":10}}}",
			fail:     false,
			expected: NewResourceFromMap(map[string]Quantity{"valid": 10}),
		},
		"double": {
			jsonRes:  "{\"resources\":{\"valid\":{\"value\":10}, \"other\":{\"value\":5}}}",
			fail:     false,
			expected: NewResourceFromMap(map[string]Quantity{"valid": 10, "other": 5}),
		},
		"same twice": {
			jsonRes:  "{\"resources\":{\"negative\":{\"value\":10}, \"negative\":{\"value\":-10}}}",
			fail:     false,
			expected: NewResourceFromMap(map[string]Quantity{"negative": -10}),
		},
	}
	for name, test := range tests {
		fromJSON, err := NewResourceFromString(test.jsonRes)
		if test.fail {
			if err == nil || fromJSON != nil {
				t.Errorf("expected error and nil resource for test: %s got results", name)
			}
		} else {
			if test.expected == nil && fromJSON != nil {
				t.Errorf("expected nil resource for test: %s, got: %s", name, fromJSON.String())
			} else if !Equals(fromJSON, test.expected) {
				t.Errorf("returned resource did not match expected resource for test: %s, expected %s, got: %v", name, test.expected, fromJSON)
			}
		}
	}
}

func TestDAOMapNil(t *testing.T) {
	// make sure we're nil safe IDE will complain about the non nil check
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil resource in daomap test")
		}
	}()
	var empty *Resource
	assert.DeepEqual(t, empty.DAOMap(), map[string]int64{})
}

func TestDAOMap(t *testing.T) {
	tests := map[string]struct {
		dao map[string]int64
		res *Resource
	}{
		"empty resource": {
			dao: map[string]int64{},
			res: NewResource(),
		},
		"single value": {
			dao: map[string]int64{"first": 1},
			res: NewResourceFromMap(map[string]Quantity{"first": 1}),
		},
		"two values": {
			dao: map[string]int64{"first": 10, "second": -10},
			res: NewResourceFromMap(map[string]Quantity{"first": 10, "second": -10}),
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.DeepEqual(t, test.res.DAOMap(), test.dao)
		})
	}
}

func TestToString(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("String panic on nil resource")
		}
	}()
	var res *Resource
	// ignore nil check from IDE we really want to do this
	resString := res.String()
	assert.Equal(t, resString, "nil resource", "Unexpected string returned for nil resource")
}

func TestString(t *testing.T) {
	testCases := []struct {
		name           string
		input          Quantity
		expectedResult string
	}{
		{"Negative quantity", -1, "-1"},
		{"Zero quantity", 0, "0"},
		{"Positive quantity", 18, "18"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedResult, tc.input.string())
		})
	}
}

func TestHasNegativeValue(t *testing.T) {
	testCases := []struct {
		name           string
		input          *Resource
		expectedResult bool
	}{
		{"Nil resource", nil, false},
		{"Empty resource", NewResource(), false},
		{"Only positive values", NewResourceFromMap(map[string]Quantity{common.Memory: 100}), false},
		{"Negative value", NewResourceFromMap(map[string]Quantity{common.Memory: -100}), true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedResult, tc.input.HasNegativeValue())
		})
	}
}
