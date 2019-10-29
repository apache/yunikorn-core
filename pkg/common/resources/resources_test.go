/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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
    "math"

    //    "math"
    "reflect"
    "testing"
)

func TestNewResourceFromConf(t *testing.T) {
    // resource wit nil input
    original, err := NewResourceFromConf(nil)
    if err != nil || len(original.Resources) != 0 {
        t.Fatalf("new resource create from nil returned error or wrong resource: error %t, res %v", err, original)
    }
    // resource without any resource mappings
    original, err = NewResourceFromConf(make(map[string]string))
    if err != nil || len(original.Resources) != 0 {
        t.Fatalf("new resource create from empty conf returned error or wrong resource: error %t, res %v", err, original)
    }
    original, err = NewResourceFromConf(map[string]string{"zero": "0"})
    if err != nil || len(original.Resources) != 1 {
        t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, original)
    }

    // failure case: parse error
    original, err = NewResourceFromConf(map[string]string{"fail": "xx"})
    if err == nil || original != nil{
        t.Fatalf("new resource create should have returned error %v, res %v", err, original)
    }
    err = fmt.Errorf("", )
}

func TestClone(t *testing.T) {
    // resource without any resource mappings
    original := NewResourceFromMap(map[string]Quantity{})
    cloned := original.Clone()
    // pointer cannot be equal
    if original == cloned {
        t.Fatalf("cloned resources pointers are equal, should not be")
    }
    // content must be equal
    if !reflect.DeepEqual(original.Resources, cloned.Resources) {
        t.Errorf("cloned resources are not equal: %v / %v", original, cloned)
    }

    original = NewResourceFromMap(map[string]Quantity{"first": 1, "second": -2, "third": 3})
    cloned = original.Clone()
    // pointer cannot be equal
    if original == cloned {
        t.Fatalf("cloned resources pointers are equal, should not be")
    }
    // content must be equal
    if !reflect.DeepEqual(original.Resources, cloned.Resources) {
        t.Errorf("cloned resources are not equal: %v / %v", original, cloned)
    }

    // set a resource value to 0
    original = NewResourceFromMap(map[string]Quantity{"first": 1, "zero": 0, "third": 3})
    cloned = original.Clone()
    // pointer cannot be equal
    if original == cloned {
        t.Fatalf("cloned resources pointers are equal, should not be")
    }
    // content must be equal
    if !reflect.DeepEqual(original.Resources, cloned.Resources) {
        t.Errorf("cloned resources are not equal: %v / %v", original, cloned)
    }
}

func TestEquals(t *testing.T) {
    // simple cases (nil checks)
    base := NewResourceFromMap(map[string]Quantity{})
    if !Equals(nil, nil) {
        t.Errorf("compared resources should have been equal (both nil)")
    }
    if Equals(nil, base) {
        t.Errorf("compared resources should not have been equal (left nil)")
    }
    if Equals(base, nil) {
        t.Errorf("compared resources should not have been equal (right nil)")
    }

    // resource without any resource mappings should be equal to one that has
    // resources set even if they are 0
    compare := NewResourceFromMap(map[string]Quantity{"zero": 0})
    if !Equals(base, compare) {
        t.Errorf("compared resources are not equal (right has resource), should be: %v / %v", base, compare)
    }
    if !Equals(compare, base) {
        t.Errorf("compared resources are not equal (left has resource), should be: %v / %v", compare, base)
    }

    // set resources values
    base = NewResourceFromMap(map[string]Quantity{"first": 10})
    compare = NewResourceFromMap(map[string]Quantity{"first": 0})
    if Equals(base, compare) {
        t.Errorf("compared resources are equal (value diff), should not be: %v / %v", base, compare)
    }
    if Equals(compare, base) {
        t.Errorf("compared resources are equal (value diff), should not be: %v / %v", base, compare)
    }
    base = NewResourceFromMap(map[string]Quantity{"first": 10})
    compare = NewResourceFromMap(map[string]Quantity{"second": 10})
    if Equals(base, compare) {
        t.Errorf("compared resources are equal (name diff), should not be: %v / %v", base, compare)
    }
    base = NewResourceFromMap(map[string]Quantity{"first": 10})
    compare = NewResourceFromMap(map[string]Quantity{"first": 10})
    if !Equals(base, compare) {
        t.Errorf("compared resources are not equal, should be: %v / %v", base, compare)
    }
    base = NewResourceFromMap(map[string]Quantity{"first": 1})
    compare = NewResourceFromMap(map[string]Quantity{"first": 1, "second": 1})
    if Equals(base, compare) {
        t.Errorf("compared resources are equal, should not be: %v / %v", base, compare)
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

func TestStrictlyGreaterThanZero (t *testing.T) {
    // simple case (nil checks)
    if StrictlyGreaterThanZero(nil) {
        t.Errorf("nil resource should not be greater than zero")
    }
    base := NewResourceFromMap(map[string]Quantity{})
    if StrictlyGreaterThanZero(base) {
        t.Errorf("no resource entries should not be greater than zero")
    }

    // set resource values
    base = NewResourceFromMap(map[string]Quantity{"zero": 0})
    if StrictlyGreaterThanZero(base) {
        t.Errorf("only zero resources should not be greater than zero")
    }
    base = NewResourceFromMap(map[string]Quantity{"first": 10})
    if !StrictlyGreaterThanZero(base) {
        t.Errorf("set resource should be greater than zero")
    }
    base = NewResourceFromMap(map[string]Quantity{"first": -1})
    if StrictlyGreaterThanZero(base) {
        t.Errorf("set negative resource should not be greater than zero")
    }
}

func TestStrictlyGreaterThan (t *testing.T) {
    // simple case (nil checks)
    if StrictlyGreaterThan(nil, nil) {
        t.Errorf("nil resource should not be greater than other nil")
    }
    // zero or empty resource values
    larger := NewResourceFromMap(map[string]Quantity{})
    smaller := NewResourceFromMap(map[string]Quantity{})
    if StrictlyGreaterThan(larger, smaller) {
        t.Errorf("empty resource entries should not be greater than empty")
    }
    larger = NewResourceFromMap(map[string]Quantity{"zero": 0})
    if StrictlyGreaterThan(larger, smaller) {
        t.Errorf("only zero resources should not be greater than empty")
    }
    smaller = NewResourceFromMap(map[string]Quantity{"other": 0})
    if StrictlyGreaterThan(larger, smaller) {
        t.Errorf("only zero resource (both objects) should not be greater than zero")
    }

    // negative resource values
    larger = NewResourceFromMap(map[string]Quantity{})
    smaller = NewResourceFromMap(map[string]Quantity{"first": -1})
    if !StrictlyGreaterThan(larger, smaller) {
        t.Errorf("negative resource should not be greater than zero")
    }
    if StrictlyGreaterThan(smaller, larger) {
        t.Errorf("negative resource should be smaller than zero")
    }

    // set resource values
    larger = NewResourceFromMap(map[string]Quantity{"first": 10})
    if StrictlyGreaterThan(larger, larger) {
        t.Errorf("same resource should not be greater than")
    }
    smaller = NewResourceFromMap(map[string]Quantity{"first": 1})
    if !StrictlyGreaterThan(larger, smaller) {
        t.Errorf("larger %v returned as smaller compared to %v", larger, smaller)
    }
    smaller = NewResourceFromMap(map[string]Quantity{"second": 1})
    if StrictlyGreaterThan(larger, smaller) {
        t.Errorf("larger %v returned as smaller compared to %v", larger, smaller)
    }
    larger = NewResourceFromMap(map[string]Quantity{"first": 10, "second": 10})
    smaller = NewResourceFromMap(map[string]Quantity{"first": 1})
    if !StrictlyGreaterThan(larger, smaller) {
        t.Errorf("larger %v returned as smaller compared to %v", larger, smaller)
    }
    // negative resource is smaller than not set
    larger = NewResourceFromMap(map[string]Quantity{"first": 10, "second": -1})
    smaller = NewResourceFromMap(map[string]Quantity{"first": 10})
    if StrictlyGreaterThan(larger, smaller) {
        t.Errorf("negative %v returned as larger compared to not set %v", larger, smaller)
    }
    if !StrictlyGreaterThan(smaller, larger) {
        t.Errorf("not set %v returned as larger compared to negative %v", smaller, larger)
    }
    larger = NewResourceFromMap(map[string]Quantity{"first": -1})
    smaller = NewResourceFromMap(map[string]Quantity{})
    if StrictlyGreaterThan(larger, smaller) {
        t.Errorf("negative %v returned as larger compared to not set %v", larger, smaller)
    }
    if !StrictlyGreaterThan(smaller, larger) {
        t.Errorf("not set %v returned as larger compared to negative %v", smaller, larger)
    }
}

func TestStrictlyGreaterThanOrEquals (t *testing.T) {
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

func TestComponentWiseMin (t *testing.T) {
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

func TestComponentWiseMax (t *testing.T) {
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
    if len(base.Resources) != 1  && base.Resources["a"] != 5 {
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
        t.Errorf("Add failed expected %v, actual %v", expected, res3.Resources)
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

    // simple empty resources
    larger := NewResourceFromMap(map[string]Quantity{"a": 5})
    if !FitIn(larger, smaller) {
        t.Errorf("fitin smaller resource with value %v should fit in larger %v", smaller, larger)
    }

    // complex case: just checking the resource merge, values check is secondary
    larger = &Resource{Resources: map[string]Quantity{"a": 10, "c": -10}}
    smaller = &Resource{Resources: map[string]Quantity{"a": 5, "c": 0, "d": -10}}
    if !FitIn(larger, smaller) {
        t.Errorf("fitin smaller resource with value %v should fit in larger %v", smaller, larger)
    }

    larger = &Resource{Resources: map[string]Quantity{"a": 10, "c": -5}}
    smaller = &Resource{Resources: map[string]Quantity{"a": 5, "c": 0, "d": 10}}
    if FitIn(larger, smaller) {
        t.Errorf("fitin smaller resource with value %v should not fit in larger %v", smaller, larger)
    }
}

func TestComp(t *testing.T) {
    cluster := MockResource(4, 4, 4)

    // Easy cases
    assertComparison(t, cluster, MockResource(1, 1, 1), MockResource(1, 1, 1), 0)
    assertComparison(t, cluster, MockResource(0, 0, 0), MockResource(0, 0, 0), 0)
    assertComparison(t, cluster, MockResource(2, 2, 2), MockResource(1, 1, 1), 1)
    assertComparison(t, cluster, MockResource(2, 2, 2), MockResource(0, 0, 0), 1)

    // Fair sharing cases
    assertComparison(t, cluster, MockResource(2, 1, 1), MockResource(2, 1, 1), 0)
    assertComparison(t, cluster, MockResource(2, 1, 1), MockResource(1, 2, 1), 0)
    assertComparison(t, cluster, MockResource(2, 1, 1), MockResource(1, 1, 2), 0)
    assertComparison(t, cluster, MockResource(2, 1, 0), MockResource(0, 1, 2), 0)
    assertComparison(t, cluster, MockResource(2, 2, 1), MockResource(1, 2, 2), 0)
    assertComparison(t, cluster, MockResource(2, 2, 1), MockResource(2, 1, 2), 0)
    assertComparison(t, cluster, MockResource(2, 2, 1), MockResource(2, 2, 1), 0)
    assertComparison(t, cluster, MockResource(2, 2, 0), MockResource(2, 0, 2), 0)
    assertComparison(t, cluster, MockResource(3, 2, 1), MockResource(3, 2, 1), 0)
    assertComparison(t, cluster, MockResource(3, 2, 1), MockResource(3, 1, 2), 0)
    assertComparison(t, cluster, MockResource(3, 2, 1), MockResource(1, 2, 3), 0)
    assertComparison(t, cluster, MockResource(3, 2, 1), MockResource(1, 3, 2), 0)
    assertComparison(t, cluster, MockResource(3, 2, 1), MockResource(2, 1, 3), 0)
    assertComparison(t, cluster, MockResource(3, 2, 1), MockResource(2, 3, 1), 0)
    assertComparison(t, cluster, MockResource(2, 1, 1), MockResource(1, 1, 1), 1)
    assertComparison(t, cluster, MockResource(2, 1, 1), MockResource(1, 1, 0), 1)
    assertComparison(t, cluster, MockResource(2, 2, 1), MockResource(2, 1, 1), 1)
    assertComparison(t, cluster, MockResource(2, 2, 1), MockResource(1, 2, 1), 1)
    assertComparison(t, cluster, MockResource(2, 2, 1), MockResource(1, 1, 2), 1)
    assertComparison(t, cluster, MockResource(2, 2, 1), MockResource(0, 2, 2), 1)
    assertComparison(t, cluster, MockResource(2, 2, 2), MockResource(2, 1, 1), 1)
    assertComparison(t, cluster, MockResource(2, 2, 2), MockResource(1, 2, 1), 1)
    assertComparison(t, cluster, MockResource(2, 2, 2), MockResource(1, 1, 2), 1)
    assertComparison(t, cluster, MockResource(2, 2, 2), MockResource(2, 2, 1), 1)
    assertComparison(t, cluster, MockResource(2, 2, 2), MockResource(2, 1, 2), 1)
    assertComparison(t, cluster, MockResource(2, 2, 2), MockResource(1, 2, 2), 1)
    assertComparison(t, cluster, MockResource(3, 2, 1), MockResource(2, 2, 2), 1)
    assertComparison(t, cluster, MockResource(3, 1, 1), MockResource(2, 2, 2), 1)
    assertComparison(t, cluster, MockResource(3, 1, 1), MockResource(3, 1, 0), 1)
    assertComparison(t, cluster, MockResource(3, 1, 1), MockResource(3, 0, 0), 1)
}

func assertFairness(t *testing.T, cluster *Resource, left *Resource, right *Resource, expected float64) {
    c := FairnessRatio(left, cluster, right, cluster)
    if c != expected {
        t.Errorf("Fairness Ratio %s to %s, expected %f, got %f", left, right, expected, c)
    }
}


func TestFairnessRatio(t *testing.T) {
    cluster := MockResource(4, 4, 4)

    // Easy cases
    assertFairness(t, cluster, MockResource(1, 1, 1), MockResource(1, 1, 1), 1)
    assertFairness(t, cluster, MockResource(0, 0, 0), MockResource(0, 0, 0), 1)
    assertFairness(t, cluster, MockResource(2, 2, 2), MockResource(1, 1, 1), 2)
    assertFairness(t, cluster, MockResource(2, 2, 2), MockResource(0, 0, 0), math.Inf(1))

    // Fair sharing cases
    assertFairness(t, cluster, MockResource(2, 1, 1), MockResource(2, 1, 1), 1)
    assertFairness(t, cluster, MockResource(2, 1, 1), MockResource(1, 2, 1), 1)
    assertFairness(t, cluster, MockResource(2, 1, 1), MockResource(1, 1, 2), 1)
    assertFairness(t, cluster, MockResource(2, 1, 0), MockResource(0, 1, 2), 1)
    assertFairness(t, cluster, MockResource(2, 2, 1), MockResource(1, 2, 2), 1)
    assertFairness(t, cluster, MockResource(2, 2, 1), MockResource(2, 1, 2), 1)
    assertFairness(t, cluster, MockResource(2, 2, 1), MockResource(2, 2, 1), 1)
    assertFairness(t, cluster, MockResource(2, 2, 0), MockResource(2, 0, 2), 1)
    assertFairness(t, cluster, MockResource(3, 2, 1), MockResource(3, 2, 1), 1)
    assertFairness(t, cluster, MockResource(3, 2, 1), MockResource(3, 1, 2), 1)
    assertFairness(t, cluster, MockResource(3, 2, 1), MockResource(1, 2, 3), 1)
    assertFairness(t, cluster, MockResource(3, 2, 1), MockResource(1, 3, 2), 1)
    assertFairness(t, cluster, MockResource(3, 2, 1), MockResource(2, 1, 3), 1)
    assertFairness(t, cluster, MockResource(3, 2, 1), MockResource(2, 3, 1), 1)
    assertFairness(t, cluster, MockResource(2, 1, 1), MockResource(1, 1, 1), 2)
    assertFairness(t, cluster, MockResource(2, 1, 1), MockResource(1, 1, 0), 2)
    assertFairness(t, cluster, MockResource(2, 2, 1), MockResource(2, 1, 1), 1)
    assertFairness(t, cluster, MockResource(2, 2, 1), MockResource(1, 2, 1), 1)
    assertFairness(t, cluster, MockResource(2, 2, 1), MockResource(1, 1, 2), 1)
    assertFairness(t, cluster, MockResource(2, 2, 1), MockResource(0, 2, 2), 1)
    assertFairness(t, cluster, MockResource(2, 2, 2), MockResource(2, 1, 1), 1)
    assertFairness(t, cluster, MockResource(2, 2, 2), MockResource(1, 2, 1), 1)
    assertFairness(t, cluster, MockResource(2, 2, 2), MockResource(1, 1, 2), 1)
    assertFairness(t, cluster, MockResource(2, 2, 2), MockResource(2, 2, 1), 1)
    assertFairness(t, cluster, MockResource(2, 2, 2), MockResource(2, 1, 2), 1)
    assertFairness(t, cluster, MockResource(2, 2, 2), MockResource(1, 2, 2), 1)
    assertFairness(t, cluster, MockResource(3, 2, 1), MockResource(2, 2, 2), 1.5)
    assertFairness(t, cluster, MockResource(3, 1, 1), MockResource(2, 2, 2), 1.5)
    assertFairness(t, cluster, MockResource(3, 1, 1), MockResource(3, 1, 0), 1)
    assertFairness(t, cluster, MockResource(3, 1, 1), MockResource(3, 0, 0), 1)
}

// utility functions
func assertComparison(t *testing.T, cluster *Resource, left *Resource, right *Resource, expected int) {
    c := Comp(cluster, left, right)
    if c != expected {
        t.Errorf("Compare %s to %s, expected %d, got %d", left, right, expected, c)
    }
}
