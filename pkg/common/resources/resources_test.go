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
    "math"
    "reflect"
    "testing"
)

func TestAddSub(t *testing.T) {
    res1 := &Resource{Resources: map[string]Quantity{"a": 1, "b": 3, "e": 4}}
    res2 := &Resource{Resources: map[string]Quantity{"a": 1, "b": 2, "c": 5}}
    res3 := Add(res1, res2)

    expected := map[string]Quantity{"a": 2, "b": 5, "c": 5, "e": 4}
    if !reflect.DeepEqual(res3.Resources, expected) {
        t.Errorf("Expected %v, Actual %v", expected, res3.Resources)
    }

    res3 = Sub(res1, res2)
    expected = map[string]Quantity{"a": 0, "b": 1, "c": -5, "e": 4}
    if !reflect.DeepEqual(res3.Resources, expected) {
        t.Errorf("Expected %v, Actual %v", expected, res3.Resources)
    }
}

func assertComparison(t *testing.T, cluster *Resource, left *Resource, right *Resource, expected int) {
    c := Comp(cluster, left, right)
    if c != expected {
        t.Errorf("Compare %s to %s, expected %d, got %d", left, right, expected, c)
    }
}

func assertFitIn(t *testing.T, larger *Resource, smaller *Resource, expected bool) {
    c := FitIn(larger, smaller)
    if c != expected {
        t.Errorf("Fit %s into %s (larger), expected=%v", smaller, larger, expected)
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


func TestFitIn(t *testing.T) {
    // Easy cases
    assertFitIn(t, MockResource(1, 1, 1), MockResource(1, 1, 1), true)
    assertFitIn(t, MockResource(0, 0, 0), MockResource(0, 0, 0), true)
    assertFitIn(t, MockResource(2, 2, 2), MockResource(1, 1, 1), true)
    assertFitIn(t, MockResource(2, 2, 2), MockResource(0, 0, 0), true)
    assertFitIn(t, MockResource(0, 2, 2), MockResource(2, 0, 0), false)
    assertFitIn(t, MockResource(0, 0, 2), MockResource(2, 2, 2), false)

    // check nil for either smaller or larger (should be seen as an empty resource
    assertFitIn(t, MockResource(1, 1, 1), nil, true)
    assertFitIn(t, nil, MockResource(1, 1, 1), false)
}
