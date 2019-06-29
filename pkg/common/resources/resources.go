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
    "github.com/cloudera/scheduler-interface/lib/go/si"
    "math"
    "sort"
    "strconv"
)

// const keys
const (
    MEMORY = "memory"
    VCORE  = "vcore"
)

type Resource struct {
    Resources map[string]Quantity
}

// No unit defined here for better performance
type Quantity int64

// Never update value of Zero
var Zero = NewResource()

func NewResource() *Resource {
    return &Resource{Resources: make(map[string]Quantity)}
}

func NewResourceFromProto(proto *si.Resource) *Resource {
    out := NewResource()
    for k, v := range proto.Resources {
        out.Resources[k] = Quantity(v.Value)
    }
    return out
}

func NewResourceFromMap(m map[string]Quantity) *Resource {
    return &Resource{Resources: m}
}

// Create a new resource from the config map.
// The config map must have been checked before being applied. The check here is just for safety so we do not crash.
// TODO support size modifiers
func NewResourceFromConf(configMap map[string]string) (*Resource, error) {
    res := NewResource()
    for key, strVal := range configMap {
        intValue, err := strconv.ParseInt(strVal, 10, 64)
        if err != nil {
            return nil, err
        }
        res.Resources[key] = Quantity(intValue)
    }
    return res, nil
}

func (m *Resource) String() string {
    return fmt.Sprintf("%v", m.Resources)
}

// Convert to a protobuf implementation
func (m *Resource) ToProto() *si.Resource {
    proto := &si.Resource{}
    proto.Resources = make(map[string]*si.Quantity)
    for k, v := range m.Resources {
        proto.Resources[k] = &si.Quantity{Value: int64(v)}
    }
    return proto
}

// Return a clone (copy) of the resource
func (m *Resource) Clone() *Resource {
    ret := NewResource()
    for k, v := range m.Resources {
        if v != 0 {
            ret.Resources[k] = v
        }
    }
    return ret
}

// Operations
// All operations must be nil safe

// Add resources returning a new resource with the result
// A nil resource is considered an empty resource
func Add(left *Resource, right *Resource) *Resource {
    if left == nil {
        left = Zero
    }
    if right == nil {
        right = Zero
    }

    out := NewResource()
    for k, v := range right.Resources {
        out.Resources[k] = v
    }
    for k, v := range left.Resources {
        out.Resources[k] += v
    }
    return out
}

// Subtract resource returning a new resource with the result
// A nil resource is considered an empty resource
// This might return negative values for specific quantities
func Sub(left *Resource, right *Resource) *Resource {
    if left == nil {
        left = Zero
    }
    if right == nil {
        right = Zero
    }

    out := NewResource()
    for k, v := range left.Resources {
        out.Resources[k] = v
    }
    for k, v := range right.Resources {
        out.Resources[k] -= v
    }
    return out
}

// Subtract resource returning a new resource with the result
// A nil resource is considered an empty resource
// This will return 0 values for negative values
func SubEliminateNegative(left *Resource, right *Resource) *Resource {
    if left == nil {
        left = Zero
    }
    if right == nil {
        right = Zero
    }

    out := NewResource()
    for k, v := range left.Resources {
        out.Resources[k] = v
    }
    for k, v := range right.Resources {
        out.Resources[k] -= v
    }

    for k := range out.Resources {
        if out.Resources[k] < 0 {
            out.Resources[k] = 0
        }
    }
    return out
}

// Add additional resource to the base updating the base resource
// Should be used by temporary computation only
// A nil base resource is considered an empty resource
// A nil addition is treated as a zero valued resource and leaves base unchanged
func AddTo(base *Resource, additional *Resource) {
    if additional == nil {
        return
    }
    if base == nil {
        base = NewResource()
    }
    for k, v := range additional.Resources {
        base.Resources[k] += v
    }
}

// Subtract from the base resource the subtract resource by updating the base resource
// Should be used by temporary computation only
// A nil base resource is considered an empty resource
// A nil subtract is treated as a zero valued resource and leaves base unchanged
func SubFrom(base *Resource, subtract *Resource) {
    if subtract == nil {
        return
    }
    if base == nil {
        base = Zero
    }
    for k, v := range subtract.Resources {
        base.Resources[k] -= v
    }
}

// Check if smaller fitin larger, negative values will be treated as 0
// A nil resource is treated as an empty resource (zero)
func FitIn(larger *Resource, smaller *Resource) bool {
    if larger == nil {
        larger = Zero
    }
    if smaller == nil {
        smaller = Zero
    }

    for k, v := range smaller.Resources {
        largerValue := larger.Resources[k]
        if largerValue < 0 {
            largerValue = 0
        }
        if v > largerValue {
            return false
        }
    }
    return true
}

// Get the share of res when compared to partition
func getShares(partition *Resource, res *Resource) []float64 {
    shares := make([]float64, len(res.Resources))
    idx := 0
    for k, v := range res.Resources {
        if v == 0 {
            continue
        }

        // Get rid of 0 denominator (mostly)
        pv := float64(1)
        if partition != nil {
            pv = float64(partition.Resources[k]) + 1e-4
        }
        if pv > 1e-8 {
            shares[idx] = float64(v) / pv
            idx++
        } else {
            shares[idx] = math.Inf(1)
            idx++
        }
    }

    sort.Float64s(shares)

    return shares
}

// Compare a1 / b1 with a2 / b2
func CompFairnessRatio(a1 *Resource, b1 *Resource, a2 *Resource, b2 *Resource) int {
    lshares := getShares(b1, a1)
    rshares := getShares(b2, a2)

    return compareShares(lshares, rshares)
}

// Compare two resources and assumes partition resource == (1, 1 ...)
func CompFairnessRatioAssumesUnitPartition(a1 *Resource, a2 *Resource) int {
    lshares := getShares(nil, a1)
    rshares := getShares(nil, a2)

    return compareShares(lshares, rshares)
}

// Get fairness ratio of a1/b1 / a2/b2
func FairnessRatio(a1 *Resource, b1 *Resource, a2 *Resource, b2 *Resource) float64 {
    lshares := getShares(b1, a1)
    rshares := getShares(b2, a2)

    lshare := float64(0)
    if shareLen := len(lshares); shareLen != 0 {
        lshare = lshares[shareLen - 1]
    }
    rshare := float64(0)
    if shareLen := len(rshares); shareLen != 0 {
        rshare = rshares[shareLen - 1]
    }

    if math.Abs(rshare) < 1e-8 {
        if math.Abs(lshare) < 1e-8 {
            // 0 == 0
            return 1
        } else {
            return math.Inf(1)
        }
    }

    return lshare / rshare
}

func compareShares(lshares, rshares []float64) int {

    lIdx := len(lshares) - 1
    rIdx := len(rshares) - 1

    for rIdx >= 0 && lIdx >= 0 {
        lValue := lshares[lIdx]
        rValue := rshares[rIdx]
        if lValue > rValue {
            return 1
        } else if lValue < rValue {
            return -1
        } else {
            lIdx --
            rIdx --
        }
    }

    if lIdx == 0 && rIdx == 0 {
        return 0
    } else if lIdx >= 0 {
        for lIdx >= 0 {
            if lshares[lIdx] > 0 {
                return 1
            } else if lshares[lIdx] < 0 {
                return -1
            } else {
                lIdx --
            }
        }
    } else {
        for rIdx >= 0 {
            if rshares[rIdx] > 0 {
                return -1
            } else if rshares[rIdx] < 0 {
                return 1
            } else {
                rIdx --
            }
        }
    }

    return 0
}

// Compare the share for the left and right resources to the partition
// This compares left / partition with right / partition
func Comp(partition *Resource, left *Resource, right *Resource) int {
    return CompFairnessRatio(left, partition, right, partition)
}

// Compare the resources equal returns the specific values for following cases:
// left  right  return
// nil   nil    true
// nil   <set>  false
// <set> nil    false
// <set> <set>  true/false  *based on the individual Quantity values
func Equals(left *Resource, right *Resource) bool {
    if left == right {
        return true
    }

    if left == nil || right == nil {
        return false
    }

    for k, v := range left.Resources {
        if right.Resources[k] != v {
            return false
        }
    }

    for k, v := range right.Resources {
        if left.Resources[k] != v {
            return false
        }
    }

    return true
}

// Multiply the resource by the ratio updating the passed in resource
// Should be used by temporary computation only
// A nil resource is returned as is
func MultiplyTo(left *Resource, ratio float64) {
    if left != nil {
        for k, v := range left.Resources {
            left.Resources[k] = Quantity(float64(v) * ratio)
        }
    }
}

// Multiply the resource by the ratio returning a new resource
// A nil resource passed in returns a new empty resource (zero)
func MultiplyBy(left *Resource, ratio float64) *Resource {
    ret := NewResource()
    if left != nil {
        for k, v := range left.Resources {
            ret.Resources[k] = Quantity(float64(v) * ratio)
        }
    }
    return ret
}

// Does any vector of larger > smaller and all vectors of larger >= smaller
func StrictlyGreaterThan(larger *Resource, smaller *Resource) bool {
    if larger == nil {
        larger = Zero
    }
    if smaller == nil {
        smaller = Zero
    }

    delta := Quantity(0)

    for k, v := range larger.Resources {
        if smaller.Resources[k] > v {
            return false
        }
        delta += v - smaller.Resources[k]
    }

    for k, v := range smaller.Resources {
        if larger.Resources[k] < v {
            return false
        }
        delta += larger.Resources[k] - v
    }

    return delta > 0
}

// Does all vector of larger >= smaller
func StrictlyGreaterThanOrEquals(larger *Resource, smaller *Resource) bool {
    if larger == nil {
        larger = Zero
    }
    if smaller == nil {
        smaller = Zero
    }

    for k, v := range larger.Resources {
        if smaller.Resources[k] > v {
            return false
        }
    }

    for k, v := range smaller.Resources {
        if larger.Resources[k] < v {
            return false
        }
    }

    return true
}

// Have at least one type > 0, and no type < 0
// A nil resource is not strictly greater than zero.
func StrictlyGreaterThanZero(larger *Resource) bool {
    var greater = false
    if larger != nil {
        for _, v := range larger.Resources {
            if v < 0 {
                greater = false
                break
            } else if v > 0 {
                greater = true
            }
        }
    }
    return greater
}

func minQuantity(x, y Quantity) Quantity {
    if x < y {
        return x
    }
    return y
}

func maxQuantity(x, y Quantity) Quantity {
    if x > y {
        return x
    }
    return y
}


// Returns a new resource with the smallest value for each entry in the resources
// If either resource passed in is nil a zero resource is returned
func ComponentWiseMin(left *Resource, right *Resource) *Resource {
    out := NewResource()
    if left != nil && right != nil {
        for k, v := range left.Resources {
            out.Resources[k] = minQuantity(v, right.Resources[k])
        }
        for k, v := range right.Resources {
            out.Resources[k] = minQuantity(v, left.Resources[k])
        }
    }
    return out
}

// Returns a new resource with the smallest value for each entry in the resources
// If either resource passed in is nil a zero resource is returned
func ComponentWiseMax(left *Resource, right *Resource) *Resource {
    out := NewResource()
    if left != nil && right != nil {
        for k, v := range left.Resources {
            out.Resources[k] = maxQuantity(v, right.Resources[k])
        }
        for k, v := range right.Resources {
            out.Resources[k] = maxQuantity(v, left.Resources[k])
        }
    }
    return out
}


// Check that the whole resource is zero
// A nil resource is zero (contrary to StrictlyGreaterThanZero)
func IsZero(zero *Resource) bool {
    if zero != nil {
        for _, v := range zero.Resources {
            if v != 0 {
                return false
            }
        }
    }
    return true
}

func MinQuantity(left Quantity, right Quantity) Quantity{
    if left < right {
        return left
    }
    return right
}

func MaxQuantity(left Quantity, right Quantity) Quantity{
    if left < right {
        return right
    }
    return left
}
