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
    "github.com/cloudera/yunikorn-core/pkg/log"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "go.uber.org/zap"
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
    if proto == nil {
        return out
    }
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

func (r *Resource) String() string {
    return fmt.Sprintf("%v", r.Resources)
}

// Convert to a protobuf implementation
func (r *Resource) ToProto() *si.Resource {
    proto := &si.Resource{}
    proto.Resources = make(map[string]*si.Quantity)
    for k, v := range r.Resources {
        proto.Resources[k] = &si.Quantity{Value: int64(v)}
    }
    return proto
}

// Return a clone (copy) of the resource it is called on.
// This provides a deep copy of the object with the exact same member set.
// NOTE: this is a clone not a sparse copy of the original.
func (r *Resource) Clone() *Resource {
    ret := NewResource()
    for k, v := range r.Resources {
        ret.Resources[k] = v
    }
    return ret
}

// Add additional resource to the base updating the base resource
// Should be used by temporary computation only
// A nil base resource is considered an empty resource
// A nil addition is treated as a zero valued resource and leaves base unchanged
func (r *Resource) AddTo(add *Resource) {
    if add == nil {
        return
    }
    for k, v := range add.Resources {
        r.Resources[k] = addVal(r.Resources[k], v)
    }
}

// Subtract from the resource the passed in resource by updating the resource it is called on.
// A nil passed in resource is treated as a zero valued resource and leaves the called on resource unchanged.
// Should be used by temporary computation only
func (r *Resource) SubFrom(sub *Resource) {
    if sub == nil {
        return
    }
    for k, v := range sub.Resources {
        r.Resources[k] = subVal(r.Resources[k], v)
    }
}

// Multiply the resource by the ratio updating the resource it is called on.
// Should be used by temporary computation only.
func (r *Resource) MultiplyTo(ratio float64) {
    if r != nil {
        for k, v := range r.Resources {
            r.Resources[k] = mulValRatio(v, ratio)
        }
    }
}

// Wrapping safe calculators for the quantities of resources.
// They will always return a valid int64. Logging if the calculator wrapped the value.
// Returning the appropriate MaxInt64 or MinInt64 value.
func addVal(valA, valB Quantity) Quantity {
    result := valA + valB
    // check if the sign wrapped
    if (result < valA) != (valB < 0) {
        if valA < 0 {
            // return the minimum possible
            log.Logger().Warn("Resource calculation wrapped: returned minimum value possible",
                zap.Int64("valueA", int64(valA)),
                zap.Int64("valueB", int64(valB)))
            return math.MinInt64
        } else {
            // return the maximum possible
            log.Logger().Warn("Resource calculation wrapped: returned maximum value possible",
                zap.Int64("valueA", int64(valA)),
                zap.Int64("valueB", int64(valB)))
            return math.MaxInt64
        }
    }
    // not wrapped normal case
    return result
}

func subVal(valA, valB Quantity) Quantity {
    return addVal(valA, -valB)
}

func mulVal(valA, valB Quantity) Quantity {
    // optimise the zero cases (often hit with zero resource)
    if valA == 0 || valB == 0 {
        return 0
    }
    result := valA * valB
    // check the wrapping
    // MinInt64 * -1 is special: it returns MinInt64, it should return MaxInt64 but does not trigger
    // wrapping if not specially checked
    if (result/valB != valA) || (valA == math.MinInt64 && valB == -1) {
        if (valA < 0) != (valB < 0) {
            // return the minimum possible
            log.Logger().Warn("Resource calculation wrapped: returned minimum value possible",
                zap.Int64("valueA", int64(valA)),
                zap.Int64("valueB", int64(valB)))
            return math.MinInt64
        } else {
            // return the maximum possible
            log.Logger().Warn("Resource calculation wrapped: returned maximum value possible",
                zap.Int64("valueA", int64(valA)),
                zap.Int64("valueB", int64(valB)))
            return math.MaxInt64
        }
    }
    // not wrapped normal case
    return result
}

func mulValRatio(value Quantity, ratio float64) Quantity {
    // optimise the zero cases (often hit with zero resource)
    if value == 0 || ratio == 0 {
        return 0
    }
    result := float64(value) * ratio
    // protect against positive integer overflow
    if result > math.MaxInt64 {
        log.Logger().Warn("Multiplication result positive overflow",
            zap.Float64("value", float64(value)),
            zap.Float64("ratio", ratio))
        return math.MaxInt64
    }
    // protect against negative integer overflow
    if result < math.MinInt64 {
        result = math.MinInt64
        log.Logger().Warn("Multiplication result negative overflow",
            zap.Float64("value", float64(value)),
            zap.Float64("ratio", ratio))
        return math.MinInt64
    }
    // not wrapped normal case
    return Quantity(result)
}

// Operations on resources: the operations leave the passed in resources unchanged.
// Resources are sparse objects in all cases an undefined quantity is assumed zero (0).
// All operations must be nil safe.
// All operations that take more than one resource return a union of resource entries
// defined in both resources passed in. Operations must be able to handle the sparseness
// of the resource objects

// Add resources returning a new resource with the result
// A nil resource is considered an empty resource
func Add(left *Resource, right *Resource) *Resource {
    // check nil inputs and shortcut
    if left == nil {
        left = Zero
    }
    if right == nil {
        return left.Clone()
    }

    // neither are nil, clone one and add the other
    out := left.Clone()
    for k, v := range right.Resources {
        out.Resources[k] = addVal(out.Resources[k], v)
    }
    return out
}

// Subtract resource returning a new resource with the result
// A nil resource is considered an empty resource
// This might return negative values for specific quantities
func Sub(left *Resource, right *Resource) *Resource {
    // check nil inputs and shortcut
    if left == nil {
        left = Zero
    }
    if right == nil {
        return left.Clone()
    }

    // neither are nil, clone one and sub the other
    out := left.Clone()
    for k, v := range right.Resources {
        out.Resources[k] = subVal(out.Resources[k], v)
    }
    return out
}

// Subtract resource returning a new resource with the result
// A nil resource is considered an empty resource
// This will return 0 values for negative values
func SubEliminateNegative(left *Resource, right *Resource) *Resource {
    // check nil inputs and shortcut
    if left == nil {
        left = Zero
    }
    if right == nil {
        return left.Clone()
    }

    // neither are nil, clone one and sub the other
    out := left.Clone()
    for k, v := range right.Resources {
        out.Resources[k] = subVal(out.Resources[k], v)
        // make sure value is not negative
        if out.Resources[k] < 0 {
            out.Resources[k] = 0
        }
    }
    return out
}

// Check if smaller fitin larger, negative values will be treated as 0
// A nil resource is treated as an empty resource (zero)
func FitIn(larger *Resource, smaller *Resource) bool {
    if larger == nil {
        larger = Zero
    }
    // shortcut: a zero resource always fits because negative values are treated as 0
    if smaller == nil {
        return true
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

func Multiply(base *Resource, ratio int64) *Resource {
    ret := NewResource()
    // shortcut nil or zero input
    if base == nil  || ratio == 0 {
        return ret
    }
    qRatio := Quantity(ratio)
    for k, v := range base.Resources {
        ret.Resources[k] = mulVal(v, qRatio)
    }
    return ret
}

// Multiply the resource by the ratio returning a new resource.
// The result is rounded down to the nearest integer value after the multiplication.
// Result is protected from overflow (positive and negative).
// A nil resource passed in returns a new empty resource (zero)
func MultiplyBy(base *Resource, ratio float64) *Resource {
    ret := NewResource()
    if base == nil || ratio == 0 {
        return ret
    }
    for k, v := range base.Resources {
        ret.Resources[k] = mulValRatio(v, ratio)
    }
    return ret
}

// Return true if all quantities in larger > smaller
// Two resources that are equal are not considered strictly larger than each other.
func StrictlyGreaterThan(larger *Resource, smaller *Resource) bool {
    if larger == nil {
        larger = Zero
    }
    if smaller == nil {
        smaller = Zero
    }

    // keep track of the number of not equal values
    notEqual := false
    // check the larger side, track non equality
    for k, v := range larger.Resources {
        if smaller.Resources[k] > v {
            return false
        }
        if smaller.Resources[k] != v {
            notEqual = true
        }
    }

    // check the smaller side, track non equality
    for k, v := range smaller.Resources {
        if larger.Resources[k] < v {
            return false
        }
        if larger.Resources[k] != v {
            notEqual = true
        }
    }
    // at this point the resources are either equal or not
    // if they are not equal larger is strictly larger than smaller
    return notEqual
}

// Return true if all quantities in larger > smaller or if the two objects  are exactly the same.
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

// Have at least one quantity > 0, and no quantities < 0
// A nil resource is not strictly greater than zero.
func StrictlyGreaterThanZero(larger *Resource) bool {
    if larger == nil {
        return false
    }
    greater := false
    for _, v := range larger.Resources {
        if v < 0 {
            return false
        }
        if v > 0 {
            greater = true
        }
    }
    return greater
}

// Return the smallest quantity
func MinQuantity(x, y Quantity) Quantity {
    if x < y {
        return x
    }
    return y
}

// Return the largest quantity
func MaxQuantity(x, y Quantity) Quantity {
    if x > y {
        return x
    }
    return y
}


// Returns a new resource with the smallest value for each quantity in the resources
// If either resource passed in is nil a zero resource is returned
func ComponentWiseMin(left *Resource, right *Resource) *Resource {
    out := NewResource()
    if left != nil && right != nil {
        for k, v := range left.Resources {
            out.Resources[k] = MinQuantity(v, right.Resources[k])
        }
        for k, v := range right.Resources {
            out.Resources[k] = MinQuantity(v, left.Resources[k])
        }
    }
    return out
}

// Returns a new resource with the largest value for each quantity in the resources
// If either resource passed in is nil a zero resource is returned
func ComponentWiseMax(left *Resource, right *Resource) *Resource {
    out := NewResource()
    if left != nil && right != nil {
        for k, v := range left.Resources {
            out.Resources[k] = MaxQuantity(v, right.Resources[k])
        }
        for k, v := range right.Resources {
            out.Resources[k] = MaxQuantity(v, left.Resources[k])
        }
    }
    return out
}


// Check that the whole resource is zero
// A nil resource is zero (contrary to StrictlyGreaterThanZero)
func IsZero(zero *Resource) bool {
    if zero == nil {
        return true
    }
    for _, v := range zero.Resources {
        if v != 0 {
            return false
        }
    }
    return true
}
