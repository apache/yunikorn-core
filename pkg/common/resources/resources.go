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
func Add(left, right *Resource) *Resource {
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
func Sub(left, right *Resource) *Resource {
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

// Subtract resource returning a new resource with the result. A nil resource is considered
// an empty resource. This will return an error if any value in the result is negative.
// The caller should at least log the error.
// The returned resource is valid and has all negative values reset to 0
func SubErrorNegative(left, right *Resource) (*Resource, error) {
	res, message := subNonNegative(left, right)
	var err error
	if message != "" {
		err = fmt.Errorf(message)
	}
	return res, err
}

// Subtract resource returning a new resource with the result
// A nil resource is considered an empty resource
// This will return 0 values for negative values
func SubEliminateNegative(left, right *Resource) *Resource {
	res, _ := subNonNegative(left, right)
	return res
}

// Internal subtract resource returning a new resource with the result and an error message when a
// quantity in the result was less than zero. All negative values are reset to 0.
func subNonNegative(left, right *Resource) (*Resource, string) {
	message := ""
	// check nil inputs and shortcut
	if left == nil {
		left = Zero
	}
	if right == nil {
		return left.Clone(), message
	}

	// neither are nil, clone one and sub the other
	out := left.Clone()
	for k, v := range right.Resources {
		out.Resources[k] = subVal(out.Resources[k], v)
		// make sure value is not negative
		if out.Resources[k] < 0 {
			if message == "" {
				message = "resource quantity less than zero for: " + k
			} else {
				message += ", " + k
			}
			out.Resources[k] = 0
		}
	}
	return out, message
}

// Check if smaller fitin larger, negative values will be treated as 0
// A nil resource is treated as an empty resource (zero)
func FitIn(larger, smaller *Resource) bool {
	if larger == nil {
		larger = Zero
	}
	// shortcut: a nil resource always fits because negative values are treated as 0
	// this step explicitly does not check for zero values or an empty resource that is handled by the loop
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

// Get the share of each resource quantity when compared to the total
// resources quantity
// NOTE: shares can be negative and positive in the current assumptions
func getShares(res, total *Resource) []float64 {
	// shortcut if the passed in resource to get the share on is nil or empty (sparse)
	if res == nil || len(res.Resources) == 0 {
		return make([]float64, 0)
	}
	shares := make([]float64, len(res.Resources))
	idx := 0
	for k, v := range res.Resources {
		// no usage then there is no share (skip prevents NaN)
		// floats init to 0 in the array anyway
		if v == 0 {
			continue
		}
		// Share is usage if total is nil or zero for this resource
		// Resources are integer so we could divide by 0. Handle it specifically here,
		// similar to a nil total resource. The check is not to prevent the divide by 0 error.
		// Compare against zero total resource fails without the check and some sorters use that.
		if total == nil || total.Resources[k] == 0 {
			// negative share is logged
			if v < 0 {
				log.Logger().Debug("usage is negative no total, share is also negative",
					zap.Int64("resource quantity", int64(v)))
			}
			shares[idx] = float64(v)
			idx++
			continue
		}
		shares[idx] = float64(v) / float64(total.Resources[k])
		// negative share is logged
		if shares[idx] < 0 {
			log.Logger().Debug("share set is negative",
				zap.Int64("resource quantity", int64(v)),
				zap.Int64("total quantity", int64(total.Resources[k])))
		}
		idx++
	}

	// sort in increasing order, NaN can not be part of the list
	sort.Float64s(shares)
	return shares
}

// Calculate share for left of total and right of total.
// This returns the same value as compareShares does:
// 0 for equal shares
// 1 if the left share is larger
// -1 if the right share is larger
func CompUsageRatio(left, right, total *Resource) int {
	lshares := getShares(left, total)
	rshares := getShares(right, total)

	return compareShares(lshares, rshares)
}

// Calculate share for left of total and right of total separately.
// This returns the same value as compareShares does:
// 0 for equal shares
// 1 if the left share is larger
// -1 if the right share is larger
func CompUsageRatioSeparately(left, leftTotal, right, rightTotal *Resource) int {
	lshares := getShares(left, leftTotal)
	rshares := getShares(right, rightTotal)

	return compareShares(lshares, rshares)
}

// Compare two resources usage shares and assumes a nil total resource.
// The share is thus equivalent to the usage passed in.
// This returns the same value as compareShares does:
// 0 for equal shares
// 1 if the left share is larger
// -1 if the right share is larger
func CompUsageShares(left, right *Resource) int {
	lshares := getShares(left, nil)
	rshares := getShares(right, nil)

	return compareShares(lshares, rshares)
}

// Get fairness ratio calculated by:
// highest share for left resource from total divided by
// highest share for right resource from total.
// If highest share for the right resource is 0 fairness is 1
func FairnessRatio(left, right, total *Resource) float64 {
	lshares := getShares(left, total)
	rshares := getShares(right, total)

	// Get the largest value from the shares
	lshare := float64(0)
	if shareLen := len(lshares); shareLen != 0 {
		lshare = lshares[shareLen-1]
	}
	rshare := float64(0)
	if shareLen := len(rshares); shareLen != 0 {
		rshare = rshares[shareLen-1]
	}
	// calculate the ratio
	ratio := lshare / rshare
	// divide by zero gives special NaN back change it to 1
	if math.IsNaN(ratio) {
		return 1
	}
	return ratio
}

// Compare the shares and return the compared value
// 0 for equal shares
// 1 if the left share is larger
// -1 if the right share is larger
func compareShares(lshares, rshares []float64) int {
	// get the length of the shares: a nil or empty share list gives -1
	lIdx := len(lshares) - 1
	rIdx := len(rshares) - 1
	// if both lists have at least 1 share start comparing
	for rIdx >= 0 && lIdx >= 0 {
		if lshares[lIdx] > rshares[rIdx] {
			return 1
		}
		if lshares[lIdx] < rshares[rIdx] {
			return -1
		}
		lIdx--
		rIdx--
	}
	// we got to the end: one of the two indexes must be negative or both are
	// case 1: nothing left on either side all shares are equal
	if lIdx == -1 && rIdx == -1 {
		return 0
	}
	// case 2: values left for the left shares
	if lIdx >= 0 {
		for lIdx >= 0 {
			if lshares[lIdx] > 0 {
				return 1
			}
			if lshares[lIdx] < 0 {
				return -1
			}
			lIdx--
		}
	}
	// case 3: values left for the right shares
	for rIdx >= 0 {
		if rshares[rIdx] > 0 {
			return -1
		}
		if rshares[rIdx] < 0 {
			return 1
		}
		rIdx--
	}
	// all left over values were 0 still equal (sparse resources)
	return 0
}

// Compare the resources equal returns the specific values for following cases:
// left  right  return
// nil   nil    true
// nil   <set>  false
// <set> nil    false
// <set> <set>  true/false  *based on the individual Quantity values
func Equals(left, right *Resource) bool {
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

// Multiply the resource by the integer ratio returning a new resource.
// Result is protected from overflow (positive and negative).
// A nil resource passed in returns a new empty resource (zero)
func Multiply(base *Resource, ratio int64) *Resource {
	ret := NewResource()
	// shortcut nil or zero input
	if base == nil || ratio == 0 {
		return ret
	}
	qRatio := Quantity(ratio)
	for k, v := range base.Resources {
		ret.Resources[k] = mulVal(v, qRatio)
	}
	return ret
}

// Multiply the resource by the floating point ratio returning a new resource.
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
func StrictlyGreaterThan(larger, smaller *Resource) bool {
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
func StrictlyGreaterThanOrEquals(larger, smaller *Resource) bool {
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
func ComponentWiseMin(left, right *Resource) *Resource {
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
func ComponentWiseMax(left, right *Resource) *Resource {
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
