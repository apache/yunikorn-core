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
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type Resource struct {
	Resources map[string]Quantity
}

// No unit defined here for better performance
type Quantity int64

func (q Quantity) string() string {
	return strconv.FormatInt(int64(q), 10)
}

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
	if m == nil {
		return NewResource()
	}
	return &Resource{Resources: m}
}

// Create a new resource from a string.
// The string must be a json marshalled si.Resource.
func NewResourceFromString(str string) (*Resource, error) {
	var siRes *si.Resource
	if err := json.Unmarshal([]byte(str), &siRes); err != nil {
		return nil, err
	}
	return NewResourceFromProto(siRes), nil
}

// Create a new resource from the config map.
// The config map must have been checked before being applied. The check here is just for safety so we do not crash.
func NewResourceFromConf(configMap map[string]string) (*Resource, error) {
	res := NewResource()
	for key, strVal := range configMap {
		var intValue Quantity
		var err error
		switch key {
		case common.CPU:
			intValue, err = ParseVCore(strVal)
		default:
			intValue, err = ParseQuantity(strVal)
		}
		if err != nil {
			return nil, err
		}
		if intValue < 0 {
			return nil, fmt.Errorf("negative resources not permitted: %v", configMap)
		}
		res.Resources[key] = intValue
	}
	return res, nil
}

func (r *Resource) String() string {
	if r == nil {
		return "nil resource"
	}
	return fmt.Sprintf("%v", r.Resources)
}

func (r *Resource) DAOMap() map[string]int64 {
	res := make(map[string]int64)
	if r != nil {
		for k, v := range r.Resources {
			res[k] = int64(v)
		}
	}
	return res
}

// Convert to a protobuf implementation
// a nil resource passes back an empty proto object
func (r *Resource) ToProto() *si.Resource {
	proto := &si.Resource{}
	proto.Resources = make(map[string]*si.Quantity)
	if r != nil {
		for k, v := range r.Resources {
			proto.Resources[k] = &si.Quantity{Value: int64(v)}
		}
	}
	return proto
}

// Return a clone (copy) of the resource it is called on.
// This provides a deep copy of the object with the exact same member set.
// NOTE: this is a clone not a sparse copy of the original.
func (r *Resource) Clone() *Resource {
	ret := NewResource()
	if r != nil {
		for k, v := range r.Resources {
			ret.Resources[k] = v
		}
		return ret
	}
	return nil
}

// Add additional resource to the base updating the base resource
// Should be used by temporary computation only
// A nil base resource does not change
// A nil passed in resource is treated as a zero valued resource and leaves base unchanged
func (r *Resource) AddTo(add *Resource) {
	if r != nil {
		if add == nil {
			return
		}
		for k, v := range add.Resources {
			r.Resources[k] = addVal(r.Resources[k], v)
		}
	}
}

// Subtract from the resource the passed in resource by updating the resource it is called on.
// Should be used by temporary computation only
// A nil base resource does not change
// A nil passed in resource is treated as a zero valued resource and leaves the base unchanged.
func (r *Resource) SubFrom(sub *Resource) {
	if r != nil {
		if sub == nil {
			return
		}
		for k, v := range sub.Resources {
			r.Resources[k] = subVal(r.Resources[k], v)
		}
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

// Calculate how well the receiver fits in "fit"
//   - A score of 0 is a fit (similar to FitIn)
//   - The score is calculated only using resource type defined in the fit resource.
//   - The score has a range between 0..#fit-res (the number of resource types in fit)
//   - Same score means same fit
//   - The lower the score the better the fit (0 is a fit)
//   - Each individual score is calculated as follows: score = (fitVal - resVal) / fitVal
//     That calculation per type is summed up for all resource types in fit.
//     example 1: fit memory 1000; resource 100; score = 0.9
//     example 2: fit memory 150; resource 15; score = 0.9
//     example 3: fit memory 100, cpu 1; resource memory 10; score = 1.9
//   - A nil receiver gives back the maximum score (number of resources types in fit)
func (r *Resource) FitInScore(fit *Resource) float64 {
	var score float64
	// short cut for a nil receiver and fit
	if r == nil || fit == nil {
		if fit != nil {
			return float64(len(fit.Resources))
		}
		return score
	}
	// walk over the defined values
	for key, fitVal := range fit.Resources {
		// negative is treated as 0 and fits always
		if fitVal <= 0 {
			continue
		}
		// negative is treated as 0 and gives max score of 1
		resVal := r.Resources[key]
		if resVal <= 0 {
			score++
			continue
		}
		// smaller values fit: score = 0 for those
		if fitVal > resVal {
			score += float64(fitVal-resVal) / float64(fitVal)
		}
	}
	return score
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
		}
		// return the maximum possible
		log.Logger().Warn("Resource calculation wrapped: returned maximum value possible",
			zap.Int64("valueA", int64(valA)),
			zap.Int64("valueB", int64(valB)))
		return math.MaxInt64
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
		}
		// return the maximum possible
		log.Logger().Warn("Resource calculation wrapped: returned maximum value possible",
			zap.Int64("valueA", int64(valA)),
			zap.Int64("valueB", int64(valB)))
		return math.MaxInt64
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

// SubOnlyExisting subtract delta from defined resource.
// Ignore any type not defined in the base resource (ie receiver).
// Used as part of the headroom updates as undefined resources are unlimited
func (r *Resource) SubOnlyExisting(delta *Resource) {
	// check nil inputs and shortcut
	if r == nil || delta == nil {
		return
	}
	// neither are nil, subtract the delta
	for k := range r.Resources {
		r.Resources[k] = subVal(r.Resources[k], delta.Resources[k])
	}
}

// SubEliminateNegative subtracts resource returning a new resource with the result
// A nil resource is considered an empty resource
// This will return 0 values for negative values
func SubEliminateNegative(left, right *Resource) *Resource {
	res, _ := subNonNegative(left, right)
	return res
}

// SubErrorNegative subtracts resource returning a new resource with the result. A nil resource is considered
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

// Check if smaller fits in larger
// Types not defined in the larger resource are considered 0 values for Quantity
// A nil resource is treated as an empty resource (all types are 0)
func FitIn(larger, smaller *Resource) bool {
	return larger.fitIn(smaller, false)
}

// Check if smaller fits in the defined resource
// Types not defined in resource this is called against are considered the maximum value for Quantity
// A nil resource is treated as an empty resource (no types defined)
func (r *Resource) FitInMaxUndef(smaller *Resource) bool {
	return r.fitIn(smaller, true)
}

// Check if smaller fits in the defined resource
// Negative values will be treated as 0
// A nil resource is treated as an empty resource, behaviour defined by skipUndef
func (r *Resource) fitIn(smaller *Resource, skipUndef bool) bool {
	if r == nil {
		r = Zero // shadows in the local function not seen by the callers.
	}
	// shortcut: a nil resource always fits because negative values are treated as 0
	// this step explicitly does not check for zero values or an empty resource that is handled by the loop
	if smaller == nil {
		return true
	}

	for k, v := range smaller.Resources {
		largerValue, ok := r.Resources[k]
		// skip if not defined (queue quota checks: undefined resources are considered max)
		if skipUndef && !ok {
			continue
		}
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
					zap.String("resource key", k),
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
				zap.String("resource key", k),
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

// Compare the resources equal returns the specific values for following cases:
// left  right  return
// nil   nil    true
// nil   <set>  false
// nil zero res true
// <set>   nil    false
// zero res nil true
// <set> <set>  true/false  *based on the individual Quantity values
func EqualsOrEmpty(left, right *Resource) bool {
	if IsZero(left) && IsZero(right) {
		return true
	}
	return Equals(left, right)
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
// If a resource type is missing from one of the Resource, it is considered 0
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

// Returns a new Resource with the smallest value for each quantity in the Resources
// If either Resource passed in is nil the other Resource is returned
// If a Resource type is missing from one of the Resource, it is considered empty and the quantity from the other Resource is returned
func ComponentWiseMinPermissive(left, right *Resource) *Resource {
	out := NewResource()
	if right == nil && left == nil {
		return nil
	}
	if left == nil {
		return right.Clone()
	}
	if right == nil {
		return left.Clone()
	}
	for k, v := range left.Resources {
		if val, ok := right.Resources[k]; ok {
			out.Resources[k] = MinQuantity(v, val)
		} else {
			out.Resources[k] = v
		}
	}
	for k, v := range right.Resources {
		if val, ok := left.Resources[k]; ok {
			out.Resources[k] = MinQuantity(v, val)
		} else {
			out.Resources[k] = v
		}
	}
	return out
}

func (r *Resource) HasNegativeValue() bool {
	if r == nil {
		return false
	}
	for _, v := range r.Resources {
		if v < 0 {
			return true
		}
	}
	return false
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
// A nil or empty resource is zero (contrary to StrictlyGreaterThanZero)
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

func CalculateAbsUsedCapacity(capacity, used *Resource) *Resource {
	absResource := NewResource()
	if capacity == nil || used == nil {
		log.Logger().Debug("Cannot calculate absolute capacity because of missing capacity or usage")
		return absResource
	}
	missingResources := &strings.Builder{}
	for resourceName, availableResource := range capacity.Resources {
		var absResValue int64
		if usedResource, ok := used.Resources[resourceName]; ok {
			if availableResource < usedResource {
				log.Logger().Warn("Higher usage than max capacity",
					zap.String("resource", resourceName),
					zap.Int64("capacity", int64(availableResource)),
					zap.Int64("usage", int64(usedResource)))
			}
			div := float64(usedResource) / float64(availableResource)
			absResValue = int64(div * 100)
			// protect against positive integer overflow
			if absResValue < 0 && div > 0 {
				log.Logger().Warn("Absolute resource value result positive overflow",
					zap.String("resource", resourceName),
					zap.Int64("capacity", int64(availableResource)),
					zap.Int64("usage", int64(usedResource)))
				absResValue = math.MaxInt64
			}
			// protect against negative integer overflow
			if absResValue > 0 && div < 0 {
				log.Logger().Warn("Absolute resource value result negative overflow",
					zap.String("resource", resourceName),
					zap.Int64("capacity", int64(availableResource)),
					zap.Int64("usage", int64(usedResource)))
				absResValue = math.MinInt64
			}
		} else {
			if missingResources.Len() != 0 {
				missingResources.WriteString(", ")
			}
			missingResources.WriteString(resourceName)
			continue
		}
		absResource.Resources[resourceName] = Quantity(absResValue)
	}
	if missingResources.Len() != 0 {
		log.Logger().Debug("Absolute usage result is missing resource information",
			zap.Stringer("missing resource(s)", missingResources))
	}
	return absResource
}
