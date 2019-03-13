/*
Copyright 2019 The Unity Scheduler Authors

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
    "github.com/universal-scheduler/scheduler-spec/lib/go/si"
    "math"
    "sort"
)

// const keys
const (
    MEMORY = "memory"
    VCORE  = "vcore"
)

type Resource struct {
    Resources map[string]Quantity
}

var zero_resource *Resource = NewResource()

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

// No unit defined here for better performance
type Quantity int64

func (m *Resource) String() string {
    return fmt.Sprintf("%v", m.Resources)
}

func (m *Resource) ToProto() *si.Resource {
    proto := &si.Resource{}
    proto.Resources = make(map[string]*si.Quantity)
    for k, v := range m.Resources {
        proto.Resources[k] = &si.Quantity{Value: int64(v)}
    }
    return proto
}

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

func Add(l *Resource, r *Resource) *Resource {
    if l == nil {
        l = zero_resource
    }
    if r == nil {
        r = zero_resource
    }

    out := NewResource()
    for k, v := range r.Resources {
        out.Resources[k] = v
    }
    for k, v := range l.Resources {
        out.Resources[k] += v
    }
    return out
}

func Sub(l *Resource, r *Resource) *Resource {
    if l == nil {
        l = zero_resource
    }
    if r == nil {
        r = zero_resource
    }

    out := NewResource()
    for k, v := range l.Resources {
        out.Resources[k] = v
    }
    for k, v := range r.Resources {
        out.Resources[k] -= v
    }
    return out
}

// Should be used by internal computation only.
func AddTo(l *Resource, r *Resource) {
    for k, v := range r.Resources {
        l.Resources[k] += v
    }
}

// Should be used by internal computation only.
func SubFrom(l *Resource, r *Resource) {
    for k, v := range r.Resources {
        l.Resources[k] -= v
    }
}

// Check if smaller fitin larger, negative values will be treated as 0
func FitIn(larger *Resource, smaller *Resource) bool {
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

func getShares(partition *Resource, res *Resource) []float64 {
    shares := make([]float64, len(res.Resources))
    idx := 0
    for k, v := range res.Resources {
        if v == 0 {
            continue
        }

        // Get rid of 0 denominator (mostly)
        pv := float64(partition.Resources[k]) + 1e-4
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
func CompFairnessRatio(a1 *Resource, b1 *Resource, a2 *Resource, b2 *Resource) (int32, error) {
    lshares := getShares(b1, a1)
    rshares := getShares(b2, a2)

    return compareShares(lshares, rshares)
}

func compareShares(lshares, rshares []float64) (int32, error) {

    lIdx := len(lshares) - 1
    rIdx := len(rshares) - 1

    for rIdx >= 0 && lIdx >= 0 {
        lValue := lshares[lIdx]
        rValue := rshares[rIdx]
        if lValue > rValue {
            return 1, nil
        } else if lValue < rValue {
            return -1, nil
        } else {
            lIdx --
            rIdx --
        }
    }

    if lIdx == 0 && rIdx == 0 {
        return 0, nil
    } else if lIdx >= 0 {
        for lIdx >= 0 {
            if lshares[lIdx] > 0 {
                return 1, nil
            } else if lshares[lIdx] < 0 {
                return -1, nil
            } else {
                lIdx --
            }
        }
    } else {
        for rIdx >= 0 {
            if rshares[rIdx] > 0 {
                return -1, nil
            } else if rshares[rIdx] < 0 {
                return 1, nil
            } else {
                rIdx --
            }
        }
    }

    return 0, nil
}

func Comp(partition *Resource, l *Resource, r *Resource) (int32, error) {
    lshares := getShares(partition, l)
    rshares := getShares(partition, r)

    return compareShares(lshares, rshares)
}

func Equals(left *Resource, right *Resource) bool {
    if left == right {
        return true
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

// Should be used by temporary computation only
func MultiplyTo(left *Resource, ratio float64) {
    for k, v := range left.Resources {
        left.Resources[k] = Quantity(float64(v) * ratio)
    }
}

func MultiplyBy(left *Resource, ratio float64) *Resource {
    ret := NewResource()
    for k, v := range left.Resources {
        ret.Resources[k] = Quantity(float64(v) * ratio)
    }
    return ret
}

// Does all vector of larger > smaller
func StrictlyGreaterThan(larger *Resource, smaller *Resource) bool {
    for k, v := range larger.Resources {
        if smaller.Resources[k] >= v {
            return false
        }
    }

    for k, v := range smaller.Resources {
        if larger.Resources[k] <= v {
            return false
        }
    }

    return true
}

// Have at least one type > 0, and no type < 0
func StrictlyGreaterThanZero(larger *Resource) bool {
    var greater bool = false
    for _, v := range larger.Resources {
        if v < 0 {
            return false
        } else if v > 0 {
            greater = true
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

func ComponentWiseMin(left *Resource, right *Resource) *Resource {
    out := NewResource()
    for k, v := range left.Resources {
        out.Resources[k] = minQuantity(v, right.Resources[k])
    }
    for k, v := range right.Resources {
        out.Resources[k] = minQuantity(v, left.Resources[k])
    }
    return out
}
