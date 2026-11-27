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

package pendingresource

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
)

// Tracker holds a pending resource and calls onChange whenever it starts or stops being strictly
// greater than zero. The zero value is not usable, call New. Not safe for concurrent use: the owner
// serialises the calls.
type Tracker struct {
	res      *resources.Resource
	onChange func(hasPending bool)
}

func New(onChange func(hasPending bool)) *Tracker {
	if onChange == nil {
		panic("pendingresource: onChange must not be nil")
	}
	return &Tracker{
		res:      resources.NewResource(),
		onChange: onChange,
	}
}

// Add adds the delta to the tracked resource.
func (t *Tracker) Add(delta *resources.Resource) {
	t.set(resources.Add(t.res, delta))
}

// Sub subtracts the delta from the tracked resource, which may turn a quantity negative.
func (t *Tracker) Sub(delta *resources.Resource) {
	t.set(resources.Sub(t.res, delta))
}

// Reset sets the tracked resource to an empty resource and returns the value it replaced.
func (t *Tracker) Reset() *resources.Resource {
	old := t.res
	t.set(resources.NewResource())
	return old
}

// HasPending returns true if the tracked resource is strictly greater than zero.
func (t *Tracker) HasPending() bool {
	if t == nil {
		return false
	}
	return resources.StrictlyGreaterThanZero(t.res)
}

// IsZero returns true if the tracked resource is zero.
func (t *Tracker) IsZero() bool {
	if t == nil {
		return true
	}
	return resources.IsZero(t.res)
}

// Clone returns a copy of the tracked resource: the tracked value itself is never handed out.
func (t *Tracker) Clone() *resources.Resource {
	if t == nil {
		return nil
	}
	return t.res.Clone()
}

func (t *Tracker) set(res *resources.Resource) {
	was := t.HasPending()
	res.Prune()
	t.res = res
	if now := t.HasPending(); now != was {
		t.onChange(now)
	}
}
