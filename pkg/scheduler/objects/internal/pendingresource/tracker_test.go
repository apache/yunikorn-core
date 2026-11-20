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
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

func TestTracker(t *testing.T) {
	// newTracker returns a tracker and the list of onChange values it recorded
	newTracker := func() (*Tracker, *[]bool) {
		changes := make([]bool, 0)
		return New(func(hasPending bool) {
			changes = append(changes, hasPending)
		}), &changes
	}
	res := func(mem int64) *resources.Resource {
		return resources.NewResourceFromMap(map[string]resources.Quantity{"memory": resources.Quantity(mem)})
	}

	t.Run("new with nil onChange panics", func(t *testing.T) {
		defer func() {
			assert.Assert(t, recover() != nil, "New(nil) did not panic")
		}()
		New(nil)
	})

	t.Run("add from zero notifies once", func(t *testing.T) {
		tracker, changes := newTracker()
		tracker.Add(res(10))
		assert.DeepEqual(t, *changes, []bool{true})
		tracker.Add(res(5))
		assert.DeepEqual(t, *changes, []bool{true})
		assert.Assert(t, tracker.HasPending(), "tracker should have pending")
	})

	t.Run("sub notifies only on reaching zero", func(t *testing.T) {
		tracker, changes := newTracker()
		tracker.Add(res(10))
		tracker.Sub(res(4))
		assert.DeepEqual(t, *changes, []bool{true})
		tracker.Sub(res(6))
		assert.DeepEqual(t, *changes, []bool{true, false})
		assert.Assert(t, tracker.IsZero(), "tracker should be zero")
	})

	t.Run("reset returns the old value", func(t *testing.T) {
		tracker, changes := newTracker()
		tracker.Add(res(10))
		old := tracker.Reset()
		assert.Assert(t, resources.Equals(old, res(10)), "reset returned %v", old)
		assert.DeepEqual(t, *changes, []bool{true, false})
		assert.Assert(t, tracker.IsZero(), "tracker should be zero")
		assert.Assert(t, resources.IsZero(tracker.Reset()), "second reset returned a value")
		assert.DeepEqual(t, *changes, []bool{true, false})
	})

	t.Run("sub prunes zero valued keys", func(t *testing.T) {
		tracker, _ := newTracker()
		tracker.Add(res(10))
		tracker.Sub(res(10))
		_, ok := tracker.Clone().Resources["memory"]
		assert.Assert(t, !ok, "zero valued key was not pruned")
	})

	t.Run("clone is independent", func(t *testing.T) {
		tracker, _ := newTracker()
		tracker.Add(res(10))
		clone := tracker.Clone()
		clone.SubFrom(res(10))
		assert.Assert(t, tracker.HasPending(), "tracker changed with the clone")
		assert.Assert(t, !tracker.IsZero(), "tracker changed with the clone")
	})

	t.Run("nil receiver reads", func(t *testing.T) {
		var tracker *Tracker
		assert.Assert(t, !tracker.HasPending(), "nil tracker has pending")
		assert.Assert(t, tracker.IsZero(), "nil tracker is not zero")
		assert.Assert(t, tracker.Clone() == nil, "nil tracker cloned a value")
	})
}
