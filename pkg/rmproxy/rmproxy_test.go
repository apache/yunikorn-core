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

package rmproxy

import (
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestRMProxy_DrainPendingEvents(t *testing.T) {
	rmp := NewRMProxy(nil)

	allocResultCh := make(chan *rmevent.Result, 1)
	releaseResultCh := make(chan *rmevent.Result, 1)

	rmp.HandleEvent(&rmevent.RMNewAllocationsEvent{
		Allocations: []*si.Allocation{},
		RmID:        "rm-test",
		Channel:     allocResultCh,
	})
	rmp.HandleEvent(&rmevent.RMReleaseAllocationEvent{
		ReleasedAllocations: []*si.AllocationRelease{},
		RmID:                "rm-test",
		Channel:             releaseResultCh,
	})

	rmp.drainPendingEvents()

	assertDrainFailedResult(t, allocResultCh, "allocResultCh")
	assertDrainFailedResult(t, releaseResultCh, "releaseResultCh")
}

func assertDrainFailedResult(t *testing.T, ch <-chan *rmevent.Result, name string) {
	t.Helper()
	select {
	case res := <-ch:
		assert.Assert(t, res != nil, "expected non-nil response on %s", name)
		assert.Assert(t, !res.Succeeded, "expected Succeeded to be false on %s", name)
		assert.Equal(t, res.Reason, "RMProxy is stopping")
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for response on %s", name)
	}
}
