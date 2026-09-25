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

	"github.com/apache/yunikorn-core/pkg/mock"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type recordingReleaseCallback struct {
	mock.ResourceManagerCallback
	responses []*si.AllocationResponse
}

func (c *recordingReleaseCallback) UpdateAllocation(
	response *si.AllocationResponse,
) error {
	c.responses = append(c.responses, response)
	return nil
}

func TestRMProxy_StopUnblocksWaitingCaller(t *testing.T) {
	rmp := NewRMProxy(nil)
	rmp.StartService()

	c := make(chan *rmevent.Result, 1)
	rmp.HandleEvent(&rmevent.RMNewAllocationsEvent{
		Allocations: []*si.Allocation{},
		RmID:        "rm-test",
		Channel:     c,
	})

	rmp.Stop() // exercises the case <-rmp.stop: drainPendingEvents() wiring

	select {
	case res := <-c:
		assert.Assert(t, res != nil) // normal reply or drained — either is fine, point is no leak
	case <-time.After(time.Second):
		t.Fatal("caller leaked: no reply after Stop")
	}
}

func TestRMProxy_DrainPendingEvents(t *testing.T) {
	rmp := NewRMProxy(nil)
	allocResultCh := make(chan *rmevent.Result, 1)

	rmp.HandleEvent(&rmevent.RMReleaseAllocationEvent{
		ReleasedAllocations: []*si.AllocationRelease{},
		RmID:                "rm-test",
	})
	rmp.HandleEvent(&rmevent.RMNewAllocationsEvent{
		Allocations: []*si.Allocation{},
		RmID:        "rm-test",
		Channel:     allocResultCh,
	})

	rmp.drainPendingEvents()

	assertDrainFailedResult(t, allocResultCh, "allocResultCh")
	assert.Equal(t, len(rmp.pendingRMEvents), 0)
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

func TestRMProxy_ReleaseWithoutReplyChannel(t *testing.T) {
	rmp := NewRMProxy(nil)
	event := &rmevent.RMReleaseAllocationEvent{
		RmID:                "rm-test",
		ReleasedAllocations: []*si.AllocationRelease{},
	}

	done := make(chan struct{})
	go func() {
		rmp.processRMReleaseAllocationEvent(event)
		close(done)
	}()

	select {
	case <-done:
		// Release processing does not require a reply channel.
	case <-time.After(time.Second):
		t.Fatal("RM proxy did not finish processing release notification")
	}
}

func TestRMProxy_ReleaseReachesCallbackWithoutReplyChannel(t *testing.T) {
	rmp := NewRMProxy(nil)
	callback := &recordingReleaseCallback{}
	rmp.rmIDToCallback["rm-test"] = callback

	release := &si.AllocationRelease{
		ApplicationID:   "app-test",
		PartitionName:   "default",
		AllocationKey:   "alloc-test",
		TerminationType: si.TerminationType_PREEMPTED_BY_SCHEDULER,
		Message:         "test preemption",
	}
	event := &rmevent.RMReleaseAllocationEvent{
		RmID:                "rm-test",
		ReleasedAllocations: []*si.AllocationRelease{release},
	}

	done := make(chan struct{})
	go func() {
		rmp.processRMReleaseAllocationEvent(event)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RM proxy did not finish processing release notification")
	}

	assert.Equal(t, len(callback.responses), 1)
	assert.Equal(t, len(callback.responses[0].Released), 1)
	got := callback.responses[0].Released[0]
	assert.Equal(t, got.ApplicationID, release.ApplicationID)
	assert.Equal(t, got.PartitionName, release.PartitionName)
	assert.Equal(t, got.AllocationKey, release.AllocationKey)
	assert.Equal(t, got.TerminationType, release.TerminationType)
	assert.Equal(t, got.Message, release.Message)
}

func TestRMProxy_ProcessesEventAfterRelease(t *testing.T) {
	rmp := NewRMProxy(nil)
	callback := &recordingReleaseCallback{}
	rmp.rmIDToCallback["rm-test"] = callback

	rmp.StartService()
	defer rmp.Stop()

	rmp.HandleEvent(&rmevent.RMReleaseAllocationEvent{
		RmID: "rm-test",
		ReleasedAllocations: []*si.AllocationRelease{
			{
				ApplicationID:   "app-test",
				AllocationKey:   "alloc-test",
				TerminationType: si.TerminationType_TIMEOUT,
			},
		},
	})

	// This event's reply proves that the preceding release finished processing.
	reply := make(chan *rmevent.Result, 1)
	rmp.HandleEvent(&rmevent.RMNewAllocationsEvent{
		RmID:        "rm-test",
		Allocations: []*si.Allocation{},
		Channel:     reply,
	})

	select {
	case result := <-reply:
		assert.Assert(t, result.Succeeded)
	case <-time.After(time.Second):
		t.Fatal("RM event loop did not process the event after release")
	}

	assert.Equal(t, len(callback.responses), 1)
	assert.Equal(t, len(callback.responses[0].Released), 1)
	assert.Equal(
		t,
		callback.responses[0].Released[0].AllocationKey,
		"alloc-test",
	)
}
