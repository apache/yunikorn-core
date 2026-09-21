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
	"os"
	"testing"
	"testing/synctest"
	"time"

	godeadlock "github.com/sasha-s/go-deadlock"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/mock"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestMain(m *testing.M) {
	// Pooled deadlock timers cannot be reused across synctest bubbles.
	godeadlock.Opts.TimerPool = godeadlock.TimerPoolDisabled
	os.Exit(m.Run())
}

func TestRMProxy_StopUnblocksWaitingCaller(t *testing.T) {
	rmp := NewRMProxy(nil)
	rmp.StartService()

	c := make(chan *rmevent.Result, 1)
	rmp.HandleEvent(&rmevent.RMReleaseAllocationEvent{
		ReleasedAllocations: []*si.AllocationRelease{},
		RmID:                "rm-test",
		Channel:             c,
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

type infraReplyHandler struct {
	events chan interface{}
}

func (h *infraReplyHandler) HandleEvent(event interface{}) { h.events <- event }

func TestRMProxyInfraRepliesBuffered(t *testing.T) {
	for _, name := range []string{"registration", "reregistration", "config"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				handler := &infraReplyHandler{events: make(chan interface{})}
				rmp := NewRMProxy(handler)
				if name == "reregistration" {
					rmp.rmIDToCallback["test"] = &mock.ResourceManagerCallback{}
				}
				done := make(chan error, 1)
				go func() {
					if name == "config" {
						done <- rmp.UpdateConfiguration(&si.UpdateConfigurationRequest{RmID: "test"})
					} else {
						_, err := rmp.RegisterResourceManager(&si.RegisterResourceManagerRequest{RmID: "test"}, nil)
						done <- err
					}
				}()
				replies := 1
				if name == "reregistration" {
					replies = 2
				}
				for i := 0; i < replies; i++ {
					event := <-handler.events
					var ch chan *rmevent.Result
					switch ev := event.(type) {
					case *rmevent.RMRegistrationEvent:
						ch = ev.Channel
					case *rmevent.RMPartitionsRemoveEvent:
						ch = ev.Channel
					case *rmevent.RMConfigUpdateEvent:
						ch = ev.Channel
					default:
						t.Fatalf("unexpected event %T", event)
					}
					assert.Check(t, cap(ch) == 1, "reply channel must hold one result without a receiver")
					ch <- &rmevent.Result{Succeeded: true}
				}
				assert.NilError(t, <-done)
			})
		})
	}
}
