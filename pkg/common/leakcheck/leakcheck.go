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

// Package leakcheck wraps uber-go/goleak so that goroutine leak detection can be
// switched on for a package by adding a main_test.go with:
//
//	func TestMain(m *testing.M) {
//		leakcheck.VerifyTestMain(m)
//	}
//
// The shared exemptions are listed in options() so that there is a single place
// that documents which goroutines are allowed to outlive a test binary. A
// package that needs an exemption nothing else needs can pass it to
// VerifyTestMain instead of widening the shared list.
//
// Every test package in the repository is instrumented. pkg/rmproxy is the one
// package without a TestMain, because it has no test functions at all: the
// check there would guard nothing. Add one along with its first test.
package leakcheck

import (
	"testing"

	"go.uber.org/goleak"
)

// options returns the goleak exemptions shared by all instrumented packages.
//
// What this list does and does not buy us, precisely:
//
//   - goleak already filters out the goroutines that the testing, runtime and
//     tracing packages run themselves, so every entry below is a goroutine that
//     really does outlive the test binary today.
//   - IgnoreTopFunction matches on the top stack frame only. It is not bounded
//     by count and not bounded by package. So the baseline stops new KINDS of
//     leak from being added; it does not stop new instances of these seven
//     shapes, and a package that has never leaked any of them still has them
//     exempted because the list is shared. It is a ratchet against regression,
//     not a proof that the exempted counts stay put.
//   - Matching also assumes the goroutine is parked in its select when goleak
//     takes the stack snapshot, which holds for the default tick intervals used
//     in tests. A test that shortens an interval enough to catch one of these
//     mid-body would see a different top frame and a spurious failure.
//
// The list is the baseline that was present when detection was switched on. It
// is meant to be burned down, not extended: each entry names the goroutine,
// what starts it, and what has to change before the entry can be deleted.
//
// Three entries key on compiler-assigned closure names (".func1"). Those names
// are positional: adding another closure earlier in the same enclosing method
// renumbers them and the exemption silently stops matching, turning the
// affected package red. goleak v1.3.0 cannot match on the creator frame, so
// there is no more robust spelling available here. The durable fix is to hoist
// those three goroutine bodies into named methods, which is a production change
// and belongs in the follow-up that fixes the leaks themselves.
func options() []goleak.Option {
	return []goleak.Option{
		// Event system handler, started by EventSystemImpl.StartServiceWithPublisher.
		// Tests across events, scheduler and objects call events.Init() followed by
		// StartService()/StartServiceWithPublisher() without a matching Stop().
		// Stoppable: delete this entry once those tests defer Stop().
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/events.(*EventSystemImpl).StartServiceWithPublisher.func1"),

		// Shim event publisher, started by eventPublisher.start via StartService.
		// Leaks together with the handler above, and additionally when a configmap
		// update restarts an already stopped event system: Init() registers a
		// configmap callback that Stop() never removes, so a reload after Stop()
		// resurrects a system nobody holds a reference to. Suspected production
		// defect; delete this entry once Stop() deregisters the callback.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/events.(*eventPublisher).start.func1"),

		// Event stream forwarder, started by EventStreaming.CreateEventStream.
		// Two distinct causes share this top frame. One is a test that creates a
		// stream and never calls RemoveStream. The other is a consumer that stops
		// reading, which wedges the forwarder on the "consumer <- event" send
		// outside its select, where neither stop channel can reach it; that one is
		// a suspected production defect. Both must be fixed before this entry goes.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/events.(*EventStreaming).CreateEventStream.func1"),

		// Partition queue cleaner, started by partitionManager.Run when a partition
		// is added. Tests that build a ClusterContext never call ClusterContext.Stop.
		// Stoppable: delete this entry once those tests defer Stop().
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*partitionManager).cleanRoot"),

		// Partition expired application cleaner, the second goroutine started by
		// partitionManager.Run. Same cause and same fix as cleanRoot above.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*partitionManager).cleanExpiredApps"),

		// User/group cache cleaner, started once by security.GetUserGroupCache when
		// a partition resolves users. Stoppable: UserGroupCache.Stop() closes the
		// cleaner and resets the singleton, and ClusterContext.Stop() calls it.
		// Delete this entry once those tests defer Stop().
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/common/security.(*UserGroupCache).run"),

		// Scheduler allocation event handler, started by Scheduler.StartService as
		// handleAllocEvent, parked on the unbuffered result channel that
		// notifyRMNewAllocation creates. Observed chain: handleAllocEvent ->
		// handleRMUpdateAllocationEvent -> processAllocations -> notifyRMNewAllocation.
		// Suspected production shutdown race: ServiceContext.StopAll stops the
		// scheduler before the RM proxy, and handleAllocEvent's select can pick a
		// queued allocation event over the just-closed stop channel. The resulting
		// notify posts to the RM proxy and blocks on its reply, but the proxy's
		// handleRMEvents returns on its own stop without draining pendingRMEvents,
		// so the reply never comes. Intermittent: it needs that select to lose the
		// race. Delete this entry once shutdown drains or abandons in-flight
		// replies. Note notifyRMAllocationReleased has the same unbuffered-reply
		// shape and would leak under a different top frame; it has not been seen
		// yet, so it is deliberately not exempted here.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*ClusterContext).notifyRMNewAllocation"),
	}
}

// VerifyTestMain runs m and fails the test binary if goroutines leaked. Any
// extra options are applied on top of the shared exemptions, so a package can
// carry an exemption of its own without widening the list for everyone.
// It calls os.Exit, so it must be the last statement of TestMain.
func VerifyTestMain(m *testing.M, extra ...goleak.Option) {
	goleak.VerifyTestMain(m, append(options(), extra...)...)
}
