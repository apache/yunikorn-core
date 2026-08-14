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
// goleak already filters out the goroutines that the testing, runtime and
// tracing packages run themselves, so every entry below is a goroutine that
// really does outlive the test binary today. IgnoreTopFunction matches on the
// top stack frame only, so an entry can also mask a new leak of the same shape:
// the list stops new KINDS of leak from being added, it is not a proof that the
// exempted counts stay put.
//
// The list is the baseline that was present when detection was switched on, and
// it is meant to be burned down rather than extended. Each exemption names its
// goroutine and the JIRA that tracks removing it.
func options() []goleak.Option {
	return []goleak.Option{
		// Event system handler (EventSystemImpl.StartServiceWithPublisher). See YUNIKORN-3366 (test cleanup) and YUNIKORN-3363 (restart-after-Stop).
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/events.(*EventSystemImpl).StartServiceWithPublisher.func1"),

		// Event publisher (eventPublisher.start). See YUNIKORN-3363.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/events.(*eventPublisher).start.func1"),

		// Event stream forwarder (EventStreaming.CreateEventStream). See YUNIKORN-3364.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/events.(*EventStreaming).CreateEventStream.func1"),

		// Partition queue cleaner (partitionManager.Run). See YUNIKORN-3366.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*partitionManager).cleanRoot"),

		// Partition expired-application cleaner (partitionManager.Run). See YUNIKORN-3366.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/scheduler.(*partitionManager).cleanExpiredApps"),

		// User/group cache cleaner (UserGroupCache.run). See YUNIKORN-3366.
		goleak.IgnoreTopFunction("github.com/apache/yunikorn-core/pkg/common/security.(*UserGroupCache).run"),

		// Scheduler allocation notify (ClusterContext.notifyRMNewAllocation). See YUNIKORN-3365.
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
