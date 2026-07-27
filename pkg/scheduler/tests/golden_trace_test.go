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

package tests

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// ---------------------------------------------------------------------------------------------
// TestGoldenDecisionTrace is a decision-regression guard for the scheduler.
//
// It drives a fixed workload through the real RMProxy/partition API and pins the exact, ordered
// sequence of scheduling decisions as the resource manager observes them through UpdateAllocation:
// (applicationID, allocationKey, nodeID) for every new allocation, (applicationID, allocationKey,
// terminationType) for every release, and (applicationID, allocationKey) for every rejected
// allocation. That sequence is stored in testdata/golden_trace.json and compared verbatim.
//
// Why pin the trace at all: the existing scheduler tests assert individual outcomes ("this ask was
// allocated", "this queue is over its limit"). What they do not assert is the scheduler's observable
// output as a whole - which application gets which allocation, on which node, in which order, and
// which allocations are handed back to the RM for release. A change to ask ordering, node iteration,
// headroom accounting, preemption victim selection or placeholder replacement can leave every
// existing assertion satisfied while still changing what the cluster actually does. This test makes
// that class of change visible.
//
// A failure here is the test working, not the test being brittle. Either the change altered
// scheduling behaviour unintentionally - in which case fix the change - or it altered it
// deliberately, in which case regenerate the golden and call out the trace diff in review so the new
// decision sequence is reviewed as a behaviour change rather than accepted as noise. Do not
// regenerate it casually. A regenerating run deliberately ends RED (see compareOrUpdateGoldenAt) so
// that regeneration can never be mistaken for verification.
//
// Regenerating:
//
//	UPDATE_GOLDEN=1 go test ./pkg/scheduler/tests/ -run '^TestGoldenDecisionTrace'
//
// A behaviour change that is worth regenerating for is usually one that changes how many decisions a
// phase produces, so the exact-length phase barriers below have to give way for it: under
// UPDATE_GOLDEN they report what the run actually produced and carry on instead of failing. The same
// goes for the workload's observations OF that behaviour - traceProbe.pin, used for statements like
// "the low-priority recovery ask is still waiting" or "the failed application's asks are gone" -
// because a golden being regenerated is precisely the claim that the behaviour those describe has
// changed.
//
// What does NOT give way, in either mode, is the workload's own PRECONDITIONS: the state a phase has
// to be in before its result means anything - the set of allocations replayed on recovery, and the
// headroom left on node-main that phases 4 and 5 make their asks compete for. Those are plain
// assert.Assert, so a regenerating run stops on them instead of writing a golden. The distinction is
// what a failure would mean. A broken observation says the scheduler now decides something else,
// which is what a new golden is for. A broken precondition says the script no longer sets up the
// situation it describes, so the golden recorded from it would pin a different experiment under the
// same name - and be green while doing it. The fix for the second is to repair the workload, never
// to bless the golden.
//
// A golden test is only as good as its determinism, so the workload removes the usual sources of
// run-to-run variation:
//   - fixed application and node IDs;
//   - pinned strictly-increasing creation times on every submitted ask instead of wall-clock times;
//   - scheduling advanced by explicit MultiStepSchedule calls rather than the background scheduling
//     loop, with an observable barrier (waitForAsks and friends) between submitting work and driving
//     the scheduler, because RMProxy.UpdateAllocation only enqueues;
//   - no two asks within one application sharing the same (priority, createTime) pair.
//
// That last point matters because on a genuine tie the comparator that orders an application's
// requests contradicts itself. Requests are held in priority-descending, then creation-time-ascending
// order (Allocation.LessThan), whose equal-priority branch is
//
//	return a.createTime.After(other.createTime) || a.createTime.Equal(other.createTime)
//
// so for two asks sharing a (priority, createTime) LessThan is true in BOTH directions: each of them
// orders before the other. sortedRequests.insert then has two branches that disagree about where such
// an ask belongs - the fast path appends it AFTER the ask it ties with, the binary probe (sort.Search)
// inserts it BEFORE - and nothing in the ordering rule decides which of those runs. So a tie does not
// fall back to some weaker but still stated order; it leaves the position undefined, and the list has
// no order it can be said to be sorted into while the tie is in it. The workload gives no two asks in
// one application the same (priority, createTime), so the expected order follows from the rule alone.
//
// The one place where creation times are NOT pinned is the recovery path exercised by phase 4: the
// core hands allocations back to the RM through Allocation.NewSIFromAllocation, which copies neither
// AllocationTags nor Priority, so an allocation replayed from that snapshot is rebuilt by
// NewAllocationFromSI with createTime = time.Now() and priority 0. Phase 4 is therefore written so
// that nothing in the pinned trace depends on the ordering of the recovered allocations - see the
// comment there.
// ---------------------------------------------------------------------------------------------

const (
	goldenPath = "testdata/golden_trace.json"

	// application IDs, fixed rather than time- or counter-derived so the trace is stable.
	appPriority = "golden-app-priority"
	appRemove   = "golden-app-remove"
	appGang     = "golden-app-gang"
	appFailer   = "golden-app-failer"
	appLow      = "golden-app-low"
	appHigh     = "golden-app-high"

	nodeMain    = "golden-node-main:1"
	nodePreempt = "golden-node-preempt:1"
	queueMain   = "root.main"
	queueLow    = "root.low"
	queueHigh   = "root.high"
	// the barrier applications traceProbe.sync submits are parked here: a leaf no phase of the
	// workload schedules in, so the barrier mechanism is not part of what the golden measures.
	queueBarrier  = "root.barrier"
	goldenRMID    = "rm:123"
	goldenPart    = "default"
	goldenTimeout = 5000
)

// node-main's capacity ledger.
//
// Phases 4 and 5 only decide anything because node-main runs out of room at exactly the right point,
// so its capacity is derived from what the earlier phases place on it rather than picked round. Every
// figure below feeds both the asks the phases submit and the headroom they assert afterwards, so an
// ask whose size changes without its entry here changes a headroom assertion, and that assertion says
// what to recompute.
//
//	phase 1  appPriority p-high/p-mid/p-low   3 x 10/1  ->  30 memory /  3 vcore
//	phase 2  appRemove   r-1/r-3/r-4          3 x  5/1  ->  45 memory /  6 vcore
//	phase 3  appGang     g-placeholder            10/1  ->  55 memory /  7 vcore (peak 65/8 while
//	                     then g-real              10/1      the placeholder and the real
//	                                                        allocation are both live)
//	phase 4  recovered usage                            ->  55 memory /  7 vcore
//	         free against 105/10                        ->  50 memory /  3 vcore
//	         p-recover-high                    30/1     ->  85 memory /  8 vcore
//	         free, so p-recover-low (30/1) does not fit ->  20 memory /  2 vcore
//	phase 5  f-1 and f-2 (2 x 10/1) fit exactly in that remaining 20/2 - which is what makes the
//	         golden able to tell "the failed application's asks were discarded" apart from "they
//	         could not have been allocated anyway"
//
// Phase 6 does not depend on node-main's headroom at all: its asks request 50 vcore each and
// node-main only has 10 in total, so FitInNode excludes it whatever the memory situation.
const (
	phase1Asks     = 3 // p-high, p-mid, p-low
	phase1AskMem   = 10
	phase1AskVcore = 1

	phase2Asks     = 3 // r-1, r-3, r-4: r-2 is removed while still pending
	phase2AskMem   = 5
	phase2AskVcore = 1

	phase3Asks     = 1 // g-real: the placeholder it replaced is released again before the restart
	phase3AskMem   = 10
	phase3AskVcore = 1

	recoverAskMem   = 30 // p-recover-high and p-recover-low, only one of which can fit
	recoverAskVcore = 1

	failAskMem   = 10 // f-1 and f-2, both of which would fit if they were not discarded
	failAskVcore = 1

	// what the restart in phase 4 has to recover, and account for
	liveAtRestart  = phase1Asks + phase2Asks + phase3Asks
	recoveredMem   = phase1Asks*phase1AskMem + phase2Asks*phase2AskMem + phase3Asks*phase3AskMem
	recoveredVcore = phase1Asks*phase1AskVcore + phase2Asks*phase2AskVcore + phase3Asks*phase3AskVcore

	// node-main holds the recovered usage, exactly one of the two phase-4 asks, and both phase-5
	// asks - and nothing more.
	nodeMainCapMem   = recoveredMem + recoverAskMem + 2*failAskMem
	nodeMainCapVcore = recoveredVcore + recoverAskVcore + 2*failAskVcore
)

const goldenConfig = `
partitions:
  - name: default
    preemption:
      enabled: true
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: main
          - name: barrier
          - name: low
            resources:
              guaranteed:
                memory: 0
                vcore: 0
              max:
                memory: 100000
                vcore: 100000
          - name: high
            resources:
              guaranteed:
                memory: 1000
                vcore: 1000
              max:
                memory: 100000
                vcore: 100000
`

// createTimeSeq produces a strictly-increasing, fixed (non-wall-clock) createTime sequence for every
// ask in the workload. Creation time is part of the request ordering, so it has to be pinned rather
// than taken from the clock. Starting far in the past additionally guarantees that the 30s default
// preemption delay (configs.DefaultPreemptionDelay) has always already elapsed whenever the test
// runs: the precondition check is `now.Before(createTime+delay)`, and "now" is trivially far past
// these fixed timestamps.
type createTimeSeq struct {
	next int64
}

func newCreateTimeSeq() *createTimeSeq {
	return &createTimeSeq{next: 1700000000}
}

func (c *createTimeSeq) get() int64 {
	c.next++
	return c.next
}

// goldenAsk builds an SI ask/allocation with a pinned creation time and explicit priority.
// Resource type keys follow the common convention (memory, vcore).
func goldenAsk(key, appID string, priority int32, mem, vcore int64, createTime int64) *si.Allocation {
	return &si.Allocation{
		AllocationKey: key,
		ApplicationID: appID,
		Priority:      priority,
		ResourcePerAlloc: &si.Resource{
			Resources: map[string]*si.Quantity{
				siCommon.Memory: {Value: mem},
				siCommon.CPU:    {Value: vcore},
			},
		},
		AllocationTags: map[string]string{
			siCommon.CreationTime: strconv.FormatInt(createTime, 10),
		},
	}
}

// goldenNodeInfo builds a node registration of the given size. recovery_test.go's createNodeInfo is
// not reused here because it hardcodes 100 memory / 20 vcore, and every capacity in these workloads
// is chosen to make a specific scheduling decision come out one way rather than the other.
func goldenNodeInfo(nodeID string, mem, vcore int64) *si.NodeInfo {
	return &si.NodeInfo{
		NodeID:     nodeID,
		Attributes: map[string]string{},
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{
				siCommon.Memory: {Value: mem},
				siCommon.CPU:    {Value: vcore},
			},
		},
		Action: si.NodeInfo_CREATE,
	}
}

// ---------------------------------------------------------------------------------------------
// Barriers.
//
// RMProxy.UpdateAllocation only ENQUEUES the request: the scheduler drains pendingAllocEvents on its
// own goroutine (Scheduler.handleAllocEvent), so there is no happens-before between UpdateAllocation
// returning and the asks being registered on the application. Driving MultiStepSchedule straight
// afterwards therefore relies on the 100ms per-cycle sleep in MultiStepSchedule - which is commented
// in the product code as only working in tests - to have let the drain finish. On a loaded machine
// that can slip, and the failure surfaces as a timeout in a later wait rather than as a behaviour
// diff. Every UpdateAllocation in the golden-trace tests is followed by one of these barriers before
// the scheduler is stepped, so the workload advances on observed state.
// ---------------------------------------------------------------------------------------------

// waitForAsks blocks until every listed allocationKey is registered as a request on the application.
// Application.AddAllocationAsk populates sa.requests and sa.sortedRequests under the same lock that
// GetAllocationAsk takes, so seeing the request means the ask is fully schedulable.
func waitForAsks(t *testing.T, ms *mockScheduler, appID string, keys ...string) {
	t.Helper()
	waitForAskCondition(t, ms, appID, "registered", caller(), keys, func(app *objects.Application) bool {
		for _, key := range keys {
			if app.GetAllocationAsk(key) == nil {
				return false
			}
		}
		return true
	})
}

// waitForAsksGone blocks until none of the listed allocationKeys is registered on the application.
func waitForAsksGone(t *testing.T, ms *mockScheduler, appID string, keys ...string) {
	t.Helper()
	waitForAskCondition(t, ms, appID, "removed", caller(), keys, func(app *objects.Application) bool {
		for _, key := range keys {
			if app.GetAllocationAsk(key) != nil {
				return false
			}
		}
		return true
	})
}

// waitForAskCondition is the shared body of waitForAsks and waitForAsksGone. from is passed in rather
// than taken here because caller() unwinds a fixed number of frames: taken at this depth it would
// name the wrapper above rather than the workload step that is actually waiting.
func waitForAskCondition(t *testing.T, ms *mockScheduler, appID, what, from string, keys []string, cond func(*objects.Application) bool) {
	t.Helper()
	err := common.WaitForCondition(10*time.Millisecond, time.Duration(goldenTimeout)*time.Millisecond, func() bool {
		app := ms.getApplication(appID)
		if app == nil {
			return false
		}
		return cond(app)
	})
	assert.NilError(t, err, "timed out waiting for asks %v of %s to be %s, called from: %s", keys, appID, what, from)
}

// requireAllocationOnNode asserts that the node holds the allocation now, and returns a function that
// blocks until it has left again. Used around telling the core that the RM has released an allocation
// (a placeholder replacement or a preemption victim): that release is processed on the
// allocation-event goroutine, so the capacity it frees is not visible to the caller until the node has
// actually dropped it.
//
// Waiting for absence on its own would prove nothing. node.GetAllocation returns nil for a misspelled
// key, for a key belonging to a different application, and for an allocation that was never placed, so
// the first evaluation of such a wait succeeds and it returns having observed no transition at all -
// the same defect the trace-length barriers avoid by draining rather than polling (see
// traceProbe.sync). Establishing that the allocation is there first turns the wait into a transition:
// it was on the node, the release was sent, and now it is gone.
//
// The two halves are one call returning a closure rather than two calls because their order is not
// interchangeable. The presence check has to happen before the release is handed to the core, and by
// the time the wait runs the allocation may legitimately be gone already.
func requireAllocationOnNode(t *testing.T, ms *mockScheduler, nodeID, allocationKey string) func() {
	t.Helper()
	node := ms.getNode(nodeID)
	assert.Assert(t, node != nil, "node %s not found, called from: %s", nodeID, caller())
	assert.Assert(t, node.GetAllocation(allocationKey) != nil,
		"allocation %s is not on node %s to begin with, so waiting for it to leave would prove nothing, called from: %s",
		allocationKey, nodeID, caller())

	return func() {
		t.Helper()
		err := common.WaitForCondition(10*time.Millisecond, time.Duration(goldenTimeout)*time.Millisecond, func() bool {
			n := ms.getNode(nodeID)
			return n != nil && n.GetAllocation(allocationKey) == nil
		})
		assert.NilError(t, err, "timed out waiting for allocation %s to leave node %s, called from: %s", allocationKey, nodeID, caller())
	}
}

// warnLogging drops the default log level to WARN. The golden tests are normally run repeatedly to
// check determinism and info-level logging dominates their runtime.
//
// It has to be re-applied after every ms.Init: RegisterResourceManager passes the registration's
// extra config to configs.SetConfigMap, whose "logging" callback (registered in configs.go) calls
// log.UpdateLoggingConfig(GetConfigMap()). That map carries no log.level key, so the level falls
// back to Info and any level set before Init is lost.
func warnLogging() {
	log.UpdateLoggingConfig(map[string]string{"log.level": "WARN"})
}

// useWarnLogging applies warnLogging for the duration of the test. Logging configuration is
// process-global and shared with every other test in this package, so the configuration that was in
// effect when this test started is snapshotted here and put back on cleanup. Reading the config map
// at cleanup time instead would restore whatever this test's own ms.Init installed, which is the
// product default rather than what the previous test left behind.
func useWarnLogging(t *testing.T) {
	t.Helper()
	before := make(map[string]string)
	for key, value := range configs.GetConfigMap() {
		before[key] = value
	}
	t.Cleanup(func() {
		log.UpdateLoggingConfig(before)
	})
	warnLogging()
}

// updatingGolden reports whether this run regenerates the golden files instead of verifying them.
// UPDATE_GOLDEN is parsed as a boolean rather than compared against "1" so that the obvious spellings
// all work. Anything that is neither empty nor a boolean is a typo, and silently downgrading a typo
// to a verification run is how a maintainer comes to believe they regenerated a golden that they did
// not, so it fails instead.
func updatingGolden(t *testing.T) bool {
	t.Helper()
	value, set := os.LookupEnv("UPDATE_GOLDEN")
	if !set || value == "" {
		return false
	}
	update, err := strconv.ParseBool(value)
	if err != nil {
		t.Fatalf("UPDATE_GOLDEN=%q is not a boolean: set it to 1 (or true) to regenerate the goldens, or unset it to verify them", value)
	}
	return update
}

// ---------------------------------------------------------------------------------------------
// Phase bookkeeping.
//
// A golden mismatch is reported against the phase that produced the offending entry. The phase
// boundaries are recorded from the test goroutine at the point where the trace length has already
// been asserted, so the mapping is a property of the workload rather than of when a callback
// happened to fire.
// ---------------------------------------------------------------------------------------------

// traceMark records the half-open range of full-trace entries [start,end) that a phase accounts for.
// A phase that produces no decisions has start == end and therefore owns no entry; the mismatch
// listing calls those out at their boundary rather than through phaseOf.
type traceMark struct {
	phase string
	start int
	end   int
}

// ---------------------------------------------------------------------------------------------
// traceProbe reads the decision trace of one running mockScheduler and pins its length at each point
// in the workload where the number of decisions taken so far is known.
//
// The equality is the point: a ">= n" barrier is satisfied early by an unexpected extra decision, so
// a behaviour change drifts into a later wait and presents as a timeout instead of a diff. Requiring
// equality pins that the scheduler produced no decision beyond the ones the workload accounts for at
// this stage, and localises any surplus to the step that produced it.
//
// For that equality to mean anything the trace has to be quiescent, which a poll for a length cannot
// establish: at a point where the workload expects no new decision the poll's first evaluation
// already succeeds, and it returns before a decision still travelling towards the RM could land. So
// every read is preceded by sync(), which drives an observable round trip through both queues that
// carry decisions.
// ---------------------------------------------------------------------------------------------
type traceProbe struct {
	t  *testing.T
	ms *mockScheduler
	// leaf queue the barrier applications of sync() are parked in. It must be a queue no workload
	// schedules in, so that the barrier mechanism stays outside what the goldens measure - see sync().
	queue string

	updating bool // regenerating the goldens rather than verifying them
	barriers int  // barrier applications submitted so far, used to keep their IDs unique

	offset int // full-trace offset, see restarted
	marks  []traceMark
}

func newTraceProbe(t *testing.T, ms *mockScheduler, queue string) *traceProbe {
	t.Helper()
	return &traceProbe{t: t, ms: ms, queue: queue, updating: updatingGolden(t)}
}

// sync blocks until every decision the scheduler has already taken is recorded in the trace.
//
// It works by submitting an application that does nothing and waiting for the acceptance to come
// back, which is a round trip through both of the queues a decision can be travelling on:
//
//   - the application add is enqueued on Scheduler.pendingAllocEvents, the same queue every
//     UpdateAllocation lands on, and that queue is drained in order by a single goroutine. So by the
//     time the add is processed, every allocation request submitted before it has been processed to
//     completion - including the RM notifications it produced, because ClusterContext's
//     notifyRMNewAllocation and notifyRMAllocationReleased block on a reply channel;
//   - the acceptance is then enqueued on RMProxy.pendingRMEvents, the queue those notifications
//     travel on, and that queue is likewise drained in order by a single goroutine. So by the time
//     the mock RM records the acceptance, it has already recorded every decision enqueued ahead of
//     it.
//
// The barrier applications are parked in a leaf queue of their own (root.barrier, and the equivalent
// in each scenario config) that no workload submits work to. A barrier application is inert - it holds
// no asks, so it has no pending resources, its queue has none either, and both Queue.sortQueues and
// filterOnPendingResources drop them before any sorting or allocation attempt sees them - but that
// inertness must not be what keeps them out of the measurement. These goldens pin, among other things,
// the order in which a leaf serves its applications; putting the barriers in the very queue whose
// ordering is being pinned would make an implementation detail of the barrier mechanism a term in the
// result, and a change to that filtering would then move goldens that have nothing to do with it.
// Barriers never appear in the trace either way, which records allocations, releases and rejections
// only.
func (p *traceProbe) sync() {
	p.t.Helper()
	p.barriers++
	appID := fmt.Sprintf("golden-barrier-%d", p.barriers)
	err := p.ms.addApp(appID, p.queue, goldenPart)
	assert.NilError(p.t, err, "failed to submit barrier application %s", appID)
	p.ms.mockRM.waitForAcceptedApplication(p.t, appID, goldenTimeout)
}

// assertLen requires the trace to hold exactly n entries at this point in the workload, and returns
// the number it actually holds. where describes the place in the workload the assertion sits, so that
// a failure names that place rather than the helper that raised it.
//
// When regenerating it reports the difference and returns the observed count instead of failing: a
// change to the number of decisions a phase produces is precisely the kind of change regeneration
// exists for, so these barriers must not stand in its way. They keep their teeth on every other run.
func (p *traceProbe) assertLen(n int, where string) int {
	p.t.Helper()
	p.sync()
	trace := p.ms.mockRM.getTrace()
	switch {
	case len(trace) == n:
	case p.updating:
		p.t.Logf("regenerating: expected exactly %d decision(s) %s, this run produced %d - update the count at that call site:\n%s",
			n, where, len(trace), indentTrace(trace))
	default:
		p.t.Fatalf("expected exactly %d decision(s) %s, got %d:\n%s", n, where, len(trace), indentTrace(trace))
	}
	return len(trace)
}

// endPhase pins the number of decisions the trace holds once the named phase is complete, and
// remembers where that boundary falls in the full trace.
func (p *traceProbe) endPhase(phase string, n int) {
	p.t.Helper()
	got := p.assertLen(n, "by the end of "+phase)
	start := 0
	if len(p.marks) > 0 {
		start = p.marks[len(p.marks)-1].end
	}
	p.marks = append(p.marks, traceMark{phase: phase, start: start, end: p.offset + got})
}

// pin asserts an OBSERVATION of the behaviour the golden records - the recovery ask that must still
// be waiting, the asks a failed application must have discarded. Like the trace lengths these describe
// the behaviour being pinned, so they report and carry on when regenerating rather than blocking the
// regeneration of the golden that is supposed to replace them.
//
// A precondition - the state a phase has to be in for its result to mean anything - does not belong
// here. One that no longer holds does not describe a new behaviour, it says the run set up a different
// situation than the one the phase is named for, and regenerating from it produces a green golden that
// pins the wrong experiment. Those are asserted with assert.Assert instead, so that they fail in both
// modes; see the capacity-ledger check and the two headroom checks in phases 4 and 5.
func (p *traceProbe) pin(ok bool, format string, args ...interface{}) {
	p.t.Helper()
	if ok {
		return
	}
	if p.updating {
		p.t.Logf("regenerating: "+format, args...)
		return
	}
	p.t.Fatalf(format, args...)
}

// restarted tells the probe that the mock RM has been replaced by a scheduler restart, keeping the
// full-trace indices continuous: captured is the number of entries the previous mock RM recorded, so
// the post-restart trace is a suffix of the full trace rather than the whole of it.
func (p *traceProbe) restarted(captured int) {
	p.offset = captured
}

// TestGoldenDecisionTrace scripts a tie-free workload over the scheduler behaviours whose output
// order is most load-bearing, and most easily changed by accident:
//   - multiple priorities within a single application: allocation order must follow priority, not
//     submission order
//   - removing an ask while it and its siblings are still pending: the removal must retire that one
//     request without reaching the RM and without disturbing the order in which the application's
//     remaining requests are then served
//   - gang scheduling: a placeholder allocation replaced by a real one, which mutates an
//     application's requests during the same scheduling cycle that walks them
//   - scheduler restart and allocation recovery: the usage the recovered allocations represent must
//     be accounted for on the node, which is pinned by making the post-recovery asks compete for the
//     capacity that correct accounting leaves free
//   - an application that fails while still holding pending asks: cleanup must discard them so later
//     cycles neither allocate them nor crash, pinned on a node that still has room for them
//   - priority-driven preemption across sibling queues: victim selection, and the reserved
//     high-priority ask completing once the victim's capacity is freed
//
// Deliberately not covered: the placeholder-replacement revert in Application.tryPlaceholderAllocate,
// where marking the placeholder released fails because a second actor released or preempted it
// concurrently. Reaching that branch needs two goroutines racing on the same placeholder inside one
// scheduling cycle, which this single-threaded, step-driven harness cannot script without
// introducing exactly the timing nondeterminism a golden test must not have.
func TestGoldenDecisionTrace(t *testing.T) { //nolint:funlen
	useWarnLogging(t)

	seq := newCreateTimeSeq()

	ms := &mockScheduler{}
	defer ms.Stop()
	err := ms.Init(goldenConfig, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")
	warnLogging()
	ms.mockRM.enableTrace()

	pt := newTraceProbe(t, ms, queueBarrier)

	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{goldenNodeInfo(nodeMain, nodeMainCapMem, nodeMainCapVcore)},
		RmID:  goldenRMID,
	})
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedNode(t, nodeMain, goldenTimeout)

	// -----------------------------------------------------------------------------------------
	// Phase 1: multiple priorities within one application. The asks are submitted out of priority
	// order, so the resulting trace distinguishes priority ordering from submission order. The
	// priorities (5, 3, 1) are pairwise distinct, so no ordering tie has to be broken.
	// -----------------------------------------------------------------------------------------
	err = ms.addApp(appPriority, queueMain, goldenPart)
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedApplication(t, appPriority, goldenTimeout)

	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			goldenAsk("p-mid", appPriority, 3, phase1AskMem, phase1AskVcore, seq.get()),
			goldenAsk("p-high", appPriority, 5, phase1AskMem, phase1AskVcore, seq.get()),
			goldenAsk("p-low", appPriority, 1, phase1AskMem, phase1AskVcore, seq.get()),
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, appPriority, "p-mid", "p-high", "p-low")
	ms.scheduler.MultiStepSchedule(4)
	pt.endPhase("phase 1 priority order", phase1Asks)

	// -----------------------------------------------------------------------------------------
	// Phase 2: remove an ask while it and its siblings are all still pending. appRemove submits four
	// asks at four distinct priorities and NO scheduling cycle is run before the middle one (r-2,
	// priority 5) is removed, so the removal lands on a request list that is entirely pending. Two
	// things are then pinned: the removal produces no RM-visible decision of its own (the trace
	// length is unchanged across it), and the three survivors are subsequently served in priority
	// order with r-2 simply absent - the removal must not reorder or lose any of them.
	// -----------------------------------------------------------------------------------------
	err = ms.addApp(appRemove, queueMain, goldenPart)
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedApplication(t, appRemove, goldenTimeout)

	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			goldenAsk("r-1", appRemove, 7, phase2AskMem, phase2AskVcore, seq.get()),
			goldenAsk("r-2", appRemove, 5, phase2AskMem, phase2AskVcore, seq.get()),
			goldenAsk("r-3", appRemove, 3, phase2AskMem, phase2AskVcore, seq.get()),
			goldenAsk("r-4", appRemove, 1, phase2AskMem, phase2AskVcore, seq.get()),
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, appRemove, "r-1", "r-2", "r-3", "r-4")

	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: []*si.AllocationRelease{
				createAllocationRelease(appRemove, goldenPart, "r-2", si.TerminationType_STOPPED_BY_RM),
			},
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsksGone(t, ms, appRemove, "r-2")
	// the other three must have survived the removal untouched.
	waitForAsks(t, ms, appRemove, "r-1", "r-3", "r-4")
	// removing a still-pending ask is not an RM-visible decision: nothing was ever allocated or
	// released at the RM level for it, so the trace is still exactly what phase 1 left behind. Seeing
	// the request gone is not enough to conclude that, because the removal is still being processed
	// when it becomes observable, so assertLen drains both decision queues before it counts.
	pt.assertLen(phase1Asks, "after removing the still-pending r-2")

	ms.scheduler.MultiStepSchedule(4)
	pt.endPhase("phase 2 pending ask removal", phase1Asks+phase2Asks)

	// -----------------------------------------------------------------------------------------
	// Phase 3: gang scheduling - a placeholder allocation replaced by the real one. The shape of
	// the exchange mirrors smoke_test.go's TestDupReleasesInGangScheduling: the core allocates the
	// placeholder, then on seeing the real ask asks the RM to release the placeholder, and only
	// once the RM confirms does the real allocation appear. All three steps are RM-visible, so the
	// golden pins the full replacement handshake and its ordering.
	// -----------------------------------------------------------------------------------------
	err = ms.addApp(appGang, queueMain, goldenPart)
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedApplication(t, appGang, goldenTimeout)

	phAsk := goldenAsk("g-placeholder", appGang, 6, phase3AskMem, phase3AskVcore, seq.get())
	phAsk.TaskGroupName = "tg"
	phAsk.Placeholder = true
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{phAsk},
		RmID:        goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, appGang, "g-placeholder")
	ms.scheduler.MultiStepSchedule(2)
	pt.assertLen(7, "after allocating the gang placeholder")

	realAsk := goldenAsk("g-real", appGang, 6, phase3AskMem, phase3AskVcore, seq.get())
	realAsk.TaskGroupName = "tg"
	realAsk.Placeholder = false
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{realAsk},
		RmID:        goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, appGang, "g-real")
	ms.scheduler.MultiStepSchedule(2)
	// the core notifies the RM the placeholder should be released.
	pt.assertLen(8, "after the core asked the RM to release the placeholder")

	placeholderGone := requireAllocationOnNode(t, ms, nodeMain, "g-placeholder")
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: []*si.AllocationRelease{
				createAllocationRelease(appGang, goldenPart, "g-placeholder", si.TerminationType_PLACEHOLDER_REPLACED),
			},
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	// no scheduling step is needed to complete the replacement: the confirmation is processed on the
	// allocation-event goroutine, and ClusterContext.processAllocationReleases notifies the RM of the
	// real allocation directly (see the "placeholder swap & preemption" branch there).
	pt.endPhase("phase 3 gang placeholder replacement", 9)
	placeholderGone()

	// -----------------------------------------------------------------------------------------
	// Phase 4: restart the scheduler and recover. Re-register the node and the three applications
	// that still hold live allocations (appPriority, appRemove, appGang), replay those allocations
	// through the recovery path, then make two post-recovery asks compete for what correct recovery
	// accounting leaves free on node-main.
	//
	// The recovered usage is 55 memory / 7 vcore (30/3 from appPriority, 15/3 from appRemove, 10/1
	// from appGang's g-real), so 50 memory / 3 vcore of node-main's 105/10 is free. The two asks are
	// 30 memory / 1 vcore each: exactly one of them fits. Correct accounting therefore allocates
	// p-recover-high (priority 9) and leaves p-recover-low (priority 7) pending. If recovery lost the
	// usage, both fit and both allocate; if it double-counted, neither does. Either way the trace
	// changes and the golden fails - which is what makes this phase pin recovery rather than merely
	// survive it. See the capacity ledger above for where 105/10 comes from.
	//
	// Note what is NOT pinned here: the recovered allocations' own createTime and priority.
	// Allocation.NewSIFromAllocation copies neither AllocationTags nor Priority, so what the RM
	// snapshot carries back into NewAllocationFromSI is rebuilt with createTime = time.Now() and
	// priority 0. Nothing in this phase depends on their relative order - they are all allocated
	// already, and tryAllocate skips allocated requests - so the wall-clock times that enter here
	// cannot reach the trace.
	// -----------------------------------------------------------------------------------------
	// the ledger has to leave room for one of the two asks and not for both, whatever else changes.
	assert.Assert(t, recoverAskMem <= nodeMainCapMem-recoveredMem && 2*recoverAskMem > nodeMainCapMem-recoveredMem,
		"the capacity ledger must leave room on node-main for exactly one of the two %d memory recovery asks, it leaves %d",
		recoverAskMem, nodeMainCapMem-recoveredMem)

	preRestartTrace := ms.mockRM.getTrace()
	oldMockRM := ms.mockRM
	ms.serviceContext.StopAll()

	err = ms.Init(goldenConfig, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed on restart")
	warnLogging()
	ms.mockRM.enableTrace()
	pt.restarted(len(preRestartTrace))

	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{goldenNodeInfo(nodeMain, nodeMainCapMem, nodeMainCapVcore)},
		RmID:  goldenRMID,
	})
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedNode(t, nodeMain, goldenTimeout)

	for _, appID := range []string{appPriority, appRemove, appGang} {
		err = ms.addApp(appID, queueMain, goldenPart)
		assert.NilError(t, err)
		ms.mockRM.waitForAcceptedApplication(t, appID, goldenTimeout)
	}

	part := ms.scheduler.GetClusterContext().GetPartition(ms.partitionName)
	assert.Assert(t, part != nil, "partition not found after restart")
	// the per-node history is append-only (see mockRMCallback.UpdateAllocation) and still contains
	// g-placeholder even though it was replaced (released) before the restart. Only allocations still
	// present in the Allocations map (which IS pruned on release) are actually live; replay those,
	// not the raw per-node history.
	live := oldMockRM.getAllocations()
	var liveOnNodeMain []*si.Allocation
	for _, alloc := range oldMockRM.getNodeAllocations(nodeMain) {
		if _, ok := live[alloc.AllocationKey]; ok {
			liveOnNodeMain = append(liveOnNodeMain, alloc)
		}
	}
	// a precondition, not an observation: this is the set of allocations the recovery under test is
	// given to replay, so a run that replays a different set is not this phase.
	assert.Assert(t, len(liveOnNodeMain) == liveAtRestart,
		"expected %d live allocations on %s to recover, found %d: phases 1 to 3 no longer leave what the capacity ledger says they do, recompute phase1Asks/phase2Asks/phase3Asks",
		liveAtRestart, nodeMain, len(liveOnNodeMain))
	err = registerAllocations(part, liveOnNodeMain)
	assert.NilError(t, err, "failed to replay recovered allocations")

	// the headroom the rest of this phase and all of phase 5 depend on, checked against the node
	// rather than against the comment above it. A precondition, and a hard one in both modes: this is
	// what makes the two recovery asks compete for one slot, so a run that starts phase 4 with a
	// different amount free is measuring something other than what the phase is named for - and a
	// golden regenerated from it would look exactly like a golden regenerated from a real behaviour
	// change.
	free := ms.getNode(nodeMain).GetAvailableResource()
	assert.Assert(t, free.Resources[siCommon.Memory] == nodeMainCapMem-recoveredMem && free.Resources[siCommon.CPU] == nodeMainCapVcore-recoveredVcore,
		"recovery left %v free on %s, the capacity ledger says %d memory / %d vcore: either recovery accounting changed or the ledger is stale",
		free, nodeMain, nodeMainCapMem-recoveredMem, nodeMainCapVcore-recoveredVcore)

	recoveredApp := ms.getApplication(appPriority)
	assert.Assert(t, recoveredApp != nil, "recovered app-priority not found")

	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			goldenAsk("p-recover-high", appPriority, 9, recoverAskMem, recoverAskVcore, seq.get()),
			goldenAsk("p-recover-low", appPriority, 7, recoverAskMem, recoverAskVcore, seq.get()),
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, appPriority, "p-recover-high", "p-recover-low")
	ms.scheduler.MultiStepSchedule(3)
	pt.endPhase("phase 4 restart and recovery", 1)
	// the trace pins that only one allocation was made; this pins that the one left behind is the
	// low-priority ask and that it is still waiting. Application.requests keeps allocated asks
	// around - tryAllocate skips them via IsAllocated - so the ask merely being present says nothing.
	recoverLow := recoveredApp.GetAllocationAsk("p-recover-low")
	pt.pin(recoverLow != nil && !recoverLow.IsAllocated(),
		"p-recover-low should still be waiting: recovered usage left room for only one of the two asks")

	// p-recover-low has done its job. It is withdrawn here because phase 6 introduces a second, much
	// larger node, and a request left pending across that would be allocated there and change both
	// the trace and the capacity phase 6 depends on.
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: []*si.AllocationRelease{
				createAllocationRelease(appPriority, goldenPart, "p-recover-low", si.TerminationType_STOPPED_BY_RM),
			},
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsksGone(t, ms, appPriority, "p-recover-low")

	// -----------------------------------------------------------------------------------------
	// Phase 5: an application fails while still holding pending asks. No scheduling cycle is run
	// between submitting the asks and failing the application, so they are pending but never
	// attempted - a deterministic way to reach the ask cleanup done on failure without relying on
	// any timer or timeout trigger.
	//
	// What is pinned here is exactly two things: the asks are no longer registered on the application,
	// and no decision about them reaches the RM - the cycles run below produce no trace entry, and
	// nothing crashes walking a failed application's requests.
	//
	// node-main is left with exactly 20 memory / 2 vcore at this point and f-1 and f-2 are 10/1 each,
	// so both of them WOULD be allocated by the cycles run below had the cleanup not happened. That
	// is what lets the trace assertion carry weight here rather than merely restating that a full
	// node cannot take more work, and that headroom is checked against the node as a hard
	// precondition rather than asserted by this comment.
	//
	// What is NOT pinned is the resource accounting behind that cleanup. Application.cleanupAsks,
	// which runs on entering Failed, clears sa.requests and sa.sortedRequests and nothing else - it
	// does not give back the application's pending resource or reset its askMaxPriority the way
	// removeAsksInternal("") does - so both still carry the two discarded asks when the state change
	// returns. Neither assertion below looks at them, and no later phase depends on them: the
	// terminated-application callback takes the application out of its queue shortly afterwards, and
	// Queue.RemoveApplication decrements the queue by whatever pending the application is still
	// carrying, which happens to be exactly the amount that was never given back. Pinning the
	// accounting would need assertions this workload does not make.
	// -----------------------------------------------------------------------------------------
	free = ms.getNode(nodeMain).GetAvailableResource()
	assert.Assert(t, free.Resources[siCommon.Memory] == 2*failAskMem && free.Resources[siCommon.CPU] == 2*failAskVcore,
		"phase 5 needs %s to have room for both of its %d memory / %d vcore asks and no more, it has %v: recompute the capacity ledger",
		nodeMain, failAskMem, failAskVcore, free)

	err = ms.addApp(appFailer, queueMain, goldenPart)
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedApplication(t, appFailer, goldenTimeout)

	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			goldenAsk("f-1", appFailer, 2, failAskMem, failAskVcore, seq.get()),
			goldenAsk("f-2", appFailer, 4, failAskMem, failAskVcore, seq.get()),
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, appFailer, "f-1", "f-2")

	failerApp := ms.getApplication(appFailer)
	assert.Assert(t, failerApp != nil, "app-failer not found")

	// NOT a duplicated line: reaching Failed takes two FailApplication events. The first moves the
	// application to Failing, the second Failing -> Failed (see the FailApplication transitions in
	// application_state.go). The ask cleanup being pinned here happens on entering Failed, so a
	// single call would leave the asks in place and the assertions below would fail.
	assert.NilError(t, failerApp.FailApplication("induced failure for golden-trace test"))
	assert.NilError(t, failerApp.FailApplication("induced failure for golden-trace test"))
	assert.Equal(t, failerApp.CurrentState(), objects.Failed.String())
	// the pending asks must be gone, and scheduling afterwards must produce no allocation for them
	// (no crash, no trace entries) even though the node could still hold both.
	ms.scheduler.MultiStepSchedule(3)
	pt.pin(failerApp.GetAllocationAsk("f-1") == nil, "failure cleanup should have cleared f-1")
	pt.pin(failerApp.GetAllocationAsk("f-2") == nil, "failure cleanup should have cleared f-2")
	pt.endPhase("phase 5 failed application ask cleanup", 1)

	// -----------------------------------------------------------------------------------------
	// Phase 6: priority-driven preemption across sibling queues. root.low and root.high each have
	// explicit (non-nil) guaranteed resources configured, which preemption requires (see
	// configs.DefaultPreemptionDelay: "guaranteed resources must be set to trigger preemption").
	// A dedicated, exactly-sized node makes the victim and the outcome unambiguous: two
	// low-priority allocations from appLow (AllowPreemptSelf) fill it completely, then a
	// higher-priority ask from appHigh (AllowPreemptOther) in the sibling queue cannot fit and
	// triggers preemption of exactly one of them. The two low-priority asks carry distinct, pinned
	// createTimes, which is also the final tiebreak in sortVictimsForPreemption - so victim
	// selection is deterministic rather than dependent on a priority tie.
	//
	// Queues are not node-pinned, so node-main is a candidate for these asks too. It is excluded not
	// by headroom but by shape: all three asks request 50 vcore and node-main has only 10 in total,
	// so FitInNode rejects it for them however much memory happens to be free.
	// -----------------------------------------------------------------------------------------
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{goldenNodeInfo(nodePreempt, 100, 100)},
		RmID:  goldenRMID,
	})
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedNode(t, nodePreempt, goldenTimeout)

	err = ms.addApp(appLow, queueLow, goldenPart)
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedApplication(t, appLow, goldenTimeout)
	err = ms.addApp(appHigh, queueHigh, goldenPart)
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedApplication(t, appHigh, goldenTimeout)

	lowAsk1 := goldenAsk("low-1", appLow, 1, 50, 50, seq.get())
	lowAsk1.PreemptionPolicy = &si.PreemptionPolicy{AllowPreemptSelf: true}
	lowAsk2 := goldenAsk("low-2", appLow, 1, 50, 50, seq.get())
	lowAsk2.PreemptionPolicy = &si.PreemptionPolicy{AllowPreemptSelf: true}
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{lowAsk1, lowAsk2},
		RmID:        goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, appLow, "low-1", "low-2")
	ms.scheduler.MultiStepSchedule(3)
	// both low-priority allocations must land before the high-priority ask is introduced, or the
	// preemption target is ambiguous (nothing yet to preempt).
	pt.assertLen(3, "after filling the preemption node")

	highAsk := goldenAsk("high-1", appHigh, 9, 50, 50, seq.get())
	highAsk.PreemptionPolicy = &si.PreemptionPolicy{AllowPreemptOther: true}
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{highAsk},
		RmID:        goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, appHigh, "high-1")
	ms.scheduler.MultiStepSchedule(4)
	// preemption fires: the core notifies the RM to release the chosen victim.
	pt.assertLen(4, "after preemption chose a victim")

	victimKey := preemptionVictim(t, ms.mockRM.getTrace())
	victimGone := requireAllocationOnNode(t, ms, nodePreempt, victimKey)

	// the RM confirms the preemption (mirrors the shim actually terminating the pod).
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: []*si.AllocationRelease{
				createAllocationRelease(appLow, goldenPart, victimKey, si.TerminationType_PREEMPTED_BY_SCHEDULER),
			},
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	// the only barrier between the RM's confirmation and the scheduling step below that has to observe
	// the freed capacity, so it has to establish that the victim actually left rather than that it is
	// not there.
	victimGone()
	ms.scheduler.MultiStepSchedule(2)
	// freed capacity lets the reserved high-priority ask complete.
	pt.endPhase("phase 6 preemption", 5)

	// -----------------------------------------------------------------------------------------
	// Assemble the full trace (pre-restart + post-restart mock RM instances) and compare to golden.
	// -----------------------------------------------------------------------------------------
	fullTrace := append(append([]traceEntry{}, preRestartTrace...), ms.mockRM.getTrace()...)
	compareOrUpdateGoldenAt(t, goldenPath, fullTrace, pt.marks, pt.updating)
}

// preemptionVictim reads the victim the core chose out of the last trace entry, which the caller has
// just pinned as the preemption notification. The identity of the victim is pinned by the golden; all
// this needs to establish is that the entry is a release of one of the two candidates.
//
// Both checks are fatal in both modes, because the key read here is fed straight back into the release
// that confirms the preemption. An empty trace leaves nothing to feed back. A last entry that is an
// allocation rather than a release means the key names something that is not a victim - and quite
// possibly an allocation of a different application - so the release request built from it would be
// addressed to the wrong work, the wait for the victim to leave the node would be waiting for
// something that was never going to leave it, and a regenerating run would write a golden that pins a
// preemption handshake nobody performed. Neither is a behaviour change a new golden should record.
func preemptionVictim(t *testing.T, trace []traceEntry) string {
	t.Helper()
	if len(trace) == 0 {
		t.Fatal("no decision to read a preemption victim from")
	}
	victim := trace[len(trace)-1]
	assert.Assert(t, victim.TerminationType != "" && (victim.AllocationKey == "low-1" || victim.AllocationKey == "low-2"),
		"expected the last decision to be the release of low-1 or low-2, got %s", formatTraceEntry(victim))
	return victim.AllocationKey
}

// compareOrUpdateGoldenAt compares the captured trace against the golden JSON file at path. marks may
// be nil; when set it is used to attribute a mismatch to the phase that produced it.
//
// When updating it (re)writes the golden file instead of asserting, and then fails: a regenerating run
// must never be green. A green regeneration is indistinguishable from a run that actually verified
// something, which is how a real regression gets written into a golden unnoticed, and how every golden
// here would silently stop asserting if UPDATE_GOLDEN ever leaked into a CI environment. Only
// regenerate for a behaviour change that is intended and reviewed (see the doc comment on
// TestGoldenDecisionTrace).
func compareOrUpdateGoldenAt(t *testing.T, path string, trace []traceEntry, marks []traceMark, updating bool) {
	t.Helper()

	data, err := json.MarshalIndent(trace, "", "  ")
	assert.NilError(t, err, "failed to marshal trace")
	data = append(data, '\n')

	if updating {
		assert.NilError(t, os.WriteFile(path, data, 0o600), "failed to write golden file")
		t.Errorf("golden %s regenerated (%d entries); re-run without UPDATE_GOLDEN to verify", path, len(trace))
		return
	}

	golden, err := os.ReadFile(path)
	assert.NilError(t, err, "failed to read golden file %s (run with UPDATE_GOLDEN=1 to create it)", path)

	var goldenTrace []traceEntry
	assert.NilError(t, json.Unmarshal(golden, &goldenTrace), "failed to parse golden file %s", path)

	if !reflect.DeepEqual(goldenTrace, trace) {
		t.Fatal(traceMismatch(path, goldenTrace, trace, marks))
	}
}

// traceMismatch renders a golden mismatch as a side-by-side listing. assert.DeepEqual's own output is
// a "--- x / +++ y" diff that says neither which side is the golden nor which golden file it came
// from - and six of the seven goldens in this package share this helper.
//
// The two sides can differ in length even though the run's own length is pinned phase by phase: those
// barriers pin what this run produced, not what the file on disk holds, so a golden that was written
// for a longer or shorter workload still lands here. That is what the <none> rows are for.
func traceMismatch(path string, golden, trace []traceEntry, marks []traceMark) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "scheduling decisions do not match the golden file.\n")
	fmt.Fprintf(&sb, "  golden file: %s (%d entries)\n", path, len(golden))
	fmt.Fprintf(&sb, "  this run:    %d entries\n\n", len(trace))

	length := len(golden)
	if len(trace) > length {
		length = len(trace)
	}
	// the phase column is only present for the shared trace, which is the only one long enough for
	// the attribution to be worth anything.
	phases := make([]string, length)
	expected := make([]string, length)
	actual := make([]string, length)
	phaseWidth, expectedWidth := 0, 0
	for i := range phases {
		phases[i] = phaseOf(marks, i)
		expected[i], actual[i] = "<none>", "<none>"
		if i < len(golden) {
			expected[i] = formatTraceEntry(golden[i])
		}
		if i < len(trace) {
			actual[i] = formatTraceEntry(trace[i])
		}
		phaseWidth = max(phaseWidth, len(phases[i]))
		expectedWidth = max(expectedWidth, len(expected[i]))
	}
	for i := 0; i < length; i++ {
		// a phase that produced no decisions owns no row, so it is named at its boundary instead -
		// without it a phase can only ever be mentioned here by the phases either side of it.
		for _, mark := range marks {
			if mark.start == i && mark.end == i {
				fmt.Fprintf(&sb, "      --- %s: no decisions ---\n", mark.phase)
			}
		}
		marker := "  "
		if expected[i] != actual[i] {
			marker = "!!"
		}
		row := fmt.Sprintf("  %s %3d ", marker, i)
		if phaseWidth > 0 {
			row += fmt.Sprintf("%-*s  ", phaseWidth, phases[i])
		}
		fmt.Fprintf(&sb, "%sgolden: %-*s got: %s\n", row, expectedWidth, expected[i], actual[i])
	}

	fmt.Fprintf(&sb, "\nA failure here means a scheduling decision changed. If that change is intended and\n")
	fmt.Fprintf(&sb, "reviewed, regenerate with UPDATE_GOLDEN=1 and call out the trace diff in review. The\n")
	fmt.Fprintf(&sb, "regenerating run fails on purpose; re-run without UPDATE_GOLDEN to verify.\n")
	return sb.String()
}

// phaseOf names the workload phase that accounts for full-trace entry i. A phase owns the half-open
// range [start,end), so a phase that produced no decisions owns no entry and never names one - see
// traceMismatch, which calls those out at their boundary instead.
func phaseOf(marks []traceMark, i int) string {
	for _, mark := range marks {
		if i >= mark.start && i < mark.end {
			return mark.phase
		}
	}
	if len(marks) == 0 {
		return ""
	}
	return "after " + marks[len(marks)-1].phase
}

func formatTraceEntry(entry traceEntry) string {
	switch {
	case entry.Rejected:
		return fmt.Sprintf("reject %s of %s", entry.AllocationKey, entry.ApplicationID)
	case entry.TerminationType != "":
		return fmt.Sprintf("release %s of %s (%s)", entry.AllocationKey, entry.ApplicationID, entry.TerminationType)
	default:
		return fmt.Sprintf("alloc %s of %s on %s", entry.AllocationKey, entry.ApplicationID, entry.NodeID)
	}
}

func indentTrace(trace []traceEntry) string {
	var sb strings.Builder
	for i, entry := range trace {
		fmt.Fprintf(&sb, "  %3d %s\n", i, formatTraceEntry(entry))
	}
	return sb.String()
}

// TestGoldenTracePhaseAttribution pins how trace entries are attributed to workload phases, including
// the case the shared workload actually produces: phase 5 allocates and releases nothing, so it is a
// zero-width phase sitting between two phases that do produce decisions. It must not swallow the
// first entry of the phase after it, and the phase before it must not swallow the boundary.
func TestGoldenTracePhaseAttribution(t *testing.T) {
	marks := []traceMark{
		{phase: "one", start: 0, end: 2},
		{phase: "two", start: 2, end: 2}, // produced nothing
		{phase: "three", start: 2, end: 4},
	}
	assert.Equal(t, phaseOf(marks, 0), "one")
	assert.Equal(t, phaseOf(marks, 1), "one")
	assert.Equal(t, phaseOf(marks, 2), "three")
	assert.Equal(t, phaseOf(marks, 3), "three")
	assert.Equal(t, phaseOf(marks, 4), "after three")
	assert.Equal(t, phaseOf(nil, 0), "")

	// the zero-width phase is unnameable through phaseOf by construction, so the mismatch listing has
	// to name it at its boundary or it can never be mentioned at all.
	listing := traceMismatch("testdata/none.json", nil, []traceEntry{
		{ApplicationID: "app", AllocationKey: "a-1", NodeID: "node-1"},
		{ApplicationID: "app", AllocationKey: "a-2", NodeID: "node-1"},
		{ApplicationID: "app", AllocationKey: "a-3", NodeID: "node-1"},
		{ApplicationID: "app", AllocationKey: "a-4", NodeID: "node-1"},
	}, marks)
	assert.Assert(t, strings.Contains(listing, "--- two: no decisions ---"), "listing does not name the zero-width phase:\n%s", listing)
}
