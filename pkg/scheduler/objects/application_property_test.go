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

package objects

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// TestApplicationPropertyFuzzHistogram drives an Application through a long randomized sequence
// of the real ask-management entry points (AddAllocationAsk, AllocateAsk, DeallocateAsk,
// RemoveAllocationAsk, RecoverAllocationAsk, AddAllocation, RollbackAllocation and FSM-to-Failed
// cleanup) and after every single step verifies that the incrementally maintained
// askMaxPriority/pendingPriorities bookkeeping matches a reference model rebuilt from a simple set
// of maps tracked alongside the application.
//
// The point is coverage of state combinations rather than of entry points: TestMaxAskPriority pins
// the handful of transitions that are easy to reason about by hand, while this test reaches the
// interleavings that are not - a replaced ask whose priority differs from the one it displaces, an
// attempted rollback of an allocation whose ask has already been dropped, an allocate that empties
// the top priority bucket while lower buckets still hold pending asks. Those are precisely the cases
// where incremental bookkeeping and a full rescan can disagree.
func TestApplicationPropertyFuzzHistogram(t *testing.T) {
	// A "ghost" rollback attempt (case 7 picking a confirmed allocation whose ask sa.requests no
	// longer holds) first needs a specific remove-then-release interleaving, but once one exists the
	// YUNIKORN-3360 guard rejects the rollback and so leaves the entry in sa.allocations for case 7 to
	// pick again - measured, every one of the 20 seeds below reaches the state a few hundred times.
	// The coverage floor is still asserted over the whole seed set rather than per seed so it keeps
	// holding if that accumulation stops: it only has to fail loudly if a change stops the fuzzer
	// reaching the state at all. Whether each individual attempt is correctly rejected is asserted in
	// case 7 itself, not here.
	totalGhostRollbackAttempts := 0
	for seed := int64(0); seed < 20; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			totalGhostRollbackAttempts += runPropertyFuzz(t, seed)
		})
	}
	assert.Assert(t, totalGhostRollbackAttempts > 0, "no seed ever attempted to roll back an ask missing from sa.requests")
}

// runPropertyFuzz executes one seeded run and returns the number of ghost rollbacks it attempted, so
// the caller can assert that coverage across the whole seed set.
func runPropertyFuzz(t *testing.T, seed int64) int { //nolint:funlen
	t.Helper()

	// Pin the Completing->Completed state timer to an effectively-infinite duration for the
	// duration of this run. enter_Completing (application_state.go) arms a real time.AfterFunc
	// timer (completingTimeout, default 30s) that - completely independently of this goroutine's
	// step loop - fires HandleApplicationEvent(CompleteApplication) and, on reaching Completed,
	// app.cleanupAsks() wipes sa.requests/sortedRequests/pendingPriorities out from under this
	// test. Under normal speed this fuzz loop finishes in a couple of seconds so the default 30s
	// would not fire, but on a loaded or CI machine it could - and if it does, the wipe is
	// invisible to the reference model,
	// producing exactly the kind of run-dependent, seed-varying divergence this test exists to
	// catch. Neutralizing the timer removes that residual, timing-dependent nondeterminism source
	// entirely rather than relying on the test finishing "fast enough".
	SetCompletingTimeout(time.Hour)
	defer SetCompletingTimeout(30 * time.Second) // restore the documented production default

	// AddAllocation (case 6) charges the confirmed allocation to the process-global user manager
	// (application.go addAllocationInternal -> incUserResourceUsage) and only RollbackAllocation
	// gives it back, so any allocation still confirmed when this run ends leaves usage attributed to
	// the shared getTestUserGroup() user. Sibling tests in this package assert exact user/group
	// totals (assertUserGroupResource in utilities_test.go), so hand the trackers back clean rather
	// than leaking this run's fuzz totals into whatever test happens to run next.
	defer setupUGM()

	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic PRNG is the point: reproducible fuzzing
	appID := fmt.Sprintf("fuzz-app-%d", seed)
	app := newApplication(appID, "default", "root.default")
	queue, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	app.queue = queue

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	// nextCreationTime hands out a strictly increasing, deterministic "creationTime" SI allocation
	// tag value to every ask this run constructs (see newFuzzAsk). Without this,
	// NewAllocationFromSI (allocation.go) falls back to time.Now() whenever the tag is absent,
	// which makes every Allocation's createTime - and therefore its position among LessThan ties in
	// sortedRequests - depend on wall-clock time instead of on the (deterministic) sequence of
	// operations this fuzzer performs. Pinning it removes that dependency so a fixed seed produces
	// bit-for-bit identical Allocation state on every run.
	nextCreationTime := int64(0)

	// keyPriority records the priority assigned to every ask key ever created (in AddAllocationAsk
	// or RecoverAllocationAsk); priority is immutable per ask for the lifetime of the key so this
	// map is only ever added to (or wiped wholesale on clear-all/cleanup), never mutated in place.
	keyPriority := make(map[string]int32)
	// pendingKeys is the reference model of what the pending histogram should contain: keys that are
	// currently pending (added or deallocated back to pending, and NOT yet allocated, removed, or
	// recovered-as-allocated).
	pendingKeys := make(map[string]bool)
	// allocatedKeys is the reference model of keys that are currently allocated (via AllocateAsk or
	// RecoverAllocationAsk) and not yet deallocated or removed.
	allocatedKeys := make(map[string]bool)
	// confirmedKeys mirrors sa.allocations: keys handed to AddAllocation and not yet removed from
	// sa.allocations again (only a successful RollbackAllocation does that here - application.go
	// ~line 907). Note that it is deliberately NOT wiped by the clear-all/Failed cleanup branches in
	// case 5: removeAsksInternal and cleanupAsks only ever touch sa.requests, so a stale entry does
	// survive in sa.allocations there and the model must say so too.
	confirmedKeys := make(map[string]bool)
	nextKey := 0

	// Coverage high-water marks: proof this fuzz run actually reaches non-trivial states rather
	// than passing trivially by never exercising the product. Checked at the end of this function.
	maxPending, maxAllocated, maxDistinctPendingPriorities := 0, 0, 0
	// successfulRollbacks counts RollbackAllocation calls that actually returned without error, i.e.
	// that really ran deallocateAsk. A rejected call (wrong app state, unknown key) mutates nothing,
	// so counting attempts would let this operation decay into a no-op unnoticed.
	successfulRollbacks := 0
	// ghostRollbackAttempts counts the case 7 calls that targeted an ask sa.requests no longer holds;
	// those never succeed (the YUNIKORN-3360 guard rejects them) so attempts, not successes, are what
	// there is to count - each one is individually asserted rejected in case 7, which is what makes
	// the count proof that the guard was exercised. replacedAsks counts the AddAllocationAsk calls
	// that took the replace-existing-ask branch (case 8). Both are narrow branches that a small change
	// to the candidate filters could stop reaching entirely, so both are asserted - replacedAsks per
	// run at the end of this function, ghostRollbackAttempts over the whole seed set by the caller.
	ghostRollbackAttempts := 0
	replacedAsks := 0

	const steps = 5000
	for i := 0; i < steps; i++ {
		switch rng.Intn(9) {
		case 0: // AddAllocationAsk
			key := fmt.Sprintf("ask-%d-%d", seed, nextKey)
			nextKey++
			priority := int32(rng.Intn(11) - 5) //nolint:gosec // bounded to -5..5, no overflow
			placeholder := rng.Intn(2) == 0
			nextCreationTime++
			var ask *Allocation
			if placeholder {
				ask = newFuzzAsk(key, appID, "tg-"+key, res, true, priority, "", nextCreationTime)
			} else {
				ask = newFuzzAsk(key, appID, "", res, false, priority, "", nextCreationTime)
			}
			addErr := app.AddAllocationAsk(ask)
			if addErr == nil {
				// unique keys every time, so this operation always takes the "brand new ask" path
				// through AddAllocationAsk; the replace-existing-ask branch is driven by case 8.
				keyPriority[key] = priority
				pendingKeys[key] = true
			}

		case 1: // AllocateAsk
			if len(pendingKeys) > 0 {
				key := pickRandomKey(rng, pendingKeys)
				if _, allocErr := app.AllocateAsk(key); allocErr == nil {
					delete(pendingKeys, key)
					allocatedKeys[key] = true
				}
			}
			// if pendingKeys is empty this step is a no-op; the invariant assertion below still
			// runs for uniform per-step coverage even though nothing changed.

		case 2: // DeallocateAsk
			if len(allocatedKeys) > 0 {
				key := pickRandomKey(rng, allocatedKeys)
				if _, deallocErr := app.DeallocateAsk(key); deallocErr == nil {
					delete(allocatedKeys, key)
					// the ask returns to pending at the SAME priority it was created with; priority
					// is immutable, so keyPriority already has the right value.
					pendingKeys[key] = true
				}
			}

		case 3: // RemoveAllocationAsk(key) - remove a single existing key
			if key, ok := pickRandomExistingKey(rng, keyPriority); ok {
				app.RemoveAllocationAsk(key)
				// removeAsksInternal deletes the key from sa.requests unconditionally regardless of
				// whether it was pending or allocated (application.go ~line 592-611: the pending
				// histogram/queue accounting is only adjusted `if !ask.IsAllocated()`, but the
				// sa.requests delete always happens) - so the reference model must drop the key from
				// every tracking map regardless of its prior pending/allocated state.
				delete(pendingKeys, key)
				delete(allocatedKeys, key)
				delete(keyPriority, key)
			}

		case 4: // RecoverAllocationAsk - build an already-allocated ask and recover it
			key := fmt.Sprintf("recovered-%d-%d", seed, nextKey)
			nextKey++
			priority := int32(rng.Intn(11) - 5) //nolint:gosec // bounded to -5..5, no overflow
			nextCreationTime++
			alloc := newFuzzAsk(key, appID, "", res, false, priority, "recovered-node", nextCreationTime)
			assert.Assert(t, alloc != nil, "NewAllocationFromSI unexpectedly returned nil")
			app.RecoverAllocationAsk(alloc)
			// a recovered ask is already allocated (NodeID set => allocated=true in
			// NewAllocationFromSI) so addAllocationAskInternal must NOT add it to the pending
			// histogram: record it only as allocated, never as pending. This is the exact invariant
			// this fuzz operation is probing.
			keyPriority[key] = priority
			allocatedKeys[key] = true

		case 5: // occasionally clear-all or fail-and-cleanup, otherwise another RemoveAllocationAsk-ish no-op
			switch rng.Intn(50) {
			case 0: // ~1-in-50 (of the ~1-in-8 chance for case 5): RemoveAllocationAsk("") clear-all
				app.RemoveAllocationAsk("")
				pendingKeys = make(map[string]bool)
				allocatedKeys = make(map[string]bool)
				keyPriority = make(map[string]int32)
			case 1: // ~1-in-50: attempt to drive to Failed (only if not already terminal), which
				// triggers cleanupAsks(). FailApplication is only a valid FSM transition from
				// New/Accepted/Running (-> Failing) and then from Failing (-> Failed)
				// (application_state.go eventDesc()). If the app is currently sitting in some other
				// non-terminal state - most notably Completing, which removeAsksInternal drives it
				// into once sa.pending AND sa.allocatedResource are both zero (application.go ~line
				// 635) - both calls below are silently rejected by the FSM, sa.requests is left
				// completely untouched, and cleanupAsks() never runs. Since case 6 confirms
				// allocations, sa.allocatedResource is no longer always zero: a confirmed allocation
				// now holds the app out of Completing on its own, so which of the two branches below
				// is taken depends on the confirm/rollback history as well as on the pending asks.
				// Only non-placeholder allocations are confirmed, so getPlaceholderAllocations() is
				// always empty and the hasPlaceHolderAllocations part of that condition never
				// changes the outcome. The previous version of this test wiped the reference model
				// unconditionally here, which diverged from the product whenever that rejection
				// happened: the model would claim empty while the product still held real
				// pending/allocated asks. Gating the wipe on the actual post-attempt state (the
				// operation's real result) keeps the model faithful to what the product actually did.
				// cleanupAsks() only clears sa.requests/sortedRequests/pendingPriorities, so
				// confirmedKeys (sa.allocations) is intentionally left alone by both wipes below.
				// The extra successfulRollbacks condition exists because Failed is an absorbing
				// state - application_state.go has no transition out of it other than Expire - and
				// RollbackAllocation refuses to do anything outside Accepted/Running, so a run that
				// fails at step 30 spends its remaining 4970 steps unable to exercise case 7 at all
				// (measured: seeds 7 and 12 failed at step 31/55 and then had every one of their
				// ~500 rollback attempts rejected on state). Holding the failure back until the
				// rollback path has run once costs no Failed coverage worth having: this branch
				// still comes up roughly every 400 steps afterwards.
				if successfulRollbacks > 0 && !app.IsFailed() {
					_ = app.HandleApplicationEvent(FailApplication) //nolint:errcheck // New/Accepted/Running -> Failing
					_ = app.HandleApplicationEvent(FailApplication) //nolint:errcheck // Failing -> Failed, runs cleanupAsks()
					if app.IsFailed() {
						pendingKeys = make(map[string]bool)
						allocatedKeys = make(map[string]bool)
						keyPriority = make(map[string]int32)
					}
				}
			default:
				// no-op filler so cases 5's sub-branches don't dominate step count; still asserted.
			}

		case 6: // AddAllocation - confirm an allocated ask so it lands in sa.allocations
			// RollbackAllocation (case 7) looks its target up in sa.allocations, not in sa.requests
			// (application.go ~line 868), so without a confirmation step there is nothing for it to
			// roll back and the whole operation would be dead code. Production confirms by handing
			// AddAllocation the very same *Allocation that already sits in sa.requests - either the
			// ask that AllocateAsk just flipped to allocated (partition.go ~line 1319-1336) or the
			// one RecoverAllocationAsk just added (~line 1263-1264) - so the fuzzer does the same and
			// passes app.GetAllocationAsk(key) rather than building a second Allocation for the same
			// key. That matters here: RollbackAllocation deallocates the object it found in
			// sa.allocations, and deallocateAsk only counts an ask as pending again when it still IS
			// the object sa.requests holds for that key - so a second Allocation built for the same
			// key would fail that identity check for a reason the product itself cannot produce,
			// making the assertions test fiction.
			if key, ok := pickRandomFilteredKey(rng, allocatedKeys, func(k string) bool {
				if confirmedKeys[k] {
					// already in sa.allocations: confirming the same key twice would add its
					// resource to sa.allocatedResource and to the user tracker a second time, which
					// no production path does.
					return false
				}
				// Placeholders take the other branch of addAllocationInternal (application.go ~line
				// 1926) which arms the placeholder execution timer through initPlaceholderTimer:
				// execTimeout is defaultPlaceholderTimeout for these apps (NewApplication ~line 174),
				// not zero, so the timer really is armed and timeoutPlaceholderProcessing can mutate
				// the state this test asserts on from another goroutine. That is exactly the
				// async-interference class the SetCompletingTimeout pin at the top of this function
				// removes, and confirming real allocations is all RollbackAllocation needs, so
				// placeholders are never confirmed.
				ask := app.GetAllocationAsk(k)
				return ask != nil && !ask.IsPlaceholder()
			}); ok {
				ask := app.GetAllocationAsk(key)
				assert.Assert(t, ask != nil, "seed=%d step=%d: allocated key %s missing from sa.requests", seed, i, key)
				app.AddAllocation(ask)
				// AddAllocation touches sa.allocations/allocatedResource and the app state only: it
				// leaves sa.requests, the pending histogram and sortedRequests alone, so the pending
				// and allocated reference sets are unchanged by design.
				confirmedKeys[key] = true
			}

		case 7: // RollbackAllocation - revert a confirmed allocation back to a pending ask
			// This is the new C1 caller under test: RollbackAllocation runs deallocateAsk
			// (application.go ~line 875), which is the function that does addToPriorities, so a
			// still-tracked ask has to reappear in the pending histogram at its original priority.
			// Candidates deliberately also include confirmed keys whose ask is no longer in
			// sa.requests at all. That "ghost" state IS reachable in production: sa.allocations is
			// only cleaned up once the shim confirms the release, so every path that drops asks while
			// leaving sa.allocations behind opens a window for a SCHEDULING_FAILED_ON_RM release to
			// still route to RollbackAllocation - RemoveAllocationAsk on an allocated key, or
			// removeAsksInternal("") from timeoutPlaceholderProcessing case 2, which is reached from
			// Running for a soft gang whose ResumeApplication transition is rejected (that event is
			// only valid from New/Accepted, application_state.go ~line 125) so the app stays Running
			// and remains rollback-eligible. Since YUNIKORN-3360, RollbackAllocation rejects such a
			// ghost before touching anything (application.go ~line 885: the ask has to be in
			// sa.requests), so the fuzzer asserts that rejection below, and the reference model
			// leaves the step as the no-op it now is.
			// The only combination excluded is a confirmed key that is still tracked but already
			// pending (deallocated without being removed): ask.deallocate() rejects that outright, so
			// picking it would only ever burn a step.
			if key, ok := pickRandomFilteredKey(rng, confirmedKeys, func(k string) bool {
				if allocatedKeys[k] {
					return true
				}
				// ghost: sa.requests no longer holds the ask, but sa.allocations still does
				_, tracked := keyPriority[k]
				return !tracked
			}); ok {
				_, tracked := keyPriority[key]
				_, rollbackErr := app.RollbackAllocation(key)
				if !tracked {
					// ghost: sa.requests no longer holds the ask. Since YUNIKORN-3360,
					// RollbackAllocation must reject this outright and mutate nothing.
					assert.Assert(t, rollbackErr != nil, "seed=%d step=%d: ghost rollback of %s was not rejected", seed, i, key)
					ghostRollbackAttempts++
				}
				if rollbackErr == nil {
					// only a successful call ran deallocateAsk: RollbackAllocation refuses outright
					// unless the app is Accepted or Running (application.go ~line 864) and bails out
					// before touching anything if the ask is no longer allocated, so a returned error
					// means the product state is unchanged and the model must be too.
					delete(confirmedKeys, key)
					delete(allocatedKeys, key)
					successfulRollbacks++
					// a successful rollback implies the ask was tracked (the 3360 guard rejects the
					// rest), so - same as DeallocateAsk (case 2) - it goes back to pending at its
					// immutable creation priority, which keyPriority already holds.
					pendingKeys[key] = true
				}
			}

		case 8: // AddAllocationAsk re-using an existing key - the replace-existing-ask branch
			// AddAllocationAsk (application.go ~line 668) handles a key that is already in
			// sa.requests and still pending separately: it has to unwind the old ask from the
			// pending histogram before addAllocationAskInternal counts the replacement, or the key
			// is counted twice and its old priority never drops out again. Case 0 only ever mints
			// fresh keys, so without this operation that branch is never executed here at all.
			if len(pendingKeys) > 0 {
				key := pickRandomKey(rng, pendingKeys)
				existing := app.GetAllocationAsk(key)
				assert.Assert(t, existing != nil, "seed=%d step=%d: pending key %s missing from sa.requests", seed, i, key)
				priority := int32(rng.Intn(11) - 5) //nolint:gosec // bounded to -5..5, no overflow
				nextCreationTime++
				// keep the placeholder/task-group shape of the ask being replaced: production
				// re-sends the same pod's ask, it never turns a placeholder into a regular ask.
				replacement := newFuzzAsk(key, appID, existing.GetTaskGroup(), res, existing.IsPlaceholder(), priority, "", nextCreationTime)
				if replaceErr := app.AddAllocationAsk(replacement); replaceErr == nil {
					// the replacement carries its OWN priority: the key stays pending but the model
					// must account for it at the new priority from here on.
					keyPriority[key] = priority
					replacedAsks++
				}
			}
		}

		distinctPriorities := assertFuzzInvariants(t, app, keyPriority, pendingKeys, seed, i)

		if len(pendingKeys) > maxPending {
			maxPending = len(pendingKeys)
		}
		if len(allocatedKeys) > maxAllocated {
			maxAllocated = len(allocatedKeys)
		}
		if distinctPriorities > maxDistinctPendingPriorities {
			maxDistinctPendingPriorities = distinctPriorities
		}
	}

	// Guard against this fuzz run passing trivially (e.g. because a change elsewhere caused every
	// operation to be rejected/no-op'd): confirm it actually drove the application through
	// non-trivial pending/allocated/multi-priority states, so a real regression in the product's
	// bookkeeping (e.g. removeFromPriorities) has states to be caught in.
	t.Logf("seed=%d coverage: maxPending=%d maxAllocated=%d maxDistinctPendingPriorities=%d successfulRollbacks=%d ghostRollbackAttempts=%d replacedAsks=%d", seed, maxPending, maxAllocated, maxDistinctPendingPriorities, successfulRollbacks, ghostRollbackAttempts, replacedAsks)
	assert.Assert(t, maxPending > 0, "seed=%d: fuzz run never observed any pending asks", seed)
	assert.Assert(t, maxAllocated > 0, "seed=%d: fuzz run never observed any allocated asks", seed)
	assert.Assert(t, maxDistinctPendingPriorities > 1, "seed=%d: fuzz run never observed a multi-priority pending histogram", seed)
	// RollbackAllocation is rejected outright unless the app is Accepted or Running, so a change that
	// leaves the app parked in some other state (or that stops case 6 producing confirmed
	// allocations) would silently turn case 7 into a no-op and stop covering deallocateAsk's new
	// caller entirely, while every assertion above kept passing.
	assert.Assert(t, successfulRollbacks > 0, "seed=%d: fuzz run never completed a RollbackAllocation", seed)
	// The replace-existing-ask branch (case 8) must not double count the key in the histogram. It is
	// only reached while pendingKeys is non-empty, so assert it really happened rather than trusting
	// that condition to keep holding. The ghost-rollback-attempt count is returned instead of asserted
	// here: whether a run reaches that state at all depends on the interleaving it happens to produce,
	// so the coverage floor for it is asserted over the whole seed set by
	// TestApplicationPropertyFuzzHistogram.
	assert.Assert(t, replacedAsks > 0, "seed=%d: fuzz run never took the replace-existing-ask branch", seed)

	return ghostRollbackAttempts
}

// newFuzzAsk builds an Allocation directly from an si.Allocation (bypassing the shared
// newAllocationAsk* test helpers in utilities_test.go, which leave the CreationTime tag unset and
// so fall back to time.Now() in NewAllocationFromSI). creationTime is a caller-supplied, strictly
// increasing logical clock value (see nextCreationTime in runPropertyFuzz) encoded into the
// si.AllocationTags[siCommon.CreationTime] tag, which NewAllocationFromSI parses back out as the
// Allocation's createTime - making construction order (and therefore sortedRequests tie-breaking)
// fully deterministic for a fixed seed instead of depending on wall-clock time.
func newFuzzAsk(key, appID, taskGroup string, res *resources.Resource, placeholder bool, priority int32, nodeID string, creationTime int64) *Allocation {
	alloc := &si.Allocation{
		AllocationKey:    key,
		ApplicationID:    appID,
		PartitionName:    "default",
		ResourcePerAlloc: res.ToProto(),
		TaskGroupName:    taskGroup,
		Placeholder:      placeholder,
		Priority:         priority,
		NodeID:           nodeID,
		AllocationTags:   map[string]string{siCommon.CreationTime: strconv.FormatInt(creationTime, 10)},
	}
	return NewAllocationFromSI(alloc)
}

// pickRandomKey returns a random key from a non-empty set of keys (map[string]bool).
func pickRandomKey(rng *rand.Rand, keys map[string]bool) string {
	list := make([]string, 0, len(keys))
	for k := range keys {
		list = append(list, k)
	}
	// sort for determinism: ranging a map yields a randomized order, so without this the same
	// rng seed would not reproduce the same operation sequence, defeating seed-based replay.
	sort.Strings(list)
	return list[rng.Intn(len(list))]
}

// pickRandomFilteredKey returns a random key from keys for which keep() returns true, or ok=false if
// none qualifies. It exists because the AddAllocation/RollbackAllocation operations are only defined
// on a subset of a tracking set (an allocated key that is not confirmed yet, a confirmed key that is
// still allocated); picking blindly and then dropping the step would make those operations fire far
// less often than their share of the step budget suggests.
// The candidate list is sorted before indexing for the same determinism reason as pickRandomKey.
func pickRandomFilteredKey(rng *rand.Rand, keys map[string]bool, keep func(string) bool) (string, bool) {
	list := make([]string, 0, len(keys))
	for k := range keys {
		if keep(k) {
			list = append(list, k)
		}
	}
	if len(list) == 0 {
		return "", false
	}
	sort.Strings(list)
	return list[rng.Intn(len(list))], true
}

// pickRandomExistingKey returns a random key from keyPriority (any key ever created that hasn't
// been fully removed yet), or ok=false if there are none.
func pickRandomExistingKey(rng *rand.Rand, keyPriority map[string]int32) (string, bool) {
	if len(keyPriority) == 0 {
		return "", false
	}
	list := make([]string, 0, len(keyPriority))
	for k := range keyPriority {
		list = append(list, k)
	}
	// sort for determinism: ranging a map yields a randomized order (Go randomizes map iteration
	// order per-process), so without this the same rng seed would pick a different key here on
	// different runs, cascading into a different overall operation sequence (which ask gets
	// removed determines the FSM state transitions that follow it) and defeating seed-based replay
	// - this was the residual nondeterminism source alongside pickRandomKey above.
	sort.Strings(list)
	return list[rng.Intn(len(list))], true
}

// assertFuzzInvariants rebuilds the expected pending-ask histogram and max from the reference model
// (keyPriority + pendingKeys) and compares it against the application's incrementally maintained
// state. On any mismatch it fails with the seed and step number embedded in the message so the
// failure is reproducible via `go test -run .../seed-<seed>`.
// It returns the number of distinct pending priorities observed this step, so the caller can track
// coverage high-water marks without a second pass over pendingKeys.
func assertFuzzInvariants(t *testing.T, app *Application, keyPriority map[string]int32, pendingKeys map[string]bool, seed int64, step int) int {
	t.Helper()

	wantHistogram := make(map[int32]int)
	wantMax := configs.MinPriority
	for k := range pendingKeys {
		p := keyPriority[k]
		wantHistogram[p]++
		if p > wantMax {
			wantMax = p
		}
	}

	app.RLock()
	gotMax := app.askMaxPriority
	gotHistogram := make(map[int32]int, len(app.pendingPriorities))
	for p, c := range app.pendingPriorities {
		gotHistogram[p] = c
	}
	app.RUnlock()

	assert.Equal(t, gotMax, wantMax, "seed=%d step=%d: askMaxPriority mismatch", seed, step)
	assert.Equal(t, len(gotHistogram), len(wantHistogram), "seed=%d step=%d: pendingPriorities histogram size mismatch, got=%v want=%v", seed, step, gotHistogram, wantHistogram)
	for p, wantCount := range wantHistogram {
		assert.Equal(t, gotHistogram[p], wantCount, "seed=%d step=%d: pendingPriorities[%d] mismatch, got histogram=%v want histogram=%v", seed, step, p, gotHistogram, wantHistogram)
	}

	return len(wantHistogram)
}
