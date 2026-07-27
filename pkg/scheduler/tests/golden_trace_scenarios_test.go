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
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// ---------------------------------------------------------------------------------------------
// TestGoldenDecisionTraceScenarios is the per-behaviour companion to TestGoldenDecisionTrace (see
// golden_trace_test.go). That test scripts one long mixed workload on a single fixed configuration;
// this one runs a set of short, independent workloads, each on its own configuration, so that a
// single scheduling decision is the only thing the resulting trace can be reporting on.
//
// Two things motivate the split. First, configuration coverage: TestGoldenDecisionTrace pins the
// default leaf application-sort policy and the default node-sort policy only. The sort policies are
// user-facing, configurable contracts - "binpacking" is documented as packing work onto loaded nodes
// and "fair" as spreading it - and nothing in the existing scheduler tests pins the observable,
// end-to-end consequence of picking one over the other. A refactor of the comparators, of node
// scoring, or of the order in which candidates are walked can leave the unit tests around those
// comparators green while changing where work actually lands.
//
// Second, diagnosability: policy pairs are captured as two goldens over an IDENTICAL workload, so the
// diff between the pair is precisely the policy's effect. When one member of a pair fails and the
// other does not, the failure has already been localised to that policy's decision path.
//
// The gang and preemption scenarios are deliberately narrower re-runs of behaviour that
// TestGoldenDecisionTrace also touches. There they are embedded in a long workload with accumulated
// queue and node state; here each runs on a fresh, exactly-sized cluster. If the shared trace breaks
// and these do not, the cause is interaction with prior state rather than the mechanism itself.
//
// Determinism follows the same rules as TestGoldenDecisionTrace: fixed strictly-increasing ask
// createTimes rather than wall-clock times, no background scheduling loop, an observable barrier
// between submitting asks and stepping the scheduler, and WARN-level logging.
// The candidate lists being sorted here are built by iterating Go maps, so every scenario is
// constructed to avoid ties in the comparator that decides it: application-sort ties are avoided by
// giving the competing applications distinct submission times and distinct usage, and node-sort score
// ties fall back to a NodeID comparison (nodeRef.Less), for which the node IDs below are chosen to
// order unambiguously.
//
// As with TestGoldenDecisionTrace, a failure here means a scheduling decision changed. Regenerate a
// golden with UPDATE_GOLDEN=1 only for a change that is intended, and call out the trace diff in
// review.
//
// The gang and preemption scenarios repeat, on a fresh cluster, the choreography that phases 3 and 6
// of TestGoldenDecisionTrace run against accumulated state. That repetition is not extracted into a
// helper the two files share, because a shared helper would be a single point of failure for both
// goldens: a mistake in it would move both traces together, and "the shared trace broke and the
// scenario did not, so the cause is interaction with prior state" - the whole reason these scenarios
// exist - would stop being a conclusion anyone could draw. What is shared between the two files is
// infrastructure that does not decide anything (building SI messages, the trace probe, the golden
// comparison), not the scripts themselves.
// ---------------------------------------------------------------------------------------------

// scenario application, queue and node identifiers, all fixed rather than time- or counter-derived so
// the traces are stable.
const (
	scLeaf = "root.leaf"
	// every scenario config carries this leaf, and no scenario ever submits work to it: it exists so
	// that traceProbe.sync can park its barrier applications outside the queue whose decision the
	// scenario is isolating. See sync() in golden_trace_test.go.
	scBarrier = "root.barrier"

	scAppEarly = "sc-app-early"
	scAppLate  = "sc-app-late"
	scAppPack  = "sc-app-pack"
	scAppGang  = "sc-app-gang"
	scAppLow   = "sc-app-low"
	scAppHigh  = "sc-app-high"

	// node IDs are chosen so that lexical order - the node-sort score tiebreak in nodeRef.Less - is
	// unambiguous: a sorts before b.
	scNode  = "sc-node:1"
	scNodeA = "sc-node-a:1"
	scNodeB = "sc-node-b:1"

	scQueueLow  = "root.plow"
	scQueueHigh = "root.phigh"
)

// goldenScenario is one behaviour: a scheduler configuration, a scripted workload, and the golden
// file its resulting decision trace is pinned to. The trace is read from ms.mockRM after run returns.
// barrierQueue names a leaf queue of config that traceProbe.sync can park its inert barrier
// applications in, and which the scenario itself never schedules in.
type goldenScenario struct {
	name         string
	goldenPath   string
	config       string
	barrierQueue string
	run          func(t *testing.T, ms *mockScheduler, seq *createTimeSeq, probe *traceProbe)
}

// leafSortConfig builds a partition whose measured leaf uses the given application.sort.policy. The
// barrier leaf alongside it deliberately carries no sort policy of its own: nothing is ever scheduled
// there, and giving it one would suggest otherwise.
func leafSortConfig(appSortPolicy string) string {
	return `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: leaf
            properties:
              application.sort.policy: ` + appSortPolicy + `
          - name: barrier
`
}

// nodeSortConfig builds a partition with the given nodesortpolicy type.
func nodeSortConfig(nodeSortPolicy string) string {
	return `
partitions:
  - name: default
    nodesortpolicy:
      type: ` + nodeSortPolicy + `
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: leaf
          - name: barrier
`
}

const scGangConfig = `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: leaf
          - name: barrier
`

// scPreemptConfig mirrors the preemption setup in TestGoldenDecisionTrace: preemption enabled, and the
// two sibling leaves carry explicit guaranteed resources, which preemption requires (see
// configs.DefaultPreemptionDelay: "guaranteed resources must be set to trigger preemption").
const scPreemptConfig = `
partitions:
  - name: default
    preemption:
      enabled: true
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: barrier
          - name: plow
            resources:
              guaranteed:
                memory: 0
                vcore: 0
              max:
                memory: 100000
                vcore: 100000
          - name: phigh
            resources:
              guaranteed:
                memory: 1000
                vcore: 1000
              max:
                memory: 100000
                vcore: 100000
`

func goldenScenarios() []goldenScenario {
	return []goldenScenario{
		{
			name:         "app-ordering-fifo",
			goldenPath:   "testdata/golden_trace_app_ordering_fifo.json",
			config:       leafSortConfig("fifo"),
			barrierQueue: scBarrier,
			run:          runAppOrdering,
		},
		{
			name:         "app-ordering-fair",
			goldenPath:   "testdata/golden_trace_app_ordering_fair.json",
			config:       leafSortConfig("fair"),
			barrierQueue: scBarrier,
			run:          runAppOrdering,
		},
		{
			name:         "node-binpacking",
			goldenPath:   "testdata/golden_trace_node_binpacking.json",
			config:       nodeSortConfig("binpacking"),
			barrierQueue: scBarrier,
			run:          runNodePlacement,
		},
		{
			name:         "node-fair",
			goldenPath:   "testdata/golden_trace_node_fair.json",
			config:       nodeSortConfig("fair"),
			barrierQueue: scBarrier,
			run:          runNodePlacement,
		},
		{
			name:         "gang",
			goldenPath:   "testdata/golden_trace_gang.json",
			config:       scGangConfig,
			barrierQueue: scBarrier,
			run:          runGang,
		},
		{
			name:         "preemption",
			goldenPath:   "testdata/golden_trace_preemption.json",
			config:       scPreemptConfig,
			barrierQueue: scBarrier,
			run:          runPreemption,
		},
	}
}

func TestGoldenDecisionTraceScenarios(t *testing.T) {
	useWarnLogging(t)

	for _, sc := range goldenScenarios() {
		t.Run(sc.name, func(t *testing.T) {
			seq := newCreateTimeSeq()

			ms := &mockScheduler{}
			defer ms.Stop()
			err := ms.Init(sc.config, false, false)
			assert.NilError(t, err, "RegisterResourceManager failed")
			// ms.Init resets the log level, see warnLogging.
			warnLogging()
			ms.mockRM.enableTrace()

			probe := newTraceProbe(t, ms, sc.barrierQueue)
			sc.run(t, ms, seq, probe)

			// each scenario is a single behaviour, so a mismatch needs no phase attribution.
			compareOrUpdateGoldenAt(t, sc.goldenPath, ms.mockRM.getTrace(), nil, probe.updating)
		})
	}
}

// runAppOrdering isolates the leaf queue's application-sort decision: when two applications in the
// same leaf both have work pending and only one can be served, which one goes first. Every ask is
// priority 5, so the applications always tie on priority - which is the leading comparison for both
// policies once priority sorting is on, and the trailing one when it is off - and the ordering is
// therefore decided by the policy's other key: submission time for FIFO
// (sortApplicationsByPriorityAndSubmissionTime), allocated-usage ratio for Fair
// (sortApplicationsByPriorityAndFairness).
//
// The node is first pre-loaded so that the two applications hold DIFFERENT amounts and their usage
// order DISAGREES with their submission order - otherwise both policies would pick the same winner
// and the trace would not distinguish them:
//   - sc-app-early submits first (smaller createTime, so the earlier submission time) and is
//     pre-loaded with 2 allocations, giving it the higher usage;
//   - sc-app-late submits second and is pre-loaded with 1 allocation, giving it the lower usage.
//
// Each application then submits one more ask, competing for a single remaining slot:
//   - FIFO: earlier submission wins -> sc-app-early (e-3).
//   - Fair: lower usage wins        -> sc-app-late  (l-2).
//
// The first three trace entries (e-1, e-2, l-1) are the shared, uncontested pre-load; the 4th entry is
// the contested slot, and is the single entry where the two goldens differ.
func runAppOrdering(t *testing.T, ms *mockScheduler, seq *createTimeSeq, probe *traceProbe) {
	// the node holds the 3-allocation pre-load (30 memory) plus exactly one contested slot (10) = 40/4.
	err := ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{goldenNodeInfo(scNode, 40, 4)},
		RmID:  goldenRMID,
	})
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedNode(t, scNode, goldenTimeout)

	err = ms.addApp(scAppEarly, scLeaf, goldenPart)
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedApplication(t, scAppEarly, goldenTimeout)
	err = ms.addApp(scAppLate, scLeaf, goldenPart)
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedApplication(t, scAppLate, goldenTimeout)

	// Pre-load step 1: only sc-app-early has pending asks, so both land without any inter-application
	// comparison being needed (e-1 then e-2, by createTime). An application's submission time is
	// lowered to the createTime of its earliest ask (Application.AddAllocationAsk), so this also fixes
	// sc-app-early's submission time to e-1's pinned createTime.
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			goldenAsk("e-1", scAppEarly, 5, 10, 1, seq.get()),
			goldenAsk("e-2", scAppEarly, 5, 10, 1, seq.get()),
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, scAppEarly, "e-1", "e-2")
	ms.scheduler.MultiStepSchedule(3)
	probe.assertLen(2, "after the first pre-load step")

	// Pre-load step 2: only sc-app-late has a pending ask, so l-1 also lands uncontested. sc-app-late's
	// submission time is now later than sc-app-early's, and its usage (10) lower than early's (20).
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			goldenAsk("l-1", scAppLate, 5, 10, 1, seq.get()),
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, scAppLate, "l-1")
	ms.scheduler.MultiStepSchedule(2)
	probe.assertLen(3, "after the second pre-load step")

	// Contested slot: one more ask from each application, but room for only one. The policy decides.
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			goldenAsk("e-3", scAppEarly, 5, 10, 1, seq.get()),
			goldenAsk("l-2", scAppLate, 5, 10, 1, seq.get()),
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, scAppEarly, "e-3")
	waitForAsks(t, ms, scAppLate, "l-2")
	ms.scheduler.MultiStepSchedule(2)
	probe.assertLen(4, "after the contested slot was decided")
}

// runNodePlacement isolates the partition node-sort decision: given more than one node that can hold
// an ask, which one is offered first. A single application submits 4 equal asks (25 memory / 25 vcore)
// against two identical, initially empty 100/100 nodes.
//
// Nodes are held in score order (baseNodeCollection, ordered by nodeRef.Less) and the policy supplies
// the score, with the iteration always running lowest score first:
//   - binpacking scores a node as the fraction of it still free, so the most-loaded node comes first
//     and all 4 asks pack onto one node until it is full: a1..a4 all on sc-node-a.
//   - fair scores a node as the fraction of it already used, so the least-loaded node comes first and
//     the asks alternate: a1 -> a, a2 -> b, a3 -> a, a4 -> b.
//
// Both nodes start identical, so the very first placement is a score tie and is settled by the NodeID
// tiebreak (sc-node-a before sc-node-b); every later placement is settled by score. Note that the two
// resulting traces differ only in the nodeID column - the allocation order is the same in both - which
// is exactly the kind of change an assertion on "was this ask allocated" cannot see.
func runNodePlacement(t *testing.T, ms *mockScheduler, seq *createTimeSeq, probe *traceProbe) {
	err := ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			goldenNodeInfo(scNodeA, 100, 100),
			goldenNodeInfo(scNodeB, 100, 100),
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedNode(t, scNodeA, goldenTimeout)
	ms.mockRM.waitForAcceptedNode(t, scNodeB, goldenTimeout)

	err = ms.addApp(scAppPack, scLeaf, goldenPart)
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedApplication(t, scAppPack, goldenTimeout)

	// four same-priority asks; strictly-increasing createTimes fix their order within the application
	// (Allocation.LessThan orders by priority descending then createTime ascending), so a1..a4 are
	// attempted in that order and the node column is the only free variable.
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{
			goldenAsk("a1", scAppPack, 5, 25, 25, seq.get()),
			goldenAsk("a2", scAppPack, 5, 25, 25, seq.get()),
			goldenAsk("a3", scAppPack, 5, 25, 25, seq.get()),
			goldenAsk("a4", scAppPack, 5, 25, 25, seq.get()),
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, scAppPack, "a1", "a2", "a3", "a4")

	ms.scheduler.MultiStepSchedule(5)
	probe.assertLen(4, "after all four asks were placed")
}

// runGang isolates the gang placeholder replacement handshake - placeholder allocated, RM asked to
// release it, real allocation issued once the RM confirms - on a cluster with no other state. The node
// is sized (20 memory / 2 vcore) to hold the placeholder and the real task simultaneously, which is
// what the replacement needs while both are briefly live.
func runGang(t *testing.T, ms *mockScheduler, seq *createTimeSeq, probe *traceProbe) {
	err := ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{goldenNodeInfo(scNode, 20, 2)},
		RmID:  goldenRMID,
	})
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedNode(t, scNode, goldenTimeout)

	err = ms.addApp(scAppGang, scLeaf, goldenPart)
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedApplication(t, scAppGang, goldenTimeout)

	phAsk := goldenAsk("g-placeholder", scAppGang, 6, 10, 1, seq.get())
	phAsk.TaskGroupName = "tg"
	phAsk.Placeholder = true
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{phAsk},
		RmID:        goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, scAppGang, "g-placeholder")
	ms.scheduler.MultiStepSchedule(2)
	probe.assertLen(1, "after allocating the gang placeholder")

	realAsk := goldenAsk("g-real", scAppGang, 6, 10, 1, seq.get())
	realAsk.TaskGroupName = "tg"
	realAsk.Placeholder = false
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{realAsk},
		RmID:        goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, scAppGang, "g-real")
	ms.scheduler.MultiStepSchedule(2)
	// the core notifies the RM the placeholder should be released.
	probe.assertLen(2, "after the core asked the RM to release the placeholder")

	placeholderGone := requireAllocationOnNode(t, ms, scNode, "g-placeholder")
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: []*si.AllocationRelease{
				createAllocationRelease(scAppGang, goldenPart, "g-placeholder", si.TerminationType_PLACEHOLDER_REPLACED),
			},
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	// no scheduling step completes the replacement: the confirmation is processed on the
	// allocation-event goroutine and ClusterContext.processAllocationReleases notifies the RM of the
	// real allocation directly.
	probe.assertLen(3, "after the replacement completed")
	placeholderGone()
}

// runPreemption isolates priority-driven preemption across sibling queues on an otherwise empty
// cluster. A single exactly-sized node is filled by two low-priority allocations (AllowPreemptSelf); a
// higher-priority ask (AllowPreemptOther) in the sibling queue cannot fit and triggers preemption of
// exactly one victim, after which the freed capacity lets the high-priority ask complete. The two
// low-priority asks carry distinct, pinned createTimes, which is the final tiebreak in
// sortVictimsForPreemption, so victim selection is deterministic rather than dependent on a tie.
func runPreemption(t *testing.T, ms *mockScheduler, seq *createTimeSeq, probe *traceProbe) {
	err := ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{goldenNodeInfo(scNode, 100, 100)},
		RmID:  goldenRMID,
	})
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedNode(t, scNode, goldenTimeout)

	err = ms.addApp(scAppLow, scQueueLow, goldenPart)
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedApplication(t, scAppLow, goldenTimeout)
	err = ms.addApp(scAppHigh, scQueueHigh, goldenPart)
	assert.NilError(t, err)
	ms.mockRM.waitForAcceptedApplication(t, scAppHigh, goldenTimeout)

	lowAsk1 := goldenAsk("low-1", scAppLow, 1, 50, 50, seq.get())
	lowAsk1.PreemptionPolicy = &si.PreemptionPolicy{AllowPreemptSelf: true}
	lowAsk2 := goldenAsk("low-2", scAppLow, 1, 50, 50, seq.get())
	lowAsk2.PreemptionPolicy = &si.PreemptionPolicy{AllowPreemptSelf: true}
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{lowAsk1, lowAsk2},
		RmID:        goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, scAppLow, "low-1", "low-2")
	ms.scheduler.MultiStepSchedule(3)
	// both low-priority allocations must land before the high-priority ask is introduced, or there is
	// nothing to preempt and the scenario proves nothing.
	probe.assertLen(2, "after filling the node")

	highAsk := goldenAsk("high-1", scAppHigh, 9, 50, 50, seq.get())
	highAsk.PreemptionPolicy = &si.PreemptionPolicy{AllowPreemptOther: true}
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: []*si.Allocation{highAsk},
		RmID:        goldenRMID,
	})
	assert.NilError(t, err)
	waitForAsks(t, ms, scAppHigh, "high-1")
	ms.scheduler.MultiStepSchedule(4)
	// preemption fires: the core notifies the RM to release the chosen victim.
	probe.assertLen(3, "after preemption chose a victim")

	victimKey := preemptionVictim(t, ms.mockRM.getTrace())
	victimGone := requireAllocationOnNode(t, ms, scNode, victimKey)

	// the RM confirms the preemption (mirrors the shim actually terminating the pod).
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: []*si.AllocationRelease{
				createAllocationRelease(scAppLow, goldenPart, victimKey, si.TerminationType_PREEMPTED_BY_SCHEDULER),
			},
		},
		RmID: goldenRMID,
	})
	assert.NilError(t, err)
	// the barrier between the RM's confirmation and the scheduling step that has to observe the freed
	// capacity: it establishes that the victim actually left, not merely that it is absent.
	victimGone()
	ms.scheduler.MultiStepSchedule(2)
	// freed capacity lets the reserved high-priority ask complete.
	probe.assertLen(4, "after the high-priority ask completed")
}
