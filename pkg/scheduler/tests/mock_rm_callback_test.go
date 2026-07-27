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
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/mock"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// traceEntry is a single, append-only observation of a scheduling decision as delivered to the
// RM via UpdateAllocation. It is one of three kinds:
//   - a "new allocation" entry: ApplicationID/AllocationKey/NodeID set, TerminationType empty;
//   - a "release" entry: ApplicationID/AllocationKey/TerminationType set, NodeID empty;
//   - a "rejection" entry: ApplicationID/AllocationKey/Rejected set, the rest empty.
//
// See golden_trace_test.go for how this is used to build a golden decision-trace.
//
// All three kinds carry the application ID because an allocation key only identifies the work, not
// whose work it is: a regression that releases the right key against the wrong application - which is
// exactly what a mistake in preemption victim selection or in placeholder replacement looks like -
// is invisible in a trace that records the key alone. The partition is deliberately NOT recorded:
// every workload here runs on the single "default" partition, so the column would be constant, and
// its value is the normalised name, which would write the RM ID into every golden line.
//
// A rejection records that the core refused the allocation, not why. The reason
// (si.RejectedAllocation.Reason) is a free-text message built from whatever error the partition
// returned, so pinning it in a golden would turn any rewording of an error string into a failed
// decision-regression test - which is the opposite of what these goldens are for.
type traceEntry struct {
	ApplicationID   string `json:"applicationID,omitempty"`
	AllocationKey   string `json:"allocationKey"`
	NodeID          string `json:"nodeID,omitempty"`
	TerminationType string `json:"terminationType,omitempty"`
	Rejected        bool   `json:"rejected,omitempty"`
}

type mockRMCallback struct {
	mock.ResourceManagerCallback
	acceptedApplications map[string]bool
	rejectedApplications map[string]bool
	acceptedNodes        map[string]bool
	rejectedNodes        map[string]bool
	nodeAllocations      map[string][]*si.Allocation
	Allocations          map[string]*si.Allocation
	releasedPhs          map[string]*si.AllocationRelease
	appStates            map[string]string

	// traceEnabled turns the capture below on. It is off by default because this mock is shared with
	// every other test in the package, including BenchmarkScheduling, which drives 10k allocations
	// per run through this callback: capturing them would add an append per allocation, inside the
	// callback's write lock, on the path being measured. Only the golden-trace tests call
	// enableTrace.
	traceEnabled bool

	// trace is an ordered, append-only record of every allocation, release and rejection observed via
	// UpdateAllocation, in the exact order the RMProxy delivered them. RMProxy.handleRMEvents
	// (rmproxy.go) drains its pending event queue from a single goroutine, so the callbacks arrive
	// in the order the events were enqueued - but enqueue order is not by itself decision order,
	// because events reach that queue from more than one goroutine (the scheduling goroutine and the
	// allocation-event goroutine both enqueue). What makes the captured order the decision order is
	// the workload: the golden-trace tests advance scheduling from a single goroutine calling
	// MultiStepSchedule and drain both queues between batches (traceProbe.sync), so at most one batch
	// of decisions is ever in flight. Used by golden_trace_test.go for the golden-trace test.
	trace []traceEntry

	locking.RWMutex
}

func newMockRMCallbackHandler() *mockRMCallback {
	return &mockRMCallback{
		acceptedApplications: make(map[string]bool),
		rejectedApplications: make(map[string]bool),
		acceptedNodes:        make(map[string]bool),
		rejectedNodes:        make(map[string]bool),
		nodeAllocations:      make(map[string][]*si.Allocation),
		Allocations:          make(map[string]*si.Allocation),
		releasedPhs:          make(map[string]*si.AllocationRelease),
		appStates:            make(map[string]string),
	}
}

func (m *mockRMCallback) UpdateApplication(response *si.ApplicationResponse) error {
	m.Lock()
	defer m.Unlock()
	for _, app := range response.Accepted {
		m.acceptedApplications[app.ApplicationID] = true
		delete(m.rejectedApplications, app.ApplicationID)
	}
	for _, app := range response.Rejected {
		m.rejectedApplications[app.ApplicationID] = true
		delete(m.acceptedApplications, app.ApplicationID)
		delete(m.appStates, app.ApplicationID)
	}
	for _, app := range response.Updated {
		m.appStates[app.ApplicationID] = app.State
	}
	return nil
}

func (m *mockRMCallback) UpdateAllocation(response *si.AllocationResponse) error {
	m.Lock()
	defer m.Unlock()
	for _, alloc := range response.New {
		m.Allocations[alloc.AllocationKey] = alloc
		if val, ok := m.nodeAllocations[alloc.NodeID]; ok {
			val = append(val, alloc)
			m.nodeAllocations[alloc.NodeID] = val
		} else {
			nodeAllocations := make([]*si.Allocation, 0)
			nodeAllocations = append(nodeAllocations, alloc)
			m.nodeAllocations[alloc.NodeID] = nodeAllocations
		}
		if m.traceEnabled {
			m.trace = append(m.trace, traceEntry{
				ApplicationID: alloc.ApplicationID,
				AllocationKey: alloc.AllocationKey,
				NodeID:        alloc.NodeID,
			})
		}
	}
	for _, alloc := range response.Released {
		delete(m.Allocations, alloc.AllocationKey)
		if alloc.TerminationType == si.TerminationType_PLACEHOLDER_REPLACED {
			m.releasedPhs[alloc.AllocationKey] = alloc
		}
		if m.traceEnabled {
			m.trace = append(m.trace, traceEntry{
				ApplicationID:   alloc.ApplicationID,
				AllocationKey:   alloc.AllocationKey,
				TerminationType: alloc.TerminationType.String(),
			})
		}
	}
	// Rejections arrive on this same callback, from RMProxy.processRMRejectedAllocationEvent. They are
	// captured because a decision trace that records only what WAS allocated cannot tell a rejected
	// ask apart from one that was simply never scheduled: both show up as a missing row, and those two
	// are different regressions - a quota or validation change against an ordering change. Recorded as
	// an entry rather than checked here because this callback runs on the RMProxy's event goroutine,
	// where a failed assertion could not stop the test anyway; as a trace entry it lands in the golden
	// comparison and in the phase-length barriers, both of which run on the test goroutine.
	for _, alloc := range response.RejectedAllocations {
		if m.traceEnabled {
			m.trace = append(m.trace, traceEntry{
				ApplicationID: alloc.ApplicationID,
				AllocationKey: alloc.AllocationKey,
				Rejected:      true,
			})
		}
	}
	return nil
}

// enableTrace turns on decision-trace capture for this mock. Called by the golden-trace tests only,
// see the traceEnabled field.
func (m *mockRMCallback) enableTrace() {
	m.Lock()
	defer m.Unlock()
	m.traceEnabled = true
}

// getTrace returns a copy of the ordered decision trace captured so far. Safe for concurrent use.
func (m *mockRMCallback) getTrace() []traceEntry {
	m.RLock()
	defer m.RUnlock()

	trace := make([]traceEntry, len(m.trace))
	copy(trace, m.trace)
	return trace
}

// getNodeAllocations returns the allocations this RM was told about for the given node.
// Note that unlike Allocations this history is append-only: nodeAllocations is never pruned when an
// allocation is released, so it is a record of what was placed on the node rather than of what is
// still live there.
//
// The returned slice is a fresh slice, so it can be walked while the mock keeps recording, but its
// elements are the *si.Allocation pointers the mock was handed, not copies: they are shared with the
// caller's other readers and must be treated as read-only.
func (m *mockRMCallback) getNodeAllocations(nodeID string) []*si.Allocation {
	m.RLock()
	defer m.RUnlock()

	allocations := make([]*si.Allocation, len(m.nodeAllocations[nodeID]))
	copy(allocations, m.nodeAllocations[nodeID])
	return allocations
}

func (m *mockRMCallback) UpdateNode(response *si.NodeResponse) error {
	m.Lock()
	defer m.Unlock()
	for _, node := range response.Accepted {
		m.acceptedNodes[node.NodeID] = true
		delete(m.rejectedNodes, node.NodeID)
	}
	for _, node := range response.Rejected {
		m.rejectedNodes[node.NodeID] = true
		delete(m.acceptedNodes, node.NodeID)
	}
	return nil
}

func (m *mockRMCallback) getAllocations() map[string]*si.Allocation {
	m.RLock()
	defer m.RUnlock()

	allocations := make(map[string]*si.Allocation)
	for key, value := range m.Allocations {
		allocations[key] = value
	}
	return allocations
}

func (m *mockRMCallback) waitForAcceptedApplication(tb testing.TB, appID string, timeoutMs int) {
	err := common.WaitForCondition(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		return m.acceptedApplications[appID]
	})
	if err != nil {
		tb.Fatalf("Failed to wait for accepted application: %s, called from: %s", appID, caller())
	}
}

func (m *mockRMCallback) waitForRejectedApplication(t *testing.T, appID string, timeoutMs int) {
	err := common.WaitForCondition(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		return m.rejectedApplications[appID]
	})
	assert.NilError(t, err, "Failed to wait for rejected application: %s, called from: %s", appID, caller())
}

func (m *mockRMCallback) waitForApplicationState(t *testing.T, appID, state string, timeoutMs int) {
	err := common.WaitForCondition(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		return m.appStates[appID] == state
	})
	assert.NilError(t, err, "Failed to wait for application %s state: %s, called from: %s", appID, state, caller())
}

func (m *mockRMCallback) waitForAcceptedNode(t *testing.T, nodeID string, timeoutMs int) {
	err := common.WaitForCondition(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		return m.acceptedNodes[nodeID]
	})
	assert.NilError(t, err, "Failed to wait for node state to become accepted: %s, called from: %s", nodeID, caller())
}

func (m *mockRMCallback) waitForMinAcceptedNodes(tb testing.TB, minNumNode int, timeoutMs int) {
	var numNodes int
	err := common.WaitForCondition(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		numNodes = len(m.acceptedNodes)
		return numNodes >= minNumNode
	})
	if err != nil {
		tb.Fatalf("Failed to wait for min accepted nodes, expected %d, actual %d, called from: %s", minNumNode, numNodes, caller())
	}
}

func (m *mockRMCallback) waitForAllocations(t *testing.T, nAlloc int, timeoutMs int) {
	var allocLen int
	err := common.WaitForCondition(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		allocLen = len(m.Allocations)
		return allocLen == nAlloc
	})
	assert.NilError(t, err, "Failed to wait for allocations, expected %d, actual %d, called from: %s", nAlloc, allocLen, caller())
}

func (m *mockRMCallback) waitForMinAllocations(tb testing.TB, nAlloc int, timeoutMs int) {
	var allocLen int
	err := common.WaitForCondition(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		allocLen = len(m.Allocations)
		return allocLen >= nAlloc
	})
	if err != nil {
		tb.Fatalf("Failed to wait for min allocations expected %d, actual %d, called from: %s", nAlloc, allocLen, caller())
	}
}

func (m *mockRMCallback) waitForReleasedPlaceholders(t *testing.T, releases int, timeoutMs int) {
	var releasesLen int
	err := common.WaitForCondition(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		releasesLen = len(m.releasedPhs)
		return releasesLen == releases
	})
	assert.NilError(t, err, "Failed to wait for placeholder releases, expected %d, actual %d, called from: %s", releases, releasesLen, caller())
}
