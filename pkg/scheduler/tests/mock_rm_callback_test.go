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
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type mockRMCallback struct {
	MockResourceManagerCallback
	acceptedApplications map[string]bool
	rejectedApplications map[string]bool
	acceptedNodes        map[string]bool
	rejectedNodes        map[string]bool
	nodeAllocations      map[string][]*si.Allocation
	Allocations          map[string]*si.Allocation

	sync.RWMutex
}

// This is only exported to allow the use in the simple_example.go.
// Lint exclusion added as the non-export return is OK
func newMockRMCallbackHandler() *mockRMCallback {
	return &mockRMCallback{
		acceptedApplications: make(map[string]bool),
		rejectedApplications: make(map[string]bool),
		acceptedNodes:        make(map[string]bool),
		rejectedNodes:        make(map[string]bool),
		nodeAllocations:      make(map[string][]*si.Allocation),
		Allocations:          make(map[string]*si.Allocation),
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
	}
	return nil
}

func (m *mockRMCallback) UpdateAllocation(response *si.AllocationResponse) error {
	m.Lock()
	defer m.Unlock()
	for _, alloc := range response.New {
		m.Allocations[alloc.UUID] = alloc
		if val, ok := m.nodeAllocations[alloc.NodeID]; ok {
			val = append(val, alloc)
			m.nodeAllocations[alloc.NodeID] = val
		} else {
			nodeAllocations := make([]*si.Allocation, 0)
			nodeAllocations = append(nodeAllocations, alloc)
			m.nodeAllocations[alloc.NodeID] = nodeAllocations
		}
	}
	for _, alloc := range response.Released {
		delete(m.Allocations, alloc.UUID)
	}
	return nil
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
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		return m.acceptedApplications[appID]
	})
	if err != nil {
		tb.Fatalf("Failed to wait for accepted application: %s, called from: %s", appID, caller())
	}
}

func (m *mockRMCallback) waitForRejectedApplication(t *testing.T, appID string, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		return m.rejectedApplications[appID]
	})
	assert.NilError(t, err, "Failed to wait for rejected application: %s, called from: %s", appID, caller())
}

func (m *mockRMCallback) waitForAcceptedNode(t *testing.T, nodeID string, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		return m.acceptedNodes[nodeID]
	})
	assert.NilError(t, err, "Failed to wait for node state to become accepted: %s, called from: %s", nodeID, caller())
}

func (m *mockRMCallback) waitForMinAcceptedNodes(tb testing.TB, minNumNode int, timeoutMs int) {
	var numNodes int
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		numNodes = len(m.acceptedNodes)
		return numNodes >= minNumNode
	})
	if err != nil {
		tb.Fatalf("Failed to wait for min accepted nodes, expected %d, actual %d, called from: %s", minNumNode, numNodes, caller())
	}
}

func (m *mockRMCallback) waitForRejectedNode(t *testing.T, nodeID string, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		return m.rejectedNodes[nodeID]
	})
	assert.NilError(t, err, "Failed to wait for node state to become rejected: %s, called from: %s", nodeID, caller())
}

func (m *mockRMCallback) waitForAllocations(t *testing.T, nAlloc int, timeoutMs int) {
	var allocLen int
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		allocLen = len(m.Allocations)
		return allocLen == nAlloc
	})
	assert.NilError(t, err, "Failed to wait for allocations, expected %d, actual %d, called from: %s", nAlloc, allocLen, caller())
}

func (m *mockRMCallback) waitForMinAllocations(tb testing.TB, nAlloc int, timeoutMs int) {
	var allocLen int
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		m.RLock()
		defer m.RUnlock()
		allocLen = len(m.Allocations)
		return allocLen >= nAlloc
	})
	if err != nil {
		tb.Fatalf("Failed to wait for min allocations expected %d, actual %d, called from: %s", nAlloc, allocLen, caller())
	}
}
