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

package mock

import (
	"errors"
	"fmt"

	"github.com/apache/yunikorn-core/pkg/locking"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type PreemptionPredicatePlugin struct {
	ResourceManagerCallback
	reservations map[string]string
	allocations  map[string]string
	preemptions  []Preemption
	errHolder    *errHolder

	locking.RWMutex
}

type Preemption struct {
	expectedAllocationKey  string
	expectedNodeID         string
	expectedAllocationKeys []string
	expectedStartIndex     int32
	success                bool
	index                  int32
}

type errHolder struct {
	err error
}

func (m *PreemptionPredicatePlugin) Predicates(args *si.PredicatesArgs) error {
	m.RLock()
	defer m.RUnlock()
	if args.Allocate {
		nodeID, ok := m.allocations[args.AllocationKey]
		if !ok {
			return errors.New("no allocation found")
		}
		if nodeID != args.NodeID {
			return errors.New("wrong node")
		}
		return nil
	} else {
		nodeID, ok := m.reservations[args.AllocationKey]
		if !ok {
			return errors.New("no allocation found")
		}
		if nodeID != args.NodeID {
			return errors.New("wrong node")
		}
		return nil
	}
}

func (m *PreemptionPredicatePlugin) PreemptionPredicates(args *si.PreemptionPredicatesArgs) *si.PreemptionPredicatesResponse {
	m.Lock()
	defer m.Unlock()
	result := &si.PreemptionPredicatesResponse{
		Success: false,
		Index:   -1,
	}
	for _, preemption := range m.preemptions {
		if preemption.expectedAllocationKey != args.AllocationKey {
			continue
		}
		if preemption.expectedNodeID != args.NodeID {
			continue
		}
		if preemption.expectedStartIndex != args.StartIndex {
			m.errHolder.err = fmt.Errorf("unexpected start index exepected=%d, actual=%d, allocationKey=%s",
				preemption.expectedStartIndex, args.StartIndex, args.AllocationKey)
			return result
		}
		if len(preemption.expectedAllocationKeys) != len(args.PreemptAllocationKeys) {
			m.errHolder.err = fmt.Errorf("unexpected alloc key length expected=%d, actual=%d, allocationKey=%s",
				len(preemption.expectedAllocationKeys), len(args.PreemptAllocationKeys), args.AllocationKey)
			return result
		}
		for idx, key := range preemption.expectedAllocationKeys {
			if args.PreemptAllocationKeys[idx] != key {
				m.errHolder.err = fmt.Errorf("unexpected preempt alloc key expected=%s, actual=%s, index=%d, allocationKey=%s",
					args.PreemptAllocationKeys[idx], key, idx, args.AllocationKey)
				return result
			}
		}
		m.errHolder = &errHolder{}
		result.Success = preemption.success
		result.Index = preemption.index
		return result
	}
	m.errHolder.err = fmt.Errorf("no match found, allocationKey=%s, nodeID=%s", args.AllocationKey, args.NodeID)
	return result
}

// GetPredicateError returns the error set by the preemption predicate check that failed.
// Returns a nil error on success.
func (m *PreemptionPredicatePlugin) GetPredicateError() error {
	m.RLock()
	defer m.RUnlock()
	return m.errHolder.err
}

// NewPreemptionPredicatePlugin returns a mock plugin that can handle multiple predicate scenarios.
// reservations: provide a list of allocations and node IDs for which the reservation predicate succeeds
// allocs: provide a list of allocations and node IDs for which the allocation predicate succeeds
// preempt: a slice of preemption scenarios configured for the plugin to check
func NewPreemptionPredicatePlugin(reservations, allocs map[string]string, preempt []Preemption) *PreemptionPredicatePlugin {
	return &PreemptionPredicatePlugin{
		reservations: reservations,
		allocations:  allocs,
		preemptions:  preempt,
		errHolder:    &errHolder{},
	}
}

// NewPreemption returns a preemption scenario
// success: overall success state
// allocKey: allocation key for this preemption scenario
// nodeID: node for this preemption scenario
// expectedPreemptions: the allocations that should be in the preemption list
// start: the point at which to start the checks
// index: the index into the expectedPreemptions to return on success
func NewPreemption(success bool, allocKey, nodeID string, expectedPreemptions []string, start, index int32) Preemption {
	return Preemption{
		expectedAllocationKey:  allocKey,
		expectedNodeID:         nodeID,
		expectedAllocationKeys: expectedPreemptions,
		expectedStartIndex:     start,
		success:                success,
		index:                  index,
	}
}
