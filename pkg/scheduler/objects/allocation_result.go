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

import "fmt"

type AllocationResultType int

const (
	None AllocationResultType = iota
	Allocated
	AllocatedReserved
	Reserved
	Unreserved
	Replaced
)

func (art AllocationResultType) String() string {
	return [...]string{"None", "Allocated", "AllocatedReserved", "Reserved", "Unreserved", "Replaced"}[art]
}

type AllocationResult struct {
	ResultType     AllocationResultType
	Request        *Allocation
	NodeID         string
	ReservedNodeID string
}

func (ar *AllocationResult) String() string {
	if ar == nil {
		return "nil allocation result"
	}
	allocationKey := ""
	if ar.Request != nil {
		allocationKey = ar.Request.GetAllocationKey()
	}
	return fmt.Sprintf("resultType=%s, nodeID=%s, reservedNodeID=%s, allocationKey=%s", ar.ResultType.String(), ar.NodeID, ar.ReservedNodeID, allocationKey)
}

// newAllocatedAllocationResult creates a new allocation result for a new allocation.
func newAllocatedAllocationResult(nodeID string, request *Allocation) *AllocationResult {
	return newAllocationResultInternal(Allocated, nodeID, request)
}

// newReservedAllocationResult creates a new allocation result for reserving a node.
func newReservedAllocationResult(nodeID string, request *Allocation) *AllocationResult {
	return newAllocationResultInternal(Reserved, nodeID, request)
}

// newUnreservedAllocationResult creates a new allocation result for unreserving a node.
func newUnreservedAllocationResult(nodeID string, request *Allocation) *AllocationResult {
	return newAllocationResultInternal(Unreserved, nodeID, request)
}

// newReplacedAllocationResult create a new allocation result for replaced allocations.
func newReplacedAllocationResult(nodeID string, request *Allocation) *AllocationResult {
	return newAllocationResultInternal(Replaced, nodeID, request)
}

// newAllocationResultInternal creates a new allocation result. It should not be called directly.
func newAllocationResultInternal(resultType AllocationResultType, nodeID string, request *Allocation) *AllocationResult {
	return &AllocationResult{
		ResultType: resultType,
		Request:    request,
		NodeID:     nodeID,
	}
}
