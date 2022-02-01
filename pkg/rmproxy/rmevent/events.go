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

package rmevent

import (
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// Incoming UpdateAllocation events from the RM to the scheduler (async)
type RMUpdateAllocationEvent struct {
	// The generic UpdateAllocation does not wait for a result,
	// results are communicated back via the outgoing events.
	Request *si.AllocationRequest
}

// Incoming UpdateApplication events from the RM to the scheduler (async)
type RMUpdateApplicationEvent struct {
	// The generic UpdateApplication does not wait for a result,
	// results are communicated back via the outgoing events.
	Request *si.ApplicationRequest
}

// Incoming UpdateNode events from the RM to the scheduler (async)
type RMUpdateNodeEvent struct {
	// The generic UpdateNode does not wait for a result,
	// results are communicated back via the outgoing events.
	Request *si.NodeRequest
}

// Incoming events from the RM to the scheduler (sync)
type RMRegistrationEvent struct {
	Registration *si.RegisterResourceManagerRequest
	Channel      chan *Result
}

type RMConfigUpdateEvent struct {
	RmID    string
	Channel chan *Result
}

type RMPartitionsRemoveEvent struct {
	RmID    string
	Channel chan *Result
}

type Result struct {
	Succeeded bool
	Reason    string
}

// Outgoing events from the scheduler to the RM
// These events communicate the response to the RMUpdateRequestEvent
type RMNewAllocationsEvent struct {
	RmID        string
	Allocations []*si.Allocation
}

type RMApplicationUpdateEvent struct {
	RmID                 string
	AcceptedApplications []*si.AcceptedApplication
	RejectedApplications []*si.RejectedApplication
	UpdatedApplications  []*si.UpdatedApplication
}

type RMRejectedAllocationAskEvent struct {
	RmID                   string
	RejectedAllocationAsks []*si.RejectedAllocationAsk
}

type RMReleaseAllocationEvent struct {
	RmID                string
	ReleasedAllocations []*si.AllocationRelease
	QueueNames          []string // probably should be in AllocationRelease, adding here temporarily
}

type RMReleaseAllocationAskEvent struct {
	RmID                   string
	ReleasedAllocationAsks []*si.AllocationAskRelease
}

type RMNodeUpdateEvent struct {
	RmID          string
	AcceptedNodes []*si.AcceptedNode
	RejectedNodes []*si.RejectedNode
}
