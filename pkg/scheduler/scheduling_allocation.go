/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
    "fmt"
    "github.com/cloudera/yunikorn-core/pkg/common/commonevents"
)

type AllocationResultType int

const (
    Allocation AllocationResultType = iota
    Reservation
    Unreserve
)

func (soe AllocationResultType) String() string {
    return [...]string{"Allocation", "Reservation", "Unreserve"}[soe]
}

type SchedulingAllocation struct {
    SchedulingAsk    *SchedulingAllocationAsk
    NumAllocation    int32
    Node             *SchedulingNode
    Releases         []*commonevents.ReleaseAllocation
    Application      *SchedulingApplication
    AllocationResult AllocationResultType // Is it a reservation?
}

func NewSchedulingAllocationFromReservationRequest(reservationRequest *ReservedSchedulingRequest) *SchedulingAllocation {
    return NewSchedulingAllocation(reservationRequest.SchedulingAsk, reservationRequest.SchedulingNode, reservationRequest.App, Reservation)
}

func NewSchedulingAllocation(ask *SchedulingAllocationAsk, node *SchedulingNode, app *SchedulingApplication, allocationResult AllocationResultType) *SchedulingAllocation {
    return &SchedulingAllocation{SchedulingAsk: ask, Node: node, NumAllocation: 1, Application: app, AllocationResult: allocationResult}
}

func (m *SchedulingAllocation) String() string {
    return fmt.Sprintf("{AllocatioKey=%s,NumAllocation=%d,Node=%s", m.SchedulingAsk.AskProto.AllocationKey, m.NumAllocation, m.Node.NodeId)
}
