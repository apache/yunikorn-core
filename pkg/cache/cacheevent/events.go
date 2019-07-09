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

package cacheevent

import (
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "github.com/cloudera/yunikorn-core/pkg/common/commonevents"
)

/******************/
/* Events from RM */
/******************/

type RMUpdateRequestEvent struct {
    Request *si.UpdateRequest
}

type RMRegistrationEvent struct {
    Registration  *si.RegisterResourceManagerRequest
    ResultChannel chan *commonevents.Result
}

/*************************/
/* Events from Scheduler */
/*************************/

// Allocation proposal include a list of Allocations/Releases
// They should be considered as an all-or-none bundle.
// If there're multiple allocations need to be allocated, and there's no
// relationship between these containers, they should be sent separately.
type AllocationProposalBundleEvent struct {
    PartitionName       string
    AllocationProposals []*commonevents.AllocationProposal
    ReleaseProposals    []*commonevents.ReleaseAllocation
}

type RejectedNewApplicationEvent struct {
    ApplicationId string
    Reason        string
}

type RemovedApplicationEvent struct {
    ApplicationId string
    PartitionName string
}

func NewReleaseAllocationEventFromProto(proto []*si.AllocationReleaseRequest) *ReleaseAllocationsEvent {
    event := &ReleaseAllocationsEvent{AllocationsToRelease: make([]*commonevents.ReleaseAllocation, 0)}

    for _, req := range proto {
        event.AllocationsToRelease = append(event.AllocationsToRelease, commonevents.NewReleaseAllocation(
            req.Uuid,
            req.ApplicationId,
            req.PartitionName,
            req.Message,
            si.AllocationReleaseResponse_STOPPED_BY_RM,
        ))
    }

    return event
}

type ReleaseAllocationsEvent struct {
    AllocationsToRelease []*commonevents.ReleaseAllocation
}
