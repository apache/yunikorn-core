/*
Copyright 2019 The Unity Scheduler Authors

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
    "github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/common/commonevents"
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
    AllocationProposals []*commonevents.AllocationProposal
    ReleaseProposals    []*ReleaseProposal
}

type ReleaseProposal struct {
    AllocationUuid int64
    Message        string
}

type RejectedNewJobEvent struct {
    JobId  string
    Reason string
}

type RemovedJobEvent struct {
    JobId         string
    PartitionName string
}

type ReleaseAllocationsEvent struct {
    AllocationsToRelease []*ReleaseAllocation
}

func NewReleaseAllocationEventFromProto(proto []*si.AllocationReleaseRequest) *ReleaseAllocationsEvent {
    event := &ReleaseAllocationsEvent{AllocationsToRelease: make([]*ReleaseAllocation, 0)}

    for _, req := range proto {
        event.AllocationsToRelease = append(event.AllocationsToRelease, &ReleaseAllocation{
            Uuid:          req.Uuid,
            JobId:         req.JobId,
            PartitionName: req.PartitionName,
            Message:       req.Message,
            ReleaseType:   si.AllocationReleaseResponse_STOPPED_BY_RM,
        })
    }

    return event
}

// Message from scheduler about release allocation
type ReleaseAllocation struct {
    // optional, when this is set, only release allocation by given uuid.
    Uuid string
    // when this is set, filter allocations by job id.
    // empty value will filter allocations don't belong to job.
    // when job id is set and uuid not set, release all allocations under the job id.
    JobId string
    // Which partition to release, required.
    PartitionName string
    // For human-readable
    Message string
    // Release type
    ReleaseType si.AllocationReleaseResponse_TerminationType
}

