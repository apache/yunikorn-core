package rmevent

import "github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"

type RMNewAllocationsEvent struct {
    RMId        string
    Allocations []*si.Allocation
}

type RMJobUpdateEvent struct {
    RMId         string
    AcceptedJobs []*si.AcceptedJob
    RejectedJobs []*si.RejectedJob
}

type RMRejectedAllocationAskEvent struct {
    RMId                   string
    RejectedAllocationAsks []*si.RejectedAllocationAsk
}

type RMReleaseAllocationEvent struct {
    RMId                string
    ReleasedAllocations []*si.AllocationReleaseResponse
}

type RMNodeUpdateEvent struct {
    RMId          string
    AcceptedNodes []*si.AcceptedNode
    RejectedNodes []*si.RejectedNode
}
