package commonevents

import (
    "github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
)

type RemoveRMPartitionsEvent struct {
    RmId    string
    Channel chan *Result
}

type RegisterRMEvent struct {
    RMRegistrationRequest *si.RegisterResourceManagerRequest
    Channel chan *Result
}

type Result struct {
    Succeeded bool
    Reason    string
}

type EventHandler interface {
    HandleEvent(ev interface{})
}

type AllocationProposal struct {
    NodeId            string
    JobId             string
    QueueName         string
    AllocatedResource *resources.Resource
    AllocationKey     string
    Tags              map[string]string
    Priority          *si.Priority
    PartitionName     string
}

