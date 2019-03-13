package commonevents

import (
    "github.com/universal-scheduler/scheduler-spec/lib/go/si"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/common/resources"
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

