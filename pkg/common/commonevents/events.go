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

type ConfigUpdateRMEvent struct {
    RmId    string
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
    ApplicationId     string
    QueueName         string
    AllocatedResource *resources.Resource
    AllocationKey     string
    Tags              map[string]string
    Priority          *si.Priority
    PartitionName     string
}

