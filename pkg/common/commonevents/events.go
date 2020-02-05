/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

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
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type RemoveRMPartitionsEvent struct {
	RmID    string
	Channel chan *Result
}

type RegisterRMEvent struct {
	RMRegistrationRequest *si.RegisterResourceManagerRequest
	Channel               chan *Result
}

type ConfigUpdateRMEvent struct {
	RmID    string
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
	NodeID            string
	ApplicationID     string
	QueueName         string
	AllocatedResource *resources.Resource
	AllocationKey     string
	Tags              map[string]string
	Priority          *si.Priority
	PartitionName     string
}

// Message from scheduler about release allocation
type ReleaseAllocation struct {
	// optional, when this is set, only release allocation by given uuid.
	UUID string
	// when this is set, filter allocations by app id.
	// empty value will filter allocations don't belong to app.
	// when app id is set and uuid not set, release all allocations under the app id.
	ApplicationID string
	// Which partition to release, required.
	PartitionName string
	// For human-readable
	Message string
	// Release type
	ReleaseType si.AllocationReleaseResponse_TerminationType
}

func NewReleaseAllocation(uuid, appID, partitionName, message string, releaseType si.AllocationReleaseResponse_TerminationType) *ReleaseAllocation {
	return &ReleaseAllocation{
		UUID:          uuid,
		ApplicationID: appID,
		PartitionName: partitionName,
		Message:       message,
		ReleaseType:   releaseType,
	}
}
