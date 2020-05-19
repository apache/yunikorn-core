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

package schedulerevent

import (
	"github.com/apache/incubator-yunikorn-core/pkg/common/commonevents"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// From Cache, update about allocations.
type SchedulerAllocationUpdatesEvent struct {
	RejectedAllocations []*commonevents.AllocationProposal
	AcceptedAllocations []*AcceptedAllocationProposal
	NewAsks             []*si.AllocationAsk
	ToReleases          *si.AllocationReleasesRequest
	ExistingAllocations []*si.Allocation // optional, only required during recovery
	RMId                string           // optional, only required during recovery
}

// each accepted allocation proposal is bound to one or more allocations
// at present, we assume there is always 1 allocation per proposal
type AcceptedAllocationProposal struct {
	AllocationProposal *commonevents.AllocationProposal
	Allocations        []*si.Allocation
}

type AllocationProposal struct {
	AllocationProposal *commonevents.AllocationProposal
	Allocations        []*si.Allocation
}

// From Cache, node updates.
type SchedulerNodeEvent struct {
	// Type is *cache.nodeInfo, avoid cyclic imports
	AddedNode interface{}
	// Type is *cache.nodeInfo, avoid cyclic imports
	RemovedNode interface{}
	// Type is *cache.nodeInfo, avoid cyclic imports
	UpdateNode interface{}
	// Resources that have been released via preemption
	PreemptedNodeResources []PreemptedNodeResource
}

// From Cache to scheduler, change in resources under preemption for the node
type PreemptedNodeResource struct {
	NodeID       string
	Partition    string
	PreemptedRes *resources.Resource
}

// From Cache, update about apps.
type SchedulerApplicationsUpdateEvent struct {
	// Type is *cache.ApplicationInfo, avoid cyclic imports
	AddedApplications   []interface{}
	RemovedApplications []*si.RemoveApplicationRequest
}

type SchedulerUpdatePartitionsConfigEvent struct {
	// Type is *cache.PartitionInfo, avoid cyclic imports
	UpdatedPartitions []interface{}
	ResultChannel     chan *commonevents.Result
}

type SchedulerDeletePartitionsConfigEvent struct {
	// Type is *cache.PartitionInfo, avoid cyclic imports
	DeletePartitions []interface{}
	ResultChannel    chan *commonevents.Result
}
