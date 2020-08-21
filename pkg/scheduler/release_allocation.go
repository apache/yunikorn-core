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

package scheduler

import "github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"

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

func AllocationReleaseRequestToReleaseAllocation(request []*si.AllocationReleaseRequest, msg string, releaseType si.AllocationReleaseResponse_TerminationType) []*ReleaseAllocation {
	ret := make([]*ReleaseAllocation, 0)
	for _, r := range request {
		ret = append(ret, &ReleaseAllocation{
			UUID:          r.UUID,
			ApplicationID: r.ApplicationID,
			PartitionName: r.PartitionName,
			Message:       msg,
			ReleaseType:   releaseType,
		})
	}
	return ret
}
