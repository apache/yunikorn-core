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

package interfaces

import (
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

// NodeIterator iterates over a list of nodes based on the defined policy
type NodeIterator interface {
	// returns true if there are more values to iterate over
	HasNext() bool
	// returns the next node from the iterator
	//
	Next() interface{}
	// reset the iterator to a clean state
	Reset()
}

// This interface helps to manage all application for a queue.
type Applications interface {
	// add or update an app,
	// return old app if present, otherwise return nil.
	AddApplication(app Application) Application
	// remove the app with the specified allocation key,
	// return removed request if present, otherwise return nil.
	RemoveApplication(appID string) Application
	// find the app with the specified appID,
	// return matched app if present, otherwise return nil.
	GetApplication(appID string) Application
	// find requests which are matched by the specified filter,
	// return a slice of matched requests (empty slice if not present).
	GetApplications(filter func(app Application) bool) []Application
	// return the size of all application
	Size() int
	// reset to a clean state
	Reset()

	// Return a iterator of sorted applications in the queue for allocation.
	// Applications are sorted using the sorting type of the queue.
	// Only applications with a pending resource request are considered.
	SortForAllocation() AppIterator
	// Return a iterator of sorted applications in the queue for preemption.
	// Applications are sorted using the sorting type of the queue.
	// Only applications with a allocated resource request are considered.
	SortForPreemption() AppIterator
}

// This interface helps to manage all requests for an application.
type Requests interface {
	// add or update a request,
	// return old request if present, otherwise return nil.
	AddRequest(request Request) Request
	// remove the request with the specified allocation key,
	// return removed request if present, otherwise return nil.
	RemoveRequest(allocationKey string) Request
	// find the request with the specified allocation key,
	// return matched request if present, otherwise return nil.
	GetRequest(allocationKey string) Request
	// find requests which are matched by the specified filter,
	// return a slice of matched requests (empty slice if not present).
	GetRequests(filter func(request Request) bool) []Request
	// return the size of all requests
	Size() int
	// reset to a clean state
	Reset()

	// Return a iterator of sorted requests in the application for allocation.
	// Only pending requests are considered.
	SortForAllocation() RequestIterator
	// Return a iterator of sorted requests in the application for preemption.
	// Only allocated requests are considered.
	SortForPreemption() RequestIterator
}

// This interface helps to iterate over a list of requests belong to the same app.
type RequestIterator interface {
	// return true if there is more request to iterate over
	HasNext() (ok bool)
	// return the next request
	Next() Request
	// return the size of elements
	Size() int
}

// This interface helps to iterate over a list of applications and their pending requests.
type AppIterator interface {
	// return true if there is more app to iterate over
	HasNext() (ok bool)
	// return the next app and request iterator
	Next() Application
	// return the size of elements
	Size() int
}

// This interface is used by scheduler plugins to get information of a queue
type Queue interface {
	// get the guaranteed resource
	GetGuaranteedResource() *resources.Resource
	// get the pending resources
	GetPendingResource() *resources.Resource
	// get the allocated resources
	GetAllocatedResource() *resources.Resource
	// get a copy of all apps
	GetCopyOfApps() map[string]Application
	// get the queue sort type
	GetSortType() policies.SortPolicy
}

// This interface is used by scheduler plugins to get information of an application
type Application interface {
	// get the application ID
	GetApplicationID() string
	// get the pending resource
	GetPendingResource() *resources.Resource
	// get the allocated resource
	GetAllocatedResource() *resources.Resource
	// get the submission time
	GetSubmissionTime() time.Time
	// get or check current state of the application
	CurrentState() string
	// get required requests via the specified filter,
	// return all requests when the specified filter is nil.
	GetRequests(filter func(request Request) bool) []Request
	// Return requests wrapper, may be used by custom plugins to enhance flexibility.
	// The caller should guarantee the consistence of requests to avoid race-condition
	// via holding application lock or based on an implementation of Requests with internal lock.
	GetRequestsWrapper() Requests
}

// This interface is used by scheduler plugins to get information of a request
type Request interface {
	// get the allocation key
	GetAllocationKey() string
	// get the pending ask repeat
	GetPendingAskRepeat() int32
	// get the priority
	GetPriority() int32
	// get the create time
	GetCreateTime() time.Time
}
