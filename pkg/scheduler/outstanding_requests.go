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

// an outstanding request is a request that fits into the queue capacity but in
// pending state because there is no enough partition resources to allocate it
type outstandingRequests struct {
	requests []*schedulingAllocationAsk
}

func newOutstandingRequests() *outstandingRequests {
	return &outstandingRequests{
		requests: make([]*schedulingAllocationAsk, 0),
	}
}

func (o *outstandingRequests) add(request *schedulingAllocationAsk) {
	o.requests = append(o.requests, request)
}

func (o *outstandingRequests) isEmpty() bool {
	return o.requests == nil || len(o.requests) == 0
}

func (o *outstandingRequests) getRequests() []*schedulingAllocationAsk {
	return o.requests
}
