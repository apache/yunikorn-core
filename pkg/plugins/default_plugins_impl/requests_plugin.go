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

package default_plugins_impl

import (
	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
)

var DefaultRequestsPluginInstance = &DefaultRequestsPlugin{}

// This is the default implementation of RequestsPlugins
type DefaultRequestsPlugin struct {
}

func (drp *DefaultRequestsPlugin) NewRequests() interfaces.Requests {
	return NewDefaultRequests()
}

// This is an implementation of Requests which keeps all requests in a map.
// It's not thread-safe, must be called while holding the lock of application.
type DefaultRequests struct {
	requests map[string]interfaces.Request
}

func NewDefaultRequests() interfaces.Requests {
	return &DefaultRequests{
		requests: make(map[string]interfaces.Request),
	}
}

func (drp *DefaultRequests) AddRequest(request interfaces.Request) interfaces.Request {
	if request == nil {
		return nil
	}
	allocKey := request.(interfaces.Request).GetAllocationKey()
	existingRequest := drp.requests[allocKey]
	drp.requests[allocKey] = request
	return existingRequest
}

func (drp *DefaultRequests) RemoveRequest(allocationKey string) interfaces.Request {
	existingRequest := drp.requests[allocationKey]
	if existingRequest != nil {
		delete(drp.requests, allocationKey)
	}
	return existingRequest
}

func (drp *DefaultRequests) GetRequest(allocationKey string) interfaces.Request {
	return drp.requests[allocationKey]
}

func (drp *DefaultRequests) GetRequests(filter func(request interfaces.Request) bool) []interfaces.Request {
	requests := make([]interfaces.Request, 0)
	for _, req := range drp.requests {
		if filter == nil || filter(req) {
			requests = append(requests, req)
		}
	}
	return requests
}

func (drp *DefaultRequests) Size() int {
	return len(drp.requests)
}

func (drp *DefaultRequests) Reset() {
	drp.requests = make(map[string]interfaces.Request)
}
