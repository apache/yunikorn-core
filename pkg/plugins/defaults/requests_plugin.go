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

package defaults

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
	mapper *CommonMapper
}

func NewDefaultRequests() interfaces.Requests {
	return &DefaultRequests{
		mapper: NewCommonMapper(),
	}
}

func (dr *DefaultRequests) AddRequest(request interfaces.Request) interfaces.Request {
	if request == nil {
		return nil
	}
	allocKey := request.(interfaces.Request).GetAllocationKey()
	if existingRequest := dr.mapper.Add(allocKey, request); existingRequest != nil {
		return existingRequest.(interfaces.Request)
	}
	return nil
}

func (dr *DefaultRequests) RemoveRequest(allocationKey string) interfaces.Request {
	if removedRequest := dr.mapper.Remove(allocationKey); removedRequest != nil {
		return removedRequest.(interfaces.Request)
	}
	return nil
}

func (dr *DefaultRequests) GetRequest(allocationKey string) interfaces.Request {
	if request := dr.mapper.Get(allocationKey); request != nil {
		return request.(interfaces.Request)
	}
	return nil
}

func (dr *DefaultRequests) GetRequests(filter func(request interfaces.Request) bool) []interfaces.Request {
	requests := make([]interfaces.Request, 0)
	for _, req := range dr.mapper.GetItems() {
		if filter == nil || filter(req.(interfaces.Request)) {
			requests = append(requests, req.(interfaces.Request))
		}
	}
	return requests
}

func (dr *DefaultRequests) Size() int {
	return dr.mapper.Size()
}

func (dr *DefaultRequests) Reset() {
	dr.mapper.Reset()
}

func (dr *DefaultRequests) SortForAllocation() interfaces.RequestIterator {
	// filter pending requests
	sortedRequests := make([]interfaces.Request, 0)
	for _, request := range dr.mapper.GetItems() {
		if request.(interfaces.Request).GetPendingAskRepeat() > 0 {
			sortedRequests = append(sortedRequests, request.(interfaces.Request))
		}
	}
	// sort asks by priority
	SortAskByPriority(sortedRequests, false)
	return NewDefaultRequestIterator(sortedRequests)
}

func (dr *DefaultRequests) SortForPreemption() interfaces.RequestIterator {
	//TODO this should be implemented when refactoring the preemption process
	return nil
}

type DefaultRequestIterator struct {
	requests []interfaces.Request
	index    int
}

func NewDefaultRequestIterator(requests []interfaces.Request) *DefaultRequestIterator {
	return &DefaultRequestIterator{
		requests: requests,
		index:    0,
	}
}

func (dri *DefaultRequestIterator) HasNext() bool {
	return dri.index < len(dri.requests)
}

func (dri *DefaultRequestIterator) Next() interfaces.Request {
	if dri.index >= len(dri.requests) {
		return nil
	}
	obj := dri.requests[dri.index]
	dri.index++
	return obj
}

func (dri *DefaultRequestIterator) Size() int {
	return len(dri.requests)
}
