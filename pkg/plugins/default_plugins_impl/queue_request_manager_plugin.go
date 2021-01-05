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

var DefaultQueueRequestManagerPluginInstance = &DefaultQueueRequestManagerPlugin{}

// This is the default implementation of QueueRequestManagerPlugin
type DefaultQueueRequestManagerPlugin struct {
}

func (dqrmp *DefaultQueueRequestManagerPlugin) NewQueueRequestManager(queue interface{}) interfaces.QueueRequestManager {
	return &DefaultQueueRequestManager{
		queue: queue.(interfaces.Queue),
	}
}

type DefaultQueueRequestManager struct {
	queue interfaces.Queue
}

func (dqrm *DefaultQueueRequestManager) SortForAllocation() interfaces.AppRequestIterator {
	// Sort applications based on the sort policy of queue
	apps := SortApplications(dqrm.queue.GetCopyOfApps(), dqrm.queue.GetSortType(),
		dqrm.queue.GetGuaranteedResource())
	// Lazy-sorting the requests for the app in order based on the priority of the request.
	// The sorted list only contains candidates that have an outstanding repeat.
	return NewDefaultAppRequestIterator(apps, pendingFilter, sortByPriorityDesc)
}

func pendingFilter(request interface{}) bool {
	return request.(interfaces.Request).GetPendingAskRepeat() > 0
}

func sortByPriorityDesc(requests []interfaces.Request) {
	SortAskByPriority(requests, false)
}

func (dqrm *DefaultQueueRequestManager) SortForPreemption() interfaces.AppRequestIterator {
	//TODO this should be implemented when refactoring the preemption process
	return nil
}

type DefaultAppRequestIterator struct {
	apps          []interfaces.Application
	index         int
	requestFilter func(request interface{}) bool
	requestSort   func(requests []interfaces.Request)
}

func NewDefaultAppRequestIterator(apps []interfaces.Application, requestFilter func(request interface{}) bool,
	requestSort func(requests []interfaces.Request)) interfaces.AppRequestIterator {
	return &DefaultAppRequestIterator{
		apps:          apps,
		index:         0,
		requestFilter: requestFilter,
		requestSort:   requestSort,
	}
}

func (dari *DefaultAppRequestIterator) HasNext() (ok bool) {
	return dari.index < len(dari.apps)
}

func (dari *DefaultAppRequestIterator) Next() (interfaces.Application, interfaces.RequestIterator) {
	if dari.index >= len(dari.apps) {
		return nil, nil
	}
	app := dari.apps[dari.index]
	dari.index++
	filter := func(request interfaces.Request) bool {
		return request.(interfaces.Request).GetPendingAskRepeat() > 0
	}
	sortedRequests := app.GetRequests(filter)
	if len(sortedRequests) > 0 {
		dari.requestSort(sortedRequests)
	}
	return app, NewDefaultRequestIterator(sortedRequests)
}

func (dari *DefaultAppRequestIterator) Size() int {
	return len(dari.apps)
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
