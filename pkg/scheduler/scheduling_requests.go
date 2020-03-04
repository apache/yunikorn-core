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

import (
    "github.com/apache/incubator-yunikorn-core/pkg/common"
    "time"
)

// This interface is intended for managing all requests and priority groups
type Requests interface {
    // add or update a request,
    // return old request if present, otherwise return nil.
    AddRequest(request *schedulingAllocationAsk) *schedulingAllocationAsk
    // remove the request with the specified allocation key,
    // return removed request if present, otherwise return nil.
    RemoveRequest(allocationKey string) *schedulingAllocationAsk
    // find the request with the specified allocation key,
    // return matched request if present, otherwise return nil.
    GetRequest(allocationKey string) *schedulingAllocationAsk
    // return the size of all requests
    Size() int
    // reset to a clean state
    Reset()
    // return top pending priority group
    GetTopPendingPriorityGroup() PriorityGroup
    // return iterator for all pending requests
    GetPendingRequestIterator() RequestIterator
}

// This interface is intended for getting useful information from priority groups
type PriorityGroup interface {
    // add or update a request
    AddRequest(request *schedulingAllocationAsk)
    // remove the request with the specified allocation key
    RemoveRequest(allocationKey string)
    // return the priority
    GetPriority() int32
    // return the create time
    GetCreateTime() time.Time
    // return the size of requests
    Size() int
    // return the pending state
    IsPending() bool
    // return requests iterator for pending requests
    GetPendingRequestIterator() RequestIterator
    // return requests iterator for all requests
    GetRequestIterator() RequestIterator
}

// This interface helps to iterate over a list of requests
type RequestIterator interface {
    // return true if there are more requests to iterate over
    HasNext() (ok bool)
    // return the next request from the iterator
    Next() (request *schedulingAllocationAsk)
}

// This is an implementation of Requests with two-level sorted structure,
// the first level has a map of all requests and priority groups in the descending order by priority.
// It's not thread-safe, must be called while holding the lock.
type SortedRequests struct {
    // a map of all requests
    requests map[string]*schedulingAllocationAsk
    // sorted priority groups in the descending order by priority
    sortedPriorityGroups *common.SortableLinkedMap
}

func NewSortedRequests() *SortedRequests {
    return &SortedRequests{
        requests:             make(map[string]*schedulingAllocationAsk),
        sortedPriorityGroups: common.NewSortableLinkedMap(func(i, j interface{}) bool {
            return i.(PriorityGroup).GetPriority() > j.(PriorityGroup).GetPriority()
        }, true, func(value interface{}) bool {
            return value.(PriorityGroup).IsPending()
        }),
    }
}

func (sr *SortedRequests) AddRequest(request *schedulingAllocationAsk) *schedulingAllocationAsk {
    if request == nil {
        return nil
    }
    existingRequest := sr.requests[request.AskProto.AllocationKey]
    // add or update in priority group
    priorityGroup := sr.sortedPriorityGroups.Get(request.priority)
    if priorityGroup == nil {
        priorityGroup = NewSortedPriorityGroup(request.priority)
    }
    priorityGroup.(PriorityGroup).AddRequest(request)
    // this must be called to update pending state
    sr.sortedPriorityGroups.Put(request.priority, priorityGroup)
    // add or update in map
    sr.requests[request.AskProto.AllocationKey] = request
    return existingRequest
}

func (sr *SortedRequests) RemoveRequest(allocationKey string) *schedulingAllocationAsk {
    existingRequest := sr.requests[allocationKey]
    if existingRequest != nil {
        // remove from map
        delete(sr.requests, allocationKey)
        // remove from priority group
        priorityGroup := sr.sortedPriorityGroups.Get(existingRequest.priority)
        if priorityGroup != nil {
            priorityGroup.(*SortedPriorityGroup).RemoveRequest(allocationKey)
            if priorityGroup.(*SortedPriorityGroup).Size() == 0 {
                // remove this priority group if it has not any request
                sr.sortedPriorityGroups.Remove(existingRequest.priority)
            } else {
                // this must be called to update pending state
                sr.sortedPriorityGroups.Put(priorityGroup.(*SortedPriorityGroup).GetPriority(), priorityGroup)
            }
        }
    }
    return existingRequest
}

func (sr *SortedRequests) GetRequest(allocationKey string) *schedulingAllocationAsk {
    return sr.requests[allocationKey]
}

func (sr *SortedRequests) Size() int {
    return len(sr.requests)
}

func (sr *SortedRequests) Reset() {
    sr.requests = make(map[string]*schedulingAllocationAsk)
    sr.sortedPriorityGroups.Reset()
}

func (sr *SortedRequests) GetTopPendingPriorityGroup() PriorityGroup {
    _, v := sr.sortedPriorityGroups.GetFirstMatched()
    if v != nil {
        return v.(PriorityGroup)
    }
    return nil
}

func (sr *SortedRequests) GetPendingRequestIterator() RequestIterator {
    pgIt := sr.sortedPriorityGroups.GetMatchedIterator()
    iterators := make([]common.MapIterator, 0)
    for pgIt.HasNext() {
        _, pg := pgIt.Next()
        pendingReqIt := pg.(*SortedPriorityGroup).sortedRequests.GetMatchedIterator()
        iterators = append(iterators, pendingReqIt)
    }
    return NewSortedRequestIterator(iterators)
}

// This is the second level of SortedRequests
type SortedPriorityGroup struct {
    // priority of the group
    priority int32
    // create time of the group
    createTime time.Time
    // sorted requests in ascending order by the creation time of request
    sortedRequests *common.SortableLinkedMap
}

func NewSortedPriorityGroup(priority int32) *SortedPriorityGroup {
    return &SortedPriorityGroup{
        priority:   priority,
        createTime: time.Now(),
        sortedRequests: common.NewSortableLinkedMap(func(i, j interface{}) bool {
            return i.(*schedulingAllocationAsk).getCreateTime().Before(j.(*schedulingAllocationAsk).getCreateTime())
        }, true, func(value interface{}) bool {
            return value.(*schedulingAllocationAsk).getPendingAskRepeat() > 0
        }),
    }
}

func (spg *SortedPriorityGroup) AddRequest(request *schedulingAllocationAsk) {
    spg.sortedRequests.Put(request.AskProto.AllocationKey, request)
}

func (spg *SortedPriorityGroup) RemoveRequest(allocationKey string) {
    spg.sortedRequests.Remove(allocationKey)
}

func (spg *SortedPriorityGroup) GetPriority() int32 {
    return spg.priority
}

func (spg *SortedPriorityGroup) GetCreateTime() time.Time {
    return spg.createTime
}

func (spg *SortedPriorityGroup) Size() int {
    return spg.sortedRequests.Size()
}

func (spg *SortedPriorityGroup) IsPending() bool {
    return spg.sortedRequests.HasMatched()
}

func (spg *SortedPriorityGroup) GetPendingRequestIterator() RequestIterator {
    return NewSortedRequestIterator([]common.MapIterator{spg.sortedRequests.GetMatchedIterator()})
}

func (spg *SortedPriorityGroup) GetRequestIterator() RequestIterator {
    return NewSortedRequestIterator([]common.MapIterator{spg.sortedRequests.GetIterator()})
}

type SortedRequestIterator struct {
    iterators []common.MapIterator
    index int
}

func NewSortedRequestIterator(iterators []common.MapIterator) *SortedRequestIterator {
    return &SortedRequestIterator{
        iterators: iterators,
        index:     0,
    }
}

func (sri *SortedRequestIterator) HasNext() (ok bool) {
    if len(sri.iterators) == 0 {
        return false
    }
    for {
        if sri.iterators[sri.index].HasNext() {
            return true
        } else if sri.index == len(sri.iterators)-1 {
            return false
        } else {
            sri.index++
        }
    }
}

func (sri *SortedRequestIterator) Next() *schedulingAllocationAsk {
    if len(sri.iterators) == 0 {
        return nil
    }
    for {
        if sri.iterators[sri.index].HasNext() {
            _, v := sri.iterators[sri.index].Next()
            return v.(*schedulingAllocationAsk)
        } else if sri.index == len(sri.iterators)-1 {
            return nil
        } else {
            sri.index++
        }
    }
}
