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

package scheduler

import (
    "github.com/golang/glog"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
)

// Find next set of allocation asks for scheduler to place
// This could be "mini batch", no need to return too many candidates
func (m *Scheduler) findAllocationAsks(partitionContext *PartitionSchedulingContext, n int,
    curStep uint64) []*SchedulingAllocationAsk {
    mayAllocateList := make([]*SchedulingAllocationAsk, 0)

    // Do we have any pending resource?
    if !resources.StrictlyGreaterThanZero(partitionContext.Root.PendingResource) {
        // If no pending resource, return empty array
        return mayAllocateList
    }

    // Reset may allocations
    m.resetMayAllocations(partitionContext)

    selectedAsksByAllocationKey := make(map[string]int32, 0)

    // Repeatedly go to queue hierarchy, find next allocation ask, until we find N allocations
    found := true
    for found {
        // Find next allocation ask, see if it can be allocated, if yes, add to
        // may allocate list.
        next := m.findNextAllocationAskCandidate([]*SchedulingQueue{partitionContext.Root}, partitionContext, nil, nil, curStep, selectedAsksByAllocationKey)
        found = next != nil

        if found {
            mayAllocateList = append(mayAllocateList, next)
            if len(mayAllocateList) >= n {
                break
            }
        }
    }

    return mayAllocateList
}

// Return a sorted copy of the queues for this parent queue.
// Only queues with a pending resource request are considered. The queues are sorted using the
// sorting type for the parent queue.
// Stopped queues will be filtered out at a later stage.
func sortSubqueuesFromQueue(parentQueue *SchedulingQueue) []*SchedulingQueue {
    parentQueue.lock.RLock()
    defer parentQueue.lock.RUnlock()

    // Create a copy of the queues with pending resources
    sortedQueues := make([]*SchedulingQueue, 0)
    for _, v := range parentQueue.childrenQueues {
        // Only look at queue when pending-res > 0
        if resources.StrictlyGreaterThanZero(v.PendingResource) {
            sortedQueues = append(sortedQueues, v)
        }
    }

    // Sort the queues
    SortQueue(sortedQueues, parentQueue.QueueSortType)

    return sortedQueues
}

// Return a sorted copy of the applications in the queue.
// Only applications with a pending resource request are considered. The applications are sorted using the
// sorting type for the leaf queue they are in.
func sortApplicationsFromQueue(leafQueue *SchedulingQueue) []*SchedulingApplication {
    leafQueue.lock.RLock()
    defer leafQueue.lock.RUnlock()

    // Create a copy of the applications with pending resources
    sortedApps := make([]*SchedulingApplication, 0)
    for _, v := range leafQueue.applications {
        // Only look at app when pending-res > 0
        if resources.StrictlyGreaterThanZero(v.Requests.TotalPendingResource) {
            sortedApps = append(sortedApps, v)
        }
    }

    // Sort the applications
    SortApplications(sortedApps, leafQueue.ApplicationSortType, leafQueue.CachedQueueInfo.GuaranteedResource)

    return sortedApps
}

// sort scheduling Requests from a app
func (m *Scheduler) findMayAllocationFromApplication(schedulingRequests *SchedulingRequests,
    headroom *resources.Resource, curStep uint64, selectedPendingAskByAllocationKey map[string]int32) *SchedulingAllocationAsk {
    schedulingRequests.lock.RLock()
    defer schedulingRequests.lock.RUnlock()

    var bestAsk *SchedulingAllocationAsk = nil

    for _, v := range schedulingRequests.requests {
        if m.missedOpportunities[v.AskProto.AllocationKey]&curStep != 0 {
            // this request is "blacklisted"
            continue
        }

        // Only sort request if its resource fits headroom
        if v.PendingRepeatAsk-selectedPendingAskByAllocationKey[v.AskProto.AllocationKey] > 0 && resources.FitIn(headroom, v.AllocatedResource) {
            if bestAsk == nil || v.NormalizedPriority > bestAsk.NormalizedPriority {
                bestAsk = v
            }
        }
    }

    if bestAsk != nil {
        selectedPendingAskByAllocationKey[bestAsk.AskProto.AllocationKey] += 1
    }

    return bestAsk
}

// do this from queue hierarchy, sortedQueueCandidates is temporary var
// and won't be shared in other goroutines
func (m *Scheduler) findNextAllocationAskCandidate(
    sortedQueueCandidates []*SchedulingQueue,
    partitionContext *PartitionSchedulingContext,
    parentHeadroom *resources.Resource,
    parentQueueMaxResource *resources.Resource,
    curStep uint64,
    selectedPendingAskByAllocationKey map[string]int32) *SchedulingAllocationAsk {
    for _, queue := range sortedQueueCandidates {
        // skip stopped queues: running and draining queues are allowed
        if queue.isStopped() {
            glog.V(4).Infof("Skip queue=%s because it is not running", queue.Name)
            continue
        }
        // Is it need any resource?
        if !resources.StrictlyGreaterThanZero(queue.PendingResource) {
            glog.V(4).Infof("Skip queue=%s because it has no pending resource", queue.Name)
            continue
        }

        var newHeadroom *resources.Resource
        maxResource := queue.CachedQueueInfo.MaxResource
        if maxResource == nil {
            maxResource = parentQueueMaxResource
        }
        // new headroom for this queue
        if nil != parentHeadroom {
            newHeadroom = resources.ComponentWiseMin(resources.Sub(maxResource, queue.MayAllocatedResource), parentHeadroom)
        } else {
            newHeadroom = resources.Sub(maxResource, queue.MayAllocatedResource)
        }

        if queue.isLeafQueue() {
            sortedApps := sortApplicationsFromQueue(queue)
            for _, app := range sortedApps {
                if ask := m.findMayAllocationFromApplication(app.Requests, newHeadroom, curStep, selectedPendingAskByAllocationKey); ask != nil {
                    app.MayAllocatedResource = resources.Add(app.MayAllocatedResource, ask.AllocatedResource)
                    queue.MayAllocatedResource = resources.Add(queue.MayAllocatedResource, ask.AllocatedResource)
                    return ask
                }
            }
        } else {
            sortedChildren := sortSubqueuesFromQueue(queue)
            if ask := m.findNextAllocationAskCandidate(sortedChildren, partitionContext, newHeadroom, maxResource, curStep, selectedPendingAskByAllocationKey); ask != nil {
                queue.MayAllocatedResource = resources.Add(queue.MayAllocatedResource, ask.AllocatedResource)
                return ask
            }
        }
    }

    return nil
}

func (m *Scheduler) resetMayAllocations(partitionContext *PartitionSchedulingContext) {
    // Recursively reset may-allocation
    // lock the partition
    partitionContext.lock.RLock()
    defer partitionContext.lock.RUnlock()

    m.resetMayAllocationsForQueue(partitionContext.Root)
}

func (m *Scheduler) resetMayAllocationsForQueue(queue *SchedulingQueue) {
    queue.MayAllocatedResource = queue.CachedQueueInfo.AllocatedResource
    if queue.isLeafQueue() {
        for _, app := range queue.applications {
            app.MayAllocatedResource = app.ApplicationInfo.AllocatedResource
        }
    } else {
        for _, child := range queue.childrenQueues {
            m.resetMayAllocationsForQueue(child)
        }
    }
}
