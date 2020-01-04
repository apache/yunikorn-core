/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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

package tests

import (
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/scheduler"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "github.com/stretchr/testify/assert"
    "testing"
)

func TestBasicReservation(t *testing.T) {
    ms := &MockScheduler{}
    defer ms.Stop()

    ms.Init(t, OneQueueConfig)

    for i := 0; i < 2; i++ {
        ms.AddNode("node-" + string(i), &si.Resource{
            Resources: map[string]*si.Quantity{
                "memory": {Value: 100},
                "vcore":  {Value: 100},
            },
        })
    }

    ms.AddApp("app-1", "root.a", "default")
    ms.AddApp("app-2", "root.a", "default")

    ms.AppRequestResource("app-1", "alloc-1", 10, 10, 10)

    schedulerQueueA := ms.scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.a", ms._partition("default"))
    schedulerQueueRoot := ms.scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", ms._partition("default"))
    schedulingApp1 := ms.scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", ms._partition("default"))
    schedulingApp2 := ms.scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-2", ms._partition("default"))

    // Allocate for apps
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Verify all requests are satisfied
    waitForAllocations(ms.mockRM, 10, 1000)
    waitForPendingResource(t, schedulerQueueA, 0, 1000)
    waitForPendingResourceForApplication(t, schedulingApp1, 0, 1000)
    assert.True(t, schedulingApp1.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 100)

    // Verify 5 allocations for every node (mem=50)
    for _, node := range ms.nodes {
        assert.True(t, node.GetAllocatedResource().Resources[resources.MEMORY] == 50)
    }

    waitForAllocatingResource(t, schedulerQueueA, 0, 1000)
    waitForAllocatingResourceForApplication(t, schedulingApp2, 0, 1000)

    // Now, app-2 asks for a pod with mem=100, it should be reserved
    ms.AppRequestResource("app-2", "alloc-1", 100, 100, 1)

    // Allocate for apps
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Allocation should not change (still 10)
    waitForAllocations(ms.mockRM, 10, 1000)

    // Allocating resource should be changed, and request should be reserved
    waitForAllocatingResource(t, schedulerQueueA, 100, 1000)
    waitForAllocatingResourceForApplication(t, schedulingApp2, 100, 1000)
    appReservation := schedulingApp2.GetReservations()
    assert.True(t, len(appReservation) == 1)
    reservationInnerMap := appReservation["alloc-1"]
    assert.True(t, len(reservationInnerMap) == 1)

    // Get reservation request
    var reservationRequest *scheduler.ReservedSchedulingRequest = nil
    for _, v := range reservationInnerMap {
        reservationRequest = v
    }

    // Node also has reservation request,
    assert.Equal(t, 1, reservationRequest.GetAmount())
    assert.True(t, len(reservationRequest.SchedulingNode.GetReservedRequests()) == 1)
    assert.True(t, reservationRequest.SchedulingNode.GetTotalReservedResources().Resources["memory"] == 100)

    // Remove app-1
    ms.RemoveApp("app-1", "default")

    // Now allocating is still 100 (reserved), but allocated should be 0
    waitForAllocatingResource(t, schedulerQueueA, 100, 1000)
    waitForAllocatingResource(t, schedulerQueueRoot, 100, 1000)

    // Check allocated resource of queue, should be 0 now
    waitForAllocatedResourceOfQueue(t, schedulerQueueA, 0, 1000)
    waitForAllocatedResourceOfQueue(t, schedulerQueueRoot, 0, 1000)

    // Allocate for app-2
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Should have 1 more container allocated
    waitForAllocations(ms.mockRM, 1, 1000)

    // And now, check allocating/allocated
    // Now allocating is 0 (reserved became allocated), but allocated should be 100
    waitForAllocatingResource(t, schedulerQueueA, 0, 1000)
    waitForAllocatingResource(t, schedulerQueueRoot, 0, 1000)

    // Check allocated resource of queue, should be 0 now
    waitForAllocatedResourceOfQueue(t, schedulerQueueA, 100, 1000)
    waitForAllocatedResourceOfQueue(t, schedulerQueueRoot, 100, 1000)

    // App/node should not have reservation now
    appReservation = schedulingApp2.GetReservations()
    assert.True(t, len(appReservation) == 0)
    assert.True(t, len(reservationRequest.SchedulingNode.GetReservedRequests()) == 0)
    assert.True(t, reservationRequest.SchedulingNode.GetTotalReservedResources().Resources["memory"] == 0)
}

// Test case:
// Register 3 nodes, each has 100 resources.
// App1 (queue-a) asks 3 request, with 80 each, after allocation there're 20 resources on each node.
// App2 (queue-b) asks 3 request, with 30 each. it will reserve on two nodes
// App3 (queue-c) asks for 20 request, with 10 each, nothing should happen.
// Then kill App1, for first two allocation, we should find app2 get 2 * 30 containers,
// Then allocate more, we should find App2 get 3 * 30 container, and app3 get 21 * 10 containers.
func TestReservationForTwoQueues(t *testing.T) {
    ms := &MockScheduler{}
    defer ms.Stop()

    ms.Init(t, TwoEqualQueueConfigDisabledPreemption)

    for i := 0; i < 3; i++ {
        ms.AddNodeWithMemAndCpu("node-" + string(i), 100, 100)
    }

    ms.AddApp("app-1", "root.a", "default")
    ms.AddApp("app-2", "root.b", "default")
    ms.AddApp("app-3", "root.a", "default")

    schedulerQueueA := ms.scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.a", ms._partition("default"))
    schedulerQueueB := ms.scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.b", ms._partition("default"))
    schedulerQueueRoot := ms.scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", ms._partition("default"))
    schedulingApp1 := ms.scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", ms._partition("default"))
    schedulingApp2 := ms.scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-2", ms._partition("default"))
    schedulingApp3 := ms.scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-3", ms._partition("default"))

    ms.AppRequestResource("app-1", "alloc-1", 80, 80, 4)

    // Allocate for apps
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Verify 3 requests are allocated
    waitForAllocations(ms.mockRM, 3, 1000)
    waitForPendingResource(t, schedulerQueueA, 80, 1000)
    waitForPendingResourceForApplication(t, schedulingApp1, 80, 1000)
    assert.True(t, schedulingApp1.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 240)

    // Verify 1 allocations for every node (mem=50)
    for _, node := range ms.nodes {
        assert.True(t, node.GetAllocatedResource().Resources[resources.MEMORY] == 80)
    }

    // Should have no allocating resource
    // (Because reservation cannot be made because of headroom limit)
    waitForAllocatingResource(t, schedulerQueueA, 0, 1000)
    waitForAllocatingResourceForApplication(t, schedulingApp1, 0, 1000)

    // Now, app-2 asks for 3 request with mem=30, we should find two request reserved (one on each node)
    ms.AppRequestResource("app-2", "alloc-1", 30, 30, 3)

    // And app-3 asks 30 * 10 requests
    ms.AppRequestResource("app-3", "alloc-1", 10, 10, 30)

    // Allocate for apps
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Allocation should not change (still 3)
    waitForAllocations(ms.mockRM, 3, 1000)

    // Allocating resource should be changed, and request should be reserved
    waitForAllocatingResource(t, schedulerQueueB, 60, 1000)
    waitForAllocatingResourceForApplication(t, schedulingApp2, 60, 1000)
    reservationRequests := schedulingApp2.GetAllReservationRequests()
    assert.True(t, len(reservationRequests) == 2)

    // Do more allocation, even node has some available resources, but since nodes are reserved, nothing should be allocated for app-3\
    waitForAllocatingResource(t, schedulerQueueA, 0, 1000)
    assert.True(t, schedulingApp3.GetTotalMayAllocated().Resources["memory"] == 0)

    // For each reservation, there's only one reservation per node
    for i := 0; i < 2; i++ {
        assert.Equal(t, 1, reservationRequests[i].GetAmount())
        assert.True(t, len(reservationRequests[i].SchedulingNode.GetReservedRequests()) == 1)
        assert.True(t, reservationRequests[i].SchedulingNode.GetTotalReservedResources().Resources["memory"] == 30)
    }

    // Now remove app-1, and do allocation
    ms.RemoveApp("app-1", "default")

    // Check allocated resource of queue, should be 0 now
    waitForAllocations(ms.mockRM, 0, 1000)
    waitForAllocatedResourceOfQueue(t, schedulerQueueA, 0, 1000)
    waitForAllocatedResourceOfQueue(t, schedulerQueueRoot, 0, 1000)

    // Allocate twice, which we should see two reservations filled
    for i := 0; i < 2; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Should have 1 more container allocated
    waitForAllocations(ms.mockRM, 2, 1000)

    // And now, check allocating/allocated
    // Now allocating is 0 (reserved became allocated), but allocated should be 100
    waitForAllocatingResource(t, schedulerQueueB, 0, 1000)
    waitForAllocatingResource(t, schedulerQueueRoot, 0, 1000)

    // Check allocated resource of queue, should be 0 now
    waitForAllocatedResourceOfQueue(t, schedulerQueueB, 60, 1000)
    waitForAllocatedResourceOfQueue(t, schedulerQueueRoot, 60, 1000)

    // Reservation should be filled
    reservationRequests = schedulingApp2.GetAllReservationRequests()
    assert.True(t, len(reservationRequests) == 0)

    // Do more allocation
    for i := 0; i < 100; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Check allocated resource of queue, queueB should have 90, queueA should have 110
    waitForAllocations(ms.mockRM, 24, 1000) // 24 allocations, 3 from queueB and 21 from queueA
    waitForAllocatedResourceOfQueue(t, schedulerQueueB, 90, 1000)
    waitForAllocatedResourceOfQueue(t, schedulerQueueA, 210, 1000)

    // Now allocating is 0 (reserved became allocated)
    waitForAllocatingResource(t, schedulerQueueB, 0, 1000)
    waitForAllocatingResource(t, schedulerQueueA, 0, 1000)

    waitForPendingResource(t, schedulerQueueB, 0, 1000)

    // Queue A still have 300 - 210 = 90 pending resource
    waitForPendingResource(t, schedulerQueueA, 90, 1000)

    // And nodes should have no reservation too
    for _, node := range ms.nodes {
        assert.True(t, len(node.GetReservedRequests()) == 0)
        assert.True(t, node.GetTotalReservedResources().Resources["memory"] == 0)
    }
}

// Test case:
// Register 3 nodes, each has 100 resources.
// App1 (queue-a) asks 3 request, with 60 each, after allocation there're 40 resources on each node. (total 120 available).
// App2 (queue-b) asks 1 request, with 110 each, nothing should happen.
// App2 (queue-b) asks 30 more requests with 10 each, should get 300 - 180 = 120 / 10 = 12 allocations
func TestReservationShouldNotGoBeyondNodeLimit(t *testing.T) {
    ms := &MockScheduler{}
    defer ms.Stop()

    ms.Init(t, OneQueueConfig)

    for i := 0; i < 3; i++ {
        ms.AddNodeWithMemAndCpu("node-" + string(i), 100, 100)
    }

    ms.AddApp("app-1", "root.a", "default")
    ms.AddApp("app-2", "root.a", "default")

    ms.AppRequestResource("app-1", "alloc-1", 60, 60, 3)

    schedulerQueueA := ms.scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.a", ms._partition("default"))
    schedulingApp1 := ms.scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", ms._partition("default"))
    schedulingApp2 := ms.scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-2", ms._partition("default"))

    // Allocate for apps
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Verify all requests are satisfied
    waitForAllocations(ms.mockRM, 3, 1000)
    assert.True(t, schedulingApp1.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 180)

    // Verify 1 allocations for every node (mem=60)
    for _, node := range ms.nodes {
        assert.True(t, node.GetAllocatedResource().Resources[resources.MEMORY] == 60)
    }

    // Now, app-2 asks for a pod with mem=100, it should be reserved
    ms.AppRequestResource("app-2", "alloc-1", 110, 100, 1)

    // Allocate for apps
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Nothing should happen
    waitForAllocations(ms.mockRM, 3, 1000) // still the same
    waitForAllocatingResource(t, schedulerQueueA, 0, 1000)
    waitForAllocatingResourceForApplication(t, schedulingApp2, 0, 1000)
    assert.Equal(t, 0, len(schedulingApp2.GetAllReservationRequests()))

    // Now, app-2 asks for a pod with mem=100 (in a different allocationKey), it should be reserved
    ms.AppRequestResource("app-2", "alloc-2", 100, 100, 10)

    // Allocate for apps, should have one reservation
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }
    waitForAllocations(ms.mockRM, 3, 1000) // still the same
    waitForAllocatingResource(t, schedulerQueueA, 100, 1000)
    waitForAllocatingResourceForApplication(t, schedulingApp2, 100, 1000)
    assert.Equal(t, 1, len(schedulingApp2.GetAllReservationRequests()))
    // verify key of reservation, it should be alloc-2 instead of 2
    assert.Equal(t, "alloc-2", schedulingApp2.GetAllReservationRequests()[0].SchedulingAsk.AskProto.AllocationKey)

    // Now, app-2 asks for  more pods with mem=10 (in a different allocationKey), it should be allocated
    ms.AppRequestResource("app-2", "alloc-3", 10, 10, 100)

    // Allocate for apps, should have 300 - 180 (allocated) - 100 (Reserved) = 20 available, so there're 2 allocations for key=alloc-3
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }
    waitForAllocations(ms.mockRM, 5, 1000)
    waitForAllocatingResource(t, schedulerQueueA, 100, 1000) // still the same
    waitForAllocatingResourceForApplication(t, schedulingApp2, 100, 1000) // still the same
    waitForAllocatedResourceOfQueue(t, schedulerQueueA, 200, 1000) // 180 + 20 allocated
}


// Test case:
// - Register 10 nodes, 100 resource each.
// - Submit app-1, ask for mem=60, repeat=10; (alloc-1)
// - For app-1, (alloc-2) ask for mem = 50, repeat = 10; there should have 8 reservation request (100 * 10 - 60 * 10) / 50 = 8;
// - app-1, change numAlloc of alloc-2 from 10 to 8, nothing should happen.
// - app-1, change numAlloc of alloc-2 from 8 to 5, there should have 5 reserved nodes remaining
// - app-1, change numAlloc of alloc-2 from 5 to 0, there should have 0 reserved node remaining
func TestUnreserveWhenPendingIsLessThanReserved(t *testing.T) {
    ms := &MockScheduler{}
    defer ms.Stop()

    ms.Init(t, OneQueueConfig)

    for i := 0; i < 10; i++ {
        ms.AddNodeWithMemAndCpu("node-" + string(i), 100, 100)
    }

    ms.AddApp("app-1", "root.a", "default")

    ms.AppRequestResource("app-1", "alloc-1", 60, 60, 10)

    schedulerQueueA := ms.scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.a", ms._partition("default"))
    schedulingApp1 := ms.scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", ms._partition("default"))

    // Allocate for apps
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Verify all requests are satisfied
    waitForAllocations(ms.mockRM, 10, 1000)
    assert.True(t, schedulingApp1.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 600)

    // Verify 1 allocations for every node (mem=60)
    for _, node := range ms.nodes {
        assert.True(t, node.GetAllocatedResource().Resources[resources.MEMORY] == 60)
    }

    // Now, app-1 asks for a pod with mem=50, it should be reserved
    ms.AppRequestResource("app-1", "alloc-2", 50, 50, 10)

    // Allocate for apps
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Nothing should happen
    waitForAllocations(ms.mockRM, 10, 1000) // still the same
    waitForAllocatingResource(t, schedulerQueueA, 400, 1000)
    waitForAllocatingResourceForApplication(t, schedulingApp1, 400, 1000)
    assert.Equal(t, 8, len(schedulingApp1.GetAllReservationRequests()))

    // Now, app-2 asks for a pod with mem=50, change repeat from 10 to 8, nothing should be changed.
    ms.AppRequestResource("app-1", "alloc-2", 50, 50, 8)

    // Allocate for apps
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Nothing should happen
    waitForAllocations(ms.mockRM, 10, 1000) // still the same
    waitForAllocatingResource(t, schedulerQueueA, 400, 1000)
    waitForAllocatingResourceForApplication(t, schedulingApp1, 400, 1000)
    assert.Equal(t, 8, len(schedulingApp1.GetAllReservationRequests()))

    // Now, app-2 asks for a pod with mem=50, change repeat from 8 to 5, reservation should be changed from 8 to 5
    ms.AppRequestResource("app-1", "alloc-2", 50, 50, 5)

    // Allocate for apps
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Nothing should happen
    waitForAllocations(ms.mockRM, 10, 1000) // still the same
    waitForAllocatingResource(t, schedulerQueueA, 250, 1000)
    waitForAllocatingResourceForApplication(t, schedulingApp1, 250, 1000)
    assert.Equal(t, 5, len(schedulingApp1.GetAllReservationRequests()))

    // Now, app-2 asks for a pod with mem=50, change repeat from 5 to 0, reservation should be changed from 5 to 0
    ms.AppRequestResource("app-1", "alloc-2", 50, 50, 0)

    // Allocate for apps
    for i := 0; i < 20; i++ {
        ms.scheduler.SingleStepScheduleAllocTest(1)
    }

    // Nothing should happen
    waitForAllocations(ms.mockRM, 10, 1000) // still the same
    waitForAllocatingResource(t, schedulerQueueA, 0, 1000)
    waitForAllocatingResourceForApplication(t, schedulingApp1, 0, 1000)
    assert.Equal(t, 0, len(schedulingApp1.GetAllReservationRequests()))
}