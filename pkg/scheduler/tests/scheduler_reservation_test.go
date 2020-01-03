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
    "github.com/cloudera/yunikorn-core/pkg/common/configs"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/entrypoint"
    "github.com/cloudera/yunikorn-core/pkg/scheduler"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "github.com/stretchr/testify/assert"
    "strconv"
    "testing"
)

func TestBasicReservation(t *testing.T) {
    const partition = "[rm:123]default"
    // Start all tests
    serviceContext := entrypoint.StartAllServicesWithManualScheduler()
    defer serviceContext.StopAll()
    proxy := serviceContext.RMProxy
    context := serviceContext.Scheduler.GetClusterSchedulingContext()

    // Register RM
    configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
`
    configs.MockSchedulerConfigByData([]byte(configData))
    mockRM := NewMockRMCallbackHandler(t)

    _, err := proxy.RegisterResourceManager(
        &si.RegisterResourceManagerRequest{
            RmId:        "rm:123",
            PolicyGroup: "policygroup",
            Version:     "0.0.2",
        }, mockRM)

    if err != nil {
        t.Fatalf("RegisterResourceManager failed: %v", err)
    }

    // Register 2 nodes, (mem=100, vcore=100) for each
    nodes := make([]*si.NewNodeInfo, 0)
    for i := 0; i < 2; i++ {
        nodeId := "node-" + strconv.Itoa(i)
        nodes = append(nodes, &si.NewNodeInfo{
            NodeId: nodeId + ":1234",
            Attributes: map[string]string{
                "si.io/hostname":  nodeId,
                "si.io/rackname":  "rack-1",
                "si.io/partition": "default",
            },
            SchedulableResource: &si.Resource{
                Resources: map[string]*si.Quantity{
                    "memory": {Value: 100},
                    "vcore":  {Value: 100},
                },
            },
        })
    }

    // Add two apps
    err = proxy.Update(&si.UpdateRequest{
        NewSchedulableNodes: nodes,
        NewApplications:     newAddAppRequest(map[string]string{"app-1": "root.a", "app-2": "root.a"}),
        RmId:                "rm:123",
    })

    if err != nil {
        t.Fatalf("UpdateRequest failed: %v", err)
    }

    // verify app and all nodes are accepted
    waitForAcceptedApplications(mockRM, "app-1", 1000)
    waitForAcceptedApplications(mockRM, "app-2", 1000)

    for _, node := range nodes {
        waitForAcceptedNodes(mockRM, node.NodeId, 1000)
    }

    for _, node := range nodes {
        waitForNewSchedulerNode(t, context, node.NodeId, partition, 1000)
    }

    // app-1 Request ask with 10 allocations, (mem-10, vcore=10) for each
    err = proxy.Update(&si.UpdateRequest{
        Asks: []*si.AllocationAsk{
            {
                AllocationKey: "alloc-1",
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 10},
                        "vcore":  {Value: 10},
                    },
                },
                MaxAllocations: 10,
                ApplicationId:  "app-1",
            },
        },
        RmId: "rm:123",
    })

    if err != nil {
        t.Fatalf("UpdateRequest failed: %v", err)
    }

    schedulerQueueA := context.GetSchedulingQueue("root.a", partition)
    schedulerQueueRoot := context.GetSchedulingQueue("root", partition)

    schedulingApp1 := context.GetSchedulingApplication("app-1", partition)
    schedulingApp2 := context.GetSchedulingApplication("app-2", partition)

    waitForPendingResource(t, schedulerQueueA, 100, 1000)

    // Allocate for apps
    for i := 0; i < 20; i++ {
        serviceContext.Scheduler.SingleStepScheduleAllocTest(1)
    }

    // Verify all requests are satisfied
    waitForAllocations(mockRM, 10, 1000)
    waitForPendingResource(t, schedulerQueueA, 0, 1000)
    waitForPendingResourceForApplication(t, schedulingApp1, 0, 1000)
    assert.True(t, schedulingApp1.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 100)

    // Verify 5 allocations for every node (mem=50)
    for _, node := range nodes {
        schedulingNode := context.GetSchedulingNode(node.NodeId, partition)
        assert.True(t, schedulingNode.GetAllocatedResource().Resources[resources.MEMORY] == 50)
    }

    waitForAllocatingResource(t, schedulerQueueA, 0, 1000)
    waitForAllocatingResourceForApplication(t, schedulingApp2, 0, 1000)

    // Now, app-2 asks for a pod with mem=100, it should be reserved
    err = proxy.Update(&si.UpdateRequest{
        Asks: []*si.AllocationAsk{
            {
                AllocationKey: "alloc-1",
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 100},
                    },
                },
                MaxAllocations: 1,
                ApplicationId:  "app-2",
            },
        },
        RmId: "rm:123",
    })
    waitForPendingResource(t, schedulerQueueA, 100, 1000)

    // Allocate for apps
    for i := 0; i < 20; i++ {
        serviceContext.Scheduler.SingleStepScheduleAllocTest(1)
    }

    // Allocation should not change (still 10)
    waitForAllocations(mockRM, 10, 1000)

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
    err = proxy.Update(&si.UpdateRequest{
        RemoveApplications: newRemoveAppRequest([]string{"app-1"}),
        RmId: "rm:123",
    })

    // Now allocating is still 100 (reserved), but allocated should be 0
    waitForAllocatingResource(t, schedulerQueueA, 100, 1000)
    waitForAllocatingResource(t, schedulerQueueRoot, 100, 1000)

    // Check allocated resource of queue, should be 0 now
    waitForAllocatedResourceOfQueue(t, schedulerQueueA, 0, 1000)
    waitForAllocatedResourceOfQueue(t, schedulerQueueRoot, 0, 1000)

    // Allocate for app-2
    for i := 0; i < 20; i++ {
        serviceContext.Scheduler.SingleStepScheduleAllocTest(1)
    }

    // Should have 1 more container allocated
    waitForAllocations(mockRM, 1, 1000)

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