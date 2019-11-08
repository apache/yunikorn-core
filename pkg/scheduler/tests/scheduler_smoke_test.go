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
    "bytes"
    cacheInfo "github.com/cloudera/yunikorn-core/pkg/cache"
    "github.com/cloudera/yunikorn-core/pkg/common"
    "github.com/cloudera/yunikorn-core/pkg/common/configs"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/entrypoint"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "gotest.tools/assert"
    "strconv"
    "testing"
    "time"
)

// Test scheduler reconfiguration
func TestConfigScheduler(t *testing.T) {
    // Start all tests
    serviceContext := entrypoint.StartAllServicesWithManualScheduler()
    defer serviceContext.StopAll()
    proxy := serviceContext.RMProxy
    cache := serviceContext.Cache
    scheduler := serviceContext.Scheduler

    // Register RM
    configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: base
            resources:
              guaranteed: {memory: 100, vcore: 10}
              max: {memory: 150, vcore: 20}
          - name: tobedeleted
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
        t.Errorf("configuration load failed: %v", err)
    }

    // memorize the checksum of current configs
    configChecksum := configs.ConfigContext.Get("policygroup").Checksum

    // Check queues of cache and scheduler.
    partitionInfo := cache.GetPartition("[rm:123]default")
    assert.Assert(t, nil == partitionInfo.Root.MaxResource)

    // Check scheduling queue root
    schedulerQueueRoot := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")
    assert.Assert(t, nil == schedulerQueueRoot.CachedQueueInfo.MaxResource)

    // Check scheduling queue root.base
    schedulerQueue := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.base", "[rm:123]default")
    assert.Assert(t, 150 == schedulerQueue.CachedQueueInfo.MaxResource.Resources[resources.MEMORY])

    // Check the queue which will be removed
    schedulerQueue = scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.tobedeleted", "[rm:123]default")
    if !schedulerQueue.CachedQueueInfo.IsRunning() {
        t.Errorf("child queue root.tobedeleted is not in running state")
    }

    configData = `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: base
            resources:
              guaranteed: {memory: 500, vcore: 250}
              max: {memory: 1000, vcore: 500}
          - name: tobeadded
            properties:
              something: withAvalue
  - name: gpu
    queues:
      - name: production
      - name: test
        properties:
          gpu: test queue property
`
    configs.MockSchedulerConfigByData([]byte(configData))
    err = proxy.ReloadConfiguration("rm:123")

    if err != nil {
        t.Errorf("configuration reload failed: %v", err)
    }

    // wait until configuration is reloaded
    if err := common.WaitFor(1*time.Second, 5*time.Second, func() bool {
        return !bytes.Equal(configs.ConfigContext.Get("policygroup").Checksum, configChecksum)
    }); err != nil {
        t.Error("timeout waiting for configuration to be reloaded")
    }

    // Check queues of cache and scheduler.
    partitionInfo = cache.GetPartition("[rm:123]default")
    assert.Assert(t, nil == partitionInfo.Root.MaxResource)

    // Check scheduling queue root
    schedulerQueueRoot = scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")
    assert.Assert(t, nil == schedulerQueueRoot.CachedQueueInfo.MaxResource)

    // Check scheduling queue root.base
    schedulerQueue = scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.base", "[rm:123]default")
    assert.Assert(t, 1000 == schedulerQueue.CachedQueueInfo.MaxResource.Resources[resources.MEMORY])
    assert.Assert(t, 250 == schedulerQueue.CachedQueueInfo.GuaranteedResource.Resources[resources.VCORE])

    // check the removed queue state
    schedulerQueue = scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.tobedeleted", "[rm:123]default")
    if !schedulerQueue.CachedQueueInfo.IsDraining() {
        t.Errorf("child queue root.tobedeleted is not in draining state")
    }
    // check the newly added queue
    schedulerQueue = scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.tobeadded", "[rm:123]default")
    if schedulerQueue == nil {
        t.Errorf("scheduling queue root.tobeadded is not found")
    }

    // Check queues of cache and scheduler for the newly added partition
    partitionInfo = cache.GetPartition("[rm:123]gpu")
    assert.Assert(t, nil == partitionInfo.Root.MaxResource)

    // Check scheduling queue root
    schedulerQueueRoot = scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]gpu")
    assert.Assert(t, nil == schedulerQueueRoot.CachedQueueInfo.MaxResource)

    // Check scheduling queue root.production
    schedulerQueue = scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.production", "[rm:123]gpu")
    if schedulerQueue == nil {
        t.Errorf("New partition: scheduling queue root.production is not found")
    }
}

// Test basic interactions from rm proxy to cache and to scheduler.
func TestBasicScheduler(t *testing.T) {
    // Start all tests
    serviceContext := entrypoint.StartAllServicesWithManualScheduler()
    defer serviceContext.StopAll()
    proxy := serviceContext.RMProxy
    cache := serviceContext.Cache
    scheduler := serviceContext.Scheduler

    // Register RM
    configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
              max:
                memory: 150
                vcore: 20
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
        t.Error(err.Error())
    }

    // Check queues of cache and scheduler.
    partitionInfo := cache.GetPartition("[rm:123]default")
    assert.Assert(t, nil == partitionInfo.Root.MaxResource)

    // Check scheduling queue root
    schedulerQueueRoot := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")
    assert.Assert(t, nil == schedulerQueueRoot.CachedQueueInfo.MaxResource)

    // Check scheduling queue a
    schedulerQueueA := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.a", "[rm:123]default")
    assert.Assert(t, 150 == schedulerQueueA.CachedQueueInfo.MaxResource.Resources[resources.MEMORY])

    // Register a node, and add apps
    err = proxy.Update(&si.UpdateRequest{
        NewSchedulableNodes: []*si.NewNodeInfo{
            {
                NodeId: "node-1:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
            {
                NodeId: "node-2:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
        },
        NewApplications: newAddAppRequest(map[string]string{"app-1":"root.a"}),
        RmId: "rm:123",
    })

    if nil != err {
        t.Error(err.Error())
    }

    waitForAcceptedApplications(mockRM, "app-1", 1000)
    waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
    waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

    // Get scheduling app
    schedulingApp := scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", "[rm:123]default")

    // Verify app initial state
    app01, err := getApplicationInfoFromPartition(partitionInfo, "app-1")
    assert.Assert(t, err == nil)
    assert.Equal(t, app01.GetApplicationState(), cacheInfo.Accepted.String())

    err = proxy.Update(&si.UpdateRequest{
        Asks: []*si.AllocationAsk{
            {
                AllocationKey: "alloc-1",
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 10},
                        "vcore":  {Value: 1},
                    },
                },
                MaxAllocations: 2,
                ApplicationId:  "app-1",
            },
        },
        RmId: "rm:123",
    })

    if nil != err {
        t.Error(err.Error())
    }

    // Wait pending resource of queue a and scheduler queue
    // Both pending memory = 10 * 2 = 20;
    waitForPendingResource(t, schedulerQueueA, 20, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 20, 1000)
    waitForPendingResourceForApplication(t, schedulingApp, 20, 1000)

    scheduler.SingleStepScheduleAllocTest(16)

    waitForAllocations(mockRM, 2, 1000)

    // Make sure pending resource updated to 0
    waitForPendingResource(t, schedulerQueueA, 0, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 0, 1000)
    waitForPendingResourceForApplication(t, schedulingApp, 0, 1000)

    // Check allocated resources of queues, apps
    assert.Assert(t, schedulerQueueA.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 20)
    assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 20)
    assert.Assert(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 20)

    // once we start to process allocation asks from this app, verify the state again
    assert.Equal(t, app01.GetApplicationState(), cacheInfo.Running.String())

    // Check allocated resources of nodes
    waitForNodesAllocatedResource(t, cache, "[rm:123]default", []string{"node-1:1234", "node-2:1234"}, 20, 1000)

    // Ask for two more resources
    err = proxy.Update(&si.UpdateRequest{
        Asks: []*si.AllocationAsk{
            {
                AllocationKey: "alloc-2",
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 50},
                        "vcore":  {Value: 5},
                    },
                },
                MaxAllocations: 2,
                ApplicationId:  "app-1",
            },
            {
                AllocationKey: "alloc-3",
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 5},
                    },
                },
                MaxAllocations: 2,
                ApplicationId:  "app-1",
            },
        },
        RmId: "rm:123",
    })

    if nil != err {
        t.Error(err.Error())
    }

    // Wait pending resource of queue a and scheduler queue
    // Both pending memory = 50 * 2 + 100 * 2 = 300;
    waitForPendingResource(t, schedulerQueueA, 300, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 300, 1000)
    waitForPendingResourceForApplication(t, schedulingApp, 300, 1000)

    // Now app-1 uses 20 resource, and queue-a's max = 150, so it can get two 50 container allocated.
    scheduler.SingleStepScheduleAllocTest(16)

    waitForAllocations(mockRM, 4, 1000)

    // Check pending resource, should be 200 now.
    waitForPendingResource(t, schedulerQueueA, 200, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 200, 1000)
    waitForPendingResourceForApplication(t, schedulingApp, 200, 1000)

    // Check allocated resources of queues, apps
    assert.Assert(t, schedulerQueueA.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 120)
    assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 120)
    assert.Assert(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 120)

    // Check allocated resources of nodes
    waitForNodesAllocatedResource(t, cache, "[rm:123]default", []string{"node-1:1234", "node-2:1234"}, 120, 1000)

    updateRequest := &si.UpdateRequest{
        Releases: &si.AllocationReleasesRequest{
            AllocationsToRelease: make([]*si.AllocationReleaseRequest, 0),
        },
        RmId: "rm:123",
    }

    // Release all allocations
    for _, v := range mockRM.Allocations {
        updateRequest.Releases.AllocationsToRelease = append(updateRequest.Releases.AllocationsToRelease, &si.AllocationReleaseRequest{
            Uuid:          v.Uuid,
            ApplicationId: v.ApplicationId,
            PartitionName: v.PartitionName,
        })
    }

    // Release Allocations.
    err = proxy.Update(updateRequest)

    if nil != err {
        t.Error(err.Error())
    }

    waitForAllocations(mockRM, 0, 1000)

    // Check pending resource, should be 200 (same)
    waitForPendingResource(t, schedulerQueueA, 200, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 200, 1000)
    waitForPendingResourceForApplication(t, schedulingApp, 200, 1000)

    // Check allocated resources of queues, apps should be 0 now
    assert.Assert(t, schedulerQueueA.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 0)
    assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 0)
    assert.Assert(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 0)

    // Release asks
    err = proxy.Update(&si.UpdateRequest{
        Releases: &si.AllocationReleasesRequest{
            AllocationAsksToRelease: []*si.AllocationAskReleaseRequest{
                {
                    ApplicationId: "app-1",
                    PartitionName: "default",
                },
            },
        },
        RmId: "rm:123",
    })

    // Release Allocations.
    err = proxy.Update(updateRequest)

    // Check pending resource
    waitForPendingResource(t, schedulerQueueA, 0, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 0, 1000)
    waitForPendingResourceForApplication(t, schedulingApp, 0, 1000)
}

func TestBasicSchedulerAutoAllocation(t *testing.T) {
    // Start all tests
    serviceContext := entrypoint.StartAllServices()
    defer serviceContext.StopAll()
    proxy := serviceContext.RMProxy
    cache := serviceContext.Cache
    scheduler := serviceContext.Scheduler

    // Register RM
    configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
              max:
                memory: 150
                vcore: 20
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
        t.Error(err.Error())
    }

    // Register a node, and add apps
    err = proxy.Update(&si.UpdateRequest{
        NewSchedulableNodes: []*si.NewNodeInfo{
            {
                NodeId: "node-1:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
            {
                NodeId: "node-2:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
        },
        NewApplications: newAddAppRequest(map[string]string{"app-1":"root.a"}),
        RmId: "rm:123",
    })

    // Check scheduling queue root
    schedulerQueueRoot := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")

    // Check scheduling queue a
    schedulerQueueA := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.a", "[rm:123]default")

    if nil != err {
        t.Error(err.Error())
    }

    waitForAcceptedApplications(mockRM, "app-1", 1000)
    waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
    waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

    // Get scheduling app
    schedulingApp := scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", "[rm:123]default")

    err = proxy.Update(&si.UpdateRequest{
        Asks: []*si.AllocationAsk{
            {
                AllocationKey: "alloc-1",
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 10},
                        "vcore":  {Value: 1},
                    },
                },
                MaxAllocations: 20,
                ApplicationId:  "app-1",
            },
        },
        RmId: "rm:123",
    })

    waitForAllocations(mockRM, 15, 1000)

    // Make sure pending resource updated to 0
    waitForPendingResource(t, schedulerQueueA, 50, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 50, 1000)
    waitForPendingResourceForApplication(t, schedulingApp, 50, 1000)

    // Check allocated resources of queues, apps
    assert.Assert(t, schedulerQueueA.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 150)
    assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 150)
    assert.Assert(t, schedulingApp.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 150)

    // Check allocated resources of nodes
    waitForNodesAllocatedResource(t, cache, "[rm:123]default", []string{"node-1:1234", "node-2:1234"}, 150, 1000)
}

func TestFairnessAllocationForQueues(t *testing.T) {
    // Start all tests
    serviceContext := entrypoint.StartAllServicesWithManualScheduler()
    defer serviceContext.StopAll()
    proxy := serviceContext.RMProxy
    scheduler := serviceContext.Scheduler

    // Register RM
    configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
          - name: b
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
        t.Error(err.Error())
    }

    // Register a node, and add apps
    err = proxy.Update(&si.UpdateRequest{
        NewSchedulableNodes: []*si.NewNodeInfo{
            {
                NodeId: "node-1:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
            {
                NodeId: "node-2:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
        },
        NewApplications: newAddAppRequest(map[string]string{"app-1":"root.a","app-2":"root.b"}),
        RmId: "rm:123",
    })

    // Check scheduling queue root
    schedulerQueueRoot := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")

    // Check scheduling queue a
    schedulerQueueA := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.a", "[rm:123]default")

    // Check scheduling queue b
    schedulerQueueB := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.b", "[rm:123]default")

    if nil != err {
        t.Error(err.Error())
    }

    waitForAcceptedApplications(mockRM, "app-1", 1000)
    waitForAcceptedApplications(mockRM, "app-2", 1000)
    waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
    waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

    // Get scheduling app
    // schedulingApp := scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", "[rm:123]default")

    err = proxy.Update(&si.UpdateRequest{
        Asks: []*si.AllocationAsk{
            {
                AllocationKey: "alloc-1",
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 10},
                        "vcore":  {Value: 1},
                    },
                },
                MaxAllocations: 20,
                ApplicationId:  "app-1",
            },
            {
                AllocationKey: "alloc-1",
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 10},
                        "vcore":  {Value: 1},
                    },
                },
                MaxAllocations: 20,
                ApplicationId:  "app-2",
            },
        },
        RmId: "rm:123",
    })

    // Check pending resource, should be 100 (same)
    waitForPendingResource(t, schedulerQueueA, 200, 1000)
    waitForPendingResource(t, schedulerQueueB, 200, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 400, 1000)

    for i := 0; i < 20; i++ {
        scheduler.SingleStepScheduleAllocTest(1)
        time.Sleep(time.Duration(100 * time.Millisecond))
    }

    waitForAllocations(mockRM, 20, 1000)

    // Make sure pending resource updated to 0
    waitForPendingResource(t, schedulerQueueA, 100, 1000)
    waitForPendingResource(t, schedulerQueueB, 100, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 200, 1000)
}

func TestFairnessAllocationForApplications(t *testing.T) {
    // Start all tests
    serviceContext := entrypoint.StartAllServicesWithManualScheduler()
    defer serviceContext.StopAll()
    proxy := serviceContext.RMProxy
    scheduler := serviceContext.Scheduler

    // Register RM
    configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            properties:
              application.sort.policy: fair
            resources:
              guaranteed:
                memory: 100
                vcore: 10
          - name: b
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
        t.Error(err.Error())
    }

    // Register a node, and add applications
    err = proxy.Update(&si.UpdateRequest{
        NewSchedulableNodes: []*si.NewNodeInfo{
            {
                NodeId: "node-1:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
            {
                NodeId: "node-2:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
        },
        NewApplications: newAddAppRequest(map[string]string{"app-1":"root.a","app-2":"root.a"}),
        RmId: "rm:123",
    })

    // Check scheduling queue root
    schedulerQueueRoot := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")

    // Check scheduling queue a
    schedulerQueueA := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.a", "[rm:123]default")

    // Check scheduling queue b
    schedulerQueueB := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.b", "[rm:123]default")

    if nil != err {
        t.Error(err.Error())
    }

    waitForAcceptedApplications(mockRM, "app-1", 1000)
    waitForAcceptedApplications(mockRM, "app-2", 1000)
    waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
    waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

    // Get scheduling app
    schedulingApp1 := scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", "[rm:123]default")
    schedulingApp2 := scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-2", "[rm:123]default")

    // Get scheduling app
    // schedulingApp := scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", "[rm:123]default")

    err = proxy.Update(&si.UpdateRequest{
        Asks: []*si.AllocationAsk{
            {
                AllocationKey: "alloc-1",
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 10},
                        "vcore":  {Value: 1},
                    },
                },
                MaxAllocations: 20,
                ApplicationId:  "app-1",
            },
            {
                AllocationKey: "alloc-1",
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 10},
                        "vcore":  {Value: 1},
                    },
                },
                MaxAllocations: 20,
                ApplicationId:  "app-2",
            },
        },
        RmId: "rm:123",
    })

    waitForPendingResource(t, schedulerQueueA, 400, 1000)
    waitForPendingResource(t, schedulerQueueB, 0, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 400, 1000)
    waitForPendingResourceForApplication(t, schedulingApp1, 200, 1000)
    waitForPendingResourceForApplication(t, schedulingApp2, 200, 1000)

    for i := 0; i < 20; i++ {
        scheduler.SingleStepScheduleAllocTest(1)
        time.Sleep(time.Duration(100 * time.Millisecond))
    }

    waitForAllocations(mockRM, 20, 1000)

    // Make sure pending resource updated to 100, which means
    waitForPendingResource(t, schedulerQueueA, 200, 1000)
    waitForPendingResource(t, schedulerQueueB, 0, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 200, 1000)
    waitForPendingResourceForApplication(t, schedulingApp1, 100, 1000)
    waitForPendingResourceForApplication(t, schedulingApp2, 100, 1000)

    // Both apps got 100 resources,
    assert.Assert(t, schedulingApp1.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 100)
    assert.Assert(t, schedulingApp2.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 100)
}

func TestRejectApplications(t *testing.T) {
    // Start all tests
    serviceContext := entrypoint.StartAllServicesWithManualScheduler()
    defer serviceContext.StopAll()
    proxy := serviceContext.RMProxy

    // Register RM
    configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            properties:
              application.sort.policy: fair
            resources:
              guaranteed:
                memory: 100
                vcore: 10
          - name: b
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
        t.Error(err.Error())
    }

    // Register a node, and add applications
    err = proxy.Update(&si.UpdateRequest{
        NewSchedulableNodes: []*si.NewNodeInfo{
            {
                NodeId: "node-1:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
            {
                NodeId: "node-2:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
        },
        RmId: "rm:123",
    })

    waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
    waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

    err = proxy.Update(&si.UpdateRequest{
        NewApplications: newAddAppRequest(map[string]string{"app-reject-1":"root.non-exist-queue"}),
        RmId: "rm:123",
    })

    waitForRejectedApplications(mockRM, "app-reject-1", 1000)

    err = proxy.Update(&si.UpdateRequest{
        NewApplications: newAddAppRequest(map[string]string{"app-added-2":"root.a"}),
        RmId: "rm:123",
    })

    waitForAcceptedApplications(mockRM, "app-added-2", 1000)
}

func TestSchedulingOverMaxCapacity(t *testing.T) {
    var parameters = []struct {
        name        string
        configData  string
        leafQueue   string
        askMemory   int64
        askCPU      int64
        numOfAsk    int32
    }{
        {"scheduleOverParentQueueCapacity",
            `
partitions:
  -
    name: default
    queues:
      -
        name: root
        submitacl: "*"
        queues:
          -
            name: parent
            resources:
              max:
                memory: 100
                vcore: 10
            queues:
              - name: child-1
              - name: child-2
`, "root.parent.child-1", 10, 1, 12},
        {"scheduleOverLeafQueueCapacity",
            `
partitions:
  -
    name: default
    queues:
      -
        name: root
        submitacl: "*"
        queues:
          -
            name: default
            resources:
              max:
                memory: 100
                vcore: 10
`, "root.default", 10, 1, 12},
    }

    for _, param := range parameters {
        t.Run(param.name, func(t *testing.T) {
            serviceContext := entrypoint.StartAllServicesWithManualScheduler()
            defer serviceContext.StopAll()

            configs.MockSchedulerConfigByData([]byte(param.configData))
            mockRM := NewMockRMCallbackHandler(t)
            proxy := serviceContext.RMProxy

            _, err := proxy.RegisterResourceManager(
                &si.RegisterResourceManagerRequest{
                    RmId:        "rm:123",
                    PolicyGroup: "policygroup",
                    Version:     "0.0.2",
                }, mockRM)

            if err != nil {
                t.Error(err.Error())
            }

            // Register a node, and add applications
            err = proxy.Update(&si.UpdateRequest{
                NewSchedulableNodes: []*si.NewNodeInfo{
                    {
                        NodeId: "node-1:1234",
                        Attributes: map[string]string{
                            "si.io/hostname": "node-1",
                            "si.io/rackname": "rack-1",
                        },
                        SchedulableResource: &si.Resource{
                            Resources: map[string]*si.Quantity{
                                "memory": {Value: 150},
                                "vcore":  {Value: 15},
                            },
                        },
                    },
                },
                NewApplications: newAddAppRequest(map[string]string{"app-1":param.leafQueue}),
                RmId: "rm:123",
            })

            waitForAcceptedNodes(mockRM, "node-1:1234", 1000)

            err = proxy.Update(&si.UpdateRequest{
                Asks: []*si.AllocationAsk{
                    {
                        AllocationKey: "alloc-1",
                        ResourceAsk: &si.Resource{
                            Resources: map[string]*si.Quantity{
                                "memory": {Value: param.askMemory},
                                "vcore":  {Value: param.askCPU},
                            },
                        },
                        MaxAllocations: param.numOfAsk,
                        ApplicationId:  "app-1",
                    },
                },
                RmId: "rm:123",
            })

            schedulingQueue := serviceContext.Scheduler.GetClusterSchedulingContext().
                GetSchedulingQueue(param.leafQueue, "[rm:123]default")

            waitForPendingResource(t, schedulingQueue, 120, 1000)

            for i := 0; i < 20; i++ {
                serviceContext.Scheduler.SingleStepScheduleAllocTest(1)
                time.Sleep(time.Duration(100 * time.Millisecond))
            }

            // 100 memory gets allocated, 20 pending because the capacity is 100
            waitForPendingResource(t, schedulingQueue, 20, 1000)
            apps := serviceContext.Cache.GetPartition("[rm:123]default").GetApplications()
            var app1 *cacheInfo.ApplicationInfo
            for _, app := range apps {
                if app.ApplicationId == "app-1" {
                    app1 = app
                }
            }
            assert.Assert(t, app1 != nil)
            assert.Equal(t, len(app1.GetAllAllocations()), 10)
            assert.Assert(t, app1.GetAllocatedResource().Resources[resources.MEMORY] == 100)
            assert.Equal(t, len(mockRM.getAllocations()), 10)

            // release all allocated allocations
            allocReleases := make([]*si.AllocationReleaseRequest, 0)
            for _, alloc := range mockRM.Allocations {
                allocReleases = append(allocReleases, &si.AllocationReleaseRequest{
                    PartitionName:        "default",
                    ApplicationId:        "app-1",
                    Uuid:                 alloc.Uuid,
                    Message:              "",
                })
            }

            err = proxy.Update(&si.UpdateRequest{
                Releases: &si.AllocationReleasesRequest{
                    AllocationsToRelease:    allocReleases,
                },
                RmId: "rm:123",
            })

            // schedule again, pending requests should be satisfied now
            for i := 0; i < 20; i++ {
                serviceContext.Scheduler.SingleStepScheduleAllocTest(1)
                time.Sleep(time.Duration(100 * time.Millisecond))
            }

            waitForPendingResource(t, schedulingQueue, 0, 1000)
            assert.Equal(t, len(mockRM.getAllocations()), 2)
        })
    }
}

// Test basic interactions from rm proxy to cache and to scheduler.
func TestRMNodeActions(t *testing.T) {
    // Start all tests
    serviceContext := entrypoint.StartAllServicesWithManualScheduler()
    defer serviceContext.StopAll()
    proxy := serviceContext.RMProxy
    cache := serviceContext.Cache
    scheduler := serviceContext.Scheduler

    // Register RM
    configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
              max:
                memory: 150
                vcore: 20
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
        t.Error(err.Error())
    }

    err = proxy.Update(&si.UpdateRequest{
        NewSchedulableNodes: []*si.NewNodeInfo{
            {
                NodeId: "node-1:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
            {
                NodeId: "node-2:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
        },
        NewApplications: newAddAppRequest(map[string]string{"app-1":"root.a"}),
        RmId: "rm:123",
    })

    if nil != err {
        t.Error(err.Error())
    }

    waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
    waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

    // verify scheduling nodes
    waitForSchedulingNode(t, scheduler, "node-1:1234", "[rm:123]default", 10000)
    waitForSchedulingNode(t, scheduler, "node-2:1234", "[rm:123]default", 10000)

    // verify all nodes are schedule-able
    assert.Equal(t, cache.GetPartition("[rm:123]default").GetNode("node-1:1234").IsSchedulable(), true)
    assert.Equal(t, cache.GetPartition("[rm:123]default").GetNode("node-2:1234").IsSchedulable(), true)

    // send RM node action: DRAIN_NODE
    err = proxy.Update(&si.UpdateRequest{
        UpdatedNodes: []*si.UpdateNodeInfo{
            {
                NodeId: "node-1:1234",
                Action: si.UpdateNodeInfo_DRAIN_NODE,
                Attributes: make(map[string]string),
            },
        },
        RmId: "rm:123",
    })

    if nil != err {
        t.Error(err.Error())
    }

    err = common.WaitFor(10*time.Millisecond, 5*time.Second, func() bool {
        return cache.GetPartition("[rm:123]default").GetNode("node-1:1234").IsSchedulable() == false &&
            cache.GetPartition("[rm:123]default").GetNode("node-2:1234").IsSchedulable()
    })

    if nil != err {
        t.Error(err.Error())
    }
    // verify that scheduling node (node-1) should be removed
    waitForSchedulingNodeRemoved(t, scheduler, "node-1:1234", "[rm:123]default", 10000)

    // send RM node action: DRAIN_TO_SCHEDULABLE
    err = proxy.Update(&si.UpdateRequest{
        UpdatedNodes: []*si.UpdateNodeInfo{
            {
                NodeId: "node-1:1234",
                Action: si.UpdateNodeInfo_DRAIN_TO_SCHEDULABLE,
                Attributes: make(map[string]string),
            },
        },
        RmId: "rm:123",
    })

    if nil != err {
        t.Error(err.Error())
    }

    err = common.WaitFor(10*time.Millisecond, 5*time.Second, func() bool {
        return cache.GetPartition("[rm:123]default").GetNode("node-1:1234").IsSchedulable() &&
            cache.GetPartition("[rm:123]default").GetNode("node-2:1234").IsSchedulable()
    })

    if nil != err {
        t.Error(err.Error())
    }
    // verify that scheduling node (node-1) should be added
    waitForSchedulingNode(t, scheduler, "node-1:1234", "[rm:123]default", 10000)
}


func TestBinPackingAllocationForApplications(t *testing.T) {
    // Start all tests
    serviceContext := entrypoint.StartAllServicesWithManualScheduler()
    defer serviceContext.StopAll()
    proxy := serviceContext.RMProxy
    cache := serviceContext.Cache
    scheduler := serviceContext.Scheduler

    // Register RM
    configData := `
partitions:
  -
    name: default
    nodesortpolicy:
        type: binpacking
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
          - name: b
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
        t.Fatalf("Register RM failed with error: %v", err)
    }

    // Register a node, and add applications
    err = proxy.Update(&si.UpdateRequest{
        NewSchedulableNodes: []*si.NewNodeInfo{
            {
                NodeId: "node-1:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
            {
                NodeId: "node-2:1234",
                Attributes: map[string]string{
                    "si.io/hostname": "node-1",
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 100},
                        "vcore":  {Value: 20},
                    },
                },
            },
        },
        NewApplications: newAddAppRequest(map[string]string{"app-1":"root.a","app-2":"root.a"}),
        RmId: "rm:123",
    })

    if err != nil {
        t.Fatalf("Proxy update failed with error: %v", err)
    }

    // Check scheduling queue root
    schedulerQueueRoot := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")

    // Check scheduling queue a
    schedulerQueueA := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.a", "[rm:123]default")

    // Check scheduling queue b
    schedulerQueueB := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.b", "[rm:123]default")

    waitForAcceptedApplications(mockRM, "app-1", 1000)
    waitForAcceptedApplications(mockRM, "app-2", 1000)
    waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
    waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

    // Get scheduling app
    schedulingApp1 := scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", "[rm:123]default")
    schedulingApp2 := scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-2", "[rm:123]default")

    err = proxy.Update(&si.UpdateRequest{
        Asks: []*si.AllocationAsk{
            {
                AllocationKey: "alloc-1",
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 10},
                        "vcore":  {Value: 1},
                    },
                },
                MaxAllocations: 20,
                ApplicationId:  "app-1",
            },
            {
                AllocationKey: "alloc-1",
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 10},
                        "vcore":  {Value: 1},
                    },
                },
                MaxAllocations: 20,
                ApplicationId:  "app-2",
            },
        },
        RmId: "rm:123",
    })

    if err != nil {
        t.Fatalf("Proxy update failed with error: %v", err)
    }

    waitForPendingResource(t, schedulerQueueA, 400, 1000)
    waitForPendingResource(t, schedulerQueueB, 0, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 400, 1000)
    waitForPendingResourceForApplication(t, schedulingApp1, 200, 1000)
    waitForPendingResourceForApplication(t, schedulingApp2, 200, 1000)

    for i := 0; i < 9; i++ {
        scheduler.SingleStepScheduleAllocTest(1)
        time.Sleep(100 * time.Millisecond)
    }

    waitForAllocations(mockRM, 9, 1000)

    totalNode1Resource := cache.GetPartition("[rm:123]default").GetNode("node-1:1234").GetAllocatedResource().Resources[resources.MEMORY]
    assert.Equal(t, int(totalNode1Resource), 90)

    totalNode2Resource := cache.GetPartition("[rm:123]default").GetNode("node-2:1234").GetAllocatedResource().Resources[resources.MEMORY]
    assert.Equal(t, int(totalNode2Resource), 0)
}

func TestFairnessAllocationForNodes(t *testing.T) {
    // Start all tests
    serviceContext := entrypoint.StartAllServicesWithManualScheduler()
    defer serviceContext.StopAll()
    proxy := serviceContext.RMProxy
    scheduler := serviceContext.Scheduler

    // Register RM
    configData := `
partitions:
  -
    name: default
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
        t.Error(err.Error())
    }

    // Register 10 nodes, and add applications
    nodes := make([]*si.NewNodeInfo, 0)
    for i := 0; i < 10; i++ {
        nodeId := "node-" + strconv.Itoa(i)
        nodes = append(nodes, &si.NewNodeInfo{
            NodeId: nodeId + ":1234",
            Attributes: map[string]string{
                "si.io/hostname": nodeId,
                "si.io/rackname": "rack-1",
            },
            SchedulableResource: &si.Resource{
                Resources: map[string]*si.Quantity{
                    "memory": {Value: 100},
                    "vcore":  {Value: 20},
                },
            },
        })
    }
    err = proxy.Update(&si.UpdateRequest{
        NewSchedulableNodes: nodes,
        NewApplications:     newAddAppRequest(map[string]string{"app-1": "root.a"}),
        RmId:                "rm:123",
    })
    if nil != err {
        t.Error(err.Error())
    }

    // verify app and all nodes are accepted
    waitForAcceptedApplications(mockRM, "app-1", 1000)
    for _, node := range nodes {
        waitForAcceptedNodes(mockRM, node.NodeId, 1000)
    }

    // Get scheduling app
    schedulerQueueA := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.a", "[rm:123]default")
    schedulingApp1 := scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", "[rm:123]default")

    // Request ask with 20 allocations
    err = proxy.Update(&si.UpdateRequest{
        Asks: []*si.AllocationAsk{
            {
                AllocationKey: "alloc-1",
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: 10},
                        "vcore":  {Value: 1},
                    },
                },
                MaxAllocations: 20,
                ApplicationId:  "app-1",
            },
        },
        RmId: "rm:123",
    })

    waitForPendingResource(t, schedulerQueueA, 200, 1000)
    waitForPendingResourceForApplication(t, schedulingApp1, 200, 1000)

    for i := 0; i < 20; i++ {
        scheduler.SingleStepScheduleAllocTest(1)
    }

    // Verify all requests are satisfied
    waitForAllocations(mockRM, 20, 1000)
    waitForPendingResource(t, schedulerQueueA, 0, 1000)
    waitForPendingResourceForApplication(t, schedulingApp1, 0, 1000)
    assert.Assert(t, schedulingApp1.ApplicationInfo.GetAllocatedResource().Resources[resources.MEMORY] == 200)

    // Verify 2 allocations for every node
    for _, node := range nodes {
        schedulingNode, _ := scheduler.GetClusterSchedulingContext().GetSchedulingNode(node.NodeId, "[rm:123]default")
        assert.Assert(t, schedulingNode.NodeInfo.GetAllocatedResource().Resources[resources.MEMORY] == 20)
        assert.Assert(t, schedulingNode.GetAllocatingResource().Resources[resources.MEMORY] == 0)
        assert.Assert(t, schedulingNode.GetAllocatingResource().Resources[resources.VCORE] == 0)
    }
}

//func TestScheduleBestEffortRequests(t *testing.T) {
//    // Start all tests
//    serviceContext := entrypoint.StartAllServicesWithManualScheduler()
//    defer serviceContext.StopAll()
//    proxy := serviceContext.RMProxy
//    cache := serviceContext.Cache
//    scheduler := serviceContext.Scheduler
//
//    // Register RM
//    configData := `
//partitions:
//  -
//    name: default
//    queues:
//      - name: root
//        submitacl: "*"
//        queues:
//          - name: a
//            resources:
//              guaranteed:
//                memory: 100
//                vcore: 10
//              max:
//                memory: 150
//                vcore: 20
//`
//    configs.MockSchedulerConfigByData([]byte(configData))
//    mockRM := NewMockRMCallbackHandler(t)
//
//    _, err := proxy.RegisterResourceManager(
//        &si.RegisterResourceManagerRequest{
//            RmId:        "rm:123",
//            PolicyGroup: "policygroup",
//            Version:     "0.0.2",
//        }, mockRM)
//
//    if err != nil {
//        t.Error(err.Error())
//    }
//
//    // Check queues of cache and scheduler.
//    partitionInfo := cache.GetPartition("[rm:123]default")
//    assert.Assert(t, nil == partitionInfo.Root.MaxResource)
//
//    // Check scheduling queue root
//    schedulerQueueRoot := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")
//    assert.Assert(t, nil == schedulerQueueRoot.CachedQueueInfo.MaxResource)
//
//    // Check scheduling queue a
//    schedulerQueueA := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.a", "[rm:123]default")
//    assert.Assert(t, 150 == schedulerQueueA.CachedQueueInfo.MaxResource.Resources[resources.MEMORY])
//
//    // Register a node, and add apps
//    err = proxy.Update(&si.UpdateRequest{
//        NewSchedulableNodes: []*si.NewNodeInfo{
//            {
//                NodeId: "node-1:1234",
//                Attributes: map[string]string{
//                    "si.io/hostname": "node-1",
//                    "si.io/rackname": "rack-1",
//                },
//                SchedulableResource: &si.Resource{
//                    Resources: map[string]*si.Quantity{
//                        "memory": {Value: 100},
//                        "vcore":  {Value: 20},
//                    },
//                },
//            },
//            {
//                NodeId: "node-2:1234",
//                Attributes: map[string]string{
//                    "si.io/hostname": "node-1",
//                    "si.io/rackname": "rack-1",
//                },
//                SchedulableResource: &si.Resource{
//                    Resources: map[string]*si.Quantity{
//                        "memory": {Value: 100},
//                        "vcore":  {Value: 20},
//                    },
//                },
//            },
//        },
//        NewApplications: newAddAppRequest(map[string]string{"app-1":"root.a"}),
//        RmId: "rm:123",
//    })
//
//    if nil != err {
//        t.Error(err.Error())
//    }
//
//    waitForAcceptedApplications(mockRM, "app-1", 1000)
//    waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
//    waitForAcceptedNodes(mockRM, "node-2:1234", 1000)
//
//    // Get scheduling app
//    // schedulingApp := scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", "[rm:123]default")
//
//    // Verify app initial state
//    app01, err := getApplicationInfoFromPartition(partitionInfo, "app-1")
//    assert.Assert(t, err == nil)
//    assert.Equal(t, app01.GetApplicationState(), cacheInfo.Accepted.String())
//
//    // submit best-effort requests, where resource ask is 0
//    err = proxy.Update(&si.UpdateRequest{
//        Asks: []*si.AllocationAsk{
//            {
//                AllocationKey: "alloc-1",
//                ResourceAsk: &si.Resource{
//                    Resources: map[string]*si.Quantity{
//                        "memory": {Value: 0},
//                        "vcore":  {Value: 0},
//                    },
//                },
//                MaxAllocations: 2,
//                ApplicationId:  "app-1",
//            },
//        },
//        RmId: "rm:123",
//    })
//
//    if nil != err {
//        t.Error(err.Error())
//    }
//
//    scheduler.SingleStepScheduleAllocTest(16)
//
//    waitForAllocations(mockRM, 2, 1000)
//}
