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

package tests

import (
    "github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/configs"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/entrypoint"
    "gotest.tools/assert"
    "testing"
    "time"
)

// Test scheduler reconfiguration
func TestConfigScheduler(t *testing.T) {
    // Start all tests
    proxy, cache, scheduler := entrypoint.StartAllServicesWithManualScheduler()

    // Register RM
    configData := `
partitions:
  - name: default
    queues:
      - name: root
        queues:
          - name: base
            resources:
              guaranteed: {memory: 100, vcore: 10}
              max: {memory: 150, vcore: 20}
          - name: tobedeleted
`
    configs.MockSchedulerConfigByDataWithChecksum([]byte(configData), []byte("v0"))
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
    configs.FileCheckSummer = func(path string) (bytes []byte, e error) {return []byte("v1"), nil}
    configs.MockSchedulerConfigByDataWithChecksum([]byte(configData), []byte("v1"))
    err = proxy.ReloadConfiguration("rm:123")

    // wait until configuration reloaded
    waitForCacheState(t, cache, []byte("v1"), 10000)

    if err != nil {
        t.Errorf("configuration reload failed: %v", err)
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
    proxy, cache, scheduler := entrypoint.StartAllServicesWithManualScheduler()

    // Register RM
    configData := `
partitions:
  -
    name: default
    queues:
      - name: root
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
        NewApplications: []*si.AddApplicationRequest{
            {
                ApplicationId:         "app-1",
                QueueName:     "root.a",
                PartitionName: "",
            },
        },
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
                QueueName:      "root.a",
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

    scheduler.SingleStepSchedule(16)

    waitForAllocations(mockRM, 2, 1000)

    // Make sure pending resource updated to 0
    waitForPendingResource(t, schedulerQueueA, 0, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 0, 1000)
    waitForPendingResourceForApplication(t, schedulingApp, 0, 1000)

    // Check allocated resources of queues, apps
    assert.Assert(t, schedulerQueueA.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 20)
    assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 20)
    assert.Assert(t, schedulingApp.ApplicationInfo.AllocatedResource.Resources[resources.MEMORY] == 20)

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
                QueueName:      "root.a",
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
                QueueName:      "root.a",
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
    scheduler.SingleStepSchedule(16)

    waitForAllocations(mockRM, 4, 1000)

    // Check pending resource, should be 200 now.
    waitForPendingResource(t, schedulerQueueA, 200, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 200, 1000)
    waitForPendingResourceForApplication(t, schedulingApp, 200, 1000)

    // Check allocated resources of queues, apps
    assert.Assert(t, schedulerQueueA.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 120)
    assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 120)
    assert.Assert(t, schedulingApp.ApplicationInfo.AllocatedResource.Resources[resources.MEMORY] == 120)

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
            PartitionName: v.Partition,
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
    assert.Assert(t, schedulerQueueA.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 0)
    assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 0)
    assert.Assert(t, schedulingApp.ApplicationInfo.AllocatedResource.Resources[resources.MEMORY] == 0)

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
    proxy, cache, scheduler := entrypoint.StartAllServices()

    // Register RM
    configData := `
partitions:
  -
    name: default
    queues:
      - name: root
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
        NewApplications: []*si.AddApplicationRequest{
            {
                ApplicationId:   "app-1",
                QueueName:       "root.a",
                PartitionName:   "",
            },
        },
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
                QueueName:      "root.a",
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
    assert.Assert(t, schedulerQueueA.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 150)
    assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 150)
    assert.Assert(t, schedulingApp.ApplicationInfo.AllocatedResource.Resources[resources.MEMORY] == 150)

    // Check allocated resources of nodes
    waitForNodesAllocatedResource(t, cache, "[rm:123]default", []string{"node-1:1234", "node-2:1234"}, 150, 1000)
}

func TestFairnessAllocationForQueues(t *testing.T) {
    // Start all tests
    proxy, _, scheduler := entrypoint.StartAllServicesWithManualScheduler()

    // Register RM
    configData := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        queues:
          -
            name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
          -
            name: b
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
        NewApplications: []*si.AddApplicationRequest{
            {
                ApplicationId: "app-1",
                QueueName:     "root.a",
                PartitionName: "",
            },
            {
                ApplicationId: "app-2",
                QueueName:     "root.b",
                PartitionName: "",
            },
        },
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
                QueueName:      "root.a",
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
                QueueName:      "root.b",
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
        scheduler.SingleStepSchedule(1)
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
    proxy, _, scheduler := entrypoint.StartAllServicesWithManualScheduler()

    // Register RM
    configData := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        queues:
          -
            name: a
            properties:
              application.sort.policy: fair
            resources:
              guaranteed:
                memory: 100
                vcore: 10
          -
            name: b
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
        NewApplications: []*si.AddApplicationRequest{
            {
                ApplicationId: "app-1",
                QueueName:     "root.a",
                PartitionName: "",
            },
            {
                ApplicationId: "app-2",
                QueueName:     "root.a",
                PartitionName: "",
            },
        },
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
                QueueName:      "root.a",
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
                QueueName:      "root.b",
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
        scheduler.SingleStepSchedule(1)
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
    assert.Assert(t, schedulingApp1.ApplicationInfo.AllocatedResource.Resources[resources.MEMORY] == 100)
    assert.Assert(t, schedulingApp2.ApplicationInfo.AllocatedResource.Resources[resources.MEMORY] == 100)
}

func TestRejectApplications(t *testing.T) {
    // Start all tests
    proxy, _, _ := entrypoint.StartAllServicesWithManualScheduler()

    // Register RM
    configData := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        queues:
          -
            name: a
            properties:
              application.sort.policy: fair
            resources:
              guaranteed:
                memory: 100
                vcore: 10
          -
            name: b
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
        NewApplications: []*si.AddApplicationRequest{
           {
               ApplicationId: "app-1",
               QueueName:     "root.non-exist-queue",
           },
        },
        RmId: "rm:123",
    })

    waitForRejectedApplications(mockRM, "app-1", 1000)

    err = proxy.Update(&si.UpdateRequest{
        NewApplications: []*si.AddApplicationRequest{
            {
                ApplicationId: "app-1",
                QueueName:     "root.a",
            },
        },
        RmId: "rm:123",
    })

    waitForAcceptedApplications(mockRM, "app-1", 1000)
}