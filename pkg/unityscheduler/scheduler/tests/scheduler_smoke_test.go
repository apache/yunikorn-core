package tests

import (
    "github.com/universal-scheduler/scheduler-spec/lib/go/si"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/common/configs"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/common/resources"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/entrypoint"
    "gotest.tools/assert"
    "testing"
    "time"
)

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
      -
        name: root
        children:
          - a
        resources:
          guaranteed:
            memory: 200
            vcore: 20
          max:
            memory: 200
            vcore: 20
      -
        name: a
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
    assert.Assert(t, 200 == partitionInfo.Root.MaxResource.Resources[resources.MEMORY])

    // Check scheduling queue root
    schedulerQueueRoot := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")
    assert.Assert(t, 200 == schedulerQueueRoot.CachedQueueInfo.MaxResource.Resources[resources.MEMORY])

    // Check scheduling queue a
    schedulerQueueA := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("a", "[rm:123]default")
    assert.Assert(t, 150 == schedulerQueueA.CachedQueueInfo.MaxResource.Resources[resources.MEMORY])

    // Register a node, and add jobs
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
        NewJobs: []*si.AddJobRequest{
            {
                JobId:         "job-1",
                QueueName:     "a",
                PartitionName: "",
            },
        },
        RmId: "rm:123",
    })

    if nil != err {
        t.Error(err.Error())
    }

    waitForAcceptedJobs(mockRM, "job-1", 1000)
    waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
    waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

    // Get scheduling job
    schedulingJob := scheduler.GetClusterSchedulingContext().GetSchedulingJob("job-1", "[rm:123]default")

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
                QueueName:      "a",
                JobId:          "job-1",
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
    waitForPendingResourceForJob(t, schedulingJob, 20, 1000)

    scheduler.SingleStepSchedule(16)

    waitForAllocations(mockRM, 2, 1000)

    // Make sure pending resource updated to 0
    waitForPendingResource(t, schedulerQueueA, 0, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 0, 1000)
    waitForPendingResourceForJob(t, schedulingJob, 0, 1000)

    // Check allocated resources of queues, jobs
    assert.Assert(t, schedulerQueueA.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 20)
    assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 20)
    assert.Assert(t, schedulingJob.JobInfo.AllocatedResource.Resources[resources.MEMORY] == 20)

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
                QueueName:      "a",
                JobId:          "job-1",
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
                QueueName:      "a",
                JobId:          "job-1",
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
    waitForPendingResourceForJob(t, schedulingJob, 300, 1000)

    // Now job-1 uses 20 resource, and queue-a's max = 150, so it can get two 50 container allocated.
    scheduler.SingleStepSchedule(16)

    waitForAllocations(mockRM, 4, 1000)

    // Check pending resource, should be 200 now.
    waitForPendingResource(t, schedulerQueueA, 200, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 200, 1000)
    waitForPendingResourceForJob(t, schedulingJob, 200, 1000)

    // Check allocated resources of queues, jobs
    assert.Assert(t, schedulerQueueA.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 120)
    assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 120)
    assert.Assert(t, schedulingJob.JobInfo.AllocatedResource.Resources[resources.MEMORY] == 120)

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
            JobId:         v.JobId,
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
    waitForPendingResourceForJob(t, schedulingJob, 200, 1000)

    // Check allocated resources of queues, jobs should be 0 now
    assert.Assert(t, schedulerQueueA.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 0)
    assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 0)
    assert.Assert(t, schedulingJob.JobInfo.AllocatedResource.Resources[resources.MEMORY] == 0)

    // Release asks
    err = proxy.Update(&si.UpdateRequest{
        Releases: &si.AllocationReleasesRequest{
            AllocationAsksToRelease: []*si.AllocationAskReleaseRequest{
                {
                    JobId:         "job-1",
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
    waitForPendingResourceForJob(t, schedulingJob, 0, 1000)
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
      -
        name: root
        children:
          - a
        resources:
          guaranteed:
            memory: 200
            vcore: 20
          max:
            memory: 200
            vcore: 20
      -
        name: a
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

    // Register a node, and add jobs
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
        NewJobs: []*si.AddJobRequest{
            {
                JobId:         "job-1",
                QueueName:     "a",
                PartitionName: "",
            },
        },
        RmId: "rm:123",
    })

    // Check scheduling queue root
    schedulerQueueRoot := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")

    // Check scheduling queue a
    schedulerQueueA := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("a", "[rm:123]default")

    if nil != err {
        t.Error(err.Error())
    }

    waitForAcceptedJobs(mockRM, "job-1", 1000)
    waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
    waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

    // Get scheduling job
    schedulingJob := scheduler.GetClusterSchedulingContext().GetSchedulingJob("job-1", "[rm:123]default")

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
                QueueName:      "a",
                JobId:          "job-1",
            },
        },
        RmId: "rm:123",
    })

    waitForAllocations(mockRM, 15, 1000)

    // Make sure pending resource updated to 0
    waitForPendingResource(t, schedulerQueueA, 50, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 50, 1000)
    waitForPendingResourceForJob(t, schedulingJob, 50, 1000)

    // Check allocated resources of queues, jobs
    assert.Assert(t, schedulerQueueA.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 150)
    assert.Assert(t, schedulerQueueRoot.CachedQueueInfo.AllocatedResource.Resources[resources.MEMORY] == 150)
    assert.Assert(t, schedulingJob.JobInfo.AllocatedResource.Resources[resources.MEMORY] == 150)

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
        children:
          - a
          - b
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

    // Register a node, and add jobs
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
        NewJobs: []*si.AddJobRequest{
            {
                JobId:         "job-1",
                QueueName:     "a",
                PartitionName: "",
            },
            {
                JobId:         "job-2",
                QueueName:     "b",
                PartitionName: "",
            },
        },
        RmId: "rm:123",
    })

    // Check scheduling queue root
    schedulerQueueRoot := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")

    // Check scheduling queue a
    schedulerQueueA := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("a", "[rm:123]default")

    // Check scheduling queue b
    schedulerQueueB := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("b", "[rm:123]default")

    if nil != err {
        t.Error(err.Error())
    }

    waitForAcceptedJobs(mockRM, "job-1", 1000)
    waitForAcceptedJobs(mockRM, "job-2", 1000)
    waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
    waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

    // Get scheduling job
    // schedulingJob := scheduler.GetClusterSchedulingContext().GetSchedulingJob("job-1", "[rm:123]default")

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
                QueueName:      "a",
                JobId:          "job-1",
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
                QueueName:      "b",
                JobId:          "job-2",
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

func TestFairnessAllocationForJobs(t *testing.T) {
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
        children:
          - a
          - b
      -
        name: a
        properties:
          job.sort.policy: fair
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

    // Register a node, and add jobs
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
        NewJobs: []*si.AddJobRequest{
            {
                JobId:         "job-1",
                QueueName:     "a",
                PartitionName: "",
            },
            {
                JobId:         "job-2",
                QueueName:     "a",
                PartitionName: "",
            },
        },
        RmId: "rm:123",
    })

    // Check scheduling queue root
    schedulerQueueRoot := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")

    // Check scheduling queue a
    schedulerQueueA := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("a", "[rm:123]default")

    // Check scheduling queue b
    schedulerQueueB := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("b", "[rm:123]default")

    if nil != err {
        t.Error(err.Error())
    }

    waitForAcceptedJobs(mockRM, "job-1", 1000)
    waitForAcceptedJobs(mockRM, "job-2", 1000)
    waitForAcceptedNodes(mockRM, "node-1:1234", 1000)
    waitForAcceptedNodes(mockRM, "node-2:1234", 1000)

    // Get scheduling job
    schedulingJob1 := scheduler.GetClusterSchedulingContext().GetSchedulingJob("job-1", "[rm:123]default")
    schedulingJob2 := scheduler.GetClusterSchedulingContext().GetSchedulingJob("job-2", "[rm:123]default")

    // Get scheduling job
    // schedulingJob := scheduler.GetClusterSchedulingContext().GetSchedulingJob("job-1", "[rm:123]default")

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
                QueueName:      "a",
                JobId:          "job-1",
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
                QueueName:      "b",
                JobId:          "job-2",
            },
        },
        RmId: "rm:123",
    })

    waitForPendingResource(t, schedulerQueueA, 400, 1000)
    waitForPendingResource(t, schedulerQueueB, 0, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 400, 1000)
    waitForPendingResourceForJob(t, schedulingJob1, 200, 1000)
    waitForPendingResourceForJob(t, schedulingJob2, 200, 1000)

    for i := 0; i < 20; i++ {
        scheduler.SingleStepSchedule(1)
        time.Sleep(time.Duration(100 * time.Millisecond))
    }

    waitForAllocations(mockRM, 20, 1000)

    // Make sure pending resource updated to 100, which means
    waitForPendingResource(t, schedulerQueueA, 200, 1000)
    waitForPendingResource(t, schedulerQueueB, 0, 1000)
    waitForPendingResource(t, schedulerQueueRoot, 200, 1000)
    waitForPendingResourceForJob(t, schedulingJob1, 100, 1000)
    waitForPendingResourceForJob(t, schedulingJob2, 100, 1000)

    // Both jobs got 100 resources,
    assert.Assert(t, schedulingJob1.JobInfo.AllocatedResource.Resources[resources.MEMORY] == 100)
    assert.Assert(t, schedulingJob2.JobInfo.AllocatedResource.Resources[resources.MEMORY] == 100)
}
