package examples

import (
    "github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/common/configs"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/entrypoint"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/scheduler/tests"
)

func exampleOfRunYourOwnRM() {
    // Start all tests
    proxy, _, _ := entrypoint.StartAllServices()

    // Setup queues
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

    // Existing scheduler already supports read from config file, but to make test easier, you can
    configs.MockSchedulerConfigByData([]byte(configData))

    // Register RM
    mockRM := tests.NewMockRMCallbackHandler(nil) // [CHANGE THIS], should use your own implementation of api.ResourceManagerCallback

    _, err := proxy.RegisterResourceManager(
        &si.RegisterResourceManagerRequest{
            RmId:        "rm:123",
            PolicyGroup: "policygroup",
            Version:     "0.0.2",
        }, mockRM)

    if err != nil {
        panic(err)
    }

    // Register a node
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
        // Please note that RM id is very important, should not be empty, and remember always set it
        // for ALL update request
        RmId: "rm:123",
    })

    if err != nil {
        panic(err)
    }

    // (IMPORTANT)
    // Different from kubernetes, we need job for allocation ask. You can put all pod requests under the same job.
    // job name can be anything non-empty. Partition name can be empty (and internally becomes "default").
    err = proxy.Update(&si.UpdateRequest{
        NewJobs: []*si.AddJobRequest{
            {
                JobId:         "job-1",
                QueueName:     "a",
                PartitionName: "",
            },
        },
        RmId: "rm:123",
    })

    if err != nil {
        panic(err)
    }

    // Refer to schedulertestutils.go:30
    // You need to check job accepted by scheduler before proceed.

    // Send request
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
}
