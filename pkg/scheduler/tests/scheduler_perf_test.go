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
    "github.com/cloudera/yunikorn-core/pkg/entrypoint"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "go.uber.org/zap"
    "strconv"
    "testing"
    "time"
)

func TestSchedulerThroughput5KNodes(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping performance testing in short mode")
    }
    log.InitAndSetLevel(zap.WarnLevel)
    // Start all tests
    serviceContext := entrypoint.StartAllServices()
    defer serviceContext.StopAll()
    proxy := serviceContext.RMProxy
    // scheduler := serviceContext.Scheduler

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
                memory: 100000
                vcore: 10000
          - name: b
            resources:
              guaranteed:
                memory: 1000000
                vcore: 10000
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
        NewApplications: newAddAppRequest(map[string]string{"app-1":"root.a", "app-2": "root.b"}),
        RmId: "rm:123",
    })

    // Check scheduling queue root
    // schedulerQueueRoot := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")

    // Check scheduling queue a
    // schedulerQueueA := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.a", "[rm:123]default")

    if nil != err {
        t.Error(err.Error())
    }

    waitForAcceptedApplications(mockRM, "app-1", 1000)
    waitForAcceptedApplications(mockRM, "app-2", 1000)

    var newNodes []*si.NewNodeInfo

    // register 5000 nodes
    totalNode := 5000
    for i:=0; i < totalNode; i++ {
        nodeName := "node-" + strconv.Itoa(i)
        node := &si.NewNodeInfo{
            NodeId:
            nodeName + ":1234",
            Attributes: map[string]string{
                "si.io/hostname": nodeName,
                "si.io/rackname": "rack-1",
            },
            SchedulableResource: &si.Resource{
                Resources: map[string]*si.Quantity{
                    "memory": {Value: 200},
                    "vcore":  {Value: 20},
                },
            },
        }

        newNodes = append(newNodes, node)
    }

    err = proxy.Update(&si.UpdateRequest{
        RmId: "rm:123",
        NewSchedulableNodes: newNodes,
    })

    startTime := time.Now()
    waitForMinNumberOfAcceptedNodes(mockRM, totalNode, 5000)
    duration := time.Now().Sub(startTime)

    t.Logf("Total time to add %d node %s, %f per second", totalNode, duration, float64(totalNode) / duration.Seconds())

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
                MaxAllocations: 200000,
                ApplicationId:  "app-1",
            },
        },
        RmId: "rm:123",
    })

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
                MaxAllocations: 200000,
                ApplicationId:  "app-2",
            },
        },
        RmId: "rm:123",
    })

    // test number of allocations, should be less than or equal to 100000 (200 * 5000 / 10)
    testNumAllocations := 10000

    // Wait for maximum 2 mins
    startTime = time.Now()
    waitForMinAllocations(mockRM, testNumAllocations, 120000)
    duration = time.Now().Sub(startTime)

    t.Logf("Total time to allocate %d containers in %s, %f per second", testNumAllocations, duration, float64(testNumAllocations) / duration.Seconds())
}