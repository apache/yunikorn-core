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
    "github.com/cloudera/yunikorn-core/pkg/api"
    "github.com/cloudera/yunikorn-core/pkg/common"
    "github.com/cloudera/yunikorn-core/pkg/common/configs"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/entrypoint"
    "github.com/cloudera/yunikorn-core/pkg/scheduler"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "testing"
)

const TwoEqualQueueConfigEnabledPreemption = `
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
                vcore: 100
              max:
                memory: 200
                vcore: 200
          - name: b
            resources:
              guaranteed:
                memory: 100
                vcore: 100
              max:
                memory: 200
                vcore: 200
    preemption:
      enabled: true
`

const TwoEqualQueueConfigDisabledPreemption = `
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
                vcore: 100
              max:
                memory: 500
                vcore: 500
          - name: b
            resources:
              guaranteed:
                memory: 100
                vcore: 100
              max:
                memory: 500
                vcore: 500
    preemption:
      enabled: false
`

const OneQueueConfig = `
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

type MockScheduler struct {
    proxy          api.SchedulerApi
    scheduler      *scheduler.Scheduler
    mockRM         *MockRMCallbackHandler
    serviceContext *entrypoint.ServiceContext
    t              *testing.T
    nodes          map[string]*scheduler.SchedulingNode
    rmId           string
}

func (m* MockScheduler) _partition(partition string) string {
    return common.GetNormalizedPartitionName(partition, m.rmId)
}

func (m *MockScheduler) Init(t *testing.T, config string) {
    m.nodes = make(map[string]*scheduler.SchedulingNode)
    m.rmId = "rm:123"
    m.t = t

    // Start all tests
    serviceContext := entrypoint.StartAllServicesWithManualScheduler()
    m.serviceContext = serviceContext
    m.proxy = serviceContext.RMProxy
    m.scheduler = serviceContext.Scheduler

    configs.MockSchedulerConfigByData([]byte(config))
    m.mockRM = NewMockRMCallbackHandler(t)

    _, err := m.proxy.RegisterResourceManager(
        &si.RegisterResourceManagerRequest{
            RmId:        m.rmId,
            PolicyGroup: "policygroup",
            Version:     "0.0.2",
        }, m.mockRM)

    if err != nil {
        t.Error(err.Error())
    }
}

func (m *MockScheduler) AddNode(nodeId string, resource *si.Resource) {
    err := m.proxy.Update(&si.UpdateRequest{
        NewSchedulableNodes: []*si.NewNodeInfo{
            {
                NodeId: nodeId,
                Attributes: map[string]string{
                    "si.io/hostname": nodeId,
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: resource,
            },
        },
        RmId: m.rmId,
    })

    if err != nil {
        m.t.Error(err.Error())
    }

    waitForAcceptedNodes(m.mockRM, nodeId, 1000)
    m.nodes[nodeId] = m.scheduler.GetClusterSchedulingContext().GetSchedulingNode(nodeId, m._partition("default"))
}

func (m *MockScheduler) AddNodeWithMemAndCpu(nodeId string, mem int64, vcore int64) {
    err := m.proxy.Update(&si.UpdateRequest{
        NewSchedulableNodes: []*si.NewNodeInfo{
            {
                NodeId: nodeId,
                Attributes: map[string]string{
                    "si.io/hostname": nodeId,
                    "si.io/rackname": "rack-1",
                },
                SchedulableResource: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: mem},
                        "vcore":  {Value: vcore},
                    },
                },
            },
        },
        RmId: m.rmId,
    })

    if err != nil {
        m.t.Error(err.Error())
    }

    waitForAcceptedNodes(m.mockRM, nodeId, 1000)
}

func (m *MockScheduler) AddApp(appId string, queue string, partition string) {
    // Register 2 node, and add apps
    err := m.proxy.Update(&si.UpdateRequest{
        NewApplications: []*si.AddApplicationRequest{
            {
                ApplicationId: appId,
                QueueName:     queue,
                PartitionName: partition,
                Ugi: &si.UserGroupInformation{
                    User: "testuser",
                },
            },
        },
        RmId: m.rmId,
    })

    if nil != err {
        m.t.Error(err.Error())
    }

    waitForAcceptedApplications(m.mockRM, appId, 1000)
}

func (m* MockScheduler) AppRequestResource(appId string, allocId string, memPerRequest int64, vcorePerRequest int64, repeat int32) {
    app := m.scheduler.GetClusterSchedulingContext().GetSchedulingApplication(appId, m._partition("default"))
    pendingBefore := app.Requests.GetPendingResource().Resources["memory"]

    m.proxy.Update(&si.UpdateRequest{
        Asks: []*si.AllocationAsk{
            {
                AllocationKey: allocId,
                ResourceAsk: &si.Resource{
                    Resources: map[string]*si.Quantity{
                        "memory": {Value: memPerRequest},
                        "vcore":  {Value: vcorePerRequest},
                    },
                },
                MaxAllocations: repeat,
                ApplicationId:  appId,
            },
        },
        RmId: m.rmId,
    })

    waitForPendingResourceForApplication(m.t, app, pendingBefore+resources.Quantity(memPerRequest)*resources.Quantity(repeat), 1000)
}

func (m* MockScheduler) RemoveApp(appId string, partition string) {
    m.proxy.Update(&si.UpdateRequest{
        RemoveApplications: newRemoveAppRequest([]string{appId}),
        RmId: m.rmId,
    })
}

func (m *MockScheduler) GetSchedulingQueue(queue string) *scheduler.SchedulingQueue {
    return m.scheduler.GetClusterSchedulingContext().GetSchedulingQueue(queue, "[rm:123]default")
}

func (m *MockScheduler) Stop() {
    if m.serviceContext != nil {
        m.serviceContext.StopAll()
    }
}
