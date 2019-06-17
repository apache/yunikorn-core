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
    "github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/configs"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/entrypoint"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/rmproxy"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/scheduler"
    "testing"
)

var TwoEqualQueueConfigEnabledPreemption = `
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


type MockScheduler struct {
    proxy          *rmproxy.RMProxy
    scheduler      *scheduler.Scheduler
    mockRM         *MockRMCallbackHandler
    serviceContext *entrypoint.ServiceContext
    t              *testing.T
    rmId           string
}

func (m *MockScheduler) Init(t *testing.T, config string) {
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
}

func (m *MockScheduler) AddApp(appId string, queue string, partition string) {
    // Register 2 node, and add apps
    err := m.proxy.Update(&si.UpdateRequest{
        NewApplications: []*si.AddApplicationRequest{
            {
                ApplicationId: appId,
                QueueName:     queue,
                PartitionName: partition,
            },
        },
        RmId: m.rmId,
    })

    if nil != err {
        m.t.Error(err.Error())
    }

    waitForAcceptedApplications(m.mockRM, appId, 1000)
}

func (m *MockScheduler) GetSchedulingQueue(queue string) *scheduler.SchedulingQueue {
    return m.scheduler.GetClusterSchedulingContext().GetSchedulingQueue(queue, "[rm:123]default")
}

func (m *MockScheduler) Stop() {
    if m.serviceContext != nil {
        m.serviceContext.StopAll()
    }
}
