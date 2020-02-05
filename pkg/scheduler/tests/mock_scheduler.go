/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

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
	"testing"

	"github.com/apache/incubator-yunikorn-core/pkg/api"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/entrypoint"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

var TwoEqualQueueConfigEnabledPreemption = `
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

type MockScheduler struct {
	proxy          api.SchedulerAPI
	scheduler      *scheduler.Scheduler
	mockRM         *MockRMCallbackHandler
	serviceContext *entrypoint.ServiceContext
	t              *testing.T
	rmID           string
}

func (m *MockScheduler) Init(t *testing.T, config string) {
	m.rmID = "rm:123"
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
			RmID:        m.rmID,
			PolicyGroup: "policygroup",
			Version:     "0.0.2",
		}, m.mockRM)

	if err != nil {
		t.Error(err.Error())
	}
}

func (m *MockScheduler) AddNode(nodeID string, resource *si.Resource) {
	err := m.proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeID: nodeID,
				Attributes: map[string]string{
					"si.io/hostname": nodeID,
					"si.io/rackname": "rack-1",
				},
				SchedulableResource: resource,
			},
		},
		RmID: m.rmID,
	})

	if err != nil {
		m.t.Error(err.Error())
	}

	waitForAcceptedNodes(m.mockRM, nodeID, 1000)
}

func (m *MockScheduler) AddApp(appID string, queue string, partition string) {
	// Register 2 node, and add apps
	err := m.proxy.Update(&si.UpdateRequest{
		NewApplications: []*si.AddApplicationRequest{
			{
				ApplicationID: appID,
				QueueName:     queue,
				PartitionName: partition,
				Ugi: &si.UserGroupInformation{
					User: "testuser",
				},
			},
		},
		RmID: m.rmID,
	})

	if nil != err {
		m.t.Error(err.Error())
	}

	waitForAcceptedApplications(m.mockRM, appID, 1000)
}

func (m *MockScheduler) GetSchedulingQueue(queue string) *scheduler.SchedulingQueue {
	return m.scheduler.GetClusterSchedulingContext().GetSchedulingQueue(queue, "[rm:123]default")
}

func (m *MockScheduler) Stop() {
	if m.serviceContext != nil {
		m.serviceContext.StopAll()
	}
}
