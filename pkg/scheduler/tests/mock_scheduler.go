/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package tests

import (
	"github.com/apache/incubator-yunikorn-core/pkg/api"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/entrypoint"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type mockScheduler struct {
	proxy          api.SchedulerAPI
	scheduler      *scheduler.Scheduler
	mockRM         *mockRMCallback
	serviceContext *entrypoint.ServiceContext
	rmID           string
}

func (m *mockScheduler) Init(config string) error {
	m.rmID = "rm:123"

	// Start all tests
	serviceContext := entrypoint.StartAllServicesWithManualScheduler()
	m.serviceContext = serviceContext
	m.proxy = serviceContext.RMProxy
	m.scheduler = serviceContext.Scheduler

	configs.MockSchedulerConfigByData([]byte(config))
	m.mockRM = NewMockRMCallbackHandler()

	_, err := m.proxy.RegisterResourceManager(
		&si.RegisterResourceManagerRequest{
			RmID:        m.rmID,
			PolicyGroup: "policygroup",
			Version:     "0.0.2",
		}, m.mockRM)
	return err
}

func (m *mockScheduler) addNode(nodeID string, resource *si.Resource) error {
	return m.proxy.Update(&si.UpdateRequest{
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
}

func (m *mockScheduler) addApp(appID string, queue string, partition string) error {
	return m.proxy.Update(&si.UpdateRequest{
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
}

func (m *mockScheduler) Stop() {
	if m.serviceContext != nil {
		m.serviceContext.StopAll()
	}
}
