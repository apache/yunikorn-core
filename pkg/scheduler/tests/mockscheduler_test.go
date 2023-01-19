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
	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/entrypoint"
	"github.com/apache/yunikorn-core/pkg/scheduler"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type mockScheduler struct {
	proxy          api.SchedulerAPI
	scheduler      *scheduler.Scheduler
	mockRM         *mockRMCallback
	serviceContext *entrypoint.ServiceContext
	rmID           string
	partitionName  string
}

// Create the mock scheduler with the config provided.
// The scheduler in the tests is normally the manual scheduler: the code must call
// MultiStepSchedule(int) to allocate.
// Auto scheduling does not give control over the scheduling steps and should only
// be used in specific use case testing.
func (m *mockScheduler) Init(config string, autoSchedule bool) error {
	m.rmID = "rm:123"
	m.partitionName = common.GetNormalizedPartitionName("default", m.rmID)

	BuildInfoMap := make(map[string]string)
	BuildInfoMap["k"] = "v"

	// Start all tests
	if autoSchedule {
		m.serviceContext = entrypoint.StartAllServices()
	} else {
		m.serviceContext = entrypoint.StartAllServicesWithManualScheduler()
	}
	m.proxy = m.serviceContext.RMProxy
	m.scheduler = m.serviceContext.Scheduler

	m.mockRM = newMockRMCallbackHandler()

	_, err := m.proxy.RegisterResourceManager(
		&si.RegisterResourceManagerRequest{
			RmID:        m.rmID,
			PolicyGroup: "policygroup",
			Version:     "0.0.2",
			BuildInfo:   BuildInfoMap,
			Config:      config,
		}, m.mockRM)
	return err
}

func (m *mockScheduler) Stop() {
	if m.serviceContext != nil {
		m.serviceContext.StopAll()
	}
}

func (m *mockScheduler) addNode(nodeID string, resource *si.Resource) error {
	return m.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:              nodeID,
				Attributes:          map[string]string{},
				SchedulableResource: resource,
				Action:              si.NodeInfo_CREATE,
			},
		},
		RmID: m.rmID,
	})
}

func (m *mockScheduler) removeNode(nodeID string) error {
	return m.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID: nodeID,
				Action: si.NodeInfo_DECOMISSION,
			},
		},
		RmID: m.rmID,
	})
}

func (m *mockScheduler) addApp(appID string, queue string, partition string) error {
	return m.proxy.UpdateApplication(&si.ApplicationRequest{
		New: []*si.AddApplicationRequest{
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

func (m *mockScheduler) removeApp(appID, partition string) error {
	return m.proxy.UpdateApplication(&si.ApplicationRequest{
		Remove: []*si.RemoveApplicationRequest{
			{
				ApplicationID: appID,
				PartitionName: partition,
			},
		},
		RmID: m.rmID,
	})
}

func (m *mockScheduler) addAppRequest(appID, allocID string, resource *si.Resource, repeat int32) error {
	return m.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey:  allocID,
				ApplicationID:  appID,
				ResourceAsk:    resource,
				MaxAllocations: repeat,
			},
		},
		RmID: m.rmID,
	})
}

func (m *mockScheduler) releaseAllocRequest(appID, uuid string) error {
	return m.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: []*si.AllocationRelease{
				{
					ApplicationID: appID,
					UUID:          uuid,
					PartitionName: m.partitionName,
				},
			},
		},
		RmID: m.rmID,
	})
}

func (m *mockScheduler) releaseAskRequest(appID, allocKey string) error {
	return m.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationAsksToRelease: []*si.AllocationAskRelease{
				{
					ApplicationID: appID,
					AllocationKey: allocKey,
					PartitionName: m.partitionName,
				},
			},
		},
		RmID: m.rmID,
	})
}

// simple wrapper to limit the repeating code getting the queue
func (m *mockScheduler) getNode(nodeName string) *objects.Node {
	return m.scheduler.GetClusterContext().GetNode(nodeName, m.partitionName)
}

// simple wrapper to limit the repeating code getting the queue
func (m *mockScheduler) getQueue(queueName string) *objects.Queue {
	return m.scheduler.GetClusterContext().GetQueue(queueName, m.partitionName)
}

// simple wrapper to limit the repeating code getting the queue with non default partition
func (m *mockScheduler) getPartitionQueue(queueName, partitionName string) *objects.Queue {
	return m.scheduler.GetClusterContext().GetQueue(queueName, partitionName)
}

// simple wrapper to limit the repeating code getting the app
func (m *mockScheduler) getApplication(appID string) *objects.Application {
	return m.scheduler.GetClusterContext().GetApplication(appID, m.partitionName)
}
