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
	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common"
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
	clusterInfo    *cache.ClusterInfo
	rmID           string
	partitionName  string
}

// Create the mock sceduler with the config provided.
// The scheduler in the tests is normally the manual scheduler: the code must call
// MultiStepSchedule(int) to allocate.
// Auto scheduling does not give control over the scheduling steps and should only
// be used in specific use case testing.
func (m *mockScheduler) Init(config string, autoSchedule bool) error {
	m.rmID = "rm:123"
	m.partitionName = common.GetNormalizedPartitionName("default", m.rmID)

	// Start all tests
	if autoSchedule {
		m.serviceContext = entrypoint.StartAllServices()
	} else {
		m.serviceContext = entrypoint.StartAllServicesWithManualScheduler()
	}
	m.proxy = m.serviceContext.RMProxy
	m.clusterInfo = m.serviceContext.Cache
	m.scheduler = m.serviceContext.Scheduler

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

func (m *mockScheduler) Stop() {
	if m.serviceContext != nil {
		m.serviceContext.StopAll()
	}
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

func (m *mockScheduler) removeNode(nodeID string) error {
	return m.proxy.Update(&si.UpdateRequest{
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID:     nodeID,
				Action:     si.UpdateNodeInfo_DECOMISSION,
				Attributes: map[string]string{},
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

func (m *mockScheduler) removeApp(appID, partition string) error {
	return m.proxy.Update(&si.UpdateRequest{
		RemoveApplications: []*si.RemoveApplicationRequest{
			{
				ApplicationID: appID,
				PartitionName: partition,
			},
		},
		RmID: m.rmID,
	})
}

func (m *mockScheduler) addAppRequest(appID, allocID string, resource *si.Resource, repeat int32) error {
	return m.proxy.Update(&si.UpdateRequest{
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

func (m *mockScheduler) removeAppRequest(app, allocID string, isAsk bool) error {
	alloc2Release := make([]*si.AllocationReleaseRequest, 0)
	ask2Release := make([]*si.AllocationAskReleaseRequest, 0)

	if isAsk {
		ask := &si.AllocationAskReleaseRequest{
			PartitionName: m.partitionName,
			ApplicationID:        app,
			Allocationkey:        allocID,
			Message:              "ask is released",
		}
		ask2Release = append(ask2Release, ask)
	} else {
		alloc := &si.AllocationReleaseRequest {
			PartitionName: m.partitionName,
			ApplicationID: app,
			UUID: allocID,
			Message: "alloc is released",
		}
		alloc2Release = append(alloc2Release, alloc)
	}

	return m.proxy.Update(&si.UpdateRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease:    alloc2Release,
			AllocationAsksToRelease: ask2Release,
		},
		RmID: m.rmID,
	})
}

// simple wrapper to limit the repeating code getting the queue
func (m *mockScheduler) getSchedulingNode(nodeName string) *scheduler.SchedulingNode {
	return m.scheduler.GetClusterSchedulingContext().GetSchedulingNode(nodeName, m.partitionName)
}

// simple wrapper to limit the repeating code getting the queue
func (m *mockScheduler) getSchedulingQueue(queueName string) *scheduler.SchedulingQueue {
	return m.scheduler.GetClusterSchedulingContext().GetSchedulingQueue(queueName, m.partitionName)
}

// simple wrapper to limit the repeating code getting the queue with non default partition
func (m *mockScheduler) getSchedulingQueuePartition(queueName, partitionName string) *scheduler.SchedulingQueue {
	return m.scheduler.GetClusterSchedulingContext().GetSchedulingQueue(queueName, partitionName)
}

// simple wrapper to limit the repeating code getting the app
func (m *mockScheduler) getSchedulingApplication(appID string) *scheduler.SchedulingApplication {
	return m.scheduler.GetClusterSchedulingContext().GetSchedulingApplication(appID, m.partitionName)
}

// simple wrapper to limit the repeating code getting the app
func (m *mockScheduler) getPartitionReservations() map[string]int {
	return m.scheduler.GetClusterSchedulingContext().GetPartitionReservations(m.partitionName)
}
