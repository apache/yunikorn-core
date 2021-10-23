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

package examples

import (
	"sync"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/entrypoint"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type exampleRMCallback struct {
	acceptedApplications map[string]bool
	rejectedApplications map[string]bool
	acceptedNodes        map[string]bool
	rejectedNodes        map[string]bool
	nodeAllocations      map[string][]*si.Allocation
	Allocations          map[string]*si.Allocation

	sync.RWMutex
}

func (m *exampleRMCallback) UpdateAllocation(response *si.AllocationResponse) error {
	m.Lock()
	defer m.Unlock()
	for _, alloc := range response.New {
		m.Allocations[alloc.UUID] = alloc
		if val, ok := m.nodeAllocations[alloc.NodeID]; ok {
			val = append(val, alloc)
			m.nodeAllocations[alloc.NodeID] = val
		} else {
			nodeAllocations := make([]*si.Allocation, 0)
			nodeAllocations = append(nodeAllocations, alloc)
			m.nodeAllocations[alloc.NodeID] = nodeAllocations
		}
	}

	for _, alloc := range response.Released {
		delete(m.Allocations, alloc.UUID)
	}
	return nil
}

func (m *exampleRMCallback) UpdateApplication(response *si.ApplicationResponse) error {
	m.Lock()
	defer m.Unlock()
	for _, app := range response.Accepted {
		m.acceptedApplications[app.ApplicationID] = true
		delete(m.rejectedApplications, app.ApplicationID)
	}

	for _, app := range response.Rejected {
		m.rejectedApplications[app.ApplicationID] = true
		delete(m.acceptedApplications, app.ApplicationID)
	}
	return nil
}

func (m *exampleRMCallback) UpdateNode(response *si.NodeResponse) error {
	m.Lock()
	defer m.Unlock()
	for _, node := range response.Accepted {
		m.acceptedNodes[node.NodeID] = true
		delete(m.rejectedNodes, node.NodeID)
	}

	for _, node := range response.Rejected {
		m.rejectedNodes[node.NodeID] = true
		delete(m.acceptedNodes, node.NodeID)
	}
	return nil
}

func newExampleRMCallback() *exampleRMCallback {
	return &exampleRMCallback{
		acceptedApplications: make(map[string]bool),
		rejectedApplications: make(map[string]bool),
		acceptedNodes:        make(map[string]bool),
		rejectedNodes:        make(map[string]bool),
		nodeAllocations:      make(map[string][]*si.Allocation),
		Allocations:          make(map[string]*si.Allocation),
	}
}

func exampleOfRunYourOwnRM() {
	// Start all tests
	serviceContext := entrypoint.StartAllServices()
	proxy := serviceContext.RMProxy

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
	mockRM := newExampleRMCallback()

	_, err := proxy.RegisterResourceManager(
		&si.RegisterResourceManagerRequest{
			RmID:        "rm:123",
			PolicyGroup: "policygroup",
			Version:     "0.0.2",
		}, mockRM)

	if err != nil {
		panic(err)
	}

	// Register a node
	err = proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100},
						"vcore":  {Value: 20},
					},
				},
			},
			{
				NodeID:     "node-2:1234",
				Attributes: map[string]string{},
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
		RmID: "rm:123",
	})

	if err != nil {
		panic(err)
	}

	// (IMPORTANT)
	// Different from kubernetes, we need app for allocation ask. You can put all pod requests under the same app.
	// app name can be anything non-empty. Partition name can be empty (and internally becomes "default").
	err = proxy.UpdateApplication(&si.ApplicationRequest{
		New: []*si.AddApplicationRequest{
			{
				ApplicationID: "app-1",
				QueueName:     "a",
				PartitionName: "",
				Ugi: &si.UserGroupInformation{
					User: "testuser",
				},
			},
		},
		RmID: "rm:123",
	})

	if err != nil {
		panic(err)
	}

	// Refer to mock_rm_callback.go:109
	// You need to check app accepted by scheduler before proceed.

	// Send request
	err = proxy.UpdateAllocation(&si.AllocationRequest{
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
				ApplicationID:  "app-1",
			},
		},
		RmID: "rm:123",
	})
}
