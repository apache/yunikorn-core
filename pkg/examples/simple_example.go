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

package examples

import (
	"github.com/cloudera/yunikorn-core/pkg/common/configs"
	"github.com/cloudera/yunikorn-core/pkg/entrypoint"
	"github.com/cloudera/yunikorn-core/pkg/scheduler/tests"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
)

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
	mockRM := tests.NewMockRMCallbackHandler(nil) // [CHANGE THIS], should use your own implementation of api.ResourceManagerCallback

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
	err = proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeID: "node-1:1234",
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
				NodeID: "node-2:1234",
				Attributes: map[string]string{
					"si.io/hostname": "node-2",
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
		RmID: "rm:123",
	})

	if err != nil {
		panic(err)
	}

	// (IMPORTANT)
	// Different from kubernetes, we need app for allocation ask. You can put all pod requests under the same app.
	// app name can be anything non-empty. Partition name can be empty (and internally becomes "default").
	err = proxy.Update(&si.UpdateRequest{
		NewApplications: []*si.AddApplicationRequest{
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

	// Refer to schedulertestutils.go:30
	// You need to check app accepted by scheduler before proceed.

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
				ApplicationID:  "app-1",
			},
		},
		RmID: "rm:123",
	})
}
