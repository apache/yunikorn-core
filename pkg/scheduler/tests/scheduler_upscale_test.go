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
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

const (
	MultiQueueConfig = `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: parent
            resources:
              max:
                memory: 20
                vcore: 10
            queues:
              - name: leaf1
                resources:
                  max:
                    memory: 10
                    vcore: 4
              - name: leaf2
`
)

func checkQueues(t *testing.T, ms *mockScheduler) {
	// Register RM
	err := ms.Init(MultiQueueConfig, false)
	assert.NilError(t, err, "RegisterResourceManager failed")
	// Check queues of cache and scheduler.
	partitionInfo := ms.clusterInfo.GetPartition("[rm:123]default")
	assert.Assert(t, partitionInfo.Root.GetMaxResource() == nil, "partition info max resource nil")

	// Check scheduling queue root
	root := ms.getSchedulingQueue("root")
	assert.Assert(t, root.QueueInfo.GetMaxResource() == nil, "root max resource expected nil:got: %s", root.QueueInfo.GetMaxResource())

	// Check scheduling queue parent
	parent := ms.getSchedulingQueue("root.parent")
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"memory": "20", "vcore": "10"})
	assert.NilError(t, err, "failed to create resource")
	assert.Assert(t, resources.Equals(res, parent.QueueInfo.GetMaxResource()), "parent max resource expected: %s, got: %s", res, parent.QueueInfo.GetMaxResource())

	// Check scheduling queue leaf (same max as parent)
	leaf := ms.getSchedulingQueue("root.parent.leaf2")
	assert.Assert(t, leaf.QueueInfo.GetMaxResource() == nil, "leaf2 max resource expected nil, got: %s", leaf.QueueInfo.GetMaxResource())

	// Check scheduling queue leaf2
	leaf = ms.getSchedulingQueue("root.parent.leaf1")
	res, err = resources.NewResourceFromConf(map[string]string{"memory": "10", "vcore": "4"})
	assert.NilError(t, err, "failed to create resource")
	assert.Assert(t, resources.Equals(res, leaf.QueueInfo.GetMaxResource()), "leaf1 max resource expected: %s, got: %s", res, leaf.QueueInfo.GetMaxResource())
}

func TestUpScaleWithoutNodes(t *testing.T) {
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()
	checkQueues(t, ms)

	// Add one application
	err := ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-1": "root.parent.leaf1"}),
		RmID:            "rm:123",
	})
	assert.NilError(t, err, "UpdateRequest failed for app")

	// Application should be accepted
	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)

	// App asks for 2 allocations
	err = ms.proxy.Update(&si.UpdateRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{"memory": {Value: 10}, "vcore": {Value: 4}},
				},
				MaxAllocations: 1,
				ApplicationID:  "app-1",
			},
			{
				AllocationKey: "alloc-2",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{"memory": {Value: 2}, "vcore": {Value: 2}},
				},
				MaxAllocations: 1,
				ApplicationID:  "app-1",
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed for allocations")

	leaf := ms.getSchedulingQueue("root.parent.leaf1")
	root := ms.getSchedulingQueue("root")
	app := ms.getSchedulingApplication("app-1")

	waitForPendingQueueResource(t, leaf, 12, 1000)
	waitForPendingQueueResource(t, root, 12, 1000)
	waitForPendingAppResource(t, app, 12, 1000)

	// no nodes available, no allocation can be made
	ms.scheduler.MultiStepSchedule(5)

	// pending resources should not change
	waitForPendingQueueResource(t, leaf, 12, 1000)
	waitForPendingQueueResource(t, root, 12, 1000)
	waitForPendingAppResource(t, app, 12, 1000)

	// upscale resources should have been set to the max of the queue
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"memory": "10", "vcore": "4"})
	assert.NilError(t, err, "failed to create resource")
	assert.Assert(t, resources.Equals(res, leaf.GetUpScalingResource()), "leaf queue upscaling resource expected: %s, got: %s", res, leaf.GetUpScalingResource())
	ask := app.GetSchedulingAllocationAsk("alloc-1")
	assert.Assert(t, ask.GetUpscaleRequested(), "alloc-1 should be marked for scale up and is not")
	ask = app.GetSchedulingAllocationAsk("alloc-2")
	assert.Assert(t, !ask.GetUpscaleRequested(), "alloc-2 should not be marked for scale up and is")
}

func TestUpScaleWithoutNodesMultiQueue(t *testing.T) {
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()
	checkQueues(t, ms)

	// Add one application at a time: maps are random on read we need reproducibility
	err := ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-1": "root.parent.leaf1"}),
		RmID:            "rm:123",
	})
	assert.NilError(t, err, "UpdateRequest failed for app-1")
	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)

	err = ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-2": "root.parent.leaf2"}),
		RmID:            "rm:123",
	})
	assert.NilError(t, err, "UpdateRequest failed for app-2")
	ms.mockRM.waitForAcceptedApplication(t, "app-2", 1000)

	// App asks for allocations
	err = ms.proxy.Update(&si.UpdateRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey:  "alloc-1",
				ResourceAsk:    &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 10}, "vcore": {Value: 4}}},
				MaxAllocations: 1,
				ApplicationID:  "app-1",
			},
			{
				AllocationKey:  "alloc-2",
				ResourceAsk:    &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 2}, "vcore": {Value: 2}}},
				MaxAllocations: 1,
				ApplicationID:  "app-1",
			},
			{
				AllocationKey:  "alloc-3",
				ResourceAsk:    &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 5}, "vcore": {Value: 3}}},
				MaxAllocations: 1,
				ApplicationID:  "app-2",
			},
			{
				AllocationKey:  "alloc-4",
				ResourceAsk:    &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 5}, "vcore": {Value: 3}}},
				MaxAllocations: 1,
				ApplicationID:  "app-2",
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "UpdateRequest failed for allocations")

	app1 := ms.getSchedulingApplication("app-1")
	app2 := ms.getSchedulingApplication("app-2")
	leaf1 := ms.getSchedulingQueue("root.parent.leaf1")
	leaf2 := ms.getSchedulingQueue("root.parent.leaf2")
	parent := ms.getSchedulingQueue("root.parent")

	// no nodes available, no allocation can be made
	// pending resources should not change
	waitForPendingQueueResource(t, leaf1, 12, 1000)
	waitForPendingQueueResource(t, leaf2, 10, 1000)
	waitForPendingQueueResource(t, parent, 22, 1000)
	waitForPendingAppResource(t, app1, 12, 1000)
	waitForPendingAppResource(t, app2, 10, 1000)
	ms.scheduler.MultiStepSchedule(10)
	waitForPendingQueueResource(t, leaf1, 12, 1000)
	waitForPendingQueueResource(t, leaf2, 10, 1000)
	waitForPendingQueueResource(t, parent, 22, 1000)

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"memory": "20", "vcore": "10"})
	assert.NilError(t, err, "failed to create resource")
	assert.Assert(t, resources.Equals(res, parent.GetUpScalingResource()), "parent queue upscaling resource expected: %s, got: %s", res, parent.GetUpScalingResource())

	// upscale resources should have been set to the max of the queue
	res, err = resources.NewResourceFromConf(map[string]string{"memory": "10", "vcore": "4"})
	assert.NilError(t, err, "failed to create resource")
	assert.Assert(t, resources.Equals(res, leaf1.GetUpScalingResource()), "leaf1 queue upscaling resource expected: %s, got: %s", res, leaf1.GetUpScalingResource())
	ask := app1.GetSchedulingAllocationAsk("alloc-1")
	assert.Assert(t, ask.GetUpscaleRequested(), "alloc-1 should be marked for scale up and is not")
	ask = app1.GetSchedulingAllocationAsk("alloc-2")
	assert.Assert(t, !ask.GetUpscaleRequested(), "alloc-2 should not be marked for scale up and is")

	// upscale should have maxed out on parent
	res, err = resources.NewResourceFromConf(map[string]string{"memory": "10", "vcore": "6"})
	assert.NilError(t, err, "failed to create resource")
	assert.Assert(t, resources.Equals(res, leaf2.GetUpScalingResource()), "leaf2 queue upscaling resource expected: %s, got: %s", res, leaf2.GetUpScalingResource())
	ask = app2.GetSchedulingAllocationAsk("alloc-3")
	assert.Assert(t, ask.GetUpscaleRequested(), "alloc-3 should be marked for scale up and is not")
	ask = app2.GetSchedulingAllocationAsk("alloc-4")
	assert.Assert(t, ask.GetUpscaleRequested(), "alloc-4 should be marked for scale up and is not")

	// App asks for allocation
	err = ms.proxy.Update(&si.UpdateRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey:  "alloc-5",
				ResourceAsk:    &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 5}, "vcore": {Value: 3}}},
				MaxAllocations: 1,
				ApplicationID:  "app-2",
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "UpdateRequest failed for allocations")

	// no nodes available, no allocation can be made
	waitForPendingQueueResource(t, leaf2, 15, 1000)
	waitForPendingQueueResource(t, parent, 27, 1000)
	waitForPendingAppResource(t, app2, 15, 1000)
	ms.scheduler.MultiStepSchedule(10)
	waitForPendingQueueResource(t, leaf2, 15, 1000)
	waitForPendingQueueResource(t, parent, 27, 1000)
	waitForPendingAppResource(t, app2, 15, 1000)

	// add more to app-2 and see if we can get above the parent queue limit
	ask = app2.GetSchedulingAllocationAsk("alloc-5")
	assert.Assert(t, !ask.GetUpscaleRequested(), "alloc-5 should not be marked for scale up and is")
	assert.Assert(t, resources.Equals(res, leaf2.GetUpScalingResource()), "leaf2 queue upscaling resource expected: %s, got: %s", res, leaf2.GetUpScalingResource())
}

func TestUpScaleWithoutNodesMultiApp(t *testing.T) {
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()
	checkQueues(t, ms)

	// Add one application at a time: maps are random on read we need reproducibility
	err := ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-1": "root.parent.leaf1"}),
		RmID:            "rm:123",
	})
	assert.NilError(t, err, "UpdateRequest failed for app-1")
	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)

	err = ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-2": "root.parent.leaf1"}),
		RmID:            "rm:123",
	})
	assert.NilError(t, err, "UpdateRequest failed for app-2")
	ms.mockRM.waitForAcceptedApplication(t, "app-2", 1000)

	// App asks for 2 allocations per app
	err = ms.proxy.Update(&si.UpdateRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{"memory": {Value: 5}, "vcore": {Value: 2}},
				},
				MaxAllocations: 1,
				ApplicationID:  "app-1",
			},
			{
				AllocationKey: "alloc-2",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{"memory": {Value: 5}, "vcore": {Value: 2}},
				},
				MaxAllocations: 1,
				ApplicationID:  "app-2",
			},
			{
				AllocationKey: "alloc-3",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{"memory": {Value: 5}, "vcore": {Value: 2}},
				},
				MaxAllocations: 1,
				ApplicationID:  "app-1",
			},
			{
				AllocationKey: "alloc-4",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{"memory": {Value: 5}, "vcore": {Value: 2}},
				},
				MaxAllocations: 1,
				ApplicationID:  "app-2",
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed for allocations")

	app1 := ms.getSchedulingApplication("app-1")
	app2 := ms.getSchedulingApplication("app-2")
	leaf1 := ms.getSchedulingQueue("root.parent.leaf1")
	parent := ms.getSchedulingQueue("root.parent")
	root := ms.getSchedulingQueue("root")

	// no nodes available, no allocation can be made
	// pending resources should not change
	waitForPendingQueueResource(t, leaf1, 20, 1000)
	waitForPendingQueueResource(t, parent, 20, 1000)
	waitForPendingQueueResource(t, root, 20, 1000)
	waitForPendingAppResource(t, app1, 10, 1000)
	waitForPendingAppResource(t, app2, 10, 1000)
	ms.scheduler.MultiStepSchedule(10)
	waitForPendingQueueResource(t, leaf1, 20, 1000)
	waitForPendingQueueResource(t, parent, 20, 1000)
	waitForPendingQueueResource(t, root, 20, 1000)

	var res *resources.Resource
	// upscale resources should have been set to the max of the queue
	res, err = resources.NewResourceFromConf(map[string]string{"memory": "10", "vcore": "4"})
	assert.NilError(t, err, "failed to create resource")
	assert.Assert(t, resources.Equals(res, parent.GetUpScalingResource()), "parent queue upscaling resource expected: %s, got: %s", res, parent.GetUpScalingResource())
	assert.Assert(t, resources.Equals(res, leaf1.GetUpScalingResource()), "leaf1 queue upscaling resource expected: %s, got: %s", res, leaf1.GetUpScalingResource())

	// oldest app will trigger upscale, even for newer request
	ask := app1.GetSchedulingAllocationAsk("alloc-1")
	assert.Assert(t, ask.GetUpscaleRequested(), "alloc-1 should be marked for scale up and is not")
	ask = app1.GetSchedulingAllocationAsk("alloc-3")
	assert.Assert(t, ask.GetUpscaleRequested(), "alloc-3 should be marked for scale up and is not")

	// newer app will not have triggered upscale
	ask = app2.GetSchedulingAllocationAsk("alloc-2")
	assert.Assert(t, !ask.GetUpscaleRequested(), "alloc-2 should not be marked for scale up and is")
	ask = app2.GetSchedulingAllocationAsk("alloc-4")
	assert.Assert(t, !ask.GetUpscaleRequested(), "alloc-4 should not be marked for scale up and is")
}

func TestUpScaleWithNodes(t *testing.T) {
	// Start all tests
	ms := &mockScheduler{}
	defer ms.Stop()
	checkQueues(t, ms)

	// create a node
	err := ms.addNode("node-1", &si.Resource{
		Resources: map[string]*si.Quantity{"memory": {Value: 10}, "vcore": {Value: 10}},
	})
	assert.NilError(t, err, "node creation failed")
	ms.mockRM.waitForMinAcceptedNodes(t, 1, 1000)

	// Add one application
	err = ms.proxy.Update(&si.UpdateRequest{
		NewApplications: newAddAppRequest(map[string]string{"app-1": "root.parent.leaf2"}),
		RmID:            "rm:123",
	})
	assert.NilError(t, err, "UpdateRequest failed for app")

	// Application should be accepted
	ms.mockRM.waitForAcceptedApplication(t, "app-1", 1000)

	// App asks for 2 allocations
	err = ms.proxy.Update(&si.UpdateRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{"memory": {Value: 5}, "vcore": {Value: 5}},
				},
				MaxAllocations: 1,
				ApplicationID:  "app-1",
			},
			{
				AllocationKey: "alloc-2",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{"memory": {Value: 8}, "vcore": {Value: 5}},
				},
				MaxAllocations: 1,
				ApplicationID:  "app-1",
			},
			{
				AllocationKey: "alloc-3",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{"memory": {Value: 8}, "vcore": {Value: 5}},
				},
				MaxAllocations: 1,
				ApplicationID:  "app-1",
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed for allocations")

	leaf := ms.getSchedulingQueue("root.parent.leaf2")
	root := ms.getSchedulingQueue("root")
	app := ms.getSchedulingApplication("app-1")

	waitForPendingQueueResource(t, leaf, 21, 1000)
	waitForPendingQueueResource(t, root, 21, 1000)
	waitForPendingAppResource(t, app, 21, 1000)

	// 1 node available, one allocation can be made: oldest request
	ms.scheduler.MultiStepSchedule(5)

	// pending resources should change on one allocation
	waitForPendingQueueResource(t, leaf, 16, 1000)
	waitForPendingQueueResource(t, root, 16, 1000)
	waitForPendingAppResource(t, app, 16, 1000)
	waitForAllocatedAppResource(t, app, 5, 1000)

	// upscale resources should have been set to the other request
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"memory": "8", "vcore": "5"})
	assert.NilError(t, err, "failed to create resource")
	assert.Assert(t, resources.Equals(res, leaf.GetUpScalingResource()), "leaf2 queue upscaling resource expected: %s, got: %s", res, leaf.GetUpScalingResource())
	ask := app.GetSchedulingAllocationAsk("alloc-1")
	assert.Assert(t, !ask.GetUpscaleRequested(), "alloc-1 should not be marked for scale up")
	ask2 := app.GetSchedulingAllocationAsk("alloc-2")
	assert.Assert(t, ask2.GetUpscaleRequested(), "alloc-2 should be marked for scale up and is not")
	ask3 := app.GetSchedulingAllocationAsk("alloc-3")
	assert.Assert(t, !ask3.GetUpscaleRequested(), "alloc-3 should not be marked for scale up")

	// create a second node
	err = ms.addNode("node-2", &si.Resource{
		Resources: map[string]*si.Quantity{"memory": {Value: 10}, "vcore": {Value: 10}},
	})
	assert.NilError(t, err, "node creation failed")
	ms.mockRM.waitForMinAcceptedNodes(t, 2, 1000)
	// 2 nodes available, another allocation made
	ms.scheduler.MultiStepSchedule(5)

	// pending resources should change another allocation
	waitForPendingQueueResource(t, leaf, 8, 1000)
	waitForPendingQueueResource(t, root, 8, 1000)
	waitForPendingAppResource(t, app, 8, 1000)
	waitForAllocatedAppResource(t, app, 13, 1000)
	// the next allocation would push the queue over its max: no allocation or upscale
	assert.Assert(t, resources.IsZero(leaf.GetUpScalingResource()), "leaf2 queue upscaling resource expected zero, got: %s", leaf.GetUpScalingResource())
	assert.Assert(t, !ask2.GetUpscaleRequested(), "alloc-2 should not be marked for scale up anymore")
	assert.Assert(t, !ask3.GetUpscaleRequested(), "alloc-3 should not be marked for scale up and is")
}
