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
	DualQueuePreemptionConfig = `
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
)

// Test basic interactions from rm proxy to cache and to scheduler.
func TestBasicPreemption(t *testing.T) {
	// PR #73 Support unconfirmed resource for nodes to improve scheduling fairness.
	// This test is skipped as the current implementation of preemption does not track the resources
	// being preempted against a candidate allocation. This means that while an allocation is in flight
	// the scheduler thinks resources marked for preemption are available. That will cause one single
	// preemption to be used by multiple allocations.
	// As a result the allocation will fail. The end result for this test is a flaky behaviour.
	t.SkipNow()

	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(DualQueuePreemptionConfig, false)
	if err != nil {
		t.Errorf("mock scheduler creation failed for preemption test: %v", err)
	}

	scheduler := ms.scheduler

	err = ms.addNode("node-1:1234", &si.Resource{
		Resources: map[string]*si.Quantity{
			"memory": {Value: 100},
			"vcore":  {Value: 100},
		},
	})
	assert.NilError(t, err, "Adding node 1 to scheduler failed")
	err = ms.addNode("node-2:1234", &si.Resource{
		Resources: map[string]*si.Quantity{
			"memory": {Value: 100},
			"vcore":  {Value: 100},
		},
	})
	assert.NilError(t, err, "Adding node 2 to scheduler failed")

	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	ms.mockRM.waitForAcceptedNode(t, "node-2:1234", 1000)

	err = ms.addApp(appID1, "root.a", "")
	assert.NilError(t, err, "Adding application 1 to scheduler failed")
	err = ms.addApp(appID2, "root.b", "")
	assert.NilError(t, err, "Adding application 2 to scheduler failed")

	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)
	ms.mockRM.waitForAcceptedApplication(t, appID2, 1000)

	// Check scheduling queue root
	rootQ := ms.getQueue("root")
	queueA := ms.getQueue("root.a")
	queueB := ms.getQueue("root.b")

	// Get scheduling app
	app1 := ms.getApplication(appID1)
	app2 := ms.getApplication(appID2)

	// Ask (10, 10) resources * 20, which will fulfill the cluster.
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 10},
					},
				},
				MaxAllocations: 20,
				ApplicationID:  appID1,
			},
		},
		RmID: "rm:123",
	})

	if nil != err {
		t.Error(err.Error())
	}

	// Make sure resource requests arrived queue
	waitForPendingQueueResource(t, queueA, 200, 1000)
	waitForPendingQueueResource(t, rootQ, 200, 1000)
	waitForPendingAppResource(t, app1, 200, 1000)

	// Try to schedule 40 allocations
	scheduler.MultiStepSchedule(20)

	// We should be able to get 20 allocations.
	ms.mockRM.waitForAllocations(t, 20, 1000)

	// Make sure pending resource updated to 0
	waitForPendingQueueResource(t, queueA, 0, 1000)

	// Check allocated resources of queues, apps
	assert.Assert(t, queueA.GetAllocatedResource().Resources[resources.MEMORY] == 200)

	// Application-2 Ask for 20 resources
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-2",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
						"vcore":  {Value: 10},
					},
				},
				MaxAllocations: 100,
				ApplicationID:  appID2,
			},
		},
		RmID: "rm:123",
	})

	if nil != err {
		t.Error(err.Error())
	}

	waitForPendingQueueResource(t, queueB, 1000, 1000)

	// Now app-1 uses 20 resource, and queue-a's max = 150, so it can get two 50 container allocated.
	scheduler.MultiStepSchedule(16)

	// Check pending resource, should be still 1000, nothing will be allocated because cluster is full
	waitForPendingQueueResource(t, queueB, 1000, 1000)

	// Check allocated resources of queue, should be 0
	assert.Assert(t, queueB.GetAllocatedResource().Resources[resources.MEMORY] == 0)

	// Now we do a preemption.
	scheduler.SingleStepPreemption()

	// Check pending resource, should be 900 now
	waitForPendingQueueResource(t, queueB, 900, 1000)
	waitForPendingAppResource(t, app2, 900, 1000)
}
