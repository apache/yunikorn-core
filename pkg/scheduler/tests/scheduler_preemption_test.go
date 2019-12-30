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
	"testing"

	"gotest.tools/assert"

	"github.com/cloudera/yunikorn-core/pkg/common/resources"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
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

	ms := &MockScheduler{}
	defer ms.Stop()

	ms.Init(t, TwoEqualQueueConfigEnabledPreemption)

	scheduler := ms.scheduler

	ms.AddNode("node-1:1234", &si.Resource{
		Resources: map[string]*si.Quantity{
			"memory": {Value: 100},
			"vcore":  {Value: 100},
		},
	})
	ms.AddNode("node-2:1234", &si.Resource{
		Resources: map[string]*si.Quantity{
			"memory": {Value: 100},
			"vcore":  {Value: 100},
		},
	})
	ms.AddApp("app-1", "root.a", "")
	ms.AddApp("app-2", "root.b", "")

	waitForAcceptedNodes(ms.mockRM, "node-1:1234", 1000)
	waitForAcceptedNodes(ms.mockRM, "node-2:1234", 1000)

	waitForAcceptedApplications(ms.mockRM, "app-1", 1000)
	waitForAcceptedApplications(ms.mockRM, "app-2", 1000)

	// Check scheduling queue root
	schedulerQueueRoot := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root", "[rm:123]default")
	schedulerQueueA := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.a", "[rm:123]default")
	schedulerQueueB := scheduler.GetClusterSchedulingContext().GetSchedulingQueue("root.b", "[rm:123]default")

	// Get scheduling app
	schedulingApp1 := scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-1", "[rm:123]default")
	schedulingApp2 := scheduler.GetClusterSchedulingContext().GetSchedulingApplication("app-2", "[rm:123]default")

	// Ask (10, 10) resources * 20, which will fulfill the cluster.
	err := ms.proxy.Update(&si.UpdateRequest{
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
				ApplicationId:  "app-1",
			},
		},
		RmId: "rm:123",
	})

	if nil != err {
		t.Error(err.Error())
	}

	// Make sure resource requests arrived queue
	waitForPendingResource(t, schedulerQueueA, 200, 1000)
	waitForPendingResource(t, schedulerQueueRoot, 200, 1000)
	waitForPendingResourceForApplication(t, schedulingApp1, 200, 1000)

	// Try to schedule 40 allocations
	scheduler.SingleStepScheduleAllocTest(20)

	// We should be able to get 20 allocations.
	waitForAllocations(ms.mockRM, 20, 1000)

	// Make sure pending resource updated to 0
	waitForPendingResource(t, schedulerQueueA, 0, 1000)

	// Check allocated resources of queues, apps
	assert.Assert(t, schedulerQueueA.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 200)

	// Application-2 Ask for 20 resources
	err = ms.proxy.Update(&si.UpdateRequest{
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
				ApplicationId:  "app-2",
			},
		},
		RmId: "rm:123",
	})

	if nil != err {
		t.Error(err.Error())
	}

	waitForPendingResource(t, schedulerQueueB, 1000, 1000)

	// Now app-1 uses 20 resource, and queue-a's max = 150, so it can get two 50 container allocated.
	scheduler.SingleStepScheduleAllocTest(16)

	// Check pending resource, should be still 1000, nothing will be allocated because cluster is full
	waitForPendingResource(t, schedulerQueueB, 1000, 1000)

	// Check allocated resources of queue, should be 0
	assert.Assert(t, schedulerQueueB.CachedQueueInfo.GetAllocatedResource().Resources[resources.MEMORY] == 0)

	// Now we do a preemption.
	scheduler.SingleStepPreemption()

	// Check pending resource, should be 900 now
	waitForPendingResource(t, schedulerQueueB, 900, 1000)
	waitForPendingResourceForApplication(t, schedulingApp2, 900, 1000)
}
