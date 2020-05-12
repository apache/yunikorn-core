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
	"strconv"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

const (
	SingleQueueConfig = `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: leaf-1
            resources:
              guaranteed:
                memory: 100
                vcore: 100
              max:
                memory: 200
                vcore: 200
    preemption:
      enabled: false
`
	DualQueueConfig = `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: leaf-1
            resources:
              guaranteed:
                memory: 100
                vcore: 100
              max:
                memory: 500
                vcore: 500
          - name: leaf-2
            resources:
              guaranteed:
                memory: 100
                vcore: 100
              max:
                memory: 500
                vcore: 500
    preemption:
      enabled: false
`
)

// simple reservation one app one queue
// multiple nodes and multiple repeats for the same ask get reserved
func TestBasicReservation(t *testing.T) {
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(SingleQueueConfig, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// override the reservation delay, and cleanup when done
	scheduler.OverrideReservationDelay(10 * time.Millisecond)
	defer scheduler.OverrideReservationDelay(2 * time.Second)

	nodes := createNodes(t, ms, 2, 50)
	ms.mockRM.waitForMinAcceptedNodes(t, 2, 1000)

	appID := "app-1"
	queueName := "root.leaf-1"
	err = ms.addApp(appID, queueName, "default")
	assert.NilError(t, err, "adding app to scheduler failed")

	ms.mockRM.waitForAcceptedApplication(t, appID, 1000)
	// Get scheduling app
	app := ms.getSchedulingApplication(appID)

	res := &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 20}, "vcore": {Value: 20}}}
	err = ms.addAppRequest(appID, "alloc-1", res, 6)
	assert.NilError(t, err, "adding requests to app failed")

	leafQueue := ms.getSchedulingQueue(queueName)
	waitForPendingQueueResource(t, leafQueue, 120, 1000)
	waitForPendingAppResource(t, app, 120, 1000)

	// Allocate for app
	ms.scheduler.MultiStepSchedule(20)
	ms.mockRM.waitForAllocations(t, 4, 1000)

	// Verify that all 4 requests are satisfied
	mem := int(app.GetAllocatedResource().Resources[resources.MEMORY])
	assert.Equal(t, 80, mem, "allocated resource after alloc not correct")
	waitForNodesAllocatedResource(t, ms.clusterInfo, ms.partitionName, nodes, 80, 1000)

	// check the pending resources (2 * 20 still outstanding)
	waitForPendingQueueResource(t, leafQueue, 40, 1000)
	waitForPendingAppResource(t, app, 40, 1000)
	ms.scheduler.MultiStepSchedule(10)

	// Allocation should not change (still 4)
	ms.mockRM.waitForAllocations(t, 4, 1000)

	// check objects have reservations assigned,
	assert.Equal(t, 2, len(app.GetReservations()), "reservations missing from app")
	assert.Equal(t, 1, len(ms.getSchedulingNode(nodes[0]).GetReservations()), "reservation missing on %s", nodes[0])
	assert.Equal(t, 1, len(ms.getSchedulingNode(nodes[1]).GetReservations()), "reservation missing on %s", nodes[1])
	assert.Equal(t, 2, ms.getPartitionReservations()[appID], "reservations missing from partition")

	// Remove app-1
	err = ms.removeApp(appID, "default")
	assert.NilError(t, err, "application removal failed")

	// Check allocated resource of queue, should be 0 now
	waitForAllocatedQueueResource(t, leafQueue, 0, 1000)
	mem = int(app.GetAllocatedResource().Resources[resources.MEMORY])
	assert.Equal(t, 0, mem, "allocated app resource not correct after app removal")
	mem = int(ms.getSchedulingNode(nodes[0]).GetAllocatedResource().Resources[resources.MEMORY])
	assert.Equal(t, 0, mem, "allocated node-1 resource not correct after app removal")
	mem = int(ms.getSchedulingNode(nodes[1]).GetAllocatedResource().Resources[resources.MEMORY])
	assert.Equal(t, 0, mem, "allocated node-2 resource not correct after app removal")

	// App/node should not have reservation now
	assert.Equal(t, 0, len(app.GetReservations()), "reservations missing from app")
	assert.Equal(t, 0, len(ms.getSchedulingNode(nodes[0]).GetReservations()), "reservation missing on %s", nodes[0])
	assert.Equal(t, 0, len(ms.getSchedulingNode(nodes[1]).GetReservations()), "reservation missing on %s", nodes[1])
	assert.Equal(t, 0, ms.getPartitionReservations()[appID], "reservations missing from partition")
}

// setup multiple queues, apps and nodes
// queue1 with app1 takes all nodes, leaving small space
// queue2 with app2 and app3, app2 has largest requests and reserves both nodes
// app3 is starved
// kill app1 and make sure app2 gets its reservations filled followed by app3
func TestReservationForTwoQueues(t *testing.T) {
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(DualQueueConfig, false)
	assert.NilError(t, err, "RegisterResourceManager failed")
	// override the reservation delay, and cleanup when done
	scheduler.OverrideReservationDelay(10 * time.Millisecond)
	defer scheduler.OverrideReservationDelay(2 * time.Second)

	nodes := createNodes(t, ms, 2, 50)
	ms.mockRM.waitForMinAcceptedNodes(t, 2, 1000)

	// add the first scheduling app
	app1ID := "app-1"
	leaf1Name := "root.leaf-1"
	err = ms.addApp(app1ID, leaf1Name, "default")
	assert.NilError(t, err, "adding app 1 to scheduler failed")

	ms.mockRM.waitForAcceptedApplication(t, app1ID, 1000)
	app1 := ms.getSchedulingApplication(app1ID)
	// add the second scheduling app
	app2ID := "app-2"
	leaf2name := "root.leaf-2"
	err = ms.addApp(app2ID, leaf2name, "default")
	assert.NilError(t, err, "adding app 2 to scheduler failed")

	ms.mockRM.waitForAcceptedApplication(t, app2ID, 1000)
	app2 := ms.getSchedulingApplication(app2ID)

	// add the third scheduling app
	app3ID := "app-3"
	err = ms.addApp(app3ID, leaf2name, "default")
	assert.NilError(t, err, "adding app 3 to scheduler failed")

	ms.mockRM.waitForAcceptedApplication(t, app3ID, 1000)
	app3 := ms.getSchedulingApplication(app3ID)

	leaf1 := ms.getSchedulingQueue("root.leaf-1")
	leaf2 := ms.getSchedulingQueue("root.leaf-2")

	resLarge := &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 20}, "vcore": {Value: 20}}}
	resSmall := &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 5}, "vcore": {Value: 5}}}

	// allocate bulk of the node
	err = ms.addAppRequest(app1ID, "alloc-1", resLarge, 4)
	assert.NilError(t, err, "adding request to app-1 failed")

	waitForPendingQueueResource(t, leaf1, 80, 1000)
	waitForPendingAppResource(t, app1, 80, 1000)

	// Allocate for app 1
	ms.scheduler.MultiStepSchedule(8)
	ms.mockRM.waitForAllocations(t, 4, 1000)
	mem := int(app1.GetAllocatedResource().Resources[resources.MEMORY])
	assert.Equal(t, 80, mem, "allocated resource after alloc not correct")
	waitForNodesAllocatedResource(t, ms.clusterInfo, ms.partitionName, nodes, 80, 1000)

	// these will be reserved (one on each node)
	err = ms.addAppRequest(app2ID, "alloc-2", resLarge, 2)
	assert.NilError(t, err, "adding request to app-2 failed")

	// nothing will happen with these
	err = ms.addAppRequest(app3ID, "alloc-3", resSmall, 2)
	assert.NilError(t, err, "adding request to app-3 failed")

	waitForPendingQueueResource(t, leaf2, 50, 1000)
	waitForPendingAppResource(t, app2, 40, 1000)
	waitForPendingAppResource(t, app3, 10, 1000)

	// Allocate for rest of apps
	ms.scheduler.MultiStepSchedule(6)

	// Allocation should not change
	ms.mockRM.waitForAllocations(t, 4, 1000)

	// both reservations should be for the same app
	appResCounter := ms.getPartitionReservations()
	assert.Equal(t, 1, len(appResCounter), "partition reservations are missing")
	assert.Equal(t, 2, appResCounter[app2ID], "partition reservations counter should have been 2")
	assert.Equal(t, 2, len(app2.GetReservations()), "app-2 should have 2 reservations")
	assert.Equal(t, 0, len(app3.GetReservations()), "app-3 should have no reservations")

	// Now remove app-1, and do allocation
	err = ms.removeApp(app1ID, "default")
	assert.NilError(t, err, "application removal from partition failed")

	// Check allocated resource of queue, should be 0 now
	ms.mockRM.waitForAllocations(t, 0, 1000)
	waitForAllocatedQueueResource(t, leaf1, 0, 1000)

	// Allocate twice, which we should see two reservations filled
	ms.scheduler.MultiStepSchedule(2)
	ms.mockRM.waitForAllocations(t, 2, 1000)

	// Check allocated resource of queue, should be 50 now
	waitForAllocatedQueueResource(t, leaf2, 40, 1000)
	waitForNodesAllocatedResource(t, ms.clusterInfo, ms.partitionName, nodes, 40, 1000)

	// Reservation should be filled
	assert.Equal(t, 0, len(ms.getPartitionReservations()), "partition should have no reservations")
	assert.Equal(t, 0, len(app2.GetReservations()), "app-2 should have no reservations")

	// Do last allocations
	ms.scheduler.MultiStepSchedule(2)
	waitForAllocatedQueueResource(t, leaf2, 50, 1000)
	waitForNodesAllocatedResource(t, ms.clusterInfo, ms.partitionName, nodes, 50, 1000)
}

// remove a node with reservation, reservation should be removed
func TestRemoveReservedNode(t *testing.T) {
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(SingleQueueConfig, false)
	assert.NilError(t, err, "RegisterResourceManager failed")
	// override the reservation delay, and cleanup when done
	scheduler.OverrideReservationDelay(10 * time.Millisecond)
	defer scheduler.OverrideReservationDelay(2 * time.Second)

	nodes := createNodes(t, ms, 2, 50)
	ms.mockRM.waitForMinAcceptedNodes(t, 2, 1000)

	appID := "app-1"
	queueName := "root.leaf-1"
	err = ms.addApp(appID, queueName, "default")
	assert.NilError(t, err, "adding app to scheduler failed")

	ms.mockRM.waitForAcceptedApplication(t, appID, 1000)
	// Get scheduling app
	app := ms.getSchedulingApplication(appID)

	res := &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 20}, "vcore": {Value: 20}}}
	err = ms.addAppRequest(appID, "alloc-1", res, 6)
	assert.NilError(t, err, "adding requests to app failed")

	leafQueue := ms.getSchedulingQueue(queueName)
	waitForPendingQueueResource(t, leafQueue, 120, 1000)
	waitForPendingAppResource(t, app, 120, 1000)

	// Allocate for app
	ms.scheduler.MultiStepSchedule(20)
	waitForAllocatedAppResource(t, app, 80, 1000)
	ms.mockRM.waitForAllocations(t, 4, 1000)
	waitForPendingAppResource(t, app, 40, 1000)
	// check objects have reservations assigned,
	assert.Equal(t, 2, len(app.GetReservations()), "reservations missing from app")
	assert.Equal(t, 1, len(ms.getSchedulingNode(nodes[0]).GetReservations()), "reservation missing on %s", nodes[0])
	assert.Equal(t, 1, len(ms.getSchedulingNode(nodes[1]).GetReservations()), "reservation missing on %s", nodes[1])
	assert.Equal(t, 2, ms.getPartitionReservations()[appID], "reservations missing from partition")

	// remove the 2nd node
	err = ms.removeNode(nodes[1])
	assert.NilError(t, err, "node removal failed")

	waitForRemovedSchedulerNode(t, ms.serviceContext.Scheduler.GetClusterSchedulingContext(), nodes[1], ms.partitionName, 1000)
	waitForAllocatedAppResource(t, app, 40, 1000)
	ms.mockRM.waitForAllocations(t, 2, 1000)
	assert.Equal(t, 1, len(app.GetReservations()), "reservations missing from app")
	assert.Equal(t, 1, len(ms.getSchedulingNode(nodes[0]).GetReservations()), "reservation missing on %s", nodes[0])
	assert.Equal(t, 1, ms.getPartitionReservations()[appID], "reservations not removed from partition")
}

// add a node, reservation should be converted to allocations automatically
func TestAddNewNode(t *testing.T) {
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(SingleQueueConfig, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// override the reservation delay, and cleanup when done
	scheduler.OverrideReservationDelay(10 * time.Millisecond)
	defer scheduler.OverrideReservationDelay(2 * time.Second)

	nodes := createNodes(t, ms, 3, 50)
	ms.mockRM.waitForMinAcceptedNodes(t, 3, 1000)
	ms.clusterInfo.GetPartition(ms.partitionName).GetNode(nodes[2]).SetSchedulable(false)

	appID := "app-1"
	queueName := "root.leaf-1"
	err = ms.addApp(appID, queueName, "default")
	assert.NilError(t, err, "adding app to scheduler failed")

	ms.mockRM.waitForAcceptedApplication(t, appID, 1000)
	// Get scheduling app
	app := ms.getSchedulingApplication(appID)

	res := &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 20}, "vcore": {Value: 20}}}
	err = ms.addAppRequest(appID, "alloc-1", res, 6)
	assert.NilError(t, err, "adding requests to app failed")

	leafQueue := ms.getSchedulingQueue(queueName)
	waitForPendingQueueResource(t, leafQueue, 120, 1000)
	waitForPendingAppResource(t, app, 120, 1000)

	// Allocate for app
	ms.scheduler.MultiStepSchedule(20)
	ms.mockRM.waitForAllocations(t, 4, 1000)
	// check objects have reservations assigned,
	assert.Equal(t, 2, len(app.GetReservations()), "reservations missing from app")
	assert.Equal(t, 1, len(ms.getSchedulingNode(nodes[0]).GetReservations()), "reservation missing on %s", nodes[0])
	assert.Equal(t, 1, len(ms.getSchedulingNode(nodes[1]).GetReservations()), "reservation missing on %s", nodes[1])
	assert.Equal(t, 2, ms.getPartitionReservations()[appID], "reservations missing from partition")

	// change the third node to scheduling (simulates new node)
	ms.clusterInfo.GetPartition(ms.partitionName).GetNode(nodes[2]).SetSchedulable(true)

	// start allocating: both reservations should be moved to the 3rd node
	ms.scheduler.MultiStepSchedule(10)
	ms.mockRM.waitForAllocations(t, 6, 1000)
	waitForAllocatedAppResource(t, app, 120, 1000)
	waitForPendingQueueResource(t, leafQueue, 0, 1000)
	waitForPendingAppResource(t, app, 0, 1000)
	mem := int(ms.getSchedulingNode(nodes[2]).GetAllocatedResource().Resources[resources.MEMORY])
	assert.Equal(t, 40, mem, "allocated node-3 resource not correct after node set to scheduling")

	// check the cleanup of the reservation
	assert.Equal(t, 0, len(app.GetReservations()), "reservations found on app")
	assert.Equal(t, 0, len(ms.getSchedulingNode(nodes[0]).GetReservations()), "reservation found on %s", nodes[0])
	assert.Equal(t, 0, len(ms.getSchedulingNode(nodes[1]).GetReservations()), "reservation found on %s", nodes[1])
	assert.Equal(t, 0, ms.getPartitionReservations()[appID], "reservations not removed from partition")
}

func createNodes(t *testing.T, ms *mockScheduler, count int, res int64) []string {
	nodes := make([]string, 0)
	nodeRes := map[string]*si.Quantity{"memory": {Value: res}, "vcore": {Value: res}}
	for i := 1; i < count+1; i++ {
		node := "node-" + strconv.Itoa(i)
		err := ms.addNode(node, &si.Resource{
			Resources: nodeRes,
		})
		assert.NilError(t, err, "node creation failed")

		nodes = append(nodes, node)
	}
	return nodes
}
