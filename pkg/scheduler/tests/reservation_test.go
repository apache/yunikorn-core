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
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
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
                memory: 100M
                vcore: 100
              max:
                memory: 200M
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
                memory: 100M
                vcore: 100
              max:
                memory: 500M
                vcore: 500
          - name: leaf-2
            resources:
              guaranteed:
                memory: 100M
                vcore: 100
              max:
                memory: 500M
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

	err := ms.Init(SingleQueueConfig, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// override the reservation delay, and cleanup when done
	objects.SetReservationDelay(10 * time.Nanosecond)
	defer objects.SetReservationDelay(2 * time.Second)

	nodes := createNodes(t, ms, 2, 50000000, 50000)
	ms.mockRM.waitForMinAcceptedNodes(t, 2, 1000)

	queueName := "root.leaf-1"
	err = ms.addApp(appID1, queueName, "default")
	assert.NilError(t, err, "adding app to scheduler failed")

	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)
	// Get scheduling app
	app := ms.getApplication(appID1)

	res := &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 20000000}, "vcore": {Value: 20000}}}
	err = ms.addAppRequest(appID1, "alloc-1", res, 6)
	assert.NilError(t, err, "adding requests to app failed")

	leafQueue := ms.getQueue(queueName)
	waitForPendingQueueResource(t, leafQueue, 120000000, 1000)
	waitForPendingAppResource(t, app, 120000000, 1000)

	// Allocate for app
	ms.scheduler.MultiStepSchedule(8)
	ms.mockRM.waitForAllocations(t, 4, 1000)

	// Verify that all 4 requests are satisfied
	mem := int(app.GetAllocatedResource().Resources[common.Memory])
	assert.Equal(t, 80000000, mem, "allocated resource after alloc not correct")
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), ms.partitionName, nodes, 80000000, 1000)

	// check the pending resources (2 * 20 still outstanding)
	waitForPendingQueueResource(t, leafQueue, 40000000, 1000)
	waitForPendingAppResource(t, app, 40000000, 1000)
	ms.scheduler.MultiStepSchedule(6)

	// Allocation should not change (still 4)
	ms.mockRM.waitForAllocations(t, 4, 1000)

	// check objects have reservations assigned,
	assert.Equal(t, 2, len(app.GetReservations()), "reservations missing from app")
	assert.Equal(t, 1, len(ms.getNode(nodes[0]).GetReservationKeys()), "reservation missing on %s", nodes[0])
	assert.Equal(t, 1, len(ms.getNode(nodes[1]).GetReservationKeys()), "reservation missing on %s", nodes[1])

	// Remove app-1
	err = ms.removeApp(appID1, "default")
	assert.NilError(t, err, "application removal failed")

	// Check allocated resource of queue, should be 0 now
	waitForAllocatedQueueResource(t, leafQueue, 0, 1000)
	mem = int(app.GetAllocatedResource().Resources[common.Memory])
	assert.Equal(t, 0, mem, "allocated app resource not correct after app removal")
	mem = int(ms.getNode(nodes[0]).GetAllocatedResource().Resources[common.Memory])
	assert.Equal(t, 0, mem, "allocated node-1 resource not correct after app removal")
	mem = int(ms.getNode(nodes[1]).GetAllocatedResource().Resources[common.Memory])
	assert.Equal(t, 0, mem, "allocated node-2 resource not correct after app removal")

	// App/node should not have reservation now
	assert.Equal(t, 0, len(app.GetReservations()), "reservations missing from app")
	assert.Equal(t, 0, len(ms.getNode(nodes[0]).GetReservationKeys()), "reservation missing on %s", nodes[0])
	assert.Equal(t, 0, len(ms.getNode(nodes[1]).GetReservationKeys()), "reservation missing on %s", nodes[1])
}

// setup multiple queues, apps and nodes
// queue1 with app1 takes all nodes, leaving small space
// queue2 with app2 and app3, app2 has largest requests and reserves both nodes
// app3 is starved
// fail app1 and make sure app2 gets its reservations filled followed by app3
func TestReservationForTwoQueues(t *testing.T) {
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(DualQueueConfig, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")
	// override the reservation delay, and cleanup when done
	objects.SetReservationDelay(10 * time.Nanosecond)
	defer objects.SetReservationDelay(2 * time.Second)

	nodes := createNodes(t, ms, 2, 50000000, 50000)
	ms.mockRM.waitForMinAcceptedNodes(t, 2, 1000)

	// add the first scheduling app
	leaf1Name := "root.leaf-1"
	err = ms.addApp(appID1, leaf1Name, "default")
	assert.NilError(t, err, "adding app 1 to scheduler failed")

	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)
	app1 := ms.getApplication(appID1)
	// add the second scheduling app
	leaf2name := "root.leaf-2"
	err = ms.addApp(appID2, leaf2name, "default")
	assert.NilError(t, err, "adding app 2 to scheduler failed")

	ms.mockRM.waitForAcceptedApplication(t, appID2, 1000)
	app2 := ms.getApplication(appID2)

	// add the third scheduling app
	app3ID := "app-3"
	err = ms.addApp(app3ID, leaf2name, "default")
	assert.NilError(t, err, "adding app 3 to scheduler failed")

	ms.mockRM.waitForAcceptedApplication(t, app3ID, 1000)
	app3 := ms.getApplication(app3ID)

	leaf1 := ms.getQueue("root.leaf-1")
	leaf2 := ms.getQueue("root.leaf-2")

	resLarge := &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 20000000}, "vcore": {Value: 20000}}}
	resSmall := &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 5000000}, "vcore": {Value: 5000}}}

	// allocate bulk of the node
	err = ms.addAppRequest(appID1, "alloc-1", resLarge, 4)
	assert.NilError(t, err, "adding request to app-1 failed")

	waitForPendingQueueResource(t, leaf1, 80000000, 1000)
	waitForPendingAppResource(t, app1, 80000000, 1000)

	// Allocate for app 1
	ms.scheduler.MultiStepSchedule(6)
	ms.mockRM.waitForAllocations(t, 4, 1000)
	mem := int(app1.GetAllocatedResource().Resources[common.Memory])
	assert.Equal(t, 80000000, mem, "allocated resource after alloc not correct")
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), ms.partitionName, nodes, 80000000, 1000)

	// these will be reserved (one on each node)
	err = ms.addAppRequest(appID2, "alloc-2", resLarge, 2)
	assert.NilError(t, err, "adding request to app-2 failed")

	// nothing will happen with these
	err = ms.addAppRequest(app3ID, "alloc-3", resSmall, 2)
	assert.NilError(t, err, "adding request to app-3 failed")

	waitForPendingQueueResource(t, leaf2, 50000000, 1000)
	waitForPendingAppResource(t, app2, 40000000, 1000)
	waitForPendingAppResource(t, app3, 10000000, 1000)

	// Allocate for rest of apps
	ms.scheduler.MultiStepSchedule(6)

	// Allocation should not change
	ms.mockRM.waitForAllocations(t, 4, 1000)

	// both reservations should be for the same app
	assert.Equal(t, 2, len(app2.GetReservations()), "app-2 should have 2 reservations")
	assert.Equal(t, 0, len(app3.GetReservations()), "app-3 should have no reservations")

	// Now remove app-1, and do allocation
	err = ms.removeApp(appID1, "default")
	assert.NilError(t, err, "application removal from partition failed")

	// Check allocated resource of queue, should be 0 now
	ms.mockRM.waitForAllocations(t, 0, 1000)
	waitForAllocatedQueueResource(t, leaf1, 0, 1000)

	// Allocate twice, which we should see two reservations filled
	ms.scheduler.MultiStepSchedule(2)
	ms.mockRM.waitForAllocations(t, 2, 1000)

	// Check allocated resource of queue, should be 40 now
	waitForAllocatedQueueResource(t, leaf2, 40000000, 1000)
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), ms.partitionName, nodes, 40000000, 1000)

	// Reservation should be filled
	assert.Equal(t, 0, len(app2.GetReservations()), "app-2 should have no reservations")

	// Do last allocations
	ms.scheduler.MultiStepSchedule(2)
	waitForAllocatedQueueResource(t, leaf2, 50000000, 1000)
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), ms.partitionName, nodes, 50000000, 1000)
}

// remove a node with reservation, reservation should be removed
func TestRemoveReservedNode(t *testing.T) {
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(SingleQueueConfig, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")
	// override the reservation delay, and cleanup when done
	objects.SetReservationDelay(10 * time.Nanosecond)
	defer objects.SetReservationDelay(2 * time.Second)

	nodes := createNodes(t, ms, 2, 50000000, 50000)
	ms.mockRM.waitForMinAcceptedNodes(t, 2, 1000)

	queueName := "root.leaf-1"
	err = ms.addApp(appID1, queueName, "default")
	assert.NilError(t, err, "adding app to scheduler failed")

	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)
	// Get scheduling app
	app := ms.getApplication(appID1)

	res := &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 20000000}, "vcore": {Value: 20000}}}
	err = ms.addAppRequest(appID1, "alloc-1", res, 6)
	assert.NilError(t, err, "adding requests to app failed")

	leafQueue := ms.getQueue(queueName)
	waitForPendingQueueResource(t, leafQueue, 120000000, 1000)
	waitForPendingAppResource(t, app, 120000000, 1000)

	// Allocate for app
	ms.scheduler.MultiStepSchedule(10)
	waitForAllocatedAppResource(t, app, 80000000, 1000)
	ms.mockRM.waitForAllocations(t, 4, 1000)
	waitForPendingAppResource(t, app, 40000000, 1000)
	// check objects have reservations assigned,
	assert.Equal(t, 2, len(app.GetReservations()), "reservations missing from app")
	assert.Equal(t, 1, len(ms.getNode(nodes[0]).GetReservationKeys()), "reservation missing on %s", nodes[0])
	assert.Equal(t, 1, len(ms.getNode(nodes[1]).GetReservationKeys()), "reservation missing on %s", nodes[1])

	// remove the 2nd node
	err = ms.removeNode(nodes[1])
	assert.NilError(t, err, "node removal failed")

	waitForRemovedNode(t, ms.serviceContext.Scheduler.GetClusterContext(), nodes[1], ms.partitionName, 1000)
	waitForAllocatedAppResource(t, app, 40000000, 1000)
	ms.mockRM.waitForAllocations(t, 2, 1000)
	assert.Equal(t, 1, len(app.GetReservations()), "reservations missing from app")
	assert.Equal(t, 1, len(ms.getNode(nodes[0]).GetReservationKeys()), "reservation missing on %s", nodes[0])
}

// add a node, reservation should be converted to allocations automatically
func TestAddNewNode(t *testing.T) {
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(SingleQueueConfig, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// override the reservation delay, and cleanup when done
	objects.SetReservationDelay(10 * time.Nanosecond)
	defer objects.SetReservationDelay(2 * time.Second)

	nodes := createNodes(t, ms, 3, 50000000, 50000)
	ms.mockRM.waitForMinAcceptedNodes(t, 3, 1000)
	ms.scheduler.GetClusterContext().GetPartition(ms.partitionName).GetNode(nodes[2]).SetSchedulable(false)

	queueName := "root.leaf-1"
	err = ms.addApp(appID1, queueName, "default")
	assert.NilError(t, err, "adding app to scheduler failed")

	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)
	// Get scheduling app
	app := ms.getApplication(appID1)

	res := &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 20000000}, "vcore": {Value: 20000}}}
	err = ms.addAppRequest(appID1, "alloc-1", res, 6)
	assert.NilError(t, err, "adding requests to app failed")

	leafQueue := ms.getQueue(queueName)
	waitForPendingQueueResource(t, leafQueue, 120000000, 1000)
	waitForPendingAppResource(t, app, 120000000, 1000)

	// Allocate for app
	ms.scheduler.MultiStepSchedule(10)
	ms.mockRM.waitForAllocations(t, 4, 1000)
	// check objects have reservations assigned,
	assert.Equal(t, 2, len(app.GetReservations()), "reservations missing from app")
	assert.Equal(t, 1, len(ms.getNode(nodes[0]).GetReservationKeys()), "reservation missing on %s", nodes[0])
	assert.Equal(t, 1, len(ms.getNode(nodes[1]).GetReservationKeys()), "reservation missing on %s", nodes[1])

	// change the third node to scheduling (simulates new node)
	ms.scheduler.GetClusterContext().GetPartition(ms.partitionName).GetNode(nodes[2]).SetSchedulable(true)

	// start allocating: both reservations should be moved to the 3rd node
	ms.scheduler.MultiStepSchedule(10)
	ms.mockRM.waitForAllocations(t, 6, 1000)
	waitForAllocatedAppResource(t, app, 120000000, 1000)
	waitForPendingQueueResource(t, leafQueue, 0, 1000)
	waitForPendingAppResource(t, app, 0, 1000)
	mem := int(ms.getNode(nodes[2]).GetAllocatedResource().Resources[common.Memory])
	assert.Equal(t, 40000000, mem, "allocated node-3 resource not correct after node set to scheduling")

	// check the cleanup of the reservation
	assert.Equal(t, 0, len(app.GetReservations()), "reservations found on app")
	assert.Equal(t, 0, len(ms.getNode(nodes[0]).GetReservationKeys()), "reservation found on %s", nodes[0])
	assert.Equal(t, 0, len(ms.getNode(nodes[1]).GetReservationKeys()), "reservation found on %s", nodes[1])
}

// simple reservation one app one queue
func TestUnReservationAndDeletion(t *testing.T) {
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(SingleQueueConfig, false, false)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// override the reservation delay, and cleanup when done
	objects.SetReservationDelay(10 * time.Nanosecond)
	defer objects.SetReservationDelay(2 * time.Second)

	nodes := createNodes(t, ms, 2, 30000000, 30000)
	ms.mockRM.waitForMinAcceptedNodes(t, 2, 1000)

	queueName := "root.leaf-1"
	err = ms.addApp(appID1, queueName, "default")
	assert.NilError(t, err, "adding app to scheduler failed")
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)
	// Get scheduling app
	app := ms.getApplication(appID1)

	// 3 asks, each one asks for 20 cpu/memory
	// we only have 2 nodes, 30 * 2
	// 2 asks can be allocated, the other one will be reserved
	res := &si.Resource{Resources: map[string]*si.Quantity{"memory": {Value: 20000000}, "vcore": {Value: 20000}}}
	err = ms.addAppRequest(appID1, "alloc-1", res, 1)
	assert.NilError(t, err, "adding requests to app failed")
	err = ms.addAppRequest(appID1, "alloc-2", res, 1)
	assert.NilError(t, err, "adding requests to app failed")
	err = ms.addAppRequest(appID1, "alloc-3", res, 1)
	assert.NilError(t, err, "adding requests to app failed")
	leafQueue := ms.getQueue(queueName)
	waitForPendingQueueResource(t, leafQueue, 60000000, 1000)
	waitForPendingAppResource(t, app, 60000000, 1000)
	// Allocate for app
	ms.scheduler.MultiStepSchedule(5)
	ms.mockRM.waitForAllocations(t, 2, 1000)
	// 40 allocated, 20 pending
	waitForPendingQueueResource(t, leafQueue, 20000000, 1000)
	waitForPendingAppResource(t, app, 20000000, 1000)
	// check objects have reservations assigned,
	assert.Equal(t, 1, len(app.GetReservations()), "reservations missing from app")
	numOfReservation := len(ms.getNode(nodes[0]).GetReservationKeys()) + len(ms.getNode(nodes[1]).GetReservationKeys())
	assert.Equal(t, 1, numOfReservation, "reservation missing on nodes")
	// delete pending asks
	for _, ask := range app.GetReservations() {
		askID := ask[strings.Index(ask, "|")+1:]
		err = ms.releaseAskRequest(appID1, askID)
		assert.NilError(t, err, "ask release update failed")
	}
	// delete existing allocations
	for _, alloc := range ms.mockRM.getAllocations() {
		err = ms.releaseAllocRequest(appID1, alloc.AllocationID)
		assert.NilError(t, err, "allocation release update failed")
	}
	ms.scheduler.MultiStepSchedule(5)
	// since all requests are removed, there should be no more pending resources
	waitForPendingQueueResource(t, leafQueue, 0, 1000)
	waitForPendingAppResource(t, app, 0, 1000)
	// there is no reservations anymore
	assert.Equal(t, len(ms.getNode(nodes[0]).GetReservationKeys()), 0)
	assert.Equal(t, len(ms.getNode(nodes[1]).GetReservationKeys()), 0)
	assert.Equal(t, len(app.GetReservations()), 0)
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), ms.partitionName, []string{nodes[0]}, 0, 1000)
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), ms.partitionName, []string{nodes[1]}, 0, 1000)
	//nolint: errcheck
	nodeRes, _ := resources.NewResourceFromConf(map[string]string{"memory": "30M", "vcore": "30"})
	waitForAvailableNodeResource(t, ms.scheduler.GetClusterContext(), ms.partitionName, []string{"node-1", "node-2"}, 60000000, 1000)
	waitForAllocatedNodeResource(t, ms.scheduler.GetClusterContext(), ms.partitionName, []string{"node-1", "node-2"}, 0, 1000)
	if !resources.Equals(nodeRes, ms.getNode(nodes[0]).GetAvailableResource()) {
		t.Fatalf("available resources not alll resources: expected %s, got %s", nodeRes, ms.getNode(nodes[0]).GetAvailableResource())
	}
	if !resources.Equals(nodeRes, ms.getNode(nodes[1]).GetAvailableResource()) {
		t.Fatalf("available resources not alll resources: expected %s, got %s", nodeRes, ms.getNode(nodes[1]).GetAvailableResource())
	}
}

func createNodes(t *testing.T, ms *mockScheduler, count int, mem int64, vcore int64) []string {
	nodes := make([]string, 0)
	nodeRes := map[string]*si.Quantity{"memory": {Value: mem}, "vcore": {Value: vcore}}
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
