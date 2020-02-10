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

package scheduler

import (
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"
	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

// verify queue ordering is working
func TestSortQueues(t *testing.T) {
	root, err := createRootQueue(nil)
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}
	root.QueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 1000, "vcore": 1000})

	var q0, q1, q2 *SchedulingQueue
	q0, err = createManagedQueue(root, "q0", false, nil)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q0.QueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 500, "vcore": 500})
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(300),
		"vcore":  resources.Quantity(300)})

	q1, err = createManagedQueue(root, "q1", false, nil)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q1.QueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 300, "vcore": 300})
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore":  resources.Quantity(200)})

	q2, err = createManagedQueue(root, "q2", false, nil)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q2.QueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q2.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(100),
		"vcore":  resources.Quantity(100)})

	// fairness ratios: q0:300/500=0.6, q1:200/300=0.67, q2:100/200=0.5
	queues := []*SchedulingQueue{q0, q1, q2}
	sortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{1, 2, 0})

	// fairness ratios: q0:200/500=0.4, q1:300/300=1, q2:100/200=0.5
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	sortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{0, 2, 1})

	// fairness ratios: q0:150/500=0.3, q1:120/300=0.4, q2:100/200=0.5
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 150, "vcore": 150})
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 120, "vcore": 120})
	q2.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100, "vcore": 100})
	sortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{0, 1, 2})
}

// queue guaranteed resource is 0
func TestNoQueueLimits(t *testing.T) {
	root, err := createRootQueue(nil)
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}
	root.QueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 0, "vcore": 0})

	var q0, q1, q2 *SchedulingQueue
	q0, err = createManagedQueue(root, "q0", false, nil)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q0.QueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 0, "vcore": 0})
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(300),
		"vcore":  resources.Quantity(300)})

	q1, err = createManagedQueue(root, "q1", false, nil)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q1.QueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 0, "vcore": 0})
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore":  resources.Quantity(200)})

	q2, err = createManagedQueue(root, "q2", false, nil)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q2.QueueInfo.GuaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 0, "vcore": 0})
	q2.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(100),
		"vcore":  resources.Quantity(100)})

	queues := []*SchedulingQueue{q0, q1, q2}
	sortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{2, 1, 0})

	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	sortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{1, 2, 0})
}

// queue guaranteed resource is not set
func TestQueueGuaranteedResourceNotSet(t *testing.T) {
	root, err := createRootQueue(nil)
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}
	root.QueueInfo.GuaranteedResource = nil

	var q0, q1, q2 *SchedulingQueue
	q0, err = createManagedQueue(root, "q0", false, nil)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q0.QueueInfo.GuaranteedResource = nil
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(300),
		"vcore":  resources.Quantity(300)})

	q1, err = createManagedQueue(root, "q1", false, nil)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q1.QueueInfo.GuaranteedResource = nil
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore":  resources.Quantity(200)})

	// q2 has no guaranteed resource (nil)
	q2, err = createManagedQueue(root, "q2", false, nil)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q2.QueueInfo.GuaranteedResource = nil

	queues := []*SchedulingQueue{q0, q1, q2}
	sortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{2, 1, 0})
}

func TestSortNodesMin(t *testing.T) {
	// nil or empty list cannot panic
	sortNodes(nil, MinAvailableResources)
	list := make([]*schedulingNode, 0)
	sortNodes(list, MinAvailableResources)
	list = append(list, newSchedulingNode(cache.NewNodeForSort("node-nil", nil)))
	sortNodes(list, MinAvailableResources)

	// stable sort is used so equal resources stay were they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})

	// setup to sort ascending
	list = make([]*schedulingNode, 3)
	for i := 0; i < 3; i++ {
		num := strconv.Itoa(i)
		node := newSchedulingNode(
			cache.NewNodeForSort("node-"+num, resources.Multiply(res, int64(3-i))),
		)
		list[i] = node
	}
	// nodes should come back in order 2 (100), 1 (200), 0 (300)
	sortNodes(list, MinAvailableResources)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-1 on place 1 in the slice to have no res
	list[1] = newSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 0)),
	)
	// nodes should come back in order 1 (0), 2 (100), 0 (300)
	sortNodes(list, MinAvailableResources)
	assertNodeList(t, list, []int{2, 0, 1})

	// change node-1 on place 0 in the slice to have 300 res
	list[0] = newSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 3)),
	)
	// nodes should come back in order 2 (100), 1 (300), 0 (300)
	sortNodes(list, MinAvailableResources)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-0 on place 2 in the slice to have -300 res
	list[2] = newSchedulingNode(
		cache.NewNodeForSort("node-0", resources.Multiply(res, -3)),
	)
	// nodes should come back in order 0 (-300), 2 (100), 1 (300)
	sortNodes(list, MinAvailableResources)
	assertNodeList(t, list, []int{0, 2, 1})
}

func TestSortNodesMax(t *testing.T) {
	// nil or empty list cannot panic
	sortNodes(nil, MaxAvailableResources)
	list := make([]*schedulingNode, 0)
	sortNodes(list, MaxAvailableResources)
	list = append(list, newSchedulingNode(cache.NewNodeForSort("node-nil", nil)))
	sortNodes(list, MaxAvailableResources)

	// stable sort is used so equal resources stay were they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending
	list = make([]*schedulingNode, 3)
	for i := 0; i < 3; i++ {
		num := strconv.Itoa(i)
		node := newSchedulingNode(
			cache.NewNodeForSort("node-"+num, resources.Multiply(res, int64(1+i))),
		)
		list[i] = node
	}
	// nodes should come back in order 2 (300), 1 (200), 0 (100)
	sortNodes(list, MaxAvailableResources)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-1 on place 1 in the slice to have no res
	list[1] = newSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 0)),
	)
	// nodes should come back in order 2 (300), 0 (100), 1 (0)
	sortNodes(list, MaxAvailableResources)
	assertNodeList(t, list, []int{1, 2, 0})

	// change node-1 on place 2 in the slice to have 300 res
	list[2] = newSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 3)),
	)
	// nodes should come back in order 2 (300), 1 (300), 0 (100)
	sortNodes(list, MaxAvailableResources)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-2 on place 0 in the slice to have -300 res
	list[0] = newSchedulingNode(
		cache.NewNodeForSort("node-2", resources.Multiply(res, -3)),
	)
	// nodes should come back in order 1 (300), 0 (100), 2 (-300)
	sortNodes(list, MaxAvailableResources)
	assertNodeList(t, list, []int{1, 0, 2})
}

func TestSortAppsFifo(t *testing.T) {
	// stable sort is used so equal values stay were they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending
	list := make([]*SchedulingApplication, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		app := newSchedulingApplication(
			cache.NewApplicationInfo("app-"+num, "partition", "queue",
				security.UserGroup{}, nil))
		app.allocating = resources.Multiply(res, int64(i+1))
		list[i] = app
		// make sure the time stamps differ at least a bit (tracking in nano seconds)
		time.Sleep(time.Nanosecond * 5)
	}
	// fifo sorts on time: move things around first
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAppList(t, list, []int{2, 3, 0, 1})
	// apps should come back in order created 0, 1, 2, 3
	sortApplications(list, FifoSortPolicy, nil)
	assertAppList(t, list, []int{0, 1, 2, 3})
}

func TestSortAppsFair(t *testing.T) {
	// stable sort is used so equal values stay were they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending
	list := make([]*SchedulingApplication, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		app := newSchedulingApplication(
			cache.NewApplicationInfo("app-"+num, "partition", "queue",
				security.UserGroup{}, nil))
		app.allocating = resources.Multiply(res, int64(i+1))
		list[i] = app
	}
	// nil resource: usage based sorting
	// move things around to see sorting
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAppList(t, list, []int{2, 3, 0, 1})
	// apps should come back in order: 0, 1, 2, 3
	sortApplications(list, FairSortPolicy, nil)
	assertAppList(t, list, []int{0, 1, 2, 3})

	// move things around
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAppList(t, list, []int{2, 3, 0, 1})
	// apps should come back in order: 0, 1, 2, 3
	sortApplications(list, FairSortPolicy, resources.Multiply(res, 0))
	assertAppList(t, list, []int{0, 1, 2, 3})

	// move things around
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAppList(t, list, []int{2, 3, 0, 1})
	// apps should come back in order: 0, 1, 2, 3
	sortApplications(list, FairSortPolicy, resources.Multiply(res, 5))
	assertAppList(t, list, []int{0, 1, 2, 3})

	// update allocated resource for app-1
	list[1].allocating = resources.Multiply(res, 10)
	// apps should come back in order: 0, 2, 3, 1
	sortApplications(list, FairSortPolicy, resources.Multiply(res, 5))
	assertAppList(t, list, []int{0, 3, 1, 2})

	// update allocated resource for app-3 to negative (move to head of the list)
	list[2].allocating = resources.Multiply(res, -10)
	// apps should come back in order: 3, 0, 2, 1
	sortApplications(list, FairSortPolicy, resources.Multiply(res, 5))
	for i := 0; i < 4; i++ {
		log.Logger().Info("allocated res",
			zap.Int("order", i),
			zap.String("name", list[i].ApplicationInfo.ApplicationID),
			zap.Any("res", list[i].allocating))
	}
	assertAppList(t, list, []int{1, 3, 2, 0})
}

func TestSortAsks(t *testing.T) {
	// stable sort is used so equal values stay were they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(1)})
	list := make([]*schedulingAllocationAsk, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		ask := newAllocationAsk("ask-"+num, "app-1", res)
		ask.priority = int32(i)
		list[i] = ask
	}
	// move things around
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAskList(t, list, []int{2, 3, 0, 1})
	sortAskByPriority(list, true)
	// asks should come back in order: 0, 1, 2, 3
	assertAskList(t, list, []int{0, 1, 2, 3})
	// move things around
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAskList(t, list, []int{2, 3, 0, 1})
	sortAskByPriority(list, false)
	// asks should come back in order: 3, 2, 1, 0
	assertAskList(t, list, []int{3, 2, 1, 0})
	// make asks with same priority
	// ask-3 and ask-1 both with prio 1 do not change order
	// ask-3 must always be earlier in the list
	list[0].priority = 1
	sortAskByPriority(list, true)
	// asks should come back in order: 0, 2, 3, 1
	assertAskList(t, list, []int{0, 2, 3, 1})
	sortAskByPriority(list, false)
	// asks should come back in order: 3, 2, 0, 1
	assertAskList(t, list, []int{3, 2, 0, 1})
}

// list of queues and the location of the named queue inside that list
// place[0] defines the location of the root.q0 in the list of queues
func assertQueueList(t *testing.T, list []*SchedulingQueue, place []int) {
	assert.Equal(t, "root.q0", list[place[0]].Name)
	assert.Equal(t, "root.q1", list[place[1]].Name)
	assert.Equal(t, "root.q2", list[place[2]].Name)
}

// list of nodes and the location of the named nodes inside that list
// place[0] defines the location of the node-0 in the list of nodes
func assertNodeList(t *testing.T, list []*schedulingNode, place []int) {
	assert.Equal(t, "node-0", list[place[0]].NodeID)
	assert.Equal(t, "node-1", list[place[1]].NodeID)
	assert.Equal(t, "node-2", list[place[2]].NodeID)
}

// list of application and the location of the named applications inside that list
// place[0] defines the location of the app-0 in the list of applications
func assertAppList(t *testing.T, list []*SchedulingApplication, place []int) {
	assert.Equal(t, "app-0", list[place[0]].ApplicationInfo.ApplicationID)
	assert.Equal(t, "app-1", list[place[1]].ApplicationInfo.ApplicationID)
	assert.Equal(t, "app-2", list[place[2]].ApplicationInfo.ApplicationID)
	assert.Equal(t, "app-3", list[place[3]].ApplicationInfo.ApplicationID)
}

// list of application and the location of the named applications inside that list
// place[0] defines the location of the app-0 in the list of applications
func assertAskList(t *testing.T, list []*schedulingAllocationAsk, place []int) {
	assert.Equal(t, "ask-0", list[place[0]].AskProto.AllocationKey)
	assert.Equal(t, "ask-1", list[place[1]].AskProto.AllocationKey)
	assert.Equal(t, "ask-2", list[place[2]].AskProto.AllocationKey)
	assert.Equal(t, "ask-3", list[place[3]].AskProto.AllocationKey)
}
