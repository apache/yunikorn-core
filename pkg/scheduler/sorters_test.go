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
	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

// verify queue ordering is working
func TestSortQueues(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	cache.SetGuaranteedResource(root.QueueInfo,
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000, "vcore": 1000}))

	var q0, q1, q2 *SchedulingQueue
	q0, err = createManagedQueue(root, "q0", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	cache.SetGuaranteedResource(q0.QueueInfo,
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500, "vcore": 500}))
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(300),
		"vcore":  resources.Quantity(300)})

	q1, err = createManagedQueue(root, "q1", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	cache.SetGuaranteedResource(q1.QueueInfo,
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300}))
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore":  resources.Quantity(200)})

	q2, err = createManagedQueue(root, "q2", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	cache.SetGuaranteedResource(q2.QueueInfo,
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200}))
	q2.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(100),
		"vcore":  resources.Quantity(100)})

	// fairness ratios: q0:300/500=0.6, q1:200/300=0.67, q2:100/200=0.5
	queues := []*SchedulingQueue{q0, q1, q2}
	sortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{1, 2, 0})

	// fairness ratios: q0:200/500=0.4, q1:300/300=1, q2:100/200=0.5
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	sortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{0, 2, 1})

	// fairness ratios: q0:150/500=0.3, q1:120/300=0.4, q2:100/200=0.5
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 150, "vcore": 150})
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 120, "vcore": 120})
	q2.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100, "vcore": 100})
	sortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{0, 1, 2})
}

// queue guaranteed resource is 0
func TestNoQueueLimits(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	cache.SetGuaranteedResource(root.QueueInfo,
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 0, "vcore": 0}))

	var q0, q1, q2 *SchedulingQueue
	q0, err = createManagedQueue(root, "q0", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	cache.SetGuaranteedResource(q0.QueueInfo,
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 0, "vcore": 0}))
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(300),
		"vcore":  resources.Quantity(300)})

	q1, err = createManagedQueue(root, "q1", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	cache.SetGuaranteedResource(q1.QueueInfo,
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 0, "vcore": 0}))
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore":  resources.Quantity(200)})

	q2, err = createManagedQueue(root, "q2", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	cache.SetGuaranteedResource(q2.QueueInfo,
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 0, "vcore": 0}))
	q2.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(100),
		"vcore":  resources.Quantity(100)})

	queues := []*SchedulingQueue{q0, q1, q2}
	sortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{2, 1, 0})

	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	sortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{1, 2, 0})
}

// queue guaranteed resource is not set
func TestQueueGuaranteedResourceNotSet(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	cache.SetGuaranteedResource(root.QueueInfo, nil)

	var q0, q1, q2 *SchedulingQueue
	q0, err = createManagedQueue(root, "q0", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	cache.SetGuaranteedResource(q0.QueueInfo, nil)
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(300),
		"vcore":  resources.Quantity(300)})

	q1, err = createManagedQueue(root, "q1", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	cache.SetGuaranteedResource(q1.QueueInfo, nil)
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore":  resources.Quantity(200)})

	// q2 has no guaranteed resource (nil)
	q2, err = createManagedQueue(root, "q2", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	cache.SetGuaranteedResource(q2.QueueInfo, nil)

	queues := []*SchedulingQueue{q0, q1, q2}
	sortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{2, 1, 0})
}

func TestSortNodesBin(t *testing.T) {
	// nil or empty list cannot panic
	sortNodes(nil, common.BinPackingPolicy)
	list := make([]*SchedulingNode, 0)
	sortNodes(list, common.BinPackingPolicy)
	list = append(list, newSchedulingNode(cache.NewNodeForSort("node-nil", nil)))
	sortNodes(list, common.BinPackingPolicy)

	// stable sort is used so equal resources stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})

	// setup to sort ascending
	list = make([]*SchedulingNode, 3)
	for i := 0; i < 3; i++ {
		num := strconv.Itoa(i)
		node := newSchedulingNode(
			cache.NewNodeForSort("node-"+num, resources.Multiply(res, int64(3-i))),
		)
		list[i] = node
	}
	// nodes should come back in order 2 (100), 1 (200), 0 (300)
	sortNodes(list, common.BinPackingPolicy)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-1 on place 1 in the slice to have no res
	list[1] = newSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 0)),
	)
	// nodes should come back in order 1 (0), 2 (100), 0 (300)
	sortNodes(list, common.BinPackingPolicy)
	assertNodeList(t, list, []int{2, 0, 1})

	// change node-1 on place 0 in the slice to have 300 res
	list[0] = newSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 3)),
	)
	// nodes should come back in order 2 (100), 1 (300), 0 (300)
	sortNodes(list, common.BinPackingPolicy)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-0 on place 2 in the slice to have -300 res
	list[2] = newSchedulingNode(
		cache.NewNodeForSort("node-0", resources.Multiply(res, -3)),
	)
	// nodes should come back in order 0 (-300), 2 (100), 1 (300)
	sortNodes(list, common.BinPackingPolicy)
	assertNodeList(t, list, []int{0, 2, 1})
}

func TestSortNodesFair(t *testing.T) {
	// nil or empty list cannot panic
	sortNodes(nil, common.FairnessPolicy)
	list := make([]*SchedulingNode, 0)
	sortNodes(list, common.FairnessPolicy)
	list = append(list, newSchedulingNode(cache.NewNodeForSort("node-nil", nil)))
	sortNodes(list, common.FairnessPolicy)

	// stable sort is used so equal resources stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending
	list = make([]*SchedulingNode, 3)
	for i := 0; i < 3; i++ {
		num := strconv.Itoa(i)
		node := newSchedulingNode(
			cache.NewNodeForSort("node-"+num, resources.Multiply(res, int64(1+i))),
		)
		list[i] = node
	}
	// nodes should come back in order 2 (300), 1 (200), 0 (100)
	sortNodes(list, common.FairnessPolicy)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-1 on place 1 in the slice to have no res
	list[1] = newSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 0)),
	)
	// nodes should come back in order 2 (300), 0 (100), 1 (0)
	sortNodes(list, common.FairnessPolicy)
	assertNodeList(t, list, []int{1, 2, 0})

	// change node-1 on place 2 in the slice to have 300 res
	list[2] = newSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 3)),
	)
	// nodes should come back in order 2 (300), 1 (300), 0 (100)
	sortNodes(list, common.FairnessPolicy)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-2 on place 0 in the slice to have -300 res
	list[0] = newSchedulingNode(
		cache.NewNodeForSort("node-2", resources.Multiply(res, -3)),
	)
	// nodes should come back in order 1 (300), 0 (100), 2 (-300)
	sortNodes(list, common.FairnessPolicy)
	assertNodeList(t, list, []int{1, 0, 2})
}

func TestSortAppsNoPending(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	input := make(map[string]*SchedulingApplication, 4)
	for i := 0; i < 2; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newSchedulingApplication(
			cache.NewApplicationInfo(appID, "partition", "queue",
				security.UserGroup{}, nil))
		app.allocating = resources.Multiply(res, int64(i+1))
		input[appID] = app
	}

	// no apps with pending resources should come back empty
	list := sortApplications(input, policies.FairSortPolicy, nil)
	assertAppListLength(t, list, []string{})
	list = sortApplications(input, policies.FifoSortPolicy, nil)
	assertAppListLength(t, list, []string{})
	list = sortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{})

	// set one app with pending
	appID := "app-1"
	input[appID].pending = res
	list = sortApplications(input, policies.FairSortPolicy, nil)
	assertAppListLength(t, list, []string{appID})
	list = sortApplications(input, policies.FifoSortPolicy, nil)
	assertAppListLength(t, list, []string{appID})
	list = sortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{appID})
}

func TestSortAppsFifo(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending: all apps have pending resources
	input := make(map[string]*SchedulingApplication, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newSchedulingApplication(
			cache.NewApplicationInfo(appID, "partition", "queue",
				security.UserGroup{}, nil))
		app.allocating = resources.Multiply(res, int64(i+1))
		app.pending = res
		input[appID] = app
		// make sure the time stamps differ at least a bit (tracking in nano seconds)
		time.Sleep(time.Nanosecond * 5)
	}
	// map iteration is random so we do not need to check input
	// apps should come back in order created 0, 1, 2, 3
	fifoAppSortPolicy, _ := newAppSortPolicy(policies.FifoSortPolicy)
	list := filterOnPendingResources(input)
	fifoAppSortPolicy.sortApplications(list, &cache.QueueInfo{})
	assertAppList(t, list, []int{0, 1, 2, 3})
}

func TestSortAppsFair(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending: all apps have pending resources
	input := make(map[string]*SchedulingApplication, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newSchedulingApplication(
			cache.NewApplicationInfo(appID, "partition", "queue",
				security.UserGroup{}, nil))
		app.allocating = resources.Multiply(res, int64(i+1))
		app.pending = res
		input[appID] = app
	}
	// nil resource: usage based sorting
	// apps should come back in order: 0, 1, 2, 3
	fairAppSortPolicy, _ := newAppSortPolicy(policies.FairSortPolicy)
	list := filterOnPendingResources(input)
	queueInfo := &cache.QueueInfo{}
	fairAppSortPolicy.sortApplications(list, queueInfo)
	assertAppList(t, list, []int{0, 1, 2, 3})

	// apps should come back in order: 0, 1, 2, 3
	queueInfo.SetGuaranteedResource(resources.Multiply(res, 0))
	fairAppSortPolicy.sortApplications(list, queueInfo)
	assertAppList(t, list, []int{0, 1, 2, 3})

	// apps should come back in order: 0, 1, 2, 3
	queueInfo.SetGuaranteedResource(resources.Multiply(res, 5))
	fairAppSortPolicy.sortApplications(list, queueInfo)
	assertAppList(t, list, []int{0, 1, 2, 3})

	// update allocated resource for app-1
	input["app-1"].allocating = resources.Multiply(res, 10)
	// apps should come back in order: 0, 2, 3, 1
	fairAppSortPolicy.sortApplications(list, queueInfo)
	assertAppList(t, list, []int{0, 3, 1, 2})

	// update allocated resource for app-3 to negative (move to head of the list)
	input["app-3"].allocating = resources.Multiply(res, -10)
	// apps should come back in order: 3, 0, 2, 1
	fairAppSortPolicy.sortApplications(list, queueInfo)
	for i := 0; i < 4; i++ {
		log.Logger().Info("allocated res",
			zap.Int("order", i),
			zap.String("name", list[i].ApplicationInfo.ApplicationID),
			zap.Any("res", list[i].allocating))
	}
	assertAppList(t, list, []int{1, 3, 2, 0})
}

func TestSortAppsStateAware(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup all apps with pending resources, all accepted state
	input := make(map[string]*SchedulingApplication, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newSchedulingApplication(
			cache.NewApplicationInfo(appID, "partition", "queue",
				security.UserGroup{}, nil))
		app.allocating = resources.Multiply(res, int64(i+1))
		app.pending = res
		input[appID] = app
		err := app.ApplicationInfo.HandleApplicationEvent(cache.RunApplication)
		assert.NilError(t, err, "state change failed for app %v", appID)
		// make sure the time stamps differ at least a bit (tracking in nano seconds)
		time.Sleep(time.Nanosecond * 5)
	}
	// only first app should be returned (all in accepted)
	list := sortApplications(input, policies.StateAwarePolicy, nil)
	appID0 := "app-0"
	assertAppListLength(t, list, []string{appID0})

	// set first app pending to zero, should get 2nd app back
	input[appID0].pending = resources.NewResource()
	list = sortApplications(input, policies.StateAwarePolicy, nil)
	appID1 := "app-1"
	assertAppListLength(t, list, []string{appID1})

	// move the first app to starting no pending resource should get nothing
	err := input[appID0].ApplicationInfo.HandleApplicationEvent(cache.RunApplication)
	assert.NilError(t, err, "state change failed for app-0")
	list = sortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{})

	// move first app to running (no pending resource) and 4th app to starting should get starting app
	err = input[appID0].ApplicationInfo.HandleApplicationEvent(cache.RunApplication)
	assert.NilError(t, err, "state change failed for app-0")
	appID3 := "app-3"
	err = input[appID3].ApplicationInfo.HandleApplicationEvent(cache.RunApplication)
	assert.NilError(t, err, "state change failed for app-3")
	list = sortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{appID3})

	// set pending for first app, should get back 1st and 4th in that order
	input[appID0].pending = res
	list = sortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{appID0, appID3})

	// move 4th to running should get back: 1st, 2nd and 4th in that order
	err = input[appID3].ApplicationInfo.HandleApplicationEvent(cache.RunApplication)
	assert.NilError(t, err, "state change failed for app-3")
	list = sortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{appID0, appID1, appID3})
}

func TestSortAppsPriorityFifo(t *testing.T) {
	priorityFifoAppSortPolicy, _ := newAppSortPolicy(policies.FifoSortPolicy)
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	queue, err := createRootQueue(nil)
	if err != nil {
		t.Fatalf("queue create failed: %v", err)
	}
	// setup to sort descending
	apps := make([]*SchedulingApplication, 4)
	list := make([]*SchedulingApplication, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		app := newSchedulingApplication(
			cache.NewApplicationInfo("app-"+num, "partition", "queue",
				security.UserGroup{}, nil))
		app.queue = queue
		// add 2 asks with priority P(i) and P(i+1) for app,
		// app-0 has P0 and P1 asks, app-1 has P1 and P2 asks,
		// app-2 has P2 and P3 asks, app-3 has P3 and P4 asks.
		ask1 := newAllocationAskWithPriority("ask-P"+strconv.Itoa(i), app.ApplicationInfo.ApplicationID, res, int32(i))
		ask2 := newAllocationAskWithPriority("ask-P"+strconv.Itoa(i+1), app.ApplicationInfo.ApplicationID, res, int32(i+1))
		app.addAllocationAsk(ask1)
		app.addAllocationAsk(ask2)
		list[i] = app
		apps[i] = app
	}
	// move things around to see sorting
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAppList(t, list, []int{2, 3, 0, 1})
	// expected sort result: app-3(P3 P4) app-2(P2 P3) app-1(P1 P2) app-0(P0 P1)
	priorityFifoAppSortPolicy.sortApplications(list, nil)
	assertAppList(t, list, []int{3, 2, 1, 0})
	// assert priorities of pending asks for scheduling,
	// only asks with top priority should be chosen for scheduling.
	reqIt := priorityFifoAppSortPolicy.getPendingRequestIterator(apps[0])
	assertAskPriorities(t, reqIt, []int32{1})
	reqIt = priorityFifoAppSortPolicy.getPendingRequestIterator(apps[1])
	assertAskPriorities(t, reqIt, []int32{2})
	reqIt = priorityFifoAppSortPolicy.getPendingRequestIterator(apps[2])
	assertAskPriorities(t, reqIt, []int32{3})
	reqIt = priorityFifoAppSortPolicy.getPendingRequestIterator(apps[3])
	assertAskPriorities(t, reqIt, []int32{4})

	// add P5 ask for app-0
	// expected sort result: app-0(P0 P1 P5) app-3(P3 P4) app-2(P2 P3) app-1(P1 P2)
	newAsk := newAllocationAskWithPriority("ask-P5", apps[0].ApplicationInfo.ApplicationID, res, int32(5))
	apps[0].addAllocationAsk(newAsk)
	// apps should come back in order: 0, 3, 2, 1
	priorityFifoAppSortPolicy.sortApplications(list, nil)
	assertAppList(t, list, []int{0, 3, 2, 1})
	// now only P5 ask can be scheduled for app-0
	reqIt = priorityFifoAppSortPolicy.getPendingRequestIterator(apps[0])
	assertAskPriorities(t, reqIt, []int32{5})

	// remove P4 ask of app-0
	// expected sort result: app-0(P0 P1 P5) app-2(P2 P3) app-3(P3) app-1(P1 P2)
	// app-2 is located before app-3 since its P3 ask is older than that of app-3.
	apps[3].removeAllocationAsk("ask-P4")
	priorityFifoAppSortPolicy.sortApplications(list, nil)
	// apps should come back in order: 0, 3, 1, 2
	assertAppList(t, list, []int{0, 3, 1, 2})
	// now only P3 ask can be scheduled for app-0
	reqIt = priorityFifoAppSortPolicy.getPendingRequestIterator(apps[3])
	assertAskPriorities(t, reqIt, []int32{3})

	// move things around to see sorting
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]

	// remove P1 ask of app-0
	// expected sort result: app-0(P0 P5) app-2(P2 P3) app-3(P3) app-1(P1 P2)
	apps[0].removeAllocationAsk("ask-P1")
	priorityFifoAppSortPolicy.sortApplications(list, nil)
	// apps should come back in order: 0, 3, 1, 2
	assertAppList(t, list, []int{0, 3, 1, 2})

	// remove P5 ask of app-0
	// expected sort result: app-2(P2 P3) app-3(P3) app-1(P1 P2) app-0(P0)
	apps[0].removeAllocationAsk("ask-P5")
	priorityFifoAppSortPolicy.sortApplications(list, nil)
	// apps should come back in order: 3, 2, 0, 1
	assertAppList(t, list, []int{3, 2, 0, 1})

	// remove P3 ask of app-3
	// expected sort result: app-2(P2 P3) app-1(P1 P2) app-0(P0) app-3()
	apps[3].removeAllocationAsk("ask-P3")
	priorityFifoAppSortPolicy.sortApplications(list, nil)
	// apps should come back in order: 2, 1, 0, 3
	assertAppList(t, list, []int{2, 1, 0, 3})
	// now on ask can be scheduled for app-3
	reqIt = priorityFifoAppSortPolicy.getPendingRequestIterator(apps[3])
	assert.Equal(t, reqIt.HasNext(), false)
}

func TestSortAsks(t *testing.T) {
	// stable sort is used so equal values stay where they were
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
func assertNodeList(t *testing.T, list []*SchedulingNode, place []int) {
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

func assertAppListLength(t *testing.T, list []*SchedulingApplication, apps []string) {
	assert.Equal(t, len(apps), len(list), "length of list differs")
	for i, app := range list {
		assert.Equal(t, apps[i], app.ApplicationInfo.ApplicationID)
	}
}

func assertAskPriorities(t *testing.T, reqIt RequestIterator, expectedPriorities []int32) {
	reqIndex := 0
	for reqIt.HasNext() {
		ask := reqIt.Next()
		assert.Equal(t, ask.priority, expectedPriorities[reqIndex])
		reqIndex++
	}
	assert.Equal(t, reqIndex, len(expectedPriorities))
}
