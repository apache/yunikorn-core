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

package objects

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
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

// verify queue ordering is working
func TestSortQueues(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	root.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000, "vcore": 1000})

	var q0, q1, q2 *Queue
	q0, err = createManagedQueue(root, "q0", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q0.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500, "vcore": 500})
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(300),
		"vcore":  resources.Quantity(300)})

	q1, err = createManagedQueue(root, "q1", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q1.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore":  resources.Quantity(200)})

	q2, err = createManagedQueue(root, "q2", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q2.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q2.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(100),
		"vcore":  resources.Quantity(100)})

	// fairness ratios: q0:300/500=0.6, q1:200/300=0.67, q2:100/200=0.5
	queues := []*Queue{q0, q1, q2}
	SortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{1, 2, 0})

	// fairness ratios: q0:200/500=0.4, q1:300/300=1, q2:100/200=0.5
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	SortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{0, 2, 1})

	// fairness ratios: q0:150/500=0.3, q1:120/300=0.4, q2:100/200=0.5
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 150, "vcore": 150})
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 120, "vcore": 120})
	q2.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100, "vcore": 100})
	SortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{0, 1, 2})
}

// queue guaranteed resource is 0
func TestNoQueueLimits(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	root.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 0, "vcore": 0})

	var q0, q1, q2 *Queue
	q0, err = createManagedQueue(root, "q0", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q0.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 0, "vcore": 0})
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(300),
		"vcore":  resources.Quantity(300)})

	q1, err = createManagedQueue(root, "q1", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q1.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 0, "vcore": 0})
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore":  resources.Quantity(200)})

	q2, err = createManagedQueue(root, "q2", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q2.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 0, "vcore": 0})
	q2.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(100),
		"vcore":  resources.Quantity(100)})

	queues := []*Queue{q0, q1, q2}
	SortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{2, 1, 0})

	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	SortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{1, 2, 0})
}

// queue guaranteed resource is not set
func TestQueueGuaranteedResourceNotSet(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	var q0, q1, q2 *Queue
	q0, err = createManagedQueue(root, "q0", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q0.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(300),
		"vcore":  resources.Quantity(300)})

	q1, err = createManagedQueue(root, "q1", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q1.allocating = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore":  resources.Quantity(200)})

	// q2 has no guaranteed resource (nil)
	q2, err = createManagedQueue(root, "q2", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")

	queues := []*Queue{q0, q1, q2}
	SortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{2, 1, 0})
}

func TestSortNodesBin(t *testing.T) {
	// nil or empty list cannot panic
	SortNodes(nil, policies.BinPackingPolicy)
	list := make([]*Node, 0)
	SortNodes(list, policies.BinPackingPolicy)
	list = append(list, newSchedulingNode(cache.NewNodeForSort("node-nil", nil)))
	SortNodes(list, policies.BinPackingPolicy)

	// stable sort is used so equal resources stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})

	// setup to sort ascending
	list = make([]*Node, 3)
	for i := 0; i < 3; i++ {
		num := strconv.Itoa(i)
		node := newSchedulingNode(
			cache.NewNodeForSort("node-"+num, resources.Multiply(res, int64(3-i))),
		)
		list[i] = node
	}
	// nodes should come back in order 2 (100), 1 (200), 0 (300)
	SortNodes(list, policies.BinPackingPolicy)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-1 on place 1 in the slice to have no res
	list[1] = newSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 0)),
	)
	// nodes should come back in order 1 (0), 2 (100), 0 (300)
	SortNodes(list, policies.BinPackingPolicy)
	assertNodeList(t, list, []int{2, 0, 1})

	// change node-1 on place 0 in the slice to have 300 res
	list[0] = newSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 3)),
	)
	// nodes should come back in order 2 (100), 1 (300), 0 (300)
	SortNodes(list, policies.BinPackingPolicy)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-0 on place 2 in the slice to have -300 res
	list[2] = newSchedulingNode(
		cache.NewNodeForSort("node-0", resources.Multiply(res, -3)),
	)
	// nodes should come back in order 0 (-300), 2 (100), 1 (300)
	SortNodes(list, policies.BinPackingPolicy)
	assertNodeList(t, list, []int{0, 2, 1})
}

func TestSortNodesFair(t *testing.T) {
	// nil or empty list cannot panic
	SortNodes(nil, policies.FairnessPolicy)
	list := make([]*Node, 0)
	SortNodes(list, policies.FairnessPolicy)
	list = append(list, newSchedulingNode(cache.NewNodeForSort("node-nil", nil)))
	SortNodes(list, policies.FairnessPolicy)

	// stable sort is used so equal resources stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending
	list = make([]*Node, 3)
	for i := 0; i < 3; i++ {
		num := strconv.Itoa(i)
		node := newSchedulingNode(
			cache.NewNodeForSort("node-"+num, resources.Multiply(res, int64(1+i))),
		)
		list[i] = node
	}
	// nodes should come back in order 2 (300), 1 (200), 0 (100)
	SortNodes(list, policies.FairnessPolicy)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-1 on place 1 in the slice to have no res
	list[1] = newSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 0)),
	)
	// nodes should come back in order 2 (300), 0 (100), 1 (0)
	SortNodes(list, policies.FairnessPolicy)
	assertNodeList(t, list, []int{1, 2, 0})

	// change node-1 on place 2 in the slice to have 300 res
	list[2] = newSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 3)),
	)
	// nodes should come back in order 2 (300), 1 (300), 0 (100)
	SortNodes(list, policies.FairnessPolicy)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-2 on place 0 in the slice to have -300 res
	list[0] = newSchedulingNode(
		cache.NewNodeForSort("node-2", resources.Multiply(res, -3)),
	)
	// nodes should come back in order 1 (300), 0 (100), 2 (-300)
	SortNodes(list, policies.FairnessPolicy)
	assertNodeList(t, list, []int{1, 0, 2})
}

func TestSortAppsNoPending(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	input := make(map[string]*Application, 4)
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
	list := SortApplications(input, policies.FairSortPolicy, nil)
	assertAppListLength(t, list, []string{})
	list = SortApplications(input, policies.FifoSortPolicy, nil)
	assertAppListLength(t, list, []string{})
	list = SortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{})

	// set one app with pending
	appID := "app-1"
	input[appID].pending = res
	list = SortApplications(input, policies.FairSortPolicy, nil)
	assertAppListLength(t, list, []string{appID})
	list = SortApplications(input, policies.FifoSortPolicy, nil)
	assertAppListLength(t, list, []string{appID})
	list = SortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{appID})
}

func TestSortAppsFifo(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending: all apps have pending resources
	input := make(map[string]*Application, 4)
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
	list := SortApplications(input, policies.FifoSortPolicy, nil)
	assertAppList(t, list, []int{0, 1, 2, 3})
}

func TestSortAppsFair(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending: all apps have pending resources
	input := make(map[string]*Application, 4)
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
	list := SortApplications(input, policies.FairSortPolicy, nil)
	assertAppList(t, list, []int{0, 1, 2, 3})

	// apps should come back in order: 0, 1, 2, 3
	list = SortApplications(input, policies.FairSortPolicy, resources.Multiply(res, 0))
	assertAppList(t, list, []int{0, 1, 2, 3})

	// apps should come back in order: 0, 1, 2, 3
	list = SortApplications(input, policies.FairSortPolicy, resources.Multiply(res, 5))
	assertAppList(t, list, []int{0, 1, 2, 3})

	// update allocated resource for app-1
	input["app-1"].allocating = resources.Multiply(res, 10)
	// apps should come back in order: 0, 2, 3, 1
	list = SortApplications(input, policies.FairSortPolicy, resources.Multiply(res, 5))
	assertAppList(t, list, []int{0, 3, 1, 2})

	// update allocated resource for app-3 to negative (move to head of the list)
	input["app-3"].allocating = resources.Multiply(res, -10)
	// apps should come back in order: 3, 0, 2, 1
	list = SortApplications(input, policies.FairSortPolicy, resources.Multiply(res, 5))
	for i := 0; i < 4; i++ {
		log.Logger().Info("allocated res",
			zap.Int("order", i),
			zap.String("name", list[i].ApplicationID),
			zap.Any("res", list[i].allocating))
	}
	assertAppList(t, list, []int{1, 3, 2, 0})
}

func TestSortAppsStateAware(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup all apps with pending resources, all accepted state
	input := make(map[string]*Application, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newSchedulingApplication(
			cache.NewApplicationInfo(appID, "partition", "queue",
				security.UserGroup{}, nil))
		app.allocating = resources.Multiply(res, int64(i+1))
		app.pending = res
		input[appID] = app
		err := app.HandleApplicationEvent(runApplication)
		assert.NilError(t, err, "state change failed for app %v", appID)
		// make sure the time stamps differ at least a bit (tracking in nano seconds)
		time.Sleep(time.Nanosecond * 5)
	}
	// only first app should be returned (all in accepted)
	list := SortApplications(input, policies.StateAwarePolicy, nil)
	appID0 := "app-0"
	assertAppListLength(t, list, []string{appID0})

	// set first app pending to zero, should get 2nd app back
	input[appID0].pending = resources.NewResource()
	list = SortApplications(input, policies.StateAwarePolicy, nil)
	appID1 := "app-1"
	assertAppListLength(t, list, []string{appID1})

	// move the first app to starting no pending resource should get nothing
	err := input[appID0].HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "state change failed for app-0")
	list = SortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{})

	// move first app to running (no pending resource) and 4th app to starting should get starting app
	err = input[appID0].HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "state change failed for app-0")
	appID3 := "app-3"
	err = input[appID3].HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "state change failed for app-3")
	list = SortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{appID3})

	// set pending for first app, should get back 1st and 4th in that order
	input[appID0].pending = res
	list = SortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{appID0, appID3})

	// move 4th to running should get back: 1st, 2nd and 4th in that order
	err = input[appID3].HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "state change failed for app-3")
	list = SortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{appID0, appID1, appID3})
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
	SortAskByPriority(list, true)
	// asks should come back in order: 0, 1, 2, 3
	assertAskList(t, list, []int{0, 1, 2, 3})
	// move things around
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAskList(t, list, []int{2, 3, 0, 1})
	SortAskByPriority(list, false)
	// asks should come back in order: 3, 2, 1, 0
	assertAskList(t, list, []int{3, 2, 1, 0})
	// make asks with same priority
	// ask-3 and ask-1 both with prio 1 do not change order
	// ask-3 must always be earlier in the list
	list[0].priority = 1
	SortAskByPriority(list, true)
	// asks should come back in order: 0, 2, 3, 1
	assertAskList(t, list, []int{0, 1, 3, 2})
	SortAskByPriority(list, false)
	// asks should come back in order: 3, 2, 0, 1
	assertAskList(t, list, []int{3, 1, 0, 2})
}

// list of queues and the location of the named queue inside that list
// place[0] defines the location of the root.q0 in the list of queues
func assertQueueList(t *testing.T, list []*Queue, place []int) {
	assert.Equal(t, "root.q0", list[place[0]].QueuePath)
	assert.Equal(t, "root.q1", list[place[1]].QueuePath)
	assert.Equal(t, "root.q2", list[place[2]].QueuePath)
}

// list of nodes and the location of the named nodes inside that list
// place[0] defines the location of the node-0 in the list of nodes
func assertNodeList(t *testing.T, list []*Node, place []int) {
	assert.Equal(t, "node-0", list[place[0]].NodeID)
	assert.Equal(t, "node-1", list[place[1]].NodeID)
	assert.Equal(t, "node-2", list[place[2]].NodeID)
}

// list of application and the location of the named applications inside that list
// place[0] defines the location of the app-0 in the list of applications
func assertAppList(t *testing.T, list []*Application, place []int) {
	assert.Equal(t, "app-0", list[place[0]].ApplicationID)
	assert.Equal(t, "app-1", list[place[1]].ApplicationID)
	assert.Equal(t, "app-2", list[place[2]].ApplicationID)
	assert.Equal(t, "app-3", list[place[3]].ApplicationID)
}

// list of application and the location of the named applications inside that list
// place[0] defines the location of the app-0 in the list of applications
func assertAskList(t *testing.T, list []*schedulingAllocationAsk, place []int) {
	assert.Equal(t, "ask-0", list[place[0]].AllocationKey)
	assert.Equal(t, "ask-1", list[place[1]].AllocationKey)
	assert.Equal(t, "ask-2", list[place[2]].AllocationKey)
	assert.Equal(t, "ask-3", list[place[3]].AllocationKey)
}

func assertAppListLength(t *testing.T, list []*Application, apps []string) {
	assert.Equal(t, len(apps), len(list), "length of list differs")
	for i, app := range list {
		assert.Equal(t, apps[i], app.ApplicationID)
	}
}
