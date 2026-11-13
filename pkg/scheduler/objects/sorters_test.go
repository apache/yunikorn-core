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
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
)

// verify queue ordering is working when explicity guarantees are provided
func TestSortQueuesWithGuarantees(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	var q0, q1, q2, q3 *Queue
	var queues, sorted []*Queue
	fairMaxResources := []*resources.Resource{resources.NewResourceFromMap(map[string]resources.Quantity{}),
		resources.NewResourceFromMap(map[string]resources.Quantity{}),
		resources.NewResourceFromMap(map[string]resources.Quantity{}),
		resources.NewResourceFromMap(map[string]resources.Quantity{}),
	}

	q0, err = createManagedQueue(root, "q0", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q0.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500, "vcore": 500})
	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	q0.currentPriority = 1

	q1, err = createManagedQueue(root, "q1", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q1.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.currentPriority = 1

	q2, err = createManagedQueue(root, "q2", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q2.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q2.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100, "vcore": 100})
	q2.currentPriority = 1

	q3, err = createManagedQueue(root, "q3", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q3.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q3.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100, "vcore": 100})
	q3.currentPriority = 3

	// fifo
	queues = []*Queue{q0, q1, q2, q3}

	sorted = sortQueue(fairMaxList(queues, fairMaxResources), policies.FifoSortPolicy, false)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q0, q1, q2, q3}), "fifo first")

	queues = []*Queue{q0, q1, q2, q3}
	sorted = sortQueue(fairMaxList(queues, fairMaxResources), policies.FifoSortPolicy, true)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q3, q0, q1, q2}), "fifo first - priority")

	// fifo - different starting order
	queues = []*Queue{q1, q3, q0, q2}
	sorted = sortQueue(fairMaxList(queues, fairMaxResources), policies.FifoSortPolicy, false)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q1, q3, q0, q2}), "fifo second")

	queues = []*Queue{q1, q3, q0, q2}
	sorted = sortQueue(fairMaxList(queues, fairMaxResources), policies.FifoSortPolicy, true)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q3, q1, q0, q2}), "fifo second - priority")

	// fairness ratios: q0:300/500=0.6, q1:200/300=0.67, q2:100/200=0.5, q3:100/200=0.5
	queues = []*Queue{q0, q1, q2, q3}
	sorted = sortQueue(fairMaxList(queues, fairMaxResources), policies.FairSortPolicy, false)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q3, q2, q0, q1}), "fair first")

	queues = []*Queue{q0, q1, q2, q3}
	sorted = sortQueue(fairMaxList(queues, fairMaxResources), policies.FairSortPolicy, true)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q3, q2, q0, q1}), "fair first - priority")

	// fairness ratios: q0:200/500=0.4, q1:300/300=1, q2:100/200=0.5, q3:100/200=0.5
	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	queues = []*Queue{q0, q1, q2, q3}
	sorted = sortQueue(fairMaxList(queues, fairMaxResources), policies.FairSortPolicy, false)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q0, q3, q2, q1}), "fair second")
	queues = []*Queue{q0, q1, q2, q3}
	sorted = sortQueue(fairMaxList(queues, fairMaxResources), policies.FairSortPolicy, true)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q3, q0, q2, q1}), "fair second - priority")

	// fairness ratios: q0:150/500=0.3, q1:120/300=0.4, q2:100/200=0.5, q3:100/200=0.5
	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 150, "vcore": 150})
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 120, "vcore": 120})
	queues = []*Queue{q0, q1, q2, q3}
	sorted = sortQueue(fairMaxList(queues, fairMaxResources), policies.FairSortPolicy, false)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q0, q1, q3, q2}), "fair third")
	queues = []*Queue{q0, q1, q2, q3}
	sorted = sortQueue(fairMaxList(queues, fairMaxResources), policies.FairSortPolicy, true)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q3, q0, q1, q2}), "fair third - priority")

	// fairness ratios: q0:400/800=0.5, q1:200/400= 0.5, q2:100/200=0.5, q3:100/200=0.5
	q0.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 800, "vcore": 400})
	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 400, "vcore": 200})
	q1.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 400, "vcore": 300})
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 150})
	queues = []*Queue{q0, q1, q2, q3}
	sorted = sortQueue(fairMaxList(queues, fairMaxResources), policies.FairSortPolicy, false)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q3, q0, q1, q2}), "fair - pending resource")
}

// queue guaranteed resource is not set (same as a zero resource)
func TestSortQueuesNoGuarantees(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	var q0, q1, q2, q3 *Queue
	q0, err = createManagedQueue(root, "q0", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	q0.currentPriority = 1

	q1, err = createManagedQueue(root, "q1", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.currentPriority = 0

	q2, err = createManagedQueue(root, "q2", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q2.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100, "vcore": 100})
	q2.currentPriority = 2

	q3, err = createManagedQueue(root, "q3", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q3.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100, "vcore": 100})
	q3.currentPriority = 3

	queues := []*Queue{q0, q1, q2, q3}
	fairMaxResources := []*resources.Resource{resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000, "vcore": 1000}),
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000, "vcore": 1000}),
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000, "vcore": 1000}),
		resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 1000, "vcore": 1000}),
	}
	// the sort reorders the list in place, so re-sorting the same list chains on the result of the
	// previous sort while every queue keeps its own fair max
	list := fairMaxList(queues, fairMaxResources)
	sorted := sortQueue(list, policies.FairSortPolicy, false)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q3, q2, q1, q0}), "fair no gaurantees first")

	sorted = sortQueue(list, policies.FairSortPolicy, true)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q3, q2, q0, q1}), "fair no gaurantees first - priority")

	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})

	sorted = sortQueue(list, policies.FairSortPolicy, false)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q3, q2, q0, q1}), "fair no gaurantees second")

	sorted = sortQueue(list, policies.FairSortPolicy, true)
	assert.Equal(t, queueNames(sorted), queueNames([]*Queue{q3, q2, q0, q1}), "fair no limit second - priority")
}

// sortQueue must compare each queue against its own fair max whatever the input order. Sort a
// fixed list in every order to cover them all.
func TestSortQueuesFairMaxInputOrder(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// no guarantees are set, so the share is the usage over the fair max: 0.5, 0.1 and 0.3
	queues := createQueuesWithFairMax(t, root, []int64{50, 100, 30}, []int64{100, 1000, 100})
	allOrders := [][]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}

	tests := []struct {
		name             string
		considerPriority bool
		orders           [][]int
		expected         string
	}{
		{"fairness and priority", false, allOrders, "q1,q2,q0"},
		{"priority and fairness", true, allOrders, "q1,q2,q0"},
		// a list of two checks the pairing of a single comparison, it does not cover the swap: the
		// fair max of another queue can only be picked up once the sort has swapped
		{"fairness and priority, two queues", false, [][]int{{0, 1}, {1, 0}}, "q1,q0"},
		{"priority and fairness, two queues", true, [][]int{{0, 1}, {1, 0}}, "q1,q0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, order := range tt.orders {
				sorted := sortQueue(inputOrder(queues, order), policies.FairSortPolicy, tt.considerPriority)
				assert.Equal(t, tt.expected, queueNames(sorted), "input order %v", order)
			}
		})
	}
}

// sortQueuesByFairnessAndPriority compares the share first, so queues with the same share fall
// through to the priority check: a different fair max per queue must not change that.
func TestSortQueuesFairMaxEqualShare(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// all shares are 0.5 while the fair max differs: 50/100, 100/200 and 200/400
	queues := createQueuesWithFairMax(t, root, []int64{50, 100, 200}, []int64{100, 200, 400})
	for i, priority := range []int32{1, 3, 2} {
		queues[i].queue.currentPriority = priority
	}
	allOrders := [][]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}

	for _, order := range allOrders {
		sorted := sortQueue(inputOrder(queues, order), policies.FairSortPolicy, false)
		assert.Equal(t, "q1,q2,q0", queueNames(sorted), "input order %v", order)
	}
}

// create a leaf queue per usage value, named q0, q1, ..., paired with the fair max to compare it against
func createQueuesWithFairMax(t *testing.T, root *Queue, usage, limit []int64) []queueFairMax {
	queues := make([]queueFairMax, 0, len(usage))
	for i, used := range usage {
		queue, err := createManagedQueue(root, "q"+strconv.Itoa(i), false, nil)
		assert.NilError(t, err, "failed to create leaf queue")
		queue.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": resources.Quantity(used)})
		queues = append(queues, queueFairMax{
			queue:   queue,
			fairMax: resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": resources.Quantity(limit[i])}),
		})
	}
	return queues
}

// pair each queue with the fair max at the same position in the list
func fairMaxList(queues []*Queue, fairMax []*resources.Resource) []queueFairMax {
	list := make([]queueFairMax, 0, len(queues))
	for i, queue := range queues {
		list = append(list, queueFairMax{queue: queue, fairMax: fairMax[i]})
	}
	return list
}

func inputOrder(queues []queueFairMax, order []int) []queueFairMax {
	list := make([]queueFairMax, 0, len(order))
	for _, idx := range order {
		list = append(list, queues[idx])
	}
	return list
}

func TestSortAppsNoPending(t *testing.T) {
	var list []*Application

	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"vcore": resources.Quantity(100)})
	input := make(map[string]*Application, 4)
	for i := 0; i < 2; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", "queue")
		input[appID] = app
	}

	// no apps with pending resources should come back empty
	list = sortApplications(input, policies.FairSortPolicy, false, nil)
	assertAppListLength(t, list, []string{}, "fair no pending")
	list = sortApplications(input, policies.FairSortPolicy, true, nil)
	assertAppListLength(t, list, []string{}, "fair no pending - priority")

	list = sortApplications(input, policies.FifoSortPolicy, false, nil)
	assertAppListLength(t, list, []string{}, "fifo no pending")
	list = sortApplications(input, policies.FifoSortPolicy, true, nil)
	assertAppListLength(t, list, []string{}, "fifo no pending - priority")

	// set one app with pending
	appID := "app-1"
	input[appID].pending = res
	list = sortApplications(input, policies.FairSortPolicy, false, nil)
	assertAppListLength(t, list, []string{appID}, "fair one pending")
	list = sortApplications(input, policies.FairSortPolicy, true, nil)
	assertAppListLength(t, list, []string{appID}, "fair one pending - priority")

	list = sortApplications(input, policies.FifoSortPolicy, false, nil)
	assertAppListLength(t, list, []string{appID}, "fifo one pending")
	list = sortApplications(input, policies.FifoSortPolicy, true, nil)
	assertAppListLength(t, list, []string{appID}, "fifo one pending - priority")
}

func TestSortAppsFifo(t *testing.T) {
	var list []*Application

	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"vcore": resources.Quantity(100)})
	// setup to sort descending: all apps have pending resources
	input := make(map[string]*Application, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", "queue")
		app.pending = res
		input[appID] = app
		app.submissionTime = time.Unix(int64(i), 0)
	}

	// fifo - apps should come back in order created 0, 1, 2, 3
	list = sortApplications(input, policies.FifoSortPolicy, false, nil)
	assertAppList(t, list, []int{0, 1, 2, 3}, "fifo simple")

	input["app-1"].askMaxPriority = 3
	input["app-3"].askMaxPriority = 5
	input["app-2"].submissionTime = input["app-3"].submissionTime
	input["app-1"].submissionTime = input["app-3"].submissionTime
	list = sortApplications(input, policies.FifoSortPolicy, false, nil)
	/*
	* apps order: 0, 3, 1, 2
	* the resultType of app index is [0, 2, 3, 1]
	* app0 with index 0, app1 with index 2, app2 with index 3 and app3 with index 1
	 */
	assertAppList(t, list, []int{0, 2, 3, 1}, "fifo first, priority second")
}

func TestSortAppsPriorityFifo(t *testing.T) {
	var list []*Application

	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"vcore": resources.Quantity(100)})
	// setup to sort descending: all apps have pending resources
	input := make(map[string]*Application, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", "queue")
		app.pending = res
		input[appID] = app
		// make sure the time stamps differ at least a bit (tracking in nano seconds)
		time.Sleep(time.Nanosecond * 5)
	}

	// set priorities
	input["app-1"].askMaxPriority = 5
	input["app-3"].askMaxPriority = 4

	// priority - apps should come back in order 1, 3, 0, 2
	list = sortApplications(input, policies.FifoSortPolicy, true, nil)
	assertAppList(t, list, []int{2, 0, 3, 1}, "fifo simple")
}

func TestSortAppsFair(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"vcore": resources.Quantity(100)})
	// setup to sort descending: all apps have pending resources
	input := make(map[string]*Application, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", "queue")
		app.allocatedResource = resources.Multiply(res, int64(i+1))
		app.pending = res
		input[appID] = app
	}
	// nil resource: usage based sorting
	// apps should come back in order: 0, 1, 2, 3
	list := sortApplications(input, policies.FairSortPolicy, false, nil)
	assertAppList(t, list, []int{0, 1, 2, 3}, "nil total")

	// apps should come back in order: 0, 1, 2, 3
	list = sortApplications(input, policies.FairSortPolicy, false, resources.Multiply(res, 0))
	assertAppList(t, list, []int{0, 1, 2, 3}, "zero total")

	// apps should come back in order: 0, 1, 2, 3
	list = sortApplications(input, policies.FairSortPolicy, false, resources.Multiply(res, 5))
	assertAppList(t, list, []int{0, 1, 2, 3}, "no alloc, set total")

	// update allocated resource for app-1
	input["app-1"].allocatedResource = resources.Multiply(res, 10)
	// apps should come back in order: 0, 2, 3, 1
	list = sortApplications(input, policies.FairSortPolicy, false, resources.Multiply(res, 5))
	assertAppList(t, list, []int{0, 3, 1, 2}, "app-1 allocated")

	// update allocated resource for app-3 to negative (move to head of the list)
	input["app-3"].allocatedResource = resources.Multiply(res, -10)
	// apps should come back in order: 3, 0, 2, 1
	list = sortApplications(input, policies.FairSortPolicy, false, resources.Multiply(res, 5))
	assertAppList(t, list, []int{1, 3, 2, 0}, "app-1 & app-3 allocated")

	// update allocated resource for app-3 & app-1 where priority of app-3 is higher
	input["app-1"].allocatedResource = resources.Multiply(res, 10)
	input["app-1"].askMaxPriority = 2
	input["app-3"].allocatedResource = resources.Multiply(res, 10)
	input["app-3"].askMaxPriority = 3
	list = sortApplications(input, policies.FairSortPolicy, false, resources.Multiply(res, 5))
	/*
	*  expected apps order: 0, 2, 3, 1 means
	*  So resultType of apps indexs is [0, 3, 1, 2]
	*  app0 in 0, app1 in 3, app2 in 1, app3 in 2:
	 */
	assertAppList(t, list, []int{0, 3, 1, 2}, "app-1 & app-3 allocated, app-3 high priority")
}

func TestSortAppsPriorityFair(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"vcore": resources.Quantity(100)})
	// setup to sort descending: all apps have pending resources
	input := make(map[string]*Application, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", "queue")
		app.allocatedResource = resources.Multiply(res, int64(i+1))
		app.pending = res
		input[appID] = app
	}

	// set priorities
	input["app-1"].askMaxPriority = 5

	// nil resource: priority then usage based sorting
	// apps should come back in order: 1, 0, 2, 3
	list := sortApplications(input, policies.FairSortPolicy, true, nil)
	assertAppList(t, list, []int{1, 0, 2, 3}, "nil total")

	// apps should come back in order: 1, 0, 2, 3
	list = sortApplications(input, policies.FairSortPolicy, true, resources.Multiply(res, 0))
	assertAppList(t, list, []int{1, 0, 2, 3}, "zero total")

	// apps should come back in order: 1, 0, 2, 3
	list = sortApplications(input, policies.FairSortPolicy, true, resources.Multiply(res, 5))
	assertAppList(t, list, []int{1, 0, 2, 3}, "no alloc, set total")

	// update allocated resource for app-2
	input["app-2"].allocatedResource = resources.Multiply(res, 10)
	// apps should come back in order: 1, 0, 3, 2
	list = sortApplications(input, policies.FairSortPolicy, true, resources.Multiply(res, 5))
	assertAppList(t, list, []int{1, 0, 3, 2}, "app-1 allocated")

	// update allocated resource for app-3 to negative (move to head of the list within priority 0)
	input["app-3"].allocatedResource = resources.Multiply(res, -10)
	// apps should come back in order: 1, 3, 0, 2
	list = sortApplications(input, policies.FairSortPolicy, true, resources.Multiply(res, 5))
	assertAppList(t, list, []int{2, 0, 3, 1}, "app-1 & app-3 allocated")
}

// sortApplications builds the list from a map, so the input order of the sort changes per run.
// Sort a fixed list in both orders to make sure the first check of each sorter runs on both sides.
func TestSortAppsPriorityInputOrder(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"vcore": resources.Quantity(100)})
	baseline := time.Now()
	// app-high has the higher priority, but more usage and a later submission time than app-low
	high := newApplication("app-high", "partition", "queue")
	high.askMaxPriority = 5
	high.allocatedResource = resources.Multiply(res, 10)
	high.submissionTime = baseline
	low := newApplication("app-low", "partition", "queue")
	low.allocatedResource = res
	low.submissionTime = baseline.Add(-time.Minute)

	tests := []struct {
		name     string
		sorter   func([]*Application)
		expected []string
	}{
		{"priority and fairness", func(apps []*Application) { sortApplicationsByPriorityAndFairness(apps, nil) }, []string{"app-high", "app-low"}},
		{"priority and submission time", sortApplicationsByPriorityAndSubmissionTime, []string{"app-high", "app-low"}},
		{"submission time and priority", sortApplicationsBySubmissionTimeAndPriority, []string{"app-low", "app-high"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, list := range [][]*Application{{high, low}, {low, high}} {
				tt.sorter(list)
				assertAppListLength(t, list, tt.expected, tt.name)
			}
		})
	}
}

func queueNames(list []*Queue) string {
	result := make([]string, 0)
	for _, v := range list {
		result = append(result, v.Name)
	}
	return strings.Join(result, ",")
}

// list of application and the location of the named applications inside that list
// place[0] defines the location of the app-0 in the list of applications
func assertAppList(t *testing.T, list []*Application, place []int, name string) {
	assert.Equal(t, "app-0", list[place[0]].ApplicationID, "test name: %s", name)
	assert.Equal(t, "app-1", list[place[1]].ApplicationID, "test name: %s", name)
	assert.Equal(t, "app-2", list[place[2]].ApplicationID, "test name: %s", name)
	assert.Equal(t, "app-3", list[place[3]].ApplicationID, "test name: %s", name)
}

func assertAppListLength(t *testing.T, list []*Application, apps []string, name string) {
	assert.Equal(t, len(apps), len(list), "length of list differs, test: %s", name)
	for i, app := range list {
		//nolint:gosec //safe to ignore as we are asserting apps and list have same length before
		assert.Equal(t, apps[i], app.ApplicationID, "test name: %s", name)
	}
}

func TestSortBySubmissionTime(t *testing.T) {
	var list []*Application

	// stable sort is used so equal values stay where they were
	// let all resource have same resource but submissionTime is different
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": resources.Quantity(100)})
	baseline := time.Now()
	// setup to sort descending: all apps have pending resource
	input := make(map[string]*Application, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", "queue")
		app.pending = res
		input[appID] = app
		app.submissionTime = baseline.Add(-time.Minute * time.Duration(i))
		input[appID] = app
	}

	list = sortApplications(input, policies.FifoSortPolicy, true, nil)
	assertAppList(t, list, []int{3, 2, 1, 0}, "sort by submission time")
}
