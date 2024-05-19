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

// verify queue ordering is working
func TestSortQueues(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	var q0, q1, q2, q3 *Queue
	var queues []*Queue
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
	sortQueue(queues, policies.FifoSortPolicy, false)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q0, q1, q2, q3}), "fifo first")

	queues = []*Queue{q0, q1, q2, q3}
	sortQueue(queues, policies.FifoSortPolicy, true)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q3, q0, q1, q2}), "fifo first - priority")

	// fifo - different starting order
	queues = []*Queue{q1, q3, q0, q2}
	sortQueue(queues, policies.FifoSortPolicy, false)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q1, q3, q0, q2}), "fifo second")

	queues = []*Queue{q1, q3, q0, q2}
	sortQueue(queues, policies.FifoSortPolicy, true)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q3, q1, q0, q2}), "fifo second - priority")

	// fairness ratios: q0:300/500=0.6, q1:200/300=0.67, q2:100/200=0.5, q3:100/200=0.5
	queues = []*Queue{q0, q1, q2, q3}
	sortQueue(queues, policies.FairSortPolicy, false)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q3, q2, q0, q1}), "fair first")

	queues = []*Queue{q0, q1, q2, q3}
	sortQueue(queues, policies.FairSortPolicy, true)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q3, q2, q0, q1}), "fair first - priority")

	// fairness ratios: q0:200/500=0.4, q1:300/300=1, q2:100/200=0.5, q3:100/200=0.5
	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	queues = []*Queue{q0, q1, q2, q3}
	sortQueue(queues, policies.FairSortPolicy, false)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q0, q3, q2, q1}), "fair second")
	queues = []*Queue{q0, q1, q2, q3}
	sortQueue(queues, policies.FairSortPolicy, true)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q3, q0, q2, q1}), "fair second - priority")

	// fairness ratios: q0:150/500=0.3, q1:120/300=0.4, q2:100/200=0.5, q3:100/200=0.5
	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 150, "vcore": 150})
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 120, "vcore": 120})
	queues = []*Queue{q0, q1, q2, q3}
	sortQueue(queues, policies.FairSortPolicy, false)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q0, q1, q3, q2}), "fair third")
	queues = []*Queue{q0, q1, q2, q3}
	sortQueue(queues, policies.FairSortPolicy, true)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q3, q0, q1, q2}), "fair third - priority")

	// fairness ratios: q0:400/800=0.5, q1:200/400= 0.5, q2:100/200=0.5, q3:100/200=0.5
	q0.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 800, "vcore": 400})
	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 400, "vcore": 200})
	q1.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 400, "vcore": 300})
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 150})
	queues = []*Queue{q0, q1, q2, q3}
	sortQueue(queues, policies.FairSortPolicy, false)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q3, q0, q1, q2}), "fair - pending resource")
}

// queue guaranteed resource is not set (same as a zero resource)
func TestNoQueueLimits(t *testing.T) {
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
	sortQueue(queues, policies.FairSortPolicy, false)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q3, q2, q1, q0}), "fair no limit first")
	sortQueue(queues, policies.FairSortPolicy, true)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q3, q2, q0, q1}), "fair no limit first - priority")

	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	sortQueue(queues, policies.FairSortPolicy, false)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q3, q2, q0, q1}), "fair no limit second")
	sortQueue(queues, policies.FairSortPolicy, true)
	assert.Equal(t, queueNames(queues), queueNames([]*Queue{q3, q2, q0, q1}), "fair no limit second - priority")
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
		// make sure the time stamps differ at least a bit (tracking in nano seconds)
		time.Sleep(time.Nanosecond * 5)
	}

	// fifo - apps should come back in order created 0, 1, 2, 3
	list = sortApplications(input, policies.FifoSortPolicy, false, nil)
	assertAppList(t, list, []int{0, 1, 2, 3}, "fifo simple")

	input["app-1"].askMaxPriority = 3
	input["app-3"].askMaxPriority = 5
	input["app-2"].SubmissionTime = input["app-3"].SubmissionTime
	input["app-1"].SubmissionTime = input["app-3"].SubmissionTime
	list = sortApplications(input, policies.FifoSortPolicy, false, nil)
	assertAppList(t, list, []int{0, 2, 3, 1}, "fifo first, priority second") // 0, 3, 1, 2
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
	// apps should come back in order: 0, 2, 3, 1
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
		app.SubmissionTime = baseline.Add(-time.Minute * time.Duration(i))
		input[appID] = app
	}

	list = sortApplications(input, policies.FifoSortPolicy, true, nil)
	assertAppList(t, list, []int{3, 2, 1, 0}, "sort by submission time")
}
