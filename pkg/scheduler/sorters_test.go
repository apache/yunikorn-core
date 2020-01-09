/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

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

package scheduler

import (
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"
	"gotest.tools/assert"

	"github.com/cloudera/yunikorn-core/pkg/cache"
	"github.com/cloudera/yunikorn-core/pkg/common/resources"
	"github.com/cloudera/yunikorn-core/pkg/common/security"
	"github.com/cloudera/yunikorn-core/pkg/log"
)

// verify queue ordering is working
func TestSortQueues(t *testing.T) {
	root, err := createRootQueue()
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}
	root.CachedQueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 1000, "vcore": 1000})

	q0, err := createManagedQueue(root, "q0", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q0.CachedQueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 500, "vcore": 500})
	q0.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(300),
		"vcore":  resources.Quantity(300)})

	q1, err := createManagedQueue(root, "q1", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q1.CachedQueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 300, "vcore": 300})
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore":  resources.Quantity(200)})

	q2, err := createManagedQueue(root, "q2", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q2.CachedQueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q2.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(100),
		"vcore":  resources.Quantity(100)})

	// fairness ratios: q0:300/500=0.6, q1:200/300=0.67, q2:100/200=0.5
	queues := []*SchedulingQueue{q0, q1, q2}
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{1, 2, 0})

	// fairness ratios: q0:200/500=0.4, q1:300/300=1, q2:100/200=0.5
	q0.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{0, 2, 1})

	// fairness ratios: q0:150/500=0.3, q1:120/300=0.4, q2:100/200=0.5
	q0.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 150, "vcore": 150})
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 120, "vcore": 120})
	q2.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100, "vcore": 100})
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{0, 1, 2})
}

// queue guaranteed resource is 0
func TestNoQueueLimits(t *testing.T) {
	root, err := createRootQueue()
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}
	root.CachedQueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 0, "vcore": 0})

	q0, err := createManagedQueue(root, "q0", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q0.CachedQueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 0, "vcore": 0})
	q0.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(300),
		"vcore":  resources.Quantity(300)})

	q1, err := createManagedQueue(root, "q1", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q1.CachedQueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 0, "vcore": 0})
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore":  resources.Quantity(200)})

	q2, err := createManagedQueue(root, "q2", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q2.CachedQueueInfo.GuaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 0, "vcore": 0})
	q2.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(100),
		"vcore":  resources.Quantity(100)})

	queues := []*SchedulingQueue{q0, q1, q2}
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{2, 1, 0})

	q0.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{1, 2, 0})
}

// queue guaranteed resource is not set
func TestQueueGuaranteedResourceNotSet(t *testing.T) {
	root, err := createRootQueue()
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}
	root.CachedQueueInfo.GuaranteedResource = nil

	q0, err := createManagedQueue(root, "q0", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q0.CachedQueueInfo.GuaranteedResource = nil
	q0.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(300),
		"vcore":  resources.Quantity(300)})

	q1, err := createManagedQueue(root, "q1", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q1.CachedQueueInfo.GuaranteedResource = nil
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore":  resources.Quantity(200)})

	// q2 has no guaranteed resource (nil)
	q2, err := createManagedQueue(root, "q2", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q2.CachedQueueInfo.GuaranteedResource = nil

	queues := []*SchedulingQueue{q0, q1, q2}
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{2, 1, 0})
}

func TestSortNodesMin(t *testing.T) {
	// nil or empty list cannot panic
	SortNodes(nil, MinAvailableResources)
	list := make([]*SchedulingNode, 0)
	SortNodes(list, MinAvailableResources)
	list = append(list, NewSchedulingNode(cache.NewNodeForSort("node-nil", nil)))
	SortNodes(list, MinAvailableResources)

	// stable sort is used so equal resources stay were they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})

	// setup to sort ascending
	list = make([]*SchedulingNode, 3)
	for i := 0; i < 3; i++ {
		num := strconv.Itoa(i)
		node := NewSchedulingNode(
			cache.NewNodeForSort("node-"+num, resources.Multiply(res, int64(3-i))),
		)
		list[i] = node
	}
	// nodes should come back in order 2 (100), 1 (200), 0 (300)
	SortNodes(list, MinAvailableResources)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-1 on place 1 in the slice to have no res
	list[1] = NewSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 0)),
	)
	// nodes should come back in order 1 (0), 2 (100), 0 (300)
	SortNodes(list, MinAvailableResources)
	assertNodeList(t, list, []int{2, 0, 1})

	// change node-1 on place 0 in the slice to have 300 res
	list[0] = NewSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 3)),
	)
	// nodes should come back in order 2 (100), 1 (300), 0 (300)
	SortNodes(list, MinAvailableResources)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-0 on place 2 in the slice to have -300 res
	list[2] = NewSchedulingNode(
		cache.NewNodeForSort("node-0", resources.Multiply(res, -3)),
	)
	// nodes should come back in order 0 (-300), 2 (100), 1 (300)
	SortNodes(list, MinAvailableResources)
	assertNodeList(t, list, []int{0, 2, 1})
}

func TestSortNodesMax(t *testing.T) {
	// nil or empty list cannot panic
	SortNodes(nil, MaxAvailableResources)
	list := make([]*SchedulingNode, 0)
	SortNodes(list, MaxAvailableResources)
	list = append(list, NewSchedulingNode(cache.NewNodeForSort("node-nil", nil)))
	SortNodes(list, MaxAvailableResources)

	// stable sort is used so equal resources stay were they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending
	list = make([]*SchedulingNode, 3)
	for i := 0; i < 3; i++ {
		num := strconv.Itoa(i)
		node := NewSchedulingNode(
			cache.NewNodeForSort("node-"+num, resources.Multiply(res, int64(1+i))),
		)
		list[i] = node
	}
	// nodes should come back in order 2 (300), 1 (200), 0 (100)
	SortNodes(list, MaxAvailableResources)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-1 on place 1 in the slice to have no res
	list[1] = NewSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 0)),
	)
	// nodes should come back in order 2 (300), 0 (100), 1 (0)
	SortNodes(list, MaxAvailableResources)
	assertNodeList(t, list, []int{1, 2, 0})

	// change node-1 on place 2 in the slice to have 300 res
	list[2] = NewSchedulingNode(
		cache.NewNodeForSort("node-1", resources.Multiply(res, 3)),
	)
	// nodes should come back in order 2 (300), 1 (300), 0 (100)
	SortNodes(list, MaxAvailableResources)
	assertNodeList(t, list, []int{2, 1, 0})

	// change node-2 on place 0 in the slice to have -300 res
	list[0] = NewSchedulingNode(
		cache.NewNodeForSort("node-2", resources.Multiply(res, -3)),
	)
	// nodes should come back in order 1 (300), 0 (100), 2 (-300)
	SortNodes(list, MaxAvailableResources)
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
		app := NewSchedulingApplication(
			cache.NewApplicationInfo("app-"+num, "partition", "queue",
				security.UserGroup{}, nil))
		app.MayAllocatedResource = resources.Multiply(res, int64(i+1))
		list[i] = app
		// make sure the time stamps differ at least a bit (tracking in nano seconds)
		time.Sleep(time.Nanosecond * 5)
	}
	// fifo sorts on time: move things around first
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAppList(t, list, []int{2, 3, 0, 1})
	// apps should come back in order created 0, 1, 2, 3
	SortApplications(list, FifoSortPolicy, nil)
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
		app := NewSchedulingApplication(
			cache.NewApplicationInfo("app-"+num, "partition", "queue",
				security.UserGroup{}, nil))
		app.MayAllocatedResource = resources.Multiply(res, int64(i+1))
		list[i] = app
	}
	// nil resource: usage based sorting
	// move things around ro see sorting
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAppList(t, list, []int{2, 3, 0, 1})
	// apps should come back in order: 0, 1, 2, 3
	SortApplications(list, FairSortPolicy, nil)
	assertAppList(t, list, []int{0, 1, 2, 3})

	// move things around
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAppList(t, list, []int{2, 3, 0, 1})
	// apps should come back in order: 0, 1, 2, 3
	SortApplications(list, FairSortPolicy, resources.Multiply(res, 0))
	assertAppList(t, list, []int{0, 1, 2, 3})

	// move things around
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAppList(t, list, []int{2, 3, 0, 1})
	// apps should come back in order: 0, 1, 2, 3
	SortApplications(list, FairSortPolicy, resources.Multiply(res, 5))
	assertAppList(t, list, []int{0, 1, 2, 3})

	// update allocated resource for app-1
	list[1].MayAllocatedResource = resources.Multiply(res, 10)
	// apps should come back in order: 0, 2, 3, 1
	SortApplications(list, FairSortPolicy, resources.Multiply(res, 5))
	assertAppList(t, list, []int{0, 3, 1, 2})

	// update allocated resource for app-3 to negative (move to head of the list)
	list[2].MayAllocatedResource = resources.Multiply(res, -10)
	// apps should come back in order: 3, 0, 2, 1
	SortApplications(list, FairSortPolicy, resources.Multiply(res, 5))
	for i := 0; i < 4; i++ {
		log.Logger().Info("allocated res",
			zap.Int("order", i),
			zap.String("name", list[i].ApplicationInfo.ApplicationID),
			zap.Any("res", list[i].MayAllocatedResource))
	}
	assertAppList(t, list, []int{1, 3, 2, 0})
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
