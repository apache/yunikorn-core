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

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

// verify queue ordering is working
func TestSortQueues(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	var q0, q1, q2 *Queue
	q0, err = createManagedQueue(root, "q0", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q0.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 500, "vcore": 500})
	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})

	q1, err = createManagedQueue(root, "q1", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q1.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})

	q2, err = createManagedQueue(root, "q2", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q2.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q2.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100, "vcore": 100})

	// fairness ratios: q0:300/500=0.6, q1:200/300=0.67, q2:100/200=0.5
	queues := []*Queue{q0, q1, q2}
	sortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{1, 2, 0}, "fair first")

	// fairness ratios: q0:200/500=0.4, q1:300/300=1, q2:100/200=0.5
	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	sortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{0, 2, 1}, "fair second")

	// fairness ratios: q0:150/500=0.3, q1:120/300=0.4, q2:100/200=0.5
	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 150, "vcore": 150})
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 120, "vcore": 120})
	sortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{0, 1, 2}, "fair third")
}

// queue guaranteed resource is not set (same as a zero resource)
func TestNoQueueLimits(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	var q0, q1, q2 *Queue
	q0, err = createManagedQueue(root, "q0", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})

	q1, err = createManagedQueue(root, "q1", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})

	q2, err = createManagedQueue(root, "q2", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	q2.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100, "vcore": 100})

	queues := []*Queue{q0, q1, q2}
	sortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{2, 1, 0}, "fair no limit first")

	q0.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.allocatedResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	sortQueue(queues, policies.FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assertQueueList(t, queues, []int{1, 2, 0}, "fair no limit second")
}

func TestSortNodesBin(t *testing.T) {
	// nil or empty list cannot panic
	SortNodes(nil, policies.BinPackingPolicy)
	list := make([]*Node, 0)
	SortNodes(list, policies.BinPackingPolicy)
	list = append(list, newNode("node-nil", nil))
	SortNodes(list, policies.BinPackingPolicy)

	// stable sort is used so equal resources stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})

	// setup to sort ascending
	list = make([]*Node, 3)
	for i := 0; i < 3; i++ {
		num := strconv.Itoa(i)
		node := newNodeRes("node-"+num, resources.Multiply(res, int64(3-i)))
		list[i] = node
	}
	// nodes should come back in order 2 (100), 1 (200), 0 (300)
	SortNodes(list, policies.BinPackingPolicy)
	assertNodeList(t, list, []int{2, 1, 0}, "bin base order")

	// change node-1 on place 1 in the slice to have no res
	list[1] = newNodeRes("node-1", resources.Multiply(res, 0))
	// nodes should come back in order 1 (0), 2 (100), 0 (300)
	SortNodes(list, policies.BinPackingPolicy)
	assertNodeList(t, list, []int{2, 0, 1}, "bin no res node-1")

	// change node-1 on place 0 in the slice to have 300 res
	list[0] = newNodeRes("node-1", resources.Multiply(res, 3))
	// nodes should come back in order 2 (100), 1 (300), 0 (300)
	SortNodes(list, policies.BinPackingPolicy)
	assertNodeList(t, list, []int{2, 1, 0}, "bin node-1 same as node-0")

	// change node-0 on place 2 in the slice to have -300 res
	list[2] = newNodeRes("node-0", resources.Multiply(res, -3))
	// nodes should come back in order 0 (-300), 2 (100), 1 (300)
	SortNodes(list, policies.BinPackingPolicy)
	assertNodeList(t, list, []int{0, 2, 1}, "bin node-0 negative")
}

func TestSortNodesFair(t *testing.T) {
	// nil or empty list cannot panic
	SortNodes(nil, policies.FairnessPolicy)
	list := make([]*Node, 0)
	SortNodes(list, policies.FairnessPolicy)
	list = append(list, newNode("node-nil", nil))
	SortNodes(list, policies.FairnessPolicy)

	// stable sort is used so equal resources stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending
	list = make([]*Node, 3)
	for i := 0; i < 3; i++ {
		num := strconv.Itoa(i)
		node := newNodeRes("node-"+num, resources.Multiply(res, int64(1+i)))
		list[i] = node
	}
	// nodes should come back in order 2 (300), 1 (200), 0 (100)
	SortNodes(list, policies.FairnessPolicy)
	assertNodeList(t, list, []int{2, 1, 0}, "fair base order")

	// change node-1 on place 1 in the slice to have no res
	list[1] = newNodeRes("node-1", resources.Multiply(res, 0))
	// nodes should come back in order 2 (300), 0 (100), 1 (0)
	SortNodes(list, policies.FairnessPolicy)
	assertNodeList(t, list, []int{1, 2, 0}, "fair no res node-1")

	// change node-1 on place 2 in the slice to have 300 res
	list[2] = newNodeRes("node-1", resources.Multiply(res, 3))
	// nodes should come back in order 2 (300), 1 (300), 0 (100)
	SortNodes(list, policies.FairnessPolicy)
	assertNodeList(t, list, []int{2, 1, 0}, "fair node-1 same as node-2")

	// change node-2 on place 0 in the slice to have -300 res
	list[0] = newNodeRes("node-2", resources.Multiply(res, -3))
	// nodes should come back in order 1 (300), 0 (100), 2 (-300)
	SortNodes(list, policies.FairnessPolicy)
	assertNodeList(t, list, []int{1, 0, 2}, "fair node-2 negative")
}

// list of queues and the location of the named queue inside that list
// place[0] defines the location of the root.q0 in the list of queues
func assertQueueList(t *testing.T, list []*Queue, place []int, name string) {
	assert.Equal(t, "root.q0", list[place[0]].QueuePath, "test name: %s", name)
	assert.Equal(t, "root.q1", list[place[1]].QueuePath, "test name: %s", name)
	assert.Equal(t, "root.q2", list[place[2]].QueuePath, "test name: %s", name)
}

// list of nodes and the location of the named nodes inside that list
// place[0] defines the location of the node-0 in the list of nodes
func assertNodeList(t *testing.T, list []*Node, place []int, name string) {
	assert.Equal(t, "node-0", list[place[0]].NodeID, "test name: %s", name)
	assert.Equal(t, "node-1", list[place[1]].NodeID, "test name: %s", name)
	assert.Equal(t, "node-2", list[place[2]].NodeID, "test name: %s", name)
}
