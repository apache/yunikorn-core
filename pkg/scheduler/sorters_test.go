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

package scheduler

import (
	"github.com/cloudera/yunikorn-core/pkg/cache"
	"github.com/cloudera/yunikorn-core/pkg/common/resources"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSortQueues(t *testing.T) {
	// define queue info without guaranteed resources
	root   := newQueueInfo("root", nil, 1000, 1000, 1000, 1000)
	q0info := newQueueInfo("q0", root, 0, 0, 500, 500)
	q1info := newQueueInfo("q1", root, 0, 0, 200, 200)
	q2info := newQueueInfo("q2", root, 0, 0, 300, 300)
	qRoot  := newSchedulingQueue("root", root, nil, 300, 200)
	q0     := newSchedulingQueue("q0", q0info, qRoot,300, 300)
	q1     := newSchedulingQueue("q1", q1info, qRoot,200, 200)
	q2     := newSchedulingQueue("q2", q2info, qRoot, 100, 100)

	queues := []*SchedulingQueue{q0, q1, q2}
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assert.Equal(t, queues[0].Name, "q2")
	assert.Equal(t, queues[1].Name, "q1")
	assert.Equal(t, queues[2].Name, "q0")

	q0.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assert.Equal(t, queues[0].Name, "q2")
	assert.Equal(t, queues[1].Name, "q0")
	assert.Equal(t, queues[2].Name, "q1")
}

func TestNoLimitQueues(t *testing.T) {
	// define queue info without guaranteed resources
	root   := newQueueInfo("root", nil, 0, 0, 0, 0)
	q0info := newQueueInfo("q0", root, 0, 0, 0, 0)
	q1info := newQueueInfo("q1", root, 0, 0, 0, 0)
	q2info := newQueueInfo("q2", root, 0, 0, 0, 0)
	qRoot  := newSchedulingQueue("root", root, nil, 300, 200)
	q0     := newSchedulingQueue("q0", q0info, qRoot,100, 100)
	q1     := newSchedulingQueue("q1", q1info, qRoot,200, 100)
	q2     := newSchedulingQueue("q2", q2info, qRoot, 0, 0)

	queues := []*SchedulingQueue{q0, q1, q2}
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assert.Equal(t, queues[0].Name, "q2")
	assert.Equal(t, queues[1].Name, "q0")
	assert.Equal(t, queues[2].Name, "q1")


	q0.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 100})
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100, "vcore": 100})
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assert.Equal(t, queues[0].Name, "q2")
	assert.Equal(t, queues[1].Name, "q1")
	assert.Equal(t, queues[2].Name, "q0")
}


func newQueueInfo(name string, parent *cache.QueueInfo,
	memoryMax, vcoreMax, memoryGuaranteed, vcoreGuaranteed int64) *cache.QueueInfo {
	return &cache.QueueInfo{
		Name:               name,
		MaxResource:        resources.NewResourceFromMap(map[string]resources.Quantity{
			"memory": resources.Quantity(memoryMax),
			"vcore" : resources.Quantity(vcoreMax)}),
		GuaranteedResource: resources.NewResourceFromMap(map[string]resources.Quantity{
			"memory": resources.Quantity(memoryGuaranteed),
			"vcore" : resources.Quantity(vcoreGuaranteed)}),
		Parent:             parent,
		Properties:         nil,
	}
}

func newSchedulingQueue(name string, queueInfo *cache.QueueInfo, parent *SchedulingQueue,
	memoryProposed, vcoreProposed int64) *SchedulingQueue {
	return &SchedulingQueue{
		Name:                name,
		CachedQueueInfo:     queueInfo,
		ProposingResource:   resources.NewResourceFromMap(map[string]resources.Quantity{
			"memory": resources.Quantity(memoryProposed),
			"vcore" : resources.Quantity(vcoreProposed)}),
		PartitionResource:   nil,
		ApplicationSortType: 0,
		QueueSortType:       FairSortPolicy,
		childrenQueues:      nil,
		applications:        nil,
		parent:              parent,
		allocatingResource:  nil,
		pendingResource:     nil,
	}
}
