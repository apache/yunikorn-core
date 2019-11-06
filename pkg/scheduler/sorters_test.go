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
	"github.com/cloudera/yunikorn-core/pkg/common/resources"
	"github.com/stretchr/testify/assert"
	"testing"
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
		"vcore" : resources.Quantity(300)})

	q1, err := createManagedQueue(root, "q1", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q1.CachedQueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 300, "vcore": 300})
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore" : resources.Quantity(200)})

	q2, err := createManagedQueue(root, "q2", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q2.CachedQueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q2.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(100),
		"vcore" : resources.Quantity(100)})

	// fairness ratios: q0:300/500=0.6, q1:200/300=0.67, q2:100/200=0.5
	queues := []*SchedulingQueue{q0, q1, q2}
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assert.Equal(t, "root.q2", queues[0].Name)
	assert.Equal(t, "root.q0", queues[1].Name)
	assert.Equal(t, "root.q1", queues[2].Name)

	// fairness ratios: q0:200/500=0.4, q1:300/300=1, q2:100/200=0.5
	q0.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assert.Equal(t, "root.q0", queues[0].Name)
	assert.Equal(t, "root.q2", queues[1].Name)
	assert.Equal(t, "root.q1", queues[2].Name)

	// fairness ratios: q0:150/500=0.3, q1:120/300=0.4, q2:100/200=0.5
	q0.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 150, "vcore": 150})
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 120, "vcore": 120})
	q2.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 100, "vcore": 100})
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assert.Equal(t, "root.q0", queues[0].Name)
	assert.Equal(t, "root.q1", queues[1].Name)
	assert.Equal(t, "root.q2", queues[2].Name)
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
		"vcore" : resources.Quantity(300)})

	q1, err := createManagedQueue(root, "q1", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q1.CachedQueueInfo.GuaranteedResource = resources.NewResourceFromMap(
		map[string]resources.Quantity{"memory": 0, "vcore": 0})
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore" : resources.Quantity(200)})

	q2, err := createManagedQueue(root, "q2", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q2.CachedQueueInfo.GuaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 0, "vcore": 0})
	q2.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(100),
		"vcore" : resources.Quantity(100)})

	queues := []*SchedulingQueue{q0, q1, q2}
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assert.Equal(t, "root.q2", queues[0].Name)
	assert.Equal(t, "root.q1", queues[1].Name)
	assert.Equal(t, "root.q0", queues[2].Name)

	q0.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 200, "vcore": 200})
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 300, "vcore": 300})
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assert.Equal(t, "root.q2", queues[0].Name)
	assert.Equal(t, "root.q0", queues[1].Name)
	assert.Equal(t, "root.q1", queues[2].Name)
}

// queue guaranteed resource is not set
func TestQueueGuaranteedResourceNotSet(t *testing.T) {
	root, err := createRootQueue()
	if err != nil {
		t.Fatalf("failed to create basic root queue: %v", err)
	}
	assert.Nil(t, root.CachedQueueInfo.GuaranteedResource)

	q0, err := createManagedQueue(root, "q0", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q0.CachedQueueInfo.GuaranteedResource = nil
	q0.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(300),
		"vcore" : resources.Quantity(300)})
	assert.Nil(t, q0.CachedQueueInfo.GuaranteedResource)

	q1, err := createManagedQueue(root, "q1", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q1.CachedQueueInfo.GuaranteedResource = nil
	q1.ProposingResource = resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(200),
		"vcore" : resources.Quantity(200)})
	assert.Nil(t, q1.CachedQueueInfo.GuaranteedResource)

	// q2 has no quaranteed resource (nil)
	q2, err := createManagedQueue(root, "q2", false)
	if err != nil {
		t.Fatalf("failed to create leaf queue: %v", err)
	}
	q2.CachedQueueInfo.GuaranteedResource = nil
	assert.Nil(t, q2.CachedQueueInfo.GuaranteedResource)

	queues := []*SchedulingQueue{q0, q1, q2}
	SortQueue(queues, FairSortPolicy)
	assert.Equal(t, len(queues), 3)
	assert.Equal(t, "root.q2", queues[0].Name)
	assert.Equal(t, "root.q1", queues[1].Name)
	assert.Equal(t, "root.q0", queues[2].Name)
}
