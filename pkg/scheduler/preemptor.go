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
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
)

// Below structures are intended to be used under single go routine, thus no
// lock needed

type preemptionContext struct {
	partitions map[string]*preemptionPartitionContext
}

type preemptionPartitionContext struct {
	partitionTotalResource *resources.Resource
	root                   *preemptionQueueContext
	leafQueues             map[string]*preemptionQueueContext
}

type preemptionQueueContext struct {
	queuePath       string
	schedulingQueue *objects.Queue

	// all resources-related for preemption decisions.
	resources *queuePreemptCalcResource

	children map[string]*preemptionQueueContext
	parent   *preemptionQueueContext
}

// resources related to preemption.
type queuePreemptCalcResource struct {
	guaranteed              *resources.Resource
	used                    *resources.Resource
	pending                 *resources.Resource
	max                     *resources.Resource
	ideal                   *resources.Resource
	markedPreemptedResource *resources.Resource
	// How much resource can be preempted by other queues.
	preemptable *resources.Resource
}

func (m *queuePreemptCalcResource) initFromSchedulingQueue(queue *objects.Queue) {
	m.guaranteed = queue.GetGuaranteedResource()
	m.used = queue.GetAllocatedResource()
	m.pending = queue.GetPendingResource()
	m.max = queue.GetMaxResource()
}

func newQueuePreemptCalcResource() *queuePreemptCalcResource {
	return &queuePreemptCalcResource{
		ideal:                   resources.NewResource(),
		preemptable:             resources.NewResource(),
		markedPreemptedResource: resources.NewResource(),
	}
}

type PreemptionPolicy interface {
	DoPreemption(scheduler *Scheduler)
}

func getPreemptionPolicies() []PreemptionPolicy {
	preemptionPolicies := make([]PreemptionPolicy, 0)
	preemptionPolicies = append(preemptionPolicies, &DRFPreemptionPolicy{})
	return preemptionPolicies
}

// Visible by tests
func (s *Scheduler) SingleStepPreemption() {
	// Skip if no preemption needed.
	if !s.clusterContext.NeedPreemption() {
		return
	}

	s.resetPreemptionContext()

	// Do preemption for each policies
	for _, policy := range getPreemptionPolicies() {
		policy.DoPreemption(s)
	}
}

// Copy & Reset PreemptionContext
func (s *Scheduler) resetPreemptionContext() {
	// Create a new preemption context
	s.preemptionContext = &preemptionContext{
		partitions: make(map[string]*preemptionPartitionContext),
	}

	// Copy from scheduler
	for partition, partitionContext := range s.clusterContext.GetPartitionMapClone() {
		preemptionPartitionCtx := &preemptionPartitionContext{
			leafQueues: make(map[string]*preemptionQueueContext),
		}
		s.preemptionContext.partitions[partition] = preemptionPartitionCtx
		preemptionPartitionCtx.root = s.recursiveInitPreemptionQueueContext(preemptionPartitionCtx, nil, partitionContext.root)
	}
}

func (s *Scheduler) recursiveInitPreemptionQueueContext(preemptionPartitionCtx *preemptionPartitionContext, parent *preemptionQueueContext,
	queue *objects.Queue) *preemptionQueueContext {
	preemptionQueue := &preemptionQueueContext{
		queuePath:       queue.QueuePath,
		parent:          parent,
		schedulingQueue: queue,
		resources:       newQueuePreemptCalcResource(),
		children:        make(map[string]*preemptionQueueContext),
	}

	if queue.IsLeafQueue() {
		preemptionPartitionCtx.leafQueues[queue.QueuePath] = preemptionQueue
	}

	for childName, child := range queue.GetCopyOfChildren() {
		preemptionQueue.children[childName] = s.recursiveInitPreemptionQueueContext(preemptionPartitionCtx, preemptionQueue, child)
	}

	return preemptionQueue
}
