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
	"math"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type QuotaChangePreemptionContext struct {
	queue               *Queue
	preemptableResource *resources.Resource
	allocations         []*Allocation
}

func NewQuotaChangePreemptor(queue *Queue) *QuotaChangePreemptionContext {
	preemptor := &QuotaChangePreemptionContext{
		queue:               queue,
		preemptableResource: nil,
		allocations:         make([]*Allocation, 0),
	}
	return preemptor
}

func (qcp *QuotaChangePreemptionContext) CheckPreconditions() bool {
	if !qcp.queue.IsLeafQueue() || !qcp.queue.IsManaged() || qcp.queue.HasTriggerredQuotaChangePreemption() || qcp.queue.IsQuotaChangePreemptionRunning() {
		return false
	}
	if qcp.queue.GetMaxResource().StrictlyGreaterThanOnlyExisting(qcp.queue.GetAllocatedResource()) {
		return false
	}
	return true
}

func (qcp *QuotaChangePreemptionContext) tryPreemption() {
	// quota change preemption has started, so mark the flag
	qcp.queue.MarkQuotaChangePreemptionRunning()

	// Get Preemptable Resource
	qcp.preemptableResource = qcp.getPreemptableResources()

	// Filter the allocations
	qcp.allocations = qcp.filterAllocations()

	// Sort the allocations
	qcp.sortAllocations()

	// quota change preemption has really evicted victims, so mark the flag
	qcp.queue.MarkTriggerredQuotaChangePreemption()
}

// getPreemptableResources Get the preemptable resources for the queue
// Subtracting the usage from the max resource gives the preemptable resources.
// It could contain both positive and negative values. Only negative values are preemptable.
func (qcp *QuotaChangePreemptionContext) getPreemptableResources() *resources.Resource {
	maxRes := qcp.queue.CloneMaxResource()
	used := resources.SubOnlyExisting(qcp.queue.GetAllocatedResource(), qcp.queue.GetPreemptingResource())
	if maxRes.IsEmpty() || used.IsEmpty() {
		return nil
	}
	actual := resources.SubOnlyExisting(maxRes, used)
	preemptableResource := resources.NewResource()
	// Keep only the resource type which needs to be preempted
	for k, v := range actual.Resources {
		if v < 0 {
			preemptableResource.Resources[k] = resources.Quantity(math.Abs(float64(v)))
		}
	}
	if preemptableResource.IsEmpty() {
		return nil
	}
	return preemptableResource
}

// filterAllocations Filter the allocations running in the queue suitable for choosing as victims
func (qcp *QuotaChangePreemptionContext) filterAllocations() []*Allocation {
	if resources.IsZero(qcp.preemptableResource) {
		return nil
	}
	var allocations []*Allocation
	apps := qcp.queue.GetCopyOfApps()

	// Traverse allocations running in the queue
	for _, app := range apps {
		appAllocations := app.GetAllAllocations()
		for _, alloc := range appAllocations {
			// at least one of a preemptable resource type should match with a potential victim
			if !qcp.preemptableResource.MatchAny(alloc.GetAllocatedResource()) {
				continue
			}

			// skip allocations which require a specific node
			if alloc.GetRequiredNode() != "" {
				continue
			}

			// skip already released allocations
			if alloc.IsReleased() {
				continue
			}

			// skip already preempted allocations
			if alloc.IsPreempted() {
				continue
			}
			allocations = append(allocations, alloc)
		}
	}
	log.Log(log.ShedQuotaChangePreemption).Info("Filtering allocations",
		zap.String("queue", qcp.queue.GetQueuePath()),
		zap.Int("filtered allocations", len(allocations)),
	)
	return allocations
}

func (qcp *QuotaChangePreemptionContext) sortAllocations() {
	if len(qcp.allocations) > 0 {
		SortAllocations(qcp.allocations)
	}
}

func (qcp *QuotaChangePreemptionContext) preemptVictims() {
	if len(qcp.allocations) == 0 {
		return
	}
	log.Log(log.ShedQuotaChangePreemption).Info("Found victims for quota change preemption",
		zap.String("queue", qcp.queue.GetQueuePath()),
		zap.Int("total victims", len(qcp.allocations)))
	apps := make(map[*Application][]*Allocation)
	victimsTotalResource := resources.NewResource()
	selectedVictimsTotalResource := resources.NewResource()
	for _, victim := range qcp.allocations {
		// stop collecting the victims once ask resource requirement met
		if qcp.preemptableResource.StrictlyGreaterThanOnlyExisting(victimsTotalResource) {
			application := qcp.queue.GetApplication(victim.applicationID)
			if _, ok := apps[application]; !ok {
				apps[application] = []*Allocation{}
			}
			apps[application] = append(apps[application], victim)
			selectedVictimsTotalResource.AddTo(victim.GetAllocatedResource())
		}
		victimsTotalResource.AddTo(victim.GetAllocatedResource())
	}

	if qcp.preemptableResource.StrictlyGreaterThanOnlyExisting(victimsTotalResource) ||
		selectedVictimsTotalResource.StrictlyGreaterThanOnlyExisting(qcp.preemptableResource) {
		// either there is a shortfall or exceeding little above than required, so try "best effort" approach later
		return
	}

	for app, victims := range apps {
		if len(victims) > 0 {
			qcp.queue.MarkTriggerredQuotaChangePreemption()
			for _, victim := range victims {
				log.Log(log.ShedQuotaChangePreemption).Info("Preempting victims for quota change preemption",
					zap.String("queue", qcp.queue.GetQueuePath()),
					zap.String("victim allocation key", victim.allocationKey),
					zap.String("victim allocated resources", victim.GetAllocatedResource().String()),
					zap.String("victim application", victim.applicationID),
					zap.String("victim node", victim.GetNodeID()),
				)
				qcp.queue.IncPreemptingResource(victim.GetAllocatedResource())
				victim.MarkPreempted()
				victim.SendPreemptedByQuotaChangeEvent(qcp.queue.GetQueuePath())
			}
			app.notifyRMAllocationReleased(victims, si.TerminationType_PREEMPTED_BY_SCHEDULER,
				"preempting allocations to enforce new max quota for queue : "+qcp.queue.GetQueuePath())
		}
	}
}

// only for testing
func (qcp *QuotaChangePreemptionContext) getVictims() []*Allocation {
	return qcp.allocations
}
