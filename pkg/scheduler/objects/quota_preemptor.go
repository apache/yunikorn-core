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

type QuotaPreemptionContext struct {
	queue               *Queue
	maxResource         *resources.Resource
	guaranteedResource  *resources.Resource
	allocatedResource   *resources.Resource
	preemptingResource  *resources.Resource
	preemptableResource *resources.Resource
	allocations         []*Allocation
}

func NewQuotaPreemptor(queue *Queue) *QuotaPreemptionContext {
	preemptor := &QuotaPreemptionContext{
		queue:               queue,
		maxResource:         queue.cloneMaxResource(),
		guaranteedResource:  queue.GetGuaranteedResource(),
		allocatedResource:   queue.GetAllocatedResource(),
		preemptingResource:  queue.GetPreemptingResource(),
		preemptableResource: nil,
		allocations:         make([]*Allocation, 0),
	}

	return preemptor
}

func (qpc *QuotaPreemptionContext) tryPreemption() {
	// Get Preemptable Resource
	qpc.setPreemptableResources()

	if qpc.queue.IsLeafQueue() {
		qpc.tryPreemptionInternal()
		return
	}
	leafQueues := make(map[*Queue]*resources.Resource)
	getChildQueuesPreemptableResource(qpc.queue, qpc.preemptableResource, leafQueues)

	log.Log(log.SchedQuotaChangePreemption).Info("Triggering quota change preemption for parent queue",
		zap.String("parent queue", qpc.queue.GetQueuePath()),
		zap.Stringer("preemptable resource", qpc.preemptableResource),
		zap.Int("no. of leaf queues with potential victims", len(leafQueues)),
	)

	for leaf, leafPreemptableResource := range leafQueues {
		leafQueueQCPC := NewQuotaPreemptor(leaf)
		leafQueueQCPC.preemptableResource = leafPreemptableResource
		leafQueueQCPC.tryPreemptionInternal()
	}
}

func (qpc *QuotaPreemptionContext) tryPreemptionInternal() {
	log.Log(log.SchedQuotaChangePreemption).Info("Triggering quota change preemption for leaf queue",
		zap.String("leaf queue", qpc.queue.GetQueuePath()),
		zap.Stringer("max resource", qpc.maxResource),
		zap.Stringer("guaranteed resource", qpc.guaranteedResource),
		zap.Stringer("actual allocated resource", qpc.allocatedResource),
		zap.Stringer("preemptable resource distribution", qpc.preemptableResource),
	)
	// quota change preemption has started, so mark the flag
	qpc.queue.setQuotaPreemptionState(true)

	// Filter the allocations
	qpc.filterAllocations()

	// Sort the allocations
	qpc.sortAllocations()

	// Preempt the victims
	qpc.preemptVictims()

	// quota change preemption has ended, so mark the flag
	qpc.queue.setQuotaPreemptionState(false)
}

// getChildQueuesPreemptableResource Compute leaf queue's preemptable resource distribution from the parent's preemptable resource.
// Starts with immediate children of parent, compute each child distribution from its parent preemptable resource and repeat the same
// for all children at all levels until end leaf queues processed recursively.
// In order to achieve a fair distribution of parent's preemptable resource among its children,
// Higher (relatively) the usage is, higher the preemptable resource would be resulted in.
// Usage above guaranteed (if set) is only considered to derive the preemptable resource.
func getChildQueuesPreemptableResource(queue *Queue, parentPreemptableResource *resources.Resource, childQueues map[*Queue]*resources.Resource) {
	children := queue.GetCopyOfChildren()
	if len(children) == 0 {
		return
	}

	// Sum of all children preemptable resources
	totalPreemptableResource := resources.NewResource()

	// Preemptable resource of all children
	childrenPreemptableResource := make(map[*Queue]*resources.Resource)

	// Traverse each child and calculate its own preemptable resource. Preemptable resource is the amount of resources used above than the guaranteed set.
	// In case guaranteed not set, entire used resources is treated as preemptable resource.
	// Total preemptable resource (sum of all children's preemptable resources) would be calculated along the way.
	for _, child := range children {
		allocated := child.GetAllocatedResource()
		guaranteed := child.GetGuaranteedResource()
		// Skip child if there is no usage or usage below or equals guaranteed
		if allocated.IsEmpty() || guaranteed.StrictlyGreaterThanOrEqualsOnlyExisting(allocated) {
			continue
		}
		var usedResource *resources.Resource
		if !guaranteed.IsEmpty() {
			usedResource = resources.SubOnlyExisting(guaranteed, allocated)
		} else {
			usedResource = allocated
		}
		preemptableResource := resources.NewResource()
		for k, v := range usedResource.Resources {
			if v < 0 {
				preemptableResource.Resources[k] = v * -1
			} else {
				preemptableResource.Resources[k] = v
			}
		}
		childrenPreemptableResource[child] = preemptableResource
		totalPreemptableResource.AddTo(preemptableResource)
	}

	// Second pass: Traverse each child and calculate percentage of each resource type based on total preemptable resource.
	// Apply percentage on parent's preemptable resource to derive its individual distribution
	// or share of resources to be preempted.
	for c, pRes := range childrenPreemptableResource {
		childPreemptableResource := resources.NewResource()
		per := resources.GetSharesTypeWise(pRes, totalPreemptableResource)
		for k := range pRes.Resources {
			if _, ok := parentPreemptableResource.Resources[k]; ok {
				value := math.RoundToEven(per[k] * float64(parentPreemptableResource.Resources[k]))
				childPreemptableResource.Resources[k] = resources.Quantity(value)
			}
		}
		if c.IsLeafQueue() {
			childQueues[c] = childPreemptableResource
		} else {
			getChildQueuesPreemptableResource(c, childPreemptableResource, childQueues)
		}
	}
}

// setPreemptableResources Get the preemptable resources for the queue
// Subtracting the usage from the max resource gives the preemptable resources.
// It could contain both positive and negative values. Only negative values are preemptable.
func (qpc *QuotaPreemptionContext) setPreemptableResources() {
	used := resources.SubOnlyExisting(qpc.allocatedResource, qpc.preemptingResource)
	if qpc.maxResource.IsEmpty() || used.IsEmpty() {
		return
	}
	actual := resources.SubOnlyExisting(qpc.maxResource, used)
	preemptableResource := resources.NewResource()
	// Keep only the resource type which needs to be preempted
	for k, v := range actual.Resources {
		if v < 0 {
			preemptableResource.Resources[k] = resources.Quantity(math.Abs(float64(v)))
		}
	}
	if preemptableResource.IsEmpty() {
		return
	}
	qpc.preemptableResource = preemptableResource
}

// filterAllocations Filter the allocations running in the queue suitable for choosing as victims
func (qpc *QuotaPreemptionContext) filterAllocations() {
	if resources.IsZero(qpc.preemptableResource) {
		return
	}
	qpc.allocations = make([]*Allocation, 0, 5)
	apps := qpc.queue.GetCopyOfApps()

	// Traverse allocations running in the queue
	for _, app := range apps {
		appAllocations := app.GetAllAllocations()
		for _, alloc := range appAllocations {
			// at least one of a preemptable resource type should match with a potential victim
			if !qpc.preemptableResource.MatchAny(alloc.GetAllocatedResource()) {
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
			qpc.allocations = append(qpc.allocations, alloc)
		}
	}
	log.Log(log.SchedQuotaChangePreemption).Info("Filtering allocations",
		zap.String("queue", qpc.queue.GetQueuePath()),
		zap.Int("filtered allocations", len(qpc.allocations)),
	)
}

// sortAllocations Sort the allocations running in the queue
func (qpc *QuotaPreemptionContext) sortAllocations() {
	if len(qpc.allocations) > 0 {
		SortAllocations(qpc.allocations)
	}
}

// preemptVictims Preempt the victims to enforce the new max resources.
// When both max and guaranteed resources are set and equal, to comply with law of preemption "Ensure usage doesn't go below guaranteed resources",
// preempt victims on best effort basis. So, preempt victims as close as possible to the required resource.
// Otherwise, exceeding above the required resources slightly is acceptable for now.
func (qpc *QuotaPreemptionContext) preemptVictims() {
	if len(qpc.allocations) == 0 {
		log.Log(log.SchedQuotaChangePreemption).Warn("BUG: No victims to enforce quota change through preemption",
			zap.String("queue", qpc.queue.GetQueuePath()))
		return
	}
	apps := make(map[*Application][]*Allocation)
	victimsTotalResource := resources.NewResource()
	log.Log(log.SchedQuotaChangePreemption).Info("Found victims for quota change preemption",
		zap.String("queue", qpc.queue.GetQueuePath()),
		zap.Int("total victims", len(qpc.allocations)),
		zap.Stringer("max resources", qpc.maxResource),
		zap.Stringer("guaranteed resources", qpc.guaranteedResource),
		zap.Stringer("allocated resources", qpc.allocatedResource),
		zap.Stringer("preemptable resources", qpc.preemptableResource),
		zap.Bool("isGuaranteedSet", qpc.guaranteedResource.IsEmpty()),
	)
	for _, victim := range qpc.allocations {
		victimALloc := victim.GetAllocatedResource()
		if !qpc.preemptableResource.FitInMaxUndef(victimALloc) {
			continue
		}
		application := qpc.queue.GetApplication(victim.applicationID)
		if application == nil {
			log.Log(log.SchedQuotaChangePreemption).Warn("BUG: application not found in queue",
				zap.String("queue", qpc.queue.GetQueuePath()),
				zap.String("application", victim.applicationID))
			continue
		}

		// Keep collecting the victims until preemptable resource reaches and subtract the usage
		if qpc.preemptableResource.StrictlyGreaterThanOnlyExisting(victimsTotalResource) {
			apps[application] = append(apps[application], victim)
			qpc.allocatedResource.SubFrom(victimALloc)
		}

		// Has usage gone below the guaranteed resources?
		// If yes, revert the recently added victim steps completely and try next victim.
		if !qpc.guaranteedResource.IsEmpty() && qpc.guaranteedResource.StrictlyGreaterThanOnlyExisting(qpc.allocatedResource) {
			victims := apps[application]
			exceptRecentlyAddedVictims := victims[:len(victims)-1]
			apps[application] = exceptRecentlyAddedVictims
			qpc.allocatedResource.AddTo(victimALloc)
			victimsTotalResource.SubFrom(victimALloc)
		} else {
			victimsTotalResource.AddTo(victimALloc)
		}
	}

	for app, victims := range apps {
		if len(victims) > 0 {
			for _, victim := range victims {
				err := victim.MarkPreempted()
				if err != nil {
					log.Log(log.SchedRequiredNodePreemption).Warn("allocation is already released, ignoring in quota preemption process",
						zap.String("applicationID", victim.GetApplicationID()),
						zap.String("allocationKey", victim.GetAllocationKey()))
					continue
				}
				log.Log(log.SchedQuotaChangePreemption).Info("Preempting victim for quota change preemption",
					zap.String("queue", qpc.queue.GetQueuePath()),
					zap.String("allocationKey", victim.GetAllocationKey()),
					zap.Stringer("allocatedResources", victim.GetAllocatedResource()),
					zap.String("applicationID", victim.GetApplicationID()),
					zap.String("nodeID", victim.GetNodeID()))
				qpc.queue.IncPreemptingResource(victim.GetAllocatedResource())
				victim.SendPreemptedByQuotaChangeEvent(qpc.queue.GetQueuePath())
			}
			app.notifyRMAllocationReleased(victims, si.TerminationType_PREEMPTED_BY_SCHEDULER,
				"preempting allocations to enforce new max quota for queue : "+qpc.queue.GetQueuePath())
		}
	}
}
