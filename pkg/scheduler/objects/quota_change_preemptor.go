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
	maxResource         *resources.Resource
	guaranteedResource  *resources.Resource
	allocatedResource   *resources.Resource
	preemptableResource *resources.Resource
	allocations         []*Allocation
}

func NewQuotaChangePreemptor(queue *Queue) *QuotaChangePreemptionContext {
	preemptor := &QuotaChangePreemptionContext{
		queue:               queue,
		maxResource:         queue.CloneMaxResource(),
		guaranteedResource:  queue.GetGuaranteedResource().Clone(),
		allocatedResource:   queue.GetAllocatedResource().Clone(),
		preemptableResource: nil,
		allocations:         make([]*Allocation, 0),
	}

	return preemptor
}

func (qcp *QuotaChangePreemptionContext) CheckPreconditions() bool {
	if !qcp.queue.IsLeafQueue() || !qcp.queue.IsManaged() || qcp.queue.IsQuotaChangePreemptionRunning() {
		return false
	}
	if qcp.maxResource.StrictlyGreaterThanOrEqualsOnlyExisting(qcp.queue.GetAllocatedResource()) {
		return false
	}
	return true
}

func (qcp *QuotaChangePreemptionContext) tryPreemption() {
	// Get Preemptable Resource
	preemptableResource := qcp.getPreemptableResources()

	if !qcp.queue.IsLeafQueue() {
		leafQueues := make(map[*Queue]*resources.Resource)
		getChildQueuesPreemptableResource(qcp.queue, preemptableResource, leafQueues)

		log.Log(log.SchedQuotaChangePreemption).Info("Triggering quota change preemption for parent queue",
			zap.String("parent queue", qcp.queue.GetQueuePath()),
			zap.String("preemptable resource", preemptableResource.String()),
			zap.Any("no. of leaf queues with potential victims", len(leafQueues)),
		)

		for leaf, leafPreemptableResource := range leafQueues {
			leafQueueQCPC := NewQuotaChangePreemptor(leaf)
			log.Log(log.SchedQuotaChangePreemption).Info("Triggering quota change preemption for leaf queue",
				zap.String("leaf queue", leaf.GetQueuePath()),
				zap.String("max resource", leafQueueQCPC.maxResource.String()),
				zap.String("guaranteed resource", leafQueueQCPC.guaranteedResource.String()),
				zap.String("actual allocated resource", leafQueueQCPC.allocatedResource.String()),
				zap.String("preemptable resource distribution", leafPreemptableResource.String()),
			)
			leafQueueQCPC.tryPreemptionInternal(leafPreemptableResource)
		}
	} else {
		qcp.tryPreemptionInternal(preemptableResource)
	}
}

func (qcp *QuotaChangePreemptionContext) tryPreemptionInternal(preemptableResource *resources.Resource) {
	// quota change preemption has started, so mark the flag
	qcp.queue.MarkQuotaChangePreemptionRunning(true)

	qcp.preemptableResource = preemptableResource

	// Filter the allocations
	qcp.allocations = qcp.filterAllocations()

	// Sort the allocations
	qcp.sortAllocations()

	// Preempt the victims
	qcp.preemptVictims()

	// quota change preemption has ended, so mark the flag
	qcp.queue.MarkQuotaChangePreemptionRunning(false)

	// reset settings
	qcp.queue.resetPreemptionSettings()
}

// getChildQueuesPreemptableResource Compute leaf queue's preemptable resource distribution from the parent's preemptable resource.
// Start with immediate children of parent, compute each child distribution from its parent preemptable resource and repeat the same
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
		// Skip child if there is no usage or usage below or equals guaranteed
		if child.GetAllocatedResource().IsEmpty() || child.GetGuaranteedResource().StrictlyGreaterThanOrEqualsOnlyExisting(child.GetAllocatedResource()) {
			continue
		}
		var usedResource *resources.Resource
		if !child.GetGuaranteedResource().IsEmpty() {
			usedResource = resources.SubOnlyExisting(child.GetGuaranteedResource(), child.GetAllocatedResource())
		} else {
			usedResource = child.GetAllocatedResource()
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

// getPreemptableResources Get the preemptable resources for the queue
// Subtracting the usage from the max resource gives the preemptable resources.
// It could contain both positive and negative values. Only negative values are preemptable.
func (qcp *QuotaChangePreemptionContext) getPreemptableResources() *resources.Resource {
	maxRes := qcp.queue.CloneMaxResource()
	used := resources.SubOnlyExisting(qcp.allocatedResource, qcp.queue.GetPreemptingResource())
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
	log.Log(log.SchedQuotaChangePreemption).Info("Filtering allocations",
		zap.String("queue", qcp.queue.GetQueuePath()),
		zap.Int("filtered allocations", len(allocations)),
	)
	return allocations
}

// sortAllocations Sort the allocations running in the queue
func (qcp *QuotaChangePreemptionContext) sortAllocations() {
	if len(qcp.allocations) > 0 {
		SortAllocations(qcp.allocations)
	}
}

// preemptVictims Preempt the victims to enforce the new max resources.
// When both max and guaranteed resources are set and equal, to comply with law of preemption "Ensure usage doesn't go below guaranteed resources",
// preempt victims on best effort basis. So, preempt victims as close as possible to the required resource.
// Otherwise, exceeding above the required resources slightly is acceptable for now.
func (qcp *QuotaChangePreemptionContext) preemptVictims() {
	if len(qcp.allocations) == 0 {
		log.Log(log.SchedQuotaChangePreemption).Warn("BUG: No victims to enforce quota change through preemption",
			zap.String("queue", qcp.queue.GetQueuePath()))
		return
	}
	apps := make(map[*Application][]*Allocation)
	victimsTotalResource := resources.NewResource()
	log.Log(log.SchedQuotaChangePreemption).Info("Found victims for quota change preemption",
		zap.String("queue", qcp.queue.GetQueuePath()),
		zap.Int("total victims", len(qcp.allocations)),
		zap.String("max resources", qcp.maxResource.String()),
		zap.String("guaranteed resources", qcp.guaranteedResource.String()),
		zap.String("allocated resources", qcp.allocatedResource.String()),
		zap.String("preemptable resources", qcp.preemptableResource.String()),
		zap.Bool("isGuaranteedSet", qcp.guaranteedResource.IsEmpty()),
	)
	for _, victim := range qcp.allocations {
		if !qcp.preemptableResource.FitInMaxUndef(victim.GetAllocatedResource()) {
			continue
		}
		application := qcp.queue.GetApplication(victim.applicationID)
		if application == nil {
			log.Log(log.SchedQuotaChangePreemption).Warn("BUG: application not found in queue",
				zap.String("queue", qcp.queue.GetQueuePath()),
				zap.String("application", victim.applicationID))
			continue
		}

		// Keep collecting the victims until preemptable resource reaches and subtract the usage
		if qcp.preemptableResource.StrictlyGreaterThanOnlyExisting(victimsTotalResource) {
			apps[application] = append(apps[application], victim)
			qcp.allocatedResource.SubFrom(victim.GetAllocatedResource())
		}

		// Has usage gone below the guaranteed resources?
		// If yes, revert the recently added victim steps completely and try next victim.
		if !qcp.guaranteedResource.IsEmpty() && qcp.guaranteedResource.StrictlyGreaterThanOnlyExisting(qcp.allocatedResource) {
			victims := apps[application]
			exceptRecentlyAddedVictims := victims[:len(victims)-1]
			apps[application] = exceptRecentlyAddedVictims
			qcp.allocatedResource.AddTo(victim.GetAllocatedResource())
			victimsTotalResource.SubFrom(victim.GetAllocatedResource())
		} else {
			victimsTotalResource.AddTo(victim.GetAllocatedResource())
		}
	}

	for app, victims := range apps {
		if len(victims) > 0 {
			for _, victim := range victims {
				log.Log(log.SchedQuotaChangePreemption).Info("Preempting victims for quota change preemption",
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
