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
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type PreemptionContext struct {
	node        *Node
	requiredAsk *Allocation
	application *Application
	allocations []*Allocation
}

type filteringResult struct {
	totalAllocations            int // total number of allocations
	requiredNodeAllocations     int // number of requiredNode (daemon set) allocations that cannot be preempted
	atLeastOneResNotMatched     int // number of allocations where there's no single resource type that would match
	higherPriorityAllocations   int // number of allocations with higher priority
	alreadyPreemptedAllocations int // number of allocations already preempted
	releasedPhAllocations       int // number of ph allocations released
}

func getRateLimitedReqNodeLog() *log.RateLimitedLogger {
	initReqNodeLogOnce.Do(func() {
		rateLimitedReqNodeLog = log.NewRateLimitedLogger(log.SchedRequiredNodePreemption, time.Minute)
	})
	return rateLimitedReqNodeLog
}

func NewRequiredNodePreemptor(node *Node, requiredAsk *Allocation, application *Application) *PreemptionContext {
	preemptor := &PreemptionContext{
		node:        node,
		requiredAsk: requiredAsk,
		application: application,
		allocations: make([]*Allocation, 0),
	}
	return preemptor
}

func (p *PreemptionContext) tryPreemption() {
	result := p.filterAllocations()
	p.sortAllocations()

	// Are there any victims/asks to preempt?
	victims := p.GetVictims()
	if len(victims) > 0 {
		log.Log(log.SchedRequiredNodePreemption).Info("Found victims for required node preemption",
			zap.String("ds allocation key", p.requiredAsk.GetAllocationKey()),
			zap.String("allocation name", p.requiredAsk.GetAllocationName()),
			zap.Int("no.of victims", len(victims)))
		for _, victim := range victims {
			err := victim.MarkPreempted()
			if err != nil {
				log.Log(log.SchedRequiredNodePreemption).Warn("allocation is already released, so not proceeding further on the daemon set preemption process",
					zap.String("applicationID", p.requiredAsk.GetApplicationID()),
					zap.String("allocationKey", victim.GetAllocationKey()))
				continue
			}
			if victimQueue := p.application.queue.GetQueueByAppID(victim.GetApplicationID()); victimQueue != nil {
				victimQueue.IncPreemptingResource(victim.GetAllocatedResource())
			} else {
				log.Log(log.SchedRequiredNodePreemption).Warn("BUG: Queue not found for daemon set preemption victim",
					zap.String("queue", p.application.queue.Name),
					zap.String("victimApplicationID", victim.GetApplicationID()),
					zap.String("victimAllocationKey", victim.GetAllocationKey()))
			}
			victim.SendPreemptedBySchedulerEvent(p.requiredAsk.GetAllocationKey(), p.requiredAsk.GetApplicationID(), p.application.queuePath)
		}
		p.requiredAsk.MarkTriggeredPreemption()
		p.application.notifyRMAllocationReleased(victims, si.TerminationType_PREEMPTED_BY_SCHEDULER,
			"preempting allocations to free up resources to run daemon set ask: "+p.requiredAsk.GetAllocationKey())
	} else {
		p.requiredAsk.LogAllocationFailure(common.NoVictimForRequiredNode, true)
		p.requiredAsk.SendRequiredNodePreemptionFailedEvent(p.node.NodeID)
		getRateLimitedReqNodeLog().Info("no victim found for required node preemption",
			zap.String("allocation key", p.requiredAsk.GetAllocationKey()),
			zap.String("allocation name", p.requiredAsk.GetAllocationName()),
			zap.String("node", p.node.NodeID),
			zap.Int("total allocations", result.totalAllocations),
			zap.Int("requiredNode allocations", result.requiredNodeAllocations),
			zap.Int("allocations already preempted", result.alreadyPreemptedAllocations),
			zap.Int("higher priority allocations", result.higherPriorityAllocations),
			zap.Int("allocations with non-matching resources", result.atLeastOneResNotMatched))
	}
}

func (p *PreemptionContext) filterAllocations() filteringResult {
	var result filteringResult
	yunikornAllocations := p.node.GetYunikornAllocations()
	result.totalAllocations = len(yunikornAllocations)

	for _, allocation := range yunikornAllocations {
		// skip daemon set pods and higher priority allocation
		if allocation.GetRequiredNode() != "" {
			result.requiredNodeAllocations++
			continue
		}

		if allocation.GetPriority() > p.requiredAsk.GetPriority() {
			result.higherPriorityAllocations++
			continue
		}

		// skip if the allocation is already being preempted
		if allocation.IsPreempted() {
			result.alreadyPreemptedAllocations++
			continue
		}

		// at least one of the required ask resource should match, otherwise skip
		if !p.requiredAsk.GetAllocatedResource().MatchAny(allocation.GetAllocatedResource()) {
			result.atLeastOneResNotMatched++
			continue
		}

		// skip placeholder tasks which are marked released
		if allocation.IsReleased() {
			result.releasedPhAllocations++
			continue
		}

		p.allocations = append(p.allocations, allocation)
	}

	return result
}

// sort based on the following criteria in the specified order:
// 1. By type (regular pods, opted out pods, driver/owner pods),
// 2. By priority (least priority ask placed first),
// 3. By Create time or age of the ask (younger ask placed first),
// 4. By resource (ask with lesser allocated resources placed first)
func (p *PreemptionContext) sortAllocations() {
	SortAllocations(p.allocations)
}

func (p *PreemptionContext) GetVictims() []*Allocation {
	var victims []*Allocation
	var currentResource = resources.NewResource()
	for _, allocation := range p.allocations {
		if !resources.StrictlyGreaterThanOrEquals(currentResource, p.requiredAsk.GetAllocatedResource()) {
			currentResource.AddTo(allocation.GetAllocatedResource())
			victims = append(victims, allocation)
		} else {
			break
		}
	}

	// Did we found the useful set of victims?
	if len(victims) > 0 && resources.StrictlyGreaterThanOrEquals(
		resources.Add(currentResource, p.node.GetAvailableResource()), p.requiredAsk.GetAllocatedResource()) {
		return victims
	}
	return nil
}

// for test only
func (p *PreemptionContext) getAllocations() []*Allocation {
	return p.allocations
}
