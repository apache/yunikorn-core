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
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type predicateCheckResult struct {
	allocationKey string
	nodeID        string
	success       bool
	index         int
	victims       []*Allocation
}

func (pcr *predicateCheckResult) betterThan(other *predicateCheckResult, allocationsByNode map[string][]*Allocation) bool {
	return pcr.getSolutionScore(allocationsByNode) < other.getSolutionScore(allocationsByNode)
}

func (pcr *predicateCheckResult) getSolutionScore(allocationsByNode map[string][]*Allocation) uint64 {
	if pcr == nil || !pcr.success {
		return scoreUnfit
	}
	allocations, ok := allocationsByNode[pcr.nodeID]
	if !ok {
		return scoreUnfit
	}

	var score uint64 = 0
	if pcr.index < 0 {
		return score
	}
	if pcr.index >= len(allocations) {
		// shouldn't happen
		return scoreUnfit
	}
	for i := 0; i <= pcr.index; i++ {
		allocation := allocations[i]
		if allocation.IsOriginator() {
			score |= scoreOriginator
		}
		if !allocation.IsAllowPreemptSelf() {
			score |= scoreNoPreempt
		}
	}
	score += uint64(pcr.index) + 1 // need to add 1 to differentiate between no preemption and preempt 1 container

	return score
}

func (pcr *predicateCheckResult) isSatisfactory(allocationsByNode map[string][]*Allocation) bool {
	return pcr.getSolutionScore(allocationsByNode) < scoreFitMax
}

func (pcr *predicateCheckResult) populateVictims(victimsByNode map[string][]*Allocation) {
	if pcr == nil {
		return
	}
	pcr.victims = nil
	if !pcr.success {
		return
	}

	// abort if node was not found
	victimList, ok := victimsByNode[pcr.nodeID]
	if !ok {
		log.Log(log.SchedPreemption).Warn("BUG: Unable to find node in victim map", zap.String("nodeID", pcr.nodeID))
		pcr.success = false
		pcr.index = -1
		return
	}

	// abort if index is too large
	if pcr.index >= len(victimList) {
		log.Log(log.SchedPreemption).Warn("BUG: Got invalid index into allocation list",
			zap.String("nodeID", pcr.nodeID),
			zap.Int("index", pcr.index),
			zap.Int("length", len(victimList)))
		pcr.success = false
		pcr.index = -1
		return
	}

	pcr.victims = make([]*Allocation, 0)
	for i := 0; i <= pcr.index; i++ {
		victim := victimList[i]
		pcr.victims = append(pcr.victims, victim)
	}
}

// preemptPredicateCheck performs a single predicate check and reports the resultType on a channel
func preemptPredicateCheck(plugin api.ResourceManagerCallback, ch chan<- *predicateCheckResult, wg *sync.WaitGroup, args *si.PreemptionPredicatesArgs) {
	defer wg.Done()
	result := &predicateCheckResult{
		allocationKey: args.AllocationKey,
		nodeID:        args.NodeID,
		success:       false,
		index:         -1,
	}
	if len(args.PreemptAllocationKeys) == 0 {
		// normal check; there are sufficient resources to run on this node
		if err := plugin.Predicates(&si.PredicatesArgs{
			AllocationKey: args.AllocationKey,
			NodeID:        args.NodeID,
			Allocate:      true,
		}); err == nil {
			result.success = true
			result.index = -1
		} else {
			log.Log(log.SchedPreemption).Debug("Normal predicate check failed",
				zap.String("AllocationKey", args.AllocationKey),
				zap.String("NodeID", args.NodeID),
				zap.Error(err))
		}
	} else if response := plugin.PreemptionPredicates(args); response != nil {
		// preemption check; at least one allocation will need preemption
		result.success = response.GetSuccess()
		if result.success {
			result.index = int(response.GetIndex())
		}
	}
	ch <- result
}

func (p *predicateCheckResult) String() string {
	if p.nodeID == "" {
		return ""
	}
	var result strings.Builder
	result.WriteString(fmt.Sprintf("node: %s, ", p.nodeID))
	result.WriteString(fmt.Sprintf("alloc: %s, ", p.allocationKey))
	result.WriteString(fmt.Sprintf("success: %v, ", p.success))
	result.WriteString(fmt.Sprintf("index: %d", p.index))
	if len(p.victims) > 0 {
		result.WriteString(", victims: [")
		for i, victim := range p.victims {
			if i > 0 {
				result.WriteString(", ")
			}
			result.WriteString(victim.String())
		}
		result.WriteString("]")
	}
	return result.String()
}
