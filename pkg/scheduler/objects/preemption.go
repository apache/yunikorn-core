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
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

var (
	preemptAttemptFrequency        = 15 * time.Second
	preemptCheckConcurrency        = 10
	scoreFitMax             uint64 = 1 << 32
	scoreOriginator         uint64 = 1 << 33
	scoreNoPreempt          uint64 = 1 << 34
	scoreUnfit              uint64 = 1 << 35
)

// Preemptor encapsulates the functionality required for preemption victim selection
type Preemptor struct {
	application     *Application        // application containing ask
	queue           *Queue              // queue to preempt for
	queuePath       string              // path of queue to preempt for
	headRoom        *resources.Resource // current queue headroom
	preemptionDelay time.Duration       // preemption delay
	ask             *AllocationAsk      // ask to be preempted for
	iterator        NodeIterator        // iterator to enumerate all nodes
	nodesTried      bool                // flag indicating that scheduling has already been tried on all nodes

	// lazily-populated work structures
	allocationsByQueue map[string]*QueuePreemptionSnapshot // map of queue snapshots by queue path
	queueByAlloc       map[string]*QueuePreemptionSnapshot // map of queue snapshots by allocationID
	allocationsByNode  map[string][]*Allocation            // map of allocation by nodeID
	nodeAvailableMap   map[string]*resources.Resource      // map of available resources by nodeID
}

// QueuePreemptionSnapshot is used to track a snapshot of a queue for preemption
type QueuePreemptionSnapshot struct {
	Parent             *QueuePreemptionSnapshot // snapshot of parent queue
	QueuePath          string                   // fully qualified path to queue
	AllocatedResource  *resources.Resource      // allocated resources
	PreemptingResource *resources.Resource      // resources currently flagged for preemption
	MaxResource        *resources.Resource      // maximum resources for this queue
	GuaranteedResource *resources.Resource      // guaranteed resources for this queue
	PotentialVictims   []*Allocation            // list of allocations which could be preempted
}

// NewPreemptor creates a new preemptor. The preemptor itself is not thread safe, and assumes the application lock is held.
func NewPreemptor(application *Application, headRoom *resources.Resource, preemptionDelay time.Duration, ask *AllocationAsk, iterator NodeIterator, nodesTried bool) *Preemptor {
	return &Preemptor{
		application:     application,
		queue:           application.queue,
		queuePath:       application.queuePath,
		headRoom:        headRoom,
		preemptionDelay: preemptionDelay,
		ask:             ask,
		iterator:        iterator,
		nodesTried:      nodesTried,
	}
}

// CheckPreconditions performs simple sanity checks designed to determine if preemption should be attempted
// for an ask. If checks succeed, updates the ask preemption check time.
func (p *Preemptor) CheckPreconditions() bool {
	now := time.Now()

	// skip if ask is not allowed to preempt other tasks
	if !p.ask.IsAllowPreemptOther() {
		return false
	}

	// skip if ask has previously triggered preemption
	if p.ask.HasTriggeredPreemption() {
		return false
	}

	// skip if ask requires a specific node (this should be handled by required node preemption algorithm)
	if p.ask.GetRequiredNode() != "" {
		return false
	}

	// skip if preemption delay has not yet passed
	if now.Before(p.ask.GetCreateTime().Add(p.preemptionDelay)) {
		return false
	}

	// skip if attempt frequency hasn't been reached again
	if now.Before(p.ask.GetPreemptCheckTime().Add(preemptAttemptFrequency)) {
		return false
	}

	// mark this ask as having been checked recently to avoid doing extra work in the next scheduling cycle
	p.ask.UpdatePreemptCheckTime()

	return true
}

// initQueueSnapshots ensures that snapshots have been taken of the queue
func (p *Preemptor) initQueueSnapshots() {
	if p.allocationsByQueue != nil {
		return
	}

	p.allocationsByQueue = p.queue.FindEligiblePreemptionVictims(p.queuePath, p.ask)
}

// initWorkingState builds helper data structures required to compute a solution
func (p *Preemptor) initWorkingState() {
	// return if we have already run
	if p.nodeAvailableMap != nil {
		return
	}

	// ensure queue snapshots are populated
	p.initQueueSnapshots()

	allocationsByNode := make(map[string][]*Allocation)
	queueByAlloc := make(map[string]*QueuePreemptionSnapshot)
	nodeAvailableMap := make(map[string]*resources.Resource)

	// build a map from NodeID to allocation and from allocationID to queue capacities
	for _, victims := range p.allocationsByQueue {
		for _, allocation := range victims.PotentialVictims {
			nodeID := allocation.GetNodeID()
			allocations, ok := allocationsByNode[nodeID]
			if !ok {
				allocations = make([]*Allocation, 0)
			}
			allocationsByNode[nodeID] = append(allocations, allocation)
			queueByAlloc[allocation.GetAllocationKey()] = victims
		}
	}

	// walk node iterator and track available resources per node
	for p.iterator.HasNext() {
		node := p.iterator.Next()
		if !node.IsSchedulable() || (node.IsReserved() && !node.isReservedForApp(reservationKey(nil, p.application, p.ask))) {
			// node is not available, remove any potential victims from consideration
			delete(allocationsByNode, node.NodeID)
		} else {
			// track allocated and available resources
			nodeAvailableMap[node.NodeID] = node.GetAvailableResource()
		}
	}

	// sort the allocations on each node in the order we'd like to try them
	sortVictimsForPreemption(allocationsByNode)

	p.allocationsByNode = allocationsByNode
	p.queueByAlloc = queueByAlloc
	p.nodeAvailableMap = nodeAvailableMap
}

// checkPreemptionQueueGuarantees verifies that it's possible to free enough resources to fit the given ask
func (p *Preemptor) checkPreemptionQueueGuarantees() bool {
	p.initQueueSnapshots()

	queues := p.duplicateQueueSnapshots()
	currentQueue, ok := queues[p.queuePath]
	if !ok {
		log.Logger().Warn("BUG: Didn't find current queue in snapshot list",
			zap.String("queuePath", p.queuePath))
		return false
	}

	currentQueue.AddAllocation(p.ask.GetAllocatedResource())

	// remove each allocation in turn, validating that at some point we free enough resources to allow this ask to fit
	for _, snapshot := range queues {
		for _, alloc := range snapshot.PotentialVictims {
			snapshot.RemoveAllocation(alloc.GetAllocatedResource())
			if currentQueue.IsWithinGuaranteedResource() {
				return true
			}
		}
	}

	return false
}

// calculateVictimsByNode takes a list of potential victims for a node and builds a list ready for the RM to process.
// Result is a list of allocations and the starting index to check for the initial preemption list.
// If the result is nil, the node should not be considered for preemption.
func (p *Preemptor) calculateVictimsByNode(nodeAvailable *resources.Resource, potentialVictims []*Allocation) (int, []*Allocation) {
	nodeCurrentAvailable := nodeAvailable.Clone()
	allocationsByQueueSnap := p.duplicateQueueSnapshots()

	// Initial check: Will allocation fit on node without preemption? This is possible if preemption was triggered due
	// to queue limits and not node resource limits.
	if resources.FitIn(nodeCurrentAvailable, p.ask.GetAllocatedResource()) {
		// return empty list so this node is considered for preemption
		return -1, make([]*Allocation, 0)
	}

	// get the current queue snapshot
	askQueue, ok := allocationsByQueueSnap[p.queuePath]
	if !ok {
		log.Logger().Warn("BUG: Queue not found by name", zap.String("queuePath", p.queuePath))
		return -1, nil
	}

	// speculatively add the current ask
	askQueue.AddAllocation(p.ask.GetAllocatedResource())

	// First pass: Check each task to see whether we are able to reduce our shortfall by preempting each
	// task in turn, and filter out tasks which will cause their queue to drop below guaranteed capacity.
	// If a task could be preempted without violating queue constraints, add it to either the 'head' list or the
	// 'tail' list depending on whether the shortfall is reduced. If added to the 'head' list, adjust the node available
	// capacity and the queue guaranteed headroom.
	head := make([]*Allocation, 0)
	tail := make([]*Allocation, 0)
	for _, victim := range potentialVictims {
		// check to see if removing this task will keep queue above guaranteed amount; if not, skip to the next one
		if qv, ok := p.queueByAlloc[victim.GetAllocationKey()]; ok {
			if queueSnapshot, ok2 := allocationsByQueueSnap[qv.QueuePath]; ok2 {
				queueSnapshot.RemoveAllocation(victim.GetAllocatedResource())
				// did removing this allocation still keep the queue over-allocated?
				if queueSnapshot.IsAtOrAboveGuaranteedResource() {
					// check to see if the shortfall on the node has changed
					shortfall := resources.SubEliminateNegative(p.ask.GetAllocatedResource(), nodeCurrentAvailable)
					newAvailable := resources.Add(nodeCurrentAvailable, victim.GetAllocatedResource())
					newShortfall := resources.SubEliminateNegative(p.ask.GetAllocatedResource(), newAvailable)
					if resources.EqualsOrEmpty(shortfall, newShortfall) {
						// shortfall did not change, so task should only be considered as a last resort
						queueSnapshot.AddAllocation(victim.GetAllocatedResource())
						tail = append(tail, victim)
					} else {
						// shortfall was decreased, so we should keep this task on the main list and adjust usage
						nodeCurrentAvailable.AddTo(victim.GetAllocatedResource())
						head = append(head, victim)
					}
				} else {
					// removing this allocation would have reduced queue below guaranteed limits, put it back
					queueSnapshot.AddAllocation(victim.GetAllocatedResource())
				}
			}
		}
	}
	// merge lists
	head = append(head, tail...)
	if len(head) == 0 {
		return -1, nil
	}

	// clone again
	nodeCurrentAvailable = nodeAvailable.Clone()
	allocationsByQueueSnap = p.duplicateQueueSnapshots()

	// get the current queue snapshot
	askQueue, ok2 := allocationsByQueueSnap[p.queuePath]
	if !ok2 {
		log.Logger().Warn("BUG: Queue not found by name", zap.String("queuePath", p.queuePath))
		return -1, nil
	}

	// speculatively add the current ask
	askQueue.AddAllocation(p.ask.GetAllocatedResource())

	// Second pass: The task ordering can no longer change. For each task, check that queue constraints would not be
	// violated if the task were to be preempted. If so, discard the task. If the task can be preempted, adjust
	// both the node available capacity and the queue headroom. Save the Index within the results of the first task
	// which would reduce the shortfall to zero.
	results := make([]*Allocation, 0)
	index := -1
	for _, victim := range head {
		// check to see if removing this task will keep queue above guaranteed amount; if not, skip to the next one
		if qv, ok := p.queueByAlloc[victim.GetAllocationKey()]; ok {
			if queueSnapshot, ok2 := allocationsByQueueSnap[qv.QueuePath]; ok2 {
				queueSnapshot.RemoveAllocation(victim.GetAllocatedResource())
				if queueSnapshot.IsAtOrAboveGuaranteedResource() {
					// removing task does not violate queue constraints, adjust queue and node
					nodeCurrentAvailable.AddTo(victim.GetAllocatedResource())
					// check if ask now fits and we haven't had this happen before
					if nodeCurrentAvailable.FitInMaxUndef(p.ask.GetAllocatedResource()) && index < 0 {
						index = len(results)
					}
					// add victim to results
					results = append(results, victim)
				} else {
					// add back resources
					queueSnapshot.AddAllocation(victim.GetAllocatedResource())
				}
			}
		}
	}

	// check to see if enough resources were freed
	if index < 0 {
		return -1, nil
	}

	return index, results
}

func (p *Preemptor) duplicateQueueSnapshots() map[string]*QueuePreemptionSnapshot {
	cache := make(map[string]*QueuePreemptionSnapshot, 0)
	for _, snapshot := range p.allocationsByQueue {
		snapshot.Duplicate(cache)
	}
	return cache
}

// checkPreemptionPredicates calls the shim via the SI to evaluate nodes for preemption
func (p *Preemptor) checkPreemptionPredicates(predicateChecks []*si.PreemptionPredicatesArgs, victimsByNode map[string][]*Allocation) *predicateCheckResult {
	// don't process empty list
	if len(predicateChecks) == 0 {
		return nil
	}

	// sort predicate checks by number of expected preempted tasks
	sort.SliceStable(predicateChecks, func(i int, j int) bool {
		return predicateChecks[i].StartIndex < predicateChecks[j].StartIndex
	})

	// check for RM callback
	plugin := plugins.GetResourceManagerCallbackPlugin()
	if plugin == nil {
		// if a plugin isn't registered, assume checks will succeed and synthesize a result
		check := predicateChecks[0]
		log.Logger().Debug("No RM callback plugin registered, using first selected node for preemption",
			zap.String("NodeID", check.NodeID),
			zap.String("AllocationKey", check.AllocationKey))

		result := &predicateCheckResult{
			allocationKey: check.AllocationKey,
			nodeID:        check.NodeID,
			success:       true,
			index:         int(check.StartIndex),
		}
		result.populateVictims(victimsByNode)
		return result
	}

	// process each batch of checks by sending to the RM
	batches := batchPreemptionChecks(predicateChecks, preemptCheckConcurrency)
	var bestResult *predicateCheckResult = nil
	for _, batch := range batches {
		var wg sync.WaitGroup
		ch := make(chan *predicateCheckResult, len(batch))
		expected := 0
		for _, args := range batch {
			// add goroutine for checking preemption
			wg.Add(1)
			expected++
			go preemptPredicateCheck(plugin, ch, &wg, args)
		}
		// wait for completion and close channel
		go func() {
			wg.Wait()
			close(ch)
		}()
		for result := range ch {
			// if result is successful, keep track of it
			if result.success {
				if bestResult == nil {
					bestResult = result
				} else if result.betterThan(bestResult, p.allocationsByNode) {
					bestResult = result
				}
			}
		}
		// if the best result we have from this batch meets all our criteria, don't run another batch
		if bestResult.isSatisfactory(p.allocationsByNode) {
			break
		}
	}
	bestResult.populateVictims(victimsByNode)
	return bestResult
}

// calculateAdditionalVictims finds additional preemption victims necessary to ensure
func (p *Preemptor) calculateAdditionalVictims(nodeVictims []*Allocation) ([]*Allocation, bool) {
	// clone the queue snapshots
	allocationsByQueueSnap := p.duplicateQueueSnapshots()

	// get the current queue snapshot
	askQueue, ok := allocationsByQueueSnap[p.queuePath]
	if !ok {
		log.Logger().Warn("BUG: Queue not found by name", zap.String("queuePath", p.queuePath))
		return nil, false
	}

	// speculatively add the current ask
	askQueue.AddAllocation(p.ask.GetAllocatedResource())

	// remove all victims previously chosen for the node
	seen := make(map[string]*Allocation, 0)
	for _, victim := range nodeVictims {
		if qv, ok := p.queueByAlloc[victim.GetAllocationKey()]; ok {
			if queueSnapshot, ok2 := allocationsByQueueSnap[qv.QueuePath]; ok2 {
				queueSnapshot.RemoveAllocation(victim.GetAllocatedResource())
				seen[victim.GetAllocationKey()] = victim
			}
		}
	}

	// build and sort list of potential victims
	potentialVictims := make([]*Allocation, 0)
	for _, alloc := range p.allocationsByQueue {
		for _, victim := range alloc.PotentialVictims {
			if _, ok := seen[victim.GetAllocationKey()]; ok {
				// skip already processed victim
				continue
			}
			potentialVictims = append(potentialVictims, victim)
		}
	}
	sort.SliceStable(potentialVictims, func(i, j int) bool {
		return compareAllocationLess(potentialVictims[i], potentialVictims[j])
	})

	// evaluate each potential victim in turn, stopping once sufficient resources have been freed
	victims := make([]*Allocation, 0)
	for _, victim := range potentialVictims {
		// stop search if the ask fits into the queue
		if askQueue.IsWithinGuaranteedResource() {
			break
		}
		// check to see if removing this task will keep queue above guaranteed amount; if not, skip to the next one
		if qv, ok := p.queueByAlloc[victim.GetAllocationKey()]; ok {
			if queueSnapshot, ok2 := allocationsByQueueSnap[qv.QueuePath]; ok2 {
				remaining := askQueue.GetRemainingGuaranteed()
				queueSnapshot.RemoveAllocation(victim.GetAllocatedResource())
				// did removing this allocation still keep the queue over-allocated?
				if queueSnapshot.IsAtOrAboveGuaranteedResource() {
					// check to see if the shortfall on the queue has changed
					newRemaining := askQueue.GetRemainingGuaranteed()
					if resources.EqualsOrEmpty(remaining, newRemaining) {
						// remaining guaranteed amount in ask queue did not change, so preempting task won't help
						queueSnapshot.AddAllocation(victim.GetAllocatedResource())
					} else {
						// remaining capacity changed, so we should keep this task
						victims = append(victims, victim)
					}
				} else {
					// removing this allocation would have reduced queue below guaranteed limits, put it back
					queueSnapshot.AddAllocation(victim.GetAllocatedResource())
				}
			}
		}
	}

	if resources.FitIn(askQueue.GetGuaranteedResource(), p.ask.GetAllocatedResource()) {
		return victims, true
	}
	return nil, false
}

// tryNodes attempts to find potential nodes for scheduling. For each node, potential victims are passed to
// the shim for evaluation, and the best solution found will be returned.
func (p *Preemptor) tryNodes() (string, []*Allocation, bool) {
	// calculate victim list for each node
	predicateChecks := make([]*si.PreemptionPredicatesArgs, 0)
	victimsByNode := make(map[string][]*Allocation)
	for nodeID, nodeAvailable := range p.nodeAvailableMap {
		allocations, ok := p.allocationsByNode[nodeID]
		if !ok {
			// no allocations present, but node may still be available for scheduling
			allocations = make([]*Allocation, 0)
		}
		// identify which victims and in which order should be tried
		if idx, victims := p.calculateVictimsByNode(nodeAvailable, allocations); victims != nil {
			victimsByNode[nodeID] = victims
			keys := make([]string, 0)
			for _, victim := range victims {
				keys = append(keys, victim.GetAllocationKey())
			}
			// only check this node if there are victims or we have not already tried scheduling
			if len(victims) > 0 || !p.nodesTried {
				predicateChecks = append(predicateChecks, &si.PreemptionPredicatesArgs{
					AllocationKey:         p.ask.GetAllocationKey(),
					NodeID:                nodeID,
					PreemptAllocationKeys: keys,
					StartIndex:            int32(idx),
				})
			}
		}
	}
	// call predicates to evaluate each node
	result := p.checkPreemptionPredicates(predicateChecks, victimsByNode)
	if result != nil && result.success {
		return result.nodeID, result.victims, true
	}

	return "", nil, false
}

func (p *Preemptor) TryPreemption() (*Allocation, bool) {
	// validate that sufficient capacity can be freed
	if !p.checkPreemptionQueueGuarantees() {
		return nil, false
	}

	// ensure required data structures are populated
	p.initWorkingState()

	// try to find a node to schedule on and victims to preempt
	nodeID, victims, ok := p.tryNodes()
	if !ok {
		// no preemption possible
		return nil, false
	}

	// look for additional victims in case we have not yet made enough capacity in the queue
	extraVictims, ok := p.calculateAdditionalVictims(victims)
	if !ok {
		// not enough resources were preempted
		return nil, false
	}
	victims = append(victims, extraVictims...)
	if len(victims) == 0 {
		return nil, false
	}

	// preempt the victims
	for _, victim := range victims {
		if victimQueue := p.queue.FindQueueByAppID(victim.GetApplicationID()); victimQueue != nil {
			victimQueue.IncPreemptingResource(victim.GetAllocatedResource())
			victim.MarkPreempted()
			log.Logger().Info("Preempting task",
				zap.String("applicationID", victim.GetApplicationID()),
				zap.String("allocationKey", victim.GetAllocationKey()),
				zap.String("nodeID", victim.GetNodeID()),
				zap.Stringer("resources", victim.GetAllocatedResource()))
		} else {
			log.Logger().Warn("BUG: Queue not found for preemption victim",
				zap.String("applicationID", victim.GetApplicationID()),
				zap.String("allocationKey", victim.GetAllocationKey()))
		}
	}

	// mark ask as having triggered preemption so that we don't preempt again
	p.ask.MarkTriggeredPreemption()

	// notify RM that victims should be released
	p.application.notifyRMAllocationReleased(p.application.rmID, victims, si.TerminationType_PREEMPTED_BY_SCHEDULER,
		"preempting allocations to free up resources to run ask: "+p.ask.GetAllocationKey())

	// reserve the selected node for the new allocation if it will fit
	if p.headRoom.FitInMaxUndef(p.ask.GetAllocatedResource()) {
		log.Logger().Info("Reserving node for ask after preemption",
			zap.String("allocationKey", p.ask.GetAllocationKey()),
			zap.String("nodeID", nodeID),
			zap.Int("victimCount", len(victims)))
		return newReservedAllocation(Reserved, nodeID, p.ask), true
	}

	// can't reserve as queue is still too full, but scheduling should succeed after preemption occurs
	log.Logger().Info("Preempting allocations for ask, but not reserving yet as queue is still above capacity",
		zap.String("allocationKey", p.ask.GetAllocationKey()),
		zap.Int("victimCount", len(victims)))

	return nil, true
}

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
		if allocation.GetAsk().IsOriginator() {
			score |= scoreOriginator
		}
		if !allocation.GetAsk().IsAllowPreemptSelf() {
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
		log.Logger().Warn("BUG: Unable to find node in victim map", zap.String("nodeID", pcr.nodeID))
		pcr.success = false
		pcr.index = -1
		return
	}

	// abort if index is too large
	if pcr.index >= len(victimList) {
		log.Logger().Warn("BUG: Got invalid index into allocation list",
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

// Duplicate creates a copy of this snapshot into the given map by queue path
func (qps *QueuePreemptionSnapshot) Duplicate(copy map[string]*QueuePreemptionSnapshot) *QueuePreemptionSnapshot {
	if qps == nil {
		return nil
	}

	if existing, ok := copy[qps.QueuePath]; ok {
		return existing
	}

	var parent *QueuePreemptionSnapshot = nil
	if qps.Parent != nil {
		qps.Parent.Duplicate(copy)
		parent = qps.Parent.Duplicate(copy)
	}
	snapshot := &QueuePreemptionSnapshot{
		Parent:             parent,
		QueuePath:          qps.QueuePath,
		AllocatedResource:  qps.AllocatedResource.Clone(),
		PreemptingResource: qps.PreemptingResource.Clone(),
		MaxResource:        qps.MaxResource.Clone(),
		GuaranteedResource: qps.GuaranteedResource.Clone(),
		PotentialVictims:   qps.PotentialVictims,
	}
	copy[qps.QueuePath] = snapshot
	return snapshot
}

// IsAtOrAboveGuaranteedResource determines if this queue is exceeding resource guarantees and therefore
// may be eligible for further preemption
func (qps *QueuePreemptionSnapshot) IsAtOrAboveGuaranteedResource() bool {
	if qps == nil {
		return false
	}
	guaranteed := qps.GetGuaranteedResource()
	max := qps.GetMaxResource()
	absGuaranteed := resources.ComponentWiseMinPermissive(guaranteed, max)
	used := resources.Sub(qps.AllocatedResource, qps.PreemptingResource)

	// if we don't fit, we're clearly above
	if !resources.FitIn(absGuaranteed, used) {
		return true
	}

	usedOrMax := resources.ComponentWiseMax(guaranteed, used)
	return resources.Equals(usedOrMax, used)
}

// IsWithinGuaranteedResource determines if this queue is within its current resource guarantees
func (qps *QueuePreemptionSnapshot) IsWithinGuaranteedResource() bool {
	if qps == nil {
		return true
	}
	// check the parent, as violations at any level mean we are not within limits
	if !qps.Parent.IsWithinGuaranteedResource() {
		return false
	}
	guaranteed := qps.GetGuaranteedResource()
	max := qps.GetMaxResource()
	absGuaranteed := resources.ComponentWiseMinPermissive(guaranteed, max)
	used := resources.Sub(qps.AllocatedResource, qps.PreemptingResource)
	return resources.FitIn(absGuaranteed, used)
}

func (qps *QueuePreemptionSnapshot) GetRemainingGuaranteed() *resources.Resource {
	if qps == nil {
		return nil
	}
	parentResult := qps.Parent.GetRemainingGuaranteed()
	if parentResult == nil {
		parentResult = resources.NewResource()
	}
	guaranteed := qps.GetGuaranteedResource()
	max := qps.GetMaxResource()
	absGuaranteed := resources.ComponentWiseMinPermissive(guaranteed, max)
	used := resources.Sub(qps.AllocatedResource, qps.PreemptingResource)
	remaining := resources.Sub(absGuaranteed, used)
	return resources.ComponentWiseMin(remaining, parentResult)
}

// GetGuaranteedResource computes the current guaranteed resources considering parent guaranteed
func (qps *QueuePreemptionSnapshot) GetGuaranteedResource() *resources.Resource {
	if qps == nil {
		return resources.NewResource()
	}
	return resources.ComponentWiseMinPermissive(qps.Parent.GetGuaranteedResource(), qps.GuaranteedResource)
}

// GetMaxResource computes the current max resources considering parent max
func (qps *QueuePreemptionSnapshot) GetMaxResource() *resources.Resource {
	if qps == nil {
		return resources.NewResource()
	}
	return resources.ComponentWiseMinPermissive(qps.Parent.GetMaxResource(), qps.MaxResource)
}

// AddAllocation adds an allocation to this snapshot's resource usage
func (qps *QueuePreemptionSnapshot) AddAllocation(alloc *resources.Resource) {
	if qps == nil {
		return
	}
	qps.Parent.AddAllocation(alloc)
	qps.AllocatedResource.AddTo(alloc)
}

// RemoveAllocation removes an allocation from this snapshot's resource usage
func (qps *QueuePreemptionSnapshot) RemoveAllocation(alloc *resources.Resource) {
	if qps == nil {
		return
	}
	qps.Parent.RemoveAllocation(alloc)
	qps.AllocatedResource.SubFrom(alloc)
}

// compareAllocationLess compares two allocations for preemption. Allocations which have opted into preemption are
// considered first, then allocations which are not the originator of their associated application. Ties are broken
// by creation time, with
// then
func compareAllocationLess(left *Allocation, right *Allocation) bool {
	scoreLeft := scoreAllocation(left)
	scoreRight := scoreAllocation(right)
	if scoreLeft != scoreRight {
		return scoreLeft < scoreRight
	}
	return left.createTime.After(right.createTime)
}

// scoreAllocation generates a relative score for an allocation. Lower-scored allocations are considered more likely
// preemption candidates. Tasks which have opted into preemption are considered first, then tasks which are not
// application originators.
func scoreAllocation(allocation *Allocation) uint64 {
	var score uint64 = 0
	if allocation.GetAsk().IsOriginator() {
		score |= scoreOriginator
	}
	if !allocation.GetAsk().IsAllowPreemptSelf() {
		score |= scoreNoPreempt
	}
	return score
}

// sortVictimsForPreemption sorts allocations on each node, preferring those that have opted-in to preemption,
// those that are not originating tasks for an application, and newest first
func sortVictimsForPreemption(allocationsByNode map[string][]*Allocation) {
	for _, allocations := range allocationsByNode {
		sort.SliceStable(allocations, func(i, j int) bool {
			leftAsk := allocations[i].GetAsk()
			rightAsk := allocations[j].GetAsk()

			// sort asks which allow themselves to be preempted first
			if leftAsk.IsAllowPreemptSelf() && !rightAsk.IsAllowPreemptSelf() {
				return true
			}
			if rightAsk.IsAllowPreemptSelf() && !leftAsk.IsAllowPreemptSelf() {
				return false
			}

			// next those that are not app originators
			if leftAsk.IsOriginator() && !rightAsk.IsOriginator() {
				return false
			}
			if rightAsk.IsOriginator() && !leftAsk.IsOriginator() {
				return true
			}

			// finally sort by creation time descending
			return leftAsk.GetCreateTime().After(rightAsk.GetCreateTime())
		})
	}
}

// preemptPredicateCheck performs a single predicate check and reports the result on a channel
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

// batchPreemptionChecks splits predicate checks into groups by batch size
func batchPreemptionChecks(checks []*si.PreemptionPredicatesArgs, batchSize int) [][]*si.PreemptionPredicatesArgs {
	var result [][]*si.PreemptionPredicatesArgs
	for i := 0; i < len(checks); i += batchSize {
		end := i + batchSize
		if end > len(checks) {
			end = len(checks)
		}
		result = append(result, checks[i:end])
	}
	return result
}
