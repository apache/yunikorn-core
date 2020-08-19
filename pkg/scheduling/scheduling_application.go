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
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/handler"
	"github.com/apache/incubator-yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/events"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

var (
	startingTimeout = time.Minute * 5
)

var reservationDelay = 2 * time.Second

type SchedulingApplication struct {
	ApplicationID  string
	Partition      string
	SubmissionTime int64
	QueueName      string

	// Private fields need protection
	queue          *SchedulingQueue                    // queue the application is running in
	user           security.UserGroup                  // owner of the application
	tags           map[string]string                   // application tags used in scheduling
	allocated      *resources.Resource                 // total allocated resources
	allocating     *resources.Resource                 // allocating resource set by the scheduler
	allocations    map[string]*schedulingAllocation    // list of all allocations
	pending        *resources.Resource                 // pending resources from asks for the app
	reservations   map[string]*reservation             // a map of reservations
	requests       map[string]*schedulingAllocationAsk // a map of asks
	sortedRequests []*schedulingAllocationAsk
	stateMachine   *fsm.FSM    // application state machine
	stateTimer     *time.Timer // timer for state time

	eventHandlers handler.EventHandlers
	rmID          string

	sync.RWMutex
}

func NewSchedulingApp(appID, partition, queueName string, ugi security.UserGroup, tags map[string]string,
	eventHandler handler.EventHandlers, rmID string) *SchedulingApplication {
	app := newSchedulingAppInternal(appID, partition, queueName, ugi, tags)
	app.eventHandlers = eventHandler
	app.rmID = rmID
	return app
}

// Create a new application
func newSchedulingAppInternal(appID, partition, queueName string, ugi security.UserGroup, tags map[string]string) *SchedulingApplication {
	return &SchedulingApplication{
		ApplicationID:  appID,
		Partition:      partition,
		QueueName:      queueName,
		SubmissionTime: time.Now().UnixNano(),
		tags:           tags,
		user:           ugi,
		allocated:      resources.NewResource(),
		allocations:    make(map[string]*schedulingAllocation),
		stateMachine:   newAppState(),
		allocating:     resources.NewResource(),
		pending:        resources.NewResource(),
		requests:       make(map[string]*schedulingAllocationAsk),
		reservations:   make(map[string]*reservation),
	}
}

// override reservation delay for tests
func OverrideReservationDelay(delay time.Duration) {
	log.Logger().Debug("Test override reservation delay",
		zap.Duration("delay", delay))
	reservationDelay = delay
}

// Return an array of all reservation keys for the app.
// This will return an empty array if there are no reservations.
// Visible for tests
func (sa *SchedulingApplication) GetReservations() []string {
	sa.RLock()
	defer sa.RUnlock()
	keys := make([]string, 0)
	for key := range sa.reservations {
		keys = append(keys, key)
	}
	return keys
}

// Return the allocation ask for the key, nil if not found
func (sa *SchedulingApplication) GetSchedulingAllocationAsk(allocationKey string) *schedulingAllocationAsk {
	sa.RLock()
	defer sa.RUnlock()
	return sa.requests[allocationKey]
}

// Return the pending resources for this application
func (sa *SchedulingApplication) GetPendingResource() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return sa.pending
}

// Return the allocating and allocated resources for this application
func (sa *SchedulingApplication) getAssumeAllocated() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return resources.Add(sa.allocating, sa.GetAllocatedResource())
}

// Return the allocating resources for this application
func (sa *SchedulingApplication) getAllocatingResource() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return sa.allocating
}

// Increment allocating resource for the app
func (sa *SchedulingApplication) incAllocatingResource(delta *resources.Resource) {
	sa.Lock()
	defer sa.Unlock()
	sa.allocating.AddTo(delta)
}

// Decrement allocating resource for the app
func (sa *SchedulingApplication) decAllocatingResource(delta *resources.Resource) {
	sa.Lock()
	defer sa.Unlock()
	var err error
	sa.allocating, err = resources.SubErrorNegative(sa.allocating, delta)
	if err != nil {
		log.Logger().Warn("Allocating resources went negative",
			zap.Error(err))
	}
}

// Remove one or more allocation asks from this application.
// This also removes any reservations that are linked to the ask.
// The return value is the number of reservations released
func (sa *SchedulingApplication) removeAllocationAsk(allocKey string) int {
	sa.Lock()
	defer sa.Unlock()
	// shortcut no need to do anything
	if len(sa.requests) == 0 {
		return 0
	}
	var deltaPendingResource *resources.Resource = nil
	// when allocation key not specified, cleanup all allocation ask
	var toRelease int
	if allocKey == "" {
		// cleanup all reservations
		for key, reserve := range sa.reservations {
			releases, err := sa.unReserveInternal(reserve.node, reserve.ask)
			if err != nil {
				log.Logger().Warn("Removal of reservation failed while removing all allocation asks",
					zap.String("appID", sa.ApplicationID),
					zap.String("reservationKey", key),
					zap.Error(err))
				continue
			}
			// clean up the queue reservation (one at a time)
			sa.queue.unReserve(sa.ApplicationID, releases)
			toRelease += releases
		}
		// Cleanup total pending resource
		deltaPendingResource = sa.pending
		sa.pending = resources.NewResource()
		sa.requests = make(map[string]*schedulingAllocationAsk)
	} else {
		// cleanup the reservation for this allocation
		for _, key := range sa.isAskReserved(allocKey) {
			reserve := sa.reservations[key]
			releases, err := sa.unReserveInternal(reserve.node, reserve.ask)
			if err != nil {
				log.Logger().Warn("Removal of reservation failed while removing allocation ask",
					zap.String("appID", sa.ApplicationID),
					zap.String("reservationKey", key),
					zap.Error(err))
				continue
			}
			// clean up the queue reservation
			sa.queue.unReserve(sa.ApplicationID, releases)
			toRelease += releases
		}
		if ask := sa.requests[allocKey]; ask != nil {
			deltaPendingResource = resources.MultiplyBy(ask.AllocatedResource, float64(ask.getPendingAskRepeat()))
			sa.pending.SubFrom(deltaPendingResource)
			delete(sa.requests, allocKey)
		}
	}
	// clean up the queue pending resources
	sa.queue.decPendingResource(deltaPendingResource)
	// Check if we need to change state based on the ask removal:
	// 1) if pending is zero (no more asks left)
	// 2) if confirmed allocations is zero (nothing is running)
	// 3) if there are no allocations in flight
	// Change the state to waiting.
	// When all 3 resources trackers are zero we should not expect anything to come in later.
	if resources.IsZero(sa.pending) && resources.IsZero(sa.GetAllocatedResource()) && resources.IsZero(sa.allocating) {
		if err := sa.HandleApplicationEvent(WaitApplication); err != nil {
			log.Logger().Warn("Application state not changed to Waiting while updating ask(s)",
				zap.String("currentState", sa.GetApplicationState()),
				zap.Error(err))
		}
	}

	return toRelease
}

// Add an allocation ask to this application
// If the ask already exist update the existing info
func (sa *SchedulingApplication) addAllocationAsk(ask *schedulingAllocationAsk) (*resources.Resource, error) {
	sa.Lock()
	defer sa.Unlock()
	if ask == nil {
		return nil, fmt.Errorf("ask cannot be nil when added to app %s", sa.ApplicationID)
	}
	if ask.getPendingAskRepeat() == 0 || resources.IsZero(ask.AllocatedResource) {
		return nil, fmt.Errorf("invalid ask added to app %s: %v", sa.ApplicationID, ask)
	}
	ask.QueueName = sa.queue.QueuePath
	delta := resources.Multiply(ask.AllocatedResource, int64(ask.getPendingAskRepeat()))

	var oldAskResource *resources.Resource = nil
	if oldAsk := sa.requests[ask.AskProto.AllocationKey]; oldAsk != nil {
		oldAskResource = resources.Multiply(oldAsk.AllocatedResource, int64(oldAsk.getPendingAskRepeat()))
	}

	// Check if we need to change state based on the ask added, there are two cases:
	// 1) first ask added on a new app: state is New
	// 2) all asks and allocation have been removed: state is Waiting
	// Move the state and get it scheduling (again)
	if sa.isNew() || sa.isWaiting() {
		if err := sa.HandleApplicationEvent(RunApplication); err != nil {
			log.Logger().Debug("Application state change failed while adding new ask",
				zap.String("currentState", sa.GetApplicationState()),
				zap.Error(err))
		}
	}
	sa.requests[ask.AskProto.AllocationKey] = ask

	// Update total pending resource
	delta.SubFrom(oldAskResource)
	sa.pending.AddTo(delta)
	sa.queue.incPendingResource(delta)

	return delta, nil
}

func (sa *SchedulingApplication) updateAskRepeat(allocKey string, delta int32) (*resources.Resource, error) {
	sa.Lock()
	defer sa.Unlock()
	if ask := sa.requests[allocKey]; ask != nil {
		return sa.updateAskRepeatInternal(ask, delta)
	}
	return nil, fmt.Errorf("failed to locate ask with key %s", allocKey)
}

func (sa *SchedulingApplication) updateAskRepeatInternal(ask *schedulingAllocationAsk, delta int32) (*resources.Resource, error) {
	// updating with delta does error checking internally
	if !ask.updatePendingAskRepeat(delta) {
		return nil, fmt.Errorf("ask repaeat not updated resulting repeat less than zero for ask %s on app %s", ask.AskProto.AllocationKey, sa.ApplicationID)
	}

	deltaPendingResource := resources.Multiply(ask.AllocatedResource, int64(delta))
	sa.pending.AddTo(deltaPendingResource)
	// update the pending of the queue with the same delta
	sa.queue.incPendingResource(deltaPendingResource)

	return deltaPendingResource, nil
}

// Return if the application has any reservations.
func (sa *SchedulingApplication) hasReserved() bool {
	sa.RLock()
	defer sa.RUnlock()
	return len(sa.reservations) > 0
}

// Return if the application has the node reserved.
// An empty nodeID is never reserved.
func (sa *SchedulingApplication) isReservedOnNode(nodeID string) bool {
	if nodeID == "" {
		return false
	}
	sa.RLock()
	defer sa.RUnlock()
	for key := range sa.reservations {
		if strings.HasPrefix(key, nodeID) {
			return true
		}
	}
	return false
}

// Reserve the application for this node and ask combination.
// If the reservation fails the function returns false, if the reservation is made it returns true.
// If the node and ask combination was already reserved for the application this is a noop and returns true.
func (sa *SchedulingApplication) reserve(node *SchedulingNode, ask *schedulingAllocationAsk) error {
	sa.Lock()
	defer sa.Unlock()
	// create the reservation (includes nil checks)
	nodeReservation := newReservation(node, sa, ask, true)
	if nodeReservation == nil {
		log.Logger().Debug("reservation creation failed unexpectedly",
			zap.String("app", sa.ApplicationID),
			zap.Any("node", node),
			zap.Any("ask", ask))
		return fmt.Errorf("reservation creation failed node or ask are nil on appID %s", sa.ApplicationID)
	}
	allocKey := ask.AskProto.AllocationKey
	if sa.requests[allocKey] == nil {
		log.Logger().Debug("ask is not registered to this app",
			zap.String("app", sa.ApplicationID),
			zap.String("allocKey", allocKey))
		return fmt.Errorf("reservation creation failed ask %s not found on appID %s", allocKey, sa.ApplicationID)
	}
	if !sa.canAskReserve(ask) {
		return fmt.Errorf("reservation of ask exceeds pending repeat, pending ask repeat %d", ask.getPendingAskRepeat())
	}
	// check if we can reserve the node before reserving on the app
	if err := node.reserve(sa, ask); err != nil {
		return err
	}
	sa.reservations[nodeReservation.getKey()] = nodeReservation
	// reservation added successfully
	return nil
}

// unReserve the application for this node and ask combination.
// This first removes the reservation from the node.
// If the reservation does not exist it returns 0 for reservations removed, if the reservation is removed it returns 1.
// The error is set if the reservation key cannot be removed from the app or node.
func (sa *SchedulingApplication) unReserve(node *SchedulingNode, ask *schedulingAllocationAsk) (int, error) {
	sa.Lock()
	defer sa.Unlock()
	return sa.unReserveInternal(node, ask)
}

// Unlocked version for unReserve that really does the work.
// Must only be called while holding the application lock.
func (sa *SchedulingApplication) unReserveInternal(node *SchedulingNode, ask *schedulingAllocationAsk) (int, error) {
	resKey := reservationKey(node, nil, ask)
	if resKey == "" {
		log.Logger().Debug("unreserve reservation key create failed unexpectedly",
			zap.String("appID", sa.ApplicationID),
			zap.Any("node", node),
			zap.Any("ask", ask))
		return 0, fmt.Errorf("reservation key failed node or ask are nil for appID %s", sa.ApplicationID)
	}
	// unReserve the node before removing from the app
	var num int
	var err error
	if num, err = node.unReserve(sa, ask); err != nil {
		return 0, err
	}
	// if the unreserve worked on the node check the app
	if _, found := sa.reservations[resKey]; found {
		// worked on the node means either found or not but no error, log difference here
		if num == 0 {
			log.Logger().Info("reservation not found while removing from node, app has reservation",
				zap.String("appID", sa.ApplicationID),
				zap.String("nodeID", node.NodeID),
				zap.String("ask", ask.AskProto.AllocationKey))
		}
		delete(sa.reservations, resKey)
		return 1, nil
	}
	// reservation was not found
	log.Logger().Info("reservation not found while removing from app",
		zap.String("appID", sa.ApplicationID),
		zap.String("nodeID", node.NodeID),
		zap.String("ask", ask.AskProto.AllocationKey),
		zap.Int("nodeReservationsRemoved", num))
	return 0, nil
}

// Return the allocation reservations on any node.
// The returned array is 0 or more keys into the reservations map.
// No locking must be called while holding the lock
func (sa *SchedulingApplication) isAskReserved(allocKey string) []string {
	reservationKeys := make([]string, 0)
	if allocKey == "" {
		return reservationKeys
	}
	for key := range sa.reservations {
		if strings.HasSuffix(key, allocKey) {
			reservationKeys = append(reservationKeys, key)
		}
	}
	return reservationKeys
}

// Check if the allocation has already been reserved. An ask can reserve multiple nodes if the request has a repeat set
// larger than 1. It can never reserve more than the repeat number of nodes.
// No locking must be called while holding the lock
func (sa *SchedulingApplication) canAskReserve(ask *schedulingAllocationAsk) bool {
	allocKey := ask.AskProto.AllocationKey
	pending := int(ask.getPendingAskRepeat())
	resNumber := sa.isAskReserved(allocKey)
	if len(resNumber) >= pending {
		log.Logger().Debug("reservation exceeds repeats",
			zap.String("askKey", allocKey),
			zap.Int("askPending", pending),
			zap.Int("askReserved", len(resNumber)))
	}
	return pending > len(resNumber)
}

// Sort the request for the app in order based on the priority of the request.
// The sorted list only contains candidates that have an outstanding repeat.
// No locking must be called while holding the lock
func (sa *SchedulingApplication) sortRequests(ascending bool) {
	sa.sortedRequests = nil
	for _, request := range sa.requests {
		if request.getPendingAskRepeat() == 0 {
			continue
		}
		sa.sortedRequests = append(sa.sortedRequests, request)
	}
	// we might not have any requests
	if len(sa.sortedRequests) > 0 {
		sortAskByPriority(sa.sortedRequests, ascending)
	}
}

func (sa *SchedulingApplication) getOutstandingRequests(headRoom *resources.Resource, total *[]*schedulingAllocationAsk) {
	sa.RLock()
	defer sa.RUnlock()

	// make sure the request are sorted
	sa.sortRequests(false)
	for _, request := range sa.sortedRequests {
		if headRoom == nil || resources.FitIn(headRoom, request.AllocatedResource) {
			// if headroom is still enough for the resources
			*total = append(*total, request)
			if headRoom != nil {
				headRoom.SubFrom(request.AllocatedResource)
			}
		}
	}
}

// Try a regular allocation of the pending requests
func (sa *SchedulingApplication) tryAllocate(headRoom *resources.Resource, ctx *PartitionSchedulingContext) *schedulingAllocation {
	sa.Lock()
	defer sa.Unlock()
	// make sure the request are sorted
	sa.sortRequests(false)
	// get all the requests from the app sorted in order
	for _, request := range sa.sortedRequests {
		// resource must fit in headroom otherwise skip the request
		if !resources.FitIn(headRoom, request.AllocatedResource) {
			// post scheduling events via the scheduler plugin
			if eventCache := events.GetEventCache(); eventCache != nil {
				message := fmt.Sprintf("Application %s does not fit into %s queue", request.ApplicationID, request.QueueName)
				askProto := request.AskProto
				if event, err := events.CreateRequestEventRecord(askProto.AllocationKey, askProto.ApplicationID, "InsufficientQueueResources", message); err != nil {
					log.Logger().Warn("Event creation failed",
						zap.String("event message", message),
						zap.Error(err))
				} else {
					eventCache.AddEvent(event)
				}
			}
			continue
		}
		if nodeIterator := ctx.getNodeIterator(); nodeIterator != nil {
			alloc := sa.tryNodes(request, nodeIterator)
			// have a candidate return it
			if alloc != nil {
				return alloc
			}
		}
	}
	// no requests fit, skip to next app
	return nil
}

// Try a reserved allocation of an outstanding reservation
func (sa *SchedulingApplication) tryReservedAllocate(headRoom *resources.Resource, ctx *PartitionSchedulingContext) *schedulingAllocation {
	sa.Lock()
	defer sa.Unlock()
	// process all outstanding reservations and pick the first one that fits
	for _, reserve := range sa.reservations {
		ask := sa.requests[reserve.askKey]
		// sanity check and cleanup if needed
		if ask == nil || ask.getPendingAskRepeat() == 0 {
			var unreserveAsk *schedulingAllocationAsk
			// if the ask was not found we need to construct one to unreserve
			if ask == nil {
				unreserveAsk = &schedulingAllocationAsk{
					AskProto:      &si.AllocationAsk{AllocationKey: reserve.askKey},
					ApplicationID: sa.ApplicationID,
					QueueName:     sa.queue.QueuePath,
				}
			} else {
				unreserveAsk = ask
			}
			// remove the reservation as this should not be reserved
			alloc := newSchedulingAllocation(unreserveAsk, reserve.nodeID)
			alloc.result = unreserved
			return alloc
		}
		// check if this fits in the queue's head room
		if !resources.FitIn(headRoom, ask.AllocatedResource) {
			continue
		}
		// check allocation possibility
		alloc := sa.tryNode(reserve.node, ask)
		// allocation worked set the result and return
		if alloc != nil {
			alloc.result = allocatedReserved
			return alloc
		}
	}
	// lets try this on all other nodes
	for _, reserve := range sa.reservations {
		if nodeIterator := ctx.getNodeIterator(); nodeIterator != nil {
			alloc := sa.tryNodesNoReserve(reserve.ask, nodeIterator, reserve.nodeID)
			// have a candidate return it, including the node that was reserved
			if alloc != nil {
				return alloc
			}
		}
	}
	return nil
}

// Try all the nodes for a reserved request that have not been tried yet.
// This should never result in a reservation as the ask is already reserved
func (sa *SchedulingApplication) tryNodesNoReserve(ask *schedulingAllocationAsk, nodeIterator NodeIterator, reservedNode string) *schedulingAllocation {
	for nodeIterator.HasNext() {
		node := nodeIterator.Next()
		// skip over the node if the resource does not fit the node or this is the reserved node.
		if !node.FitInNode(ask.AllocatedResource) || node.NodeID == reservedNode {
			continue
		}
		alloc := sa.tryNode(node, ask)
		// allocation worked so return
		if alloc != nil {
			alloc.reservedNodeID = reservedNode
			alloc.result = allocatedReserved
			return alloc
		}
	}
	// ask does not fit, skip to next ask
	return nil
}

// Try all the nodes for a request. The result is an allocation or reservation of a node.
// New allocations can only be reserved after a delay.
func (sa *SchedulingApplication) tryNodes(ask *schedulingAllocationAsk, nodeIterator NodeIterator) *schedulingAllocation {
	var nodeToReserve *SchedulingNode
	scoreReserved := math.Inf(1)
	// check if the ask is reserved or not
	allocKey := ask.AskProto.AllocationKey
	reservedAsks := sa.isAskReserved(allocKey)
	allowReserve := len(reservedAsks) < int(ask.pendingRepeatAsk)
	for nodeIterator.HasNext() {
		node := nodeIterator.Next()
		// skip over the node if the resource does not fit the node at all.
		if !node.FitInNode(ask.AllocatedResource) {
			continue
		}
		alloc := sa.tryNode(node, ask)
		// allocation worked so return
		if alloc != nil {
			// check if the node was reserved for this ask: if it is set the result and return
			// NOTE: this is a safeguard as reserved nodes should never be part of the iterator
			// but we have no locking
			if _, ok := sa.reservations[reservationKey(node, nil, ask)]; ok {
				log.Logger().Debug("allocate found reserved ask during non reserved allocate",
					zap.String("appID", sa.ApplicationID),
					zap.String("nodeID", node.NodeID),
					zap.String("allocationKey", allocKey))
				alloc.result = allocatedReserved
				return alloc
			}
			// we could also have a different node reserved for this ask if it has pick one of
			// the reserved nodes to unreserve (first one in the list)
			if len(reservedAsks) > 0 {
				nodeID := strings.TrimSuffix(reservedAsks[0], "|"+allocKey)
				log.Logger().Debug("allocate picking reserved ask during non reserved allocate",
					zap.String("appID", sa.ApplicationID),
					zap.String("nodeID", nodeID),
					zap.String("allocationKey", allocKey))
				alloc.result = allocatedReserved
				alloc.reservedNodeID = nodeID
				return alloc
			}
			// nothing reserved just return this as a normal alloc
			alloc.result = allocated
			return alloc
		}
		// nothing allocated should we look at a reservation?
		// TODO make this smarter a hardcoded delay is not the right thing
		askAge := time.Since(ask.getCreateTime())
		if allowReserve && askAge > reservationDelay {
			log.Logger().Debug("app reservation check",
				zap.String("allocationKey", allocKey),
				zap.Time("createTime", ask.getCreateTime()),
				zap.Duration("askAge", askAge),
				zap.Duration("reservationDelay", reservationDelay))
			score := ask.AllocatedResource.FitInScore(node.GetAvailableResource())
			// Record the so-far best node to reserve
			if score < scoreReserved {
				scoreReserved = score
				nodeToReserve = node
			}
		}
	}
	// we have not allocated yet, check if we should reserve
	// NOTE: the node should not be reserved as the iterator filters them but we do not lock the nodes
	if nodeToReserve != nil && !nodeToReserve.isReserved() {
		log.Logger().Debug("found candidate node for app reservation",
			zap.String("appID", sa.ApplicationID),
			zap.String("nodeID", nodeToReserve.NodeID),
			zap.String("allocationKey", allocKey),
			zap.Int("reservations", len(reservedAsks)),
			zap.Int32("pendingRepeats", ask.pendingRepeatAsk))
		// skip the node if conditions can not be satisfied
		if !nodeToReserve.preReserveConditions(allocKey) {
			return nil
		}
		// return allocation proposal and mark it as a reservation
		alloc := newSchedulingAllocation(ask, nodeToReserve.NodeID)
		alloc.result = reserved
		return alloc
	}
	// ask does not fit, skip to next ask
	return nil
}

// Try allocating on one specific node
func (sa *SchedulingApplication) tryNode(node *SchedulingNode, ask *schedulingAllocationAsk) *schedulingAllocation {
	allocKey := ask.AskProto.AllocationKey
	toAllocate := ask.AllocatedResource
	// create the key for the reservation
	if err := node.preAllocateCheck(toAllocate, reservationKey(nil, sa, ask), false); err != nil {
		// skip schedule onto node
		return nil
	}
	// skip the node if conditions can not be satisfied
	if !node.preAllocateConditions(allocKey) {
		return nil
	}
	// everything OK really allocate
	if node.allocateResource(toAllocate, false) {
		// before deciding on an allocation, call the reconcile plugin to sync scheduler cache
		// between core and shim if necessary. This is useful when running multiple allocations
		// in parallel and need to handle inter container affinity and anti-affinity.
		if rp := plugins.GetReconcilePlugin(); rp != nil {
			if err := rp.ReSyncSchedulerCache(&si.ReSyncSchedulerCacheArgs{
				AssumedAllocations: []*si.AssumedAllocation{
					{
						AllocationKey: allocKey,
						NodeID:        node.NodeID,
					},
				},
			}); err != nil {
				log.Logger().Error("failed to sync shim cache",
					zap.Error(err))
			}
		}
		// update the allocating resources
		sa.queue.incAllocatingResource(toAllocate)
		sa.allocating.AddTo(toAllocate)
		// mark this ask as allocating by lowering the repeat
		_, err := sa.updateAskRepeatInternal(ask, -1)
		if err != nil {
			log.Logger().Debug("ask repeat update failed unexpectedly",
				zap.Error(err))
		}

		// return allocation
		return newSchedulingAllocation(ask, node.NodeID)
	}
	return nil
}

// Recover the allocation for this app on the node provided.
// This is only called for recovering existing allocations on a node. We can not use the normal scheduling for this as
// the cache has already been updated and the allocation is confirmed. Checks for resource limits would fail. However
// the scheduler fakes a confirmation from the cache later and we thus need this to track correctly.
func (sa *SchedulingApplication) recoverOnNode(node *SchedulingNode, ask *schedulingAllocationAsk) {
	sa.Lock()
	defer sa.Unlock()
	toAllocate := ask.AllocatedResource
	// update the scheduling objects with the in progress resource
	node.incAllocatingResource(toAllocate)
	sa.queue.incAllocatingResource(toAllocate)
	sa.allocating.AddTo(toAllocate)
	// mark this ask as allocating by lowering the repeat
	if _, err := sa.updateAskRepeatInternal(ask, -1); err != nil {
		log.Logger().Error("application recovery update of existing allocation failed",
			zap.String("appID", sa.ApplicationID),
			zap.String("allocKey", ask.AskProto.AllocationKey),
			zap.Error(err))
	}
}

// Application status methods reflecting the underlying app object state
// link back to the underlying app object to prevent out of sync states
func (sa *SchedulingApplication) isAccepted() bool {
	return sa.IsAccepted()
}

func (sa *SchedulingApplication) isStarting() bool {
	return sa.IsStarting()
}

func (sa *SchedulingApplication) isNew() bool {
	return sa.IsNew()
}

func (sa *SchedulingApplication) isWaiting() bool {
	return sa.IsWaiting()
}

// Get a tag from the cache object
func (sa *SchedulingApplication) getTag(tag string) string {
	return sa.GetTag(tag)
}

// Move the app state to running after allocation has been recovered.
// Since we do not add allocations in the normal way states will not change during recovery.
// There could also be multiple nodes that recover the app and
// This should move via starting directly to running.
func (sa *SchedulingApplication) finishRecovery() {
	// no need to do anything if we are already running
	if sa.IsRunning() {
		return
	}
	// If we're not running we need to cover two cases in one: move from accepted, via starting to running
	err := sa.HandleApplicationEvent(RunApplication)
	if err == nil {
		err = sa.HandleApplicationEvent(RunApplication)
	}
	// log the unexpected failure
	if err != nil {
		log.Logger().Error("Unexpected app state change failure while recovering allocation",
			zap.String("currentState", sa.GetApplicationState()),
			zap.Error(err))
	}
}

// Return the current allocations for the application.
func (sa *SchedulingApplication) GetAllAllocations() []*schedulingAllocation {
	sa.RLock()
	defer sa.RUnlock()

	var allocations []*schedulingAllocation
	for _, alloc := range sa.allocations {
		allocations = append(allocations, alloc)
	}
	return allocations
}

// Return the current state or a checked specific state for the application.
// The state machine handles the locking.
func (sa *SchedulingApplication) GetApplicationState() string {
	return sa.stateMachine.Current()
}

func (sa *SchedulingApplication) IsStarting() bool {
	return sa.stateMachine.Is(Starting.String())
}

func (sa *SchedulingApplication) IsAccepted() bool {
	return sa.stateMachine.Is(Accepted.String())
}

func (sa *SchedulingApplication) IsNew() bool {
	return sa.stateMachine.Is(New.String())
}

func (sa *SchedulingApplication) IsRunning() bool {
	return sa.stateMachine.Is(Running.String())
}

func (sa *SchedulingApplication) IsWaiting() bool {
	return sa.stateMachine.Is(Waiting.String())
}

// Handle the state event for the application.
// The state machine handles the locking.
func (sa *SchedulingApplication) HandleApplicationEvent(event applicationEvent) error {
	err := sa.stateMachine.Event(event.String(), sa)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() == "no transition" {
		return nil
	}
	return err
}

func (sa *SchedulingApplication) onStateChange(event *fsm.Event) {
	updatedApps := make([]*si.UpdatedApplication, 0)
	updatedApps = append(updatedApps, &si.UpdatedApplication{
		ApplicationID:            sa.ApplicationID,
		State:                    sa.stateMachine.Current(),
		StateTransitionTimestamp: time.Now().UnixNano(),
		Message:                  fmt.Sprintf("{Status change triggered by the event : %v}", event),
	})

	if sa.eventHandlers.RMProxyEventHandler != nil {
		sa.eventHandlers.RMProxyEventHandler.HandleEvent(
			&rmevent.RMApplicationUpdateEvent{
				RmID:                 sa.rmID,
				AcceptedApplications: make([]*si.AcceptedApplication, 0),
				RejectedApplications: make([]*si.RejectedApplication, 0),
				UpdatedApplications:  updatedApps,
			})
	}
}

// Set the starting timer to make sure the application will not get stuck in a starting state too long.
// This prevents an app from not progressing to Running when it only has 1 allocation.
// Called when entering the Starting state by the state machine.
func (sa *SchedulingApplication) setStartingTimer() {
	sa.Lock()
	defer sa.Unlock()

	log.Logger().Debug("Application Starting state timer initiated",
		zap.String("appID", sa.ApplicationID),
		zap.Duration("timeout", startingTimeout))
	sa.stateTimer = time.AfterFunc(startingTimeout, sa.timeOutStarting)
}

// Clear the starting timer. If the application has progressed out of the starting state we need to stop the
// timer and clean up.
// Called when leaving the Starting state by the state machine.
func (sa *SchedulingApplication) clearStartingTimer() {
	sa.Lock()
	defer sa.Unlock()

	sa.stateTimer.Stop()
	sa.stateTimer = nil
}

// In case of state aware scheduling we do not want to get stuck in starting as we might have an application that only
// requires one allocation or is really slow asking for more than the first one.
// This will progress the state of the application from Starting to Running
func (sa *SchedulingApplication) timeOutStarting() {
	// make sure we are still in the right state
	// we could have been killed or something might have happened while waiting for a lock
	if sa.IsStarting() {
		log.Logger().Warn("Application in starting state timed out: auto progress",
			zap.String("applicationID", sa.ApplicationID),
			zap.String("state", sa.stateMachine.Current()))

		//nolint: errcheck
		_ = sa.HandleApplicationEvent(RunApplication)
	}
}

// Return the total allocated resources for the application.
func (sa *SchedulingApplication) GetAllocatedResource() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()

	return sa.allocated.Clone()
}

// Set the leaf queue the application runs in. Update the queue name also to match as this might be different from the
// queue that was given when submitting the application.
func (sa *SchedulingApplication) SetQueue(leaf *SchedulingQueue) {
	sa.Lock()
	defer sa.Unlock()

	sa.queue = leaf
	sa.QueueName = leaf.QueuePath
}

// Add a new allocation to the application
func (sa *SchedulingApplication) addAllocation(alloc *schedulingAllocation) {
	// progress the state based on where we are, we should never fail in this case
	// keep track of a failure
	if err := sa.HandleApplicationEvent(RunApplication); err != nil {
		log.Logger().Error("Unexpected app state change failure while adding allocation",
			zap.String("currentState", sa.stateMachine.Current()),
			zap.Error(err))
	}
	// add the allocation
	sa.Lock()
	defer sa.Unlock()

	sa.allocations[alloc.GetUUID()] = alloc
	sa.allocated = resources.Add(sa.allocated, alloc.SchedulingAsk.AllocatedResource)
}

// Remove a specific allocation from the application.
// Return the allocation that was removed.
func (sa *SchedulingApplication) removeAllocation(uuid string) *schedulingAllocation {
	sa.Lock()
	defer sa.Unlock()

	alloc := sa.allocations[uuid]

	if alloc != nil {
		// When app has the allocation, update map, and update allocated resource of the app
		sa.allocated = resources.Sub(sa.allocated, alloc.SchedulingAsk.AllocatedResource)
		delete(sa.allocations, uuid)
		return alloc
	}

	return nil
}

// Remove all allocations from the application.
// All allocations that have been removed are returned.
func (sa *SchedulingApplication) removeAllAllocations() []*schedulingAllocation {
	sa.Lock()
	defer sa.Unlock()

	allocationsToRelease := make([]*schedulingAllocation, 0)
	for _, alloc := range sa.allocations {
		allocationsToRelease = append(allocationsToRelease, alloc)
	}
	// cleanup allocated resource for app
	sa.allocated = resources.NewResource()
	sa.allocations = make(map[string]*schedulingAllocation)

	return allocationsToRelease
}

// get a copy of the user details for the application
func (sa *SchedulingApplication) GetUser() security.UserGroup {
	sa.Lock()
	defer sa.Unlock()

	return sa.user
}

// Get a tag from the application
// Note: Tags are not case sensitive
func (sa *SchedulingApplication) GetTag(tag string) string {
	sa.Lock()
	defer sa.Unlock()

	tagVal := ""
	for key, val := range sa.tags {
		if strings.EqualFold(key, tag) {
			tagVal = val
			break
		}
	}
	return tagVal
}

func (sa *SchedulingApplication) GetQueue() *SchedulingQueue {
	sa.RLock()
	defer sa.RUnlock()

	return sa.queue
}

func (sa *SchedulingApplication) SetQueue(queue *SchedulingQueue) {
	sa.Lock()
	defer sa.Unlock()

	sa.queue = queue
}