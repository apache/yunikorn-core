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
	"math"
	"strings"
	"sync"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/events"
	"github.com/apache/incubator-yunikorn-core/pkg/handler"
	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

var (
	reservationDelay          = 2 * time.Second
	startingTimeout           = 5 * time.Minute
	completingTimeout         = 30 * time.Second
	terminatedTimeout         = 3 * 24 * time.Hour
	defaultPlaceholderTimeout = 15 * time.Minute
)

const (
	Soft string = "Soft"
	Hard string = "Hard"
)

type Application struct {
	ApplicationID  string
	Partition      string
	QueueName      string
	SubmissionTime time.Time

	// Private fields need protection
	queue                *Queue                    // queue the application is running in
	pending              *resources.Resource       // pending resources from asks for the app
	reservations         map[string]*reservation   // a map of reservations
	requests             map[string]*AllocationAsk // a map of asks
	sortedRequests       []*AllocationAsk
	user                 security.UserGroup     // owner of the application
	tags                 map[string]string      // application tags used in scheduling
	allocatedResource    *resources.Resource    // total allocated resources
	allocatedPlaceholder *resources.Resource    // total allocated placeholder resources
	allocations          map[string]*Allocation // list of all allocations
	placeholderAsk       *resources.Resource    // total placeholder request for the app (all task groups)
	stateMachine         *fsm.FSM               // application state machine
	stateTimer           *time.Timer            // timer for state time
	execTimeout          time.Duration          // execTimeout for the application run
	placeholderTimer     *time.Timer            // placeholder replace timer
	gangSchedulingStyle  string                 // gang scheduling style can be hard (after timeout we fail the application), or soft (after timeeout we schedule it as a normal application)

	rmEventHandler     handler.EventHandler
	rmID               string
	terminatedCallback func(appID string)

	sync.RWMutex
}

func NewApplication(siApp *si.AddApplicationRequest, ugi security.UserGroup, eventHandler handler.EventHandler, rmID string) *Application {
	app := &Application{
		ApplicationID:        siApp.ApplicationID,
		Partition:            siApp.PartitionName,
		QueueName:            siApp.QueueName,
		SubmissionTime:       time.Now(),
		tags:                 siApp.Tags,
		pending:              resources.NewResource(),
		allocatedResource:    resources.NewResource(),
		allocatedPlaceholder: resources.NewResource(),
		requests:             make(map[string]*AllocationAsk),
		reservations:         make(map[string]*reservation),
		allocations:          make(map[string]*Allocation),
		stateMachine:         NewAppState(),
		placeholderAsk:       resources.NewResourceFromProto(siApp.PlaceholderAsk),
	}
	placeholderTimeout := common.ConvertSITimeout(siApp.ExecutionTimeoutMilliSeconds)
	if time.Duration(0) == placeholderTimeout {
		placeholderTimeout = defaultPlaceholderTimeout
	}
	gangSchedStyle := siApp.GetGangSchedulingStyle()
	if gangSchedStyle != Soft && gangSchedStyle != Hard {
		log.Logger().Info("Unknown gang scheduling style, using soft style as default",
			zap.String("gang scheduling style", gangSchedStyle))
		gangSchedStyle = Soft
	}
	app.gangSchedulingStyle = gangSchedStyle
	app.execTimeout = placeholderTimeout
	app.user = ugi
	app.rmEventHandler = eventHandler
	app.rmID = rmID
	return app
}

func (sa *Application) String() string {
	if sa == nil {
		return "application is nil"
	}
	return fmt.Sprintf("ApplicationID: %s, Partition: %s, QueueName: %s, SubmissionTime: %x, State: %s",
		sa.ApplicationID, sa.Partition, sa.QueueName, sa.SubmissionTime, sa.stateMachine.Current())
}

func (sa *Application) SetState(state string) {
	sa.stateMachine.SetState(state)
}

// Set the reservation delay.
// Set when the cluster context is created to disable reservation.
func SetReservationDelay(delay time.Duration) {
	log.Logger().Debug("Set reservation delay",
		zap.Duration("delay", delay))
	reservationDelay = delay
}

// Return the current state or a checked specific state for the application.
// The state machine handles the locking.
func (sa *Application) CurrentState() string {
	return sa.stateMachine.Current()
}

func (sa *Application) IsStarting() bool {
	return sa.stateMachine.Is(Starting.String())
}

func (sa *Application) IsAccepted() bool {
	return sa.stateMachine.Is(Accepted.String())
}

func (sa *Application) IsNew() bool {
	return sa.stateMachine.Is(New.String())
}

func (sa *Application) IsRunning() bool {
	return sa.stateMachine.Is(Running.String())
}

func (sa *Application) IsCompleting() bool {
	return sa.stateMachine.Is(Completing.String())
}

func (sa *Application) IsCompleted() bool {
	return sa.stateMachine.Is(Completed.String())
}

func (sa *Application) IsExpired() bool {
	return sa.stateMachine.Is(Expired.String())
}

func (sa *Application) IsFailing() bool {
	return sa.stateMachine.Is(Failing.String())
}

func (sa *Application) IsFailed() bool {
	return sa.stateMachine.Is(Failed.String())
}

func (sa *Application) IsResuming() bool {
	return sa.stateMachine.Is(Resuming.String())
}

// HandleApplicationEvent handles the state event for the application.
// The state machine handles the locking.
func (sa *Application) HandleApplicationEvent(event applicationEvent) error {
	err := sa.stateMachine.Event(event.String(), sa)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() == noTransition {
		return nil
	}
	return err
}

func (sa *Application) HandleApplicationEventWithInfo(event applicationEvent, eventInfo string) error {
	err := sa.stateMachine.Event(event.String(), sa, eventInfo)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() == noTransition {
		return nil
	}
	return err
}

func (sa *Application) OnStateChange(event *fsm.Event, eventInfo string) {
	updatedApps := make([]*si.UpdatedApplication, 0)
	var message string
	if len(eventInfo) == 0 {
		message = event.Event
	} else {
		message = eventInfo
	}
	updatedApps = append(updatedApps, &si.UpdatedApplication{
		ApplicationID:            sa.ApplicationID,
		State:                    sa.stateMachine.Current(),
		StateTransitionTimestamp: time.Now().UnixNano(),
		Message:                  message,
	})

	if sa.rmEventHandler != nil {
		sa.rmEventHandler.HandleEvent(
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
func (sa *Application) setStateTimer(timeout time.Duration, currentState string, event applicationEvent) {
	log.Logger().Debug("Application state timer initiated",
		zap.String("appID", sa.ApplicationID),
		zap.String("state", sa.stateMachine.Current()),
		zap.Duration("timeout", timeout))

	sa.stateTimer = time.AfterFunc(timeout, sa.timeoutStateTimer(currentState, event))
}

func (sa *Application) timeoutStateTimer(expectedState string, event applicationEvent) func() {
	return func() {
		// make sure we are still in the right state
		// we could have been failed or something might have happened while waiting for a lock
		if expectedState == sa.stateMachine.Current() {
			log.Logger().Debug("Application state: auto progress",
				zap.String("applicationID", sa.ApplicationID),
				zap.String("state", sa.stateMachine.Current()))
			// if the app is completing, but there are placeholders left, first do the cleanup
			if sa.IsCompleting() && !resources.IsZero(sa.GetPlaceholderResource()) {
				var toRelease []*Allocation
				for _, alloc := range sa.getPlaceholderAllocations() {
					// skip over the allocations that are already marked for release
					if alloc.released {
						continue
					}
					alloc.released = true
					toRelease = append(toRelease, alloc)
				}
				sa.notifyRMAllocationReleased(sa.rmID, toRelease, si.TerminationType_TIMEOUT, "releasing placeholders on app complete")
				sa.clearStateTimer()
			} else {
				//nolint: errcheck
				_ = sa.HandleApplicationEvent(event)
			}
		}
	}
}

// Clear the starting timer. If the application has progressed out of the starting state we need to stop the
// timer and clean up.
// Called when leaving the Starting state by the state machine.
func (sa *Application) clearStateTimer() {
	if sa == nil || sa.stateTimer == nil {
		return
	}
	sa.stateTimer.Stop()
	sa.stateTimer = nil
	log.Logger().Debug("Application state timer cleared",
		zap.String("appID", sa.ApplicationID),
		zap.String("state", sa.stateMachine.Current()))
}

func (sa *Application) initPlaceholderTimer() {
	if sa.placeholderTimer != nil || !sa.IsAccepted() || sa.execTimeout <= 0 {
		return
	}
	log.Logger().Debug("Application placeholder timer initiated",
		zap.String("AppID", sa.ApplicationID),
		zap.Duration("Timeout", sa.execTimeout))
	sa.placeholderTimer = time.AfterFunc(sa.execTimeout, sa.timeoutPlaceholderProcessing)
}

func (sa *Application) clearPlaceholderTimer() {
	if sa == nil || sa.placeholderTimer == nil {
		return
	}
	sa.placeholderTimer.Stop()
	sa.placeholderTimer = nil
	log.Logger().Debug("Application placeholder timer cleared",
		zap.String("AppID", sa.ApplicationID),
		zap.Duration("Timeout", sa.execTimeout))
}

func (sa *Application) timeoutPlaceholderProcessing() {
	sa.Lock()
	defer sa.Unlock()
	switch {
	// Case 1: if all app's placeholders are allocated, only part of them gets replaced, just delete the remaining placeholders
	case (sa.IsRunning() || sa.IsStarting() || sa.IsCompleting()) && !resources.IsZero(sa.allocatedPlaceholder):
		var toRelease []*Allocation
		replacing := 0
		for _, alloc := range sa.getPlaceholderAllocations() {
			// skip over the allocations that are already marked for release, they will be replaced soon
			if alloc.released {
				replacing++
				continue
			}
			alloc.released = true
			toRelease = append(toRelease, alloc)
		}
		log.Logger().Info("Placeholder timeout, releasing placeholders",
			zap.String("AppID", sa.ApplicationID),
			zap.Int("placeholders being replaced", replacing),
			zap.Int("releasing placeholders", len(toRelease)))
		sa.notifyRMAllocationReleased(sa.rmID, toRelease, si.TerminationType_TIMEOUT, "releasing allocated placeholders on placeholder timeout")
	// Case 2: in every other case fail the application, and notify the context about the expired placeholder asks
	default:
		log.Logger().Info("Placeholder timeout, releasing asks and placeholders",
			zap.String("AppID", sa.ApplicationID),
			zap.Int("releasing placeholders", len(sa.allocations)),
			zap.Int("releasing asks", len(sa.requests)),
			zap.String("gang scheduling style", sa.gangSchedulingStyle))
		// change the status of the app to Failing. Once all the placeholders are cleaned up, if will be changed to Failed
		event := ResumeApplication
		if sa.gangSchedulingStyle == Hard {
			event = FailApplication
		}
		if err := sa.HandleApplicationEventWithInfo(event, "ResourceReservationTimeout"); err != nil {
			log.Logger().Debug("Application state change failed when placeholder timed out",
				zap.String("AppID", sa.ApplicationID),
				zap.String("currentState", sa.CurrentState()),
				zap.Error(err))
		}
		sa.notifyRMAllocationAskReleased(sa.rmID, sa.getAllRequests(), si.TerminationType_TIMEOUT, "releasing placeholders asks on placeholder timeout")
		sa.removeAsksInternal("")
		// all allocations are placeholders but GetAllAllocations is locked and cannot be used
		sa.notifyRMAllocationReleased(sa.rmID, sa.getPlaceholderAllocations(), si.TerminationType_TIMEOUT, "releasing allocated placeholders on placeholder timeout")
	}
	sa.clearPlaceholderTimer()
}

// Return an array of all reservation keys for the app.
// This will return an empty array if there are no reservations.
// Visible for tests
func (sa *Application) GetReservations() []string {
	sa.RLock()
	defer sa.RUnlock()
	keys := make([]string, 0)
	for key := range sa.reservations {
		keys = append(keys, key)
	}
	return keys
}

// Return the allocation ask for the key, nil if not found
func (sa *Application) GetAllocationAsk(allocationKey string) *AllocationAsk {
	sa.RLock()
	defer sa.RUnlock()
	return sa.requests[allocationKey]
}

// Return the allocated resources for this application
func (sa *Application) GetAllocatedResource() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return sa.allocatedResource.Clone()
}

// Return the allocated placeholder resources for this application
func (sa *Application) GetPlaceholderResource() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return sa.allocatedPlaceholder.Clone()
}

// Return the total placeholder ask for this application
// Is only set on app creation and used when app is added to a queue
func (sa *Application) GetPlaceholderAsk() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return sa.placeholderAsk
}

// Return the pending resources for this application
func (sa *Application) GetPendingResource() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return sa.pending
}

// Remove one or more allocation asks from this application.
// This also removes any reservations that are linked to the ask.
// The return value is the number of reservations released
func (sa *Application) RemoveAllocationAsk(allocKey string) int {
	sa.Lock()
	defer sa.Unlock()
	return sa.removeAsksInternal(allocKey)
}

// unlocked version of the allocation ask removal
func (sa *Application) removeAsksInternal(allocKey string) int {
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
			sa.queue.UnReserve(sa.ApplicationID, releases)
			toRelease += releases
		}
		// Cleanup total pending resource
		deltaPendingResource = sa.pending
		sa.pending = resources.NewResource()
		sa.requests = make(map[string]*AllocationAsk)
	} else {
		// cleanup the reservation for this allocation
		for _, key := range sa.GetAskReservations(allocKey) {
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
			sa.queue.UnReserve(sa.ApplicationID, releases)
			toRelease += releases
		}
		if ask := sa.requests[allocKey]; ask != nil {
			deltaPendingResource = resources.MultiplyBy(ask.AllocatedResource, float64(ask.GetPendingAskRepeat()))
			sa.pending = resources.Sub(sa.pending, deltaPendingResource)
			delete(sa.requests, allocKey)
		}
	}
	// clean up the queue pending resources
	sa.queue.decPendingResource(deltaPendingResource)
	// Check if we need to change state based on the ask removal:
	// 1) if pending is zero (no more asks left)
	// 2) if confirmed allocations is zero (no real tasks running)
	// Change the state to completing.
	// When the resource trackers are zero we should not expect anything to come in later.
	if resources.IsZero(sa.pending) && resources.IsZero(sa.allocatedResource) && !sa.IsFailing() && !sa.IsCompleting() {
		if err := sa.HandleApplicationEvent(CompleteApplication); err != nil {
			log.Logger().Warn("Application state not changed to Completing while updating ask(s)",
				zap.String("currentState", sa.CurrentState()),
				zap.Error(err))
		}
	}

	log.Logger().Info("Ask removed successfully from application",
		zap.String("appID", sa.ApplicationID),
		zap.String("ask", allocKey),
		zap.String("pendingDelta", deltaPendingResource.String()))

	return toRelease
}

// Add an allocation ask to this application
// If the ask already exist update the existing info
func (sa *Application) AddAllocationAsk(ask *AllocationAsk) error {
	sa.Lock()
	defer sa.Unlock()
	if ask == nil {
		return fmt.Errorf("ask cannot be nil when added to app %s", sa.ApplicationID)
	}
	if ask.GetPendingAskRepeat() == 0 || resources.IsZero(ask.AllocatedResource) {
		return fmt.Errorf("invalid ask added to app %s: %v", sa.ApplicationID, ask)
	}
	ask.setQueue(sa.queue.QueuePath)
	delta := resources.Multiply(ask.AllocatedResource, int64(ask.GetPendingAskRepeat()))

	var oldAskResource *resources.Resource = nil
	if oldAsk := sa.requests[ask.AllocationKey]; oldAsk != nil {
		oldAskResource = resources.Multiply(oldAsk.AllocatedResource, int64(oldAsk.GetPendingAskRepeat()))
	}

	// Check if we need to change state based on the ask added, there are two cases:
	// 1) first ask added on a new app: state is New
	// 2) all asks and allocation have been removed: state is Completing
	// Move the state and get it scheduling (again)
	if sa.stateMachine.Is(New.String()) || sa.stateMachine.Is(Completing.String()) {
		if err := sa.HandleApplicationEvent(RunApplication); err != nil {
			log.Logger().Debug("Application state change failed while adding new ask",
				zap.String("currentState", sa.CurrentState()),
				zap.Error(err))
		}
	}
	sa.requests[ask.AllocationKey] = ask

	// Update total pending resource
	delta.SubFrom(oldAskResource)
	sa.pending = resources.Add(sa.pending, delta)
	sa.queue.incPendingResource(delta)

	log.Logger().Info("Ask added successfully to application",
		zap.String("appID", sa.ApplicationID),
		zap.String("ask", ask.AllocationKey),
		zap.Bool("placeholder", ask.placeholder),
		zap.String("pendingDelta", delta.String()))

	return nil
}

// Add the ask when a node allocation is recovered. Maintaining the rule that an Allocation always has a
// link to an AllocationAsk.
// Safeguarded against a nil but the recovery generates the ask and should never be nil.
func (sa *Application) RecoverAllocationAsk(ask *AllocationAsk) {
	sa.Lock()
	defer sa.Unlock()
	if ask == nil {
		return
	}
	ask.setQueue(sa.queue.QueuePath)
	sa.requests[ask.AllocationKey] = ask
	// progress the application from New to Accepted.
	if sa.IsNew() {
		if err := sa.HandleApplicationEvent(RunApplication); err != nil {
			log.Logger().Debug("Application state change failed while recovering allocation ask",
				zap.Error(err))
		}
	}
}

func (sa *Application) updateAskRepeat(allocKey string, delta int32) (*resources.Resource, error) {
	sa.Lock()
	defer sa.Unlock()
	if ask := sa.requests[allocKey]; ask != nil {
		return sa.updateAskRepeatInternal(ask, delta)
	}
	return nil, fmt.Errorf("failed to locate ask with key %s", allocKey)
}

func (sa *Application) updateAskRepeatInternal(ask *AllocationAsk, delta int32) (*resources.Resource, error) {
	// updating with delta does error checking internally
	if !ask.updatePendingAskRepeat(delta) {
		return nil, fmt.Errorf("ask repaeat not updated resulting repeat less than zero for ask %s on app %s", ask.AllocationKey, sa.ApplicationID)
	}

	deltaPendingResource := resources.Multiply(ask.AllocatedResource, int64(delta))
	sa.pending = resources.Add(sa.pending, deltaPendingResource)
	// update the pending of the queue with the same delta
	sa.queue.incPendingResource(deltaPendingResource)

	return deltaPendingResource, nil
}

// Return if the application has any reservations.
func (sa *Application) hasReserved() bool {
	sa.RLock()
	defer sa.RUnlock()
	return len(sa.reservations) > 0
}

// Return if the application has the node reserved.
// An empty nodeID is never reserved.
func (sa *Application) IsReservedOnNode(nodeID string) bool {
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
func (sa *Application) Reserve(node *Node, ask *AllocationAsk) error {
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
	allocKey := ask.AllocationKey
	if sa.requests[allocKey] == nil {
		log.Logger().Debug("ask is not registered to this app",
			zap.String("app", sa.ApplicationID),
			zap.String("allocKey", allocKey))
		return fmt.Errorf("reservation creation failed ask %s not found on appID %s", allocKey, sa.ApplicationID)
	}
	if !sa.canAskReserve(ask) {
		return fmt.Errorf("reservation of ask exceeds pending repeat, pending ask repeat %d", ask.GetPendingAskRepeat())
	}
	// check if we can reserve the node before reserving on the app
	if err := node.Reserve(sa, ask); err != nil {
		return err
	}
	sa.reservations[nodeReservation.getKey()] = nodeReservation
	log.Logger().Info("reservation added successfully",
		zap.String("app", sa.ApplicationID),
		zap.String("node", node.NodeID),
		zap.String("ask", ask.AllocationKey))
	return nil
}

// UnReserve the application for this node and ask combination.
// This first removes the reservation from the node.
// If the reservation does not exist it returns 0 for reservations removed, if the reservation is removed it returns 1.
// The error is set if the reservation key cannot be removed from the app or node.
func (sa *Application) UnReserve(node *Node, ask *AllocationAsk) (int, error) {
	sa.Lock()
	defer sa.Unlock()
	return sa.unReserveInternal(node, ask)
}

// Unlocked version for UnReserve that really does the work.
// Must only be called while holding the application lock.
func (sa *Application) unReserveInternal(node *Node, ask *AllocationAsk) (int, error) {
	resKey := reservationKey(node, nil, ask)
	if resKey == "" {
		log.Logger().Debug("unreserve reservation key create failed unexpectedly",
			zap.String("appID", sa.ApplicationID),
			zap.String("node", node.String()),
			zap.String("ask", ask.String()))
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
				zap.String("ask", ask.AllocationKey))
		}
		delete(sa.reservations, resKey)
		log.Logger().Info("reservation removed successfully", zap.String("node", node.NodeID),
			zap.String("app", ask.ApplicationID), zap.String("ask", ask.AllocationKey))
		return 1, nil
	}
	// reservation was not found
	log.Logger().Info("reservation not found while removing from app",
		zap.String("appID", sa.ApplicationID),
		zap.String("nodeID", node.NodeID),
		zap.String("ask", ask.AllocationKey),
		zap.Int("nodeReservationsRemoved", num))
	return 0, nil
}

// Return the allocation reservations on any node.
// The returned array is 0 or more keys into the reservations map.
// No locking must be called while holding the lock
func (sa *Application) GetAskReservations(allocKey string) []string {
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
func (sa *Application) canAskReserve(ask *AllocationAsk) bool {
	allocKey := ask.AllocationKey
	pending := int(ask.GetPendingAskRepeat())
	resNumber := sa.GetAskReservations(allocKey)
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
func (sa *Application) sortRequests(ascending bool) {
	sa.sortedRequests = nil
	for _, request := range sa.requests {
		if request.GetPendingAskRepeat() == 0 {
			continue
		}
		sa.sortedRequests = append(sa.sortedRequests, request)
	}
	// we might not have any requests
	if len(sa.sortedRequests) > 0 {
		sortAskByPriority(sa.sortedRequests, ascending)
	}
}

func (sa *Application) getOutstandingRequests(headRoom *resources.Resource, total *[]*AllocationAsk) {
	sa.RLock()
	defer sa.RUnlock()

	// make sure the request are sorted
	sa.sortRequests(false)
	for _, request := range sa.sortedRequests {
		// ignore nil checks resource function calls are nil safe
		if headRoom.FitInMaxUndef(request.AllocatedResource) {
			// if headroom is still enough for the resources
			*total = append(*total, request)
			headRoom.SubFrom(request.AllocatedResource)
		}
	}
}

// Try a regular allocation of the pending requests
// This includes placeholders
func (sa *Application) tryAllocate(headRoom *resources.Resource, nodeIterator func() interfaces.NodeIterator) *Allocation {
	sa.Lock()
	defer sa.Unlock()
	// make sure the request are sorted
	sa.sortRequests(false)
	// get all the requests from the app sorted in order
	for _, request := range sa.sortedRequests {
		// if request is not a placeholder but part of a task group and there are still placeholders allocated we do
		// them on their own.
		// the iterator might not have the node we need as it could be reserved
		if !request.placeholder && request.taskGroupName != "" && !resources.IsZero(sa.allocatedPlaceholder) {
			continue
		}
		// resource must fit in headroom otherwise skip the request
		if !headRoom.FitInMaxUndef(request.AllocatedResource) {
			// post scheduling events via the event plugin
			if eventCache := events.GetEventCache(); eventCache != nil {
				message := fmt.Sprintf("Application %s does not fit into %s queue", request.ApplicationID, sa.QueueName)
				if event, err := events.CreateRequestEventRecord(request.AllocationKey, request.ApplicationID, "InsufficientQueueResources", message); err != nil {
					log.Logger().Warn("Event creation failed",
						zap.String("event message", message),
						zap.Error(err))
				} else {
					eventCache.AddEvent(event)
				}
			}
			continue
		}
		iterator := nodeIterator()
		if iterator != nil {
			alloc := sa.tryNodes(request, iterator)
			// have a candidate return it
			if alloc != nil {
				return alloc
			}
		}
	}
	// no requests fit, skip to next app
	return nil
}

// Try to replace a placeholder with a real allocation
func (sa *Application) tryPlaceholderAllocate(nodeIterator func() interfaces.NodeIterator, getnode func(string) *Node) *Allocation {
	sa.Lock()
	defer sa.Unlock()
	// nothing to do if we have no placeholders allocated
	if resources.IsZero(sa.allocatedPlaceholder) {
		return nil
	}
	// make sure the request are sorted
	sa.sortRequests(false)
	// keep the first fits for later
	var phFit *Allocation
	var reqFit *AllocationAsk
	// get all the requests from the app sorted in order
	for _, request := range sa.sortedRequests {
		// skip placeholders they follow standard allocation
		// this should also be part of a task group just make sure it is
		if request.placeholder || request.taskGroupName == "" {
			continue
		}
		// walk over the placeholders, allow for processing all as we can have multiple task groups
		phAllocs := sa.getPlaceholderAllocations()
		for _, ph := range phAllocs {
			// we could have already released this placeholder and are waiting for the shim to confirm
			// and check that we have the correct task group before trying to swap
			if ph.released || request.taskGroupName != ph.taskGroupName {
				continue
			}
			if phFit == nil && reqFit == nil {
				phFit = ph
				reqFit = request
			}
			node := getnode(ph.NodeID)
			// got the node run same checks as for reservation (all but fits)
			// resource usage should not change anyway between placeholder and real one
			if node != nil && node.preReserveConditions(request.AllocationKey) {
				alloc := NewAllocation(common.GetNewUUID(), node.NodeID, request)
				// double link to make it easier to find
				// alloc (the real one) releases points to the placeholder in the releases list
				alloc.Releases = []*Allocation{ph}
				// placeholder point to the real one in the releases list
				ph.Releases = []*Allocation{alloc}
				alloc.Result = Replaced
				// mark placeholder as released
				ph.released = true
				_, err := sa.updateAskRepeatInternal(request, -1)
				if err != nil {
					log.Logger().Warn("ask repeat update failed unexpectedly",
						zap.Error(err))
				}
				// Is real allocation matches with placeholder resource?
				if !resources.Equals(alloc.AllocatedResource, ph.AllocatedResource) {
					log.Logger().Warn("Real allocation is not matching with placeholder allocation", zap.String("Real alloc: ", alloc.AllocationKey), zap.String("Real alloc resource: ", alloc.AllocatedResource.DAOString()), zap.String("Placeholder alloc: ", ph.AllocationKey), zap.String("Placeholder alloc resource: ", ph.AllocatedResource.DAOString()))
					// post scheduling events via the event plugin
					if eventCache := events.GetEventCache(); eventCache != nil {
						message := fmt.Sprintf("Real Pod %s allocation %s is not matching with placeholder %s allocation %s in application %s", request.AllocationKey, request.AllocatedResource.DAOString(), ph.AllocationKey, ph.AllocatedResource.DAOString(), request.ApplicationID)
						if event, err := events.CreateRequestEventRecord(request.AllocationKey, request.ApplicationID, "Resource Allocation Mismatch between real and placeholder", message); err != nil {
							log.Logger().Warn("Event creation failed",
								zap.String("event message", message),
								zap.Error(err))
						} else {
							eventCache.AddEvent(event)
						}
					}
				}
				return alloc
			}
		}
	}
	// cannot allocate if the iterator is not giving us any schedulable nodes
	iterator := nodeIterator()
	if iterator == nil {
		return nil
	}
	// we checked all placeholders and asks nothing worked as yet
	// pick the first fit and try all nodes if that fails give up
	if phFit != nil && reqFit != nil {
		for iterator.HasNext() {
			node, ok := iterator.Next().(*Node)
			if !ok {
				log.Logger().Warn("Node iterator failed to return a node")
				return nil
			}
			// placeholder is not daemonSet pod, so set ignoreUnschedulable node to false
			if err := node.preAllocateCheck(reqFit.AllocatedResource, reservationKey(nil, sa, reqFit), false, false); err != nil {
				continue
			}
			// skip the node if conditions can not be satisfied
			if !node.preAllocateConditions(reqFit.AllocationKey) {
				continue
			}
			// allocation worked: on a non placeholder node update result and return
			alloc := NewAllocation(common.GetNewUUID(), node.NodeID, reqFit)
			// double link to make it easier to find
			// alloc (the real one) releases points to the placeholder in the releases list
			alloc.Releases = []*Allocation{phFit}
			// placeholder point to the real one in the releases list
			phFit.Releases = []*Allocation{alloc}
			alloc.Result = Replaced
			// mark placeholder as released
			phFit.released = true
			// update just the node to make sure we keep its spot
			// no queue update as we're releasing the placeholder and are just temp over the size
			if !node.AddAllocation(alloc) {
				log.Logger().Debug("Node update failed unexpectedly",
					zap.String("applicationID", sa.ApplicationID),
					zap.String("ask", reqFit.String()),
					zap.String("placeholder", phFit.String()))
				return nil
			}
			_, err := sa.updateAskRepeatInternal(reqFit, -1)
			if err != nil {
				log.Logger().Warn("ask repeat update failed unexpectedly",
					zap.Error(err))
			}
			return alloc
		}
	}
	// still nothing worked give up and hope the next round works
	return nil
}

// Try a reserved allocation of an outstanding reservation
func (sa *Application) tryReservedAllocate(headRoom *resources.Resource, nodeIterator func() interfaces.NodeIterator) *Allocation {
	sa.Lock()
	defer sa.Unlock()
	// process all outstanding reservations and pick the first one that fits
	for _, reserve := range sa.reservations {
		ask := sa.requests[reserve.askKey]
		// sanity check and cleanup if needed
		if ask == nil || ask.GetPendingAskRepeat() == 0 {
			var unreserveAsk *AllocationAsk
			// if the ask was not found we need to construct one to unreserve
			if ask == nil {
				unreserveAsk = &AllocationAsk{
					AllocationKey: reserve.askKey,
					ApplicationID: sa.ApplicationID,
				}
			} else {
				unreserveAsk = ask
			}
			// remove the reservation as this should not be reserved
			alloc := newReservedAllocation(Unreserved, reserve.nodeID, unreserveAsk)
			return alloc
		}
		// check if this fits in the queue's head room
		if !headRoom.FitInMaxUndef(ask.AllocatedResource) {
			continue
		}
		// check allocation possibility
		alloc := sa.tryNode(reserve.node, ask)
		// allocation worked fix the result and return
		if alloc != nil {
			alloc.Result = AllocatedReserved
			return alloc
		}
	}
	// lets try this on all other nodes
	for _, reserve := range sa.reservations {
		iterator := nodeIterator()
		if iterator != nil {
			alloc := sa.tryNodesNoReserve(reserve.ask, iterator, reserve.nodeID)
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
func (sa *Application) tryNodesNoReserve(ask *AllocationAsk, iterator interfaces.NodeIterator, reservedNode string) *Allocation {
	for iterator.HasNext() {
		node, ok := iterator.Next().(*Node)
		if !ok {
			log.Logger().Warn("Node iterator failed to return a node")
			return nil
		}
		// skip over the node if the resource does not fit the node or this is the reserved node.
		if !node.FitInNode(ask.AllocatedResource) || node.NodeID == reservedNode {
			continue
		}
		alloc := sa.tryNode(node, ask)
		// allocation worked: update result and return
		if alloc != nil {
			alloc.ReservedNodeID = reservedNode
			alloc.Result = AllocatedReserved
			return alloc
		}
	}
	// ask does not fit, skip to next ask
	return nil
}

// Try all the nodes for a request. The result is an allocation or reservation of a node.
// New allocations can only be reserved after a delay.
func (sa *Application) tryNodes(ask *AllocationAsk, iterator interfaces.NodeIterator) *Allocation {
	var nodeToReserve *Node
	scoreReserved := math.Inf(1)
	// check if the ask is reserved or not
	allocKey := ask.AllocationKey
	reservedAsks := sa.GetAskReservations(allocKey)
	allowReserve := len(reservedAsks) < int(ask.pendingRepeatAsk)
	for iterator.HasNext() {
		node, ok := iterator.Next().(*Node)
		if !ok {
			log.Logger().Warn("Node iterator failed to return a node")
			return nil
		}
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
			if _, ok = sa.reservations[reservationKey(node, nil, ask)]; ok {
				log.Logger().Debug("allocate found reserved ask during non reserved allocate",
					zap.String("appID", sa.ApplicationID),
					zap.String("nodeID", node.NodeID),
					zap.String("allocationKey", allocKey))
				alloc.Result = AllocatedReserved
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
				alloc.Result = AllocatedReserved
				alloc.ReservedNodeID = nodeID
				return alloc
			}
			// nothing reserved just return this as a normal alloc
			return alloc
		}
		// nothing allocated should we look at a reservation?
		// TODO make this smarter a hardcoded delay is not the right thing
		askAge := time.Since(ask.GetCreateTime())
		if allowReserve && askAge > reservationDelay {
			log.Logger().Debug("app reservation check",
				zap.String("allocationKey", allocKey),
				zap.Time("createTime", ask.GetCreateTime()),
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
	if nodeToReserve != nil && !nodeToReserve.IsReserved() {
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
		// return reservation allocation and mark it as a reservation
		alloc := newReservedAllocation(Reserved, nodeToReserve.NodeID, ask)
		return alloc
	}
	// ask does not fit, skip to next ask
	return nil
}

// Try allocating on one specific node
func (sa *Application) tryNode(node *Node, ask *AllocationAsk) *Allocation {
	allocKey := ask.AllocationKey
	toAllocate := ask.AllocatedResource
	// create the key for the reservation
	ignore := common.GetIgnoreUnschedulable(ask.Tags)
	if err := node.preAllocateCheck(toAllocate, reservationKey(nil, sa, ask), false, ignore); err != nil {
		// skip schedule onto node
		return nil
	}
	// skip the node if conditions can not be satisfied
	if !node.preAllocateConditions(allocKey) {
		return nil
	}
	// everything OK really allocate
	alloc := NewAllocation(common.GetNewUUID(), node.NodeID, ask)
	if node.AddAllocation(alloc) {
		if err := sa.queue.IncAllocatedResource(alloc.AllocatedResource, false); err != nil {
			log.Logger().Warn("queue update failed unexpectedly",
				zap.Error(err))
			// revert the node update
			node.RemoveAllocation(alloc.UUID)
			return nil
		}
		// mark this ask as allocated by lowering the repeat
		_, err := sa.updateAskRepeatInternal(ask, -1)
		if err != nil {
			log.Logger().Warn("ask repeat update failed unexpectedly",
				zap.Error(err))
		}
		// all is OK, last update for the app
		sa.addAllocationInternal(alloc)
		// return allocation
		return alloc
	}
	return nil
}

func (sa *Application) GetQueueName() string {
	sa.RLock()
	defer sa.RUnlock()
	return sa.queue.QueuePath
}

func (sa *Application) GetQueue() *Queue {
	sa.RLock()
	defer sa.RUnlock()
	return sa.queue
}

// Set the leaf queue the application runs in. The queue will be created when the app is added to the partition.
// The queue name is set to what the placement rule returned.
func (sa *Application) SetQueueName(queuePath string) {
	sa.Lock()
	defer sa.Unlock()
	sa.QueueName = queuePath
}

// Set the leaf queue the application runs in.
func (sa *Application) SetQueue(queue *Queue) {
	sa.Lock()
	defer sa.Unlock()
	sa.QueueName = queue.QueuePath
	sa.queue = queue
}

// remove the leaf queue the application runs in, used when completing the app
func (sa *Application) UnSetQueue() {
	if sa.queue != nil {
		sa.queue.RemoveApplication(sa)
	}
	sa.Lock()
	defer sa.Unlock()
	sa.queue = nil
}

// get a copy of all allocations of the application
func (sa *Application) GetAllAllocations() []*Allocation {
	sa.RLock()
	defer sa.RUnlock()

	var allocations []*Allocation
	for _, alloc := range sa.allocations {
		allocations = append(allocations, alloc)
	}
	return allocations
}

// get a copy of all placeholder allocations of the application
// No locking must be called while holding the lock
func (sa *Application) getPlaceholderAllocations() []*Allocation {
	var allocations []*Allocation
	if sa == nil || len(sa.allocations) == 0 {
		return allocations
	}
	for _, alloc := range sa.allocations {
		if alloc.placeholder {
			allocations = append(allocations, alloc)
		}
	}
	return allocations
}

func (sa *Application) getAllRequests() []*AllocationAsk {
	var requests []*AllocationAsk
	for _, req := range sa.requests {
		requests = append(requests, req)
	}
	return requests
}

// Add a new Allocation to the application
func (sa *Application) AddAllocation(info *Allocation) {
	sa.Lock()
	defer sa.Unlock()
	sa.addAllocationInternal(info)
}

// Add the Allocation to the application
// No locking must be called while holding the lock
func (sa *Application) addAllocationInternal(info *Allocation) {
	// placeholder allocations do not progress the state of the app and are tracked in a separate total
	if info.placeholder {
		// when we have the first placeholder allocation start the placeholder timer.
		// It will start to use the resources only after the first allocation, so we will count the time from this point.
		// Also this is the first stable point on the placeholder handling, what is easy to explain and troubleshoot
		// If we would start it when we just try to allocate, that is something very unstable, and we don't really have any
		// impact on what is happening until this point
		if resources.IsZero(sa.allocatedPlaceholder) {
			sa.initPlaceholderTimer()
		}
		sa.allocatedPlaceholder = resources.Add(sa.allocatedPlaceholder, info.AllocatedResource)
		// If there are no more placeholder to allocate we should move state
		if resources.Equals(sa.allocatedPlaceholder, sa.placeholderAsk) {
			if err := sa.HandleApplicationEvent(RunApplication); err != nil {
				log.Logger().Error("Unexpected app state change failure while adding allocation",
					zap.String("currentState", sa.stateMachine.Current()),
					zap.Error(err))
			}
		}
	} else {
		// skip the state change if this is the first replacement allocation as we have done that change
		// already when the last placeholder was allocated
		if info.Result != Replaced || !resources.IsZero(sa.allocatedResource) {
			// progress the state based on where we are, we should never fail in this case
			// keep track of a failure in log.
			if err := sa.HandleApplicationEvent(RunApplication); err != nil {
				log.Logger().Error("Unexpected app state change failure while adding allocation",
					zap.String("currentState", sa.stateMachine.Current()),
					zap.Error(err))
			}
		}
		sa.allocatedResource = resources.Add(sa.allocatedResource, info.AllocatedResource)
	}
	sa.allocations[info.UUID] = info
}

func (sa *Application) ReplaceAllocation(uuid string) *Allocation {
	sa.Lock()
	defer sa.Unlock()
	// remove the placeholder that was just confirmed by the shim
	ph := sa.removeAllocationInternal(uuid)
	// this has already been replaced or it is a duplicate message from the shim
	if ph == nil || len(ph.Releases) == 0 {
		log.Logger().Debug("Unexpected placeholder released",
			zap.String("applicationID", sa.ApplicationID),
			zap.String("placeholder", ph.String()))
		return nil
	}
	if len(ph.Releases) > 1 {
		log.Logger().Error("Unexpected release number, placeholder released, only 1 real allocations processed",
			zap.String("applicationID", sa.ApplicationID),
			zap.String("placeholderID", uuid),
			zap.Int("releases", len(ph.Releases)))
	}
	// update the replacing allocation
	// we double linked the real and placeholder allocation
	// ph is the placeholder, the releases entry points to the real one
	alloc := ph.Releases[0]
	sa.addAllocationInternal(alloc)
	// order is important: clean up the allocation after adding it to the app
	// we need the original Replaced allocation result.
	alloc.Releases = nil
	alloc.Result = Allocated
	return ph
}

// Remove the Allocation from the application.
// Return the allocation that was removed or nil if not found.
func (sa *Application) RemoveAllocation(uuid string) *Allocation {
	sa.Lock()
	defer sa.Unlock()
	return sa.removeAllocationInternal(uuid)
}

// Remove the Allocation from the application
// No locking must be called while holding the lock
func (sa *Application) removeAllocationInternal(uuid string) *Allocation {
	alloc := sa.allocations[uuid]

	// When app has the allocation, update map, and update allocated resource of the app
	if alloc == nil {
		return nil
	}
	// update correct allocation tracker
	if alloc.placeholder {
		sa.allocatedPlaceholder = resources.Sub(sa.allocatedPlaceholder, alloc.AllocatedResource)
		// if all the placeholders are replaced, clear the placeholder timer
		if resources.IsZero(sa.allocatedPlaceholder) {
			sa.clearPlaceholderTimer()
			if (sa.IsCompleting() && sa.stateTimer == nil) || sa.IsFailing() || sa.IsResuming() {
				event := CompleteApplication
				if sa.IsFailing() {
					event = FailApplication
				}
				if sa.IsResuming() {
					event = RunApplication
				}
				if err := sa.HandleApplicationEvent(event); err != nil {
					log.Logger().Warn("Application state not changed while removing a placeholder allocation",
						zap.String("currentState", sa.CurrentState()),
						zap.String("event", event.String()),
						zap.Error(err))
				}
			}
		}
	} else {
		sa.allocatedResource = resources.Sub(sa.allocatedResource, alloc.AllocatedResource)
		// When the resource trackers are zero we should not expect anything to come in later.
		if resources.IsZero(sa.pending) && resources.IsZero(sa.allocatedResource) {
			if err := sa.HandleApplicationEvent(CompleteApplication); err != nil {
				log.Logger().Warn("Application state not changed to Waiting while removing an allocation",
					zap.String("currentState", sa.CurrentState()),
					zap.Error(err))
			}
		}
	}
	delete(sa.allocations, uuid)
	return alloc
}

// Remove all allocations from the application.
// All allocations that have been removed are returned.
func (sa *Application) RemoveAllAllocations() []*Allocation {
	sa.Lock()
	defer sa.Unlock()

	allocationsToRelease := make([]*Allocation, 0)
	for _, alloc := range sa.allocations {
		allocationsToRelease = append(allocationsToRelease, alloc)
	}
	// cleanup allocated resource for app (placeholders and normal)
	sa.allocatedResource = resources.NewResource()
	sa.allocatedPlaceholder = resources.NewResource()
	sa.allocations = make(map[string]*Allocation)
	// When the resource trackers are zero we should not expect anything to come in later.
	if resources.IsZero(sa.pending) {
		if err := sa.HandleApplicationEvent(CompleteApplication); err != nil {
			log.Logger().Warn("Application state not changed to Waiting while removing all allocations",
				zap.String("currentState", sa.CurrentState()),
				zap.Error(err))
		}
	}
	sa.clearPlaceholderTimer()
	sa.clearStateTimer()
	return allocationsToRelease
}

// get a copy of the user details for the application
func (sa *Application) GetUser() security.UserGroup {
	sa.RLock()
	defer sa.RUnlock()

	return sa.user
}

// Get a tag from the application
// Note: Tags are not case sensitive
func (sa *Application) GetTag(tag string) string {
	sa.RLock()
	defer sa.RUnlock()

	tagVal := ""
	for key, val := range sa.tags {
		if strings.EqualFold(key, tag) {
			tagVal = val
			break
		}
	}
	return tagVal
}

func (sa *Application) SetTerminatedCallback(callback func(appID string)) {
	sa.Lock()
	defer sa.Unlock()
	sa.terminatedCallback = callback
}

func (sa *Application) executeTerminatedCallback() {
	if sa.terminatedCallback != nil {
		go sa.terminatedCallback(sa.ApplicationID)
	}
}

func (sa *Application) notifyRMAllocationReleased(rmID string, released []*Allocation, terminationType si.TerminationType, message string) {
	// only generate event if needed
	if len(released) == 0 {
		return
	}
	releaseEvent := &rmevent.RMReleaseAllocationEvent{
		ReleasedAllocations: make([]*si.AllocationRelease, 0),
		RmID:                rmID,
	}
	for _, alloc := range released {
		releaseEvent.ReleasedAllocations = append(releaseEvent.ReleasedAllocations, &si.AllocationRelease{
			ApplicationID:   alloc.ApplicationID,
			PartitionName:   alloc.PartitionName,
			UUID:            alloc.UUID,
			TerminationType: terminationType,
			Message:         message,
		})
	}
	sa.rmEventHandler.HandleEvent(releaseEvent)
}

func (sa *Application) notifyRMAllocationAskReleased(rmID string, released []*AllocationAsk, terminationType si.TerminationType, message string) {
	// only generate event if needed
	if len(released) == 0 {
		return
	}
	releaseEvent := &rmevent.RMReleaseAllocationAskEvent{
		ReleasedAllocationAsks: make([]*si.AllocationAskRelease, 0),
		RmID:                   rmID,
	}
	for _, alloc := range released {
		releaseEvent.ReleasedAllocationAsks = append(releaseEvent.ReleasedAllocationAsks, &si.AllocationAskRelease{
			ApplicationID:   alloc.ApplicationID,
			PartitionName:   alloc.PartitionName,
			Allocationkey:   alloc.AllocationKey,
			TerminationType: terminationType,
			Message:         message,
		})
	}
	sa.rmEventHandler.HandleEvent(releaseEvent)
}
