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
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/handler"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
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

type PlaceholderData struct {
	TaskGroupName string
	Count         int64
	MinResource   *resources.Resource
	Replaced      int64
	TimedOut      int64
}

type StateLogEntry struct {
	Time             time.Time
	ApplicationState string
}

type Application struct {
	ApplicationID  string
	Partition      string
	SubmissionTime time.Time

	// Private fields need protection
	queuePath            string
	queue                *Queue                    // queue the application is running in
	pending              *resources.Resource       // pending resources from asks for the app
	reservations         map[string]*reservation   // a map of reservations
	requests             map[string]*AllocationAsk // a map of asks
	sortedRequests       []*AllocationAsk
	user                 security.UserGroup          // owner of the application
	tags                 map[string]string           // application tags used in scheduling
	allocatedResource    *resources.Resource         // total allocated resources
	maxAllocatedResource *resources.Resource         // max allocated resources
	allocatedPlaceholder *resources.Resource         // total allocated placeholder resources
	allocations          map[string]*Allocation      // list of all allocations
	placeholderAsk       *resources.Resource         // total placeholder request for the app (all task groups)
	stateMachine         *fsm.FSM                    // application state machine
	stateTimer           *time.Timer                 // timer for state time
	startTimeout         time.Duration               // timeout for the application starting state
	execTimeout          time.Duration               // execTimeout for the application run
	placeholderTimer     *time.Timer                 // placeholder replace timer
	gangSchedulingStyle  string                      // gang scheduling style can be hard (after timeout we fail the application), or soft (after timeeout we schedule it as a normal application)
	finishedTime         time.Time                   // the time of finishing this application. the default value is zero time
	rejectedMessage      string                      // If the application is rejected, save the rejected message
	stateLog             []*StateLogEntry            // state log for this application
	placeholderData      map[string]*PlaceholderData // track placeholder and gang related info
	askMaxPriority       int32                       // highest priority value of outstanding asks

	rmEventHandler     handler.EventHandler
	rmID               string
	terminatedCallback func(appID string)

	sync.RWMutex
}

func NewApplication(siApp *si.AddApplicationRequest, ugi security.UserGroup, eventHandler handler.EventHandler, rmID string) *Application {
	app := &Application{
		ApplicationID:        siApp.ApplicationID,
		Partition:            siApp.PartitionName,
		SubmissionTime:       time.Now(),
		queuePath:            siApp.QueueName,
		tags:                 siApp.Tags,
		pending:              resources.NewResource(),
		allocatedResource:    resources.NewResource(),
		maxAllocatedResource: resources.NewResource(),
		allocatedPlaceholder: resources.NewResource(),
		requests:             make(map[string]*AllocationAsk),
		reservations:         make(map[string]*reservation),
		allocations:          make(map[string]*Allocation),
		stateMachine:         NewAppState(),
		placeholderAsk:       resources.NewResourceFromProto(siApp.PlaceholderAsk),
		finishedTime:         time.Time{},
		rejectedMessage:      "",
		stateLog:             make([]*StateLogEntry, 0),
		askMaxPriority:       configs.MinPriority,
	}
	placeholderTimeout := common.ConvertSITimeoutWithAdjustment(siApp, defaultPlaceholderTimeout)
	gangSchedStyle := siApp.GetGangSchedulingStyle()
	if gangSchedStyle != Soft && gangSchedStyle != Hard {
		log.Logger().Info("Unknown gang scheduling style, using soft style as default",
			zap.String("gang scheduling style", gangSchedStyle))
		gangSchedStyle = Soft
	}
	app.gangSchedulingStyle = gangSchedStyle
	app.execTimeout = placeholderTimeout
	if app.GetTag(siCommon.AppTagStateAwareDisable) != "" {
		app.startTimeout = 0 // transition immediately to Running
	} else {
		app.startTimeout = startingTimeout
	}
	app.user = ugi
	app.rmEventHandler = eventHandler
	app.rmID = rmID
	return app
}

func (sa *Application) String() string {
	if sa == nil {
		return "application is nil"
	}
	return fmt.Sprintf("applicationID: %s, Partition: %s, SubmissionTime: %x, State: %s",
		sa.ApplicationID, sa.Partition, sa.SubmissionTime, sa.stateMachine.Current())
}

func (sa *Application) SetState(state string) {
	sa.stateMachine.SetState(state)
}

func (sa *Application) recordState(appState string) {
	// lock not acquired here as it is already held during HandleApplicationEvent() / OnStateChange()
	sa.stateLog = append(sa.stateLog, &StateLogEntry{
		Time:             time.Now(),
		ApplicationState: appState,
	})
}

func (sa *Application) GetStateLog() []*StateLogEntry {
	sa.RLock()
	defer sa.RUnlock()
	return sa.stateLog
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

func (sa *Application) IsRejected() bool {
	return sa.stateMachine.Is(Rejected.String())
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
// The application lock is expected to be held.
func (sa *Application) HandleApplicationEvent(event applicationEvent) error {
	err := sa.stateMachine.Event(context.Background(), event.String(), sa)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() == noTransition {
		return nil
	}
	return err
}

// HandleApplicationEventWithInfo handles the state event for the application with associated info object.
// The application lock is expected to be held.
func (sa *Application) HandleApplicationEventWithInfo(event applicationEvent, eventInfo string) error {
	err := sa.stateMachine.Event(context.Background(), event.String(), sa, eventInfo)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() == noTransition {
		return nil
	}
	return err
}

// OnStatChange every time the application enters a new state.
// It sends an event about the state change to the shim as an application update.
// The only state that does not generate an event is Rejected.
func (sa *Application) OnStateChange(event *fsm.Event, eventInfo string) {
	sa.recordState(event.Dst)
	if event.Dst == Rejected.String() || sa.rmEventHandler == nil {
		return
	}
	var message string
	if len(eventInfo) == 0 {
		message = event.Event
	} else {
		message = eventInfo
	}
	sa.rmEventHandler.HandleEvent(
		&rmevent.RMApplicationUpdateEvent{
			RmID:                 sa.rmID,
			AcceptedApplications: make([]*si.AcceptedApplication, 0),
			RejectedApplications: make([]*si.RejectedApplication, 0),
			UpdatedApplications: []*si.UpdatedApplication{{
				ApplicationID:            sa.ApplicationID,
				State:                    sa.stateMachine.Current(),
				StateTransitionTimestamp: time.Now().UnixNano(),
				Message:                  message,
			}},
		})
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
		sa.Lock()
		defer sa.Unlock()

		// make sure we are still in the right state
		// we could have been failed or something might have happened while waiting for a lock
		if expectedState == sa.stateMachine.Current() {
			log.Logger().Debug("Application state: auto progress",
				zap.String("applicationID", sa.ApplicationID),
				zap.String("state", sa.stateMachine.Current()))
			// if the app is completing, but there are placeholders left, first do the cleanup
			if sa.IsCompleting() && !resources.IsZero(sa.allocatedPlaceholder) {
				var toRelease []*Allocation
				for _, alloc := range sa.getPlaceholderAllocations() {
					// skip over the allocations that are already marked for release
					if alloc.IsReleased() {
						continue
					}
					alloc.SetReleased(true)
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

// timeoutPlaceholderProcessing cleans up all placeholder asks and allocations that are not used after the timeout.
// If the application has started processing, Starting state or further, the application keeps on processing without
// being able to use the placeholders.
// If the application is in New or Accepted state we clean up and take followup action based on the gang scheduling
// style.
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
			if alloc.IsReleased() {
				replacing++
				continue
			}
			alloc.SetReleased(true)
			toRelease = append(toRelease, alloc)
			// mark as timeout out in the tracking data
			if _, ok := sa.placeholderData[alloc.GetTaskGroup()]; ok {
				sa.placeholderData[alloc.GetTaskGroup()].TimedOut++
			}
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
		// change the status of the app based on gang style: soft resume normal allocations, hard fail the app
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
		sa.notifyRMAllocationAskReleased(sa.rmID, sa.getAllRequestsInternal(), si.TerminationType_TIMEOUT, "releasing placeholders asks on placeholder timeout")
		sa.removeAsksInternal("")
		// all allocations are placeholders but GetAllAllocations is locked and cannot be used
		sa.notifyRMAllocationReleased(sa.rmID, sa.getPlaceholderAllocations(), si.TerminationType_TIMEOUT, "releasing allocated placeholders on placeholder timeout")
		// we are in an accepted or new state so nothing can be replaced yet: mark everything as timedout
		for _, phData := range sa.placeholderData {
			phData.TimedOut = phData.Count
		}
	}
	sa.clearPlaceholderTimer()
}

// GetReservations returns an array of all reservation keys for the application.
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

func (sa *Application) GetMaxAllocatedResource() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return sa.maxAllocatedResource.Clone()
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
		sa.askMaxPriority = configs.MinPriority
		sa.queue.UpdateApplicationPriority(sa.ApplicationID, sa.askMaxPriority)
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
			deltaPendingResource = resources.MultiplyBy(ask.GetAllocatedResource(), float64(ask.GetPendingAskRepeat()))
			sa.pending = resources.Sub(sa.pending, deltaPendingResource)
			delete(sa.requests, allocKey)
			if priority := ask.GetPriority(); priority >= sa.askMaxPriority {
				sa.updateAskMaxPriority()
			}
		}
	}
	// clean up the queue pending resources
	sa.queue.decPendingResource(deltaPendingResource)
	// Check if we need to change state based on the ask removal:
	// 1) if pending is zero (no more asks left)
	// 2) if confirmed allocations is zero (no real tasks running)
	// Change the state to completing.
	// When the resource trackers are zero we should not expect anything to come in later.
	hasPlaceHolderAllocations := len(sa.getPlaceholderAllocations()) > 0
	if resources.IsZero(sa.pending) && resources.IsZero(sa.allocatedResource) && !sa.IsFailing() && !sa.IsCompleting() && !hasPlaceHolderAllocations {
		if err := sa.HandleApplicationEvent(CompleteApplication); err != nil {
			log.Logger().Warn("Application state not changed to Completing while updating ask(s)",
				zap.String("currentState", sa.CurrentState()),
				zap.Error(err))
		}
	}

	log.Logger().Info("ask removed successfully from application",
		zap.String("appID", sa.ApplicationID),
		zap.String("ask", allocKey),
		zap.Stringer("pendingDelta", deltaPendingResource))

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
	if ask.GetPendingAskRepeat() == 0 || resources.IsZero(ask.GetAllocatedResource()) {
		return fmt.Errorf("invalid ask added to app %s: %v", sa.ApplicationID, ask)
	}
	delta := resources.Multiply(ask.GetAllocatedResource(), int64(ask.GetPendingAskRepeat()))

	var oldAskResource *resources.Resource = nil
	if oldAsk := sa.requests[ask.GetAllocationKey()]; oldAsk != nil {
		oldAskResource = resources.Multiply(oldAsk.GetAllocatedResource(), int64(oldAsk.GetPendingAskRepeat()))
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
	sa.requests[ask.GetAllocationKey()] = ask

	// update app priority
	repeat := ask.GetPendingAskRepeat()
	priority := ask.GetPriority()
	if repeat > 0 && priority > sa.askMaxPriority {
		sa.askMaxPriority = priority
		sa.queue.UpdateApplicationPriority(sa.ApplicationID, sa.askMaxPriority)
	}

	// Update total pending resource
	delta.SubFrom(oldAskResource)
	sa.pending = resources.Add(sa.pending, delta)
	sa.queue.incPendingResource(delta)

	if ask.IsPlaceholder() {
		sa.addPlaceholderData(ask)
	}

	log.Logger().Info("ask added successfully to application",
		zap.String("appID", sa.ApplicationID),
		zap.String("ask", ask.GetAllocationKey()),
		zap.Bool("placeholder", ask.IsPlaceholder()),
		zap.Stringer("pendingDelta", delta))

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
	sa.requests[ask.GetAllocationKey()] = ask

	// update app priority
	repeat := ask.GetPendingAskRepeat()
	priority := ask.GetPriority()
	if repeat > 0 && priority > sa.askMaxPriority {
		sa.askMaxPriority = priority
		sa.queue.UpdateApplicationPriority(sa.ApplicationID, sa.askMaxPriority)
	}

	// progress the application from New to Accepted.
	if sa.IsNew() {
		if err := sa.HandleApplicationEvent(RunApplication); err != nil {
			log.Logger().Debug("Application state change failed while recovering allocation ask",
				zap.Error(err))
		}
	}
}

func (sa *Application) UpdateAskRepeat(allocKey string, delta int32) (*resources.Resource, error) {
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
		return nil, fmt.Errorf("ask repaeat not updated resulting repeat less than zero for ask %s on app %s", ask.GetAllocationKey(), sa.ApplicationID)
	}

	askPriority := ask.GetPriority()
	if ask.GetPendingAskRepeat() == 0 {
		// ask removed
		if askPriority >= sa.askMaxPriority {
			// recalculate downward
			sa.updateAskMaxPriority()
		}
	} else {
		// ask added
		if askPriority > sa.askMaxPriority {
			// increase app priority
			sa.askMaxPriority = askPriority
			sa.queue.UpdateApplicationPriority(sa.ApplicationID, askPriority)
		}
	}

	deltaPendingResource := resources.Multiply(ask.GetAllocatedResource(), int64(delta))
	sa.pending = resources.Add(sa.pending, deltaPendingResource)
	// update the pending of the queue with the same delta
	sa.queue.incPendingResource(deltaPendingResource)

	return deltaPendingResource, nil
}

// HasReserved returns true if the application has any reservations.
func (sa *Application) HasReserved() bool {
	sa.RLock()
	defer sa.RUnlock()
	return len(sa.reservations) > 0
}

// IsReservedOnNode returns true if and only if the node has been reserved by the application
// An empty nodeID is never reserved.
func (sa *Application) IsReservedOnNode(nodeID string) bool {
	if nodeID == "" {
		return false
	}
	sa.RLock()
	defer sa.RUnlock()
	// make sure matches only for the whole nodeID
	separator := nodeID + "|"
	for key := range sa.reservations {
		if strings.HasPrefix(key, separator) {
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
	return sa.reserveInternal(node, ask)
}

// Unlocked version for Reserve that really does the work.
// Must only be called while holding the application lock.
func (sa *Application) reserveInternal(node *Node, ask *AllocationAsk) error {
	// create the reservation (includes nil checks)
	nodeReservation := newReservation(node, sa, ask, true)
	if nodeReservation == nil {
		log.Logger().Debug("reservation creation failed unexpectedly",
			zap.String("app", sa.ApplicationID),
			zap.Any("node", node),
			zap.Any("ask", ask))
		return fmt.Errorf("reservation creation failed node or ask are nil on appID %s", sa.ApplicationID)
	}
	allocKey := ask.GetAllocationKey()
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
		zap.String("ask", allocKey))
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
			zap.Stringer("node", node),
			zap.Stringer("ask", ask))
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
				zap.String("ask", ask.GetAllocationKey()))
		}
		delete(sa.reservations, resKey)
		log.Logger().Info("reservation removed successfully", zap.String("node", node.NodeID),
			zap.String("app", ask.GetApplicationID()), zap.String("ask", ask.GetAllocationKey()))
		return 1, nil
	}
	// reservation was not found
	log.Logger().Info("reservation not found while removing from app",
		zap.String("appID", sa.ApplicationID),
		zap.String("nodeID", node.NodeID),
		zap.String("ask", ask.GetAllocationKey()),
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
	allocKey := ask.GetAllocationKey()
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
	// make sure the request are sorted
	sa.Lock()
	sa.sortRequests(false)
	sa.Unlock()

	sa.RLock()
	defer sa.RUnlock()

	for _, request := range sa.sortedRequests {
		// ignore nil checks resource function calls are nil safe
		if headRoom.FitInMaxUndef(request.GetAllocatedResource()) {
			// if headroom is still enough for the resources
			*total = append(*total, request)
			headRoom.SubOnlyExisting(request.GetAllocatedResource())
		}
	}
}

// canReplace returns true if there is a placeholder for the task group available for the request.
// False for all other cases. Placeholder replacements are handled separately from normal allocations.
func (sa *Application) canReplace(request *AllocationAsk) bool {
	// a placeholder or a request without task group can never replace a placeholder
	if request == nil || request.IsPlaceholder() || request.GetTaskGroup() == "" {
		return false
	}
	// get the tracked placeholder data and check if there are still placeholder that can be replaced
	if phData, ok := sa.placeholderData[request.GetTaskGroup()]; ok {
		return phData.Count > (phData.Replaced + phData.TimedOut)
	}
	return false
}

// tryAllocate will perform a regular allocation of a pending request, includes placeholders.
func (sa *Application) tryAllocate(headRoom *resources.Resource, preemptionDelay time.Duration, preemptAttemptsRemaining *int, nodeIterator func() NodeIterator, fullNodeIterator func() NodeIterator, getNodeFn func(string) *Node) *Allocation {
	sa.Lock()
	defer sa.Unlock()
	// make sure the request are sorted
	sa.sortRequests(false)
	// get all the requests from the app sorted in order
	for _, request := range sa.sortedRequests {
		// check if there is a replacement possible
		if sa.canReplace(request) {
			continue
		}

		// resource must fit in headroom otherwise skip the request (unless preemption could help)
		if !headRoom.FitInMaxUndef(request.GetAllocatedResource()) {
			// attempt preemption
			if *preemptAttemptsRemaining > 0 {
				*preemptAttemptsRemaining--
				fullIterator := fullNodeIterator()
				if fullIterator != nil {
					if alloc, ok := sa.tryPreemption(headRoom, preemptionDelay, request, fullIterator, false); ok {
						// preemption occurred, and possibly reservation
						return alloc
					}
				}
			}

			// post scheduling events via the event plugin
			if eventCache := events.GetEventCache(); eventCache != nil {
				message := fmt.Sprintf("Application %s does not fit into %s queue", request.GetApplicationID(), sa.queuePath)
				if event, err := events.CreateRequestEventRecord(request.GetAllocationKey(), request.GetApplicationID(), "InsufficientQueueResources", message); err != nil {
					log.Logger().Warn("Event creation failed",
						zap.String("event message", message),
						zap.Error(err))
				} else {
					eventCache.AddEvent(event)
				}
			}
			continue
		}

		requiredNode := request.GetRequiredNode()
		// does request have any constraint to run on specific node?
		if requiredNode != "" {
			// the iterator might not have the node we need as it could be reserved, or we have not added it yet
			node := getNodeFn(requiredNode)
			if node == nil {
				log.Logger().Warn("required node is not found (could be transient)",
					zap.String("application ID", sa.ApplicationID),
					zap.String("allocationKey", request.GetAllocationKey()),
					zap.String("required node", requiredNode))
				return nil
			}
			// Are there any non daemon set reservations on specific required node?
			// Cancel those reservations to run daemon set pods
			reservations := node.GetReservations()
			if len(reservations) > 0 {
				if !sa.cancelReservations(reservations) {
					return nil
				}
			}
			alloc := sa.tryNode(node, request)
			if alloc != nil {
				// check if the node was reserved and we allocated after a release
				if _, ok := sa.reservations[reservationKey(node, nil, request)]; ok {
					log.Logger().Debug("allocation on required node after release",
						zap.String("appID", sa.ApplicationID),
						zap.String("nodeID", requiredNode),
						zap.String("allocationKey", request.GetAllocationKey()))
					alloc.SetResult(AllocatedReserved)
					return alloc
				}
				log.Logger().Debug("allocation on required node is completed",
					zap.String("nodeID", node.NodeID),
					zap.String("allocationKey", request.GetAllocationKey()),
					zap.Stringer("AllocationResult", alloc.GetResult()))
				return alloc
			}
			return newReservedAllocation(Reserved, node.NodeID, request)
		}

		iterator := nodeIterator()
		if iterator != nil {
			if alloc := sa.tryNodes(request, iterator); alloc != nil {
				// have a candidate return it
				return alloc
			}

			// no nodes qualify, attempt preemption
			if *preemptAttemptsRemaining > 0 {
				*preemptAttemptsRemaining--
				fullIterator := fullNodeIterator()
				if fullIterator != nil {
					if alloc, ok := sa.tryPreemption(headRoom, preemptionDelay, request, fullIterator, true); ok {
						// preemption occurred, and possibly reservation
						return alloc
					}
				}
			}
		}
	}
	// no requests fit, skip to next app
	return nil
}

func (sa *Application) cancelReservations(reservations []*reservation) bool {
	for _, res := range reservations {
		// skip the node
		if res.ask.GetRequiredNode() != "" {
			log.Logger().Warn("reservation for ask with required node already exists on the node",
				zap.String("required node", res.node.NodeID),
				zap.String("existing ask reservation key", res.getKey()))
			return false
		}
	}
	var err error
	var num int
	// un reserve all the apps that were reserved on the node
	for _, res := range reservations {
		thisApp := res.app.ApplicationID == sa.ApplicationID
		if thisApp {
			num, err = sa.unReserveInternal(res.node, res.ask)
		} else {
			num, err = res.app.UnReserve(res.node, res.ask)
		}
		if err != nil {
			log.Logger().Warn("Unable to cancel reservations on node",
				zap.String("victim application ID", res.app.ApplicationID),
				zap.String("victim allocationKey", res.getKey()),
				zap.String("required node", res.node.NodeID),
				zap.Int("reservations count", num),
				zap.String("application ID", sa.ApplicationID))
			return false
		} else {
			log.Logger().Info("Cancelled reservation on required node",
				zap.String("affected application ID", res.app.ApplicationID),
				zap.String("affected allocationKey", res.getKey()),
				zap.String("required node", res.node.NodeID),
				zap.Int("reservations count", num),
				zap.String("application ID", sa.ApplicationID))
		}
		// remove the reservation of the queue
		if thisApp {
			sa.queue.UnReserve(sa.ApplicationID, num)
		} else {
			res.app.GetQueue().UnReserve(res.app.ApplicationID, num)
		}
	}
	return true
}

// tryPlaceholderAllocate tries to replace a placeholder that is allocated with a real allocation
//
//nolint:funlen
func (sa *Application) tryPlaceholderAllocate(nodeIterator func() NodeIterator, getNodeFn func(string) *Node) *Allocation {
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
		if request.IsPlaceholder() || request.GetTaskGroup() == "" {
			continue
		}
		// walk over the placeholders, allow for processing all as we can have multiple task groups
		phAllocs := sa.getPlaceholderAllocations()
		for _, ph := range phAllocs {
			// we could have already released this placeholder and are waiting for the shim to confirm
			// and check that we have the correct task group before trying to swap
			if ph.IsReleased() || request.GetTaskGroup() != ph.GetTaskGroup() {
				continue
			}
			// before we check anything we need to check the resources equality
			delta := resources.Sub(ph.GetAllocatedResource(), request.GetAllocatedResource())
			// Any negative value in the delta means that at least one of the requested resource in the real
			// allocation is larger than the placeholder. We need to cancel this placeholder and check the next
			// placeholder. This should trigger the removal of all the placeholder that are part of this task group.
			// All placeholders in the same task group are always the same size.
			if delta.HasNegativeValue() {
				log.Logger().Warn("releasing placeholder: real allocation is larger than placeholder",
					zap.Stringer("requested resource", request.GetAllocatedResource()),
					zap.String("placeholderID", ph.GetUUID()),
					zap.Stringer("placeholder resource", ph.GetAllocatedResource()))
				// release the placeholder and tell the RM
				ph.SetReleased(true)
				sa.notifyRMAllocationReleased(sa.rmID, []*Allocation{ph}, si.TerminationType_TIMEOUT, "cancel placeholder: resource incompatible")
				// add an event on the app to show the release
				if eventCache := events.GetEventCache(); eventCache != nil {
					message := fmt.Sprintf("Task group '%s' in application '%s': allocation resources '%s' are not matching placeholder '%s' allocation with ID '%s'", ph.GetTaskGroup(), sa.ApplicationID, request.GetAllocatedResource().String(), ph.GetAllocatedResource().String(), ph.GetAllocationKey())
					if event, err := events.CreateRequestEventRecord(ph.GetAllocationKey(), sa.ApplicationID, "releasing placeholder: real allocation is larger than placeholder", message); err != nil {
						log.Logger().Warn("Event creation failed",
							zap.String("event message", message),
							zap.Error(err))
					} else {
						eventCache.AddEvent(event)
					}
				}
				continue
			}
			// placeholder is the same or larger continue processing and difference is handled when the placeholder
			// is swapped with the real one.
			if phFit == nil && reqFit == nil {
				phFit = ph
				reqFit = request
			}
			node := getNodeFn(ph.GetNodeID())
			// got the node run same checks as for reservation (all but fits)
			// resource usage should not change anyway between placeholder and real one at this point
			if node != nil && node.preReserveConditions(request) {
				alloc := NewAllocation(common.GetNewUUID(), node.NodeID, request)
				// double link to make it easier to find
				// alloc (the real one) releases points to the placeholder in the releases list
				alloc.SetRelease(ph)
				// placeholder point to the real one in the releases list
				ph.SetRelease(alloc)
				alloc.SetResult(Replaced)
				// mark placeholder as released
				ph.SetReleased(true)
				_, err := sa.updateAskRepeatInternal(request, -1)
				if err != nil {
					log.Logger().Warn("ask repeat update failed unexpectedly",
						zap.Error(err))
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
			node := iterator.Next()
			if node == nil {
				log.Logger().Warn("Node iterator failed to return a node")
				return nil
			}
			if !node.IsSchedulable() {
				log.Logger().Debug("skipping node for placeholder ask as state is unschedulable",
					zap.String("allocationKey", reqFit.GetAllocationKey()),
					zap.String("node", node.NodeID))
				continue
			}
			if err := node.preAllocateCheck(reqFit.GetAllocatedResource(), reservationKey(nil, sa, reqFit)); err != nil {
				continue
			}
			// skip the node if conditions can not be satisfied
			if !node.preAllocateConditions(reqFit) {
				continue
			}
			// allocation worked: on a non placeholder node update result and return
			alloc := NewAllocation(common.GetNewUUID(), node.NodeID, reqFit)
			// double link to make it easier to find
			// alloc (the real one) releases points to the placeholder in the releases list
			alloc.SetRelease(phFit)
			// placeholder point to the real one in the releases list
			phFit.SetRelease(alloc)
			alloc.SetResult(Replaced)
			// mark placeholder as released
			phFit.SetReleased(true)
			// update just the node to make sure we keep its spot
			// no queue update as we're releasing the placeholder and are just temp over the size
			if !node.AddAllocation(alloc) {
				log.Logger().Debug("Node update failed unexpectedly",
					zap.String("applicationID", sa.ApplicationID),
					zap.Stringer("ask", reqFit),
					zap.Stringer("placeholder", phFit))
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
func (sa *Application) tryReservedAllocate(headRoom *resources.Resource, nodeIterator func() NodeIterator) *Allocation {
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
					allocationKey: reserve.askKey,
					applicationID: sa.ApplicationID,
					allocLog:      make(map[string]*AllocationLogEntry),
				}
			} else {
				unreserveAsk = ask
			}
			// remove the reservation as this should not be reserved
			alloc := newReservedAllocation(Unreserved, reserve.nodeID, unreserveAsk)
			return alloc
		}

		// check if this fits in the queue's head room
		if !headRoom.FitInMaxUndef(ask.GetAllocatedResource()) {
			continue
		}

		// Do we need a specific node?
		if ask.GetRequiredNode() != "" {
			if !reserve.node.CanAllocate(ask.GetAllocatedResource()) && !ask.HasTriggeredPreemption() {
				sa.tryRequiredNodePreemption(reserve, ask)
				continue
			}
		}
		// check allocation possibility
		alloc := sa.tryNode(reserve.node, ask)

		// allocation worked fix the result and return
		if alloc != nil {
			alloc.SetResult(AllocatedReserved)
			return alloc
		}
	}

	// lets try this on all other nodes
	for _, reserve := range sa.reservations {
		// Other nodes cannot be tried if the ask has a required node
		if reserve.ask.GetRequiredNode() != "" {
			continue
		}
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

func (sa *Application) tryPreemption(headRoom *resources.Resource, preemptionDelay time.Duration, ask *AllocationAsk, iterator NodeIterator, nodesTried bool) (*Allocation, bool) {
	preemptor := NewPreemptor(sa, headRoom, preemptionDelay, ask, iterator, nodesTried)

	// validate prerequisites for preemption of an ask and mark ask for preemption if successful
	if !preemptor.CheckPreconditions() {
		return nil, false
	}

	// track time spent trying preemption
	tryPreemptionStart := time.Now()
	defer metrics.GetSchedulerMetrics().ObserveTryPreemptionLatency(tryPreemptionStart)

	// attempt preemption
	return preemptor.TryPreemption()
}

func (sa *Application) tryRequiredNodePreemption(reserve *reservation, ask *AllocationAsk) bool {
	log.Logger().Info("Triggering preemption process for daemon set ask",
		zap.String("ds allocation key", ask.GetAllocationKey()))

	// try preemption and see if we can free up resource
	preemptor := NewRequiredNodePreemptor(reserve.node, ask)
	preemptor.filterAllocations()
	preemptor.sortAllocations()

	// Are there any victims/asks to preempt?
	victims := preemptor.GetVictims()
	if len(victims) > 0 {
		log.Logger().Info("Found victims for required node preemption",
			zap.String("ds allocation key", ask.GetAllocationKey()),
			zap.Int("no.of victims", len(victims)))
		for _, victim := range victims {
			if victimQueue := sa.queue.FindQueueByAppID(victim.GetApplicationID()); victimQueue != nil {
				victimQueue.IncPreemptingResource(victim.GetAllocatedResource())
			}
			victim.MarkPreempted()
		}
		ask.MarkTriggeredPreemption()
		sa.notifyRMAllocationReleased(sa.rmID, victims, si.TerminationType_PREEMPTED_BY_SCHEDULER,
			"preempting allocations to free up resources to run daemon set ask: "+ask.GetAllocationKey())
		return true
	}
	log.Logger().Warn("Problem in finding the victims for preempting resources to meet required ask requirements",
		zap.String("ds allocation key", ask.GetAllocationKey()),
		zap.String("node id", reserve.nodeID))
	return false
}

// Try all the nodes for a reserved request that have not been tried yet.
// This should never result in a reservation as the ask is already reserved
func (sa *Application) tryNodesNoReserve(ask *AllocationAsk, iterator NodeIterator, reservedNode string) *Allocation {
	for iterator.HasNext() {
		node := iterator.Next()
		if node == nil {
			log.Logger().Warn("Node iterator failed to return a node")
			return nil
		}
		if !node.IsSchedulable() {
			log.Logger().Debug("skipping node for reserved ask as state is unschedulable",
				zap.String("allocationKey", ask.GetAllocationKey()),
				zap.String("node", node.NodeID))
			continue
		}
		// skip over the node if the resource does not fit the node or this is the reserved node.
		if !node.FitInNode(ask.GetAllocatedResource()) || node.NodeID == reservedNode {
			continue
		}
		alloc := sa.tryNode(node, ask)
		// allocation worked: update result and return
		if alloc != nil {
			alloc.SetReservedNodeID(reservedNode)
			alloc.SetResult(AllocatedReserved)
			return alloc
		}
	}
	// ask does not fit, skip to next ask
	return nil
}

// Try all the nodes for a request. The result is an allocation or reservation of a node.
// New allocations can only be reserved after a delay.
func (sa *Application) tryNodes(ask *AllocationAsk, iterator NodeIterator) *Allocation {
	var nodeToReserve *Node
	scoreReserved := math.Inf(1)
	// check if the ask is reserved or not
	allocKey := ask.GetAllocationKey()
	reservedAsks := sa.GetAskReservations(allocKey)
	allowReserve := len(reservedAsks) < int(ask.GetPendingAskRepeat())
	for iterator.HasNext() {
		node := iterator.Next()
		if node == nil {
			log.Logger().Warn("Node iterator failed to return a node")
			return nil
		}
		// skip the node if the node is not valid for the ask
		if !node.IsSchedulable() {
			log.Logger().Debug("skipping node for ask as state is unschedulable",
				zap.String("allocationKey", allocKey),
				zap.String("node", node.NodeID))
			continue
		}
		// skip over the node if the resource does not fit the node at all.
		if !node.FitInNode(ask.GetAllocatedResource()) {
			continue
		}
		tryNodeStart := time.Now()
		alloc := sa.tryNode(node, ask)
		// allocation worked so return
		if alloc != nil {
			metrics.GetSchedulerMetrics().ObserveTryNodeLatency(tryNodeStart)
			// check if the node was reserved for this ask: if it is set the result and return
			// NOTE: this is a safeguard as reserved nodes should never be part of the iterator
			// but we have no locking
			if _, ok := sa.reservations[reservationKey(node, nil, ask)]; ok {
				log.Logger().Debug("allocate found reserved ask during non reserved allocate",
					zap.String("appID", sa.ApplicationID),
					zap.String("nodeID", node.NodeID),
					zap.String("allocationKey", allocKey))
				alloc.SetResult(AllocatedReserved)
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
				alloc.SetResult(AllocatedReserved)
				alloc.SetReservedNodeID(nodeID)
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
			score := ask.GetAllocatedResource().FitInScore(node.GetAvailableResource())
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
			zap.Int32("pendingRepeats", ask.GetPendingAskRepeat()))
		// skip the node if conditions can not be satisfied
		if !nodeToReserve.preReserveConditions(ask) {
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
	toAllocate := ask.GetAllocatedResource()
	// create the key for the reservation
	if err := node.preAllocateCheck(toAllocate, reservationKey(nil, sa, ask)); err != nil {
		// skip schedule onto node
		return nil
	}
	// skip the node if conditions can not be satisfied
	if !node.preAllocateConditions(ask) {
		return nil
	}
	// everything OK really allocate
	alloc := NewAllocation(common.GetNewUUID(), node.NodeID, ask)
	if node.AddAllocation(alloc) {
		if err := sa.queue.IncAllocatedResource(alloc.GetAllocatedResource(), false); err != nil {
			log.Logger().Warn("queue update failed unexpectedly",
				zap.Error(err))
			// revert the node update
			node.RemoveAllocation(alloc.GetUUID())
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

func (sa *Application) GetQueuePath() string {
	sa.RLock()
	defer sa.RUnlock()
	return sa.queuePath
}

func (sa *Application) GetQueue() *Queue {
	sa.RLock()
	defer sa.RUnlock()
	return sa.queue
}

// Set the leaf queue the application runs in. The queue will be created when the app is added to the partition.
// The queue name is set to what the placement rule returned.
func (sa *Application) SetQueuePath(queuePath string) {
	sa.Lock()
	defer sa.Unlock()
	sa.queuePath = queuePath
}

// Set the leaf queue the application runs in.
func (sa *Application) SetQueue(queue *Queue) {
	sa.Lock()
	defer sa.Unlock()
	sa.queuePath = queue.QueuePath
	sa.queue = queue
	sa.queue.UpdateApplicationPriority(sa.ApplicationID, sa.askMaxPriority)
}

// remove the leaf queue the application runs in, used when completing the app
func (sa *Application) UnSetQueue() {
	if sa.queue != nil {
		sa.queue.RemoveApplication(sa)
	}
	sa.Lock()
	defer sa.Unlock()
	sa.queue = nil
	sa.finishedTime = time.Now()
}

func (sa *Application) FinishedTime() time.Time {
	sa.RLock()
	defer sa.RUnlock()
	return sa.finishedTime
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
		if alloc.IsPlaceholder() {
			allocations = append(allocations, alloc)
		}
	}
	return allocations
}

// GetAllRequests returns a copy of all requests of the application
func (sa *Application) GetAllRequests() []*AllocationAsk {
	sa.RLock()
	defer sa.RUnlock()
	return sa.getAllRequestsInternal()
}

func (sa *Application) getAllRequestsInternal() []*AllocationAsk {
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
	if info.IsPlaceholder() {
		// when we have the first placeholder allocation start the placeholder timer.
		// It will start to use the resources only after the first allocation, so we will count the time from this point.
		// Also this is the first stable point on the placeholder handling, what is easy to explain and troubleshoot
		// If we would start it when we just try to allocate, that is something very unstable, and we don't really have any
		// impact on what is happening until this point
		if resources.IsZero(sa.allocatedPlaceholder) {
			sa.initPlaceholderTimer()
		}
		// User resource usage needs to be updated even during resource allocation happen for ph's itself even though state change would happen only after all ph allocation completes.
		sa.incUserResourceUsage(info.GetAllocatedResource())
		sa.allocatedPlaceholder = resources.Add(sa.allocatedPlaceholder, info.GetAllocatedResource())
		sa.maxAllocatedResource = resources.ComponentWiseMax(sa.allocatedPlaceholder, sa.maxAllocatedResource)

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
		if info.GetResult() != Replaced || !resources.IsZero(sa.allocatedResource) {
			// progress the state based on where we are, we should never fail in this case
			// keep track of a failure in log.
			if err := sa.HandleApplicationEvent(RunApplication); err != nil {
				log.Logger().Error("Unexpected app state change failure while adding allocation",
					zap.String("currentState", sa.stateMachine.Current()),
					zap.Error(err))
			}
		}
		sa.incUserResourceUsage(info.GetAllocatedResource())
		sa.allocatedResource = resources.Add(sa.allocatedResource, info.GetAllocatedResource())
		sa.maxAllocatedResource = resources.ComponentWiseMax(sa.allocatedResource, sa.maxAllocatedResource)
	}
	sa.allocations[info.GetUUID()] = info
}

// Increase user resource usage
// No locking must be called while holding the lock
func (sa *Application) incUserResourceUsage(resource *resources.Resource) {
	if err := ugm.GetUserManager().IncreaseTrackedResource(sa.queuePath, sa.ApplicationID, resource, sa.user); err != nil {
		log.Logger().Error("Unable to track the user resource usage",
			zap.String("application id", sa.ApplicationID),
			zap.String("user", sa.user.User),
			zap.String("currentState", sa.stateMachine.Current()),
			zap.Error(err))
	}
}

// Decrease user resource usage
// No locking must be called while holding the lock
func (sa *Application) decUserResourceUsage(resource *resources.Resource, removeApp bool) {
	if err := ugm.GetUserManager().DecreaseTrackedResource(sa.queuePath, sa.ApplicationID, resource, sa.user, removeApp); err != nil {
		log.Logger().Error("Unable to track the user resource usage",
			zap.String("application id", sa.ApplicationID),
			zap.String("user", sa.user.User),
			zap.String("currentState", sa.stateMachine.Current()),
			zap.Error(err))
	}
}

func (sa *Application) ReplaceAllocation(uuid string) *Allocation {
	sa.Lock()
	defer sa.Unlock()
	// remove the placeholder that was just confirmed by the shim
	ph := sa.removeAllocationInternal(uuid, si.TerminationType_PLACEHOLDER_REPLACED)
	// this has already been replaced or it is a duplicate message from the shim
	if ph == nil || ph.GetReleaseCount() == 0 {
		log.Logger().Debug("Unexpected placeholder released",
			zap.String("applicationID", sa.ApplicationID),
			zap.Stringer("placeholder", ph))
		return nil
	}
	// weird issue we should never have more than 1 log it for debugging this error
	if ph.GetReleaseCount() > 1 {
		log.Logger().Error("Unexpected release number, placeholder released, only 1 real allocations processed",
			zap.String("applicationID", sa.ApplicationID),
			zap.String("placeholderID", uuid),
			zap.Int("releases", ph.GetReleaseCount()))
	}
	// update the replacing allocation
	// we double linked the real and placeholder allocation
	// ph is the placeholder, the releases entry points to the real one
	alloc := ph.GetFirstRelease()
	alloc.SetPlaceholderUsed(true)
	alloc.SetPlaceholderCreateTime(ph.GetCreateTime())
	alloc.SetCreateTime(time.Now())
	sa.addAllocationInternal(alloc)
	// order is important: clean up the allocation after adding it to the app
	// we need the original Replaced allocation result.
	alloc.ClearReleases()
	alloc.SetResult(Allocated)
	if sa.placeholderData != nil {
		sa.placeholderData[ph.GetTaskGroup()].Replaced++
	}
	return ph
}

// Remove the Allocation from the application.
// Return the allocation that was removed or nil if not found.
func (sa *Application) RemoveAllocation(uuid string, releaseType si.TerminationType) *Allocation {
	sa.Lock()
	defer sa.Unlock()
	return sa.removeAllocationInternal(uuid, releaseType)
}

// Remove the Allocation from the application
// No locking must be called while holding the lock
func (sa *Application) removeAllocationInternal(uuid string, releaseType si.TerminationType) *Allocation {
	alloc := sa.allocations[uuid]

	// When app has the allocation, update map, and update allocated resource of the app
	if alloc == nil {
		return nil
	}

	var event applicationEvent = EventNotNeeded
	var eventWarning string
	removeApp := false
	// update correct allocation tracker
	if alloc.IsPlaceholder() {
		// make sure we account for the placeholders being removed in the tracking data
		if releaseType == si.TerminationType_STOPPED_BY_RM || releaseType == si.TerminationType_PREEMPTED_BY_SCHEDULER || releaseType == si.TerminationType_UNKNOWN_TERMINATION_TYPE {
			if _, ok := sa.placeholderData[alloc.taskGroupName]; ok {
				sa.placeholderData[alloc.taskGroupName].TimedOut++
			}
		}
		// as and when every ph gets removed (for replacement), resource usage would be reduced.
		// When real allocation happens as part of replacement, usage would be increased again with real alloc resource
		sa.allocatedPlaceholder = resources.Sub(sa.allocatedPlaceholder, alloc.GetAllocatedResource())

		// if all the placeholders are replaced, clear the placeholder timer
		if resources.IsZero(sa.allocatedPlaceholder) {
			sa.clearPlaceholderTimer()
			if (sa.IsCompleting() && sa.stateTimer == nil) || sa.IsFailing() || sa.IsResuming() || sa.hasZeroAllocations() {
				removeApp = true
				event = CompleteApplication
				if sa.IsFailing() {
					event = FailApplication
				}
				if sa.IsResuming() {
					event = RunApplication
					removeApp = false
				}
				eventWarning = "Application state not changed while removing a placeholder allocation"
			}
		}
		sa.decUserResourceUsage(alloc.GetAllocatedResource(), removeApp)
	} else {
		sa.allocatedResource = resources.Sub(sa.allocatedResource, alloc.GetAllocatedResource())
		// When the resource trackers are zero we should not expect anything to come in later.
		if sa.hasZeroAllocations() {
			removeApp = true
			event = CompleteApplication
			eventWarning = "Application state not changed to Waiting while removing an allocation"
		}
		sa.decUserResourceUsage(alloc.GetAllocatedResource(), removeApp)
	}
	if event != EventNotNeeded {
		if err := sa.HandleApplicationEvent(event); err != nil {
			log.Logger().Warn(eventWarning,
				zap.String("currentState", sa.CurrentState()),
				zap.Stringer("event", event),
				zap.Error(err))
		}
	}
	delete(sa.allocations, uuid)
	return alloc
}

func (sa *Application) updateAskMaxPriority() {
	value := configs.MinPriority
	for _, v := range sa.requests {
		if v.GetPendingAskRepeat() == 0 {
			continue
		}
		p := v.GetPriority()
		if p > value {
			value = p
		}
	}
	sa.askMaxPriority = value
	sa.queue.UpdateApplicationPriority(sa.ApplicationID, value)
}

func (sa *Application) hasZeroAllocations() bool {
	return resources.IsZero(sa.pending) && resources.IsZero(sa.allocatedResource)
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
	if resources.IsZero(sa.pending) {
		sa.decUserResourceUsage(resources.Add(sa.allocatedResource, sa.allocatedPlaceholder), true)
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

// RejectApplication rejects this application.
func (sa *Application) RejectApplication(rejectedMessage string) error {
	sa.Lock()
	defer sa.Unlock()

	return sa.HandleApplicationEventWithInfo(RejectApplication, rejectedMessage)
}

// FailApplication fails this application.
func (sa *Application) FailApplication(failureMessage string) error {
	sa.Lock()
	defer sa.Unlock()

	return sa.HandleApplicationEventWithInfo(FailApplication, failureMessage)
}

// get a copy of the user details for the application
func (sa *Application) GetUser() security.UserGroup {
	sa.RLock()
	defer sa.RUnlock()

	return sa.user
}

// Get a tag from the application
// Note: tags are not case sensitive
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

// notifyRMAllocationReleased send an allocation release event to the RM to if the event handler is configured
// and at least one allocation has been released.
// No locking must be called while holding the lock
func (sa *Application) notifyRMAllocationReleased(rmID string, released []*Allocation, terminationType si.TerminationType, message string) {
	// only generate event if needed
	if len(released) == 0 || sa.rmEventHandler == nil {
		return
	}
	c := make(chan *rmevent.Result)
	releaseEvent := &rmevent.RMReleaseAllocationEvent{
		ReleasedAllocations: make([]*si.AllocationRelease, 0),
		RmID:                rmID,
		Channel:             c,
	}
	for _, alloc := range released {
		releaseEvent.ReleasedAllocations = append(releaseEvent.ReleasedAllocations, &si.AllocationRelease{
			ApplicationID:   alloc.GetApplicationID(),
			PartitionName:   alloc.GetPartitionName(),
			AllocationKey:   alloc.GetAllocationKey(),
			UUID:            alloc.GetUUID(),
			TerminationType: terminationType,
			Message:         message,
		})
	}
	sa.rmEventHandler.HandleEvent(releaseEvent)
	// Wait from channel
	result := <-c
	if result.Succeeded {
		log.Logger().Debug("Successfully synced shim on released allocations. response: " + result.Reason)
	} else {
		log.Logger().Info("failed to sync shim on released allocations")
	}
}

// notifyRMAllocationAskReleased send an ask release event to the RM to if the event handler is configured
// and at least one ask has been released.
// No locking must be called while holding the lock
func (sa *Application) notifyRMAllocationAskReleased(rmID string, released []*AllocationAsk, terminationType si.TerminationType, message string) {
	// only generate event if needed
	if len(released) == 0 || sa.rmEventHandler == nil {
		return
	}
	releaseEvent := &rmevent.RMReleaseAllocationAskEvent{
		ReleasedAllocationAsks: make([]*si.AllocationAskRelease, 0),
		RmID:                   rmID,
	}
	for _, alloc := range released {
		releaseEvent.ReleasedAllocationAsks = append(releaseEvent.ReleasedAllocationAsks, &si.AllocationAskRelease{
			ApplicationID:   alloc.GetApplicationID(),
			PartitionName:   alloc.GetPartitionName(),
			AllocationKey:   alloc.GetAllocationKey(),
			TerminationType: terminationType,
			Message:         message,
		})
	}
	sa.rmEventHandler.HandleEvent(releaseEvent)
}

func (sa *Application) IsAllocationAssignedToApp(alloc *Allocation) bool {
	sa.RLock()
	defer sa.RUnlock()
	_, ok := sa.allocations[alloc.GetUUID()]
	return ok
}

func (sa *Application) GetRejectedMessage() string {
	sa.RLock()
	defer sa.RUnlock()
	return sa.rejectedMessage
}

func (sa *Application) addPlaceholderData(ask *AllocationAsk) {
	if sa.placeholderData == nil {
		sa.placeholderData = make(map[string]*PlaceholderData)
	}
	taskGroupName := ask.GetTaskGroup()
	if _, ok := sa.placeholderData[taskGroupName]; !ok {
		sa.placeholderData[taskGroupName] = &PlaceholderData{
			TaskGroupName: taskGroupName,
			MinResource:   ask.GetAllocatedResource().Clone(),
		}
	}
	sa.placeholderData[taskGroupName].Count++
}

func (sa *Application) GetAllPlaceholderData() []*PlaceholderData {
	sa.RLock()
	defer sa.RUnlock()
	var placeholders []*PlaceholderData
	for _, taskGroup := range sa.placeholderData {
		placeholders = append(placeholders, taskGroup)
	}
	return placeholders
}

func (sa *Application) GetAskMaxPriority() int32 {
	sa.RLock()
	defer sa.RUnlock()
	return sa.askMaxPriority
}

func (sa *Application) cleanupAsks() {
	sa.requests = make(map[string]*AllocationAsk)
	sa.sortedRequests = nil
}

// test only
func SetCompletingTimeout(duration time.Duration) {
	completingTimeout = duration
}

// test only
func (sa *Application) SetTimedOutPlaceholder(taskGroupName string, timedOut int64) {
	sa.Lock()
	defer sa.Unlock()
	if sa.placeholderData == nil {
		return
	}
	if _, ok := sa.placeholderData[taskGroupName]; ok {
		sa.placeholderData[taskGroupName].TimedOut = timedOut
	}
}
