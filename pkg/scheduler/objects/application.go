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
	"strconv"
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
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	schedEvt "github.com/apache/yunikorn-core/pkg/scheduler/objects/events"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

var (
	reservationDelay          = 2 * time.Second
	completingTimeout         = 30 * time.Second
	terminatedTimeout         = 3 * 24 * time.Hour
	defaultPlaceholderTimeout = 15 * time.Minute
)
var initAppLogOnce sync.Once
var rateLimitedAppLog *log.RateLimitedLogger

const (
	Soft string = "Soft"
	Hard string = "Hard"

	NotEnoughUserQuota  = "Not enough user quota"
	NotEnoughQueueQuota = "Not enough queue quota"
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
	ApplicationID string            // application ID
	Partition     string            // partition Name
	tags          map[string]string // application tags used in scheduling

	// Private mutable fields need protection
	queuePath         string
	queue             *Queue                  // queue the application is running in
	pending           *resources.Resource     // pending resources from asks for the app
	reservations      map[string]*reservation // a map of reservations
	requests          map[string]*Allocation  // a map of allocations, pending or satisfied
	sortedRequests    sortedRequests          // list of requests pre-sorted
	user              security.UserGroup      // owner of the application
	allocatedResource *resources.Resource     // total allocated resources
	submissionTime    time.Time               // time application was submitted (based on the first ask)

	usedResource        *resources.TrackedResource // keep track of resource usage of the application
	preemptedResource   *resources.TrackedResource // keep track of preempted resource usage of the application
	placeholderResource *resources.TrackedResource // keep track of placeholder resource usage of the application

	maxAllocatedResource *resources.Resource         // max allocated resources
	allocatedPlaceholder *resources.Resource         // total allocated placeholder resources
	allocations          map[string]*Allocation      // list of all satisfied allocations
	placeholderAsk       *resources.Resource         // total placeholder request for the app (all task groups)
	stateMachine         *fsm.FSM                    // application state machine
	stateTimer           *time.Timer                 // timer for state time
	execTimeout          time.Duration               // execTimeout for the application run
	placeholderTimer     *time.Timer                 // placeholder replace timer
	gangSchedulingStyle  string                      // gang scheduling style can be hard (after timeout we fail the application), or soft (after timeeout we schedule it as a normal application)
	startTime            time.Time                   // the time that the application starts running. Default is zero.
	finishedTime         time.Time                   // the time of finishing this application. the default value is zero time
	rejectedMessage      string                      // If the application is rejected, save the rejected message
	stateLog             []*StateLogEntry            // state log for this application
	placeholderData      map[string]*PlaceholderData // track placeholder and gang related info
	askMaxPriority       int32                       // highest priority value of outstanding asks
	hasPlaceholderAlloc  bool                        // Whether there is at least one allocated placeholder
	runnableInQueue      bool                        // whether the application is runnable/schedulable in the queue. Default is true.
	runnableByUserLimit  bool                        // whether the application is runnable/schedulable based on user/group quota. Default is true.

	rmEventHandler        handler.EventHandler
	rmID                  string
	terminatedCallback    func(appID string)
	appEvents             *schedEvt.ApplicationEvents
	sendStateChangeEvents bool // whether to send state-change events or not (simplifies testing)

	locking.RWMutex
}

func NewApplication(siApp *si.AddApplicationRequest, ugi security.UserGroup, eventHandler handler.EventHandler, rmID string) *Application {
	app := &Application{
		ApplicationID:         siApp.ApplicationID,
		Partition:             siApp.PartitionName,
		submissionTime:        time.Now(),
		queuePath:             siApp.QueueName,
		tags:                  siApp.Tags,
		pending:               resources.NewResource(),
		allocatedResource:     resources.NewResource(),
		usedResource:          resources.NewTrackedResource(),
		preemptedResource:     resources.NewTrackedResource(),
		placeholderResource:   resources.NewTrackedResource(),
		maxAllocatedResource:  resources.NewResource(),
		allocatedPlaceholder:  resources.NewResource(),
		requests:              make(map[string]*Allocation),
		reservations:          make(map[string]*reservation),
		allocations:           make(map[string]*Allocation),
		stateMachine:          NewAppState(),
		placeholderAsk:        resources.NewResourceFromProto(siApp.PlaceholderAsk),
		startTime:             time.Time{},
		finishedTime:          time.Time{},
		rejectedMessage:       "",
		stateLog:              make([]*StateLogEntry, 0),
		askMaxPriority:        configs.MinPriority,
		sortedRequests:        sortedRequests{},
		sendStateChangeEvents: true,
		runnableByUserLimit:   true,
		runnableInQueue:       true,
	}
	placeholderTimeout := common.ConvertSITimeoutWithAdjustment(siApp, defaultPlaceholderTimeout)
	gangSchedStyle := siApp.GetGangSchedulingStyle()
	if gangSchedStyle != Soft && gangSchedStyle != Hard {
		log.Log(log.SchedApplication).Info("Unknown gang scheduling style, using soft style as default",
			zap.String("gang scheduling style", gangSchedStyle))
		gangSchedStyle = Soft
	}
	app.gangSchedulingStyle = gangSchedStyle
	app.execTimeout = placeholderTimeout
	app.user = ugi
	app.rmEventHandler = eventHandler
	app.rmID = rmID
	app.appEvents = schedEvt.NewApplicationEvents(events.GetEventSystem())
	app.appEvents.SendNewApplicationEvent(app.ApplicationID)
	return app
}

func (sa *Application) String() string {
	if sa == nil {
		return "application is nil"
	}
	return fmt.Sprintf("applicationID: %s, Partition: %s, SubmissionTime: %x, State: %s",
		sa.ApplicationID, sa.Partition, sa.GetSubmissionTime(), sa.stateMachine.Current())
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
	log.Log(log.SchedApplication).Debug("Set reservation delay",
		zap.Duration("delay", delay))
	reservationDelay = delay
}

// Return the current state or a checked specific state for the application.
// The state machine handles the locking.
func (sa *Application) CurrentState() string {
	return sa.stateMachine.Current()
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

// Set the state timer to make sure the application will not get stuck in a time-sensitive state too long.
// This prevents an app from not progressing to the next state if a timeout is required.
// Used for placeholder timeout and completion handling.
func (sa *Application) setStateTimer(timeout time.Duration, currentState string, event applicationEvent) {
	log.Log(log.SchedApplication).Debug("Application state timer initiated",
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
			log.Log(log.SchedApplication).Debug("Application state: auto progress",
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
				sa.notifyRMAllocationReleased(toRelease, si.TerminationType_TIMEOUT, "releasing placeholders on app complete")
				sa.clearStateTimer()
			} else {
				// nolint: errcheck
				_ = sa.HandleApplicationEvent(event)
			}
		}
	}
}

// Clear the state timer. If the application has progressed out of a time-sensitive state we need to stop the timer and
// clean up. Called when transitioning from Completed to Completing or when expiring an application.
func (sa *Application) clearStateTimer() {
	if sa == nil || sa.stateTimer == nil {
		return
	}
	sa.stateTimer.Stop()
	sa.stateTimer = nil
	log.Log(log.SchedApplication).Debug("Application state timer cleared",
		zap.String("appID", sa.ApplicationID),
		zap.String("state", sa.stateMachine.Current()))
}

func (sa *Application) initPlaceholderTimer() {
	if sa.placeholderTimer != nil || !sa.IsAccepted() || sa.execTimeout <= 0 {
		return
	}
	log.Log(log.SchedApplication).Debug("Application placeholder timer initiated",
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
	log.Log(log.SchedApplication).Debug("Application placeholder timer cleared",
		zap.String("AppID", sa.ApplicationID),
		zap.Duration("Timeout", sa.execTimeout))
}

// timeoutPlaceholderProcessing cleans up all placeholder asks and allocations that are not used after the timeout.
// If the application has started processing, Running state or further, the application keeps on processing without
// being able to use the placeholders.
// If the application is in New or Accepted state we clean up and take followup action based on the gang scheduling
// style.
func (sa *Application) timeoutPlaceholderProcessing() {
	sa.Lock()
	defer sa.Unlock()
	if (sa.IsRunning() || sa.IsCompleting()) && !resources.IsZero(sa.allocatedPlaceholder) {
		// Case 1: if all app's placeholders are allocated, only part of them gets replaced, just delete the remaining placeholders
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
		}
		log.Log(log.SchedApplication).Info("Placeholder timeout, releasing allocated placeholders",
			zap.String("AppID", sa.ApplicationID),
			zap.Int("placeholders being replaced", replacing),
			zap.Int("releasing placeholders", len(toRelease)))
		// trigger the release of the placeholders: accounting updates when the release is done
		sa.notifyRMAllocationReleased(toRelease, si.TerminationType_TIMEOUT, "releasing allocated placeholders on placeholder timeout")
	} else {
		// Case 2: in every other case progress the application, and notify the context about the expired placeholders
		// change the status of the app based on gang style: soft resume normal allocations, hard fail the app
		event := ResumeApplication
		if sa.gangSchedulingStyle == Hard {
			event = FailApplication
		}
		if err := sa.HandleApplicationEventWithInfo(event, "ResourceReservationTimeout"); err != nil {
			log.Log(log.SchedApplication).Debug("Application state change failed when placeholder timed out",
				zap.String("AppID", sa.ApplicationID),
				zap.String("currentState", sa.CurrentState()),
				zap.Error(err))
		}
		// all allocations are placeholders release them all
		var toRelease, pendingRelease []*Allocation
		for _, alloc := range sa.allocations {
			alloc.SetReleased(true)
			toRelease = append(toRelease, alloc)
		}
		// get all open requests and remove them all filter out already allocated as they are already released
		for _, alloc := range sa.requests {
			if !alloc.IsAllocated() {
				alloc.SetReleased(true)
				pendingRelease = append(pendingRelease, alloc)
				sa.placeholderData[alloc.taskGroupName].TimedOut++
			}
		}
		log.Log(log.SchedApplication).Info("Placeholder timeout, releasing allocated and pending placeholders",
			zap.String("AppID", sa.ApplicationID),
			zap.Int("releasing placeholders", len(toRelease)),
			zap.Int("pending placeholders", len(pendingRelease)),
			zap.String("gang scheduling style", sa.gangSchedulingStyle))
		sa.removeAsksInternal("", si.EventRecord_REQUEST_TIMEOUT)
		// trigger the release of the allocated placeholders: accounting updates when the release is done
		sa.notifyRMAllocationReleased(toRelease, si.TerminationType_TIMEOUT, "releasing allocated placeholders on placeholder timeout")
		// trigger the release of the pending placeholders: accounting has been done
		sa.notifyRMAllocationReleased(pendingRelease, si.TerminationType_TIMEOUT, "releasing pending placeholders on placeholder timeout")
	}
	sa.clearPlaceholderTimer()
}

// GetReservations returns an array of all reservation keys for the application.
// This will return an empty array if there are no reservations.
// Visible for tests
func (sa *Application) GetReservations() []string {
	sa.RLock()
	defer sa.RUnlock()
	keys := make([]string, len(sa.reservations))
	var i int
	for key := range sa.reservations {
		keys[i] = key
		i++
	}
	return keys
}

// GetAllocationAsk returns the allocation alloc for the key, nil if not found
func (sa *Application) GetAllocationAsk(allocationKey string) *Allocation {
	sa.RLock()
	defer sa.RUnlock()
	return sa.requests[allocationKey]
}

// GetAllocatedResource returns the currently allocated resources for this application
func (sa *Application) GetAllocatedResource() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return sa.allocatedResource.Clone()
}

// GetMaxAllocatedResource returns the peak of the allocated resources for this application
func (sa *Application) GetMaxAllocatedResource() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return sa.maxAllocatedResource.Clone()
}

// GetPlaceholderResource returns the currently allocated placeholder resources for this application
func (sa *Application) GetPlaceholderResource() *resources.Resource {
	sa.RLock()
	defer sa.RUnlock()
	return sa.allocatedPlaceholder.Clone()
}

// GetPlaceholderAsk returns the total placeholder resource request for this application
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

// RemoveAllocationAsk removes one or more allocation asks from this application.
// This also removes any reservations that are linked to the allocations.
// The return value is the number of reservations released
func (sa *Application) RemoveAllocationAsk(allocKey string) int {
	sa.Lock()
	defer sa.Unlock()
	return sa.removeAsksInternal(allocKey, si.EventRecord_REQUEST_CANCEL)
}

// unlocked version of the allocation ask removal
func (sa *Application) removeAsksInternal(allocKey string, detail si.EventRecord_ChangeDetail) int {
	// shortcut no need to do anything
	if len(sa.requests) == 0 {
		return 0
	}
	var deltaPendingResource *resources.Resource = nil
	// when allocation key is not specified, cleanup all allocations
	var toRelease int
	if allocKey == "" {
		// cleanup all reservations
		for _, reserve := range sa.reservations {
			releases := sa.unReserveInternal(reserve)
			toRelease += releases
		}
		// clean up the queue reservation
		sa.queue.UnReserve(sa.ApplicationID, toRelease)
		// Cleanup total pending resource
		deltaPendingResource = sa.pending
		sa.pending = resources.NewResource()
		for _, ask := range sa.requests {
			sa.appEvents.SendRemoveAskEvent(sa.ApplicationID, ask.allocationKey, ask.GetAllocatedResource(), detail)
		}
		sa.requests = make(map[string]*Allocation)
		sa.sortedRequests = sortedRequests{}
		sa.askMaxPriority = configs.MinPriority
		sa.queue.UpdateApplicationPriority(sa.ApplicationID, sa.askMaxPriority)
	} else {
		// cleanup the reservation for this allocation
		if reserve, ok := sa.reservations[allocKey]; ok {
			releases := sa.unReserveInternal(reserve)
			// clean up the queue reservation
			sa.queue.UnReserve(sa.ApplicationID, releases)
			toRelease += releases
		}
		if ask := sa.requests[allocKey]; ask != nil {
			if !ask.IsAllocated() {
				deltaPendingResource = ask.GetAllocatedResource()
				sa.pending = resources.Sub(sa.pending, deltaPendingResource)
				sa.pending.Prune()
			}
			delete(sa.requests, allocKey)
			sa.sortedRequests.remove(ask)
			sa.appEvents.SendRemoveAskEvent(sa.ApplicationID, ask.allocationKey, ask.GetAllocatedResource(), detail)
			if priority := ask.GetPriority(); priority >= sa.askMaxPriority {
				sa.updateAskMaxPriority()
			}
		}
	}
	// clean up the queue pending resources
	sa.queue.decPendingResource(deltaPendingResource)
	// Check if we need to change state based on the removal:
	// 1) if pending is zero (no more asks left)
	// 2) if confirmed allocations is zero (no real tasks running)
	// Change the state to completing.
	// When the resource trackers are zero we should not expect anything to come in later.
	hasPlaceHolderAllocations := len(sa.getPlaceholderAllocations()) > 0
	if resources.IsZero(sa.pending) && resources.IsZero(sa.allocatedResource) && !sa.IsFailing() && !sa.IsCompleting() && !hasPlaceHolderAllocations {
		if err := sa.HandleApplicationEvent(CompleteApplication); err != nil {
			log.Log(log.SchedApplication).Warn("Application state not changed to Completing while updating ask(s)",
				zap.String("currentState", sa.CurrentState()),
				zap.Error(err))
		}
	}

	log.Log(log.SchedApplication).Info("ask removed successfully from application",
		zap.String("appID", sa.ApplicationID),
		zap.String("ask", allocKey),
		zap.Stringer("pendingDelta", deltaPendingResource))

	return toRelease
}

// Add an allocation ask to this application
// If the ask already exist update the existing info
func (sa *Application) AddAllocationAsk(ask *Allocation) error {
	sa.Lock()
	defer sa.Unlock()
	if ask == nil {
		return fmt.Errorf("ask cannot be nil when added to app %s", sa.ApplicationID)
	}
	if ask.IsAllocated() || resources.IsZero(ask.GetAllocatedResource()) {
		return fmt.Errorf("invalid ask added to app %s: %v", sa.ApplicationID, ask)
	}
	if ask.createTime.Before(sa.submissionTime) {
		sa.submissionTime = ask.createTime
	}
	delta := ask.GetAllocatedResource().Clone()

	var oldAskResource *resources.Resource = nil
	if oldAsk := sa.requests[ask.GetAllocationKey()]; oldAsk != nil && !oldAsk.IsAllocated() {
		oldAskResource = oldAsk.GetAllocatedResource().Clone()
	}

	// Check if we need to change state based on the ask added, there are two cases:
	// 1) first ask added on a new app: state is New
	// 2) all asks and allocation have been removed: state is Completing
	// Move the state and get it scheduling (again)
	if sa.stateMachine.Is(New.String()) || sa.stateMachine.Is(Completing.String()) {
		if err := sa.HandleApplicationEvent(RunApplication); err != nil {
			log.Log(log.SchedApplication).Debug("Application state change failed while adding new ask",
				zap.String("currentState", sa.CurrentState()),
				zap.Error(err))
		}
	}
	sa.addAllocationAskInternal(ask)

	// Update total pending resource
	delta.SubFrom(oldAskResource)
	sa.pending = resources.Add(sa.pending, delta)
	sa.pending.Prune()
	sa.queue.incPendingResource(delta)

	log.Log(log.SchedApplication).Info("ask added successfully to application",
		zap.String("appID", sa.ApplicationID),
		zap.String("user", sa.user.User),
		zap.String("ask", ask.GetAllocationKey()),
		zap.Bool("placeholder", ask.IsPlaceholder()),
		zap.Stringer("pendingDelta", delta))
	sa.sortedRequests.insert(ask)
	sa.appEvents.SendNewAskEvent(sa.ApplicationID, ask.allocationKey, ask.GetAllocatedResource())

	return nil
}

// UpdateAllocationResources updates the app, queue, and user tracker with deltas for an allocation.
// If an existing allocation cannot be found or alloc is invalid, an error is returned.
func (sa *Application) UpdateAllocationResources(alloc *Allocation) error {
	sa.Lock()
	defer sa.Unlock()
	if alloc == nil {
		return fmt.Errorf("alloc cannot be nil when updating resources for app %s", sa.ApplicationID)
	}
	if resources.IsZero(alloc.GetAllocatedResource()) {
		return fmt.Errorf("cannot update alloc with zero resources on app %s: %v", sa.ApplicationID, alloc)
	}
	existing := sa.requests[alloc.GetAllocationKey()]
	if existing == nil {
		return fmt.Errorf("existing alloc not found when updating resources on app %s: %v", sa.ApplicationID, alloc)
	}

	newResource := alloc.GetAllocatedResource().Clone()
	existingResource := existing.GetAllocatedResource().Clone()
	delta := resources.Sub(newResource, existingResource)
	if resources.IsZero(delta) {
		return nil
	}
	delta.Prune()

	if existing.IsAllocated() {
		// update allocated resources
		sa.allocatedResource = resources.Add(sa.allocatedResource, delta)
		sa.allocatedResource.Prune()
		sa.queue.IncAllocatedResource(delta)

		// update user usage
		sa.incUserResourceUsage(delta)

		log.Log(log.SchedApplication).Info("updated allocated resources for application",
			zap.String("appID", sa.ApplicationID),
			zap.String("user", sa.user.User),
			zap.String("alloc", existing.GetAllocationKey()),
			zap.Bool("placeholder", existing.IsPlaceholder()),
			zap.Stringer("existingResources", existingResource),
			zap.Stringer("updatedResources", newResource),
			zap.Stringer("delta", delta))
	} else {
		// update pending resources
		sa.pending = resources.Add(sa.pending, delta)
		sa.pending.Prune()
		sa.queue.incPendingResource(delta)
		log.Log(log.SchedApplication).Info("updated pending resources for application",
			zap.String("appID", sa.ApplicationID),
			zap.String("user", sa.user.User),
			zap.String("alloc", existing.GetAllocationKey()),
			zap.Bool("placeholder", existing.IsPlaceholder()),
			zap.Stringer("existingResources", existingResource),
			zap.Stringer("updatedResources", newResource),
			zap.Stringer("delta", delta))
	}

	// update the allocation itself
	existing.SetAllocatedResource(newResource)
	return nil
}

// Add the ask when a node allocation is recovered.
// Safeguarded against a nil but the recovery generates the ask and should never be nil.
func (sa *Application) RecoverAllocationAsk(alloc *Allocation) {
	sa.Lock()
	defer sa.Unlock()
	if alloc == nil {
		return
	}

	sa.addAllocationAskInternal(alloc)

	// progress the application from New to Accepted.
	if sa.IsNew() {
		if err := sa.HandleApplicationEvent(RunApplication); err != nil {
			log.Log(log.SchedApplication).Debug("Application state change failed while recovering allocation ask",
				zap.Error(err))
		}
	}
}

func (sa *Application) addAllocationAskInternal(ask *Allocation) {
	sa.requests[ask.GetAllocationKey()] = ask

	// update app priority
	allocated := ask.IsAllocated()
	priority := ask.GetPriority()
	if !allocated && priority > sa.askMaxPriority {
		sa.askMaxPriority = priority
		sa.queue.UpdateApplicationPriority(sa.ApplicationID, sa.askMaxPriority)
	}

	if ask.IsPlaceholder() {
		sa.addPlaceholderData(ask)
	}
}

func (sa *Application) AllocateAsk(allocKey string) (*resources.Resource, error) {
	sa.Lock()
	defer sa.Unlock()
	if ask := sa.requests[allocKey]; ask != nil {
		return sa.allocateAsk(ask)
	}
	return nil, fmt.Errorf("failed to locate ask with key %s", allocKey)
}

func (sa *Application) DeallocateAsk(allocKey string) (*resources.Resource, error) {
	sa.Lock()
	defer sa.Unlock()
	if ask := sa.requests[allocKey]; ask != nil {
		return sa.deallocateAsk(ask)
	}
	return nil, fmt.Errorf("failed to locate ask with key %s", allocKey)
}

func (sa *Application) allocateAsk(ask *Allocation) (*resources.Resource, error) {
	if !ask.allocate() {
		return nil, fmt.Errorf("unable to allocate previously allocated ask %s on app %s", ask.GetAllocationKey(), sa.ApplicationID)
	}

	if ask.GetPriority() >= sa.askMaxPriority {
		// recalculate downward
		sa.updateAskMaxPriority()
	}

	delta := ask.GetAllocatedResource()
	sa.pending = resources.Sub(sa.pending, delta)
	sa.pending.Prune()
	// update the pending of the queue with the same delta
	sa.queue.decPendingResource(delta)

	return delta, nil
}

func (sa *Application) deallocateAsk(ask *Allocation) (*resources.Resource, error) {
	if !ask.deallocate() {
		return nil, fmt.Errorf("unable to deallocate pending ask %s on app %s", ask.GetAllocationKey(), sa.ApplicationID)
	}

	askPriority := ask.GetPriority()
	if askPriority > sa.askMaxPriority {
		// increase app priority
		sa.askMaxPriority = askPriority
		sa.queue.UpdateApplicationPriority(sa.ApplicationID, askPriority)
	}

	delta := ask.GetAllocatedResource()
	sa.pending = resources.Add(sa.pending, delta)
	// update the pending of the queue with the same delta
	sa.queue.incPendingResource(delta)

	return delta, nil
}

// HasReserved returns true if the application has any reservations.
func (sa *Application) HasReserved() bool {
	sa.RLock()
	defer sa.RUnlock()
	return len(sa.reservations) > 0
}

// NodeReservedForAsk returns the nodeID that has been reserved by the application for the ask
// An empty nodeID means the ask is not reserved. An empty askKey is never reserved.
func (sa *Application) NodeReservedForAsk(askKey string) string {
	sa.RLock()
	defer sa.RUnlock()
	if reserved, ok := sa.reservations[askKey]; ok {
		return reserved.nodeID
	}
	return ""
}

// Reserve the application for this node and alloc combination.
// If the reservation fails the function returns false, if the reservation is made it returns true.
// If the node and alloc combination was already reserved for the application this is a noop and returns true.
func (sa *Application) Reserve(node *Node, ask *Allocation) error {
	if node == nil || ask == nil {
		return fmt.Errorf("reservation creation failed node or alloc are nil on appID %s", sa.ApplicationID)
	}
	sa.Lock()
	defer sa.Unlock()
	return sa.reserveInternal(node, ask)
}

// reserveInternal is the unlocked version for Reserve that really does the work.
// Must only be called while holding the application lock.
func (sa *Application) reserveInternal(node *Node, ask *Allocation) error {
	allocKey := ask.GetAllocationKey()
	if sa.requests[allocKey] == nil {
		log.Log(log.SchedApplication).Debug("alloc is not registered to this app",
			zap.String("app", sa.ApplicationID),
			zap.String("allocKey", allocKey))
		return fmt.Errorf("reservation creation failed alloc %s not found on appID %s", allocKey, sa.ApplicationID)
	}
	// create the reservation (includes nil checks)
	nodeReservation := newReservation(node, sa, ask, true)
	if nodeReservation == nil {
		log.Log(log.SchedApplication).Debug("reservation creation failed unexpectedly",
			zap.String("app", sa.ApplicationID),
			zap.Stringer("node", node),
			zap.Stringer("alloc", ask))
		return fmt.Errorf("reservation creation failed node or alloc are nil on appID %s", sa.ApplicationID)
	}
	// the alloc should not have reserved a node yet: do not allow multiple nodes to be reserved
	if err := sa.canAllocationReserve(ask); err != nil {
		return err
	}
	// check if we can reserve the node before reserving on the app
	if err := node.Reserve(sa, ask); err != nil {
		return err
	}
	sa.reservations[allocKey] = nodeReservation
	log.Log(log.SchedApplication).Info("reservation added successfully",
		zap.String("app", sa.ApplicationID),
		zap.String("node", node.NodeID),
		zap.String("alloc", allocKey))
	return nil
}

// UnReserve the application for this node and alloc combination.
// If the reservation does not exist it returns 0 for reservations removed, if the reservation is removed it returns 1.
// The error is set if the reservation key cannot be removed from the app or node.
func (sa *Application) UnReserve(node *Node, ask *Allocation) int {
	log.Log(log.SchedApplication).Info("unreserving allocation from application",
		zap.String("appID", sa.ApplicationID),
		zap.Stringer("node", node),
		zap.Stringer("alloc", ask))
	if node == nil || ask == nil {
		return 0
	}
	sa.Lock()
	defer sa.Unlock()
	reserve, ok := sa.reservations[ask.allocationKey]
	if !ok {
		log.Log(log.SchedApplication).Debug("reservation not found on application",
			zap.String("appID", sa.ApplicationID),
			zap.String("allocationKey", ask.allocationKey))
		return 0
	}
	if reserve.nodeID != node.NodeID {
		log.Log(log.SchedApplication).Warn("UnReserve: provided info not consistent with reservation",
			zap.String("appID", sa.ApplicationID),
			zap.String("node", reserve.nodeID),
			zap.String("alloc", reserve.allocKey))
	}
	return sa.unReserveInternal(reserve)
}

// Unlocked version for UnReserve that really does the work.
// This is idempotent and will not fail
// Must only be called while holding the application lock.
func (sa *Application) unReserveInternal(reserve *reservation) int {
	// this should not happen
	if reserve == nil {
		return 0
	}
	// unReserve the node before removing from the app
	num := reserve.node.unReserve(reserve.alloc)
	// if the unreserve worked on the node check the app
	if _, found := sa.reservations[reserve.allocKey]; found {
		// worked on the node means either found or not but no error, log difference here
		if num == 0 {
			log.Log(log.SchedApplication).Info("reservation not found while removing from node, app has reservation",
				zap.String("appID", sa.ApplicationID),
				zap.String("nodeID", reserve.nodeID),
				zap.String("alloc", reserve.allocKey))
		}
		delete(sa.reservations, reserve.allocKey)
		log.Log(log.SchedApplication).Info("reservation removed successfully",
			zap.String("appID", sa.ApplicationID),
			zap.String("node", reserve.nodeID),
			zap.String("alloc", reserve.allocKey))
		return 1
	}
	// reservation was not found
	log.Log(log.SchedApplication).Info("reservation not found while removing from app",
		zap.String("appID", sa.ApplicationID),
		zap.String("node", reserve.nodeID),
		zap.String("alloc", reserve.allocKey),
		zap.Int("nodeReservationsRemoved", num))
	return 0
}

// canAllocationReserve Check if the allocation has already been reserved. An alloc can never reserve more than one node.
// No locking must be called while holding the lock
func (sa *Application) canAllocationReserve(alloc *Allocation) error {
	allocKey := alloc.GetAllocationKey()
	if alloc.IsAllocated() {
		log.Log(log.SchedApplication).Debug("allocation is marked as allocated, no reservation allowed",
			zap.String("allocationKey", allocKey))
		return common.ErrorReservingAlloc
	}
	reserved := sa.reservations[allocKey]
	if reserved != nil {
		log.Log(log.SchedApplication).Debug("reservation already exists",
			zap.String("allocKey", allocKey),
			zap.String("nodeID", reserved.nodeID))
		return common.ErrorDuplicateReserve
	}
	return nil
}

func (sa *Application) getOutstandingRequests(headRoom *resources.Resource, userHeadRoom *resources.Resource, total *[]*Allocation) {
	sa.RLock()
	defer sa.RUnlock()
	if sa.sortedRequests == nil {
		return
	}
	for _, request := range sa.sortedRequests {
		if request.IsAllocated() || !request.IsSchedulingAttempted() {
			continue
		}

		// ignore nil checks resource function calls are nil safe
		if headRoom.FitInMaxUndef(request.GetAllocatedResource()) && userHeadRoom.FitInMaxUndef(request.GetAllocatedResource()) {
			if !request.HasTriggeredScaleUp() && request.requiredNode == common.Empty && !sa.canReplace(request) {
				// if headroom is still enough for the resources
				*total = append(*total, request)
			}
			headRoom = resources.SubOnlyExisting(headRoom, request.GetAllocatedResource())
			userHeadRoom = resources.SubOnlyExisting(userHeadRoom, request.GetAllocatedResource())
		}
	}
}

// canReplace returns true if there is a placeholder for the task group available for the request.
// False for all other cases. Placeholder replacements are handled separately from normal allocations.
func (sa *Application) canReplace(request *Allocation) bool {
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
func (sa *Application) tryAllocate(headRoom *resources.Resource, allowPreemption bool, preemptionDelay time.Duration, preemptAttemptsRemaining *int, nodeIterator func() NodeIterator, fullNodeIterator func() NodeIterator, getNodeFn func(string) *Node) *AllocationResult {
	sa.Lock()
	defer sa.Unlock()
	if sa.sortedRequests == nil {
		return nil
	}
	// calculate the users' headroom, includes group check which requires the applicationID
	userHeadroom := ugm.GetUserManager().Headroom(sa.queuePath, sa.ApplicationID, sa.user)
	// get all the requests from the app sorted in order
	for _, request := range sa.sortedRequests {
		if request.IsAllocated() {
			continue
		}
		// check if there is a replacement possible
		if sa.canReplace(request) {
			continue
		}
		// check if this fits in the users' headroom first, if that fits check the queues' headroom
		// NOTE: preemption most likely will not help in this case. The chance that preemption helps is mall
		// as the preempted allocation must be for the same user in a different queue in the hierarchy...
		if !userHeadroom.FitInMaxUndef(request.GetAllocatedResource()) {
			request.LogAllocationFailure(NotEnoughUserQuota, true) // error message MUST be constant!
			request.setUserQuotaCheckFailed(userHeadroom)
			continue
		}
		request.setUserQuotaCheckPassed()
		request.SetSchedulingAttempted(true)

		// resource must fit in headroom otherwise skip the request (unless preemption could help)
		if !headRoom.FitInMaxUndef(request.GetAllocatedResource()) {
			// attempt preemption
			if allowPreemption && *preemptAttemptsRemaining > 0 {
				*preemptAttemptsRemaining--
				fullIterator := fullNodeIterator()
				if fullIterator != nil {
					if result, ok := sa.tryPreemption(headRoom, preemptionDelay, request, fullIterator, false); ok {
						// preemption occurred, and possibly reservation
						return result
					}
					request.LogAllocationFailure(common.PreemptionDoesNotHelp, true)
				}
			}
			request.LogAllocationFailure(NotEnoughQueueQuota, true) // error message MUST be constant!
			request.setHeadroomCheckFailed(headRoom, sa.queuePath)
			continue
		}
		request.setHeadroomCheckPassed(sa.queuePath)

		requiredNode := request.GetRequiredNode()
		// does request have any constraint to run on specific node?
		if requiredNode != "" {
			result := sa.tryRequiredNode(request, getNodeFn)
			if result != nil {
				return result
			}
			// it did not allocate or reserve: should only happen if the node is not registered yet
			// just continue with the next request
			continue
		}

		iterator := nodeIterator()
		if iterator != nil {
			if result := sa.tryNodes(request, iterator); result != nil {
				// have a candidate return it
				return result
			}

			// no nodes qualify, attempt preemption
			if allowPreemption && *preemptAttemptsRemaining > 0 {
				*preemptAttemptsRemaining--
				fullIterator := fullNodeIterator()
				if fullIterator != nil {
					if result, ok := sa.tryPreemption(headRoom, preemptionDelay, request, fullIterator, true); ok {
						// preemption occurred, and possibly reservation
						return result
					}
				}
				request.LogAllocationFailure(common.PreemptionDoesNotHelp, true)
			}
		}
	}
	// no requests fit, skip to next app
	return nil
}

// tryRequiredNode tries to place the allocation in the specific node that is set as the required node in the allocation.
// The first time the allocation is seen it will try to make the allocation on the node. If that does not work it will
// always trigger the reservation of the node.
func (sa *Application) tryRequiredNode(request *Allocation, getNodeFn func(string) *Node) *AllocationResult {
	requiredNode := request.GetRequiredNode()
	allocationKey := request.GetAllocationKey()
	// the iterator might not have the node we need as it could be reserved, or we have not added it yet
	node := getNodeFn(requiredNode)
	if node == nil {
		getRateLimitedAppLog().Info("required node is not found (could be transient)",
			zap.String("application ID", sa.ApplicationID),
			zap.String("allocationKey", allocationKey),
			zap.String("required node", requiredNode))
		return nil
	}
	// Are there any reservations on this node that does not specifically require this node?
	// Cancel any reservations to make room for the allocations that require the node
	var num int
	reservations := node.GetReservations()
	if len(reservations) > 0 {
		num = sa.cancelReservations(reservations)
	}
	_, thisReserved := sa.reservations[allocationKey]
	// now try the request, we don't care about predicate error messages here
	result, _ := sa.tryNode(node, request) //nolint:errcheck
	if result != nil {
		result.CancelledReservations = num
		// check if the node was reserved and we allocated after a release
		if thisReserved {
			log.Log(log.SchedApplication).Debug("allocation on required node after release",
				zap.String("appID", sa.ApplicationID),
				zap.String("nodeID", requiredNode),
				zap.String("allocationKey", allocationKey))
			result.ResultType = AllocatedReserved
			return result
		}
		log.Log(log.SchedApplication).Debug("allocation on required node is completed",
			zap.String("nodeID", node.NodeID),
			zap.String("allocationKey", allocationKey),
			zap.Stringer("resultType", result.ResultType))
		return result
	}
	// if this ask was already reserved we should not have deleted any reservations
	// we also do not need to send back a reservation result and just return nil to check the next ask
	if thisReserved {
		return nil
	}
	result = newReservedAllocationResult(node.NodeID, request)
	result.CancelledReservations = num
	return result
}

// cancelReservations will cancel all non required node reservations for a node. The list of reservations passed in is
// a copy of all reservations of a single node. This is called during the required node allocation cycle only.
// The returned int value is used to update the partition counter of active reservations.
func (sa *Application) cancelReservations(reservations []*reservation) int {
	var released, num int
	// un reserve all the apps that were reserved on the node
	for _, res := range reservations {
		// cleanup if the reservation does not have this node as a requirement
		if res.alloc.requiredNode != "" {
			continue
		}
		thisApp := res.app.ApplicationID == sa.ApplicationID
		if thisApp {
			num = sa.unReserveInternal(res)
			sa.queue.UnReserve(sa.ApplicationID, num)
		} else {
			num = res.app.UnReserve(res.node, res.alloc)
			res.app.GetQueue().UnReserve(res.app.ApplicationID, num)
		}
		log.Log(log.SchedApplication).Info("Cancelled reservation for required node allocation",
			zap.String("triggered by appID", sa.ApplicationID),
			zap.String("affected application ID", res.appID),
			zap.String("affected allocationKey", res.allocKey),
			zap.String("required node", res.nodeID),
			zap.Int("reservations count", num))
		released += num
	}
	return released
}

// tryPlaceholderAllocate tries to replace a placeholder that is allocated with a real allocation
//
//nolint:funlen
func (sa *Application) tryPlaceholderAllocate(nodeIterator func() NodeIterator, getNodeFn func(string) *Node) *AllocationResult {
	sa.Lock()
	defer sa.Unlock()
	// nothing to do if we have no placeholders allocated
	if resources.IsZero(sa.allocatedPlaceholder) || sa.sortedRequests == nil {
		return nil
	}
	// keep the first fits for later
	var phFit *Allocation
	var reqFit *Allocation
	// get all the requests from the app sorted in order
	for _, request := range sa.sortedRequests {
		// skip placeholders they follow standard allocation
		// this should also be part of a task group just make sure it is
		if request.IsPlaceholder() || request.GetTaskGroup() == "" || request.IsAllocated() {
			continue
		}
		// walk over the placeholders, allow for processing all as we can have multiple task groups
		phAllocs := sa.getPlaceholderAllocations()
		for _, ph := range phAllocs {
			// we could have already released preempted this placeholder and are waiting for the shim to confirm
			// and check that we have the correct task group before trying to swap
			if ph.IsReleased() || ph.IsPreempted() || request.GetTaskGroup() != ph.GetTaskGroup() {
				continue
			}
			// before we check anything we need to check the resources equality
			delta := resources.Sub(ph.GetAllocatedResource(), request.GetAllocatedResource())
			// Any negative value in the delta means that at least one of the requested resource in the real
			// allocation is larger than the placeholder. We need to cancel this placeholder and check the next
			// placeholder. This should trigger the removal of all the placeholder that are part of this task group.
			// All placeholders in the same task group are always the same size.
			if delta.HasNegativeValue() {
				log.Log(log.SchedApplication).Warn("releasing placeholder: real allocation is larger than placeholder",
					zap.Stringer("requested resource", request.GetAllocatedResource()),
					zap.String("placeholderKey", ph.GetAllocationKey()),
					zap.Stringer("placeholder resource", ph.GetAllocatedResource()))
				// release the placeholder and tell the RM
				ph.SetReleased(true)
				sa.notifyRMAllocationReleased([]*Allocation{ph}, si.TerminationType_TIMEOUT, "cancel placeholder: resource incompatible")
				sa.appEvents.SendPlaceholderLargerEvent(ph.taskGroupName, sa.ApplicationID, ph.allocationKey, request.GetAllocatedResource(), ph.GetAllocatedResource())
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
			if node != nil && node.preReserveConditions(request) == nil {
				_, err := sa.allocateAsk(request)
				if err != nil {
					log.Log(log.SchedApplication).Warn("allocation of ask failed unexpectedly",
						zap.Error(err))
				}
				// double link to make it easier to find
				// alloc (the real one) releases points to the placeholder in the releases list
				request.SetRelease(ph)
				// placeholder point to the real one in the releases list
				ph.SetRelease(request)
				// mark placeholder as released
				ph.SetReleased(true)
				// bind node here so it will be handled properly upon replacement
				request.SetBindTime(time.Now())
				request.SetNodeID(node.NodeID)
				request.SetInstanceType(node.GetInstanceType())
				return newReplacedAllocationResult(node.NodeID, request)
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
	var allocResult *AllocationResult
	if phFit != nil && reqFit != nil {
		resKey := reqFit.GetAllocationKey()
		iterator.ForEachNode(func(node *Node) bool {
			if !node.IsSchedulable() {
				log.Log(log.SchedApplication).Debug("skipping node for placeholder alloc as state is unschedulable",
					zap.String("allocationKey", resKey),
					zap.String("node", node.NodeID))
				return true
			}
			if !node.preAllocateCheck(reqFit.GetAllocatedResource(), resKey) {
				return true
			}
			// skip the node if conditions can not be satisfied
			if err := node.preAllocateConditions(reqFit); err != nil {
				return true
			}
			// update just the node to make sure we keep its spot
			// no queue update as we're releasing the placeholder and are just temp over the size
			if !node.TryAddAllocation(reqFit) {
				log.Log(log.SchedApplication).Debug("Node update failed unexpectedly",
					zap.String("applicationID", sa.ApplicationID),
					zap.Stringer("alloc", reqFit),
					zap.Stringer("placeholder", phFit))
				return false
			}
			_, err := sa.allocateAsk(reqFit)
			if err != nil {
				log.Log(log.SchedApplication).Warn("allocation of ask failed unexpectedly",
					zap.Error(err))
				// unwind node allocation
				_ = node.RemoveAllocation(resKey)
				return false
			}
			// allocation worked: on a non placeholder node update resultType and return
			// double link to make it easier to find
			// alloc (the real one) releases points to the placeholder in the releases list
			reqFit.SetRelease(phFit)
			// placeholder point to the real one in the releases list
			phFit.SetRelease(reqFit)
			// mark placeholder as released
			phFit.SetReleased(true)
			// bind node here so it will be handled properly upon replacement
			reqFit.SetBindTime(time.Now())
			reqFit.SetNodeID(node.NodeID)
			reqFit.SetInstanceType(node.GetInstanceType())
			result := newReplacedAllocationResult(node.NodeID, reqFit)

			allocResult = result
			return false
		})
	}
	// still nothing worked give up and hope the next round works
	return allocResult
}

// check ask against both user headRoom and queue headRoom
func (sa *Application) checkHeadRooms(ask *Allocation, userHeadroom *resources.Resource, headRoom *resources.Resource) bool {
	// check if this fits in the users' headroom first, if that fits check the queues' headroom
	return userHeadroom.FitInMaxUndef(ask.GetAllocatedResource()) && headRoom.FitInMaxUndef(ask.GetAllocatedResource())
}

// tryReservedAllocate tries allocating an outstanding reservation
func (sa *Application) tryReservedAllocate(headRoom *resources.Resource, nodeIterator func() NodeIterator) *AllocationResult {
	sa.Lock()
	defer sa.Unlock()
	// calculate the users' headroom, includes group check which requires the applicationID
	userHeadroom := ugm.GetUserManager().Headroom(sa.queuePath, sa.ApplicationID, sa.user)

	// process all outstanding reservations and pick the first one that fits
	for _, reserve := range sa.reservations {
		ask := sa.requests[reserve.allocKey]
		// sanity check and cleanup if needed
		if ask == nil || ask.IsAllocated() {
			var unreserveAsk *Allocation
			// if the ask was not found we need to construct one to unreserve
			if ask == nil {
				unreserveAsk = &Allocation{
					allocationKey: reserve.allocKey,
					applicationID: sa.ApplicationID,
					allocLog:      make(map[string]*AllocationLogEntry),
				}
			} else {
				unreserveAsk = ask
			}
			// remove the reservation as this should not be reserved
			return newUnreservedAllocationResult(reserve.nodeID, unreserveAsk)
		}

		if !sa.checkHeadRooms(ask, userHeadroom, headRoom) {
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
		// we don't care about predicate error messages here
		result, _ := sa.tryNode(reserve.node, ask) //nolint:errcheck

		// allocation worked fix the resultType and return
		if result != nil {
			result.ResultType = AllocatedReserved
			return result
		}
	}

	// try this on all other nodes
	for _, reserve := range sa.reservations {
		// Other nodes cannot be tried if a required node is requested
		alloc := reserve.alloc
		if alloc.GetRequiredNode() != "" {
			continue
		}
		iterator := nodeIterator()
		if iterator != nil {
			if !sa.checkHeadRooms(alloc, userHeadroom, headRoom) {
				continue
			}
			result := sa.tryNodesNoReserve(alloc, iterator, reserve.nodeID)
			// have a candidate return it, including the node that was reserved
			if result != nil {
				return result
			}
		}
	}
	return nil
}

func (sa *Application) tryPreemption(headRoom *resources.Resource, preemptionDelay time.Duration, ask *Allocation, iterator NodeIterator, nodesTried bool) (*AllocationResult, bool) {
	preemptor := NewPreemptor(sa, headRoom, preemptionDelay, ask, iterator, nodesTried)

	// validate prerequisites for preemption of an ask and mark ask for preemption if successful
	if !preemptor.CheckPreconditions() {
		ask.LogAllocationFailure(common.PreemptionPreconditionsFailed, true)
		return nil, false
	}

	// track time spent trying preemption
	tryPreemptionStart := time.Now()
	defer metrics.GetSchedulerMetrics().ObserveTryPreemptionLatency(tryPreemptionStart)

	// attempt preemption
	return preemptor.TryPreemption()
}

func (sa *Application) tryRequiredNodePreemption(reserve *reservation, ask *Allocation) bool {
	// try preemption and see if we can free up resource
	preemptor := NewRequiredNodePreemptor(reserve.node, ask)
	preemptor.filterAllocations()
	preemptor.sortAllocations()

	// Are there any victims/asks to preempt?
	victims := preemptor.GetVictims()
	if len(victims) > 0 {
		log.Log(log.SchedApplication).Info("Found victims for required node preemption",
			zap.String("ds allocation key", ask.GetAllocationKey()),
			zap.Int("no.of victims", len(victims)))
		for _, victim := range victims {
			if victimQueue := sa.queue.FindQueueByAppID(victim.GetApplicationID()); victimQueue != nil {
				victimQueue.IncPreemptingResource(victim.GetAllocatedResource())
			}
			victim.MarkPreempted()
		}
		ask.MarkTriggeredPreemption()
		sa.notifyRMAllocationReleased(victims, si.TerminationType_PREEMPTED_BY_SCHEDULER,
			"preempting allocations to free up resources to run daemon set ask: "+ask.GetAllocationKey())
		return true
	}
	ask.LogAllocationFailure(common.NoVictimForRequiredNode, true)
	ask.SendRequiredNodePreemptionFailedEvent(reserve.node.NodeID)
	return false
}

// tryNodesNoReserve tries all the nodes for a reserved request that have not been tried yet.
// This should never result in a reservation as the allocation is already reserved
func (sa *Application) tryNodesNoReserve(ask *Allocation, iterator NodeIterator, reservedNode string) *AllocationResult {
	var allocResult *AllocationResult
	iterator.ForEachNode(func(node *Node) bool {
		if !node.IsSchedulable() {
			log.Log(log.SchedApplication).Debug("skipping node for reserved ask as state is unschedulable",
				zap.String("allocationKey", ask.GetAllocationKey()),
				zap.String("node", node.NodeID))
			return true
		}
		// skip over the node if the resource does not fit the node or this is the reserved node.
		if !node.FitInNode(ask.GetAllocatedResource()) || node.NodeID == reservedNode {
			return true
		}
		// we don't care about predicate error messages here
		result, _ := sa.tryNode(node, ask) //nolint:errcheck
		// allocation worked: update resultType and return
		if result != nil {
			result.ResultType = AllocatedReserved
			result.ReservedNodeID = reservedNode
			allocResult = result
			return false
		}

		return true
	})

	return allocResult
}

// Try all the nodes for a request. The resultType is an allocation or reservation of a node.
// New allocations can only be reserved after a delay.
func (sa *Application) tryNodes(ask *Allocation, iterator NodeIterator) *AllocationResult {
	var nodeToReserve *Node
	scoreReserved := math.Inf(1)
	// check if the alloc is reserved or not
	allocKey := ask.GetAllocationKey()
	reserved := sa.reservations[allocKey]
	var allocResult *AllocationResult
	var predicateErrors map[string]int
	iterator.ForEachNode(func(node *Node) bool {
		// skip the node if the node is not schedulable
		if !node.IsSchedulable() {
			log.Log(log.SchedApplication).Debug("skipping node for ask as state is unschedulable",
				zap.String("allocationKey", allocKey),
				zap.String("node", node.NodeID))
			return true
		}
		// skip over the node if the resource does not fit the node at all.
		if !node.FitInNode(ask.GetAllocatedResource()) {
			return true
		}
		tryNodeStart := time.Now()
		result, err := sa.tryNode(node, ask)
		if err != nil {
			if predicateErrors == nil {
				predicateErrors = make(map[string]int)
			}
			predicateErrors[err.Error()]++
		}
		// allocation worked so return
		if result != nil {
			metrics.GetSchedulerMetrics().ObserveTryNodeLatency(tryNodeStart)
			// check if the alloc had a reservation: if it has set the resultType and return
			if reserved != nil {
				if reserved.nodeID != node.NodeID {
					// we have a different node reserved for this alloc
					log.Log(log.SchedApplication).Debug("allocate picking reserved alloc during non reserved allocate",
						zap.String("appID", sa.ApplicationID),
						zap.String("reserved nodeID", reserved.nodeID),
						zap.String("allocationKey", allocKey))
					result.ReservedNodeID = reserved.nodeID
				} else {
					// NOTE: this is a safeguard as reserved nodes should never be part of the iterator
					log.Log(log.SchedApplication).Debug("allocate found reserved alloc during non reserved allocate",
						zap.String("appID", sa.ApplicationID),
						zap.String("nodeID", node.NodeID),
						zap.String("allocationKey", allocKey))
				}
				result.ResultType = AllocatedReserved
				allocResult = result
				return false
			}
			// nothing reserved just return this as a normal alloc
			allocResult = result
			return false
		}
		// nothing allocated should we look at a reservation?
		askAge := time.Since(ask.GetCreateTime())
		if reserved == nil && askAge > reservationDelay {
			log.Log(log.SchedApplication).Debug("app reservation check",
				zap.String("allocationKey", allocKey),
				zap.Time("createTime", ask.GetCreateTime()),
				zap.Duration("askAge", askAge),
				zap.Duration("reservationDelay", reservationDelay))
			score := node.GetFitInScoreForAvailableResource(ask.GetAllocatedResource())
			// Record the best node so-far to reserve
			if score < scoreReserved {
				scoreReserved = score
				nodeToReserve = node
			}
		}
		return true
	})

	if allocResult != nil {
		return allocResult
	}

	if predicateErrors != nil {
		ask.SendPredicatesFailedEvent(predicateErrors)
	}

	// we have not allocated yet, check if we should reserve
	// NOTE: the node should not be reserved as the iterator filters them but we do not lock the nodes
	if nodeToReserve != nil && !nodeToReserve.IsReserved() {
		log.Log(log.SchedApplication).Debug("found candidate node for app reservation",
			zap.String("appID", sa.ApplicationID),
			zap.String("nodeID", nodeToReserve.NodeID),
			zap.String("allocationKey", allocKey),
			zap.Int("reservations", len(sa.reservations)))
		// skip the node if conditions can not be satisfied
		if nodeToReserve.preReserveConditions(ask) != nil {
			return nil
		}
		// return reservation allocation and mark it as a reservation
		return newReservedAllocationResult(nodeToReserve.NodeID, ask)
	}
	// ask does not fit, skip to next ask
	return nil
}

// tryNode tries allocating on one specific node
func (sa *Application) tryNode(node *Node, ask *Allocation) (*AllocationResult, error) {
	toAllocate := ask.GetAllocatedResource()
	allocationKey := ask.GetAllocationKey()
	// create the key for the reservation
	if !node.preAllocateCheck(toAllocate, allocationKey) {
		// skip schedule onto node
		return nil, nil
	}
	// skip the node if conditions can not be satisfied
	if err := node.preAllocateConditions(ask); err != nil {
		return nil, err
	}

	// everything OK really allocate
	if node.TryAddAllocation(ask) {
		if err := sa.queue.TryIncAllocatedResource(ask.GetAllocatedResource()); err != nil {
			log.Log(log.SchedApplication).DPanic("queue update failed unexpectedly",
				zap.Error(err))
			// revert the node update
			node.RemoveAllocation(allocationKey)
			return nil, nil
		}
		// mark this alloc as allocated
		_, err := sa.allocateAsk(ask)
		if err != nil {
			log.Log(log.SchedApplication).Warn("allocation of alloc failed unexpectedly",
				zap.Error(err))
		}
		// all is OK, last update for the app
		result := newAllocatedAllocationResult(node.NodeID, ask)
		sa.addAllocationInternal(result.ResultType, ask)
		return result, nil
	}
	return nil, nil
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
	// here we can make sure the queue is not empty
	metrics.GetQueueMetrics(queue.QueuePath).IncQueueApplicationsNew()
	metrics.GetSchedulerMetrics().IncTotalApplicationsNew()
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

func (sa *Application) StartTime() time.Time {
	sa.RLock()
	defer sa.RUnlock()
	return sa.startTime
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
func (sa *Application) GetAllRequests() []*Allocation {
	sa.RLock()
	defer sa.RUnlock()
	return sa.getAllRequestsInternal()
}

func (sa *Application) getAllRequestsInternal() []*Allocation {
	var requests []*Allocation
	for _, req := range sa.requests {
		requests = append(requests, req)
	}
	return requests
}

// Add a new Allocation to the application
func (sa *Application) AddAllocation(alloc *Allocation) {
	sa.Lock()
	defer sa.Unlock()
	sa.addAllocationInternal(Allocated, alloc)
}

// Add the Allocation to the application
// No locking must be called while holding the lock
func (sa *Application) addAllocationInternal(allocType AllocationResultType, alloc *Allocation) {
	// placeholder allocations do not progress the state of the app and are tracked in a separate total
	if alloc.IsPlaceholder() {
		// when we have the first placeholder allocation start the placeholder timer.
		// It will start to use the resources only after the first allocation, so we will count the time from this point.
		// Also this is the first stable point on the placeholder handling, what is easy to explain and troubleshoot
		// If we would start it when we just try to allocate, that is something very unstable, and we don't really have any
		// impact on what is happening until this point
		if resources.IsZero(sa.allocatedPlaceholder) {
			sa.hasPlaceholderAlloc = true
			sa.initPlaceholderTimer()
		}
		// User resource usage needs to be updated even during resource allocation happen for ph's itself even though state change would happen only after all ph allocation completes.
		sa.incUserResourceUsage(alloc.GetAllocatedResource())
		sa.allocatedPlaceholder = resources.Add(sa.allocatedPlaceholder, alloc.GetAllocatedResource())
		sa.maxAllocatedResource = resources.ComponentWiseMax(sa.allocatedPlaceholder, sa.maxAllocatedResource)

		// If there are no more placeholder to allocate we should move state
		if resources.Equals(sa.allocatedPlaceholder, sa.placeholderAsk) {
			if err := sa.HandleApplicationEvent(RunApplication); err != nil {
				log.Log(log.SchedApplication).Error("Unexpected app state change failure while adding allocation",
					zap.String("currentState", sa.stateMachine.Current()),
					zap.Error(err))
			}
		}
	} else {
		// skip the state change if this is the first replacement allocation as we have done that change
		// already when the last placeholder was allocated
		// special case COMPLETING: gang with only one placeholder moves to COMPLETING and causes orphaned
		// allocations
		if allocType != Replaced || !resources.IsZero(sa.allocatedResource) || sa.IsCompleting() {
			// progress the state based on where we are, we should never fail in this case
			// keep track of a failure in log.
			if err := sa.HandleApplicationEvent(RunApplication); err != nil {
				log.Log(log.SchedApplication).Error("Unexpected app state change failure while adding allocation",
					zap.String("currentState", sa.stateMachine.Current()),
					zap.Error(err))
			}
		}
		sa.incUserResourceUsage(alloc.GetAllocatedResource())
		sa.allocatedResource = resources.Add(sa.allocatedResource, alloc.GetAllocatedResource())
		sa.maxAllocatedResource = resources.ComponentWiseMax(sa.allocatedResource, sa.maxAllocatedResource)
	}
	if alloc.createTime.Before(sa.submissionTime) {
		sa.submissionTime = alloc.createTime
	}
	sa.appEvents.SendNewAllocationEvent(sa.ApplicationID, alloc.allocationKey, alloc.GetAllocatedResource())
	sa.allocations[alloc.GetAllocationKey()] = alloc
}

// Increase user resource usage
// No locking must be called while holding the lock
func (sa *Application) incUserResourceUsage(resource *resources.Resource) {
	ugm.GetUserManager().IncreaseTrackedResource(sa.queuePath, sa.ApplicationID, resource, sa.user)
}

// Decrease user resource usage
// No locking must be called while holding the lock
func (sa *Application) decUserResourceUsage(resource *resources.Resource, removeApp bool) {
	ugm.GetUserManager().DecreaseTrackedResource(sa.queuePath, sa.ApplicationID, resource, sa.user, removeApp)
}

// Track used and preempted resources
func (sa *Application) trackCompletedResource(info *Allocation) {
	switch {
	case info.IsPreempted():
		sa.updatePreemptedResource(info)
	case info.IsPlaceholder():
		sa.updatePlaceholderResource(info)
	default:
		sa.updateUsedResource(info)
	}
}

// When the resource allocated with this allocation is to be removed,
// have the usedResource to aggregate the resource used by this allocation
func (sa *Application) updateUsedResource(info *Allocation) {
	sa.usedResource.AggregateTrackedResource(info.GetInstanceType(),
		info.GetAllocatedResource(), info.GetBindTime())
}

// When the placeholder allocated with this allocation is to be removed,
// have the placeholderResource to aggregate the resource used by this allocation
func (sa *Application) updatePlaceholderResource(info *Allocation) {
	sa.placeholderResource.AggregateTrackedResource(info.GetInstanceType(),
		info.GetAllocatedResource(), info.GetBindTime())
}

// When the resource allocated with this allocation is to be preempted,
// have the preemptedResource to aggregate the resource used by this allocation
func (sa *Application) updatePreemptedResource(info *Allocation) {
	sa.preemptedResource.AggregateTrackedResource(info.GetInstanceType(),
		info.GetAllocatedResource(), info.GetBindTime())
}

// ReplaceAllocation removes the placeholder from the allocation list and replaces it with the real allocation.
// If no replacing allocation is linked to the placeholder it will still be removed from the application.
// Queue and Node objects are updated by the caller.
func (sa *Application) ReplaceAllocation(allocationKey string) *Allocation {
	sa.Lock()
	defer sa.Unlock()
	// remove the placeholder that was just confirmed by the shim
	ph := sa.removeAllocationInternal(allocationKey, si.TerminationType_PLACEHOLDER_REPLACED)
	// this has already been replaced, or it is a duplicate message from the shim just ignore
	if ph == nil {
		return nil
	}
	// ph is the placeholder, the releases entry points to the real allocation we need to swap in
	alloc := ph.GetRelease()
	if alloc == nil {
		log.Log(log.SchedApplication).Warn("Placeholder replaced without replacement allocation",
			zap.String("applicationID", sa.ApplicationID),
			zap.Stringer("placeholder", ph))
		return ph
	}
	// update the replacing allocation
	// we double linked the real and placeholder allocation
	alloc.SetPlaceholderUsed(true)
	alloc.SetPlaceholderCreateTime(ph.GetCreateTime())
	alloc.SetBindTime(time.Now())
	sa.addAllocationInternal(Replaced, alloc)
	// order is important: clean up the allocation after adding it to the app
	// we need the original Replaced allocation resultType.
	alloc.ClearRelease()
	return ph
}

// RemoveAllocation removes the Allocation from the application.
// Return the allocation that was removed or nil if not found.
func (sa *Application) RemoveAllocation(allocationKey string, releaseType si.TerminationType) *Allocation {
	sa.Lock()
	defer sa.Unlock()
	return sa.removeAllocationInternal(allocationKey, releaseType)
}

// removeAllocationInternal removes the Allocation from the application.
// Returns the allocation that was removed or nil if not found.
// No locking must be called while holding the application lock.
func (sa *Application) removeAllocationInternal(allocationKey string, releaseType si.TerminationType) *Allocation {
	alloc := sa.allocations[allocationKey]

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
		// update based on termination type: everything is counted as a timeout except for a real replace
		if sa.placeholderData != nil {
			if phData, ok := sa.placeholderData[alloc.taskGroupName]; ok {
				if releaseType == si.TerminationType_PLACEHOLDER_REPLACED {
					phData.Replaced++
				} else {
					phData.TimedOut++
				}
			}
		}
		// as and when every ph gets removed (for replacement), resource usage would be reduced.
		// When real allocation happens as part of replacement, usage would be increased again with real alloc resource
		sa.allocatedPlaceholder = resources.Sub(sa.allocatedPlaceholder, alloc.GetAllocatedResource())
		sa.allocatedPlaceholder.Prune()

		// if all the placeholders are replaced, clear the placeholder timer
		if resources.IsZero(sa.allocatedPlaceholder) {
			sa.clearPlaceholderTimer()
			sa.hasPlaceholderAlloc = false
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
		// Aggregate the resources used by this alloc to the application's resource tracker
		sa.trackCompletedResource(alloc)

		sa.decUserResourceUsage(alloc.GetAllocatedResource(), removeApp)
	} else {
		sa.allocatedResource = resources.Sub(sa.allocatedResource, alloc.GetAllocatedResource())
		sa.allocatedResource.Prune()

		// Aggregate the resources used by this alloc to the application's resource tracker
		sa.trackCompletedResource(alloc)

		// When the resource trackers are zero we should not expect anything to come in later.
		if sa.hasZeroAllocations() {
			removeApp = true
			event = CompleteApplication
			eventWarning = "Application state not changed to Completing while removing an allocation"
		}
		sa.decUserResourceUsage(alloc.GetAllocatedResource(), removeApp)
	}
	if event != EventNotNeeded {
		if err := sa.HandleApplicationEvent(event); err != nil {
			log.Log(log.SchedApplication).Warn(eventWarning,
				zap.String("currentState", sa.CurrentState()),
				zap.Stringer("event", event),
				zap.Error(err))
		}
	}
	delete(sa.allocations, allocationKey)
	sa.appEvents.SendRemoveAllocationEvent(sa.ApplicationID, alloc.allocationKey, alloc.GetAllocatedResource(), releaseType)
	return alloc
}

func (sa *Application) updateAskMaxPriority() {
	value := configs.MinPriority
	for _, v := range sa.requests {
		if v.IsAllocated() {
			continue
		}
		value = max(value, v.GetPriority())
	}
	sa.askMaxPriority = value
	sa.queue.UpdateApplicationPriority(sa.ApplicationID, value)
}

func (sa *Application) hasZeroAllocations() bool {
	return resources.IsZero(sa.pending) && resources.IsZero(sa.allocatedResource)
}

// RemoveAllAllocations removes all allocations from the application.
// All allocations that have been removed are returned.
func (sa *Application) RemoveAllAllocations() []*Allocation {
	sa.Lock()
	defer sa.Unlock()

	allocationsToRelease := make([]*Allocation, 0)
	for _, alloc := range sa.allocations {
		// update placeholder tracking data
		if alloc.IsPlaceholder() && sa.placeholderData != nil {
			if phData, ok := sa.placeholderData[alloc.taskGroupName]; ok {
				phData.TimedOut++
			}
		}
		allocationsToRelease = append(allocationsToRelease, alloc)
		// Aggregate the resources used by this alloc to the application's user resource tracker
		sa.trackCompletedResource(alloc)
		sa.appEvents.SendRemoveAllocationEvent(sa.ApplicationID, alloc.allocationKey, alloc.GetAllocatedResource(), si.TerminationType_STOPPED_BY_RM)
	}

	// if an app doesn't have any allocations and the user doesn't have other applications,
	// the user tracker is nonexistent. We don't want to decrease resource usage in this case.
	if ugm.GetUserManager().GetUserTracker(sa.user.User) != nil && resources.IsZero(sa.pending) {
		sa.decUserResourceUsage(resources.Add(sa.allocatedResource, sa.allocatedPlaceholder), true)
	}
	// cleanup allocated resource for app (placeholders and normal)
	sa.allocatedResource = resources.NewResource()
	sa.allocatedPlaceholder = resources.NewResource()
	sa.allocations = make(map[string]*Allocation)
	// When the resource trackers are zero we should not expect anything to come in later.
	if resources.IsZero(sa.pending) {
		if err := sa.HandleApplicationEvent(CompleteApplication); err != nil {
			log.Log(log.SchedApplication).Warn("Application state not changed to Completing while removing all allocations",
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
	tagVal := ""
	for key, val := range sa.tags {
		if strings.EqualFold(key, tag) {
			tagVal = val
			break
		}
	}
	return tagVal
}

func (sa *Application) IsCreateForced() bool {
	return common.IsAppCreationForced(sa.tags)
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
func (sa *Application) notifyRMAllocationReleased(released []*Allocation, terminationType si.TerminationType, message string) {
	// only generate event if needed
	if len(released) == 0 || sa.rmEventHandler == nil {
		return
	}
	c := make(chan *rmevent.Result)
	releaseEvent := &rmevent.RMReleaseAllocationEvent{
		ReleasedAllocations: make([]*si.AllocationRelease, 0),
		RmID:                sa.rmID,
		Channel:             c,
	}
	for _, alloc := range released {
		releaseEvent.ReleasedAllocations = append(releaseEvent.ReleasedAllocations, &si.AllocationRelease{
			ApplicationID:   alloc.GetApplicationID(),
			PartitionName:   sa.Partition,
			AllocationKey:   alloc.GetAllocationKey(),
			TerminationType: terminationType,
			Message:         message,
		})
	}
	sa.rmEventHandler.HandleEvent(releaseEvent)
	// Wait from channel
	result := <-c
	if result.Succeeded {
		log.Log(log.SchedApplication).Debug("Successfully synced shim on released allocations. response: " + result.Reason)
	} else {
		log.Log(log.SchedApplication).Info("failed to sync shim on released allocations")
	}
}

func (sa *Application) IsAllocationAssignedToApp(alloc *Allocation) bool {
	sa.RLock()
	defer sa.RUnlock()
	_, ok := sa.allocations[alloc.GetAllocationKey()]
	return ok
}

func (sa *Application) GetRejectedMessage() string {
	sa.RLock()
	defer sa.RUnlock()
	return sa.rejectedMessage
}

func (sa *Application) addPlaceholderData(ask *Allocation) {
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
	sa.requests = make(map[string]*Allocation)
	sa.sortedRequests = nil
}

func (sa *Application) cleanupTrackedResource() {
	sa.usedResource = nil
	sa.placeholderResource = nil
	sa.preemptedResource = nil
}

// GetApplicationSummary locked version to get the application summary
// Exposed for test only
func (sa *Application) GetApplicationSummary(rmID string) *ApplicationSummary {
	sa.RLock()
	defer sa.RUnlock()
	return sa.getApplicationSummary(rmID)
}

func (sa *Application) getApplicationSummary(rmID string) *ApplicationSummary {
	return &ApplicationSummary{
		ApplicationID:       sa.ApplicationID,
		SubmissionTime:      sa.submissionTime,
		StartTime:           sa.startTime,
		FinishTime:          sa.finishedTime,
		User:                sa.user.User,
		Queue:               sa.queuePath,
		State:               sa.stateMachine.Current(),
		RmID:                rmID,
		ResourceUsage:       sa.usedResource.Clone(),
		PreemptedResource:   sa.preemptedResource.Clone(),
		PlaceholderResource: sa.placeholderResource.Clone(),
	}
}

// LogAppSummary log the summary details for the application if it has run at any point in time.
// The application summary only contains correct data when the application is in the Completed state.
// Logging the data in any other state will show incomplete or inconsistent data.
// After the data is logged the objects are cleaned up to lower overhead of Completed application tracking.
func (sa *Application) LogAppSummary(rmID string) {
	sa.Lock()
	defer sa.Unlock()
	if !sa.startTime.IsZero() {
		appSummary := sa.getApplicationSummary(rmID)
		appSummary.DoLogging()
	}
	sa.cleanupTrackedResource()
}

// GetTrackedDAOMap returns the tracked resources type specified in which as a DAO similar to the normal resources.
func (sa *Application) GetTrackedDAOMap(which string) map[string]map[string]int64 {
	sa.RLock()
	defer sa.RUnlock()
	switch which {
	case "usedResource":
		return sa.usedResource.DAOMap()
	case "preemptedResource":
		return sa.preemptedResource.DAOMap()
	case "placeholderResource":
		return sa.placeholderResource.DAOMap()
	default:
		return map[string]map[string]int64{}
	}
}

func (sa *Application) HasPlaceholderAllocation() bool {
	sa.RLock()
	defer sa.RUnlock()
	return sa.hasPlaceholderAlloc
}

// SetCompletingTimeout should be used for testing only.
func SetCompletingTimeout(duration time.Duration) {
	completingTimeout = duration
}

// SetTimedOutPlaceholder should be used for testing only.
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

// getRateLimitedAppLog lazy initializes the application logger the first time is needed.
func getRateLimitedAppLog() *log.RateLimitedLogger {
	initAppLogOnce.Do(func() {
		rateLimitedAppLog = log.NewRateLimitedLogger(log.SchedApplication, time.Second)
	})
	return rateLimitedAppLog
}

func (sa *Application) updateRunnableStatus(runnableInQueue, runnableByUserLimit bool) {
	sa.Lock()
	defer sa.Unlock()
	if sa.runnableInQueue != runnableInQueue {
		if runnableInQueue {
			log.Log(log.SchedApplication).Info("Application is now runnable in queue",
				zap.String("appID", sa.ApplicationID),
				zap.String("queue", sa.queuePath))
			sa.appEvents.SendAppRunnableInQueueEvent(sa.ApplicationID)
		} else {
			log.Log(log.SchedApplication).Info("Maximum number of running applications reached the queue limit",
				zap.String("appID", sa.ApplicationID),
				zap.String("queue", sa.queuePath))
			sa.appEvents.SendAppNotRunnableInQueueEvent(sa.ApplicationID)
		}
	}
	sa.runnableInQueue = runnableInQueue

	if sa.runnableByUserLimit != runnableByUserLimit {
		if runnableByUserLimit {
			log.Log(log.SchedApplication).Info("Application is now runnable based on user/group quota",
				zap.String("appID", sa.ApplicationID),
				zap.String("queue", sa.queuePath),
				zap.String("user", sa.user.User),
				zap.Strings("groups", sa.user.Groups))
			sa.appEvents.SendAppRunnableQuotaEvent(sa.ApplicationID)
		} else {
			log.Log(log.SchedApplication).Info("Maximum number of running applications reached the user/group limit",
				zap.String("appID", sa.ApplicationID),
				zap.String("queue", sa.queuePath),
				zap.String("user", sa.user.User),
				zap.Strings("groups", sa.user.Groups))
			sa.appEvents.SendAppNotRunnableQuotaEvent(sa.ApplicationID)
		}
	}
	sa.runnableByUserLimit = runnableByUserLimit
}

// GetGuaranteedResource returns the guaranteed resource that is set in the application tags
func (sa *Application) GetGuaranteedResource() *resources.Resource {
	return sa.getResourceFromTags(siCommon.AppTagNamespaceResourceGuaranteed)
}

// GetMaxResource returns the max resource that is set in the application tags
func (sa *Application) GetMaxResource() *resources.Resource {
	return sa.getResourceFromTags(siCommon.AppTagNamespaceResourceQuota)
}

// GetMaxApps returns the max apps that is set in the application tags
func (sa *Application) GetMaxApps() uint64 {
	return sa.getUint64Tag(siCommon.AppTagNamespaceResourceMaxApps)
}

func (sa *Application) getUint64Tag(tag string) uint64 {
	value := sa.GetTag(tag)
	if value == "" {
		return 0
	}
	uintValue, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		log.Log(log.SchedApplication).Warn("application tag conversion failure",
			zap.String("tag", tag),
			zap.String("json string", value),
			zap.Error(err))
		return 0
	}
	return uintValue
}

func (sa *Application) getResourceFromTags(tag string) *resources.Resource {
	value := sa.GetTag(tag)
	if value == "" {
		return nil
	}

	resource, err := resources.NewResourceFromString(value)
	if err != nil {
		log.Log(log.SchedQueue).Warn("application resource conversion failure",
			zap.String("tag", tag),
			zap.String("json string", value),
			zap.Error(err))
	} else if !resources.StrictlyGreaterThanZero(resource) {
		log.Log(log.SchedQueue).Warn("resource quantities should be greater than zero",
			zap.Stringer("resource", resource))
		resource = nil
	}

	return resource
}

func (sa *Application) GetSubmissionTime() time.Time {
	sa.RLock()
	defer sa.RUnlock()
	return sa.submissionTime
}
