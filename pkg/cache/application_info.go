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

package cache

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

var (
	startingTimeout = time.Minute * 5
)

/* Related to applications */
type ApplicationInfo struct {
	ApplicationID  string
	Partition      string
	QueueName      string
	SubmissionTime int64

	// Private fields need protection
	user              security.UserGroup         // owner of the application
	tags              map[string]string          // application tags used in scheduling
	leafQueue         *QueueInfo                 // link to the leaf queue
	allocatedResource *resources.Resource        // total allocated resources
	allocations       map[string]*AllocationInfo // list of all allocations
	stateMachine      *fsm.FSM                   // application state machine
	stateTimer        *time.Timer                // timer for state time

	sync.RWMutex
}

// Create a new application
func NewApplicationInfo(appID, partition, queueName string, ugi security.UserGroup, tags map[string]string) *ApplicationInfo {
	return &ApplicationInfo{
		ApplicationID:     appID,
		Partition:         partition,
		QueueName:         queueName,
		SubmissionTime:    time.Now().UnixNano(),
		tags:              tags,
		user:              ugi,
		allocatedResource: resources.NewResource(),
		allocations:       make(map[string]*AllocationInfo),
		stateMachine:      newAppState(),
	}
}

// Return the current allocations for the application.
func (ai *ApplicationInfo) GetAllAllocations() []*AllocationInfo {
	ai.RLock()
	defer ai.RUnlock()

	var allocations []*AllocationInfo
	for _, alloc := range ai.allocations {
		allocations = append(allocations, alloc)
	}
	return allocations
}

// Return the current state or a checked specific state for the application.
// The state machine handles the locking.
func (ai *ApplicationInfo) GetApplicationState() string {
	return ai.stateMachine.Current()
}

func (ai *ApplicationInfo) IsStarting() bool {
	return ai.stateMachine.Is(Starting.String())
}

func (ai *ApplicationInfo) IsAccepted() bool {
	return ai.stateMachine.Is(Accepted.String())
}

func (ai *ApplicationInfo) IsNew() bool {
	return ai.stateMachine.Is(New.String())
}

func (ai *ApplicationInfo) IsRunning() bool {
	return ai.stateMachine.Is(Running.String())
}

func (ai *ApplicationInfo) IsWaiting() bool {
	return ai.stateMachine.Is(Waiting.String())
}

// Handle the state event for the application.
// The state machine handles the locking.
func (ai *ApplicationInfo) HandleApplicationEvent(event applicationEvent) error {
	err := ai.stateMachine.Event(event.String(), ai)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() == "no transition" {
		return nil
	}
	return err
}

// Set the starting timer to make sure the application will not get stuck in a starting state too long.
// This prevents an app from not progressing to Running when it only has 1 allocation.
// Called when entering the Starting state by the state machine.
func (ai *ApplicationInfo) setStartingTimer() {
	ai.Lock()
	defer ai.Unlock()

	log.Logger().Debug("Application Starting state timer initiated",
		zap.String("appID", ai.ApplicationID),
		zap.Duration("timeout", startingTimeout))
	ai.stateTimer = time.AfterFunc(startingTimeout, ai.timeOutStarting)
}

// Clear the starting timer. If the application has progressed out of the starting state we need to stop the
// timer and clean up.
// Called when leaving the Starting state by the state machine.
func (ai *ApplicationInfo) clearStartingTimer() {
	ai.Lock()
	defer ai.Unlock()

	ai.stateTimer.Stop()
	ai.stateTimer = nil
}

// In case of state aware scheduling we do not want to get stuck in starting as we might have an application that only
// requires one allocation or is really slow asking for more than the first one.
// This will progress the state of the application from Starting to Running
func (ai *ApplicationInfo) timeOutStarting() {
	// make sure we are still in the right state
	// we could have been killed or something might have happened while waiting for a lock
	if ai.IsStarting() {
		log.Logger().Warn("Application in starting state timed out: auto progress",
			zap.String("applicationID", ai.ApplicationID),
			zap.String("state", ai.stateMachine.Current()))

		//nolint: errcheck
		_ = ai.HandleApplicationEvent(RunApplication)
	}
}

// Return the total allocated resources for the application.
func (ai *ApplicationInfo) GetAllocatedResource() *resources.Resource {
	ai.RLock()
	defer ai.RUnlock()

	return ai.allocatedResource.Clone()
}

// Set the leaf queue the application runs in. Update the queue name also to match as this might be different from the
// queue that was given when submitting the application.
func (ai *ApplicationInfo) SetQueue(leaf *QueueInfo) {
	ai.Lock()
	defer ai.Unlock()

	ai.leafQueue = leaf
	ai.QueueName = leaf.GetQueuePath()
}

// Add a new allocation to the application
func (ai *ApplicationInfo) addAllocation(info *AllocationInfo) {
	// progress the state based on where we are, we should never fail in this case
	// keep track of a failure
	if err := ai.HandleApplicationEvent(RunApplication); err != nil {
		log.Logger().Error("Unexpected app state change failure while adding allocation",
			zap.String("currentState", ai.stateMachine.Current()),
			zap.Error(err))
	}
	// add the allocation
	ai.Lock()
	defer ai.Unlock()

	ai.allocations[info.AllocationProto.UUID] = info
	ai.allocatedResource = resources.Add(ai.allocatedResource, info.AllocatedResource)
}

// Remove a specific allocation from the application.
// Return the allocation that was removed.
func (ai *ApplicationInfo) removeAllocation(uuid string) *AllocationInfo {
	ai.Lock()
	defer ai.Unlock()

	alloc := ai.allocations[uuid]

	if alloc != nil {
		// When app has the allocation, update map, and update allocated resource of the app
		ai.allocatedResource = resources.Sub(ai.allocatedResource, alloc.AllocatedResource)
		delete(ai.allocations, uuid)
		return alloc
	}

	return nil
}

// Remove all allocations from the application.
// All allocations that have been removed are returned.
func (ai *ApplicationInfo) removeAllAllocations() []*AllocationInfo {
	ai.Lock()
	defer ai.Unlock()

	allocationsToRelease := make([]*AllocationInfo, 0)
	for _, alloc := range ai.allocations {
		allocationsToRelease = append(allocationsToRelease, alloc)
	}
	// cleanup allocated resource for app
	ai.allocatedResource = resources.NewResource()
	ai.allocations = make(map[string]*AllocationInfo)

	return allocationsToRelease
}

// get a copy of the user details for the application
func (ai *ApplicationInfo) GetUser() security.UserGroup {
	ai.Lock()
	defer ai.Unlock()

	return ai.user
}

// Get a tag from the application
// Note: Tags are not case sensitive
func (ai *ApplicationInfo) GetTag(tag string) string {
	ai.Lock()
	defer ai.Unlock()

	tagVal := ""
	for key, val := range ai.tags {
		if strings.EqualFold(key, tag) {
			tagVal = val
			break
		}
	}
	return tagVal
}

func (ai *ApplicationInfo) String() string {
	ai.RLock()
	defer ai.RUnlock()
	return fmt.Sprintf("{ApplicationID: %s, Partition: %s, QueueName: %s, SubmissionTime: %x}",
		ai.ApplicationID, ai.Partition, ai.QueueName, ai.SubmissionTime)
}
