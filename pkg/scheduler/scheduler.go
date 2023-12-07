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
	"reflect"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/handler"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// Main Scheduler service that starts the needed sub services
type Scheduler struct {
	clusterContext  *ClusterContext  // main context
	pendingEvents   chan interface{} // queue for events
	activityPending chan bool        // activity pending channel
	stop            chan struct{}    // channel to signal stop request
	healthChecker   *HealthChecker
	nodesMonitor    *nodesResourceUsageMonitor
}

func NewScheduler() *Scheduler {
	m := &Scheduler{}
	m.clusterContext = newClusterContext()
	m.pendingEvents = make(chan interface{}, 1024*1024)
	m.activityPending = make(chan bool, 1)
	m.stop = make(chan struct{})
	return m
}

// Start service
func (s *Scheduler) StartService(handlers handler.EventHandlers, manualSchedule bool) {
	// set the proxy handler in the context
	s.clusterContext.setEventHandler(handlers.RMProxyEventHandler)

	// Start event handlers
	go s.handleRMEvent()

	// Start resource monitor if necessary (majorly for testing)
	s.nodesMonitor = newNodesResourceUsageMonitor(s.clusterContext)
	s.nodesMonitor.start()

	// Start health check periodically
	s.healthChecker = NewHealthChecker(s.clusterContext)
	s.healthChecker.Start()

	if !manualSchedule {
		go s.internalSchedule()
		go s.internalInspectOutstandingRequests()
	}
}

// Internal start scheduling service
func (s *Scheduler) internalSchedule() {
	for {
		select {
		case <-s.stop:
			return
		case <-s.activityPending:
			// activity pending
		case <-time.After(100 * time.Millisecond):
			// timeout, run scheduler anyway
		}

		if s.clusterContext.schedule() {
			s.registerActivity()
		}
	}
}

func (s *Scheduler) internalInspectOutstandingRequests() {
	for {
		select {
		case <-s.stop:
			return
		case <-time.After(time.Second):
			s.inspectOutstandingRequests()
		}
	}
}

// Implement methods for Scheduler events
func (s *Scheduler) HandleEvent(ev interface{}) {
	enqueueAndCheckFull(s.pendingEvents, ev)
}

func enqueueAndCheckFull(queue chan interface{}, ev interface{}) {
	select {
	case queue <- ev:
		log.Log(log.Scheduler).Debug("enqueued event",
			zap.Stringer("eventType", reflect.TypeOf(ev)),
			zap.Any("event", ev),
			zap.Int("currentQueueSize", len(queue)))
	default:
		log.Log(log.Scheduler).DPanic("failed to enqueue event",
			zap.Stringer("event", reflect.TypeOf(ev)))
	}
}

func (s *Scheduler) handleRMEvent() {
	for {
		select {
		case ev := <-s.pendingEvents:
			switch v := ev.(type) {
			case *rmevent.RMUpdateAllocationEvent:
				s.clusterContext.handleRMUpdateAllocationEvent(v)
			case *rmevent.RMUpdateApplicationEvent:
				s.clusterContext.handleRMUpdateApplicationEvent(v)
			case *rmevent.RMUpdateNodeEvent:
				s.clusterContext.handleRMUpdateNodeEvent(v)
			case *rmevent.RMPartitionsRemoveEvent:
				s.clusterContext.removePartitionsByRMID(v)
			case *rmevent.RMRegistrationEvent:
				s.clusterContext.processRMRegistrationEvent(v)
			case *rmevent.RMConfigUpdateEvent:
				s.clusterContext.processRMConfigUpdateEvent(v)
			default:
				log.Log(log.Scheduler).Error("Received type is not an acceptable type for RM event.",
					zap.Stringer("received type", reflect.TypeOf(v)))
			}
			s.registerActivity()
		case <-s.stop:
			return
		}
	}
}

// registerActivity is used to notify the scheduler that some activity that may impact scheduling results has occurred.
func (s *Scheduler) registerActivity() {
	select {
	case s.activityPending <- true:
		// activity registered
	default:
		// buffer is full, activity will be processed at the next available opportunity
	}
}

// inspect on the outstanding requests for each of the queues,
// update request state accordingly to shim if needed.
// this function filters out all outstanding requests that being
// skipped due to insufficient cluster resources and update the
// state through the ContainerSchedulingStateUpdaterPlugin in order
// to trigger the auto-scaling.
func (s *Scheduler) inspectOutstandingRequests() {
	log.Log(log.Scheduler).Debug("inspect outstanding requests")
	// schedule each partition defined in the cluster
	for _, psc := range s.clusterContext.GetPartitionMapClone() {
		requests := psc.calculateOutstandingRequests()
		if len(requests) > 0 {
			for _, ask := range requests {
				log.Log(log.Scheduler).Debug("outstanding request",
					zap.String("appID", ask.GetApplicationID()),
					zap.String("allocationKey", ask.GetAllocationKey()))
				// these asks are queue outstanding requests,
				// they can fit into the max head room, but they are pending because lack of partition resources
				if updater := plugins.GetResourceManagerCallbackPlugin(); updater != nil {
					updater.UpdateContainerSchedulingState(&si.UpdateContainerSchedulingStateRequest{
						ApplicationID: ask.GetApplicationID(),
						AllocationKey: ask.GetAllocationKey(),
						State:         si.UpdateContainerSchedulingStateRequest_FAILED,
						Reason:        "request is waiting for cluster resources become available",
					})
				}
			}
		}
	}
}

// Visible by tests
func (s *Scheduler) GetClusterContext() *ClusterContext {
	return s.clusterContext
}

// The scheduler for testing which runs nAlloc times the normal schedule routine.
// Visible by tests
func (s *Scheduler) MultiStepSchedule(nAlloc int) {
	for i := 0; i < nAlloc; i++ {
		log.Log(log.Scheduler).Debug("Scheduler manual stepping",
			zap.Int("count", i))
		s.clusterContext.schedule()

		// sometimes the smoke tests are failing because they are competing CPU resources.
		// each scheduling cycle, let's sleep for a small amount of time (100ms),
		// this can ensure even CPU is intensive, the main thread can give up some CPU time
		// for other go routines to process, such as event handling routines.
		// Note, this sleep only works in tests.
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *Scheduler) Stop() {
	log.Log(log.Scheduler).Info("Stopping scheduler & background services")
	s.healthChecker.Stop()
	s.nodesMonitor.stop()
	s.clusterContext.Stop()
	close(s.stop)
}
