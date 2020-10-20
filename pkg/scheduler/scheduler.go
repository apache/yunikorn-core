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

	"github.com/apache/incubator-yunikorn-core/pkg/handler"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// Main Scheduler service that starts the needed sub services
type Scheduler struct {
	clusterSchedulingContext *ClusterSchedulingContext // main context
	preemptionContext        *preemptionContext        // Preemption context
	eventHandlers            handler.EventHandlers     // list of event handlers
	pendingEvents            chan interface{}          // queue for events
}

func NewScheduler() *Scheduler {
	m := &Scheduler{}
	m.clusterSchedulingContext = newClusterSchedulingContext()
	m.pendingEvents = make(chan interface{}, 1024*1024)
	return m
}

// Start service
func (s *Scheduler) StartService(handlers handler.EventHandlers, manualSchedule bool) {
	// set the proxy handler in the context
	s.clusterSchedulingContext.setEventHandler(handlers.RMProxyEventHandler)

	// Start event handlers
	go s.handleRMEvent()

	// Start resource monitor if necessary (majorly for testing)
	monitor := newNodesResourceUsageMonitor(s.clusterSchedulingContext)
	monitor.start()

	if !manualSchedule {
		go s.internalSchedule()
		go s.internalInspectOutstandingRequests()
		go s.internalPreemption()
	}
}

// Internal start scheduling service
func (s *Scheduler) internalSchedule() {
	for {
		s.clusterSchedulingContext.schedule()
	}
}

// Internal start preemption service
func (s *Scheduler) internalPreemption() {
	for {
		s.SingleStepPreemption()
		time.Sleep(1000 * time.Millisecond)
	}
}

func (s *Scheduler) internalInspectOutstandingRequests() {
	for {
		time.Sleep(1000 * time.Millisecond)
		s.inspectOutstandingRequests()
	}
}

// Implement methods for Scheduler events
func (s *Scheduler) HandleEvent(ev interface{}) {
	enqueueAndCheckFull(s.pendingEvents, ev)
}

func enqueueAndCheckFull(queue chan interface{}, ev interface{}) {
	select {
	case queue <- ev:
		log.Logger().Debug("enqueued event",
			zap.String("eventType", reflect.TypeOf(ev).String()),
			zap.Any("event", ev),
			zap.Int("currentQueueSize", len(queue)))
	default:
		log.Logger().DPanic("failed to enqueue event",
			zap.String("event", reflect.TypeOf(ev).String()))
	}
}

func (s *Scheduler) handleRMEvent() {
	for {
		ev := <-s.pendingEvents
		switch v := ev.(type) {
		case *rmevent.RMUpdateRequestEvent:
			s.clusterSchedulingContext.processRMUpdateEvent(v)
		case *rmevent.RMPartitionsRemoveEvent:
			s.clusterSchedulingContext.removePartitionsByRMID(v)
		case *rmevent.RMRegistrationEvent:
			s.clusterSchedulingContext.processRMRegistrationEvent(v)
		case *rmevent.RMConfigUpdateEvent:
			s.clusterSchedulingContext.processRMConfigUpdateEvent(v)
		default:
			log.Logger().Error("Received type is not an acceptable type for RM event.",
				zap.String("received type", reflect.TypeOf(v).String()))
		}
	}
}

// inspect on the outstanding requests for each of the queues,
// update request state accordingly to shim if needed.
// this function filters out all outstanding requests that being
// skipped due to insufficient cluster resources and update the
// state through the ContainerSchedulingStateUpdaterPlugin in order
// to trigger the auto-scaling.
func (s *Scheduler) inspectOutstandingRequests() {
	log.Logger().Debug("inspect outstanding requests")
	// schedule each partition defined in the cluster
	for _, psc := range s.clusterSchedulingContext.GetPartitionMapClone() {
		requests := psc.calculateOutstandingRequests()
		if len(requests) > 0 {
			for _, ask := range requests {
				log.Logger().Debug("outstanding request",
					zap.String("appID", ask.ApplicationID),
					zap.String("allocationKey", ask.AllocationKey))
				// these asks are queue outstanding requests,
				// they can fit into the max head room, but they are pending because lack of partition resources
				if updater := plugins.GetContainerSchedulingStateUpdaterPlugin(); updater != nil {
					updater.Update(&si.UpdateContainerSchedulingStateRequest{
						ApplicartionID: ask.ApplicationID,
						AllocationKey:  ask.AllocationKey,
						State:          si.UpdateContainerSchedulingStateRequest_FAILED,
						Reason:         "request is waiting for cluster resources become available",
					})
				}
			}
		}
	}
}

// Visible by tests
func (s *Scheduler) GetClusterSchedulingContext() *ClusterSchedulingContext {
	return s.clusterSchedulingContext
}

// The scheduler for testing which runs nAlloc times the normal schedule routine.
// Visible by tests
func (s *Scheduler) MultiStepSchedule(nAlloc int) {
	for i := 0; i < nAlloc; i++ {
		log.Logger().Debug("Scheduler manual stepping",
			zap.Int("count", i))
		s.clusterSchedulingContext.schedule()

		// sometimes the smoke tests are failing because they are competing CPU resources.
		// each scheduling cycle, let's sleep for a small amount of time (100ms),
		// this can ensure even CPU is intensive, the main thread can give up some CPU time
		// for other go routines to process, such as event handling routines.
		// Note, this sleep only works in tests.
		time.Sleep(100 * time.Millisecond)
	}
}
