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

package events

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

var (
	once sync.Once
	ev   EventSystem
)

type EventSystem interface {
	// AddEvent adds an event record to the event system for processing:
	// 1. It is added to a slice from where it is periodically read by the shim publisher.
	// 2. It is added to an internal ring buffer so that clients can retrieve the event history.
	// 3. Streaming clients are updated.
	AddEvent(event *si.EventRecord)

	// StartService starts the event system.
	// This method does not block. Events are processed on a separate goroutine.
	StartService()

	// Stop stops the event system.
	Stop()

	// IsEventTrackingEnabled whether history tracking is currently enabled or not.
	IsEventTrackingEnabled() bool

	// GetEventsFromID retrieves "count" number of elements from the history buffer from "id". Every
	// event has a unique ID inside the ring buffer.
	// If "id" is not in the buffer, then no record is returned, but the currently available range
	// [low..high] is set.
	GetEventsFromID(id, count uint64) ([]*si.EventRecord, uint64, uint64)

	// CreateEventStream creates an event stream (channel) for a consumer.
	// The "name" argument is an arbitrary string for a consumer, which is used for logging. It does not need to be unique.
	// The "count" argument defines how many historical elements should be returned on the stream. Zero is a valid value for "count".
	// The returned type contains a read-only channel which is updated as soon as there is a new event record.
	// It is also used as a handle to stop the streaming.
	// Consumers must read the channel and process the event objects as soon as they can to avoid
	// events piling up inside the channel buffers.
	CreateEventStream(name string, count uint64) *EventStream

	// RemoveStream stops streaming for a given consumer.
	// Consumers that no longer wish to be updated (e.g., a remote client
	// disconnected) *must* call this method to gracefully stop the streaming.
	RemoveStream(*EventStream)

	// GetEventStreams returns the current active event streams.
	GetEventStreams() []EventStreamData
}

// GetEventSystem returns the event system instance. Initialization happens during the first call.
// This does not start the service or publisher. Call StartService or StartServiceWithPublisher
// on the returned system to start the service.
func GetEventSystem() EventSystem {
	once.Do(func() {
		Init()
	})
	return ev
}

// Init Initializes the event system.
// Only exported for testing.
func Init() {
	// load from config for setting in two places
	confRequestCapacity := getRequestCapacity()
	confRingBufferCapacity := getRingBufferCapacity()

	store := newEventStore(confRequestCapacity)
	buffer := newEventRingBuffer(confRingBufferCapacity)
	evImpl := &EventSystemImpl{
		Store:              store,
		eventBuffer:        buffer,
		eventSystemId:      fmt.Sprintf("event-system-%d", time.Now().Unix()),
		streaming:          NewEventStreaming(buffer),
		trackingEnabled:    isTrackingEnabled(),
		requestCapacity:    confRequestCapacity,
		ringBufferCapacity: confRingBufferCapacity,
	}
	evImpl.stopped.Store(true)
	// start the callback for this instance: must be done
	configs.AddConfigMapCallback(evImpl.eventSystemId, func() {
		go evImpl.reloadConfig()
	})

	ev = evImpl
}

// EventSystemImpl main implementation of the event system which is used for history tracking.
type EventSystemImpl struct {
	eventSystemId string      // set on creation and never changed, no lock needed
	Store         *EventStore // storing eventChannel, exported for test
	// +checklocks:RWMutex
	publisher   *eventPublisher
	eventBuffer *eventRingBuffer // set on creation and never changed, no lock needed
	streaming   *EventStreaming  // set on creation and never changed, no lock needed

	// +checklocks:RWMutex
	channel chan *si.EventRecord // channelling input eventChannel
	// +checklocks:RWMutex
	stop    chan struct{} // channel to stop the system
	stopped atomic.Bool   // whether the service is stopped

	// +checklocks:RWMutex
	trackingEnabled bool
	// +checklocks:RWMutex
	requestCapacity uint64
	// +checklocks:RWMutex
	ringBufferCapacity uint64

	locking.RWMutex
}

// CreateEventStream creates an event stream. See the interface for details.
func (ec *EventSystemImpl) CreateEventStream(name string, count uint64) *EventStream {
	return ec.streaming.CreateEventStream(name, count)
}

// RemoveStream graceful termination of an event streaming for a consumer. See the interface for details.
func (ec *EventSystemImpl) RemoveStream(consumer *EventStream) {
	ec.streaming.RemoveEventStream(consumer)
}

// GetEventsFromID retrieves historical elements. See the interface for details.
func (ec *EventSystemImpl) GetEventsFromID(id, count uint64) ([]*si.EventRecord, uint64, uint64) {
	return ec.eventBuffer.GetEventsFromID(id, count)
}

// IsEventTrackingEnabled whether history tracking is currently enabled or not.
func (ec *EventSystemImpl) IsEventTrackingEnabled() bool {
	ec.RLock()
	defer ec.RUnlock()
	return ec.trackingEnabled
}

// StartService starts the event processing in the background. See the interface for details.
func (ec *EventSystemImpl) StartService() {
	ec.StartServiceWithPublisher(true)
}

// Stop stops the event system, including the shim publisher if it was started.
func (ec *EventSystemImpl) Stop() {
	ec.Lock()
	defer ec.Unlock()
	// no need to stop twice
	if !ec.stopped.CompareAndSwap(false, true) {
		return
	}
	log.Log(log.Events).Info("Stopping event system handler")

	ec.stop <- struct{}{}
	if ec.channel != nil {
		close(ec.channel)
		ec.channel = nil
	}
	if ec.publisher != nil {
		ec.publisher.stop()
		ec.publisher = nil
	}
}

// GetEventStreams returns the current active event streams.
func (ec *EventSystemImpl) GetEventStreams() []EventStreamData {
	return ec.streaming.GetEventStreams()
}

// AddEvent adds an event record to the event system. See the interface for details.
func (ec *EventSystemImpl) AddEvent(event *si.EventRecord) {
	if event != nil {
		event.Message = truncateEventMessage(event.Message)
	}
	// all events get tracked, even ones we drop
	metrics.GetEventMetrics().IncEventsCreated()
	ec.RLock()
	defer ec.RUnlock()
	// not running just track the metric
	if ec.stopped.Load() {
		metrics.GetEventMetrics().IncEventsNotChanneled()
		return
	}

	select {
	case ec.channel <- event:
		metrics.GetEventMetrics().IncEventsChanneled()
	default:
		// make sure we do not drop events when running. events generated while turned off are not "dropped"
		log.Log(log.Events).Info("Event dropped due to channel full or closed",
			zap.Int("channelSize", len(ec.channel)))
		metrics.GetEventMetrics().IncEventsDropped()
	}
}

// StartServiceWithPublisher starts the event processing background routines.
// Only exported for testing.
func (ec *EventSystemImpl) StartServiceWithPublisher(withPublisher bool) {
	ec.Lock()
	defer ec.Unlock()
	if !ec.stopped.CompareAndSwap(true, false) {
		log.Log(log.Events).Info("Event system is already running")
		return
	}
	ec.trackingEnabled = isTrackingEnabled()
	ec.stop = make(chan struct{})
	ec.channel = make(chan *si.EventRecord, configs.DefaultEventChannelSize)

	// YUNIKORN-3412: handler reads channel fields that Stop() rewrites under the lock
	go func() {
		log.Log(log.Events).Info("Starting event system handler")
		for {
			select {
			case <-ec.stop: // +checklocksignore
				return
			case event, ok := <-ec.channel: // +checklocksignore
				if !ok {
					return
				}
				if event != nil {
					ec.Store.Store(event)
					ec.eventBuffer.Add(event)
					ec.streaming.PublishEvent(event)
					metrics.GetEventMetrics().IncEventsProcessed()
				}
			}
		}
	}()
	if withPublisher {
		ec.publisher = createShimPublisher(ec.Store)
		ec.publisher.start()
	}
}

// getRequestCapacity returns the capacity of an intermediate storage which is used by the shim publisher.
func (ec *EventSystemImpl) getRequestCapacity() uint64 {
	ec.RLock()
	defer ec.RUnlock()
	return ec.requestCapacity
}

// getRingBufferCapacity returns the capacity of the buffer which stores historical elements.
func (ec *EventSystemImpl) getRingBufferCapacity() uint64 {
	ec.RLock()
	defer ec.RUnlock()
	return ec.ringBufferCapacity
}

// isRestartNeeded returns true if the tracking mode has switched from off to on or vice versa.
// Returns false if tracking mode has not changed
func (ec *EventSystemImpl) isRestartNeeded() bool {
	ec.RLock()
	defer ec.RUnlock()
	return isTrackingEnabled() != ec.trackingEnabled
}

// restart restarts the event system, used during config update.
func (ec *EventSystemImpl) restart() {
	ec.Stop()
	ec.StartServiceWithPublisher(true)
}

// CloseAllStreams closes all existing streams.
// VisibleForTesting
func (ec *EventSystemImpl) CloseAllStreams() {
	ec.streaming.Lock()
	defer ec.streaming.Unlock()
	for consumer := range ec.streaming.eventStreams {
		ec.streaming.removeEventStream(consumer)
	}
}

// reloadConfig function called by the config
func (ec *EventSystemImpl) reloadConfig() {
	// load from config for setting in two places
	confRequestCapacity := getRequestCapacity()
	confRingBufferCapacity := getRingBufferCapacity()

	ec.Lock()
	ec.requestCapacity = confRequestCapacity
	ec.ringBufferCapacity = confRingBufferCapacity
	ec.Unlock()

	// resize the ring buffer & event store with new capacity
	ec.Store.SetStoreSize(confRequestCapacity)
	ec.eventBuffer.Resize(confRingBufferCapacity)

	if ec.isRestartNeeded() {
		log.Log(log.Events).Info("Restarting event system handler on config reload")
		ec.Lock()
		ec.trackingEnabled = isTrackingEnabled()
		ec.Unlock()
		ec.restart()
	}
}

// truncateEventMessage limits the event message to 1024 characters for k8s compatibility
func truncateEventMessage(message string) string {
	const k8sEventMessageLimit = 1024
	if len(message) <= k8sEventMessageLimit {
		return message
	}
	return message[:k8sEventMessageLimit-3] + "..."
}

// isTrackingEnabled gets the current state of tracking from the configuration.
func isTrackingEnabled() bool {
	return common.GetConfigurationBool(configs.GetConfigMap(), configs.CMEventTrackingEnabled, configs.DefaultEventTrackingEnabled)
}

// getRequestCapacity returns the size of an intermediate storage from the configuration, using the
// configs.DefaultEventRequestCapacity if 0 or not defined.
func getRequestCapacity() uint64 {
	capacity := common.GetConfigurationUint(configs.GetConfigMap(), configs.CMEventRequestCapacity, configs.DefaultEventRequestCapacity)
	if capacity == 0 {
		log.Log(log.Events).Warn("Request capacity is set to 0, using default",
			zap.String("property", configs.CMEventRequestCapacity),
			zap.Uint64("default", configs.DefaultEventRequestCapacity))
		return configs.DefaultEventRequestCapacity
	}
	return capacity
}

// getRingBufferCapacity returns the ring buffer capacity from the configuration, using the
// configs.DefaultEventRingBufferCapacity if 0 or not defined.
func getRingBufferCapacity() uint64 {
	capacity := common.GetConfigurationUint(configs.GetConfigMap(), configs.CMEventRingBufferCapacity, configs.DefaultEventRingBufferCapacity)
	if capacity == 0 {
		log.Log(log.Events).Warn("Ring buffer capacity is set to 0, using default",
			zap.String("property", configs.CMEventRingBufferCapacity),
			zap.Uint64("default", configs.DefaultEventRingBufferCapacity))
		return configs.DefaultEventRingBufferCapacity
	}
	return capacity
}
