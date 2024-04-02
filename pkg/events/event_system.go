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
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// need to change for testing
var defaultEventChannelSize = 100000
var defaultRingBufferSize uint64 = 100000

var once sync.Once
var ev EventSystem

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

// EventSystemImpl main implementation of the event system which is used for history tracking.
type EventSystemImpl struct {
	eventSystemId string
	Store         *EventStore // storing eventChannel
	publisher     *EventPublisher
	eventBuffer   *eventRingBuffer
	streaming     *EventStreaming

	channel chan *si.EventRecord // channelling input eventChannel
	stop    chan bool            // whether the service is stopped
	stopped bool

	trackingEnabled    bool
	requestCapacity    uint64
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

// GetEventSystem returns the event system instance. Initialization happens during the first call.
func GetEventSystem() EventSystem {
	once.Do(func() {
		Init()
	})
	return ev
}

// IsEventTrackingEnabled whether history tracking is currently enabled or not.
func (ec *EventSystemImpl) IsEventTrackingEnabled() bool {
	ec.RLock()
	defer ec.RUnlock()
	return ec.trackingEnabled
}

// GetRequestCapacity returns the capacity of an intermediate storage which is used by the shim publisher.
func (ec *EventSystemImpl) GetRequestCapacity() uint64 {
	ec.RLock()
	defer ec.RUnlock()
	return ec.requestCapacity
}

// GetRingBufferCapacity returns the capacity of the buffer which stores historical elements.
func (ec *EventSystemImpl) GetRingBufferCapacity() uint64 {
	ec.RLock()
	defer ec.RUnlock()
	return ec.ringBufferCapacity
}

// Init Initializes the event system.
// Only exported for testing.
func Init() {
	store := newEventStore(getRequestCapacity())
	buffer := newEventRingBuffer(getRingBufferCapacity())
	ev = &EventSystemImpl{
		Store:         store,
		channel:       make(chan *si.EventRecord, defaultEventChannelSize),
		stop:          make(chan bool),
		stopped:       false,
		eventBuffer:   buffer,
		eventSystemId: fmt.Sprintf("event-system-%d", time.Now().Unix()),
		publisher:     CreateShimPublisher(store),
		streaming:     NewEventStreaming(buffer),
	}
}

// StartService starts the event processing in the background. See the interface for details.
func (ec *EventSystemImpl) StartService() {
	ec.StartServiceWithPublisher(true)
}

// StartServiceWithPublisher starts the event processing background routines.
// Only exported for testing.
func (ec *EventSystemImpl) StartServiceWithPublisher(withPublisher bool) {
	ec.Lock()
	defer ec.Unlock()

	configs.AddConfigMapCallback(ec.eventSystemId, func() {
		go ec.reloadConfig()
	})

	ec.trackingEnabled = isTrackingEnabled()
	ec.ringBufferCapacity = getRingBufferCapacity()
	ec.requestCapacity = getRequestCapacity()

	go func() {
		log.Log(log.Events).Info("Starting event system handler")
		for {
			select {
			case <-ec.stop:
				return
			case event, ok := <-ec.channel:
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
		ec.publisher.StartService()
	}
}

// Stop stops the event system.
func (ec *EventSystemImpl) Stop() {
	ec.Lock()
	defer ec.Unlock()

	configs.RemoveConfigMapCallback(ec.eventSystemId)

	if ec.stopped {
		return
	}

	ec.stop <- true
	if ec.channel != nil {
		close(ec.channel)
		ec.channel = nil
	}
	ec.publisher.Stop()
	ec.stopped = true
}

// AddEvent adds an event record to the event system. See the interface for details.
func (ec *EventSystemImpl) AddEvent(event *si.EventRecord) {
	metrics.GetEventMetrics().IncEventsCreated()
	select {
	case ec.channel <- event:
		metrics.GetEventMetrics().IncEventsChanneled()
	default:
		log.Log(log.Events).Debug("could not add Event to channel")
		metrics.GetEventMetrics().IncEventsNotChanneled()
	}
}

func isTrackingEnabled() bool {
	return common.GetConfigurationBool(configs.GetConfigMap(), configs.CMEventTrackingEnabled, configs.DefaultEventTrackingEnabled)
}

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

func (ec *EventSystemImpl) isRestartNeeded() bool {
	ec.RLock()
	defer ec.RUnlock()
	return isTrackingEnabled() != ec.trackingEnabled
}

// Restart restarts the event system, used during config update.
func (ec *EventSystemImpl) Restart() {
	ec.Stop()
	ec.StartServiceWithPublisher(true)
}

// GetEventStreams returns the current active event streams.
func (ec *EventSystemImpl) GetEventStreams() []EventStreamData {
	return ec.streaming.GetEventStreams()
}

// VisibleForTesting
func (ec *EventSystemImpl) CloseAllStreams() {
	ec.streaming.Lock()
	defer ec.streaming.Unlock()
	for consumer := range ec.streaming.eventStreams {
		ec.streaming.removeEventStream(consumer)
	}
}

func (ec *EventSystemImpl) reloadConfig() {
	requestCapacity, ringBufferCapacity := ec.updateCapacity()

	// resize the ring buffer & event store with new capacity
	ec.Store.SetStoreSize(requestCapacity)
	ec.eventBuffer.Resize(ringBufferCapacity)

	if ec.isRestartNeeded() {
		ec.Restart()
	}
}

func (ec *EventSystemImpl) updateCapacity() (uint64, uint64) {
	ec.Lock()
	defer ec.Unlock()
	ec.requestCapacity = getRequestCapacity()
	ec.ringBufferCapacity = getRingBufferCapacity()

	return ec.requestCapacity, ec.ringBufferCapacity
}
