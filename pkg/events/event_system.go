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
	"strconv"
	"sync"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
	"go.uber.org/zap"
)

// need to change for testing
var defaultEventChannelSize = 100000
var defaultRingBufferSize uint64 = 100000

var ev EventSystem

type EventSystem interface {
	AddEvent(event *si.EventRecord)
	StartService()
	Stop()
	GetEventsFromID(uint64, uint64) ([]*si.EventRecord, uint64, uint64)
}

type EventSystemImpl struct {
	Store       *EventStore // storing eventChannel
	publisher   *EventPublisher
	eventBuffer *eventRingBuffer
	channel     chan *si.EventRecord // channelling input eventChannel
	stop        chan bool            // whether the service is stopped
	stopped     bool

	trackingEnabled bool
	requestCapacity int
	bufferCapacity  int

	sync.RWMutex
}

func (ec *EventSystemImpl) GetEventsFromID(id, count uint64) ([]*si.EventRecord, uint64, uint64) {
	return ec.eventBuffer.GetEventsFromID(id, count)
}

func GetEventSystem() EventSystem {
	return ev
}

func (ec *EventSystemImpl) IsEventTrackingEnabled() bool {
	ec.RLock()
	defer ec.RUnlock()
	return ec.trackingEnabled
}

func (ec *EventSystemImpl) GetRequestCapacity() int {
	ec.RLock()
	defer ec.RUnlock()
	return ec.requestCapacity
}

func (ec *EventSystemImpl) GetBufferCapacity() int {
	ec.RLock()
	defer ec.RUnlock()
	return ec.bufferCapacity
}

func CreateAndSetEventSystem() {
	store := newEventStore()
	ev = &EventSystemImpl{
		Store:       store,
		channel:     make(chan *si.EventRecord, defaultEventChannelSize),
		stop:        make(chan bool),
		stopped:     false,
		publisher:   CreateShimPublisher(store),
		eventBuffer: newEventRingBuffer(defaultRingBufferSize),
	}

	if eventSystemImpl, ok := ev.(*EventSystemImpl); ok {
		eventSystemImpl.eventSystemId = fmt.Sprintf("event-system-%p", eventSystemImpl)
		ev = eventSystemImpl
	}
}

func (ec *EventSystemImpl) StartService() {
	ec.StartServiceWithPublisher(true)
}

func getConfigurationBool(key string, defaultValue bool) bool {
	value, ok := configs.GetConfigMap()[key]
	if !ok {
		return defaultValue
	}
	boolValue, err := strconv.ParseBool(value)
	if err != nil {
		log.Log(log.Events).Warn("Failed to parse configuration value",
			zap.String("key", key),
			zap.String("value", value),
			zap.Error(err))
		return defaultValue
	}
	return boolValue
}

func getConfigurationInt(key string, defaultValue int) int {
	value, ok := configs.GetConfigMap()[key]
	if !ok {
		return defaultValue
	}
	intVal, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		log.Log(log.Events).Warn("Failed to parse configuration value",
			zap.String("key", key),
			zap.String("value", value),
			zap.Error(err))
		return defaultValue
	}
	return int(intVal)
}

func (ec *EventSystemImpl) readIsTrackingEnabled() bool {
	return getConfigurationBool(configs.CMEventTrackingEnabled, configs.DefaultEventTrackingEnabled)
}

func (ec *EventSystemImpl) readRequestCapacity() int {
	return getConfigurationInt(configs.CMEventRequestCapacity, configs.DefaultEventRequestCapacity)
}

func (ec *EventSystemImpl) readBufferCapacity() int {
	return getConfigurationInt(configs.CMEventRingBufferCapacity, configs.DefaultEventBufferCapacity)
}

func (ec *EventSystemImpl) isRestartNeeded() bool {
	ec.Lock()
	defer ec.Unlock()

	trackingEnabled := ec.readIsTrackingEnabled()
	requestCapacity := ec.readRequestCapacity()
	bufferCapacity := ec.readBufferCapacity()

	if trackingEnabled != ec.trackingEnabled ||
		requestCapacity != ec.requestCapacity ||
		bufferCapacity != ec.bufferCapacity {
		return true
	}

	return false
}

func (ec *EventSystemImpl) Restart() {
	ec.Stop()
	ec.StartServiceWithPublisher(true)
}

func (ec *EventSystemImpl) reloadConfig() {
	if ec.isRestartNeeded() {
		ec.Restart()
	}
}

func (ec *EventSystemImpl) StartServiceWithPublisher(withPublisher bool) {
	ec.Lock()
	defer ec.Unlock()

	configs.AddConfigMapCallback(ec.eventSystemId, func() {
		go ec.reloadConfig()
	})

	ec.trackingEnabled = ec.readIsTrackingEnabled()
	ec.bufferCapacity = ec.readBufferCapacity()
	ec.requestCapacity = ec.readRequestCapacity()

	go func() {
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
					metrics.GetEventMetrics().IncEventsProcessed()
				}
			}
		}
	}()
	if withPublisher {
		ec.publisher.StartService()
	}
}

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
