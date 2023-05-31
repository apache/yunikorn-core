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
	"sync"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// need to change for testing
var defaultEventChannelSize = 100000

var ev *EventSystem

type EventSystem struct {
	Store     *EventStore // storing eventChannel
	publisher *EventPublisher

	channel chan *si.EventRecord // channelling input eventChannel
	stop    chan bool            // whether the service is stop
	stopped bool

	sync.Mutex
}

func GetEventSystem() *EventSystem {
	return ev
}

func CreateAndSetEventSystem() {
	store := newEventStore()
	ev = &EventSystem{
		Store:     store,
		channel:   make(chan *si.EventRecord, defaultEventChannelSize),
		stop:      make(chan bool),
		stopped:   false,
		publisher: CreateShimPublisher(store),
	}
}

func (ec *EventSystem) StartService() {
	ec.StartServiceWithPublisher(true)
}

// VisibleForTesting
func (ec *EventSystem) StartServiceWithPublisher(withPublisher bool) {
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
					metrics.GetEventMetrics().IncEventsProcessed()
				}
			}
		}
	}()
	if withPublisher {
		ec.publisher.StartService()
	}
}

func (ec *EventSystem) Stop() {
	ec.Lock()
	defer ec.Unlock()

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

func (ec *EventSystem) AddEvent(event *si.EventRecord) {
	metrics.GetEventMetrics().IncEventsCreated()
	select {
	case ec.channel <- event:
		metrics.GetEventMetrics().IncEventsChanneled()
	default:
		log.Logger().Debug("could not add Event to channel")
		metrics.GetEventMetrics().IncEventsNotChanneled()
	}
}
