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
	"time"
)

// TODO this should be configurable?
const sleepTimeInterval = 10 * time.Millisecond
const pushEventInterval = 2 * time.Second

var once sync.Once
var cache *EventCache

type EventCache struct {
	channel    EventChannel     // channelling input events
	store      EventStore       // storing events
	publishers []EventPublisher // publishing events to sinks
	stopped    bool             // whether the service is stopped
}

func GetEventCache() *EventCache {
	once.Do(func(){
		store := newEventStoreImpl()
		pub := newShimPublisher(store)

		cache = &EventCache{
			newEventChannelImpl(),
			store,
			[]EventPublisher{pub},
			false,
		}
	})
	return cache
}

func (ec EventCache) StartService() {
	// TODO consider thread pool

	// event processing thread
	go ec.processEvent()

	// start event publishers
	for _, publisher := range ec.publishers {
		publisher.StartService()
	}
}

func (ec EventCache) Stop() {
	if ec.stopped {
		panic("could not stop EventCache service")
	}
	ec.stopped = true

	// stop event publishers
	for _, publisher := range ec.publishers {
		publisher.Stop()
	}
}

func (ec EventCache) AddPublisher(pub EventPublisher) {
	ec.publishers = append(ec.publishers, pub)
}

func (ec EventCache) AddEvent(event Event) {
	ec.channel.AddEvent(event)
}

func (ec EventCache) processEvent() {
	for {
		// break the main loop if stopped
		if ec.stopped {
			break
		}
		for {
			// do not process any new event if the process has been stopped
			if ec.stopped {
				break
			}
			// TODO for debugging: add time info about how long did this step take
			event, ok := ec.channel.GetNextEvent()
			if ok {
				ec.store.Store(event)
			} else {
				break
			}
		}
		time.Sleep(sleepTimeInterval)
	}
}
