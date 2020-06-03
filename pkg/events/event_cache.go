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

	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

const defaultEventChannelSize = 100000

const sleepTimeInterval = 10 * time.Millisecond
const pushEventInterval = 2 * time.Second

var once sync.Once
var cache *EventCache

type EventCache struct {
	channel    EventChannel     // channelling input events
	store      EventStore       // storing events
	publishers []EventPublisher // publishing events to sinks
	started    bool             // whether the service is started
	stopped    bool             // whether the service is stopped

	sync.Mutex
}

func GetEventCache() *EventCache {
	once.Do(func(){
		store := newEventStoreImpl()

		cache = &EventCache{
			channel:    newEventChannelImpl(defaultEventChannelSize),
			store:      store,
			publishers: make([]EventPublisher, 0),
			started:    false,
			stopped:    false,
		}
	})
	return cache
}

// TODO consider thread pool
func (ec *EventCache) StartService() {
	ec.Lock()
	defer ec.Unlock()

	ec.started = true

	// start main event processing thread
	go ec.processEvent()
}

func (ec *EventCache) Stop() {
	ec.Lock()
	defer ec.Unlock()

	if ec.started {
		if ec.stopped {
			panic("could not stop EventCache service")
		}
		ec.stopped = true

		// stop event publishers
		for _, publisher := range ec.publishers {
			publisher.Stop()
		}
	}
}

func (ec *EventCache) AddPublisher(pub EventPublisher) {
	ec.Lock()
	defer ec.Unlock()

	if ec.started  {
		log.Logger().Error("Added Publisher to a running EventCache!")
	} else {
		ec.publishers = append(ec.publishers, pub)
	}
}

func (ec *EventCache) AddEvent(event Event) {
	ec.channel.AddEvent(event)
}

func (ec *EventCache) IsStarted() bool {
	ec.Lock()
	defer ec.Unlock()

	return ec.started
}

func (ec *EventCache) isStopped() bool {
	ec.Lock()
	defer ec.Unlock()

	return ec.stopped
}

func (ec *EventCache) GetEventStore() EventStore {
	return ec.store
}

func (ec *EventCache) processEvent() {
	for {
		// break the main loop if stopped
		if ec.isStopped() {
			break
		}
		for {
			// do not process any new event if the process has been stopped
			if ec.isStopped() {
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
