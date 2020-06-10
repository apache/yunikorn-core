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

const defaultEventChannelSize = 100000

const sleepTimeInterval = 10 * time.Millisecond

var once sync.Once
var cache *EventCache

type EventCache struct {
	channel EventChannel // channelling input events
	store   EventStore   // storing events
	started bool         // whether the service is started
	stopped bool         // whether the service is stop

	sync.Mutex
}

func GetEventCache() *EventCache {
	once.Do(func(){
		store := newEventStoreImpl()

		cache = &EventCache{
			channel: newEventChannelImpl(defaultEventChannelSize),
			store:   store,
			started: false,
			stopped: false,
		}
	})
	return cache
}

// TODO consider thread pool
func (ec *EventCache) StartService() {
	ec.Lock()
	defer ec.Unlock()

	// start main event processing thread
	go ec.processEvent()

	ec.started = true
}

func (ec *EventCache) Stop() {
	ec.Lock()
	defer ec.Unlock()

	if ec.started {
		ec.stopped = true
		ec.started = false
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
		// break the main loop if stop
		if ec.isStopped() {
			break
		}
		for {
			// do not process any new event if the process has been stop
			if ec.isStopped() {
				break
			}
			// TODO for debugging: add time info about how long did this step take
			event := ec.channel.GetNextEvent()
			if event != nil {
				ec.store.Store(event)
			} else {
				break
			}
		}
		time.Sleep(sleepTimeInterval)
	}
}
