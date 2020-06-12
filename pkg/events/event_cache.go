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

	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

const defaultEventChannelSize = 100000

var once sync.Once
var cache *EventCache

type EventCache struct {
	channel EventChannel // channelling input eventChannel
	store   EventStore   // storing eventChannel
	started bool         // whether the service is started
	stop    chan bool    // whether the service is stop

	sync.Mutex
}

func GetEventCache() *EventCache {
	once.Do(func() {
		store := newEventStoreImpl()

		cache = &EventCache{
			channel: newEventChannelImpl(defaultEventChannelSize),
			store:   store,
			started: false,
			stop:    make(chan bool),
		}
	})
	return cache
}

func (ec *EventCache) StartService() {
	ec.Lock()
	defer ec.Unlock()

	if !ec.started {
		// start main event processing thread
		go ec.processEvent()

		ec.started = true
	}
}

func (ec *EventCache) IsStarted() bool {
	ec.Lock()
	defer ec.Unlock()

	return ec.started
}

func (ec *EventCache) Stop() {
	ec.Lock()
	defer ec.Unlock()

	if ec.started {
		ec.stop <- true
		ec.channel.Stop()
		ec.started = false
	}
}

func (ec *EventCache) AddEvent(event Event) {
	ec.channel.AddEvent(event)
}

func (ec *EventCache) GetEventStore() EventStore {
	return ec.store
}

func (ec *EventCache) processEvent() {
	for {
		select {
		case <-ec.stop:
			return
		case event, ok := <-ec.channel.GetChannel():
			if event != nil {
				ec.store.Store(event)
			} else {
				log.Logger().Debug("nil object in EventChannel")
				if !ok {
					log.Logger().Info("the EventChannel is closed - closing the EventCache")
					ec.Stop()
					return
				}
			}
		}
	}
}
