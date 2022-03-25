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

var cache *EventCache

type EventCache struct {
	Store EventStore // storing eventChannel

	channel chan *si.EventRecord // channelling input eventChannel
	stop    chan bool            // whether the service is stop

	sync.Mutex
}

func GetEventCache() *EventCache {
	return cache
}

func CreateAndSetEventCache() {
	cache = createEventStoreAndCache()
}

func createEventStoreAndCache() *EventCache {
	store := newEventStoreImpl()
	return createEventCacheInternal(store)
}

func createEventCacheInternal(store EventStore) *EventCache {
	return &EventCache{
		Store:   store,
		channel: make(chan *si.EventRecord, defaultEventChannelSize),
		stop:    make(chan bool),
	}
}

func (ec *EventCache) StartService() {
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
}

func (ec *EventCache) Stop() {
	ec.Lock()
	defer ec.Unlock()

	ec.stop <- true
	if ec.channel != nil {
		close(ec.channel)
		ec.channel = nil
	}
}

func (ec *EventCache) AddEvent(event *si.EventRecord) {
	metrics.GetEventMetrics().IncEventsCreated()
	select {
	case ec.channel <- event:
		metrics.GetEventMetrics().IncEventsChanneled()
	default:
		log.Logger().Debug("could not add Event to channel")
		metrics.GetEventMetrics().IncEventsNotChanneled()
	}
}
