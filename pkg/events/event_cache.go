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
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"go.uber.org/zap"
)

// TODO this should be configurable?
const sleepTimeInterval = 10 * time.Millisecond
const pushEventInterval = 2 * time.Second

var Cache *EventCache

type EventCache struct {
	// input
	channel *eventChannel

	// output
	store *eventStore
}

func NewEventCache() *EventCache {
	Cache = &EventCache{
		newEventChannel(),
		newEventStore(),
	}
	return Cache
}

func (ec EventCache) StartService() {
	// TODO consider thread pool

	// event processing thread
	go ec.processEvent()

	// event pushing thread to shim
	go func() {
		for {
			if eventPlugin := plugins.GetEventPlugin(); eventPlugin != nil {
				messages := ec.store.collectEvents()
				if err := eventPlugin.SendEvent(messages); err != nil && err.Error() != "" {
					log.Logger().Warn("Callback failed - could not sent EventMessage to shim",
						zap.Error(err), zap.Int("number of messages", len(messages)))
				}
			}
			time.Sleep(pushEventInterval)
		}
	}()
}

func (ec EventCache) AddEvent(event Event) {
	ec.channel.addEvent(event)
}

func (ec EventCache) processEvent() {
	for {
		for {
			// TODO for debugging: add time info about how long did this step take
			event, ok := ec.channel.getNextEvent()
			if ok {
				ec.store.store(event)
			} else {
				break
			}
		}
		time.Sleep(sleepTimeInterval)
	}
}
