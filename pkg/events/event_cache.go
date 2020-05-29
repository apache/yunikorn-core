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
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"go.uber.org/zap"
)

// TODO this should be configurable?
const sleepTimeInterval = 10 * time.Millisecond
const pushEventInterval = 2 * time.Second

var once sync.Once
var cache *EventCache

type EventCache struct {
	channel EventChannel // input
	store   EventStore   // output
	stopped bool
}

func GetEventCache() *EventCache {
	once.Do(func(){
		cache = &EventCache{
			newEventChannelImpl(),
			newEventStoreImpl(),
			false,
		}
	})
	return cache
}

func (ec EventCache) StartService() {
	// TODO consider thread pool

	// event processing thread
	go ec.processEvent()

	// event pushing thread to shim
	go func() {
		for {
			if ec.stopped {
				break
			}
			if eventPlugin := plugins.GetEventPlugin(); eventPlugin != nil {
				messages := ec.store.CollectEvents()
				if err := eventPlugin.SendEvent(messages); err != nil && err.Error() != "" {
					log.Logger().Warn("Callback failed - could not sent EventMessage to shim",
						zap.Error(err), zap.Int("number of messages", len(messages)))
				}
			}
			time.Sleep(pushEventInterval)
		}
	}()
}

func (ec EventCache) Stop() {
	if ec.stopped {
		panic("could not stop EventCache service")
	}
	ec.stopped = true
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
