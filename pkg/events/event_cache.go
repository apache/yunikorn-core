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
)

// TODO this should be configurable?
const sleepTime = 1 * time.Second

type EventCache struct {
	// input
	channel *EventChannel

	// output
	store *EventStore
}

func NewEventCache() *EventCache {
	return &EventCache{
		NewEventChannel(),
		NewEventStore(),
	}
}

func (ec EventCache) StartService() {
	// event processing thread
	// TODO consider thread pool
	go ec.processEvent()

	// old event clearing thread
	go func() {
		for {
			// TODO add time info: how long did this step take?
			log.Logger().Info("Started clearing out old event objects")
			time.Sleep(1 * time.Hour)
			ec.store.ClearOldObjects()
		}
	}()
}

func (ec EventCache) GetEventChannel() *EventChannel {
	return ec.channel
}

func (ec EventCache) GetEventStore() *EventStore {
	return ec.store
}

func (ec EventCache) processEvent() {
	for {
		for {
			// TODO add time info: how long did this step take?
			event, ok := ec.channel.GetNextEvent()
			if ok {
				ec.store.Visit(event)
			} else {
				break
			}
		}
		time.Sleep(sleepTime)
	}
}
