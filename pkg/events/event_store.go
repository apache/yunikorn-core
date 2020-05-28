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
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
	"go.uber.org/zap"
)

type eventStore struct {
	eventMap map[interface{}]Event

	sync.RWMutex
}

func newEventStore() *eventStore {
	return &eventStore{
		eventMap: make(map[interface{}]Event),
	}
}

func (es *eventStore) store(event Event) {
	es.Lock()
	defer es.Unlock()

	es.eventMap[event.GetSource()] = event
}



func (es *eventStore) collectEvents() []*si.EventMessage {
	if eventPlugin := plugins.GetEventPlugin(); eventPlugin != nil {
		es.Lock()
		defer es.Unlock()

		messages := make([]*si.EventMessage, 0)

		// collect events
		for _, v := range es.eventMap {
			message, err := toEventMessage(v)
			if err != nil {
				log.Logger().Warn("Could not translate object to EventMessage", zap.Any("object", v))
				continue
			}
			messages = append(messages, message)
		}

		// clear map
		es.eventMap = make(map[interface{}]Event)

		return messages
	}
	return nil
}
