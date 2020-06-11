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
	"errors"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type EventStore interface {
	Store(Event)
	CollectEvents() ([]*si.EventRecord, error)
}

type defaultEventStore struct {
	eventMap map[interface{}]Event

	sync.RWMutex
}

func newEventStoreImpl() EventStore {
	return &defaultEventStore{
		eventMap: make(map[interface{}]Event),
	}
}

func (es *defaultEventStore) Store(event Event) {
	es.Lock()
	defer es.Unlock()

	es.eventMap[event.GetSource()] = event
}

func (es *defaultEventStore) CollectEvents() ([]*si.EventRecord, error) {
	es.Lock()
	defer es.Unlock()

	messages := make([]*si.EventRecord, 0)
	errorList := make([]string, 0)

	// collect eventChannel
	for _, v := range es.eventMap {
		message, err := toEventMessage(v)
		if err != nil {
			log.Logger().Debug("could not translate object to EventMessage", zap.Any("object", v))
			errorList = append(errorList, err.Error())
			continue
		}
		messages = append(messages, message)
	}

	// clear map
	es.eventMap = make(map[interface{}]Event)

	var errorsToReturn error
	if len(errorList) > 0 {
		errorsToReturn = errors.New(strings.Join(errorList, ","))
	}
	return messages, errorsToReturn
}
