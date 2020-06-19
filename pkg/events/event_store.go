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

	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type EventStore interface {
	Store(event *si.EventRecord)
	CollectEvents() []*si.EventRecord
}

type defaultEventStore struct {
	eventMap map[string]*si.EventRecord

	sync.RWMutex
}

func newEventStoreImpl() EventStore {
	return &defaultEventStore{
		eventMap: make(map[string]*si.EventRecord),
	}
}

func (es *defaultEventStore) Store(event *si.EventRecord) {
	es.Lock()
	defer es.Unlock()

	es.eventMap[event.ObjectID] = event
}

func (es *defaultEventStore) CollectEvents() []*si.EventRecord {
	es.Lock()
	defer es.Unlock()

	messages := make([]*si.EventRecord, 0)
	for _, v := range es.eventMap {
		messages = append(messages, v)
	}

	// TODO how not to clear map
	es.eventMap = make(map[string]*si.EventRecord)

	metrics.GetEventMetrics().AddEventsCollected(len(messages))
	return messages
}
