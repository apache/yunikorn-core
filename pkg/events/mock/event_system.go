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

package mock

import (
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type EventSystem struct {
	Events  []*si.EventRecord
	enabled bool
}

func (m *EventSystem) CreateEventStream(_ string, _ uint64) *events.EventStream {
	return nil
}

func (m *EventSystem) RemoveStream(_ *events.EventStream) {
}

func (m *EventSystem) AddEvent(event *si.EventRecord) {
	m.Events = append(m.Events, event)
}

func (m *EventSystem) StartService() {}

func (m *EventSystem) Stop() {}

func (m *EventSystem) Reset() {
	m.Events = make([]*si.EventRecord, 0)
}

func (m *EventSystem) GetEventsFromID(uint64, uint64) ([]*si.EventRecord, uint64, uint64) {
	return nil, 0, 0
}

func (m *EventSystem) IsEventTrackingEnabled() bool {
	return m.enabled
}

func (m *EventSystem) GetEventStreams() []events.EventStreamData {
	return nil
}

func NewEventSystem() *EventSystem {
	return &EventSystem{Events: make([]*si.EventRecord, 0), enabled: true}
}
func NewEventSystemDisabled() *EventSystem {
	return &EventSystem{Events: make([]*si.EventRecord, 0), enabled: false}
}
