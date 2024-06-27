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
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type EventPlugin struct {
	ResourceManagerCallback
	records chan *si.EventRecord

	locking.Mutex
}

func (m *EventPlugin) SendEvent(events []*si.EventRecord) {
	m.Lock()
	defer m.Unlock()

	for _, event := range events {
		m.records <- event
	}
}

func (m *EventPlugin) GetNextEventRecord() *si.EventRecord {
	m.Lock()
	defer m.Unlock()

	select {
	case record := <-m.records:
		return record
	default:
		return nil
	}
}

// NewEventPlugin creates a mocked event plugin
func NewEventPlugin() *EventPlugin {
	return &EventPlugin{
		records: make(chan *si.EventRecord, 3),
	}
}
