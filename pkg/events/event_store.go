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
	"go.uber.org/zap"
)

const keepTime = 24 * time.Hour // 1 day

type EventStore struct {
	apps   *AppEventStore
	queues *QueueEventStore
}

func NewEventStore() *EventStore {
	return &EventStore{
		NewAppEventStore(),
		NewQueueEventStore(),
	}
}

func (es EventStore) Visit(event interface{}) {
	// TODO process event
	switch v := event.(type) {
	case *AppEvent:
		es.apps.VisitApp(v.app)
	case *QueueEvent:
		es.queues.VisitQueue(v.queue)
	default:
		log.Logger().Error("event could not be processed in the event cache.", zap.Any("event", v))
	}
}


func (es EventStore) GetAppLastVisitTime(app string) time.Time {
	return es.apps.GetAppLastVisitTime(app)
}

func (es EventStore) GetQueueLastVisitTime(queue string) time.Time {
	return es.queues.GetQueueLastVisitTime(queue)
}


func (es EventStore) ClearOldObjects() {
	es.apps.clearOldApps()
	es.queues.clearOldQueues()
}
