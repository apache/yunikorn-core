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

package common

import (
	"reflect"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"go.uber.org/zap"
)

type EventQueue struct {
	name       string
	eventQueue chan interface{}
}

func NewEventQueue(name string, size int) *EventQueue {
	return &EventQueue{
		name:       name,
		eventQueue: make(chan interface{}, size),
	}
}

func (q *EventQueue) EnqueueAndCheckFull(ev interface{}) {
	q.eventQueue <- ev
	log.Logger().Debug("enqueued event",
		zap.String("eventType", reflect.TypeOf(ev).String()),
		zap.Any("event", ev),
		zap.String("EventQueueName", q.name),
		zap.Int("currentQueueSize", len(q.eventQueue)))

	if cap(q.eventQueue) == len(q.eventQueue) {
		log.Logger().Warn("Event-queue is full, next event will be blocked",
			zap.String("EventQueueName", q.name))
	}

	if len(q.eventQueue)%1000 == 0 && len(q.eventQueue) > 5000 {
		log.Logger().Warn("#events are waiting in the queue",
			zap.String("EventQueueName", q.name),
			zap.Int("currentQueueSize", len(q.eventQueue)))
	}
}

func (q *EventQueue) Pop() interface{} {
	return <-q.eventQueue
}

func (q *EventQueue) size() int {
	return len(q.eventQueue)
}

func (q *EventQueue) cap() int {
	return cap(q.eventQueue)
}
