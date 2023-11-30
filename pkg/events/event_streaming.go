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

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const defaultChannelBufSize = 100

type EventStreaming struct {
	buffer       *eventRingBuffer
	stopCh       chan struct{}
	eventStreams map[*EventStream]eventConsumerDetails
	sync.Mutex
}

type eventConsumerDetails struct {
	local     chan *si.EventRecord
	consumer  chan<- *si.EventRecord
	stopCh    chan struct{}
	name      string
	createdAt time.Time
}

// EventStream handle type returned to the client who wants to capture the stream of events.
type EventStream struct {
	Events <-chan *si.EventRecord
}

// PublishEvent publishes an event to all event stream consumers.
func (e *EventStreaming) PublishEvent(event *si.EventRecord) {
	e.Lock()
	defer e.Unlock()

	for consumer, details := range e.eventStreams {
		if len(details.local) == defaultChannelBufSize {
			log.Log(log.Events).Warn("Listener buffer full due to potentially slow consumer, removing it")
			e.removeEventStream(consumer)
			continue
		}

		details.local <- event
	}
}

// CreateEventStream sets up event streaming for a consumer. The returned EventStream object
// contains a channel that can be used for reading.
// When a consumer is finished, it is supposed to call RemoveEventStream to free up resources.
// Consumers have an arbitrary name for logging purposes. The "count" parameter defines the number
// of maximum historical events from the ring buffer.
func (e *EventStreaming) CreateEventStream(name string, count uint64) *EventStream {
	consumer := make(chan *si.EventRecord, defaultChannelBufSize)
	stream := &EventStream{
		Events: consumer,
	}
	local := make(chan *si.EventRecord, defaultChannelBufSize)
	stop := make(chan struct{})
	history := e.createEventStreamInternal(stream, local, consumer, stop, name, count)

	go func(consumer chan<- *si.EventRecord, local <-chan *si.EventRecord, stop <-chan struct{}) {
		// store the refs of historical events; it's possible that some events are added to the
		// ring buffer and also to "local" channel
		seen := make(map[*si.EventRecord]bool)
		for _, event := range history {
			consumer <- event
			seen[event] = true
		}
		for {
			select {
			case <-e.stopCh:
				close(consumer)
				return
			case <-stop:
				close(consumer)
				return
			case event := <-local:
				if seen[event] {
					continue
				}
				// since events are processed in a single goroutine, doubling is no longer
				// possible at this point
				seen = make(map[*si.EventRecord]bool)
				consumer <- event
			}
		}
	}(consumer, local, stop)

	log.Log(log.Events).Info("Created event stream", zap.String("consumer name", name))
	return stream
}

func (e *EventStreaming) createEventStreamInternal(stream *EventStream,
	local chan *si.EventRecord,
	consumer chan *si.EventRecord,
	stop chan struct{},
	name string,
	count uint64) []*si.EventRecord {
	// stuff that needs locking
	e.Lock()
	defer e.Unlock()

	e.eventStreams[stream] = eventConsumerDetails{
		local:     local,
		consumer:  consumer,
		stopCh:    stop,
		name:      name,
		createdAt: time.Now(),
	}

	return e.buffer.GetRecentEvents(count)
}

func (e *EventStreaming) RemoveEventStream(consumer *EventStream) {
	e.Lock()
	defer e.Unlock()

	e.removeEventStream(consumer)
}

func (e *EventStreaming) removeEventStream(consumer *EventStream) {
	if details, ok := e.eventStreams[consumer]; ok {
		log.Log(log.Events).Info("Removing event stream consumer", zap.String("name", details.name),
			zap.Time("creation time", details.createdAt))
		close(details.stopCh)
		close(details.local)
		delete(e.eventStreams, consumer)
	}
}

func (e *EventStreaming) Close() {
	close(e.stopCh)
}

func NewEventStreaming(eventBuffer *eventRingBuffer) *EventStreaming {
	stopCh := make(chan struct{})
	e := &EventStreaming{
		buffer:       eventBuffer,
		stopCh:       stopCh,
		eventStreams: make(map[*EventStream]eventConsumerDetails),
	}

	return e
}
