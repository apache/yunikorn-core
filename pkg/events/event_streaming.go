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

const defaultChannelBufSize = 1000

// EventStreaming implements the event streaming logic.
// New events are immediately forwarded to all active consumers.
type EventStreaming struct {
	buffer       *eventRingBuffer
	stopCh       chan struct{}
	eventStreams map[*EventStream]eventConsumerDetails
	sync.RWMutex
}

type eventConsumerDetails struct {
	local     chan *si.EventRecord
	consumer  chan<- *si.EventRecord
	stopCh    chan struct{}
	name      string
	createdAt time.Time
}

// EventStreamData contains data about an event stream.
type EventStreamData struct {
	Name      string
	CreatedAt time.Time
}

// EventStream handle type returned to the client that wants to capture the stream of events.
type EventStream struct {
	Events <-chan *si.EventRecord
}

// PublishEvent publishes an event to all event stream consumers.
//
// The streaming logic uses bridging to ensure proper ordering of existing and new events.
// Events are sent to the "local" channel from where it is forwarded to the "consumer" channel.
//
// If "local" is full, it means that the consumer side has not processed the events at an appropriate pace.
// Such a consumer is removed and the related channels are closed.
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
//
// When a consumer is finished, it must call RemoveEventStream to free up resources.
//
// Consumers have an arbitrary name for logging purposes. The "count" parameter defines the number
// of maximum historical events from the ring buffer. "0" is a valid value and means no past events.
func (e *EventStreaming) CreateEventStream(name string, count uint64) *EventStream {
	consumer := make(chan *si.EventRecord, defaultChannelBufSize)
	stream := &EventStream{
		Events: consumer,
	}
	local := make(chan *si.EventRecord, defaultChannelBufSize)
	stop := make(chan struct{})
	e.createEventStreamInternal(stream, local, consumer, stop, name)
	history := e.buffer.GetRecentEvents(count)

	go func(consumer chan<- *si.EventRecord, local <-chan *si.EventRecord, stop <-chan struct{}) {
		// Store the refs of historical events; it's possible that some events are added to the
		// ring buffer and also to "local" channel.
		// It is because we use two separate locks, so event updates are not atomic.
		// Example: an event has been just added to the ring buffer (before createEventStreamInternal()),
		// and execution is about to enter PublishEvent(); at this point we have an updated "eventStreams"
		// map, so "local" will also contain the new event.
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
	name string) {
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
}

// RemoveEventStream stops the streaming for a given consumer. Must be called to avoid resource leaks.
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

// Close stops event streaming completely.
func (e *EventStreaming) Close() {
	close(e.stopCh)
}

// GetEventStreams returns the current active event streams.
func (e *EventStreaming) GetEventStreams() []EventStreamData {
	e.RLock()
	defer e.RUnlock()
	var streams []EventStreamData
	for _, s := range e.eventStreams {
		streams = append(streams, EventStreamData{
			Name:      s.name,
			CreatedAt: s.createdAt,
		})
	}

	return streams
}

// NewEventStreaming creates a new event streaming infrastructure.
func NewEventStreaming(eventBuffer *eventRingBuffer) *EventStreaming {
	return &EventStreaming{
		buffer:       eventBuffer,
		stopCh:       make(chan struct{}),
		eventStreams: make(map[*EventStream]eventConsumerDetails),
	}
}
