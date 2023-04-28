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
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

var defaultCount = uint64(10000)

func TestEventStreaming_WithoutHistory(t *testing.T) {
	buffer := newEventRingBuffer(10)
	streaming := NewEventStreaming(buffer)
	es := streaming.CreateEventStream("test", defaultCount)
	defer streaming.Close()

	sent := &si.EventRecord{
		Message: "testMessage",
	}
	streaming.PublishEvent(sent)
	received := receive(t, es.Events)
	streaming.RemoveEventStream(es)
	assert.Equal(t, 0, len(streaming.eventStreams[es].local))
	assert.Equal(t, 0, len(streaming.eventStreams[es].consumer))
	assert.Equal(t, received.Message, sent.Message)
	assert.Equal(t, 0, len(streaming.eventStreams))
}

func TestEventStreaming_WithHistory(t *testing.T) {
	buffer := newEventRingBuffer(10)
	streaming := NewEventStreaming(buffer)
	defer streaming.Close()

	buffer.Add(&si.EventRecord{TimestampNano: 1})
	buffer.Add(&si.EventRecord{TimestampNano: 5})
	buffer.Add(&si.EventRecord{TimestampNano: 6})
	buffer.Add(&si.EventRecord{TimestampNano: 9})
	es := streaming.CreateEventStream("test", defaultCount)

	streaming.PublishEvent(&si.EventRecord{TimestampNano: 10})

	received1 := receive(t, es.Events)
	received2 := receive(t, es.Events)
	received3 := receive(t, es.Events)
	received4 := receive(t, es.Events)
	received5 := receive(t, es.Events)
	assert.Equal(t, 0, len(streaming.eventStreams[es].local))
	assert.Equal(t, 0, len(streaming.eventStreams[es].consumer))
	assert.Equal(t, int64(1), received1.TimestampNano)
	assert.Equal(t, int64(5), received2.TimestampNano)
	assert.Equal(t, int64(6), received3.TimestampNano)
	assert.Equal(t, int64(9), received4.TimestampNano)
	assert.Equal(t, int64(10), received5.TimestampNano)
	streaming.RemoveEventStream(es)
	assert.Equal(t, 0, len(streaming.eventStreams))
}

func TestEventStreaming_WithHistoryCount(t *testing.T) {
	buffer := newEventRingBuffer(10)
	streaming := NewEventStreaming(buffer)
	defer streaming.Close()

	buffer.Add(&si.EventRecord{TimestampNano: 1})
	buffer.Add(&si.EventRecord{TimestampNano: 5})
	buffer.Add(&si.EventRecord{TimestampNano: 6})
	buffer.Add(&si.EventRecord{TimestampNano: 9})
	es := streaming.CreateEventStream("test", 2)

	streaming.PublishEvent(&si.EventRecord{TimestampNano: 10})

	received1 := receive(t, es.Events)
	received2 := receive(t, es.Events)
	received3 := receive(t, es.Events)
	assert.Equal(t, 0, len(streaming.eventStreams[es].local))
	assert.Equal(t, 0, len(streaming.eventStreams[es].consumer))
	assert.Equal(t, int64(6), received1.TimestampNano)
	assert.Equal(t, int64(9), received2.TimestampNano)
	assert.Equal(t, int64(10), received3.TimestampNano)
}

func TestEventStreaming_TwoConsumers(t *testing.T) {
	buffer := newEventRingBuffer(10)
	streaming := NewEventStreaming(buffer)
	defer streaming.Close()

	es1 := streaming.CreateEventStream("stream1", defaultCount)
	es2 := streaming.CreateEventStream("stream2", defaultCount)
	for i := 0; i < 5; i++ {
		streaming.PublishEvent(&si.EventRecord{TimestampNano: int64(i)})
	}

	for i := 0; i < 5; i++ {
		assert.Equal(t, int64(i), receive(t, es1.Events).TimestampNano)
		assert.Equal(t, int64(i), receive(t, es2.Events).TimestampNano)
	}
	assert.Equal(t, 0, len(streaming.eventStreams[es1].local))
	assert.Equal(t, 0, len(streaming.eventStreams[es1].consumer))
	assert.Equal(t, 0, len(streaming.eventStreams[es2].local))
	assert.Equal(t, 0, len(streaming.eventStreams[es2].consumer))
}

func TestEventStreaming_SlowConsumer(t *testing.T) {
	// simulating a slow event consumer by ignoring events
	buffer := newEventRingBuffer(10)
	streaming := NewEventStreaming(buffer)
	defer streaming.Close()
	streaming.CreateEventStream("test", 10000)

	for i := 0; i < 300; i++ {
		streaming.PublishEvent(&si.EventRecord{TimestampNano: int64(i)})
	}

	assert.Equal(t, 0, len(streaming.eventStreams))
}

func receive(t *testing.T, input <-chan *si.EventRecord) *si.EventRecord {
	select {
	case event := <-input:
		return event
	case <-time.After(time.Second):
		t.Fatal("receive failed")
		return nil
	}
}
