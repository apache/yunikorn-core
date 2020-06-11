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

	"gotest.tools/assert"
)

func isChannelEmpty(channel chan Event) bool {
	select {
	case <- channel:
		return false
	default:
		return true
	}
}

func TestEmptyChannel(t *testing.T) {
	eventChannel := newEventChannelImpl(1)
	assert.Equal(t, isChannelEmpty(eventChannel.GetChannel()), true)
}

func TestPushAndRetrieve(t *testing.T) {
	eventChannel := newEventChannelImpl(1)
	assert.Equal(t, isChannelEmpty(eventChannel.GetChannel()), true)

	newEvent := &baseEvent{}
	eventChannel.AddEvent(newEvent)
	select {
	case event := <- eventChannel.GetChannel():
		assert.Equal(t, event, newEvent)
	default:
		t.Fatal("expected event object in EventChannel")
	}

	assert.Equal(t, isChannelEmpty(eventChannel.GetChannel()), true)
}

func TestLimit(t *testing.T) {
	eventChannel := newEventChannelImpl(1)
	event1 := &baseEvent{
		reason: "reason1",
	}
	event2 := &baseEvent{
		reason: "reason2",
	}

	eventChannel.AddEvent(event1)
	eventChannel.AddEvent(event2)

	select {
	case event := <- eventChannel.GetChannel():
		assert.Equal(t, event, event1)
	default:
		t.Fatal("expected event object in EventChannel")
	}

	assert.Equal(t, isChannelEmpty(eventChannel.GetChannel()), true)
}
