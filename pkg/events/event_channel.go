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
	"fmt"
	"strconv"
	"sync"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
)


type EventChannel interface {
	GetNextEvent() (Event, bool)
	AddEvent(event Event)
}

type defaultEventChannel struct {
	events       chan Event
	diagCounter  int
	diagInterval int

	sync.RWMutex
}

func newEventChannelImpl(eventChannelSize int) EventChannel {
	return &defaultEventChannel{
		events:       make(chan Event, eventChannelSize),
		diagCounter:  0,
		diagInterval: eventChannelSize,
	}

}

func (ec *defaultEventChannel) GetNextEvent() (Event, bool) {
	ec.Lock()
	defer ec.Unlock()

	select {
	case msg, ok := <-ec.events:
		return msg, ok
	default:
		return nil, false
	}
}


func (ec *defaultEventChannel) AddEvent(event Event) {
	ec.Lock()
	defer ec.Unlock()

	ec.diagCounter += 1
	if ec.diagCounter >= ec.diagInterval {
		msg := fmt.Sprintf("Event cache channel has %s size and %s capacity.", strconv.Itoa(len(ec.events)), strconv.Itoa(cap(ec.events)))
		log.Logger().Debug(msg)
		ec.diagCounter = 0
	}
	select {
		case ec.events <- event:
			// event is successfully pushed to channel
		default:
			// if the channel is full, emitting log entries on DEBUG=< level is going to have serious performance impact
			log.Logger().Debug("Channel is full - discarding event.")
	}
}