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

package rmproxy

import (
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
)

// MockedRMProxy Implements RMProxy Mock Event Handler for testing
type MockedRMProxy struct {
	handled bool
	events  []interface{}
	locking.RWMutex
}

func NewMockedRMProxy() *MockedRMProxy {
	return &MockedRMProxy{}
}

// HandleEvent implements event handling for a limited set of events for testing
func (rmp *MockedRMProxy) HandleEvent(ev interface{}) {
	rmp.Lock()
	defer rmp.Unlock()
	if rmp.events == nil {
		rmp.events = make([]interface{}, 0)
	}
	rmp.events = append(rmp.events, ev)
	var c chan *rmevent.Result
	switch v := ev.(type) {
	case *rmevent.RMApplicationUpdateEvent:
		rmp.handled = true
	case *rmevent.RMNewAllocationsEvent:
		c = v.Channel
	case *rmevent.RMReleaseAllocationEvent:
		c = v.Channel
	}
	if c != nil {
		go func(rc chan *rmevent.Result) {
			rc <- &rmevent.Result{Succeeded: true, Reason: "test"}
		}(c)
	}
}

// IsHandled return the last action performed by the handler and reset
func (rmp *MockedRMProxy) IsHandled() bool {
	rmp.Lock()
	defer rmp.Unlock()
	keep := rmp.handled
	rmp.handled = false
	return keep
}

// GetEvents return the list of events processed by the handler and reset
func (rmp *MockedRMProxy) GetEvents() []interface{} {
	rmp.RLock()
	defer rmp.RUnlock()
	return rmp.events
}
