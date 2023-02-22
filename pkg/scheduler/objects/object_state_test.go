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

package objects

import (
	"context"
	"testing"

	"gotest.tools/v3/assert"
)

func TestStateTransition(t *testing.T) {
	// base is active
	stateMachine := NewObjectState()
	assert.Equal(t, stateMachine.Current(), Active.String())

	// active to stopped
	err := stateMachine.Event(context.Background(), Stop.String(), "testobject")
	assert.Assert(t, err == nil)
	assert.Equal(t, stateMachine.Current(), Stopped.String())

	// remove on stopped not allowed
	err = stateMachine.Event(context.Background(), Remove.String(), "testobject")
	assert.Assert(t, err != nil)
	assert.Equal(t, stateMachine.Current(), Stopped.String())

	// stopped to active
	err = stateMachine.Event(context.Background(), Start.String(), "testobject")
	assert.Assert(t, err == nil)
	assert.Equal(t, stateMachine.Current(), Active.String())

	// active to draining
	err = stateMachine.Event(context.Background(), Remove.String(), "testobject")
	assert.Assert(t, err == nil)
	assert.Equal(t, stateMachine.Current(), Draining.String())

	// start on draining not allowed
	err = stateMachine.Event(context.Background(), Start.String(), "test_object")
	assert.Assert(t, err != nil)
	assert.Equal(t, stateMachine.Current(), Draining.String())

	// stop on draining not allowed
	err = stateMachine.Event(context.Background(), Stop.String(), "test_object")
	assert.Assert(t, err != nil)
	assert.Equal(t, stateMachine.Current(), Draining.String())
}

func TestTransitionToSelf(t *testing.T) {
	// base is active
	stateMachine := NewObjectState()

	// start on active
	err := stateMachine.Event(context.Background(), Start.String(), "testobject")
	assert.Assert(t, err != nil)
	if err != nil && err.Error() != noTransition {
		t.Errorf("state change failed with error: %v", err)
	}
	assert.Equal(t, stateMachine.Current(), Active.String())

	// remove on draining
	stateMachine.SetState(Draining.String())
	err = stateMachine.Event(context.Background(), Remove.String(), "testobject")
	assert.Assert(t, err != nil)
	if err != nil && err.Error() != noTransition {
		t.Errorf("state change failed with error: %v", err)
	}
	assert.Equal(t, stateMachine.Current(), Draining.String())

	// stop on stopped
	stateMachine.SetState(Stopped.String())
	err = stateMachine.Event(context.Background(), Stop.String(), "testobject")
	assert.Assert(t, err != nil)
	if err != nil && err.Error() != noTransition {
		t.Errorf("state change failed with error: %v", err)
	}
	assert.Equal(t, stateMachine.Current(), Stopped.String())
}
