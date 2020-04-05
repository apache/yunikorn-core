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
	"testing"

	"gotest.tools/assert"
)

func TestEventQueue(t *testing.T) {
	eventQueue := NewEventQueue("aa", 5)
	for i := 0; i < 5; i++ {
		eventQueue.EnqueueAndCheckFull(i)
	}
	assert.Equal(t, 5, eventQueue.size())
	for i := 0; i < 5; i++ {
		v := eventQueue.Pop()
		assert.Equal(t, v, i)
	}
}
