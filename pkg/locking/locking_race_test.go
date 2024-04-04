//go:build !race

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

//nolint:staticcheck
package locking

import (
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestDeadlockDetection(t *testing.T) {
	enableTracking()
	deadlockDetected.Store(false)
	defer disableTracking()

	var mutex Mutex
	go func() {
		mutex.Lock()
		mutex.Lock()   // will deadlock
		mutex.Unlock() // will unwind second lock
	}()
	time.Sleep(2 * time.Second)
	mutex.Unlock() // will unwind first lock
	assert.Assert(t, IsDeadlockDetected(), "Deadlock should have been detected")
}
