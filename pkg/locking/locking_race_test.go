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
	"sync"
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

// TestLockOrderDetection
// lock order detection looks at the ordering of the same mutexes in different go routines
// if the order changes for two different go routines then that could be a potential deadlock
// this case happens in preemption when looking for victims when queues hover around guaranteed
func TestLockOrderDetection(t *testing.T) {
	var tests = []struct {
		name    string
		disable bool
	}{
		{"ordered", false},
		{"no order", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enableTrackingWithOrder(tt.disable)
			deadlockDetected.Store(false)
			defer disableTracking()

			var wg sync.WaitGroup
			wg.Add(1)
			var a, b RWMutex
			go func() { // lock ordering: a, b, b, a
				defer wg.Done()
				a.Lock()
				b.RLock()
				b.RUnlock()
				a.Unlock()
			}()
			wg.Wait()
			wg.Add(1)
			go func() { // lock ordering: b, a, a, b
				defer wg.Done()
				b.Lock()
				a.RLock()
				a.RUnlock()
				b.Unlock()
			}()
			wg.Wait()
			assert.Assert(t, IsDeadlockDetected() == tt.disable, "Deadlock detected not as expected")
		})
	}
}
