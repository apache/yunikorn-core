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
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	godeadlock "github.com/sasha-s/go-deadlock"
)

func disableTracking() {
	_ = os.Unsetenv(EnvDeadlockDetectionEnabled)
	_ = os.Unsetenv(EnvDeadlockTimeoutSeconds)
	_ = os.Unsetenv(EnvDisableLockOrder)
	testingMode.Store(false)
	reInit()
}

func enableTracking() {
	enableTrackingWithOrder(true)
}

func enableTrackingWithOrder(enableOrder bool) {
	_ = os.Setenv(EnvDeadlockDetectionEnabled, "true")
	_ = os.Setenv(EnvDeadlockTimeoutSeconds, "1")
	_ = os.Setenv(EnvDisableLockOrder, fmt.Sprint(!enableOrder))
	testingMode.Store(true)
	reInit()
}

func BenchmarkSyncMutex(b *testing.B) {
	lock := sync.Mutex{}
	for i := 0; i < b.N; i++ {
		lock.Lock()
		lock.Unlock()
	}
}

func BenchmarkSyncRWMutexRead(b *testing.B) {
	lock := sync.RWMutex{}
	for i := 0; i < b.N; i++ {
		lock.RLock()
		lock.RUnlock()
	}
}

func BenchmarkSyncRWMutexWrite(b *testing.B) {
	lock := sync.RWMutex{}
	for i := 0; i < b.N; i++ {
		lock.Lock()
		lock.Unlock()
	}
}

func BenchmarkGoDeadlockMutex(b *testing.B) {
	enableTracking()
	defer disableTracking()
	lock := godeadlock.Mutex{}
	for i := 0; i < b.N; i++ {
		lock.Lock()
		lock.Unlock()
	}
}

func BenchmarkGoDeadlockRWMutexRead(b *testing.B) {
	enableTracking()
	defer disableTracking()
	lock := godeadlock.RWMutex{}
	for i := 0; i < b.N; i++ {
		lock.RLock()
		lock.RUnlock()
	}
}

func BenchmarkGoDeadlockRWMutexWrite(b *testing.B) {
	enableTracking()
	defer disableTracking()
	lock := godeadlock.RWMutex{}
	for i := 0; i < b.N; i++ {
		lock.Lock()
		lock.Unlock()
	}
}

func BenchmarkUntrackedMutex(b *testing.B) {
	disableTracking()
	var lock Mutex
	for i := 0; i < b.N; i++ {
		lock.Lock()
		lock.Unlock()
	}
}

func BenchmarkUntrackedRWMutexRead(b *testing.B) {
	disableTracking()
	var lock RWMutex
	for i := 0; i < b.N; i++ {
		lock.RLock()
		lock.RUnlock()
	}
}

func BenchmarkUntrackedRWMutexWrite(b *testing.B) {
	disableTracking()
	var lock RWMutex
	for i := 0; i < b.N; i++ {
		lock.Lock()
		lock.Unlock()
	}
}
func BenchmarkTrackedMutex(b *testing.B) {
	enableTracking()
	defer disableTracking()
	var lock Mutex
	for i := 0; i < b.N; i++ {
		lock.Lock()
		lock.Unlock()
	}
}

func BenchmarkTrackedRWMutexRead(b *testing.B) {
	enableTracking()
	defer disableTracking()
	var lock RWMutex
	for i := 0; i < b.N; i++ {
		lock.RLock()
		lock.RUnlock()
	}
}

func BenchmarkTrackedRWMutexWrite(b *testing.B) {
	enableTracking()
	defer disableTracking()
	var lock RWMutex
	for i := 0; i < b.N; i++ {
		lock.Lock()
		lock.Unlock()
	}
}

func BenchmarkTrackedNoOrderMutex(b *testing.B) {
	enableTrackingWithOrder(false)
	defer disableTracking()
	var lock Mutex
	for i := 0; i < b.N; i++ {
		lock.Lock()
		lock.Unlock()
	}
}

func BenchmarkTrackedNoOrderRWMutexRead(b *testing.B) {
	enableTrackingWithOrder(false)
	defer disableTracking()
	var lock RWMutex
	for i := 0; i < b.N; i++ {
		lock.RLock()
		lock.RUnlock()
	}
}

func BenchmarkTrackedNoOrderRWMutexWrite(b *testing.B) {
	enableTrackingWithOrder(false)
	defer disableTracking()
	var lock RWMutex
	for i := 0; i < b.N; i++ {
		lock.Lock()
		lock.Unlock()
	}
}

func TestMutex(t *testing.T) {
	var mutex Mutex
	var result atomic.Int32
	mutex.Lock()
	go func() {
		mutex.Lock()
		result.Store(2)
		mutex.Unlock()
	}()
	time.Sleep(100 * time.Millisecond)
	result.Store(1)
	mutex.Unlock()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(2), result.Load())
}

func TestRWMutex(t *testing.T) {
	var mutex RWMutex
	var count atomic.Int32
	mutex.RLock()
	go func() {
		mutex.Lock()
		count.Add(1)
		mutex.Unlock()
	}()
	go func() {
		mutex.Lock()
		count.Add(1)
		mutex.Unlock()
	}()
	time.Sleep(100 * time.Millisecond)
	before := count.Load()
	mutex.RUnlock()
	time.Sleep(500 * time.Millisecond)
	after := count.Load()
	assert.Equal(t, before, int32(0))
	assert.Equal(t, after, int32(2))
}
