/*
Copyright 2019 The Unity Scheduler Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package configs

import (
	"fmt"
	"gotest.tools/assert"
	"testing"
	"time"
)

// test singleton
func TestGetConfigWatcher(t *testing.T) {
	sum := []byte("abc")
	cw0 := GetConfigWatcher(sum, "file")
	cw1 := GetConfigWatcher(sum, "file")
	assert.Equal(t, cw0, cw1)
}

// this test simulates a file stays same for some time and then changes,
// it verifies the callback is not triggered until file state changes.
func TestTriggerCallback(t *testing.T) {
	// reset configWatcher before each test
	checkSumChan := make(chan int)
	callbackChan := make(chan int)

	// simulate file state stays same
	FileCheckSummer = func(path string) (bytes []byte, e error) {
		checkSumChan <- 0
		return []byte("abc"), nil
	}

	// the original checksum
	sum := []byte("abc")
	cw := newInstance(sum, "file")

	// add a callback, this is triggered when state changes
	err := cw.AddCallback(func() {
		callbackChan <- 0
	})
	assert.Assert(t, err == nil)

	// wait until checksum being called for 3 times,
	// ensure that callback is still not invoked
	wait(t, checkSumChan, 3, 10*time.Second)
	assert.Equal(t, len(callbackChan), 0)

	// simulate file state changes
	FileCheckSummer = func(path string) (bytes []byte, e error) {
		checkSumChan <- 0
		return []byte("bcd"), nil
	}

	wait(t, checkSumChan, 1, 10*time.Second)
	assert.Equal(t, len(configWatcher.callbacks), 0)
}

func TestMultipleCallbacks(t *testing.T) {
	// reset configWatcher before each test
	configWatcher = nil

	checkSumChan := make(chan int, 10)
	callbackChan := make(chan int, 10)

	// simulate file state changes
	FileCheckSummer = func(path string) (bytes []byte, e error) {
		checkSumChan <- 0
		return []byte("bcd"), nil
	}

	sum := []byte("abc")
	cw := newInstance(sum, "file")

	var i int
	for i = 0; i < 10; i++ {
		err := cw.AddCallback(func() {
			callbackChan <- 0
		})
		assert.Assert(t, err == nil)
	}

	// verify all callbacks should be called
	wait(t, checkSumChan, 10, 30*time.Second)
	wait(t, callbackChan, 10, 30*time.Second)
	assert.Equal(t, len(configWatcher.callbacks), 0)
}

func TestCheckSumFailure(t *testing.T) {
	// reset configWatcher before each test
	configWatcher = nil

	callbackExecuted := false
	checkSumChan := make(chan int)
	callbackChan := make(chan int)

	FileCheckSummer = func(path string) (bytes []byte, e error) {
		checkSumChan <- 0
		return nil, fmt.Errorf("this is a failure")
	}

	sum := []byte("abc")
	cw := newInstance(sum, "file")

	err := cw.AddCallback(func() {
		callbackExecuted = true
		callbackChan <- 0
	})
	assert.Assert(t, err == nil)

	// check sum should be called once
	wait(t, checkSumChan, 1, 10*time.Second)
	assert.Equal(t, len(configWatcher.callbacks), 0)

	// due to the failure in calculating checksum,
	// callback is ignored
	assert.Equal(t, callbackExecuted, false)
}

func wait(t *testing.T, result chan int, expectedSize int, timeout time.Duration) {
	var actualSize = 0
	for {
		select {
		case <-result:
			actualSize++
			if actualSize == expectedSize {
				return
			}
		case <-time.After(timeout):
			t.Errorf("failed to reach expected state in given time")
		}
	}
}
