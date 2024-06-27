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

package locking

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	godeadlock "github.com/sasha-s/go-deadlock"

	"github.com/apache/yunikorn-core/pkg/log"
)

const (
	EnvDeadlockDetectionEnabled = "DEADLOCK_DETECTION_ENABLED"
	EnvDeadlockTimeoutSeconds   = "DEADLOCK_TIMEOUT_SECONDS"
	EnvExitOnDeadlock           = "DEADLOCK_EXIT"
	EnvDisableLockOrder         = "DEADLOCK_DISABLE_LOCK_ORDER"
)

var (
	once               sync.Once
	trackingEnabled    atomic.Bool
	timeoutSeconds     atomic.Int32
	deadlockDetected   atomic.Bool
	testingMode        atomic.Bool
	exitOnDeadlock     atomic.Bool
	disableOrderDetect atomic.Bool
)

type errorBuf struct {
	data string
	sync.Mutex
}

func (b *errorBuf) Write(p []byte) (n int, err error) {
	if b == nil {
		return len(p), nil
	}
	b.Lock()
	defer b.Unlock()
	b.data += string(p)
	return len(p), nil
}

func init() {
	once.Do(reInit)
}

func reInit() {
	enabled, err := strconv.ParseBool(os.Getenv(EnvDeadlockDetectionEnabled))
	if err != nil {
		enabled = false
	}
	trackingEnabled.Store(enabled)

	var timeoutSec int64
	timeoutSec, err = strconv.ParseInt(os.Getenv(EnvDeadlockTimeoutSeconds), 10, 32)
	if err != nil {
		timeoutSec = 60
	}
	timeoutSeconds.Store(int32(timeoutSec))

	var disableOrder bool
	disableOrder, err = strconv.ParseBool(os.Getenv(EnvDisableLockOrder))
	if err != nil {
		disableOrder = false
	}
	disableOrderDetect.Store(disableOrder)

	var exitOnDetect bool
	exitOnDetect, err = strconv.ParseBool(os.Getenv(EnvExitOnDeadlock))
	if err != nil {
		exitOnDetect = false
	}
	exitOnDeadlock.Store(exitOnDetect)

	// set deadlock detection options
	godeadlock.Opts.Disable = !enabled
	godeadlock.Opts.DeadlockTimeout = time.Duration(timeoutSec) * time.Second
	godeadlock.Opts.LogBuf = &errorBuf{}
	godeadlock.Opts.OnPotentialDeadlock = onPotentialDeadlock
	godeadlock.Opts.DisableLockOrderDetection = disableOrder

	if enabled {
		// We want to ensure that we write this before any other subsystem is initialized, including logging which may also use locks.
		// no way to handle errors just ignore
		_, _ = fmt.Fprintf(os.Stderr, "=== Deadlock detection enabled (timeout: %d seconds, exit on deadlock: %t, locking order disabled: %t) ===\n", timeoutSec, exitOnDetect, disableOrder)
	}
}

func onPotentialDeadlock() {
	deadlockDetected.Store(true)
	printBufContents()
	if exitOnDeadlock.Load() && !testingMode.Load() {
		os.Exit(1)
	}
}

func printBufContents() {
	buf, ok := godeadlock.Opts.LogBuf.(*errorBuf)
	buf.Lock()
	defer buf.Unlock()
	if !ok {
		log.Log(log.Diagnostics).Error("POTENTIAL DEADLOCK: No details available")
	} else {
		log.Log(log.Diagnostics).Error(buf.data)
	}
	buf.data = ""
}

func IsTrackingEnabled() bool {
	return trackingEnabled.Load()
}

func GetDeadlockTimeoutSeconds() int {
	return int(timeoutSeconds.Load())
}

func IsDeadlockDetected() bool {
	return deadlockDetected.Load()
}

type Mutex struct {
	godeadlock.Mutex
}

type RWMutex struct {
	godeadlock.RWMutex
}
