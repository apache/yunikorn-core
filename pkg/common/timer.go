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
    "sync"
    "time"
)

type Timer interface {
    NanoTimeNow() int64
    SetNanoTimeNow(int64)
    Sleep(duration time.Duration)
}

type regularTimer struct {
    Timer
}

func (rt *regularTimer) NanoTimeNow() int64 {
    return time.Now().UnixNano()
}

func (rt *regularTimer) SetNanoTimeNow(int64) {
}

func (rt *regularTimer) Sleep(duration time.Duration) {
    time.Sleep(duration)
}

type mockTimer struct {
    Timer
    now int64
    sync.RWMutex
}

func (mt *mockTimer) NanoTimeNow() int64 {
    mt.RLock()
    defer mt.RUnlock()
    return mt.now
}

func (mt *mockTimer) SetNanoTimeNow(now int64) {
    mt.Lock()
    defer mt.Unlock()
    mt.now = now
}

func (mt *mockTimer) Sleep(duration time.Duration) {
    mt.Lock()
    defer mt.Unlock()
    mt.now += duration.Nanoseconds()
}

func NewTimer() Timer {
    return &regularTimer{}
}

func NewMockTimer() Timer {
    return &mockTimer{}
}
