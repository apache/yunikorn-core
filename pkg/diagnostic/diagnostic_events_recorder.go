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

package diagnostic

import (
    "github.com/apache/incubator-yunikorn-core/pkg/api"
    "github.com/apache/incubator-yunikorn-core/pkg/common"
    "sync"
    "time"
)

var initOnce sync.Once
var recorder *DiagEventsRecorder

type Level int

const (
    Debug Level = iota
    Info        = iota
    Warn        = iota
    Error       = iota
)

type DiagEventsRecorder struct {
    eventMap map[string]*api.DiagnosticEvent
    timer    common.Timer

    // Nano-second interval of record for same key
    minimumRecordIntervalForSameKeyInNS int64
    // How long it takes to expire an event (and will be removed from cache)
    recordExpireIntervalInNS int64
    // Wait time In NS between cleanup runs
    waitTimeBetweenCleanupRuns time.Duration

    sync.RWMutex
}

func emptyOrAlternative(s string) string {
    if s == "" {
        return "(_)"
    }
    return s
}

func (de *DiagEventsRecorder) shouldRecord(level Level, uid string) bool {
    de.RLock()
    defer de.RUnlock()

    // check level
    // TODO

    // check timestamp
    event := de.eventMap[uid]
    if event == nil {
        return true
    }
    if de.timer.NanoTimeNow()-event.NanoTimestamp < de.minimumRecordIntervalForSameKeyInNS {
        return false
    }

    return true
}

func (de *DiagEventsRecorder) RecordResourceRequestEvent(level Level, requestId string, appId string, queueName string, nodeId string, message string) {
    // Use one single layer of uid, instead of multi-layer map.
    requestPrimaryKey := emptyOrAlternative(requestId) + emptyOrAlternative(appId) + emptyOrAlternative(nodeId)

    if de.shouldRecord(level, requestPrimaryKey) {
        de.recordResourceRequestEventInternal(requestPrimaryKey, level, requestId, appId, queueName, nodeId, message, de.timer.NanoTimeNow())
    }
}

func (de *DiagEventsRecorder) recordResourceRequestEventInternal(requestPrimaryKey string, level Level, requestId string, appId string, queueName string, nodeId string,
    message string,
    nanoTs int64) {
    ev := &api.DiagnosticEvent{
        RequestId:     requestId,
        AppId:         appId,
        QueueName:     queueName,
        NodeId:        nodeId,
        Message:       message,
        NanoTimestamp: nanoTs,
    }

    de.Lock()
    defer de.Unlock()
    de.eventMap[requestPrimaryKey] = ev
}

// It should be invoked by a period-executed goroutine and cleanup too old events
func (de *DiagEventsRecorder) start() {
    // Run cleanup goroutine
    go func() {
        for {
            de.cleanupOldEvents()
            de.timer.Sleep(de.waitTimeBetweenCleanupRuns)
        }
    }()
}

func (de *DiagEventsRecorder) cleanupOldEvents() {
    de.Lock()
    defer de.Unlock()

    if len(de.eventMap) == 0 {
        return
    }

    for k, v := range de.eventMap {
        if de.timer.NanoTimeNow()-v.NanoTimestamp > de.recordExpireIntervalInNS {
            delete(de.eventMap, k)
        }
    }
}

func (de *DiagEventsRecorder) FlushEvents() []*api.DiagnosticEvent {
    de.Lock()
    defer de.Unlock()

    if len(de.eventMap) == 0 {
        return nil
    }

    ret := make([]*api.DiagnosticEvent, len(de.eventMap))
    idx := 0
    for _, v := range de.eventMap {
        ret[idx] = v
        idx ++
    }

    de.eventMap = make(map[string]*api.DiagnosticEvent)
    return ret
}

// Invoked by external method (like scheduler to get instance)
func GetEventsRecorder() *DiagEventsRecorder {
    initOnce.Do(func() {
        if recorder == nil {
            recorder = newDiagEventsRecorder()
            recorder.start()
        }
    })
    return recorder
}

func newDiagEventsRecorder() *DiagEventsRecorder {
    recorder := &DiagEventsRecorder{
        eventMap: make(map[string]*api.DiagnosticEvent),
        timer:    common.NewTimer(),

        // Default interval is one sec
        minimumRecordIntervalForSameKeyInNS: time.Second.Nanoseconds(),
        waitTimeBetweenCleanupRuns:          time.Minute,
        recordExpireIntervalInNS:            2 * time.Minute.Nanoseconds(),
    }
    return recorder
}
