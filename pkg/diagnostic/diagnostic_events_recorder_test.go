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
    "github.com/apache/incubator-yunikorn-core/pkg/common"
    "gotest.tools/assert"
    "testing"
    "time"
)

func TestDiagnosticEvents(t *testing.T) {
    recorder := newDiagEventsRecorder()
    timer := common.NewMockTimer()
    recorder.timer = timer

    // Flush should return 0
    assert.Equal(t, 0, len(recorder.FlushEvents()))

    // Now add event with same key twice, should have one single entry, and previous one kept.
    recorder.RecordResourceRequestEvent(Info, "req-1", "app-1", "queue-1", "node-1", "blah")
    recorder.RecordResourceRequestEvent(Info, "req-1", "app-1", "queue-1", "node-1", "blah_1")

    events := recorder.FlushEvents()
    assert.Equal(t, 1, len(events))
    assert.Equal(t, "blah", events[0].Message)
    assert.Equal(t, int64(0), events[0].NanoTimestamp)

    // After flush, there's no more events left
    assert.Equal(t, 0, len(recorder.FlushEvents()))

    // Now, set timer and add new events, timestamp should be the new one
    timer.SetNanoTimeNow(100 * time.Millisecond.Nanoseconds())
    recorder.RecordResourceRequestEvent(Info, "req-1", "app-1", "queue-1", "node-1", "blah")
    events = recorder.FlushEvents()
    assert.Equal(t, 100*time.Millisecond.Nanoseconds(), events[0].NanoTimestamp)

    // Now add 5 events, with 4 identical primary keys
    recorder.RecordResourceRequestEvent(Info, "req-1", "app-1", "queue-1", "node-1", "blah")
    recorder.RecordResourceRequestEvent(Info, "req-1", "app-1", "queue-1", "node-2", "blah_3")
    recorder.RecordResourceRequestEvent(Info, "req-2", "app-1", "queue-1", "node-1", "blah_1")
    recorder.RecordResourceRequestEvent(Info, "req-2", "app-1", "queue-1", "node-1", "blah_2")
    recorder.RecordResourceRequestEvent(Info, "req-3", "app-3", "queue-1", "node-1", "blah_2")

    events = recorder.FlushEvents()
    assert.Equal(t, 4, len(events))

    // Now test timeout feature, record two events in a row, second added after minimum wait time,
    // second one will be kept
    recorder.RecordResourceRequestEvent(Info, "req-1", "app-1", "queue-1", "node-1", "blah")
    timer.Sleep(time.Duration(2 * recorder.minimumRecordIntervalForSameKeyInNS))
    recorder.RecordResourceRequestEvent(Info, "req-1", "app-1", "queue-1", "node-1", "blah-1")
    events = recorder.FlushEvents()
    assert.Equal(t, 1, len(events))
    assert.Equal(t, "blah-1", events[0].Message)

    // Test missing fields
    recorder.RecordResourceRequestEvent(Info, "", "app-1", "queue-1", "node-1", "blah")
    recorder.RecordResourceRequestEvent(Info, "req-1", "", "queue-1", "node-2", "blah_3")
    recorder.RecordResourceRequestEvent(Info, "req-2", "app-1", "", "node-1", "blah_1")
    recorder.RecordResourceRequestEvent(Info, "req-2", "app-1", "queue-1", "", "blah_2")
    recorder.RecordResourceRequestEvent(Info, "", "", "", "", "blah_2")
    events = recorder.FlushEvents()
    assert.Equal(t, 5, len(events))

    // Test cleanup runs
    recorder.RecordResourceRequestEvent(Info, "req-1", "app-1", "queue-1", "node-1", "blah")
    recorder.RecordResourceRequestEvent(Info, "req-2", "app-1", "queue-1", "node-1", "blah")
    recorder.RecordResourceRequestEvent(Info, "req-3", "app-1", "queue-1", "node-1", "blah")
    timer.Sleep(time.Duration(2 * recorder.recordExpireIntervalInNS))
    recorder.RecordResourceRequestEvent(Info, "req-4", "app-1", "queue-1", "node-1", "blah")
    recorder.RecordResourceRequestEvent(Info, "req-5", "app-1", "queue-1", "node-1", "blah")
    recorder.RecordResourceRequestEvent(Info, "req-6", "app-1", "queue-1", "node-1", "blah")
    assert.Equal(t, 6, len(recorder.eventMap))

    recorder.cleanupOldEvents()
    events = recorder.FlushEvents()
    assert.Equal(t, 3, len(events))
    for _, e := range events {
        if e.RequestId == "req-1" || e.RequestId == "req-2" || e.RequestId == "req-3" {
            t.Errorf("Failed to cleanup old events")
        }
    }
}
