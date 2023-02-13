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

package metrics

import (
	"fmt"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/metrics/history"
)

func TestStop(t *testing.T) {
	metricsHistory := history.NewInternalMetricsHistory(3)
	metricsCollector := newInternalMetricsCollector(metricsHistory, 1*time.Second)
	metricsCollector.StartService()

	metricsCollector.Stop()
	// wait for the thread to store record. it should not happen
	time.Sleep(1500 * time.Millisecond)

	records := metricsHistory.GetRecords()
	assert.Equal(t, 3, len(records), "Expected exactly 3 history records")
	for _, record := range records {
		assert.Assert(t, record == nil, "The 1st item should be nil!")
	}
}

func TestStartService(t *testing.T) {
	metricsHistory := history.NewInternalMetricsHistory(3)
	metricsCollector := newInternalMetricsCollector(metricsHistory, 1*time.Second)
	metricsCollector.StartService()

	// wait for the thread to store record
	time.Sleep(1500 * time.Millisecond)
	metricsCollector.Stop()

	records := metricsHistory.GetRecords()
	assert.Equal(t, 3, len(records), "Expected exactly 3 history records")
	for i, record := range records {
		if i == 2 {
			assert.Assert(t, record != nil, fmt.Sprintf("record should not be nil, index: %d", i))
		} else {
			assert.Assert(t, record == nil, fmt.Sprintf("record should be nil, index: %d", i))
		}
	}
}

func TestHistoricalPartitionInfoUpdater(t *testing.T) {
	metricsHistory := history.NewInternalMetricsHistory(3)
	metricsCollector := NewInternalMetricsCollector(metricsHistory)

	metrics := GetSchedulerMetrics()

	// skip to store record for first application
	metrics.IncTotalApplicationsRunning()
	metrics.AddAllocatedContainers(2)

	metrics.IncTotalApplicationsRunning()
	metrics.AddAllocatedContainers(2)
	metricsCollector.store()

	metrics.IncTotalApplicationsRunning()
	metrics.AddAllocatedContainers(2)
	metricsCollector.store()

	records := metricsHistory.GetRecords()
	assert.Equal(t, 3, len(records), "Expected exactly 3 history records")
	for i, record := range records {
		switch i {
		case 0:
			assert.Assert(t, record == nil, fmt.Sprintf("record should be nil, index: %d", i))
		case 1:
			assert.Equal(t, 2, record.TotalApplications, "Expected exactly 2 applications")
			assert.Equal(t, 4, record.TotalContainers, "Expected exactly 4 allocations")
		case 2:
			assert.Equal(t, 3, record.TotalApplications, "Expected exactly 3 applications")
			assert.Equal(t, 6, record.TotalContainers, "Expected exactly 6 allocations")
		}
	}
}
