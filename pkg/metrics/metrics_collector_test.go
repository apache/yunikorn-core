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
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/metrics/history"
)

func TestHistoricalPartitionInfoUpdater(t *testing.T) {
	metricsHistory := history.NewInternalMetricsHistory(3)
	setInternalMetricsCollectorTicker(5 * time.Millisecond)
	metricsCollector := NewInternalMetricsCollector(metricsHistory)
	metricsCollector.StartService()

	go func() {
		metrics := GetSchedulerMetrics()
		i := 0
		for i < 3 {
			metrics.IncTotalApplicationsRunning()
			metrics.AddAllocatedContainers(2)
			i += 1
			time.Sleep(4 * time.Millisecond)
		}
	}()

	time.Sleep(11 * time.Millisecond)
	metricsCollector.Stop()

	records := metricsHistory.GetRecords()
	assert.Equal(t, 2, len(records), "Expected exactly 2 history records")
	for i, record := range records {
		if i == 0 {
			assert.Equal(t, 2, record.TotalApplications, "Expected exactly 2 applications at 10 msec")
			assert.Equal(t, 4, record.TotalContainers, "Expected exactly 4 allocations at 10 msec")
		} else if i == 1 {
			assert.Equal(t, 3, record.TotalApplications, "Expected exactly 3 applications at 20 msec")
			assert.Equal(t, 6, record.TotalContainers, "Expected exactly 4 allocations at 20 msec")
		}
	}
}
