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
	"runtime/metrics"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"gotest.tools/v3/assert"
)

var o sync.Once
var collect = func() {
	GetRuntimeMetrics().Collect()
}

func TestMemStatsMetrics(t *testing.T) {
	o.Do(collect)

	mfs, err := prometheus.DefaultGatherer.Gather()
	assert.NilError(t, err)

	var memStatsChecked bool
	var pauseChecked bool
	var pauseNsChecked bool
	var bySizeSizeChecked bool
	var bySizeFreeVerified bool
	var bySizeMallocVerified bool

	for _, metric := range mfs {
		metricName := metric.GetName()

		if strings.Contains(metricName, "yunikorn_runtime_go_mem_stats") {
			validateMetrics(t, metric, 26, "MemStats")
			memStatsChecked = true
		}

		if strings.Contains(metricName, "yunikorn_runtime_go_alloc_bysize_maxsize") {
			validateBySize(t, metric)
			bySizeSizeChecked = true
		}

		if strings.Contains(metricName, "yunikorn_runtime_go_alloc_bysize_free") {
			validateBySize(t, metric)
			bySizeFreeVerified = true
		}

		if strings.Contains(metricName, "yunikorn_runtime_go_alloc_bysize_malloc") {
			validateBySize(t, metric)
			bySizeMallocVerified = true
		}

		if strings.Contains(metricName, "yunikorn_runtime_go_pause_ns") {
			validateMetrics(t, metric, 256, "PauseNs")
			pauseNsChecked = true
		}

		if strings.Contains(metricName, "yunikorn_runtime_go_pause_end") {
			validateMetrics(t, metric, 256, "PauseEnd")
			pauseChecked = true
		}
	}

	assert.Assert(t, memStatsChecked)
	assert.Assert(t, bySizeSizeChecked)
	assert.Assert(t, bySizeFreeVerified)
	assert.Assert(t, bySizeMallocVerified)
	assert.Assert(t, pauseChecked)
	assert.Assert(t, pauseNsChecked)
}

func TestGenericMetrics(t *testing.T) {
	o.Do(collect)
	var expectedNoOfMetrics int

	for _, m := range metrics.All() {
		if m.Kind == metrics.KindUint64 || m.Kind == metrics.KindFloat64 {
			expectedNoOfMetrics++
		}
	}

	mfs, err := prometheus.DefaultGatherer.Gather()
	assert.NilError(t, err)

	var metricsChecked bool
	for _, metric := range mfs {
		if strings.Contains(metric.GetName(), "yunikorn_runtime_go_generic") {
			validateMetrics(t, metric, expectedNoOfMetrics, "Generic")
			metricsChecked = true
		}
	}

	assert.Assert(t, metricsChecked)
}

func validateMetrics(t *testing.T, metric *dto.MetricFamily, expectedLabelCount int, expectedLabel string) {
	var labelCount int
	assert.Equal(t, dto.MetricType_GAUGE, metric.GetType())
	for _, m := range metric.Metric {
		assert.Equal(t, 1, len(m.Label))
		assert.Equal(t, expectedLabel, *m.Label[0].Name)
		labelCount++
	}
	assert.Equal(t, expectedLabelCount, labelCount)
}

func validateBySize(t *testing.T, metric *dto.MetricFamily) {
	assert.Equal(t, dto.MetricType_HISTOGRAM, metric.GetType())
	assert.Equal(t, 1, len(metric.Metric))
	assert.Equal(t, 68, len(metric.Metric[0].Histogram.Bucket))
}
