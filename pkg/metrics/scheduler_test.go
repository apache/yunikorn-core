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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"gotest.tools/v3/assert"
)

var csm CoreSchedulerMetrics

func TestDrainingNodes(t *testing.T) {
	csm = getSchedulerMetrics(t)
	defer unregisterMetrics(t)

	csm.IncDrainingNodes()
	verifyMetric(t, 1, "draining")

	csm.DecDrainingNodes()
	verifyMetric(t, 0, "draining")
}

func TestTotalDecommissionedNodes(t *testing.T) {
	csm = getSchedulerMetrics(t)
	defer unregisterMetrics(t)

	csm.IncTotalDecommissionedNodes()
	verifyMetric(t, 1, "decommissioned")
}

func TestUnhealthyNodes(t *testing.T) {
	csm = getSchedulerMetrics(t)
	defer unregisterMetrics(t)

	csm.IncUnhealthyNodes()
	verifyMetric(t, 1, "unhealthy")

	csm.DecUnhealthyNodes()
	verifyMetric(t, 0, "unhealthy")
}

func TestTryPreemptionLatency(t *testing.T) {
	csm = getSchedulerMetrics(t)
	defer unregisterMetrics(t)

	csm.ObserveTryPreemptionLatency(time.Now().Add(-1 * time.Minute))
	verifyHistogram(t, "trypreemption_latency_milliseconds", 60, 1)
}

func getSchedulerMetrics(t *testing.T) *SchedulerMetrics {
	unregisterMetrics(t)
	return InitSchedulerMetrics()
}

func verifyHistogram(t *testing.T, name string, value float64, delta float64) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	assert.NilError(t, err)
	for _, metric := range mfs {
		if strings.Contains(metric.GetName(), name) {
			assert.Equal(t, 1, len(metric.Metric))
			assert.Equal(t, dto.MetricType_HISTOGRAM, metric.GetType())
			m := metric.Metric[0]
			realDelta := math.Abs(*m.Histogram.SampleSum - value)
			assert.Check(t, realDelta < delta, fmt.Sprintf("wrong delta, expected <= %f, was %f", delta, realDelta))
		}
	}
}

func verifyMetric(t *testing.T, expectedCounter float64, expectedState string) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	assert.NilError(t, err)

	var checked bool
	for _, metric := range mfs {
		if strings.Contains(metric.GetName(), "yunikorn_scheduler_node") {
			assert.Equal(t, 1, len(metric.Metric))
			assert.Equal(t, dto.MetricType_GAUGE, metric.GetType())
			m := metric.Metric[0]
			assert.Equal(t, 1, len(m.Label))
			assert.Equal(t, "state", *m.Label[0].Name)
			assert.Equal(t, expectedState, *m.Label[0].Value)
			assert.Assert(t, m.Gauge != nil)
			assert.Equal(t, expectedCounter, *m.Gauge.Value)
			checked = true
			break
		}
	}

	assert.Assert(t, checked, "Failed to find metric")
}

func unregisterMetrics(t *testing.T) {
	sm, ok := GetSchedulerMetrics().(*SchedulerMetrics)
	if !ok {
		t.Fatalf("Type assertion failed, metrics is not SchedulerMetrics")
	}

	prometheus.Unregister(sm.containerAllocation)
	prometheus.Unregister(sm.applicationSubmission)
	prometheus.Unregister(sm.application)
	prometheus.Unregister(sm.node)
	prometheus.Unregister(sm.schedulingLatency)
	prometheus.Unregister(sm.sortingLatency)
	prometheus.Unregister(sm.tryNodeLatency)
	prometheus.Unregister(sm.tryPreemptionLatency)
}
