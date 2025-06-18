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

var sm *SchedulerMetrics

func TestDrainingNodes(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.IncDrainingNodes()
	verifyMetric(t, 1, "draining", "yunikorn_scheduler_node", dto.MetricType_GAUGE, "state")

	sm.DecDrainingNodes()
	verifyMetric(t, 0, "draining", "yunikorn_scheduler_node", dto.MetricType_GAUGE, "state")
}

func TestTotalDecommissionedNodes(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.IncTotalDecommissionedNodes()
	verifyMetric(t, 1, "decommissioned", "yunikorn_scheduler_node", dto.MetricType_GAUGE, "state")
}

func TestTryPreemptionLatency(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.ObserveTryPreemptionLatency(time.Now().Add(-1 * time.Minute))
	verifyHistogram(t, "trypreemption_latency_milliseconds", 60, 1)
}

func TestSchedulerApplicationsNew(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.IncTotalApplicationsNew()
	verifyMetric(t, 1, "new", "yunikorn_scheduler_application_submission_total", dto.MetricType_COUNTER, "result")

	curr, err := sm.GetTotalApplicationsNew()
	assert.NilError(t, err)
	assert.Equal(t, curr, 1)
}

func TestSchedulerApplicationsAccepted(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.IncTotalApplicationsAccepted()
	verifyMetric(t, 1, "accepted", "yunikorn_scheduler_application_submission_total", dto.MetricType_COUNTER, "result")

	curr, err := sm.GetTotalApplicationsAccepted()
	assert.NilError(t, err)
	assert.Equal(t, curr, 1)
}

func TestSchedulerApplicationsRejected(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.IncTotalApplicationsRejected()
	verifyMetric(t, 1, "rejected", "yunikorn_scheduler_application_submission_total", dto.MetricType_COUNTER, "result")

	curr, err := sm.GetTotalApplicationsRejected()
	assert.NilError(t, err)
	assert.Equal(t, curr, 1)
}

func TestSchedulerApplicationsRunning(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.IncTotalApplicationsRunning()
	verifyMetric(t, 1, "running", "yunikorn_scheduler_application_total", dto.MetricType_GAUGE, "state")

	curr, err := sm.GetTotalApplicationsRunning()
	assert.NilError(t, err)
	assert.Equal(t, curr, 1)

	sm.DecTotalApplicationsRunning()
	verifyMetric(t, 0, "running", "yunikorn_scheduler_application_total", dto.MetricType_GAUGE, "state")
}

func TestSchedulerApplicationsCompleting(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.IncTotalApplicationsCompleting()
	verifyMetric(t, 1, "completing", "yunikorn_scheduler_application_total", dto.MetricType_GAUGE, "state")

	sm.DecTotalApplicationsCompleting()
	verifyMetric(t, 0, "completing", "yunikorn_scheduler_application_total", dto.MetricType_GAUGE, "state")
}

func TestSchedulerApplicationsResuming(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.IncTotalApplicationsResuming()
	verifyMetric(t, 1, "resuming", "yunikorn_scheduler_application_total", dto.MetricType_GAUGE, "state")

	curr, err := sm.GetTotalApplicationsResuming()
	assert.NilError(t, err)
	assert.Equal(t, curr, 1)

	sm.DecTotalApplicationsResuming()
	verifyMetric(t, 0, "resuming", "yunikorn_scheduler_application_total", dto.MetricType_GAUGE, "state")
}

func TestSchedulerApplicationsFailing(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.IncTotalApplicationsFailing()
	verifyMetric(t, 1, "failing", "yunikorn_scheduler_application_total", dto.MetricType_GAUGE, "state")

	sm.DecTotalApplicationsFailing()
	verifyMetric(t, 0, "failing", "yunikorn_scheduler_application_total", dto.MetricType_GAUGE, "state")
}

func TestSchedulerApplicationsCompleted(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.IncTotalApplicationsCompleted()
	verifyMetric(t, 1, "completed", "yunikorn_scheduler_application_total", dto.MetricType_GAUGE, "state")

	curr, err := sm.GetTotalApplicationsCompleted()
	assert.NilError(t, err)
	assert.Equal(t, curr, 1)
}

func TestSchedulerApplicationsFailed(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.IncTotalApplicationsFailed()
	verifyMetric(t, 1, "failed", "yunikorn_scheduler_application_total", dto.MetricType_GAUGE, "state")
}

func TestSchedulingCycle(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.ObserveSchedulingCycle(time.Now().Add(-1 * time.Minute))
	verifyHistogram(t, "scheduling_cycle_milliseconds", 60, 1)
}

func TestTryNodeEvaluation(t *testing.T) {
	sm = getSchedulerMetrics(t)
	defer unregisterMetrics()

	sm.ObserveTryNodeEvaluation(time.Now().Add(-1 * time.Minute))
	verifyHistogram(t, "trynode_evaluation_milliseconds", 60, 1)
}

func getSchedulerMetrics(t *testing.T) *SchedulerMetrics {
	unregisterMetrics()
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

func verifyMetric(t *testing.T, expectedCounter float64, expectedState string, name string, metricType dto.MetricType, labelName string) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	assert.NilError(t, err)

	var checked bool
	for _, metric := range mfs {
		if strings.Contains(metric.GetName(), name) {
			assert.Equal(t, 1, len(metric.Metric))
			assert.Equal(t, metricType, metric.GetType())
			m := metric.Metric[0]
			assert.Equal(t, 1, len(m.Label))
			assert.Equal(t, labelName, *m.Label[0].Name)
			assert.Equal(t, expectedState, *m.Label[0].Value)
			switch metricType {
			case dto.MetricType_GAUGE:
				assert.Equal(t, expectedCounter, *m.Gauge.Value)
			case dto.MetricType_COUNTER:
				assert.Equal(t, expectedCounter, *m.Counter.Value)
			default:
				assert.Assert(t, false, "unsupported")
			}
			checked = true
			break
		}
	}

	assert.Assert(t, checked, "Failed to find metric")
}

func unregisterMetrics() {
	sm := GetSchedulerMetrics()
	prometheus.Unregister(sm.containerAllocation)
	prometheus.Unregister(sm.applicationSubmission)
	prometheus.Unregister(sm.application)
	prometheus.Unregister(sm.node)
	prometheus.Unregister(sm.schedulingLatency)
	prometheus.Unregister(sm.schedulingCycle)
	prometheus.Unregister(sm.sortingLatency)
	prometheus.Unregister(sm.tryNodeLatency)
	prometheus.Unregister(sm.tryNodeEvaluation)
	prometheus.Unregister(sm.tryPreemptionLatency)
}
