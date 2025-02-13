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
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

var qm *QueueMetrics

func TestApplicationsNew(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.IncQueueApplicationsNew()
	verifyAppMetrics(t, "new")

	curr, err := qm.GetQueueApplicationsNew()
	assert.NilError(t, err)
	assert.Equal(t, 1, curr)

	qm.DecQueueApplicationsNew()
	curr, err = qm.GetQueueApplicationsNew()
	assert.NilError(t, err)
	assert.Equal(t, 0, curr)
}

func TestApplicationsRunning(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.IncQueueApplicationsRunning()
	verifyAppMetrics(t, "running")

	curr, err := qm.GetQueueApplicationsRunning()
	assert.NilError(t, err)
	assert.Equal(t, 1, curr)

	qm.DecQueueApplicationsRunning()
	curr, err = qm.GetQueueApplicationsRunning()
	assert.NilError(t, err)
	assert.Equal(t, 0, curr)
}

func TestApplicationsAccepted(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.IncQueueApplicationsAccepted()
	verifyAppMetrics(t, "accepted")

	curr, err := qm.GetQueueApplicationsAccepted()
	assert.NilError(t, err)
	assert.Equal(t, 1, curr)

	qm.DecQueueApplicationsAccepted()
	curr, err = qm.GetQueueApplicationsAccepted()
	assert.NilError(t, err)
	assert.Equal(t, 0, curr)
}

func TestApplicationsResuming(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.IncQueueApplicationsResuming()
	verifyAppMetrics(t, "resuming")

	curr, err := qm.GetQueueApplicationsResuming()
	assert.NilError(t, err)
	assert.Equal(t, 1, curr)

	qm.DecQueueApplicationsResuming()
	curr, err = qm.GetQueueApplicationsResuming()
	assert.NilError(t, err)
	assert.Equal(t, 0, curr)
}

func TestApplicationsFailing(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.IncQueueApplicationsFailing()
	verifyAppMetrics(t, "failing")

	curr, err := qm.GetQueueApplicationsFailing()
	assert.NilError(t, err)
	assert.Equal(t, 1, curr)

	qm.DecQueueApplicationsFailing()
	curr, err = qm.GetQueueApplicationsFailing()
	assert.NilError(t, err)
	assert.Equal(t, 0, curr)
}

func TestApplicationsRejected(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.IncQueueApplicationsRejected()
	verifyAppMetrics(t, "rejected")

	curr, err := qm.GetQueueApplicationsRejected()
	assert.NilError(t, err)
	assert.Equal(t, 1, curr)
}

func TestApplicationsFailed(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.IncQueueApplicationsFailed()
	verifyAppMetrics(t, "failed")

	curr, err := qm.GetQueueApplicationsFailed()
	assert.NilError(t, err)
	assert.Equal(t, 1, curr)
}

func TestApplicationsCompleting(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.IncQueueApplicationsCompleting()
	verifyAppMetrics(t, "completing")

	curr, err := qm.GetQueueApplicationsCompleting()
	assert.NilError(t, err)
	assert.Equal(t, 1, curr)

	qm.DecQueueApplicationsCompleting()
	curr, err = qm.GetQueueApplicationsCompleting()
	assert.NilError(t, err)
	assert.Equal(t, 0, curr)
}

func TestApplicationsCompleted(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.IncQueueApplicationsCompleted()
	verifyAppMetrics(t, "completed")

	curr, err := qm.GetQueueApplicationsCompleted()
	assert.NilError(t, err)
	assert.Equal(t, 1, curr)
}

func TestAllocatedContainers(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.IncAllocatedContainer()
	verifyContainerMetrics(t, "allocated", float64(1))
}

func TestReleasedContainers(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.IncReleasedContainer()
	verifyContainerMetrics(t, "released", float64(1))
}

func TestAddReleasedContainers(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.AddReleasedContainers(2)
	verifyContainerMetrics(t, "released", float64(2))
}

func TestQueueGuaranteedResourceMetrics(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.UpdateQueueResourceMetrics("guaranteed", map[string]resources.Quantity{
		"cpu": 1,
	})
	verifyResourceMetrics(t, "guaranteed", "cpu")
	assert.DeepEqual(t, qm.knownResourceTypes, map[string]struct{}{"cpu": {}})

	qm.UpdateQueueResourceMetrics("guaranteed", map[string]resources.Quantity{"memory": 1})
	assert.DeepEqual(t, qm.knownResourceTypes, map[string]struct{}{"cpu": {}, "memory": {}})
}

func TestQueueMaxResourceMetrics(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.UpdateQueueResourceMetrics("max", map[string]resources.Quantity{
		"cpu": 1,
	})
	verifyResourceMetrics(t, "max", "cpu")
	assert.DeepEqual(t, qm.knownResourceTypes, map[string]struct{}{"cpu": {}})

	qm.UpdateQueueResourceMetrics("max", map[string]resources.Quantity{"memory": 1})
	assert.DeepEqual(t, qm.knownResourceTypes, map[string]struct{}{"cpu": {}, "memory": {}})
}

func TestQueueAllocatedResourceMetrics(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.SetQueueAllocatedResourceMetrics("cpu", 1)
	verifyResourceMetrics(t, "allocated", "cpu")
}

func TestQueuePendingResourceMetrics(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.SetQueuePendingResourceMetrics("cpu", 1)
	verifyResourceMetrics(t, "pending", "cpu")
}

func TestQueuePreemptingResourceMetrics(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.SetQueuePreemptingResourceMetrics("cpu", 1)
	verifyResourceMetrics(t, "preempting", "cpu")
}

func TestQueueMaxRunningAppsResourceMetrics(t *testing.T) {
	qm = getQueueMetrics()
	defer unregisterQueueMetrics()

	qm.SetQueueMaxRunningAppsMetrics(1)
	verifyResourceMetrics(t, "maxRunningApps", "apps")
}

func TestRemoveQueueMetrics(t *testing.T) {
	testQueueName := "root.test"
	qm = GetQueueMetrics(testQueueName)
	defer unregisterQueueMetrics()

	qm.SetQueueAllocatedResourceMetrics("cpu", 1)
	assert.Assert(t, m.queues[testQueueName] != nil)
	RemoveQueueMetrics(testQueueName)
	assert.Assert(t, m.queues[testQueueName] == nil)
	metrics, err := prometheus.DefaultGatherer.Gather()
	assert.NilError(t, err)
	for _, metric := range metrics {
		assert.Assert(t, metric.GetName() != "yunikorn_queue_resource",
			"Queue metrics are not cleaned up")
	}
}

func getQueueMetrics() *QueueMetrics {
	return InitQueueMetrics("root.test")
}

func verifyAppMetrics(t *testing.T, expectedState string) {
	verifyAppMetricsLabel(t, expectedState)
	verifyAppMetricsSubsystem(t, expectedState)
}

func verifyAppMetricsLabel(t *testing.T, expectedState string) {
	checkFn := func(labels []*dto.LabelPair) {
		assert.Equal(t, 2, len(labels))
		assert.Equal(t, "queue", *labels[0].Name)
		assert.Equal(t, "root.test", *labels[0].Value)
		assert.Equal(t, "state", *labels[1].Name)
		assert.Equal(t, expectedState, *labels[1].Value)
	}

	verifyMetricsLabel(t, checkFn)
}

func verifyAppMetricsSubsystem(t *testing.T, expectedState string) {
	checkFn := func(labels []*dto.LabelPair) {
		assert.Equal(t, 1, len(labels))
		assert.Equal(t, "state", *labels[0].Name)
		assert.Equal(t, expectedState, *labels[0].Value)
	}

	verifyMetricsSubsytem(t, checkFn)
}

func verifyContainerMetrics(t *testing.T, expectedState string, value float64) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	assert.NilError(t, err)

	var checked bool
	for _, metric := range mfs {
		if strings.Contains(metric.GetName(), "yunikorn_root_test") {
			assert.Equal(t, 1, len(metric.Metric))
			assert.Equal(t, dto.MetricType_COUNTER, metric.GetType())
			m := metric.Metric[0]
			assert.Equal(t, 1, len(m.Label))
			assert.Equal(t, "state", *m.Label[0].Name)
			assert.Equal(t, expectedState, *m.Label[0].Value)
			assert.Assert(t, m.Counter != nil)
			assert.Equal(t, value, *m.Counter.Value)
			checked = true
			break
		}
	}

	assert.Assert(t, checked, "Failed to find metric")
}
func verifyResourceMetrics(t *testing.T, expectedState, expectedResource string) {
	verifyResourceMetricsLabel(t, expectedState, expectedResource)
	verifyResourceMetricsSubsystem(t, expectedState, expectedResource)
}

func verifyResourceMetricsLabel(t *testing.T, expectedState, expectedResource string) {
	checkFn := func(labels []*dto.LabelPair) {
		assert.Equal(t, 3, len(labels))
		assert.Equal(t, "queue", *labels[0].Name)
		assert.Equal(t, "root.test", *labels[0].Value)
		assert.Equal(t, "resource", *labels[1].Name)
		assert.Equal(t, expectedResource, *labels[1].Value)
		assert.Equal(t, "state", *labels[2].Name)
		assert.Equal(t, expectedState, *labels[2].Value)
	}

	verifyMetricsLabel(t, checkFn)
}

func verifyResourceMetricsSubsystem(t *testing.T, expectedState, expectedResource string) {
	checkFn := func(labels []*dto.LabelPair) {
		assert.Equal(t, 2, len(labels))
		assert.Equal(t, "resource", *labels[0].Name)
		assert.Equal(t, expectedResource, *labels[0].Value)
		assert.Equal(t, "state", *labels[1].Name)
		assert.Equal(t, expectedState, *labels[1].Value)
	}

	verifyMetricsSubsytem(t, checkFn)
}

func verifyMetricsLabel(t *testing.T, checkLabel func(label []*dto.LabelPair)) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	assert.NilError(t, err)

	var checked bool
	for _, metric := range mfs {
		if strings.Contains(metric.GetName(), "yunikorn_queue") {
			assert.Equal(t, 1, len(metric.Metric))
			assert.Equal(t, dto.MetricType_GAUGE, metric.GetType())
			m := metric.Metric[0]
			checkLabel(m.Label)
			assert.Assert(t, m.Gauge != nil)
			assert.Equal(t, float64(1), *m.Gauge.Value)
			checked = true
			break
		}
	}

	assert.Assert(t, checked, "Failed to find metric")
}

func verifyMetricsSubsytem(t *testing.T, checkLabel func(label []*dto.LabelPair)) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	assert.NilError(t, err)

	var checked bool
	for _, metric := range mfs {
		if strings.Contains(metric.GetName(), "yunikorn_root_test") {
			assert.Equal(t, 1, len(metric.Metric))
			assert.Equal(t, dto.MetricType_GAUGE, metric.GetType())
			m := metric.Metric[0]
			checkLabel(m.Label)
			assert.Assert(t, m.Gauge != nil)
			assert.Equal(t, float64(1), *m.Gauge.Value)
			checked = true
			break
		}
	}

	assert.Assert(t, checked, "Failed to find metric")
}

func unregisterQueueMetrics() {
	prometheus.Unregister(qm.appMetricsLabel)
	prometheus.Unregister(qm.appMetricsSubsystem)
	prometheus.Unregister(qm.containerMetrics)
	prometheus.Unregister(qm.resourceMetricsLabel)
	prometheus.Unregister(qm.resourceMetricsSubsystem)
	qm.knownResourceTypes = make(map[string]struct{})
}
