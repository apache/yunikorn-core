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
)

var cqm CoreQueueMetrics

func TestApplicationsRunning(t *testing.T) {
	cqm = getQueueMetrics()
	defer unregisterQueueMetrics(t)

	cqm.IncQueueApplicationsRunning()
	verifyAppMetrics(t, "running")
}

func TestApplicationsAccepted(t *testing.T) {
	cqm = getQueueMetrics()
	defer unregisterQueueMetrics(t)

	cqm.IncQueueApplicationsAccepted()
	verifyAppMetrics(t, "accepted")
}

func TestApplicationsRejected(t *testing.T) {
	cqm = getQueueMetrics()
	defer unregisterQueueMetrics(t)

	cqm.IncQueueApplicationsRejected()
	verifyAppMetrics(t, "rejected")
}

func TestApplicationsFailed(t *testing.T) {
	cqm = getQueueMetrics()
	defer unregisterQueueMetrics(t)

	cqm.IncQueueApplicationsFailed()
	verifyAppMetrics(t, "failed")
}

func TestApplicationsCompleted(t *testing.T) {
	cqm = getQueueMetrics()
	defer unregisterQueueMetrics(t)

	cqm.IncQueueApplicationsCompleted()
	verifyAppMetrics(t, "completed")
}

func TestAllocatedContainers(t *testing.T) {
	cqm = getQueueMetrics()
	defer unregisterQueueMetrics(t)

	cqm.IncAllocatedContainer()
	verifyAppMetrics(t, "allocated")
}

func TestReleasedContainers(t *testing.T) {
	cqm = getQueueMetrics()
	defer unregisterQueueMetrics(t)

	cqm.IncReleasedContainer()
	verifyAppMetrics(t, "released")
}

func TestQueueGuaranteedResourceMetrics(t *testing.T) {
	cqm = getQueueMetrics()
	defer unregisterQueueMetrics(t)

	cqm.SetQueueGuaranteedResourceMetrics("cpu", 1)
	verifyResourceMetrics(t, "guaranteed", "cpu")
}

func TestQueueMaxResourceMetrics(t *testing.T) {
	cqm = getQueueMetrics()
	defer unregisterQueueMetrics(t)

	cqm.SetQueueMaxResourceMetrics("cpu", 1)
	verifyResourceMetrics(t, "max", "cpu")
}

func TestQueueAllocatedResourceMetrics(t *testing.T) {
	cqm = getQueueMetrics()
	defer unregisterQueueMetrics(t)

	cqm.SetQueueAllocatedResourceMetrics("cpu", 1)
	verifyResourceMetrics(t, "allocated", "cpu")
}

func TestQueuePendingResourceMetrics(t *testing.T) {
	cqm = getQueueMetrics()
	defer unregisterQueueMetrics(t)

	cqm.SetQueuePendingResourceMetrics("cpu", 1)
	verifyResourceMetrics(t, "pending", "cpu")
}

func TestQueuePreemptingResourceMetrics(t *testing.T) {
	cqm = getQueueMetrics()
	defer unregisterMetrics(t)

	cqm.SetQueuePreemptingResourceMetrics("cpu", 1)
	verifyResourceMetrics(t, "preempting", "cpu")
}

func getQueueMetrics() CoreQueueMetrics {
	return InitQueueMetrics("root.test")
}

func verifyAppMetrics(t *testing.T, expectedState string) {
	checkFn := func(labels []*dto.LabelPair) {
		assert.Equal(t, 1, len(labels))
		assert.Equal(t, "state", *labels[0].Name)
		assert.Equal(t, expectedState, *labels[0].Value)
	}

	verifyMetrics(t, checkFn)
}

func verifyResourceMetrics(t *testing.T, expectedState, expectedResource string) {
	checkFn := func(labels []*dto.LabelPair) {
		assert.Equal(t, 2, len(labels))
		assert.Equal(t, "resource", *labels[0].Name)
		assert.Equal(t, expectedResource, *labels[0].Value)
		assert.Equal(t, "state", *labels[1].Name)
		assert.Equal(t, expectedState, *labels[1].Value)
	}

	verifyMetrics(t, checkFn)
}

func verifyMetrics(t *testing.T, checkLabel func(label []*dto.LabelPair)) {
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
func unregisterQueueMetrics(t *testing.T) {
	qm, ok := cqm.(*QueueMetrics)
	if !ok {
		t.Fatalf("Type assertion failed, metrics is not QueueMetrics")
	}

	prometheus.Unregister(qm.appMetrics)
	prometheus.Unregister(qm.ResourceMetrics)
}
