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
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
)

// QueueMetrics to declare queue metrics
type QueueMetrics struct {
	appMetrics      *prometheus.GaugeVec
	ResourceMetrics *prometheus.GaugeVec
}

// InitQueueMetrics to initialize queue metrics
func InitQueueMetrics(name string) CoreQueueMetrics {
	q := &QueueMetrics{}

	replaceStr := formatMetricName(name)

	q.appMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: replaceStr,
			Name:      "queue_app",
			Help:      "Queue application metrics. State of the application includes `running`.",
		}, []string{"state"})

	q.ResourceMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: replaceStr,
			Name:      "queue_resource",
			Help:      "Queue resource metrics. State of the resource includes `guaranteed`, `max`, `allocated`, `pending`, `preempting`.",
		}, []string{"state", "resource"})

	var queueMetricsList = []prometheus.Collector{
		q.appMetrics,
		q.ResourceMetrics,
	}

	// Register the metrics
	for _, metric := range queueMetricsList {
		// registration might be failed if queue name is not valid
		// metrics name must be complied with regex: [a-zA-Z_:][a-zA-Z0-9_:]*,
		// queue name regex: ^[a-zA-Z0-9_-]{1,64}$
		if err := prometheus.Register(metric); err != nil {
			log.Logger().Warn("failed to register metrics collector", zap.Error(err))
		}
	}

	return q
}

func (m *QueueMetrics) Reset() {
	m.appMetrics.Reset()
	m.ResourceMetrics.Reset()
}

func (m *QueueMetrics) IncQueueApplicationsRunning() {
	m.appMetrics.With(prometheus.Labels{"state": "running"}).Inc()
}

func (m *QueueMetrics) DecQueueApplicationsRunning() {
	m.appMetrics.With(prometheus.Labels{"state": "running"}).Dec()
}

func (m *QueueMetrics) IncQueueApplicationsAccepted() {
	m.appMetrics.With(prometheus.Labels{"state": "accepted"}).Inc()
}

func (m *QueueMetrics) IncQueueApplicationsRejected() {
	m.appMetrics.With(prometheus.Labels{"state": "rejected"}).Inc()
}

func (m *QueueMetrics) IncQueueApplicationsFailed() {
	m.appMetrics.With(prometheus.Labels{"state": "failed"}).Inc()
}

func (m *QueueMetrics) IncQueueApplicationsCompleted() {
	m.appMetrics.With(prometheus.Labels{"state": "completed"}).Inc()
}

func (m *QueueMetrics) IncAllocatedContainer() {
	m.appMetrics.With(prometheus.Labels{"state": "allocated"}).Inc()
}

func (m *QueueMetrics) IncReleasedContainer() {
	m.appMetrics.With(prometheus.Labels{"state": "released"}).Inc()
}

func (m *QueueMetrics) SetQueueGuaranteedResourceMetrics(resourceName string, value float64) {
	m.ResourceMetrics.With(prometheus.Labels{"state": "guaranteed", "resource": resourceName}).Set(value)
}

func (m *QueueMetrics) SetQueueMaxResourceMetrics(resourceName string, value float64) {
	m.ResourceMetrics.With(prometheus.Labels{"state": "max", "resource": resourceName}).Set(value)
}

func (m *QueueMetrics) SetQueueAllocatedResourceMetrics(resourceName string, value float64) {
	m.ResourceMetrics.With(prometheus.Labels{"state": "allocated", "resource": resourceName}).Set(value)
}

func (m *QueueMetrics) AddQueueAllocatedResourceMetrics(resourceName string, value float64) {
	m.ResourceMetrics.With(prometheus.Labels{"state": "allocated", "resource": resourceName}).Add(value)
}

func (m *QueueMetrics) SetQueuePendingResourceMetrics(resourceName string, value float64) {
	m.ResourceMetrics.With(prometheus.Labels{"state": "pending", "resource": resourceName}).Set(value)
}

func (m *QueueMetrics) AddQueuePendingResourceMetrics(resourceName string, value float64) {
	m.ResourceMetrics.With(prometheus.Labels{"state": "pending", "resource": resourceName}).Add(value)
}

func (m *QueueMetrics) SetQueuePreemptingResourceMetrics(resourceName string, value float64) {
	m.ResourceMetrics.With(prometheus.Labels{"state": "preempting", "resource": resourceName}).Set(value)
}

func (m *QueueMetrics) AddQueuePreemptingResourceMetrics(resourceName string, value float64) {
	m.ResourceMetrics.With(prometheus.Labels{"state": "preempting", "resource": resourceName}).Add(value)
}
