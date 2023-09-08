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
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
)

// QueueMetrics to declare queue metrics
type QueueMetrics struct {
	appMetricsLabel          *prometheus.GaugeVec
	appMetricsSubsystem      *prometheus.GaugeVec
	containerMetrics *prometheus.CounterVec
	ResourceMetricsLabel     *prometheus.GaugeVec
	ResourceMetricsSubsystem *prometheus.GaugeVec
}

// InitQueueMetrics to initialize queue metrics
func InitQueueMetrics(name string) CoreQueueMetrics {
	q := &QueueMetrics{}

	replaceStr := formatMetricName(name)

	q.appMetricsLabel = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "queue_app",
			ConstLabels: prometheus.Labels{"queue": name},
			Help:      "Queue application metrics. State of the application includes `running`.",
		}, []string{"state"})

	q.appMetricsSubsystem = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: replaceStr,
			Name:      "queue_app",
			Help:      "Queue application metrics. State of the application includes `accepted`, `rejected`, `running`, `failed`, `completed`.",
		}, []string{"state"})

	q.containerMetrics = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: replaceStr,
			Name:      "queue_container",
			Help:      "Queue container metrics. State of the attempt includes `allocated`, `released`.",
		}, []string{"state"})

	q.ResourceMetricsLabel = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "queue_resource",
			ConstLabels: prometheus.Labels{"queue": name},
			Help:      "Queue resource metrics. State of the resource includes `guaranteed`, `max`, `allocated`, `pending`, `preempting`.",
		}, []string{"state", "resource"})

	q.ResourceMetricsSubsystem = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: replaceStr,
			Name:      "queue_resource",
			Help:      "Queue resource metrics. State of the resource includes `guaranteed`, `max`, `allocated`, `pending`, `preempting`.",
		}, []string{"state", "resource"})

	var queueMetricsList = []prometheus.Collector{
		q.appMetricsLabel,
		q.appMetricsSubsystem,
		q.containerMetrics,
		q.ResourceMetricsLabel,
		q.ResourceMetricsSubsystem,
	}

	// Register the metrics
	for _, metric := range queueMetricsList {
		// registration might be failed if queue name is not valid
		// metrics name must be complied with regex: [a-zA-Z_:][a-zA-Z0-9_:]*,
		// queue name regex: ^[a-zA-Z0-9_-]{1,64}$
		if err := prometheus.Register(metric); err != nil {
			log.Log(log.Metrics).Warn("failed to register metrics collector", zap.Error(err))
		}
	}

	return q
}

func (m *QueueMetrics) Reset() {
	m.appMetricsLabel.Reset()
	m.appMetricsSubsystem.Reset()
	m.ResourceMetricsLabel.Reset()
	m.ResourceMetricsSubsystem.Reset()
}

func (m *QueueMetrics) IncQueueApplicationsRunning() {
	m.appMetricsLabel.With(prometheus.Labels{"state": "running"}).Inc()
	m.appMetricsSubsystem.With(prometheus.Labels{"state": "running"}).Inc()
}

func (m *QueueMetrics) DecQueueApplicationsRunning() {
	m.appMetricsLabel.With(prometheus.Labels{"state": "running"}).Dec()
	m.appMetricsSubsystem.With(prometheus.Labels{"state": "running"}).Dec()

}

func (m *QueueMetrics) GetQueueApplicationsRunning() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.With(prometheus.Labels{"state": "running"}).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncQueueApplicationsAccepted() {
	m.appMetricsLabel.With(prometheus.Labels{"state": "accepted"}).Inc()
	m.appMetricsSubsystem.With(prometheus.Labels{"state": "accepted"}).Inc()
}

func (m *QueueMetrics) GetQueueApplicationsAccepted() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.With(prometheus.Labels{"state": "accepted"}).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncQueueApplicationsRejected() {
	m.appMetricsLabel.With(prometheus.Labels{"state": "rejected"}).Inc()
	m.appMetricsSubsystem.With(prometheus.Labels{"state": "rejected"}).Inc()
}

func (m *QueueMetrics) GetQueueApplicationsRejected() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.With(prometheus.Labels{"state": "rejected"}).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncQueueApplicationsFailed() {
	m.appMetricsLabel.With(prometheus.Labels{"state": "failed"}).Inc()
	m.appMetricsSubsystem.With(prometheus.Labels{"state": "failed"}).Inc()
}

func (m *QueueMetrics) GetQueueApplicationsFailed() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.With(prometheus.Labels{"state": "failed"}).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncQueueApplicationsCompleted() {
	m.appMetricsLabel.With(prometheus.Labels{"state": "completed"}).Inc()
	m.appMetricsSubsystem.With(prometheus.Labels{"state": "completed"}).Inc()
}

func (m *QueueMetrics) GetQueueApplicationsCompleted() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.With(prometheus.Labels{"state": "completed"}).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncAllocatedContainer() {
	m.containerMetrics.With(prometheus.Labels{"state": "allocated"}).Inc()
}

func (m *QueueMetrics) IncReleasedContainer() {
	m.containerMetrics.With(prometheus.Labels{"state": "released"}).Inc()
}

func (m *QueueMetrics) AddReleasedContainers(value int) {
	m.containerMetrics.With(prometheus.Labels{"state": "released"}).Add(float64(value))
}

func (m *QueueMetrics) SetQueueGuaranteedResourceMetrics(resourceName string, value float64) {
	m.ResourceMetricsLabel.With(prometheus.Labels{"state": "guaranteed", "resource": resourceName}).Set(value)
	m.ResourceMetricsSubsystem.With(prometheus.Labels{"state": "guaranteed", "resource": resourceName}).Set(value)
}

func (m *QueueMetrics) SetQueueMaxResourceMetrics(resourceName string, value float64) {
	m.ResourceMetricsLabel.With(prometheus.Labels{"state": "max", "resource": resourceName}).Set(value)
	m.ResourceMetricsSubsystem.With(prometheus.Labels{"state": "max", "resource": resourceName}).Set(value)
}

func (m *QueueMetrics) SetQueueAllocatedResourceMetrics(resourceName string, value float64) {
	m.ResourceMetricsLabel.With(prometheus.Labels{"state": "allocated", "resource": resourceName}).Set(value)
	m.ResourceMetricsSubsystem.With(prometheus.Labels{"state": "allocated", "resource": resourceName}).Set(value)
}

func (m *QueueMetrics) AddQueueAllocatedResourceMetrics(resourceName string, value float64) {
	m.ResourceMetricsLabel.With(prometheus.Labels{"state": "allocated", "resource": resourceName}).Add(value)
	m.ResourceMetricsSubsystem.With(prometheus.Labels{"state": "allocated", "resource": resourceName}).Add(value)
}

func (m *QueueMetrics) SetQueuePendingResourceMetrics(resourceName string, value float64) {
	m.ResourceMetricsLabel.With(prometheus.Labels{"state": "pending", "resource": resourceName}).Set(value)
	m.ResourceMetricsSubsystem.With(prometheus.Labels{"state": "pending", "resource": resourceName}).Set(value)
}

func (m *QueueMetrics) AddQueuePendingResourceMetrics(resourceName string, value float64) {
	m.ResourceMetricsLabel.With(prometheus.Labels{"state": "pending", "resource": resourceName}).Add(value)
	m.ResourceMetricsSubsystem.With(prometheus.Labels{"state": "pending", "resource": resourceName}).Add(value)
}

func (m *QueueMetrics) SetQueuePreemptingResourceMetrics(resourceName string, value float64) {
	m.ResourceMetricsLabel.With(prometheus.Labels{"state": "preempting", "resource": resourceName}).Set(value)
	m.ResourceMetricsSubsystem.With(prometheus.Labels{"state": "preempting", "resource": resourceName}).Set(value)
}

func (m *QueueMetrics) AddQueuePreemptingResourceMetrics(resourceName string, value float64) {
	m.ResourceMetricsLabel.With(prometheus.Labels{"state": "preempting", "resource": resourceName}).Add(value)
	m.ResourceMetricsSubsystem.With(prometheus.Labels{"state": "preempting", "resource": resourceName}).Add(value)
}
