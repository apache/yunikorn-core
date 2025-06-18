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

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/log"
)

const (
	AppNew        = "new"
	AppAccepted   = "accepted"
	AppRunning    = "running"
	AppFailing    = "failing"
	AppFailed     = "failed"
	AppRejected   = "rejected"
	AppResuming   = "resuming"
	AppCompleting = "completing"
	AppCompleted  = "completed"
	AppExpired    = "expired"

	ContainerReleased  = "released"
	ContainerAllocated = "allocated"
	ContainerRejected  = "rejected"

	QueueGuaranteed     = "guaranteed"
	QueueMax            = "max"
	QueuePending        = "pending"
	QueuePreempting     = "preempting"
	QueueMaxRunningApps = "maxRunningApps"
)

// QueueMetrics to declare queue metrics
type QueueMetrics struct {
	appMetrics           *prometheus.GaugeVec
	containerMetrics     *prometheus.CounterVec
	resourceMetricsLabel *prometheus.GaugeVec
	// Track known resource types
	knownResourceTypes map[string]struct{}
	lock               locking.Mutex
}

// InitQueueMetrics to initialize queue metrics
func InitQueueMetrics(name string) *QueueMetrics {
	q := &QueueMetrics{}

	replaceStr := formatMetricName(name)

	q.appMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   Namespace,
			Name:        "queue_app",
			ConstLabels: prometheus.Labels{"queue": name},
			Help:        "Queue application metrics. State of the application includes `new`, `accepted`, `rejected`, `running`, `failing`, `failed`, `resuming`, `completing`, `completed`.",
		}, []string{"state"})

	q.containerMetrics = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: replaceStr,
			Name:      "queue_container",
			Help:      "Queue container metrics. State of the attempt includes `allocated`, `released`.",
		}, []string{"state"})

	q.resourceMetricsLabel = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   Namespace,
			Name:        "queue_resource",
			ConstLabels: prometheus.Labels{"queue": name},
			Help:        "Queue resource metrics. State of the resource includes `guaranteed`, `max`, `allocated`, `pending`, `preempting`, `maxRunningApps`.",
		}, []string{"state", "resource"})

	var queueMetricsList = []prometheus.Collector{
		q.appMetrics,
		q.containerMetrics,
		q.resourceMetricsLabel,
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

	q.knownResourceTypes = make(map[string]struct{})
	return q
}

func (m *QueueMetrics) UnregisterMetrics() {
	var queueMetricsList = []prometheus.Collector{
		m.appMetrics,
		m.containerMetrics,
		m.resourceMetricsLabel,
	}

	// Unregister the metrics
	for _, metric := range queueMetricsList {
		prometheus.Unregister(metric)
	}
}

func (m *QueueMetrics) incQueueApplications(state string) {
	m.appMetrics.WithLabelValues(state).Inc()
}

func (m *QueueMetrics) decQueueApplications(state string) {
	m.appMetrics.WithLabelValues(state).Dec()
}

func (m *QueueMetrics) setQueueResource(state string, resourceName string, value float64) {
	m.resourceMetricsLabel.WithLabelValues(state, resourceName).Set(value)
}

func (m *QueueMetrics) Reset() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.appMetrics.Reset()
	m.resourceMetricsLabel.Reset()
	m.knownResourceTypes = make(map[string]struct{})
}

func (m *QueueMetrics) IncQueueApplicationsRunning() {
	m.incQueueApplications(AppRunning)
}

func (m *QueueMetrics) DecQueueApplicationsRunning() {
	m.decQueueApplications(AppRunning)
}

func (m *QueueMetrics) GetQueueApplicationsRunning() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.WithLabelValues(AppRunning).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncQueueApplicationsNew() {
	m.incQueueApplications(AppNew)
}

func (m *QueueMetrics) DecQueueApplicationsNew() {
	m.decQueueApplications(AppNew)
}

func (m *QueueMetrics) GetQueueApplicationsNew() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.WithLabelValues(AppNew).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncQueueApplicationsAccepted() {
	m.incQueueApplications(AppAccepted)
}

func (m *QueueMetrics) DecQueueApplicationsAccepted() {
	m.decQueueApplications(AppAccepted)
}

func (m *QueueMetrics) GetQueueApplicationsAccepted() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.WithLabelValues(AppAccepted).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncQueueApplicationsRejected() {
	m.incQueueApplications(AppRejected)
}

func (m *QueueMetrics) GetQueueApplicationsRejected() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.WithLabelValues(AppRejected).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncQueueApplicationsResuming() {
	m.incQueueApplications(AppResuming)
}

func (m *QueueMetrics) DecQueueApplicationsResuming() {
	m.decQueueApplications(AppResuming)
}

func (m *QueueMetrics) GetQueueApplicationsResuming() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.WithLabelValues(AppResuming).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncQueueApplicationsFailing() {
	m.incQueueApplications(AppFailing)
}

func (m *QueueMetrics) DecQueueApplicationsFailing() {
	m.decQueueApplications(AppFailing)
}

func (m *QueueMetrics) GetQueueApplicationsFailing() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.WithLabelValues(AppFailing).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncQueueApplicationsFailed() {
	m.incQueueApplications(AppFailed)
}

func (m *QueueMetrics) GetQueueApplicationsFailed() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.WithLabelValues(AppFailed).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncQueueApplicationsCompleting() {
	m.incQueueApplications(AppCompleting)
}

func (m *QueueMetrics) DecQueueApplicationsCompleting() {
	m.decQueueApplications(AppCompleting)
}

func (m *QueueMetrics) GetQueueApplicationsCompleting() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.WithLabelValues(AppCompleting).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncQueueApplicationsCompleted() {
	m.incQueueApplications(AppCompleted)
}

func (m *QueueMetrics) GetQueueApplicationsCompleted() (int, error) {
	metricDto := &dto.Metric{}
	err := m.appMetrics.WithLabelValues(AppCompleted).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *QueueMetrics) IncAllocatedContainer() {
	m.containerMetrics.WithLabelValues(ContainerAllocated).Inc()
}

func (m *QueueMetrics) IncReleasedContainer() {
	m.containerMetrics.WithLabelValues(ContainerReleased).Inc()
}

func (m *QueueMetrics) AddReleasedContainers(value int) {
	m.containerMetrics.WithLabelValues(ContainerReleased).Add(float64(value))
}

func (m *QueueMetrics) UpdateQueueResourceMetrics(state string, newResources map[string]resources.Quantity) {
	m.lock.Lock()
	defer m.lock.Unlock()
	// Iterate over new resource types and set their values
	for resourceName, value := range newResources {
		m.setQueueResource(state, resourceName, float64(value))
		// Add new resources to the known list
		m.knownResourceTypes[resourceName] = struct{}{}
	}

	// Emit old resource types that are missing in the new collection with zero
	for resourceName := range m.knownResourceTypes {
		if _, exists := newResources[resourceName]; !exists {
			m.setQueueResource(state, resourceName, float64(0))
		}
	}
}

func (m *QueueMetrics) SetQueueAllocatedResourceMetrics(resourceName string, value float64) {
	m.setQueueResource(ContainerAllocated, resourceName, value)
}

func (m *QueueMetrics) SetQueuePendingResourceMetrics(resourceName string, value float64) {
	m.setQueueResource(QueuePending, resourceName, value)
}

func (m *QueueMetrics) SetQueuePreemptingResourceMetrics(resourceName string, value float64) {
	m.setQueueResource(QueuePreempting, resourceName, value)
}

func (m *QueueMetrics) SetQueueMaxRunningAppsMetrics(value uint64) {
	m.setQueueResource(QueueMaxRunningApps, "apps", float64(value))
}
