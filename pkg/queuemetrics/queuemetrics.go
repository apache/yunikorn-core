/*
Copyright 2019 The Unity Scheduler Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queuemetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

const (
	// 	QueuesSubsystem = "queues_metrics" - subsystem name used by queues
	QueuesSubsystem = "queues_metrics"
)

type CoreQueueMetrics interface {
	// Metrics ops related to ApplicationsAdded
	IncApplicationsAdded()
	AddApplicationsAdded(value int)

	// Metrics ops related to ApplicationsAdded
	IncApplicationsRejected()
	AddApplicationsRejected(value int)

	// Metrics Ops related to ApplicationsRunning
	IncApplicationsRunning()
	AddApplicationsRunning(value int)
	DecApplicationsRunning()
	SubApplicationsRunning(value int)
	SetApplicationsRunning(value int)

	// Metrics Ops related to ApplicationsCompleted
	IncApplicationsCompleted()
	AddApplicationsCompleted(value int)
	DecApplicationsCompleted()
	SubApplicationsCompleted(value int)
	SetApplicationsCompleted(value int)

	// Metrics Ops related to QueuePendingResourceMetrics
	IncQueuePendingResourceMetrics()
	AddQueuePendingResourceMetrics(value float64)
	DecQueuePendingResourceMetrics()
	SubQueuePendingResourceMetrics(value float64)
	SetQueuePendingResourceMetrics(value float64)

	// Metrics Ops related to queueUsedResourceMetrics
	IncQueueUsedResourceMetrics()
	AddQueueUsedResourceMetrics(value float64)
	DecQueueUsedResourceMetrics()
	SubQueueUsedResourceMetrics(value float64)
	SetQueueUsedResourceMetrics(value float64)

	// Metrics Ops related to queueAvailableResourceMetrics
	IncQueueAvailableResourceMetrics()
	AddQueueAvailableResourceMetrics(value float64)
	DecQueueAvailableResourceMetrics()
	SubQueueAvailableResourceMetrics(value float64)
	SetQueueAvailableResourceMetrics(value float64)
}


type QueueMetrics struct  {
	Name string
	queueAppMetrics  *prometheus.CounterVec
	applicationsAdded prometheus.Counter
	applicationsRejected prometheus.Counter
	applicationsRunning prometheus.Gauge
	applicationsCompleted prometheus.Gauge
	queuePendingResourceMetrics prometheus.Gauge
	queueUsedResourceMetrics prometheus.Gauge
	queueAvailableResourceMetrics prometheus.Gauge
}

var queueRegisterMetrics sync.Once

func InitQueueMetrics(name string) *QueueMetrics {
	q := &QueueMetrics{Name:name}

	// Queue Metrics
	q.queueAppMetrics = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: QueuesSubsystem,
			Name:      "queue_metrics_for_apps",
			Help:      "Application Metrics related to queues etc.",
		}, []string{"result"})

	// ApplicationsAdded counts how many apps are added to YuniKorn.
	q.applicationsAdded = q.queueAppMetrics.With(prometheus.Labels{"result": "added"})
	// ApplicationsRejected counts how many apps are rejected in YuniKorn.
	q.applicationsRejected = q.queueAppMetrics.With(prometheus.Labels{"result": "rejected"})
	// ApplicationsRunning counts how many apps are running active.
	q.applicationsRunning = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: QueuesSubsystem,
			Name:      "queue_running_apps",
			Help:      "active apps",
		})
	// ApplicationsCompleted counts how many apps are completed.
	q.applicationsCompleted = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: QueuesSubsystem,
			Name:      "queue_completed_apps",
			Help:      "completed apps",
		})

	q.queuePendingResourceMetrics = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: QueuesSubsystem,
			Name:      "queue_pending_resource_metrics",
			Help:      "pending resource metrics related to queues etc.",
		})
	q.queueUsedResourceMetrics = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: QueuesSubsystem,
			Name:      "queue_used_resource_metrics",
			Help:      "used resource metrics related to queues etc.",
		})
	q.queueAvailableResourceMetrics = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: QueuesSubsystem,
			Name:      "queue_available_resource_metrics",
			Help:      "available resource metrics related to queues etc.",
		})

	var queueMetricsList = []prometheus.Collector{
		q.queueAppMetrics,
		q.queuePendingResourceMetrics,
		q.queueUsedResourceMetrics,
		q.queueAvailableResourceMetrics,
	}

	// Register the metrics.
	queueRegisterMetrics.Do(func() {
		for _, metric := range queueMetricsList {
			prometheus.MustRegister(metric)
		}
	})

	return q
}


// Define and implement all the metrics ops for Prometheus.
// Metrics Ops related to applicationsAdded
func (m *QueueMetrics) IncApplicationsAdded() {
	m.applicationsAdded.Inc()
}

func (m *QueueMetrics) AddApplicationsAdded(value int) {
	m.applicationsAdded.Add(float64(value))
}

// Metrics ops related to ApplicationsAdded
func (m *QueueMetrics) IncApplicationsRejected() {
	m.applicationsRejected.Inc()
}

func (m *QueueMetrics) AddApplicationsRejected(value int) {
	m.applicationsRejected.Add(float64(value))
}

// Metrics Ops related to ApplicationsRunning
func (m *QueueMetrics) IncApplicationsRunning() {
	m.applicationsRunning.Inc()
}

func (m *QueueMetrics) AddApplicationsRunning(value int) {
	m.applicationsRunning.Add(float64(value))
}

func (m *QueueMetrics) DecApplicationsRunning() {
	m.applicationsRunning.Dec()
}

func (m *QueueMetrics) SubApplicationsRunning(value int) {
	m.applicationsRunning.Sub(float64(value))
}

func (m *QueueMetrics) SetApplicationsRunning(value int) {
	m.applicationsRunning.Set(float64(value))
}

// Metrics Ops related to ApplicationsCompleted
func (m *QueueMetrics) IncApplicationsCompleted() {
	m.applicationsCompleted.Inc()
}

func (m *QueueMetrics) AddApplicationsCompleted(value int) {
	m.applicationsCompleted.Add(float64(value))
}

func (m *QueueMetrics) DecApplicationsCompleted() {
	m.applicationsCompleted.Dec()
}

func (m *QueueMetrics) SubApplicationsCompleted(value int) {
	m.applicationsCompleted.Sub(float64(value))
}

func (m *QueueMetrics) SetApplicationsCompleted(value int) {
	m.applicationsCompleted.Set(float64(value))
}

// Metrics Ops related to QueuePendingResourceMetrics
func (m *QueueMetrics) IncQueuePendingResourceMetrics() {
	m.queuePendingResourceMetrics.Inc()
}

func (m *QueueMetrics) AddQueuePendingResourceMetrics(value float64) {
	m.queuePendingResourceMetrics.Add(float64(value))
}

func (m *QueueMetrics) DecQueuePendingResourceMetrics() {
	m.queuePendingResourceMetrics.Dec()
}

func (m *QueueMetrics) SubQueuePendingResourceMetrics(value float64) {
	m.queuePendingResourceMetrics.Sub(float64(value))
}

func (m *QueueMetrics) SetQueuePendingResourceMetrics(value float64) {
	m.queuePendingResourceMetrics.Set(float64(value))
}

// Metrics Ops related to queueUsedResourceMetrics
func (m *QueueMetrics) IncQueueUsedResourceMetrics() {
	m.queueUsedResourceMetrics.Inc()
}

func (m *QueueMetrics) AddQueueUsedResourceMetrics(value float64) {
	m.queueUsedResourceMetrics.Add(float64(value))
}

func (m *QueueMetrics) DecQueueUsedResourceMetrics() {
	m.queueUsedResourceMetrics.Dec()
}

func (m *QueueMetrics) SubQueueUsedResourceMetrics(value float64) {
	m.queueUsedResourceMetrics.Sub(float64(value))
}

func (m *QueueMetrics) SetQueueUsedResourceMetrics(value float64) {
	m.queueUsedResourceMetrics.Set(float64(value))
}

// Metrics Ops related to queueAvailableResourceMetrics
func (m *QueueMetrics) IncQueueAvailableResourceMetrics() {
	m.queueAvailableResourceMetrics.Inc()
}

func (m *QueueMetrics) AddQueueAvailableResourceMetrics(value float64) {
	m.queueAvailableResourceMetrics.Add(float64(value))
}

func (m *QueueMetrics) DecQueueAvailableResourceMetrics() {
	m.queueAvailableResourceMetrics.Dec()
}

func (m *QueueMetrics) SubQueueAvailableResourceMetrics(value float64) {
	m.queueAvailableResourceMetrics.Sub(float64(value))
}

func (m *QueueMetrics) SetQueueAvailableResourceMetrics(value float64) {
	m.queueAvailableResourceMetrics.Set(float64(value))
}