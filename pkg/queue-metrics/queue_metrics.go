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

package queue_metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

const (
	// 	QueuesSubsystem = "queues_metrics" - subsystem name used by queues
	QueuesSubsystem = "queues_metrics"
)

type QueueMetrics struct  {
	Name string
	queueAppMetrics  *prometheus.CounterVec
	ApplicationsAdded prometheus.Counter
	ApplicationsRejected prometheus.Counter
	ApplicationsRunning prometheus.Gauge
	ApplicationsCompleted prometheus.Gauge
	queuePendingResourceMetrics prometheus.Gauge
	queueUsedResourceMetrics prometheus.Gauge
	queueAvailableResourceMetrics prometheus.Gauge
}

var queueRegisterMetrics sync.Once

func NewQueueMetrics(name string) *QueueMetrics {
	q := &QueueMetrics{Name:name}

	// Queue Metrics
	q.queueAppMetrics = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: QueuesSubsystem,
			Name:      "queue_metrics_for_apps",
			Help:      "Application Metrics related to queues etc.",
		}, []string{"result"})

	// ApplicationsAdded counts how many apps are added to YuniKorn.
	q.ApplicationsAdded = q.queueAppMetrics.With(prometheus.Labels{"result": "added"})
	// ApplicationsRejected counts how many apps are rejected in YuniKorn.
	q.ApplicationsRejected = q.queueAppMetrics.With(prometheus.Labels{"result": "rejected"})
	// ApplicationsRunning counts how many apps are running active.
	q.ApplicationsRunning = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: QueuesSubsystem,
			Name:      "queue_running_apps",
			Help:      "active apps",
		})
	// ApplicationsCompleted counts how many apps are completed.
	q.ApplicationsCompleted = prometheus.NewGauge(
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
