/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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

package metrics

import (
	"fmt"
	"github.com/cloudera/yunikorn-core/pkg/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"strings"
)

type QueueMetrics struct {
	// metrics related to app
	appMetrics *prometheus.CounterVec

	// metrics related to resource
	usedResourceMetrics      *prometheus.GaugeVec
	pendingResourceMetrics   *prometheus.GaugeVec
	availableResourceMetrics *prometheus.GaugeVec
}

func forQueue(name string) CoreQueueMetrics {
	q := &QueueMetrics{}

	// Queue Metrics
	q.appMetrics = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: substituteQueueName(name),
			Name:      "app_metrics",
			Help:      "Application Metrics",
		}, []string{"state"})

	q.usedResourceMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: substituteQueueName(name),
			Name:      "used_resource",
			Help:      "Queue used resource",
		}, []string{"resource"})

	q.pendingResourceMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: substituteQueueName(name),
			Name:      "pending_resource",
			Help:      "Queue pending resource",
		}, []string{"resource"})

	q.availableResourceMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: substituteQueueName(name),
			Name:      "used_resource_metrics",
			Help:      "used resource metrics related to queues etc.",
		}, []string{"resource"})

	var queueMetricsList = []prometheus.Collector{
		q.appMetrics,
		q.usedResourceMetrics,
		q.pendingResourceMetrics,
		q.availableResourceMetrics,
	}

	// Register the metrics.
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

func substituteQueueName(queueName string) string {
	str := fmt.Sprintf("queue_%s",
		strings.Replace(queueName, ".", "_", -1))
	return strings.Replace(str, "-", "_", -1)
}

func (m *QueueMetrics) IncApplicationsAccepted() {
	m.appMetrics.With(prometheus.Labels{"state": "accepted"}).Inc()
}

func (m *QueueMetrics) IncApplicationsRejected() {
	m.appMetrics.With(prometheus.Labels{"state": "rejected"}).Inc()
}

func (m *QueueMetrics) IncApplicationsCompleted() {
	m.appMetrics.With(prometheus.Labels{"state": "completed"}).Inc()
}

func (m *QueueMetrics) AddQueueUsedResourceMetrics(resourceName string, value float64) {
	m.usedResourceMetrics.With(prometheus.Labels{"resource": resourceName}).Add(value)
}

func (m *QueueMetrics) SetQueueUsedResourceMetrics(resourceName string, value float64) {
	m.usedResourceMetrics.With(prometheus.Labels{"resource": resourceName}).Set(value)
}
