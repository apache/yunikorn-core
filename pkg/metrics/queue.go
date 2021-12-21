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
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

// QueueMetrics to declare queue metrics
type QueueMetrics struct {
	appMetrics               *prometheus.CounterVec
	usedResourceMetrics      *prometheus.GaugeVec
	pendingResourceMetrics   *prometheus.GaugeVec
	availableResourceMetrics *prometheus.GaugeVec
}

// InitQueueMetrics to initialize queue metrics
func InitQueueMetrics(name string) CoreQueueMetrics {
	q := &QueueMetrics{}

	q.appMetrics = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: substituteQueueName(name),
			Name:      "queue_app_metrics",
			Help:      "Queue application metrics",
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

func substituteQueueName(queueName string) string {
	str := fmt.Sprintf("queue_%s",
		strings.Replace(queueName, ".", "_", -1))
	return strings.Replace(str, "-", "_", -1)
}

func (m *QueueMetrics) IncQueueApplicationsAccepted() {
	m.appMetrics.With(prometheus.Labels{"state": "accepted"}).Inc()
}

func (m *QueueMetrics) IncQueueApplicationsRejected() {
	m.appMetrics.With(prometheus.Labels{"state": "rejected"}).Inc()
}

func (m *QueueMetrics) IncQueueApplicationsCompleted() {
	m.appMetrics.With(prometheus.Labels{"state": "completed"}).Inc()
}

func (m *QueueMetrics) AddQueueUsedResourceMetrics(resourceName string, value float64) {
	m.usedResourceMetrics.With(prometheus.Labels{"resource": resourceName}).Add(value)
}

func (m *QueueMetrics) SetQueueUsedResourceMetrics(resourceName string, value float64) {
	m.usedResourceMetrics.With(prometheus.Labels{"resource": resourceName}).Set(value)
}
