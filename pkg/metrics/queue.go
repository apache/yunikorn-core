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
	appMetrics      *prometheus.GaugeVec
	ResourceMetrics *prometheus.GaugeVec
}

// InitQueueMetrics to initialize queue metrics
func InitQueueMetrics(name string) CoreQueueMetrics {
	q := &QueueMetrics{}

	q.appMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: substituteQueueName(name),
			Name:      "queue_app",
			Help:      "Queue application metrics. State of the application includes `running`.",
		}, []string{"state"})

	q.ResourceMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: substituteQueueName(name),
			Name:      "queue_resource",
			Help:      "Queue resource metrics. State of the resource includes `guaranteed`, `max`, `allocated`, `pending`.",
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

func substituteQueueName(queueName string) string {
	str := fmt.Sprintf("queue_%s",
		strings.Replace(queueName, ".", "_", -1))
	return strings.Replace(str, "-", "_", -1)
}

func (m *QueueMetrics) IncQueueApplicationsRunning() {
	m.appMetrics.With(prometheus.Labels{"state": "running"}).Inc()
}

func (m *QueueMetrics) DecQueueApplicationsRunning() {
	m.appMetrics.With(prometheus.Labels{"state": "running"}).Dec()
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
