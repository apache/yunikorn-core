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
	"sync"

	"github.com/apache/yunikorn-core/pkg/locking"
)

const (
	// Namespace for all metrics inside the scheduler
	Namespace = "yunikorn"
	// SchedulerSubsystem - subsystem name used by scheduler
	SchedulerSubsystem = "scheduler"
	// EventSubsystem - subsystem name used by event cache
	EventSubsystem = "event"
	// MetricNameInvalidByteReplacement byte used to replace invalid bytes in prometheus metric names
	MetricNameInvalidByteReplacement = '_'
)

var once sync.Once
var m *Metrics

type Metrics struct {
	scheduler *SchedulerMetrics
	queues    map[string]*QueueMetrics
	event     *EventMetrics
	runtime   *RuntimeMetrics
	lock      locking.RWMutex
}

func init() {
	once.Do(func() {
		m = &Metrics{
			scheduler: InitSchedulerMetrics(),
			queues:    make(map[string]*QueueMetrics),
			event:     initEventMetrics(),
			lock:      locking.RWMutex{},
			runtime:   initRuntimeMetrics(),
		}
	})
}

func Reset() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.scheduler.Reset()
	m.event.Reset()
	for _, qm := range m.queues {
		qm.Reset()
	}
	m.runtime.Reset()
}

func GetSchedulerMetrics() *SchedulerMetrics {
	return m.scheduler
}

func GetQueueMetrics(name string) *QueueMetrics {
	m.lock.Lock()
	defer m.lock.Unlock()
	if qm, ok := m.queues[name]; ok {
		return qm
	}
	queueMetrics := InitQueueMetrics(name)
	m.queues[name] = queueMetrics
	return queueMetrics
}

func RemoveQueueMetrics(name string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if metrics, ok := m.queues[name]; ok {
		metrics.UnregisterMetrics()
		delete(m.queues, name)
	}
}

func GetEventMetrics() *EventMetrics {
	return m.event
}

func GetRuntimeMetrics() *RuntimeMetrics {
	return m.runtime
}

// Format metric name based on the definition of metric name in prometheus, as per
// https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
func formatMetricName(metricName string) string {
	if len(metricName) == 0 {
		return metricName
	}
	newBytes := make([]byte, len(metricName))
	for i := 0; i < len(metricName); i++ {
		b := metricName[i]
		// nolint: staticcheck
		if !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_' || b == ':' || (b >= '0' && b <= '9')) {
			newBytes[i] = MetricNameInvalidByteReplacement
		} else {
			newBytes[i] = b
		}
	}
	if '0' <= metricName[0] && metricName[0] <= '9' {
		return string(MetricNameInvalidByteReplacement) + string(newBytes)
	}
	return string(newBytes)
}
