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
	"time"
)

const (
	// all metrics should be declared under this namespace
	Namespace = "yunikorn"
	// SchedulerSubsystem - subsystem name used by scheduler
	SchedulerSubsystem = "scheduler"
	// EventSubsystem - subsystem name used by event cache
	EventSubsystem = "event"
	// replacement of invalid byte for prometheus metric names
	MetricNameInvalidByteReplacement = '_'
)

var once sync.Once
var m *Metrics

type Metrics struct {
	scheduler CoreSchedulerMetrics
	queues    map[string]CoreQueueMetrics
	event     CoreEventMetrics
	lock      sync.RWMutex
}

type CoreQueueMetrics interface {
	IncApplicationsAccepted()
	IncApplicationsRejected()
	IncApplicationsCompleted()
	AddQueueUsedResourceMetrics(resourceName string, value float64)
	SetQueueUsedResourceMetrics(resourceName string, value float64)
}

// Declare all core metrics ops in this interface
type CoreSchedulerMetrics interface {
	// Metrics Ops related to ScheduledAllocationSuccesses
	IncAllocatedContainer()
	AddAllocatedContainers(value int)
	getAllocatedContainers() (int, error)

	// Metrics Ops related to ScheduledAllocationFailures
	IncRejectedContainer()
	AddRejectedContainers(value int)

	// Metrics Ops related to ScheduledAllocationErrors
	IncSchedulingError()
	AddSchedulingErrors(value int)
	GetSchedulingErrors() (int, error)

	// Metrics Ops related to released allocations
	IncReleasedContainer()
	AddReleasedContainers(value int)

	// Metrics Ops related to TotalApplicationsAdded
	IncTotalApplicationsAdded()
	AddTotalApplicationsAdded(value int)

	// Metrics Ops related to TotalApplicationsRejected
	IncTotalApplicationsRejected()
	AddTotalApplicationsRejected(value int)

	// Metrics Ops related to TotalApplicationsRunning
	IncTotalApplicationsRunning()
	AddTotalApplicationsRunning(value int)
	DecTotalApplicationsRunning()
	SubTotalApplicationsRunning(value int)
	SetTotalApplicationsRunning(value int)
	getTotalApplicationsRunning() (int, error)

	// Metrics Ops related to TotalApplicationsCompleted
	IncTotalApplicationsCompleted()
	AddTotalApplicationsCompleted(value int)
	DecTotalApplicationsCompleted()
	SubTotalApplicationsCompleted(value int)
	SetTotalApplicationsCompleted(value int)

	// Metrics Ops related to ActiveNodes
	IncActiveNodes()
	AddActiveNodes(value int)
	DecActiveNodes()
	SubActiveNodes(value int)
	SetActiveNodes(value int)

	// Metrics Ops related to failedNodes
	IncFailedNodes()
	AddFailedNodes(value int)
	DecFailedNodes()
	SubFailedNodes(value int)
	SetFailedNodes(value int)
	SetNodeResourceUsage(resourceName string, rangeIdx int, value float64)
	GetFailedNodes() (int, error)

	//latency change
	ObserveSchedulingLatency(start time.Time)
	ObserveNodeSortingLatency(start time.Time)
	ObserveAppSortingLatency(start time.Time)
	ObserveQueueSortingLatency(start time.Time)
}

type CoreEventMetrics interface {
	IncEventsCreated()
	IncEventsChanneled()
	IncEventsNotChanneled()
	IncEventsProcessed()
	IncEventsStored()
	IncEventsNotStored()
	AddEventsCollected(collectedEvents int)
}

func init() {
	once.Do(func() {
		m = &Metrics{
			scheduler: initSchedulerMetrics(),
			queues:    make(map[string]CoreQueueMetrics),
			event:     initEventMetrics(),
			lock:      sync.RWMutex{},
		}
	})
}

func GetSchedulerMetrics() CoreSchedulerMetrics {
	return m.scheduler
}

func GetQueueMetrics(name string) CoreQueueMetrics {
	m.lock.Lock()
	defer m.lock.Unlock()
	if qm, ok := m.queues[name]; ok {
		return qm
	}
	queueMetrics := forQueue(name)
	m.queues[name] = queueMetrics
	return queueMetrics
}

func GetEventMetrics() CoreEventMetrics {
	return m.event
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
