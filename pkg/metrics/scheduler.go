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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/log"
)

const (
	SchedulingError = "error"

	SortingApp   = "app"
	SortingQueue = "queue"

	NodeActive         = "active"
	NodeDraining       = "draining"
	NodeDecommissioned = "decommissioned"
)

var resourceUsageRangeBuckets = []string{
	"[0,10%]",
	"(10%, 20%]",
	"(20%,30%]",
	"(30%,40%]",
	"(40%,50%]",
	"(50%,60%]",
	"(60%,70%]",
	"(70%,80%]",
	"(80%,90%]",
	"(90%,100%]",
}

// SchedulerMetrics to declare scheduler metrics
type SchedulerMetrics struct {
	containerAllocation   *prometheus.CounterVec
	applicationSubmission *prometheus.CounterVec
	application           *prometheus.GaugeVec
	node                  *prometheus.GaugeVec
	nodeResourceUsage     map[string]*prometheus.GaugeVec
	schedulingLatency     prometheus.Histogram
	sortingLatency        *prometheus.HistogramVec
	tryNodeLatency        prometheus.Histogram
	tryPreemptionLatency  prometheus.Histogram
	lock                  locking.RWMutex
}

// InitSchedulerMetrics to initialize scheduler metrics
func InitSchedulerMetrics() *SchedulerMetrics {
	s := &SchedulerMetrics{
		lock: locking.RWMutex{},
	}

	s.nodeResourceUsage = make(map[string]*prometheus.GaugeVec) // Note: This map might be updated at runtime

	s.containerAllocation = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "container_allocation_attempt_total",
			Help:      "Total number of attempts to allocate containers. State of the attempt includes `allocated`, `rejected`, `error`, `released`",
		}, []string{"state"})

	s.applicationSubmission = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "application_submission_total",
			Help:      "Total number of application submissions. State of the attempt includes `accepted` and `rejected`.",
		}, []string{"result"})

	s.application = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "application_total",
			Help:      "Total number of applications. State of the application includes `running`, `completed` and `failed`.",
		}, []string{"state"})

	s.node = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "node",
			Help:      "Total number of nodes. State of the node includes `active` and `failed`.",
		}, []string{"state"})

	s.schedulingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "scheduling_latency_milliseconds",
			Help:      "Latency of the main scheduling routine, in milliseconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 6), // start from 0.1ms
		},
	)
	s.sortingLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "node_sorting_latency_milliseconds",
			Help:      "Latency of all nodes sorting, in milliseconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 6), // start from 0.1ms
		}, []string{"level"})

	s.tryNodeLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "trynode_latency_milliseconds",
			Help:      "Latency of node condition checks for container allocations, such as placement constraints, in milliseconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 6),
		},
	)

	s.tryPreemptionLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "trypreemption_latency_milliseconds",
			Help:      "Latency of preemption condition checks for container allocations, in milliseconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 6),
		},
	)

	// Register the metrics
	var metricsList = []prometheus.Collector{
		s.containerAllocation,
		s.applicationSubmission,
		s.application,
		s.node,
		s.schedulingLatency,
		s.sortingLatency,
		s.tryNodeLatency,
		s.tryPreemptionLatency,
	}
	for _, metric := range metricsList {
		if err := prometheus.Register(metric); err != nil {
			log.Log(log.Metrics).Warn("failed to register metrics collector", zap.Error(err))
		}
	}
	return s
}

// Reset all metrics that implement the Reset functionality.
// should only be used in tests
func (m *SchedulerMetrics) Reset() {
	m.node.Reset()
	m.application.Reset()
	m.applicationSubmission.Reset()
	m.containerAllocation.Reset()
}

func SinceInSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}

func (m *SchedulerMetrics) ObserveSchedulingLatency(start time.Time) {
	m.schedulingLatency.Observe(SinceInSeconds(start))
}

func (m *SchedulerMetrics) ObserveAppSortingLatency(start time.Time) {
	m.sortingLatency.WithLabelValues(SortingApp).Observe(SinceInSeconds(start))
}

func (m *SchedulerMetrics) ObserveQueueSortingLatency(start time.Time) {
	m.sortingLatency.WithLabelValues(SortingQueue).Observe(SinceInSeconds(start))
}

func (m *SchedulerMetrics) ObserveTryNodeLatency(start time.Time) {
	m.tryNodeLatency.Observe(SinceInSeconds(start))
}

func (m *SchedulerMetrics) ObserveTryPreemptionLatency(start time.Time) {
	m.tryPreemptionLatency.Observe(SinceInSeconds(start))
}

func (m *SchedulerMetrics) AddAllocatedContainers(value int) {
	m.containerAllocation.WithLabelValues(ContainerAllocated).Add(float64(value))
}

func (m *SchedulerMetrics) getAllocatedContainers() (int, error) {
	metricDto := &dto.Metric{}
	err := m.containerAllocation.WithLabelValues(ContainerAllocated).Write(metricDto)
	if err == nil {
		return int(*metricDto.Counter.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) AddReleasedContainers(value int) {
	m.containerAllocation.WithLabelValues(ContainerReleased).Add(float64(value))
}

func (m *SchedulerMetrics) getReleasedContainers() (int, error) {
	metricDto := &dto.Metric{}
	err := m.containerAllocation.WithLabelValues(ContainerReleased).Write(metricDto)
	if err == nil {
		return int(*metricDto.Counter.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) AddRejectedContainers(value int) {
	m.containerAllocation.WithLabelValues(ContainerRejected).Add(float64(value))
}

func (m *SchedulerMetrics) IncSchedulingError() {
	m.containerAllocation.WithLabelValues(SchedulingError).Inc()
}

func (m *SchedulerMetrics) GetSchedulingErrors() (int, error) {
	metricDto := &dto.Metric{}
	err := m.containerAllocation.WithLabelValues(SchedulingError).Write(metricDto)
	if err == nil {
		return int(*metricDto.Counter.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) IncTotalApplicationsAccepted() {
	m.applicationSubmission.WithLabelValues(AppAccepted).Inc()
}

func (m *SchedulerMetrics) AddTotalApplicationsAccepted(value int) {
	m.applicationSubmission.WithLabelValues(AppAccepted).Add(float64(value))
}

func (m *SchedulerMetrics) IncTotalApplicationsRejected() {
	m.applicationSubmission.WithLabelValues(ContainerRejected).Inc()
}

func (m *SchedulerMetrics) AddTotalApplicationsRejected(value int) {
	m.applicationSubmission.WithLabelValues(ContainerRejected).Add(float64(value))
}

func (m *SchedulerMetrics) GetTotalApplicationsRejected() (int, error) {
	metricDto := &dto.Metric{}
	err := m.applicationSubmission.WithLabelValues(ContainerRejected).Write(metricDto)
	if err == nil {
		return int(*metricDto.Counter.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) IncTotalApplicationsRunning() {
	m.application.WithLabelValues(AppRunning).Inc()
}

func (m *SchedulerMetrics) DecTotalApplicationsRunning() {
	m.application.WithLabelValues(AppRunning).Dec()
}

func (m *SchedulerMetrics) SubTotalApplicationsRunning(value int) {
	m.application.WithLabelValues(AppRunning).Sub(float64(value))
}

func (m *SchedulerMetrics) GetTotalApplicationsRunning() (int, error) {
	metricDto := &dto.Metric{}
	err := m.application.WithLabelValues(AppRunning).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) IncTotalApplicationsFailed() {
	m.application.WithLabelValues(AppFailed).Inc()
}

func (m *SchedulerMetrics) IncTotalApplicationsCompleted() {
	m.application.WithLabelValues(AppCompleted).Inc()
}

func (m *SchedulerMetrics) AddTotalApplicationsCompleted(value int) {
	m.application.WithLabelValues(AppCompleted).Add(float64(value))
}

func (m *SchedulerMetrics) GetTotalApplicationsCompleted() (int, error) {
	metricDto := &dto.Metric{}
	err := m.application.WithLabelValues(AppCompleted).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) IncActiveNodes() {
	m.node.WithLabelValues(NodeActive).Inc()
}

func (m *SchedulerMetrics) DecActiveNodes() {
	m.node.WithLabelValues(NodeActive).Dec()
}

func (m *SchedulerMetrics) IncFailedNodes() {
	m.node.WithLabelValues(AppFailed).Inc()
}

func (m *SchedulerMetrics) DecFailedNodes() {
	m.node.WithLabelValues(AppFailed).Dec()
}

func (m *SchedulerMetrics) GetFailedNodes() (int, error) {
	metricDto := &dto.Metric{}
	err := m.node.WithLabelValues(AppFailed).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) SetNodeResourceUsage(resourceName string, rangeIdx int, value float64) {
	m.lock.Lock()
	defer m.lock.Unlock()
	var resourceMetrics *prometheus.GaugeVec
	resourceMetrics, ok := m.nodeResourceUsage[resourceName]
	if !ok {
		metricsName := fmt.Sprintf("%s_node_usage_total", formatMetricName(resourceName))
		resourceMetrics = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Subsystem: SchedulerSubsystem,
				Name:      metricsName,
				Help:      "Total resource usage of node, by resource name.",
			}, []string{"range"})
		if err := prometheus.Register(resourceMetrics); err != nil {
			log.Log(log.Metrics).Warn("failed to register metrics collector", zap.Error(err))
			return
		}
		m.nodeResourceUsage[resourceName] = resourceMetrics
	}
	resourceMetrics.WithLabelValues(resourceUsageRangeBuckets[rangeIdx]).Set(value)
}

func (m *SchedulerMetrics) IncDrainingNodes() {
	m.node.WithLabelValues(NodeDraining).Inc()
}

func (m *SchedulerMetrics) DecDrainingNodes() {
	m.node.WithLabelValues(NodeDraining).Dec()
}

func (m *SchedulerMetrics) GetDrainingNodes() (int, error) {
	metricDto := &dto.Metric{}
	err := m.node.WithLabelValues(NodeDraining).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) IncTotalDecommissionedNodes() {
	m.node.WithLabelValues(NodeDecommissioned).Inc()
}
