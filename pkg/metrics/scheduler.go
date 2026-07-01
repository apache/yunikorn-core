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
	"(10%,20%]",
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
	schedulingCycle       prometheus.Histogram
	sortingLatency        *prometheus.HistogramVec
	tryNodeLatency        prometheus.Histogram
	tryPreemptionLatency  prometheus.Histogram
	tryNodeEvaluation     prometheus.Histogram
	lock                  locking.RWMutex
	tryNodeCount          *prometheus.CounterVec
	tryApplicationCount   *prometheus.CounterVec
}

// InitSchedulerMetrics to initialize scheduler metrics
func InitSchedulerMetrics() *SchedulerMetrics {
	s := &SchedulerMetrics{
		lock: locking.RWMutex{},
	}

	s.nodeResourceUsage = make(map[string]*prometheus.GaugeVec) // Note: This map might be updated at runtime

	initCounterMetrics(s)
	initGaugeMetrics(s)
	initHistogramMetrics(s)
	registerSchedulerMetrics(s)

	return s
}

// initCounterMetrics initializes counter-based metrics
func initCounterMetrics(s *SchedulerMetrics) {
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
			Help:      "Total number of application submissions. State of the attempt includes `new`, `accepted` and `rejected`.",
		}, []string{"result"})

	s.tryNodeCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "trynode_count",
			Help:      "Total number of nodes evaluated during scheduling cycle",
		}, nil)

	s.tryApplicationCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "tryapplication_count",
			Help:      "Total number of applications evaluated during scheduling cycle",
		}, nil)
}

// initGaugeMetrics initializes gauge-based metrics
func initGaugeMetrics(s *SchedulerMetrics) {
	s.application = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "application_total",
			Help:      "Total number of applications. State of the application includes `running`, `resuming`, `failing`, `completing`, `completed` and `failed`.",
		}, []string{"state"})

	s.node = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "node",
			Help:      "Total number of nodes. State of the node includes `active` and `failed`.",
		}, []string{"state"})
}

// initHistogramMetrics initializes histogram-based metrics
func initHistogramMetrics(s *SchedulerMetrics) {
	s.schedulingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "scheduling_latency_milliseconds",
			Help:      "Latency of the main scheduling routine, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 8), // start from 0.1ms
		},
	)

	s.schedulingCycle = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "scheduling_cycle_milliseconds",
			Help:      "Time taken for a scheduling cycle, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 8),
		},
	)

	s.sortingLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "node_sorting_latency_milliseconds",
			Help:      "Latency of all nodes sorting, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 8), // start from 0.1ms
		}, []string{"level"})

	s.tryNodeLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "trynode_latency_milliseconds",
			Help:      "Latency of node condition checks for container allocations, such as placement constraints, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 8),
		},
	)

	s.tryNodeEvaluation = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "trynode_evaluation_milliseconds",
			Help:      "Time taken to evaluate nodes for a pod, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 8),
		},
	)

	s.tryPreemptionLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "trypreemption_latency_milliseconds",
			Help:      "Latency of preemption condition checks for container allocations, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 8),
		},
	)
}

// registerSchedulerMetrics registers all scheduler metrics with Prometheus
func registerSchedulerMetrics(s *SchedulerMetrics) {
	var metricsList = []prometheus.Collector{
		s.containerAllocation,
		s.applicationSubmission,
		s.application,
		s.node,
		s.schedulingLatency,
		s.sortingLatency,
		s.tryNodeLatency,
		s.schedulingCycle,
		s.tryNodeEvaluation,
		s.tryPreemptionLatency,
		s.tryNodeCount,
		s.tryApplicationCount,
	}
	for _, metric := range metricsList {
		if err := prometheus.Register(metric); err != nil {
			log.Log(log.Metrics).Warn("failed to register metrics collector", zap.Error(err))
		}
	}
}

// Reset all metrics that implement the Reset functionality.
// should only be used in tests
func (m *SchedulerMetrics) Reset() {
	m.node.Reset()
	m.application.Reset()
	m.applicationSubmission.Reset()
	m.containerAllocation.Reset()
	m.tryNodeCount.Reset()
	m.tryApplicationCount.Reset()
}

func SinceInSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}

func (m *SchedulerMetrics) ObserveSchedulingLatency(start time.Time) {
	m.schedulingLatency.Observe(SinceInSeconds(start))
}

func (m *SchedulerMetrics) ObserveSchedulingCycle(start time.Time) {
	m.schedulingCycle.Observe(SinceInSeconds(start))
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

func (m *SchedulerMetrics) ObserveTryNodeEvaluation(start time.Time) {
	m.tryNodeEvaluation.Observe(SinceInSeconds(start))
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

func (m *SchedulerMetrics) AddTryNodeCount(count int64) {
	m.tryNodeCount.With(nil).Add(float64(count))
}

func (m *SchedulerMetrics) ResetTryNodeCount() {
	m.tryNodeCount.Reset()
}

func (m *SchedulerMetrics) AddTryApplicationCount(count int64) {
	m.tryApplicationCount.With(nil).Add(float64(count))
}

func (m *SchedulerMetrics) ResetTryApplicationCount() {
	m.tryApplicationCount.Reset()
}

func (m *SchedulerMetrics) GetTryNodeCount() (int64, error) {
	metricDto := &dto.Metric{}
	err := m.tryNodeCount.With(nil).Write(metricDto)
	if err == nil {
		return int64(*metricDto.Counter.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) IncTotalApplicationsNew() {
	m.applicationSubmission.WithLabelValues(AppNew).Inc()
}

func (m *SchedulerMetrics) GetTotalApplicationsNew() (int, error) {
	metricDto := &dto.Metric{}
	err := m.applicationSubmission.WithLabelValues(AppNew).Write(metricDto)
	if err == nil {
		return int(*metricDto.Counter.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) IncTotalApplicationsAccepted() {
	m.applicationSubmission.WithLabelValues(AppAccepted).Inc()
}

func (m *SchedulerMetrics) GetTotalApplicationsAccepted() (int, error) {
	metricDto := &dto.Metric{}
	err := m.applicationSubmission.WithLabelValues(AppAccepted).Write(metricDto)
	if err == nil {
		return int(*metricDto.Counter.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) IncTotalApplicationsRejected() {
	m.applicationSubmission.WithLabelValues(AppRejected).Inc()
}

func (m *SchedulerMetrics) GetTotalApplicationsRejected() (int, error) {
	metricDto := &dto.Metric{}
	err := m.applicationSubmission.WithLabelValues(AppRejected).Write(metricDto)
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

func (m *SchedulerMetrics) GetTotalApplicationsRunning() (int, error) {
	metricDto := &dto.Metric{}
	err := m.application.WithLabelValues(AppRunning).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) IncTotalApplicationsFailing() {
	m.application.WithLabelValues(AppFailing).Inc()
}

func (m *SchedulerMetrics) DecTotalApplicationsFailing() {
	m.application.WithLabelValues(AppFailing).Dec()
}

func (m *SchedulerMetrics) IncTotalApplicationsFailed() {
	m.application.WithLabelValues(AppFailed).Inc()
}

func (m *SchedulerMetrics) IncTotalApplicationsCompleting() {
	m.application.WithLabelValues(AppCompleting).Inc()
}

func (m *SchedulerMetrics) DecTotalApplicationsCompleting() {
	m.application.WithLabelValues(AppCompleting).Dec()
}

func (m *SchedulerMetrics) IncTotalApplicationsResuming() {
	m.application.WithLabelValues(AppResuming).Inc()
}

func (m *SchedulerMetrics) DecTotalApplicationsResuming() {
	m.application.WithLabelValues(AppResuming).Dec()
}

func (m *SchedulerMetrics) GetTotalApplicationsResuming() (int, error) {
	metricDto := &dto.Metric{}
	err := m.application.WithLabelValues(AppResuming).Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) IncTotalApplicationsCompleted() {
	m.application.WithLabelValues(AppCompleted).Inc()
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
