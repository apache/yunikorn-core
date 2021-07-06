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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
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

// SchedulerMetrics includes all core metrics variables to be declared
type SchedulerMetrics struct {
	containerAllocation        *prometheus.CounterVec
	allocatedContainers        prometheus.Counter
	rejectedContainers         prometheus.Counter
	schedulingErrors           prometheus.Counter
	releasedContainers         prometheus.Counter
	applicationSubmissions     *prometheus.CounterVec
	totalApplicationsRunning   prometheus.Gauge
	totalApplicationsCompleted prometheus.Gauge
	totalNodesActive           prometheus.Gauge
	totalNodesFailed           prometheus.Gauge
	nodesResourceUsages        map[string]*prometheus.GaugeVec
	schedulingLatency          prometheus.Histogram
	nodeSortingLatency         prometheus.Histogram
	appSortingLatency          prometheus.Histogram
	queueSortingLatency        prometheus.Histogram
	lock                       sync.RWMutex
}

// InitSchedulerMetrics initializes scheduler metrics
func InitSchedulerMetrics() *SchedulerMetrics {
	s := &SchedulerMetrics{
		lock: sync.RWMutex{},
	}

	// This map might be updated at runtime
	s.nodesResourceUsages = make(map[string]*prometheus.GaugeVec)

	// Container allocation
	s.containerAllocation = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "container_allocation_attempt_total",
			Help: "Total number of attempts to allocate containers. State of the attempt includes `allocated`, " +
				"`rejected`, `error`, `released`",
		}, []string{"state"})
	s.allocatedContainers = s.containerAllocation.With(prometheus.Labels{"state": "allocated"})
	s.rejectedContainers = s.containerAllocation.With(prometheus.Labels{"state": "rejected"})
	s.schedulingErrors = s.containerAllocation.With(prometheus.Labels{"state": "error"})
	s.releasedContainers = s.containerAllocation.With(prometheus.Labels{"state": "released"})

	// Application submission
	s.applicationSubmissions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "application_submission_total",
			Help:      "Total number of application submissions. State of the attempt includes `accepted` and `rejected`.",
		}, []string{"result"})

	// Application status
	s.totalApplicationsRunning = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "application_running_total",
			Help:      "Total number of applications running.",
		})
	s.totalApplicationsCompleted = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "application_completed_total",
			Help:      "Total number of applications completed.",
		})

	// Node status
	s.totalNodesActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "node_active_total",
			Help:      "Total number of active nodes.",
		})
	s.totalNodesFailed = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "node_failed_total",
			Help:      "Total number of failed nodes.",
		})

	// Latency of scheduling/sorting
	s.schedulingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "scheduling_latency_seconds",
			Help:      "Latency of the main scheduling routine, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 6), //start from 0.1ms
		},
	)
	s.nodeSortingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "node_sorting_latency_seconds",
			Help:      "Latency of all nodes sorting, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 6), //start from 0.1ms
		},
	)
	s.queueSortingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "queue_sorting_latency_seconds",
			Help:      "Latency of all queues sorting, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 6), //start from 0.1ms
		},
	)
	s.appSortingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "app_sorting_latency_seconds",
			Help:      "Latency of all applications sorting, in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 6), //start from 0.1ms
		},
	)

	// Register metrics
	var metricsList = []prometheus.Collector{
		s.containerAllocation,
		s.applicationSubmissions,
		s.schedulingLatency,
		s.nodeSortingLatency,
		s.queueSortingLatency,
		s.appSortingLatency,
		s.totalApplicationsRunning,
		s.totalApplicationsCompleted,
		s.totalNodesActive,
		s.totalNodesFailed,
	}
	for _, metric := range metricsList {
		if err := prometheus.Register(metric); err != nil {
			log.Logger().Warn("failed to register metrics collector", zap.Error(err))
		}
	}
	return s
}

// Reset metrics
func Reset() {}

// SinceInSeconds gets the time since the specified start in seconds
func SinceInSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}

func (m *SchedulerMetrics) ObserveSchedulingLatency(start time.Time) {
	m.schedulingLatency.Observe(SinceInSeconds(start))
}

func (m *SchedulerMetrics) ObserveNodeSortingLatency(start time.Time) {
	m.nodeSortingLatency.Observe(SinceInSeconds(start))
}

func (m *SchedulerMetrics) ObserveAppSortingLatency(start time.Time) {
	m.appSortingLatency.Observe(SinceInSeconds(start))
}

func (m *SchedulerMetrics) ObserveQueueSortingLatency(start time.Time) {
	m.queueSortingLatency.Observe(SinceInSeconds(start))
}

// Below is to define and implement all the metrics operation for Prometheus

func (m *SchedulerMetrics) IncAllocatedContainer() {
	m.allocatedContainers.Inc()
}

func (m *SchedulerMetrics) AddAllocatedContainers(value int) {
	m.allocatedContainers.Add(float64(value))
}

func (m *SchedulerMetrics) getAllocatedContainers() (int, error) {
	metricDto := &dto.Metric{}
	err := m.allocatedContainers.Write(metricDto)
	if err == nil {
		return int(*metricDto.Counter.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) IncReleasedContainer() {
	m.releasedContainers.Inc()
}

func (m *SchedulerMetrics) AddReleasedContainers(value int) {
	m.releasedContainers.Add(float64(value))
}

func (m *SchedulerMetrics) getReleasedContainers() (int, error) {
	metricDto := &dto.Metric{}
	err := m.releasedContainers.Write(metricDto)
	if err == nil {
		return int(*metricDto.Counter.Value), nil
	}
	return -1, err
}

// Metrics Ops related to allocationScheduleFailures

func (m *SchedulerMetrics) IncRejectedContainer() {
	m.rejectedContainers.Inc()
}

func (m *SchedulerMetrics) AddRejectedContainers(value int) {
	m.rejectedContainers.Add(float64(value))
}

func (m *SchedulerMetrics) IncSchedulingError() {
	m.schedulingErrors.Inc()
}

func (m *SchedulerMetrics) AddSchedulingErrors(value int) {
	m.schedulingErrors.Add(float64(value))
}

func (m *SchedulerMetrics) GetSchedulingErrors() (int, error) {
	metricDto := &dto.Metric{}
	err := m.schedulingErrors.Write(metricDto)
	if err == nil {
		return int(*metricDto.Counter.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) IncTotalApplicationsAccepted() {
	m.applicationSubmissions.With(prometheus.Labels{"result": "accepted"}).Inc()
}

func (m *SchedulerMetrics) AddTotalApplicationsAccepted(value int) {
	m.applicationSubmissions.With(prometheus.Labels{"result": "accepted"}).Add(float64(value))
}

func (m *SchedulerMetrics) IncTotalApplicationsRejected() {
	m.applicationSubmissions.With(prometheus.Labels{"result": "rejected"}).Inc()
}

func (m *SchedulerMetrics) AddTotalApplicationsRejected(value int) {
	m.applicationSubmissions.With(prometheus.Labels{"result": "rejected"}).Add(float64(value))
}

func (m *SchedulerMetrics) IncTotalApplicationsRunning() {
	m.totalApplicationsRunning.Inc()
}

func (m *SchedulerMetrics) AddTotalApplicationsRunning(value int) {
	m.totalApplicationsRunning.Add(float64(value))
}

func (m *SchedulerMetrics) DecTotalApplicationsRunning() {
	m.totalApplicationsRunning.Dec()
}

func (m *SchedulerMetrics) SubTotalApplicationsRunning(value int) {
	m.totalApplicationsRunning.Sub(float64(value))
}

func (m *SchedulerMetrics) SetTotalApplicationsRunning(value int) {
	m.totalApplicationsRunning.Set(float64(value))
}

func (m *SchedulerMetrics) getTotalApplicationsRunning() (int, error) {
	metricDto := &dto.Metric{}
	err := m.totalApplicationsRunning.Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) IncTotalApplicationsCompleted() {
	m.totalApplicationsCompleted.Inc()
}

func (m *SchedulerMetrics) AddTotalApplicationsCompleted(value int) {
	m.totalApplicationsCompleted.Add(float64(value))
}

func (m *SchedulerMetrics) DecTotalApplicationsCompleted() {
	m.totalApplicationsCompleted.Dec()
}

func (m *SchedulerMetrics) SubTotalApplicationsCompleted(value int) {
	m.totalApplicationsCompleted.Sub(float64(value))
}

func (m *SchedulerMetrics) SetTotalApplicationsCompleted(value int) {
	m.totalApplicationsCompleted.Set(float64(value))
}

func (m *SchedulerMetrics) IncActiveNodes() {
	m.totalNodesActive.Inc()
}

func (m *SchedulerMetrics) AddActiveNodes(value int) {
	m.totalNodesActive.Add(float64(value))
}

func (m *SchedulerMetrics) DecActiveNodes() {
	m.totalNodesActive.Dec()
}

func (m *SchedulerMetrics) SubActiveNodes(value int) {
	m.totalNodesActive.Sub(float64(value))
}

func (m *SchedulerMetrics) SetActiveNodes(value int) {
	m.totalNodesActive.Set(float64(value))
}

func (m *SchedulerMetrics) IncFailedNodes() {
	m.totalNodesFailed.Inc()
}

func (m *SchedulerMetrics) AddFailedNodes(value int) {
	m.totalNodesFailed.Add(float64(value))
}

func (m *SchedulerMetrics) DecFailedNodes() {
	m.totalNodesFailed.Dec()
}

func (m *SchedulerMetrics) SubFailedNodes(value int) {
	m.totalNodesFailed.Sub(float64(value))
}

func (m *SchedulerMetrics) SetFailedNodes(value int) {
	m.totalNodesFailed.Set(float64(value))
}
func (m *SchedulerMetrics) GetFailedNodes() (int, error) {
	metricDto := &dto.Metric{}
	err := m.totalNodesFailed.Write(metricDto)
	if err == nil {
		return int(*metricDto.Gauge.Value), nil
	}
	return -1, err
}

func (m *SchedulerMetrics) SetNodeResourceUsage(resourceName string, rangeIdx int, value float64) {
	m.lock.Lock()
	defer m.lock.Unlock()
	var resourceMetrics *prometheus.GaugeVec
	resourceMetrics, ok := m.nodesResourceUsages[resourceName]
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
			log.Logger().Warn("failed to register metrics collector", zap.Error(err))
			return
		}
		m.nodesResourceUsages[resourceName] = resourceMetrics
	}
	resourceMetrics.With(prometheus.Labels{"range": resourceUsageRangeBuckets[rangeIdx]}).Set(value)
}
