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
	"sync"
	"time"
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

// All core metrics variables to be declared in this struct
type SchedulerMetrics struct  {
	allocations                *prometheus.CounterVec
	allocatedContainers        prometheus.Counter
	rejectedContainers         prometheus.Counter
	schedulingErrors           prometheus.Counter
	releasedContainers         prometheus.Counter
	scheduleApplications       *prometheus.CounterVec
	totalApplicationsAdded     prometheus.Counter
	totalApplicationsRejected  prometheus.Counter
	totalApplicationsRunning   prometheus.Gauge
	totalApplicationsCompleted prometheus.Gauge
	activeNodes                prometheus.Gauge
	failedNodes                prometheus.Gauge
	nodesResourceUsages        map[string]*prometheus.GaugeVec
	schedulingLatency          prometheus.Histogram
	nodeSortingLatency         prometheus.Histogram
	lock                       sync.RWMutex
}

// Initialize scheduler metrics
func initSchedulerMetrics() *SchedulerMetrics {
	s := &SchedulerMetrics{
		lock: sync.RWMutex{},
	}

	// this map might be updated at runtime
	s.nodesResourceUsages = make(map[string]*prometheus.GaugeVec)

	// containers
	s.allocations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "container_allocation",
			Help:      "Number of attempts to schedule containers, by the result. error means attempt failed due to internal errors",
		}, []string{"state"})
	s.allocatedContainers = s.allocations.With(prometheus.Labels{"state": "allocated"})
	s.rejectedContainers = s.allocations.With(prometheus.Labels{"state": "rejected"})
	s.schedulingErrors = s.allocations.With(prometheus.Labels{"state": "error"})
	s.releasedContainers = s.allocations.With(prometheus.Labels{"state": "released"})

	// apps
	s.scheduleApplications = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "submitted_apps_total",
			Help:      "Number of applications submitted, by the result.",
		}, []string{"result"})
	// ApplicationsPending counts how many apps are in pending state.
	s.totalApplicationsAdded = s.scheduleApplications.With(prometheus.Labels{"result": "added"})
	// ApplicationsSubmitted counts how many apps are submitted.
	s.totalApplicationsRejected = s.scheduleApplications.With(prometheus.Labels{"result": "rejected"})
	s.totalApplicationsRunning = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "running_apps",
			Help:      "active apps",
		})
	s.totalApplicationsCompleted = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "completed_apps",
			Help:      "completed apps",
		})

	// Nodes
	s.activeNodes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "active_nodes",
			Help:      "active nodes",
		})
	s.failedNodes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "failed_nodes",
			Help:      "failed nodes",
		})

	s.schedulingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "allocating_latency_seconds",
			Help:      "container allocating latency in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 6), //start from 0.1ms
		},
	)

	s.nodeSortingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: SchedulerSubsystem,
			Name:      "nodes_sorting_latency_seconds",
			Help:      "nodes sorting latency in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 10, 6), //start from 0.1ms
		},
	)
	var metricsList = []prometheus.Collector{
		s.allocations,
		s.scheduleApplications,
		s.schedulingLatency,
		s.nodeSortingLatency,
		s.totalApplicationsRunning,
		s.totalApplicationsCompleted,
		s.activeNodes,
		s.failedNodes,
	}

	// Register the metrics.
	for _, metric := range metricsList {
		if err:= prometheus.Register(metric); err != nil {
			log.Logger().Warn("failed to register metrics collector", zap.Error(err))
		}
	}

	return s
}

// Reset resets metrics
func Reset() {
	//SchedulingLatency.Reset()
}

// SinceInMicroseconds gets the time since the specified start in microseconds.
func SinceInMicroseconds(start time.Time) float64 {
	return float64(time.Since(start).Nanoseconds() / time.Microsecond.Nanoseconds())
}

// SinceInSeconds gets the time since the specified start in seconds.
func SinceInSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}

func (m *SchedulerMetrics) ObserveSchedulingLatency(start time.Time) {
	m.schedulingLatency.Observe(SinceInSeconds(start))
}

func (m *SchedulerMetrics) ObserveNodeSortingLatency(start time.Time) {
	m.nodeSortingLatency.Observe(SinceInSeconds(start))
}

// Define and implement all the metrics ops for Prometheus.
// Metrics Ops related to allocationScheduleSuccesses
func (m *SchedulerMetrics) IncAllocatedContainer() {
	m.allocatedContainers.Inc()
}

func (m *SchedulerMetrics) AddAllocatedContainers(value int) {
	m.allocatedContainers.Add(float64(value))
}

func (m *SchedulerMetrics) IncReleasedContainer() {
	m.releasedContainers.Inc()
}

func (m *SchedulerMetrics) AddReleasedContainers(value int) {
	m.releasedContainers.Add(float64(value))
}

// Metrics Ops related to allocationScheduleFailures
func (m *SchedulerMetrics) IncRejectedContainer() {
	m.rejectedContainers.Inc()
}

func (m *SchedulerMetrics) AddRejectedContainers(value int) {
	m.rejectedContainers.Add(float64(value))
}

// Metrics Ops related to allocationScheduleErrors
func (m *SchedulerMetrics) IncSchedulingError() {
	m.schedulingErrors.Inc()
}

func (m *SchedulerMetrics) AddSchedulingErrors(value int) {
	m.schedulingErrors.Add(float64(value))
}

// Metrics Ops related to totalApplicationsAdded
func (m *SchedulerMetrics) IncTotalApplicationsAdded() {
	m.totalApplicationsAdded.Inc()
}

func (m *SchedulerMetrics) AddTotalApplicationsAdded(value int) {
	m.totalApplicationsAdded.Add(float64(value))
}

// Metrics Ops related to totalApplicationsRejected
func (m *SchedulerMetrics) IncTotalApplicationsRejected() {
	m.totalApplicationsRejected.Inc()
}

func (m *SchedulerMetrics) AddTotalApplicationsRejected(value int) {
	m.totalApplicationsRejected.Add(float64(value))
}

// Metrics Ops related to totalApplicationsRunning
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

// Metrics Ops related to totalApplicationsCompleted
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

// Metrics Ops related to ActiveNodes
func (m *SchedulerMetrics) IncActiveNodes() {
	m.activeNodes.Inc()
}

func (m *SchedulerMetrics) AddActiveNodes(value int) {
	m.activeNodes.Add(float64(value))
}

func (m *SchedulerMetrics) DecActiveNodes() {
	m.activeNodes.Dec()
}

func (m *SchedulerMetrics) SubActiveNodes(value int) {
	m.activeNodes.Sub(float64(value))
}

func (m *SchedulerMetrics) SetActiveNodes(value int) {
	m.activeNodes.Set(float64(value))
}

// Metrics Ops related to failedNodes
func (m *SchedulerMetrics) IncFailedNodes() {
	m.failedNodes.Inc()
}

func (m *SchedulerMetrics) AddFailedNodes(value int) {
	m.failedNodes.Add(float64(value))
}

func (m *SchedulerMetrics) DecFailedNodes() {
	m.failedNodes.Dec()
}

func (m *SchedulerMetrics) SubFailedNodes(value int) {
	m.failedNodes.Sub(float64(value))
}

func (m *SchedulerMetrics) SetFailedNodes(value int) {
	m.failedNodes.Set(float64(value))
}

func (m *SchedulerMetrics) SetNodeResourceUsage(resourceName string, rangeIdx int, value float64) {
	m.lock.Lock()
	defer m.lock.Unlock()
	var resourceMetrics *prometheus.GaugeVec
	resourceMetrics, ok := m.nodesResourceUsages[resourceName]
	if !ok {
		metricsName := strings.Replace(fmt.Sprintf("%s_nodes_usage", resourceName), "-", "_", -1)
		resourceMetrics = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   Namespace,
				Subsystem:   SchedulerSubsystem,
				Name:        metricsName,
				Help:        "Nodes resource usage, by resource name.",
			}, []string{"range"})
		if err := prometheus.Register(resourceMetrics); err != nil {
			log.Logger().Warn("failed to register metrics collector", zap.Error(err))
			return
		}
		m.nodesResourceUsages[resourceName] = resourceMetrics
	}
	resourceMetrics.With(prometheus.Labels{"range": resourceUsageRangeBuckets[rangeIdx]}).Set(value)
}
