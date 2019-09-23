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
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

const (
	// SchedulerSubsystem - subsystem name used by scheduler
	SchedulerSubsystem = "yunikorn_scheduler_metrics"
	// SchedulingLatencyName - scheduler latency metric name
	SchedulingLatencyName = "scheduling_duration_seconds"
)

// Declare all core metrics ops in this interface
type CoreSchedulerMetrics interface {
	// Metrics Ops related to ScheduledAllocationSuccesses
	IncScheduledAllocationSuccesses()
	AddScheduledAllocationSuccesses(value int)

	// Metrics Ops related to ScheduledAllocationFailures
	IncScheduledAllocationFailures()
	AddScheduledAllocationFailures(value int)

	// Metrics Ops related to ScheduledAllocationErrors
	IncScheduledAllocationErrors()
	AddScheduledAllocationErrors(value int)

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

	//latency change
	ObserveSchedulingLatency(start time.Time)
	ObserveNodeSortingLatency(start time.Time)
}

// All core metrics variables to be declared in this struct
type SchedulerMetrics struct  {
	scheduleAllocations  *prometheus.CounterVec
	allocationScheduleSuccesses prometheus.Counter
	allocationScheduleFailures prometheus.Counter
	allocationScheduleErrors prometheus.Counter
	scheduleApplications *prometheus.CounterVec
	totalApplicationsAdded prometheus.Counter
	totalApplicationsRejected prometheus.Counter
	totalApplicationsRunning prometheus.Gauge
	totalApplicationsCompleted prometheus.Gauge
	activeNodes prometheus.Gauge
	failedNodes prometheus.Gauge
	schedulingLatency prometheus.Histogram
	nodeSortingLatency prometheus.Histogram
}

// Initialize scheduler metrics
func initSchedulerMetrics() *SchedulerMetrics {
	s := &SchedulerMetrics{}
	s.scheduleAllocations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "schedule_attempts_total",
			Help:      "Number of attempts to schedule pods, by the result. 'unschedulable' means a pod could not be scheduled, while 'error' means an internal scheduler problem.",
		}, []string{"result"})
	// AllocationScheduleSuccesses counts how many pods were scheduled.
	s.allocationScheduleSuccesses = s.scheduleAllocations.With(prometheus.Labels{"result": "scheduled"})
	// AllocationScheduleFailures counts how many pods could not be scheduled.
	s.allocationScheduleFailures = s.scheduleAllocations.With(prometheus.Labels{"result": "unschedulable"})
	// AllocationScheduleErrors counts how many pods could not be scheduled due to a scheduler error.
	s.allocationScheduleErrors = s.scheduleAllocations.With(prometheus.Labels{"result": "error"})
	s.scheduleApplications = prometheus.NewCounterVec(
		prometheus.CounterOpts{
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
			Subsystem: SchedulerSubsystem,
			Name:      "running_apps",
			Help:      "active apps",
		})
	s.totalApplicationsCompleted = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "completed_apps",
			Help:      "completed apps",
		})

	// Nodes
	s.activeNodes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "active_nodes",
			Help:      "active nodes",
		})
	s.failedNodes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "failed_nodes",
			Help:      "failed nodes",
		})

	s.schedulingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "scheduling_latency_seconds",
			Help:      "scheduling latency in seconds",
			Buckets:   prometheus.ExponentialBuckets(2, 2, 15),
		},
	)

	s.nodeSortingLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "nodes_sorting_latency_ms",
			Help:      "nodes sorting latency in microseconds",
			Buckets:   prometheus.ExponentialBuckets(2, 2, 15),
		},
	)
	var metricsList = []prometheus.Collector{
		s.scheduleAllocations,
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
		prometheus.MustRegister(metric)
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

func SinceInMilliseconds(start time.Time) float64 {
	return float64(time.Since(start).Nanoseconds() / time.Millisecond.Nanoseconds())
}


// SinceInSeconds gets the time since the specified start in seconds.
func SinceInSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}

func (m *SchedulerMetrics) ObserveSchedulingLatency(start time.Time) {
	m.schedulingLatency.Observe(SinceInMilliseconds(start))
}

func (m *SchedulerMetrics) ObserveNodeSortingLatency(start time.Time) {
	m.nodeSortingLatency.Observe(SinceInMilliseconds(start))
}

// Define and implement all the metrics ops for Prometheus.
// Metrics Ops related to allocationScheduleSuccesses
func (m *SchedulerMetrics) IncScheduledAllocationSuccesses() {
	m.allocationScheduleSuccesses.Inc()
}

func (m *SchedulerMetrics) AddScheduledAllocationSuccesses(value int) {
	m.allocationScheduleSuccesses.Add(float64(value))
}

// Metrics Ops related to allocationScheduleFailures
func (m *SchedulerMetrics) IncScheduledAllocationFailures() {
	m.allocationScheduleFailures.Inc()
}

func (m *SchedulerMetrics) AddScheduledAllocationFailures(value int) {
	m.allocationScheduleFailures.Add(float64(value))
}

// Metrics Ops related to allocationScheduleErrors
func (m *SchedulerMetrics) IncScheduledAllocationErrors() {
	m.allocationScheduleErrors.Inc()
}

func (m *SchedulerMetrics) AddScheduledAllocationErrors(value int) {
	m.allocationScheduleErrors.Add(float64(value))
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
