/*
Copyright 2019 The Unity Scheduler Authors

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
	"sync"
	"time"
)

const (
	// SchedulerSubsystem - subsystem name used by scheduler
	SchedulerSubsystem = "yunikorn_scheduler_metrics"
	// SchedulingLatencyName - scheduler latency metric name
	SchedulingLatencyName = "scheduling_duration_seconds"
)

// All the histogram based metrics have 1ms as size for the smallest bucket.
var (
	scheduleAllocations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "schedule_attempts_total",
			Help:      "Number of attempts to schedule pods, by the result. 'unschedulable' means a pod could not be scheduled, while 'error' means an internal scheduler problem.",
		}, []string{"result"})
	// AllocationScheduleSuccesses counts how many pods were scheduled.
	AllocationScheduleSuccesses = scheduleAllocations.With(prometheus.Labels{"result": "scheduled"})
	// AllocationScheduleFailures counts how many pods could not be scheduled.
	AllocationScheduleFailures = scheduleAllocations.With(prometheus.Labels{"result": "unschedulable"})
	// AllocationScheduleErrors counts how many pods could not be scheduled due to a scheduler error.
	AllocationScheduleErrors = scheduleAllocations.With(prometheus.Labels{"result": "error"})
	scheduleApplications = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "submitted_apps_total",
			Help:      "Number of applications submitted, by the result.",
		}, []string{"result"})
	// ApplicationsPending counts how many apps are in pending state.
	TotalApplicationsAdded = scheduleAllocations.With(prometheus.Labels{"result": "added"})
	// ApplicationsSubmitted counts how many apps are submitted.
	TotalApplicationsRejected = scheduleAllocations.With(prometheus.Labels{"result": "rejected"})
	TotalApplicationsRunning = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "queue_running_apps",
			Help:      "active apps",
		})
	TotalApplicationsCompleted = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "queue_completed_apps",
			Help:      "completed apps",
		})

	// Nodes
	ActiveNodes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "active_nodes",
			Help:      "active nodes",
		})
	FailedNodes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "failed_nodes",
			Help:      "failed nodes",
		})

	SchedulingLatency = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Subsystem: SchedulerSubsystem,
			Name:      SchedulingLatencyName,
			Help:      "Scheduling latency in seconds split by sub-parts of the scheduling operation",
			// Make the sliding window of 5h.
			MaxAge: 5 * time.Hour,
		},
		[]string{"operation"},
	)

	metricsList = []prometheus.Collector{
		scheduleAllocations,
		scheduleApplications,
		SchedulingLatency,
		TotalApplicationsRunning,
		TotalApplicationsCompleted,
		ActiveNodes,
		FailedNodes,
	}
)

var registerMetrics sync.Once

// Register all metrics.
func Register() {
	// Register the metrics.
	registerMetrics.Do(func() {
		for _, metric := range metricsList {
			prometheus.MustRegister(metric)
		}
	})
}

// Reset resets metrics
func Reset() {
	SchedulingLatency.Reset()
}

// SinceInMicroseconds gets the time since the specified start in microseconds.
func SinceInMicroseconds(start time.Time) float64 {
	return float64(time.Since(start).Nanoseconds() / time.Microsecond.Nanoseconds())
}

// SinceInSeconds gets the time since the specified start in seconds.
func SinceInSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}