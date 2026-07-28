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
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type EventMetrics struct {
	totalEventsCreated      prometheus.Gauge
	totalEventsChanneled    prometheus.Gauge
	totalEventsNotChanneled prometheus.Gauge
	totalEventsDropped      prometheus.Gauge
	totalEventsProcessed    prometheus.Gauge
	totalEventsStored       prometheus.Gauge
	totalEventsNotStored    prometheus.Gauge
	totalEventsCollected    prometheus.Gauge
}

func initEventMetrics() *EventMetrics {
	metrics := &EventMetrics{}

	metrics.totalEventsCreated = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: EventSubsystem,
			Name:      "total_created",
			Help:      "total events created",
		})
	metrics.totalEventsChanneled = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: EventSubsystem,
			Name:      "total_channeled",
			Help:      "total events channeled",
		})
	metrics.totalEventsNotChanneled = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: EventSubsystem,
			Name:      "total_not_channeled",
			Help:      "total events not channeled",
		})
	metrics.totalEventsDropped = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: EventSubsystem,
			Name:      "total_dropped",
			Help:      "total events dropped",
		})
	metrics.totalEventsProcessed = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: EventSubsystem,
			Name:      "total_processed",
			Help:      "total events processed",
		})
	metrics.totalEventsStored = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: EventSubsystem,
			Name:      "total_stored",
			Help:      "total events stored",
		})
	metrics.totalEventsNotStored = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: EventSubsystem,
			Name:      "total_not_stored",
			Help:      "total events not stored",
		})
	metrics.totalEventsCollected = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: EventSubsystem,
			Name:      "total_collected",
			Help:      "total events collected",
		})

	return metrics
}

// Reset all metrics that implement the Set functionality.
// Should only be used in tests
func (em *EventMetrics) Reset() {
	em.totalEventsCollected.Set(0)
	em.totalEventsCreated.Set(0)
	em.totalEventsChanneled.Set(0)
	em.totalEventsNotChanneled.Set(0)
	em.totalEventsDropped.Set(0)
	em.totalEventsStored.Set(0)
	em.totalEventsNotStored.Set(0)
	em.totalEventsProcessed.Set(0)
}

func (em *EventMetrics) IncEventsCreated() {
	em.totalEventsCreated.Inc()
}

func (em *EventMetrics) IncEventsChanneled() {
	em.totalEventsChanneled.Inc()
}

func (em *EventMetrics) IncEventsNotChanneled() {
	em.totalEventsNotChanneled.Inc()
}

func (em *EventMetrics) IncEventsDropped() {
	em.totalEventsDropped.Inc()
}

func (em *EventMetrics) IncEventsProcessed() {
	em.totalEventsProcessed.Inc()
}

func (em *EventMetrics) IncEventsStored() {
	em.totalEventsStored.Inc()
}

func (em *EventMetrics) IncEventsNotStored() {
	em.totalEventsNotStored.Inc()
}

func (em *EventMetrics) AddEventsCollected(collectedEvents int) {
	em.totalEventsCollected.Add(float64(collectedEvents))
}

// Event system metrics

func (em *EventMetrics) GetEventsCreated() int {
	metricDto := &dto.Metric{}
	if err := em.totalEventsCreated.Write(metricDto); err == nil {
		return int(*metricDto.Gauge.Value)
	}
	return -1
}

func (em *EventMetrics) GetEventsChanneled() int {
	metricDto := &dto.Metric{}
	if err := em.totalEventsChanneled.Write(metricDto); err == nil {
		return int(*metricDto.Gauge.Value)
	}
	return -1
}

func (em *EventMetrics) GetEventsNotChanneled() int {
	metricDto := &dto.Metric{}
	if err := em.totalEventsNotChanneled.Write(metricDto); err == nil {
		return int(*metricDto.Gauge.Value)
	}
	return -1
}

func (em *EventMetrics) GetEventsDropped() int {
	metricDto := &dto.Metric{}
	if err := em.totalEventsDropped.Write(metricDto); err == nil {
		return int(*metricDto.Gauge.Value)
	}
	return -1
}

// Publisher metrics

func (em *EventMetrics) GetEventsProcessed() int {
	metricDto := &dto.Metric{}
	if err := em.totalEventsProcessed.Write(metricDto); err == nil {
		return int(*metricDto.Gauge.Value)
	}
	return -1
}

// Event store metrics

func (em *EventMetrics) GetEventsStored() int {
	metricDto := &dto.Metric{}
	if err := em.totalEventsStored.Write(metricDto); err == nil {
		return int(*metricDto.Gauge.Value)
	}
	return -1
}

func (em *EventMetrics) GetEventsNotStored() int {
	metricDto := &dto.Metric{}
	if err := em.totalEventsNotStored.Write(metricDto); err == nil {
		return int(*metricDto.Gauge.Value)
	}
	return -1
}

func (em *EventMetrics) GetEventsCollected() int {
	metricDto := &dto.Metric{}
	if err := em.totalEventsCollected.Write(metricDto); err == nil {
		return int(*metricDto.Gauge.Value)
	}
	return -1
}
