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

import "github.com/prometheus/client_golang/prometheus"

type eventMetrics struct {
	totalEventsCreated      prometheus.Gauge
	totalEventsChanneled    prometheus.Gauge
	totalEventsNotChanneled prometheus.Gauge
	totalEventsProcessed    prometheus.Gauge
	totalEventsStored       prometheus.Gauge
	totalEventsNotStored    prometheus.Gauge
	totalEventsCollected    prometheus.Gauge
}

func initEventMetrics() CoreEventMetrics {
	metrics := &eventMetrics{}

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
			Name:      "total_processed",
			Help:      "total events processed",
		})
	metrics.totalEventsNotStored = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: EventSubsystem,
			Name:      "total_processed",
			Help:      "total events processed",
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

func (em *eventMetrics) Reset() {
	em.totalEventsCollected.Set(0)
	em.totalEventsCreated.Set(0)
	em.totalEventsChanneled.Set(0)
	em.totalEventsNotChanneled.Set(0)
	em.totalEventsStored.Set(0)
	em.totalEventsNotStored.Set(0)
	em.totalEventsProcessed.Set(0)
}

func (em *eventMetrics) IncEventsCreated() {
	em.totalEventsCreated.Inc()
}

func (em *eventMetrics) IncEventsChanneled() {
	em.totalEventsChanneled.Inc()
}

func (em *eventMetrics) IncEventsNotChanneled() {
	em.totalEventsNotChanneled.Inc()
}

func (em *eventMetrics) IncEventsProcessed() {
	em.totalEventsProcessed.Inc()
}

func (em *eventMetrics) IncEventsStored() {
	em.totalEventsStored.Inc()
}

func (em *eventMetrics) IncEventsNotStored() {
	em.totalEventsNotStored.Inc()
}

func (em *eventMetrics) AddEventsCollected(collectedEvents int) {
	em.totalEventsCollected.Add(float64(collectedEvents))
}
