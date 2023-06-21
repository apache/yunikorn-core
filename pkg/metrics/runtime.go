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
	"runtime"
	"runtime/metrics"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
)

const (
	MemStatsLabel = "MemStats"
	PauseNsLabel  = "PauseNs"
	PauseEndLabel = "PauseEnd"
	GenericLabel  = "Generic"
	Runtime       = "runtime"
)

var (
	// taken from sizeclasses.go - needed for bySize histogram buckets
	sizeBuckets = []float64{0, 8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320, 352, 384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024, 1152, 1280, 1408, 1536, 1792, 2048, 2304, 2688, 3072, 3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472, 9728, 10240, 10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576, 27264, 28672, 32768}
)

type RuntimeMetrics struct {
	*MStatsMetrics
	*GenericMetrics
}

func (a *RuntimeMetrics) Reset() {
	a.MStatsMetrics.Reset()
	a.GenericMetrics.Reset()
}

func (a *RuntimeMetrics) Collect() {
	a.MStatsMetrics.Collect()
	a.GenericMetrics.Collect()
}

type MStatsMetrics struct {
	mstats        *prometheus.GaugeVec
	pauseNsTimes  *prometheus.GaugeVec
	pauseEndTimes *prometheus.GaugeVec
	bySizeSize    prometheus.Histogram
	bySizeFree    prometheus.Histogram
	bySizeMalloc  prometheus.Histogram
}

func (ms *MStatsMetrics) Collect() {
	memStats := runtime.MemStats{}
	runtime.ReadMemStats(&memStats)

	ms.gauge("Alloc").Set(float64(memStats.Alloc))
	ms.gauge("TotalAlloc").Set(float64(memStats.TotalAlloc))
	ms.gauge("Sys").Set(float64(memStats.Sys))
	ms.gauge("Lookups").Set(float64(memStats.Lookups))
	ms.gauge("Mallocs").Set(float64(memStats.Mallocs))
	ms.gauge("Frees").Set(float64(memStats.Frees))
	ms.gauge("HeapAlloc").Set(float64(memStats.HeapAlloc))
	ms.gauge("HeapSys").Set(float64(memStats.HeapSys))
	ms.gauge("HeapIdle").Set(float64(memStats.HeapIdle))
	ms.gauge("HeapInuse").Set(float64(memStats.HeapInuse))
	ms.gauge("HeapReleased").Set(float64(memStats.HeapReleased))
	ms.gauge("HeapObjects").Set(float64(memStats.HeapObjects))
	ms.gauge("StackInuse").Set(float64(memStats.StackInuse))
	ms.gauge("StackSys").Set(float64(memStats.StackSys))
	ms.gauge("MSpanInuse").Set(float64(memStats.MSpanInuse))
	ms.gauge("MSpanSys").Set(float64(memStats.MSpanSys))
	ms.gauge("MCacheSys").Set(float64(memStats.MCacheSys))
	ms.gauge("BuckHashSys").Set(float64(memStats.BuckHashSys))
	ms.gauge("GCSys").Set(float64(memStats.GCSys))
	ms.gauge("OtherSys").Set(float64(memStats.OtherSys))
	ms.gauge("NextGC").Set(float64(memStats.NextGC))
	ms.gauge("LastGC").Set(float64(memStats.LastGC))
	ms.gauge("PauseTotalNs").Set(float64(memStats.PauseTotalNs))
	ms.gauge("NumGC").Set(float64(memStats.NumGC))
	ms.gauge("NumForcedGC").Set(float64(memStats.NumForcedGC))
	ms.gauge("GCCPUFraction").Set(memStats.GCCPUFraction)

	for i, v := range memStats.PauseNs {
		ms.pauseNsTimes.With(prometheus.Labels{PauseNsLabel: strconv.Itoa(i)}).Set(float64(v))
	}

	for i, v := range memStats.PauseEnd {
		ms.pauseEndTimes.With(prometheus.Labels{PauseEndLabel: strconv.Itoa(i)}).Set(float64(v))
	}

	for _, v := range memStats.BySize {
		ms.bySizeSize.Observe(float64(v.Size))
		ms.bySizeFree.Observe(float64(v.Frees))
		ms.bySizeMalloc.Observe(float64(v.Mallocs))
	}
}

func (ms *MStatsMetrics) gauge(name string) prometheus.Gauge {
	return ms.mstats.With(prometheus.Labels{MemStatsLabel: name})
}

func (ms *MStatsMetrics) Reset() {
	ms.mstats.Reset()
	ms.pauseNsTimes.Reset()
	ms.pauseEndTimes.Reset()
}

type GenericMetrics struct {
	genericMetrics *prometheus.GaugeVec
}

func (gm *GenericMetrics) Collect() {
	descs := metrics.All()

	samples := make([]metrics.Sample, len(descs))
	for i := range samples {
		samples[i].Name = descs[i].Name
	}

	metrics.Read(samples)

	for _, sample := range samples {
		name, value := sample.Name, sample.Value

		switch value.Kind() {
		case metrics.KindUint64:
			gm.gauge(name).Set(float64(value.Uint64()))
		case metrics.KindFloat64:
			gm.gauge(name).Set(value.Float64())
		case metrics.KindFloat64Histogram:
			// ignore
		case metrics.KindBad:
			// ignore
		default:
			// ignore
		}
	}
}

func (gm *GenericMetrics) Reset() {
	gm.genericMetrics.Reset()
}

func (gm *GenericMetrics) gauge(name string) prometheus.Gauge {
	formattedName := formatMetricName(name)
	return gm.genericMetrics.With(prometheus.Labels{GenericLabel: formattedName})
}

func initRuntimeMetrics() *RuntimeMetrics {
	msm := &MStatsMetrics{
		mstats: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Subsystem: Runtime,
				Name:      "go_mem_stats",
				Help:      "Go MemStats metrics",
			}, []string{MemStatsLabel}),
		pauseNsTimes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Subsystem: Runtime,
				Name:      "go_pause_ns",
				Help:      "Go MemStats - PauseNs",
			}, []string{PauseNsLabel}),
		pauseEndTimes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Subsystem: Runtime,
				Name:      "go_pause_end",
				Help:      "Go MemStats - PauseEnd",
			}, []string{PauseEndLabel}),
		bySizeSize: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: Namespace,
				Subsystem: Runtime,
				Name:      "go_alloc_bysize_maxsize",
				Help:      "Go MemStats - maximum byte size of this bucket",
				Buckets:   sizeBuckets,
			}),
		bySizeFree: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: Namespace,
				Subsystem: Runtime,
				Name:      "go_alloc_bysize_free",
				Help:      "Go MemStats - cumulative count of heap objects freed in this size class",
				Buckets:   sizeBuckets,
			}),
		bySizeMalloc: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: Namespace,
				Subsystem: Runtime,
				Name:      "go_alloc_bysize_malloc",
				Help:      "Go MemStats - cumulative count of heap objects allocated in this size bucket",
				Buckets:   sizeBuckets,
			}),
	}

	gen := &GenericMetrics{
		genericMetrics: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: Namespace,
				Subsystem: Runtime,
				Name:      "go_generic",
				Help:      "Go Generics metrics",
			}, []string{GenericLabel}),
	}

	rtm := &RuntimeMetrics{
		msm,
		gen,
	}

	var metricsList = []prometheus.Collector{
		msm.mstats,
		msm.pauseNsTimes,
		msm.pauseEndTimes,
		msm.bySizeMalloc,
		msm.bySizeSize,
		msm.bySizeFree,
		gen.genericMetrics,
	}

	for _, metric := range metricsList {
		if err := prometheus.Register(metric); err != nil {
			log.Log(log.Metrics).Warn("failed to register metrics collector", zap.Error(err))
		}
	}

	return rtm
}
