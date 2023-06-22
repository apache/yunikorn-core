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

// Package trace provides functions and constants for tracing.
package trace

import (
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-client-go/log/zap"
	"github.com/uber/jaeger-lib/metrics"

	"github.com/apache/yunikorn-core/pkg/log"
)

// NewConstTracer returns an instance of Jaeger Tracer that samples 100% or 0% of traces for test.
func NewConstTracer(serviceName string, collect bool) (opentracing.Tracer, io.Closer, error) {
	if serviceName == "" {
		return nil, nil, fmt.Errorf("service name is empty")
	}
	param := 0.0
	if collect {
		param = 1.0
	}
	// Sample configuration for testing. Use constant sampling to sample every trace
	// and enable LogSpan to log every span via configured Logger.
	cfg := jaegercfg.Configuration{
		ServiceName: serviceName,
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: param,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans: true,
		},
	}
	return cfg.NewTracer(
		jaegercfg.Logger(zap.NewLogger(log.Log(log.OpenTracing).Named(serviceName))),
		jaegercfg.Metrics(metrics.NullFactory),
	)
}

// NewTracerFromEnv returns an instance of Jaeger Tracer that get sampling strategy from env settings.
func NewTracerFromEnv(serviceName string) (opentracing.Tracer, io.Closer, error) {
	cfg, err := jaegercfg.FromEnv()
	if err != nil {
		return nil, nil, err
	}
	if serviceName != "" {
		cfg.ServiceName = serviceName
	}
	// Example logger and metrics factory. Use github.com/uber/jaeger-client-go/log
	// and github.com/uber/jaeger-lib/metrics respectively to bind to real logging and metrics
	// frameworks.
	// Initialize tracer with a logger and a metrics factory
	return cfg.NewTracer(
		jaegercfg.Logger(zap.NewLogger(log.Log(log.OpenTracing).Named(cfg.ServiceName))),
		jaegercfg.Metrics(metrics.NullFactory),
	)
}
