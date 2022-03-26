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

package entrypoint

import (
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/handler"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/metrics/history"
	"github.com/apache/yunikorn-core/pkg/rmproxy"
	"github.com/apache/yunikorn-core/pkg/scheduler"
	"github.com/apache/yunikorn-core/pkg/webservice"
)

// options used to control how services are started
type startupOptions struct {
	manualScheduleFlag bool
	startWebAppFlag    bool
	metricsHistorySize int
	eventCacheEnabled  bool
}

func StartAllServices() *ServiceContext {
	log.Logger().Info("ServiceContext start all services")
	return startAllServicesWithParameters(
		startupOptions{
			manualScheduleFlag: false,
			startWebAppFlag:    true,
			metricsHistorySize: 1440,
			eventCacheEnabled:  false,
		})
}

func StartAllServicesWithLogger(logger *zap.Logger, zapConfigs *zap.Config) *ServiceContext {
	log.InitializeLogger(logger, zapConfigs)
	return StartAllServices()
}

// Visible by tests
func StartAllServicesWithManualScheduler() *ServiceContext {
	log.Logger().Info("ServiceContext start all services (manual scheduler)")
	return startAllServicesWithParameters(
		startupOptions{
			manualScheduleFlag: true,
			startWebAppFlag:    false,
			metricsHistorySize: 0,
			eventCacheEnabled:  false,
		})
}

func startAllServicesWithParameters(opts startupOptions) *ServiceContext {
	var eventCache *events.EventCache
	var eventPublisher events.EventPublisher
	if opts.eventCacheEnabled {
		log.Logger().Info("creating event cache")
		events.CreateAndSetEventCache()
		eventCache = events.GetEventCache()
		eventPublisher = events.CreateShimPublisher(eventCache.Store)
	}

	sched := scheduler.NewScheduler()
	proxy := rmproxy.NewRMProxy()

	eventHandler := handler.EventHandlers{
		SchedulerEventHandler: sched,
		RMProxyEventHandler:   proxy,
	}

	// start services
	log.Logger().Info("ServiceContext start scheduling services")
	sched.StartService(eventHandler, opts.manualScheduleFlag)
	proxy.StartService(eventHandler)
	if opts.eventCacheEnabled && eventCache != nil {
		eventCache.StartService()
		if eventPublisher != nil {
			eventPublisher.StartService()
		}
	}

	context := &ServiceContext{
		RMProxy:   proxy,
		Scheduler: sched,
	}

	var imHistory *history.InternalMetricsHistory
	if opts.metricsHistorySize != 0 {
		log.Logger().Info("creating InternalMetricsHistory")
		imHistory = history.NewInternalMetricsHistory(opts.metricsHistorySize)
		metricsCollector := metrics.NewInternalMetricsCollector(imHistory)
		metricsCollector.StartService()
	}

	if opts.startWebAppFlag {
		log.Logger().Info("ServiceContext start web application service")
		webapp := webservice.NewWebApp(sched.GetClusterContext(), imHistory)
		webapp.StartWebApp()
		context.WebApp = webapp
	}

	return context
}
