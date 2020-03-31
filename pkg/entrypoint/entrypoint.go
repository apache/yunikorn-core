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
	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/handler"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics/history"
	"github.com/apache/incubator-yunikorn-core/pkg/rmproxy"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice"
)

// options used to control how services are started
type StartupOptions struct {
	manualScheduleFlag bool
	startWebAppFlag    bool
	metricsHistorySize int
}

func StartAllServices() *ServiceContext {
	log.Logger().Info("ServiceContext start all services")
	return startAllServicesWithParameters(
		StartupOptions{
			manualScheduleFlag: false,
			startWebAppFlag:    true,
			metricsHistorySize: 1440,
		})
}

// Visible by tests
func StartAllServicesWithManualScheduler() *ServiceContext {
	log.Logger().Info("ServiceContext start all services (manual scheduler)")
	return startAllServicesWithParameters(
		StartupOptions{
			manualScheduleFlag: true,
			startWebAppFlag:    false,
			metricsHistorySize: 0,
		})
}

func startAllServicesWithParameters(opts StartupOptions) *ServiceContext {
	cache := cache.NewClusterInfo()
	scheduler := scheduler.NewScheduler(cache)
	proxy := rmproxy.NewRMProxy()

	eventHandler := handler.EventHandlers{
		CacheEventHandler:     cache,
		SchedulerEventHandler: scheduler,
		RMProxyEventHandler:   proxy,
	}

	// start services
	log.Logger().Info("ServiceContext start scheduling services")
	cache.StartService(eventHandler)
	scheduler.StartService(eventHandler, opts.manualScheduleFlag)
	proxy.StartService(eventHandler)

	context := &ServiceContext{
		RMProxy:   proxy,
		Cache:     cache,
		Scheduler: scheduler,
	}

	imHistory := history.NewInternalMetricsHistory(opts.metricsHistorySize)
	if opts.metricsHistorySize != 0 {
		metricsCollector := metrics.NewInternalMetricsCollector(imHistory)
		metricsCollector.StartService()
	}

	if opts.startWebAppFlag {
		log.Logger().Info("ServiceContext start web application service")
		webapp := webservice.NewWebApp(cache, imHistory)
		webapp.StartWebApp()
		context.WebApp = webapp
	}

	return context
}
