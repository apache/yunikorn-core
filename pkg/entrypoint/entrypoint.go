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

package entrypoint

import (
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/cache"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/handler"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/rmproxy"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/scheduler"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/webservice"
)

func StartAllServices() (*rmproxy.RMProxy, *cache.ClusterInfo, *scheduler.Scheduler) {
    return startAllServicesWithParameters(false)
}

// Visible by tests
func StartAllServicesWithManualScheduler() (*rmproxy.RMProxy, *cache.ClusterInfo, *scheduler.Scheduler) {
    return startAllServicesWithParameters(true)
}

func startAllServicesWithParameters(manualSchedule bool) (*rmproxy.RMProxy, *cache.ClusterInfo, *scheduler.Scheduler) {
    cache, metrics := cache.NewClusterInfo()
    scheduler := scheduler.NewScheduler(cache, metrics)
    proxy := rmproxy.NewRMProxy()
    webapp := webservice.NewWebApp(cache)

    eventHandler := handler.EventHandlers{
        CacheEventHandler:     cache,
        SchedulerEventHandler: scheduler,
        RMProxyEventHandler:   proxy,
    }

    // start services
    cache.StartService(eventHandler)
    scheduler.StartService(eventHandler, manualSchedule)
    proxy.StartService(eventHandler)
    webapp.StartWebApp()

    return proxy, cache, scheduler
}