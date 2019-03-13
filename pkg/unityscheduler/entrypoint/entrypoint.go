package entrypoint

import (
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/cache"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/handler"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/rmproxy"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/scheduler"
)

func StartAllServices() (*rmproxy.RMProxy, *cache.ClusterInfo, *scheduler.Scheduler) {
    return startAllServicesWithParameters(false)
}

// Visible by tests
func StartAllServicesWithManualScheduler() (*rmproxy.RMProxy, *cache.ClusterInfo, *scheduler.Scheduler) {
    return startAllServicesWithParameters(true)
}

func startAllServicesWithParameters(manualSchedule bool) (*rmproxy.RMProxy, *cache.ClusterInfo, *scheduler.Scheduler) {
    cache := cache.NewClusterInfo()
    scheduler := scheduler.NewScheduler(cache)
    proxy := rmproxy.NewRMProxy()

    eventHandler := handler.EventHandlers{
        CacheEventHandler:     cache,
        SchedulerEventHandler: scheduler,
        RMProxyEventHandler:   proxy,
    }

    // start services
    cache.StartService(eventHandler)
    scheduler.StartService(eventHandler, manualSchedule)
    proxy.StartService(eventHandler)

    return proxy, cache, scheduler
}