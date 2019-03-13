package entrypoint

import (
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/cache"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/handler"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/rmproxy"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/scheduler"
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