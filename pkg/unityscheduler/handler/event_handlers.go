package handler

import (
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/common/commonevents"
)

type EventHandlers struct {
    RMProxyEventHandler   commonevents.EventHandler
    CacheEventHandler     commonevents.EventHandler
    SchedulerEventHandler commonevents.EventHandler
}
