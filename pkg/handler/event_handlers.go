package handler

import (
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/commonevents"
)

type EventHandlers struct {
    RMProxyEventHandler   commonevents.EventHandler
    CacheEventHandler     commonevents.EventHandler
    SchedulerEventHandler commonevents.EventHandler
}
