package tracking

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
	"go.uber.org/atomic"
	"sync"
	"time"
)

var resourceTracker ResourceTracker

// Util struct to keep track of application resource usage
type TrackedResource struct {
	// Two level map for aggregated resource usage
	// With instance type being the top level key, the mapped value is a map:
	//   resource type (CPU, memory etc) -> the aggregated used time (in seconds) of the resource type
	//
	TrackedResourceMap map[string]map[string]int64

	sync.RWMutex
}

// Aggregate the resource usage to UsedResourceMap[instType]
// The time the given resource used is the delta between the resource createTime and currentTime
func (ur *TrackedResource) AggregateTrackedResource(instType string,
	resource *resources.Resource, releaseTime time.Time, bindTime time.Time) {
	ur.Lock()
	defer ur.Unlock()

	timeDiff := int64(releaseTime.Sub(bindTime).Seconds())
	aggregatedResourceTime, ok := ur.TrackedResourceMap[instType]
	if !ok {
		aggregatedResourceTime = map[string]int64{}
	}
	for key, element := range resource.Resources {
		curUsage, ok := aggregatedResourceTime[key]
		if !ok {
			curUsage = 0
		}
		curUsage += int64(element) * timeDiff // resource size times timeDiff
		aggregatedResourceTime[key] = curUsage
	}
	ur.TrackedResourceMap[instType] = aggregatedResourceTime
}

type ResourceTracker interface {
	AddEvent(event *si.EventRecord)
	StartService()
	Stop()
	IsResourceTrackerEnabled() bool
	GetEventsFromID(uint64, uint64) ([]*si.EventRecord, uint64, uint64)
}

type ResourceTrackerImpl struct {
	store          *events.EventStore          // storing eventChannel
	trackingAppMap map[string]*TrackedResource // storing eventChannel
	channel        chan *si.EventRecord        // channelling input eventChannel
	stop           atomic.Bool
	stopped        bool

	trackingEnabled  bool
	trackingInterval time.Duration
	sync.RWMutex
}

func (rt *ResourceTrackerImpl) StartService() {
	go func() {
		for {
			if rt.stop.Load() {
				break
			}
			messages := rt.store.CollectEvents()
			if len(messages) > 0 {
				for _, message := range messages {
					if message.Type == si.EventRecord_APP {
						if _, ok := rt.trackingAppMap[message.ObjectID]; !ok {
							rt.trackingAppMap[message.ObjectID] = &TrackedResource{
								TrackedResourceMap: make(map[string]map[string]int64),
							}
						} else {
							rt.trackingAppMap[message.ObjectID].AggregateTrackedResource("", resources.NewResourceFromProto(message.Resource), time.Unix(0, message.TimestampNano), time.Unix(0, strings.)
						}
					}
				}
			}
			time.Sleep(rt.trackingInterval)
		}
	}()
}
