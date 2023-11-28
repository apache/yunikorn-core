package tracking

import (
	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
func (ur *TrackedResource) AggregateTrackedResource(resource *resources.Resource, releaseTime time.Time, message string) {
	ur.Lock()
	defer ur.Unlock()
	// The message is in the format of "instanceType:timestamp"
	// Split the message to get the instance type and the timestamp for bind time
	// Convert the string to an int64
	unixNano, err := strconv.ParseInt(strings.Split(message, common.Separator)[1], 10, 64)
	if err != nil {
		log.Log(log.Events).Warn("Failed to parse the timestamp", zap.Error(err), zap.String("message", message))
		return
	}

	// Convert Unix timestamp in nanoseconds to a time.Time object
	bindTime := time.Unix(0, unixNano)
	timeDiff := int64(releaseTime.Sub(bindTime).Seconds())
	instType := strings.Split(message, common.Separator)[0]
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
