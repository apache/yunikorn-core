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

package events

import (
	"fmt"
	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/plugins"
)

// stores the push event internal
var defaultPushEventInterval = 1 * time.Second

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

type EventPublisher struct {
	store             *EventStore
	pushEventInterval time.Duration
	stop              atomic.Bool
	trackingAppMap    map[string]*TrackedResource // storing eventChannel
}

func CreateShimPublisher(store *EventStore) *EventPublisher {
	publisher := &EventPublisher{
		store:             store,
		pushEventInterval: defaultPushEventInterval,
		trackingAppMap:    make(map[string]*TrackedResource),
	}
	publisher.stop.Store(false)
	return publisher
}

func (sp *EventPublisher) StartService() {
	go func() {
		for {
			if sp.stop.Load() {
				break
			}
			messages := sp.store.CollectEvents()
			if len(messages) > 0 {
				if eventPlugin := plugins.GetResourceManagerCallbackPlugin(); eventPlugin != nil {
					log.Log(log.Events).Debug("Sending eventChannel", zap.Int("number of messages", len(messages)))
					eventPlugin.SendEvent(messages)
				}
				for _, message := range messages {
					if message.Type == si.EventRecord_APP && message.EventChangeType == si.EventRecord_REMOVE {
						log.Log(log.Events).Debug("aggregate resource usage", zap.String("message", fmt.Sprintf("%+v", message)))
						// We need to clean up the trackingAppMap when an application is removed
						if message.ReferenceID == "" {
							log.Log(log.Events).Info("YK_APP_SUMMARY:",
								zap.String("appID", message.ObjectID),
								zap.Any("resourceUsage", sp.trackingAppMap[message.ObjectID].TrackedResourceMap),
							)
							// This is an application removal event, remove the application from the trackingAppMap
							delete(sp.trackingAppMap, message.ObjectID)
						} else {
							// This is an allocation removal event, aggregate the resources used by the allocation
							if _, ok := sp.trackingAppMap[message.ObjectID]; !ok {
								sp.trackingAppMap[message.ObjectID] = &TrackedResource{
									TrackedResourceMap: make(map[string]map[string]int64),
								}
							}
							sp.trackingAppMap[message.ObjectID].AggregateTrackedResource(resources.NewResourceFromProto(message.Resource), time.Unix(0, message.TimestampNano), message.Message)
							//log.Log(log.Events).Info("YK_APP_SUMMARY:",
							//	zap.String("appID", message.ObjectID),
							//	zap.Any("resourceUsage", sp.trackingAppMap[message.ObjectID].TrackedResourceMap),
							//)
						}
					}
				}
			}
			time.Sleep(sp.pushEventInterval)
		}
	}()
}

func (sp *EventPublisher) Stop() {
	sp.stop.Store(true)
}

func (sp *EventPublisher) getEventStore() *EventStore {
	return sp.store
}
