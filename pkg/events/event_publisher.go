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
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/plugins"
)

// stores the push event internal
var defaultPushEventInterval = 1 * time.Second

type EventPublisher struct {
	store             *EventStore
	pushEventInterval time.Duration
	stop              atomic.Bool
	trackingAppMap    map[string]*resources.TrackedResource // storing eventChannel
}

func CreateShimPublisher(store *EventStore) *EventPublisher {
	publisher := &EventPublisher{
		store:             store,
		pushEventInterval: defaultPushEventInterval,
		trackingAppMap:    make(map[string]*resources.TrackedResource),
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
				sp.AggregateAppTrackedResourceFromEvents(messages)
			}
			time.Sleep(sp.pushEventInterval)
		}
	}()
}

func (sp *EventPublisher) Stop() {
	sp.stop.Store(true)
}

func (sp *EventPublisher) AggregateAppTrackedResourceFromEvents(messages []*si.EventRecord) {
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
					sp.trackingAppMap[message.ObjectID] = &resources.TrackedResource{
						TrackedResourceMap: make(map[string]map[string]int64),
					}
				}

				// The message is in the format of "instanceType:timestamp"
				// Split the message to get the instance type and the timestamp for bind time
				// Convert the string to an int64
				unixNano, err := strconv.ParseInt(strings.Split(message.Message, common.Separator)[1], 10, 64)
				if err != nil {
					log.Log(log.Events).Warn("Failed to parse the timestamp", zap.Error(err), zap.String("message", message.Message))
					return
				}
				instType := strings.Split(message.Message, common.Separator)[0]
				// Convert Unix timestamp in nanoseconds to a time.Time object
				bindTime := time.Unix(0, unixNano)
				sp.trackingAppMap[message.ObjectID].AggregateTrackedResource(instType, resources.NewResourceFromProto(message.Resource), bindTime, time.Unix(0, message.TimestampNano))
			}
		}
	}
}

func (sp *EventPublisher) getEventStore() *EventStore {
	return sp.store
}
