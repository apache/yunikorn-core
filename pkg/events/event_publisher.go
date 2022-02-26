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
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
)

// stores the push event internal
var defaultPushEventInterval = 2 * time.Second

type EventPublisher interface {
	StartService()
	Stop()
}

type shimPublisher struct {
	store             EventStore
	pushEventInterval time.Duration
	stop              atomic.Value
}

func CreateShimPublisher(store EventStore) EventPublisher {
	return createShimPublisherInternal(store)
}

func createShimPublisherInternal(store EventStore) *shimPublisher {
	return createShimPublisherWithParameters(store, defaultPushEventInterval)
}

func createShimPublisherWithParameters(store EventStore, pushEventInterval time.Duration) *shimPublisher {
	publisher := &shimPublisher{
		store:             store,
		pushEventInterval: pushEventInterval,
	}
	publisher.stop.Store(false)
	return publisher
}

func (sp *shimPublisher) StartService() {
	go func() {
		for {
			if sp.stop.Load().(bool) {
				break
			}
			messages := sp.store.CollectEvents()
			if len(messages) > 0 {
				if eventPlugin := plugins.GetResourceManagerCallbackPlugin(); eventPlugin != nil {
					log.Logger().Debug("Sending eventChannel", zap.Int("number of messages", len(messages)))
					eventPlugin.SendEvent(messages)
				}
			}
			time.Sleep(sp.pushEventInterval)
		}
	}()
}

func (sp *shimPublisher) Stop() {
	sp.stop.Store(true)
}

func (sp *shimPublisher) getEventStore() EventStore {
	return sp.store
}
