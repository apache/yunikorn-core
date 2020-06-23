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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
)

var pushEventInterval = 2 * time.Second

type EventPublisher interface {
	StartService()
	Stop()
}

type shimPublisher struct {
	store EventStore
	stop  bool

	sync.Mutex
}

func CreateShimPublisher(store EventStore) EventPublisher {
	return createShimPublisherInternal(store)
}

func createShimPublisherInternal(store EventStore) *shimPublisher {
	return &shimPublisher{
		store: store,
		stop:  false,
	}
}

func (sp *shimPublisher) isStopped() bool {
	sp.Lock()
	defer sp.Unlock()

	return sp.stop
}

func (sp *shimPublisher) StartService() {
	go func() {
		for {
			if sp.isStopped() {
				break
			}
			if eventPlugin := plugins.GetEventPlugin(); eventPlugin != nil {
				messages := sp.store.CollectEvents()
				if len(messages) > 0 {
					log.Logger().Debug("Sending eventChannel", zap.Int("number of messages", len(messages)))
					if err := eventPlugin.SendEvent(messages); err != nil && err.Error() != "" {
						log.Logger().Warn("Callback failed - could not sent EventMessage to shim",
							zap.Error(err), zap.Int("number of messages", len(messages)))
					}
				}
			}
			time.Sleep(pushEventInterval)
		}
	}()
}

func (sp *shimPublisher) Stop() {
	sp.Lock()
	defer sp.Unlock()

	sp.stop = true
}

func (sp *shimPublisher) getEventStore() EventStore {
	// only set in the constructor, no need to lock
	return sp.store
}
