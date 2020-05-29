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
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"go.uber.org/zap"
)

type EventPublisher interface {
	StartService()
	Stop()
	GetEventStore() EventStore
}

type shimPublisher struct {
	store EventStore
	stopped bool
}

func newShimPublisher(event EventStore) EventPublisher {
	return shimPublisher{
		event,
		false,
	}
}

func (sp shimPublisher) StartService() {
	go func () {
		for {
			if sp.stopped {
				break
			}
			if eventPlugin := plugins.GetEventPlugin(); eventPlugin != nil {
				messages := sp.store.CollectEvents()
				if err := eventPlugin.SendEvent(messages); err != nil && err.Error() != "" {
					log.Logger().Warn("Callback failed - could not sent EventMessage to shim",
						zap.Error(err), zap.Int("number of messages", len(messages)))
				}
			}
			time.Sleep(pushEventInterval)
		}
	}()
}

func (sp shimPublisher) Stop() {
	if sp.stopped {
		panic("could not stop shimPublisher service: already stopped")
	}
	sp.stopped = true
}

func (sp shimPublisher) GetEventStore() EventStore {
	return sp.store
}