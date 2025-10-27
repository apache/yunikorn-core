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

package objects

import "github.com/apache/yunikorn-core/pkg/locking"

// AppQueueMapping maintains a mapping between application IDs and their corresponding queues.
type AppQueueMapping struct {
	byAppID map[string]*Queue
	locking.RWMutex
}

func NewAppQueueMapping() *AppQueueMapping {
	return &AppQueueMapping{
		byAppID: make(map[string]*Queue),
	}
}

func (aqm *AppQueueMapping) AddAppQueueMapping(appID string, queue *Queue) {
	aqm.Lock()
	defer aqm.Unlock()
	aqm.byAppID[appID] = queue
}

func (aqm *AppQueueMapping) GetQueueByAppId(appID string) *Queue {
	aqm.RLock()
	defer aqm.RUnlock()
	return aqm.byAppID[appID]
}

func (aqm *AppQueueMapping) RemoveAppQueueMapping(appID string) {
	aqm.Lock()
	defer aqm.Unlock()
	delete(aqm.byAppID, appID)
}
