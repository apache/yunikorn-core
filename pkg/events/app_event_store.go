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
	"strconv"
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

// TODO add more elements to this in the future
type AppEventStore struct {
	// last visited time of the app by the scheduler
	apps map[string]time.Time

	sync.RWMutex
}

func NewAppEventStore() *AppEventStore {
	aes := AppEventStore{}
	aes.apps = make(map[string]time.Time)
	return &aes
}


func (a AppEventStore) VisitApp(app string) {
	a.Lock()
	defer a.Unlock()
	a.apps[app] = time.Now()
}

func (a AppEventStore) GetAppLastVisitTime(app string) time.Time {
	a.RLock()
	defer a.RUnlock()
	return a.apps[app]
}

func (a AppEventStore) clearOldApps() {
	a.Lock()
	defer a.Unlock()
	num := 0

	limit := time.Now().Add(-1 * keepTime)
	for key, value := range a.apps {
		if limit.After(value) {
			delete(a.apps, key)
			num++
		}
	}
	msg := fmt.Sprintf("Deleted %s old application event.", strconv.Itoa(num))
	log.Logger().Info(msg)
}
