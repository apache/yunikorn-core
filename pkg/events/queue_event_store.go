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
type QueueEventStore struct {
	// last visited time of the queue by the scheduler
	queues map[string]time.Time

	sync.RWMutex
}

func NewQueueEventStore() *QueueEventStore {
	qes := QueueEventStore{}
	qes.queues = make(map[string]time.Time)
	return &qes
}

func (q QueueEventStore) VisitQueue(queue string) {
	q.Lock()
	defer q.Unlock()
	q.queues[queue] = time.Now()
}

func (q QueueEventStore) GetQueueLastVisitTime(queue string) time.Time {
	q.RLock()
	defer q.RUnlock()
	return q.queues[queue]
}

func (q QueueEventStore) clearOldQueues() {
	q.Lock()
	defer q.Unlock()

	num := 0
	limit := time.Now().Add(-1 * keepTime)
	for key, value := range q.queues {
		if limit.After(value) {
			delete(q.queues, key)
		}
	}
	msg := fmt.Sprintf("Deleted %s old queue event.", strconv.Itoa(num))
	log.Logger().Info(msg)
}
