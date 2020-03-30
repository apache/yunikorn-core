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

package history

import (
	"sync"
	"time"
)

// This class collects basic information about the cluster
// for the web UI's front page.
// For more detailed metrics collection use Prometheus.
type InternalMetricsHistory struct {
	records []*MetricsRecord
	limit   int
	mutex   sync.Mutex
}

type MetricsRecord struct {
	Timestamp         time.Time
	TotalApplications int
	TotalContainers   int
}

func NewInternalMetricsHistory(limit int) *InternalMetricsHistory {
	return &InternalMetricsHistory{
		records: make([]*MetricsRecord, 0),
		limit:   limit,
	}
}

func (h *InternalMetricsHistory) Store(totalApplications, totalContainers int) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.records = append(h.records,
		&MetricsRecord{
			time.Now(),
			totalApplications,
			totalContainers,
		})
	if len(h.records) > h.limit {
		// remove oldest entry
		h.records = h.records[1:]
	}
}

func (h *InternalMetricsHistory) GetRecords() []*MetricsRecord {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.records
}

func (h *InternalMetricsHistory) GetLimit() int {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.limit
}
