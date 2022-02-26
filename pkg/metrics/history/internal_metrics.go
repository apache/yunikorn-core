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

	// internal implementation of limited array
	pointer int

	sync.RWMutex
}

type MetricsRecord struct {
	Timestamp         time.Time
	TotalApplications int
	TotalContainers   int
}

func NewInternalMetricsHistory(limit int) *InternalMetricsHistory {
	return &InternalMetricsHistory{
		records: make([]*MetricsRecord, limit),
		limit:   limit,
	}
}

func (h *InternalMetricsHistory) Store(totalApplications, totalContainers int) {
	h.Lock()
	defer h.Unlock()

	h.records[h.pointer] = &MetricsRecord{
		time.Now(),
		totalApplications,
		totalContainers,
	}
	h.pointer++
	if h.pointer == h.limit {
		h.pointer = 0
	}
}

// contract: the non-nil values are ordered by the time of addition
// may contains nil values, those should be handled (filtered) on the caller's side
func (h *InternalMetricsHistory) GetRecords() []*MetricsRecord {
	h.RLock()
	defer h.RUnlock()

	returnRecords := make([]*MetricsRecord, h.limit-h.pointer)
	copy(returnRecords, h.records[h.pointer:])
	returnRecords = append(returnRecords, h.records[:h.pointer]...)
	return returnRecords
}

func (h *InternalMetricsHistory) GetLimit() int {
	h.RLock()
	defer h.RUnlock()
	return h.limit
}
