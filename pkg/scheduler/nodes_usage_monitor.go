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

package scheduler

import (
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
)

type nodesResourceUsageMonitor struct {
	done              chan bool
	ticker            *time.Ticker
	schedulingContext *ClusterSchedulingContext
}

func newNodesResourceUsageMonitor(scheduler *ClusterSchedulingContext) *nodesResourceUsageMonitor {
	return &nodesResourceUsageMonitor{
		done:              make(chan bool),
		ticker:            time.NewTicker(1 * time.Second),
		schedulingContext: scheduler,
	}
}

func (m *nodesResourceUsageMonitor) start() {
	go func() {
		for {
			select {
			case <-m.done:
				m.ticker.Stop()
				return
			case <-m.ticker.C:
				m.runOnce()
			}
		}
	}()
}

func (m *nodesResourceUsageMonitor) runOnce() {
	for _, p := range m.schedulingContext.GetPartitionMapClone() {
		usageMap := p.CalculateNodesResourceUsage()
		if len(usageMap) > 0 {
			for resourceName, usageBuckets := range usageMap {
				for idx, bucketValue := range usageBuckets {
					metrics.GetSchedulerMetrics().SetNodeResourceUsage(resourceName, idx, float64(bucketValue))
				}
			}
		}
	}
}

// Stop the node usage monitor.
//nolint:unused
func (m *nodesResourceUsageMonitor) stop() {
	m.done <- true
}
