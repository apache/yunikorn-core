/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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

	"github.com/cloudera/yunikorn-core/pkg/metrics"
)

type nodesResourceUsageMonitor struct {
	done      chan bool
	ticker    *time.Ticker
	scheduler *Scheduler
}

func newNodesResourceUsageMonitor(scheduler *Scheduler) *nodesResourceUsageMonitor {
	return &nodesResourceUsageMonitor{
		done:      make(chan bool),
		ticker:    time.NewTicker(1 * time.Second),
		scheduler: scheduler,
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
	for _, p := range m.scheduler.GetClusterSchedulingContext().getPartitionMapClone() {
		usageMap := p.partition.CalculateNodesResourceUsage()
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
