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

package metrics

import (
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics/history"
)

// collecting metrics for YuniKorn-internal usage
// will fill missing values with -1, in case of failures
type internalMetricsCollector struct {
	ticker         *time.Ticker
	stopped        chan bool
	metricsHistory *history.InternalMetricsHistory
}

func NewInternalMetricsCollector(hcInfo *history.InternalMetricsHistory) *internalMetricsCollector {
	return newInternalMetricsCollector(hcInfo, 1*time.Minute)
}

// create a internalMetricsCollector with specify tick duration.
func newInternalMetricsCollector(hcInfo *history.InternalMetricsHistory, tickerDefault time.Duration) *internalMetricsCollector {
	finished := make(chan bool)
	ticker := time.NewTicker(tickerDefault)

	return &internalMetricsCollector{
		ticker,
		finished,
		hcInfo,
	}
}

func (u *internalMetricsCollector) StartService() {
	go func() {
		for {
			select {
			case <-u.stopped:
				return
			case <-u.ticker.C:
				u.store()
			}
		}
	}()
}

func (u *internalMetricsCollector) store() {
	log.Log(log.Metrics).Debug("Adding current status to historical partition data")

	totalAppsRunning, err := m.scheduler.getTotalApplicationsRunning()
	if err != nil {
		log.Log(log.Metrics).Warn("Could not encode totalApplications metric.", zap.Error(err))
		totalAppsRunning = -1
	}
	allocatedContainers, err := m.scheduler.getAllocatedContainers()
	if err != nil {
		log.Log(log.Metrics).Warn("Could not encode allocatedContainers metric.", zap.Error(err))
	}
	releasedContainers, err := m.scheduler.getReleasedContainers()
	if err != nil {
		log.Log(log.Metrics).Warn("Could not encode releasedContainers metric.", zap.Error(err))
	}
	totalContainersRunning := allocatedContainers - releasedContainers
	if totalContainersRunning < 0 {
		log.Log(log.Metrics).Warn("Could not calculate the totalContainersRunning.",
			zap.Int("allocatedContainers", allocatedContainers),
			zap.Int("releasedContainers", releasedContainers))
	}
	u.metricsHistory.Store(totalAppsRunning, totalContainersRunning)
}

func (u *internalMetricsCollector) Stop() {
	u.stopped <- true
}
