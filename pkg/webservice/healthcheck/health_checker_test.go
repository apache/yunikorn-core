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

package healthcheck

import (
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
)

const configDefault = `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: default
            resources:
              guaranteed: 
                memory: 100000
                vcore: 10000
              max:
                memory: 100000
                vcore: 10000
`

func TestGetSchedulerHealthStatusMetrics(t *testing.T) {
	configs.MockSchedulerConfigByData([]byte(configDefault))
	schedulerMetrics := metrics.GetSchedulerMetrics()
	schedulerContext, err := scheduler.NewClusterContext("rmID", "policyGroup")
	assert.NilError(t, err, "Error when load schedulerContext from config")
	// everything OK
	healthInfo := GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	assert.Assert(t, healthInfo.Healthy, "Scheduler should be healthy")

	// Add some failed nodes
	schedulerMetrics.IncFailedNodes()
	healthInfo = GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	assert.Assert(t, !healthInfo.Healthy, "Scheduler should not be healthy")

	// decrease the failed nodes
	schedulerMetrics.DecFailedNodes()
	healthInfo = GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	assert.Assert(t, healthInfo.Healthy, "Scheduler should be healthy again")

	// insert some scheduling errors
	schedulerMetrics.IncSchedulingError()
	healthInfo = GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	assert.Assert(t, !healthInfo.Healthy, "Scheduler should not be healthy")
}
