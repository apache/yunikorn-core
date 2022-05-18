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
	"fmt"
	"testing"
	"time"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"

	"gotest.tools/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/metrics"
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

const configHealthCheck = `
partitions:
  - name: default
    queues:
      - name: root
    healthcheck:
      enabled: %s
      interval: 99s
`

func TestNewHealthChecker(t *testing.T) {
	configs.MockSchedulerConfigByData([]byte(configDefault))
	schedulerContext, err := NewClusterContext("rmID", "policyGroup")
	assert.NilError(t, err, "Error when load schedulerContext from config")
	assert.Assert(t, schedulerContext.lastHealthCheckResult == nil, "lastHealthCheckResult should be nil initially")

	c := NewHealthChecker(schedulerContext)
	assert.Assert(t, c != nil, "HealthChecker shouldn't be nil")

	assert.Assert(t, c.enabled, "HealthChecker shouldn't be enabled")

	assert.Assert(t, c.interval == defaultInterval, "HealthChecker should have default interval")
}

func TestNewHealthCheckerCustom(t *testing.T) {
	configs.MockSchedulerConfigByData([]byte(fmt.Sprintf(configHealthCheck, "true")))
	schedulerContext, err := NewClusterContext("rmID", "policyGroup")
	assert.NilError(t, err, "Error when load schedulerContext from config")
	assert.Assert(t, schedulerContext.lastHealthCheckResult == nil, "lastHealthCheckResult should be nil initially")

	expectedPeriod, err := time.ParseDuration("99s")
	assert.NilError(t, err, "failed to parse expected interval duration")

	c := NewHealthChecker(schedulerContext)
	assert.Assert(t, c != nil, "HealthChecker shouldn't be nil")

	assert.Assert(t, c.enabled, "HealthChecker shouldn't be enabled")

	assert.Assert(t, c.interval == expectedPeriod, "HealthChecker should have custom interval")
}

func TestNewHealthCheckerDisabled(t *testing.T) {
	configs.MockSchedulerConfigByData([]byte(fmt.Sprintf(configHealthCheck, "false")))
	schedulerContext, err := NewClusterContext("rmID", "policyGroup")
	assert.NilError(t, err, "Error when load schedulerContext from config")
	assert.Assert(t, schedulerContext.lastHealthCheckResult == nil, "lastHealthCheckResult should be nil initially")

	expectedPeriod, err := time.ParseDuration("99s")
	assert.NilError(t, err, "failed to parse expected interval duration")

	c := NewHealthChecker(schedulerContext)
	assert.Assert(t, c != nil, "HealthChecker shouldn't be nil")

	assert.Assert(t, !c.enabled, "HealthChecker should be disabled")

	assert.Assert(t, c.interval == expectedPeriod, "HealthChecker should have custom interval")
}

func TestRunOnce(t *testing.T) {
	configs.MockSchedulerConfigByData([]byte(configDefault))
	metrics.Reset()
	schedulerContext, err := NewClusterContext("rmID", "policyGroup")
	assert.NilError(t, err, "Error when load schedulerContext from config")
	assert.Assert(t, schedulerContext.lastHealthCheckResult == nil, "lastHealthCheckResult should be nil initially")

	healthChecker := NewHealthChecker(schedulerContext)
	healthChecker.runOnce(schedulerContext)
	assert.Assert(t, schedulerContext.lastHealthCheckResult != nil, "lastHealthCheckResult shouldn't be nil")
	assert.Assert(t, schedulerContext.lastHealthCheckResult.Healthy == true, "Scheduler should be healthy")
}

func TestStartStop(t *testing.T) {
	partName := "[rmID]default"
	configs.MockSchedulerConfigByData([]byte(configDefault))
	metrics.Reset()
	schedulerContext, err := NewClusterContext("rmID", "policyGroup")
	assert.NilError(t, err, "Error when load schedulerContext from config")
	assert.Assert(t, schedulerContext.lastHealthCheckResult == nil, "lastHealthCheckResult should be nil initially")

	// update resources to some negative value
	negativeRes := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": -10})
	schedulerContext.partitions[partName].totalPartitionResource = negativeRes

	healthChecker := NewHealthCheckerWithParameters(3 * time.Second)
	healthChecker.start(schedulerContext)
	assert.Assert(t, schedulerContext.lastHealthCheckResult != nil, "lastHealthCheckResult shouldn't be nil")
	assert.Assert(t, schedulerContext.lastHealthCheckResult.Healthy == false, "Scheduler should be unhealthy")
	healthChecker.stop()
}

func TestUpdateSchedulerLastHealthStatus(t *testing.T) {
	configs.MockSchedulerConfigByData([]byte(configDefault))
	metrics.Reset()
	schedulerMetrics := metrics.GetSchedulerMetrics()
	schedulerContext, err := NewClusterContext("rmID", "policyGroup")
	assert.NilError(t, err, "Error when load schedulerContext from config")

	healthInfo := GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	updateSchedulerLastHealthStatus(healthInfo, schedulerContext)
	assert.Equal(t, healthInfo.Healthy, schedulerContext.lastHealthCheckResult.Healthy, "lastHealthCheckResult should be updated")
	for i := 0; i < len(healthInfo.HealthChecks); i++ {
		assert.Equal(t, healthInfo.HealthChecks[i], schedulerContext.lastHealthCheckResult.HealthChecks[i], "lastHealthCheckResult should be updated")
	}
}

func TestGetSchedulerHealthStatusContext(t *testing.T) {
	partName := "[rmID]default"
	configs.MockSchedulerConfigByData([]byte(configDefault))
	metrics.Reset()
	schedulerMetrics := metrics.GetSchedulerMetrics()
	schedulerContext, err := NewClusterContext("rmID", "policyGroup")
	assert.NilError(t, err, "Error when load schedulerContext from config")
	// everything OK
	healthInfo := GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	assert.Assert(t, healthInfo.Healthy, "Scheduler should be healthy")

	// update resources to some negative value
	negativeRes := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": -10})
	originalRes := schedulerContext.partitions[partName].totalPartitionResource
	schedulerContext.partitions[partName].totalPartitionResource = negativeRes

	// check should fail because of negative resources
	healthInfo = GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	assert.Assert(t, !healthInfo.Healthy, "Scheduler should not be healthy")

	// set back the original resource, so both the negative and consistency check should pass
	schedulerContext.partitions[partName].totalPartitionResource = originalRes
	healthInfo = GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	assert.Assert(t, healthInfo.Healthy, "Scheduler should be healthy")

	// set some negative node resources
	err = schedulerContext.partitions[partName].AddNode(objects.NewNode(&si.NodeInfo{
		NodeID:     "node",
		Attributes: nil,
		SchedulableResource: &si.Resource{
			Resources: map[string]*si.Quantity{"memory": {Value: -10}},
		},
	}), []*objects.Allocation{})
	assert.NilError(t, err, "Unexpected error while adding a new node")
	healthInfo = GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	assert.Assert(t, !healthInfo.Healthy, "Scheduler should not be healthy")

	// add orphan allocation to a node
	node := schedulerContext.partitions[partName].nodes.GetNode("node")
	alloc := objects.NewAllocation(allocID, "node", newAllocationAsk("key", "appID", resources.NewResource()))
	node.AddAllocation(alloc)
	healthInfo = GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	assert.Assert(t, !healthInfo.Healthy, "Scheduler should not be healthy")
	assert.Assert(t, !healthInfo.HealthChecks[9].Succeeded, "The orphan allocation check on the node should not be successful")

	// add the allocation to the app as well
	part := schedulerContext.partitions[partName]
	app := newApplication("appID", partName, "root.default")
	app.AddAllocation(alloc)
	err = part.AddApplication(app)
	assert.NilError(t, err, "Could not add application")
	healthInfo = GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	assert.Assert(t, healthInfo.HealthChecks[9].Succeeded, "The orphan allocation check on the node should be successful")

	// remove the allocation from the node, so we will have an orphan allocation assigned to the app
	node.RemoveAllocation(allocID)
	healthInfo = GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	assert.Assert(t, healthInfo.HealthChecks[9].Succeeded, "The orphan allocation check on the node should be successful")
	assert.Assert(t, !healthInfo.HealthChecks[10].Succeeded, "The orphan allocation check on the app should not be successful")
}

func TestGetSchedulerHealthStatusMetrics(t *testing.T) {
	configs.MockSchedulerConfigByData([]byte(configDefault))
	metrics.Reset()
	schedulerMetrics := metrics.GetSchedulerMetrics()
	schedulerContext, err := NewClusterContext("rmID", "policyGroup")
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
