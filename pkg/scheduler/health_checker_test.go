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
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
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

func TestNewHealthChecker(t *testing.T) {
	schedulerContext, err := NewClusterContext("rmID", "policyGroup", []byte(configDefault))
	assert.NilError(t, err, "Error when load schedulerContext from config")
	c := NewHealthChecker(schedulerContext)
	assert.Assert(t, c != nil, "HealthChecker shouldn't be nil")
}

func TestRunOnce(t *testing.T) {
	metrics.Reset()
	schedulerContext, err := NewClusterContext("rmID", "policyGroup", []byte(configDefault))
	assert.NilError(t, err, "Error when load schedulerContext from config")
	assert.Assert(t, schedulerContext.GetLastHealthCheckResult() == nil, "lastHealthCheckResult should be nil initially")

	healthChecker := NewHealthChecker(schedulerContext)
	healthChecker.runOnce()
	result := schedulerContext.GetLastHealthCheckResult()
	assert.Assert(t, result != nil, "lastHealthCheckResult shouldn't be nil")
	assert.Assert(t, result.Healthy == true, "Scheduler should be healthy")
}

func TestStartStop(t *testing.T) {
	partName := "[rmID]default"
	metrics.Reset()

	configs.SetConfigMap(map[string]string{configs.HealthCheckInterval: "3s"})
	defer configs.SetConfigMap(map[string]string{})

	schedulerContext, err := NewClusterContext("rmID", "policyGroup", []byte(configDefault))
	assert.NilError(t, err, "Error when load schedulerContext from config")
	assert.Assert(t, schedulerContext.GetLastHealthCheckResult() == nil, "lastHealthCheckResult should be nil initially")

	// update resources to some negative value
	negativeRes := resources.NewResourceFromMap(map[string]resources.Quantity{"memory": -10})
	schedulerContext.partitions[partName].totalPartitionResource = negativeRes

	healthChecker := NewHealthChecker(schedulerContext)
	healthChecker.Start()
	defer healthChecker.Stop()

	result := schedulerContext.GetLastHealthCheckResult()
	assert.Assert(t, result != nil, "lastHealthCheckResult shouldn't be nil")
	assert.Assert(t, result.Healthy == false, "Scheduler should be unhealthy")
}

func TestConfigUpdate(t *testing.T) {
	metrics.Reset()
	defer metrics.Reset()

	configs.SetConfigMap(map[string]string{configs.HealthCheckInterval: "30s"})
	defer configs.SetConfigMap(map[string]string{})

	schedulerContext, err := NewClusterContext("rmID", "policyGroup", []byte(configDefault))
	assert.NilError(t, err, "Error when load schedulerContext from config")
	assert.Assert(t, schedulerContext.GetLastHealthCheckResult() == nil, "lastHealthCheckResult should be nil initially")

	healthChecker := NewHealthChecker(schedulerContext)
	healthChecker.Start()
	defer healthChecker.Stop()

	// initial result should exist
	assert.Assert(t, schedulerContext.GetLastHealthCheckResult() != nil, "lastHealthCheckResult shouldn't be nil")

	// clear results
	schedulerContext.SetLastHealthCheckResult(nil)

	// sleep for 1 second; update should not yet have happened
	time.Sleep(1 * time.Second)
	assert.Assert(t, schedulerContext.GetLastHealthCheckResult() == nil, "lastHealthCheckResult should be nil")

	// update config to run every 100ms and wait for refresh
	configs.SetConfigMap(map[string]string{configs.HealthCheckInterval: "100ms"})
	err = common.WaitForCondition(func() bool {
		return healthChecker.GetPeriod() == 100*time.Millisecond
	}, 10*time.Millisecond, 5*time.Second)
	assert.NilError(t, err, "timed out waiting for config refresh")

	// clear results
	schedulerContext.SetLastHealthCheckResult(nil)

	// sleep for 1 second, update should now have happened at least once
	time.Sleep(1 * time.Second)
	assert.Assert(t, schedulerContext.GetLastHealthCheckResult() != nil, "lastHealthCheckResult shouldn't be nil")

	// disable and wait for refresh
	configs.SetConfigMap(map[string]string{configs.HealthCheckInterval: "0"})
	err = common.WaitForCondition(func() bool {
		return !healthChecker.IsEnabled()
	}, 10*time.Millisecond, 5*time.Second)
	assert.NilError(t, err, "timed out waiting for config refresh")

	// clear results
	schedulerContext.SetLastHealthCheckResult(nil)

	// sleep for 1 second, update should not have happened
	time.Sleep(1 * time.Second)
	assert.Assert(t, schedulerContext.GetLastHealthCheckResult() == nil, "lastHealthCheckResult should be nil")
}

func TestUpdateSchedulerLastHealthStatus(t *testing.T) {
	metrics.Reset()
	schedulerMetrics := metrics.GetSchedulerMetrics()
	schedulerContext, err := NewClusterContext("rmID", "policyGroup", []byte(configDefault))
	assert.NilError(t, err, "Error when load schedulerContext from config")

	healthInfo := GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	updateSchedulerLastHealthStatus(&healthInfo, schedulerContext)
	result := schedulerContext.GetLastHealthCheckResult()
	assert.Equal(t, healthInfo.Healthy, result.Healthy, "lastHealthCheckResult should be updated")
	for i := 0; i < len(healthInfo.HealthChecks); i++ {
		assert.Equal(t, healthInfo.HealthChecks[i], result.HealthChecks[i], "lastHealthCheckResult should be updated")
	}
}

func TestGetSchedulerHealthStatusContext(t *testing.T) {
	partName := "[rmID]default"
	metrics.Reset()
	schedulerMetrics := metrics.GetSchedulerMetrics()
	schedulerContext, err := NewClusterContext("rmID", "policyGroup", []byte(configDefault))
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
	alloc := objects.NewAllocation("node", newAllocationAsk("key", "appID", resources.NewResource()))
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
	node.RemoveAllocation("key")
	healthInfo = GetSchedulerHealthStatus(schedulerMetrics, schedulerContext)
	assert.Assert(t, healthInfo.HealthChecks[9].Succeeded, "The orphan allocation check on the node should be successful")
	assert.Assert(t, !healthInfo.HealthChecks[10].Succeeded, "The orphan allocation check on the app should not be successful")
}

func TestGetSchedulerHealthStatusMetrics(t *testing.T) {
	metrics.Reset()
	schedulerMetrics := metrics.GetSchedulerMetrics()
	schedulerContext, err := NewClusterContext("rmID", "policyGroup", []byte(configDefault))
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
