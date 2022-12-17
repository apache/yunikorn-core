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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

const defaultPeriod = 30 * time.Second

type HealthChecker struct {
	context       *ClusterContext
	confWatcherId string

	// mutable values require locking
	stopChan *chan struct{}
	period   time.Duration
	enabled  bool

	sync.RWMutex
}

func NewHealthChecker(schedulerContext *ClusterContext) *HealthChecker {
	checker := &HealthChecker{
		context: schedulerContext,
	}
	checker.confWatcherId = fmt.Sprintf("health-checker-%p", checker)

	return checker
}

func (c *HealthChecker) GetPeriod() time.Duration {
	c.RLock()
	defer c.RUnlock()
	return c.period
}

func (c *HealthChecker) IsEnabled() bool {
	c.RLock()
	defer c.RUnlock()
	return c.enabled
}

func (c *HealthChecker) readPeriod() time.Duration {
	value, ok := configs.GetConfigMap()[configs.HealthCheckInterval]
	if !ok {
		return configs.DefaultHealthCheckInterval
	}

	result, err := time.ParseDuration(value)
	if err != nil {
		log.Logger().Warn("Failed to parse configuration value",
			zap.String("key", configs.HealthCheckInterval),
			zap.String("value", value),
			zap.Error(err))
		return configs.DefaultHealthCheckInterval
	}
	if result < 0 {
		result = 0
	}
	return result
}

// Start executes healthCheck service in the background
func (c *HealthChecker) Start() {
	c.startInternal(true)
}

func (c *HealthChecker) startInternal(runImmediately bool) {
	if runImmediately {
		c.runOnce()
	}

	c.Lock()
	defer c.Unlock()

	configs.AddConfigMapCallback(c.confWatcherId, func() {
		go c.reloadConfig()
	})

	period := c.readPeriod()
	if period > 0 {
		stopChan := make(chan struct{})
		c.stopChan = &stopChan
		c.period = period
		c.enabled = true

		log.Logger().Info("Starting periodic health checker", zap.Duration("interval", period))

		go func() {
			ticker := time.NewTicker(period)
			for {
				select {
				case <-stopChan:
					ticker.Stop()
					return
				case <-ticker.C:
					c.runOnce()
				}
			}
		}()
	} else {
		// disabled
		c.stopChan = nil
		c.period = 0
		c.enabled = false
		log.Logger().Info("Periodic health checker disabled")
	}
}

func (c *HealthChecker) Stop() {
	c.Lock()
	defer c.Unlock()

	configs.RemoveConfigMapCallback(c.confWatcherId)

	if c.stopChan != nil {
		log.Logger().Info("Stopping periodic health checker")
		*c.stopChan <- struct{}{}
		close(*c.stopChan)
		c.stopChan = nil
	}
}

func (c *HealthChecker) Restart() {
	c.Stop()
	c.startInternal(false)
}

func (c *HealthChecker) reloadConfig() {
	if c.isRestartNeeded() {
		c.Restart()
	}
}

func (c *HealthChecker) isRestartNeeded() bool {
	c.Lock()
	defer c.Unlock()

	period := c.readPeriod()
	return period != c.period
}

func (c *HealthChecker) runOnce() {
	schedulerMetrics := metrics.GetSchedulerMetrics()
	result := GetSchedulerHealthStatus(schedulerMetrics, c.context)
	updateSchedulerLastHealthStatus(&result, c.context)
	if !result.Healthy {
		log.Logger().Warn("Scheduler is not healthy",
			zap.Any("health check values", result.HealthChecks))
	} else {
		log.Logger().Debug("Scheduler is healthy",
			zap.Any("health check values", result.HealthChecks))
	}
}

func updateSchedulerLastHealthStatus(latest *dao.SchedulerHealthDAOInfo, schedulerContext *ClusterContext) {
	schedulerContext.SetLastHealthCheckResult(latest)
}

func GetSchedulerHealthStatus(metrics metrics.CoreSchedulerMetrics, schedulerContext *ClusterContext) dao.SchedulerHealthDAOInfo {
	var healthInfo []dao.HealthCheckInfo
	healthInfo = append(healthInfo, checkSchedulingErrors(metrics))
	healthInfo = append(healthInfo, checkFailedNodes(metrics))
	healthInfo = append(healthInfo, checkSchedulingContext(schedulerContext)...)
	healthy := true
	for _, h := range healthInfo {
		if !h.Succeeded {
			healthy = false
			break
		}
	}
	return dao.SchedulerHealthDAOInfo{
		Healthy:      healthy,
		HealthChecks: healthInfo,
	}
}
func CreateCheckInfo(succeeded bool, name, description, message string) dao.HealthCheckInfo {
	return dao.HealthCheckInfo{
		Name:             name,
		Succeeded:        succeeded,
		Description:      description,
		DiagnosisMessage: message,
	}
}
func checkSchedulingErrors(metrics metrics.CoreSchedulerMetrics) dao.HealthCheckInfo {
	schedulingErrors, err := metrics.GetSchedulingErrors()
	if err != nil {
		return CreateCheckInfo(false, "Scheduling errors", "Check for scheduling error entries in metrics", err.Error())
	}
	diagnosisMsg := fmt.Sprintf("There were %v scheduling errors logged in the metrics", schedulingErrors)
	return CreateCheckInfo(schedulingErrors == 0, "Scheduling errors", "Check for scheduling error entries in metrics", diagnosisMsg)
}

func checkFailedNodes(metrics metrics.CoreSchedulerMetrics) dao.HealthCheckInfo {
	failedNodes, err := metrics.GetFailedNodes()
	if err != nil {
		return CreateCheckInfo(false, "Failed nodes", "Check for failed nodes entries in metrics", err.Error())
	}
	diagnosisMsg := fmt.Sprintf("There were %v failed nodes logged in the metrics", failedNodes)
	return CreateCheckInfo(failedNodes == 0, "Failed nodes", "Check for failed nodes entries in metrics", diagnosisMsg)
}

func checkSchedulingContext(schedulerContext *ClusterContext) []dao.HealthCheckInfo {
	// 1. check resources
	// 1.1 check for negative resources
	var partitionsWithNegResources []string
	var nodesWithNegResources []string
	// 1.3 node allocated resource <= total resource of the node
	var allocationMismatch []string
	// 1.2 total partition resource = sum of node resources
	var totalResourceMismatch []string
	// 1.4 node total resource = allocated resource + occupied resource + available resource
	var nodeTotalMismatch []string
	// 1.5 node capacity >= allocated resources on the node
	var nodeCapacityMismatch []string
	// 2. check reservation/node ration
	var partitionReservationRatio []float32
	// 3. check for orphan allocations
	orphanAllocationsOnNode := make([]*objects.Allocation, 0)
	orphanAllocationsOnApp := make([]*objects.Allocation, 0)

	for _, part := range schedulerContext.GetPartitionMapClone() {
		if part.GetAllocatedResource().HasNegativeValue() {
			partitionsWithNegResources = append(partitionsWithNegResources, part.Name)
		}
		if part.GetTotalPartitionResource().HasNegativeValue() {
			partitionsWithNegResources = append(partitionsWithNegResources, part.Name)
		}
		sumNodeResources := resources.NewResource()
		sumNodeAllocatedResources := resources.NewResource()
		sumReservation := 0
		for _, node := range part.GetNodes() {
			sumNodeResources.AddTo(node.GetCapacity())
			sumNodeAllocatedResources.AddTo(node.GetAllocatedResource())
			sumReservation += len(node.GetReservationKeys())
			calculatedTotalNodeRes := resources.Add(node.GetAllocatedResource(), node.GetOccupiedResource())
			calculatedTotalNodeRes.AddTo(node.GetAvailableResource())
			if !resources.Equals(node.GetCapacity(), calculatedTotalNodeRes) {
				nodeTotalMismatch = append(nodeTotalMismatch, node.NodeID)
			}
			if node.GetAllocatedResource().HasNegativeValue() {
				nodesWithNegResources = append(nodesWithNegResources, node.NodeID)
			}
			if node.GetAvailableResource().HasNegativeValue() {
				nodesWithNegResources = append(nodesWithNegResources, node.NodeID)
			}
			if node.GetCapacity().HasNegativeValue() {
				nodesWithNegResources = append(nodesWithNegResources, node.NodeID)
			}
			if node.GetOccupiedResource().HasNegativeValue() {
				nodesWithNegResources = append(nodesWithNegResources, node.NodeID)
			}
			if !resources.StrictlyGreaterThanOrEquals(node.GetCapacity(), node.GetAllocatedResource()) {
				nodeCapacityMismatch = append(nodeCapacityMismatch, node.NodeID)
			}
			orphanAllocationsOnNode = append(orphanAllocationsOnNode, checkNodeAllocations(node, part)...)
		}
		// check if there are allocations assigned to an app but there are missing from the nodes
		for _, app := range part.GetApplications() {
			orphanAllocationsOnApp = append(orphanAllocationsOnApp, checkAppAllocations(app, part.nodes)...)
		}
		partitionReservationRatio = append(partitionReservationRatio, float32(sumReservation)/(float32(part.GetTotalNodeCount())))
		if !resources.Equals(sumNodeAllocatedResources, part.GetAllocatedResource()) {
			allocationMismatch = append(allocationMismatch, part.Name)
		}
		if !resources.EqualsOrEmpty(sumNodeResources, part.GetTotalPartitionResource()) {
			totalResourceMismatch = append(totalResourceMismatch, part.Name)
		}
	}
	var info = make([]dao.HealthCheckInfo, 9)
	info[0] = CreateCheckInfo(len(partitionsWithNegResources) == 0, "Negative resources",
		"Check for negative resources in the partitions",
		fmt.Sprintf("Partitions with negative resources: %q", partitionsWithNegResources))
	info[1] = CreateCheckInfo(len(nodesWithNegResources) == 0, "Negative resources",
		"Check for negative resources in the nodes",
		fmt.Sprintf("Nodes with negative resources: %q", partitionsWithNegResources))
	info[2] = CreateCheckInfo(len(allocationMismatch) == 0, "Consistency of data",
		"Check if a node's allocated resource <= total resource of the node",
		fmt.Sprintf("Nodes with inconsistent data: %q", allocationMismatch))
	info[3] = CreateCheckInfo(len(totalResourceMismatch) == 0, "Consistency of data",
		"Check if total partition resource == sum of the node resources from the partition",
		fmt.Sprintf("Partitions with inconsistent data: %q", totalResourceMismatch))
	info[4] = CreateCheckInfo(len(nodeTotalMismatch) == 0, "Consistency of data",
		"Check if node total resource = allocated resource + occupied resource + available resource",
		fmt.Sprintf("Nodes with inconsistent data: %q", nodeTotalMismatch))
	info[5] = CreateCheckInfo(len(nodeCapacityMismatch) == 0, "Consistency of data",
		"Check if node capacity >= allocated resources on the node",
		fmt.Sprintf("Nodes with inconsistent data: %q", nodeCapacityMismatch))
	// mark it as succeeded for a while until we will know what is not considered a normal value anymore
	info[6] = CreateCheckInfo(true, "Reservation check",
		"Check the reservation nr compared to the number of nodes",
		fmt.Sprintf("Reservation/node nr ratio: %f", partitionReservationRatio))
	info[7] = CreateCheckInfo(len(orphanAllocationsOnNode) == 0, "Orphan allocation on node check",
		"Check if there are orphan allocations on the nodes",
		fmt.Sprintf("Orphan allocations: %v", orphanAllocationsOnNode))
	info[8] = CreateCheckInfo(len(orphanAllocationsOnApp) == 0, "Orphan allocation on app check",
		"Check if there are orphan allocations on the applications",
		fmt.Sprintf("OrphanAllocations: %v", orphanAllocationsOnApp))
	return info
}

func checkAppAllocations(app *objects.Application, nodes objects.NodeCollection) []*objects.Allocation {
	orphanAllocationsOnApp := make([]*objects.Allocation, 0)
	for _, alloc := range app.GetAllAllocations() {
		if node := nodes.GetNode(alloc.GetNodeID()); node != nil {
			if node.GetAllocation(alloc.GetUUID()) == nil {
				orphanAllocationsOnApp = append(orphanAllocationsOnApp, alloc)
			}
		} else {
			orphanAllocationsOnApp = append(orphanAllocationsOnApp, alloc)
		}
	}
	return orphanAllocationsOnApp
}

func checkNodeAllocations(node *objects.Node, partitionContext *PartitionContext) []*objects.Allocation {
	orphanAllocationsOnNode := make([]*objects.Allocation, 0)
	for _, alloc := range node.GetAllAllocations() {
		if app := partitionContext.getApplication(alloc.GetApplicationID()); app != nil {
			if !app.IsAllocationAssignedToApp(alloc) {
				orphanAllocationsOnNode = append(orphanAllocationsOnNode, alloc)
			}
		} else {
			orphanAllocationsOnNode = append(orphanAllocationsOnNode, alloc)
		}
	}
	return orphanAllocationsOnNode
}
