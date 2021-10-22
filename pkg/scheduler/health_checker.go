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

	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
)

func GetSchedulerHealthStatus(metrics metrics.CoreSchedulerMetrics, schedulerContext *ClusterContext) dao.SchedulerHealthDAOInfo {
	var healthInfos []dao.HealthCheckInfo
	healthInfos = append(healthInfos, checkSchedulingErrors(metrics))
	healthInfos = append(healthInfos, checkFailedNodes(metrics))
	healthInfos = append(healthInfos, checkSchedulingContext(schedulerContext)...)
	healthy := true
	for _, h := range healthInfos {
		if !h.Succeeded {
			healthy = false
			break
		}
	}
	return dao.SchedulerHealthDAOInfo{
		Healthy:      healthy,
		HealthChecks: healthInfos,
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
			sumReservation += len(node.GetReservations())
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
			orphanAllocationsOnNode = append(orphanAllocationsOnNode, checkNodeAllocations(node, part.applications)...)
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
	var infos = make([]dao.HealthCheckInfo, 9)
	infos[0] = CreateCheckInfo(len(partitionsWithNegResources) == 0, "Negative resources",
		"Check for negative resources in the partitions",
		fmt.Sprintf("Partitions with negative resources: %q", partitionsWithNegResources))
	infos[1] = CreateCheckInfo(len(nodesWithNegResources) == 0, "Negative resources",
		"Check for negative resources in the nodes",
		fmt.Sprintf("Nodes with negative resources: %q", partitionsWithNegResources))
	infos[2] = CreateCheckInfo(len(allocationMismatch) == 0, "Consistency of data",
		"Check if a node's allocated resource <= total resource of the node",
		fmt.Sprintf("Nodes with inconsistent data: %q", allocationMismatch))
	infos[3] = CreateCheckInfo(len(totalResourceMismatch) == 0, "Consistency of data",
		"Check if total partition resource == sum of the node resources from the partition",
		fmt.Sprintf("Partitions with inconsistent data: %q", totalResourceMismatch))
	infos[4] = CreateCheckInfo(len(nodeTotalMismatch) == 0, "Consistency of data",
		"Check if node total resource = allocated resource + occupied resource + available resource",
		fmt.Sprintf("Nodes with inconsistent data: %q", nodeTotalMismatch))
	infos[5] = CreateCheckInfo(len(nodeCapacityMismatch) == 0, "Consistency of data",
		"Check if node capacity >= allocated resources on the node",
		fmt.Sprintf("Nodes with inconsistent data: %q", nodeCapacityMismatch))
	// mark it as succeeded for a while until we will know what is not considered a normal value anymore
	infos[6] = CreateCheckInfo(true, "Reservation check",
		"Check the reservation nr compared to the number of nodes",
		fmt.Sprintf("Reservation/node nr ratio: %f", partitionReservationRatio))
	infos[7] = CreateCheckInfo(len(orphanAllocationsOnNode) == 0, "Orphan allocation on node check",
		"Check if there are orphan allocations on the nodes",
		fmt.Sprintf("Orphan allocations: %v", orphanAllocationsOnNode))
	infos[8] = CreateCheckInfo(len(orphanAllocationsOnApp) == 0, "Orphan allocation on app check",
		"Check if there are orphan allocations on the applications",
		fmt.Sprintf("OrphanAllocations: %v", orphanAllocationsOnApp))
	return infos
}

func checkAppAllocations(app *objects.Application, nodes objects.NodeCollection) []*objects.Allocation {
	orphanAllocationsOnApp := make([]*objects.Allocation, 0)
	for _, alloc := range app.GetAllAllocations() {
		if node := nodes.GetNode(alloc.NodeID); node != nil {
			if node.GetAllocation(alloc.UUID) == nil {
				orphanAllocationsOnApp = append(orphanAllocationsOnApp, alloc)
			}
		} else {
			orphanAllocationsOnApp = append(orphanAllocationsOnApp, alloc)
		}
	}
	return orphanAllocationsOnApp
}

func checkNodeAllocations(node *objects.Node, applications map[string]*objects.Application) []*objects.Allocation {
	orphanAllocationsOnNode := make([]*objects.Allocation, 0)
	for _, alloc := range node.GetAllAllocations() {
		if app, ok := applications[alloc.ApplicationID]; ok {
			if !app.IsAllocationAssignedToApp(alloc) {
				orphanAllocationsOnNode = append(orphanAllocationsOnNode, alloc)
			}
		} else {
			orphanAllocationsOnNode = append(orphanAllocationsOnNode, alloc)
		}
	}
	return orphanAllocationsOnNode
}
