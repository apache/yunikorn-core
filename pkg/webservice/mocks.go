/*/*
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
package webservice

import (
	"github.com/cloudera/yunikorn-core/pkg/webservice/dao"
)

func getDummyPartitionJson() *dao.PartitionDAOInfo {
	partitionInfo := &dao.PartitionDAOInfo{}
	partitionInfo.PartitionName = "default"
	partitionInfo.Capacity = dao.PartitionCapacity{
		Capacity:     "8000mb, 450vcores",
		UsedCapacity: "500mb, 50vcores",
	}
	partitionInfo.Nodes = []dao.NodeInfo{
		{
			Capability: "3000mb, 200vcores",
			NodeID:     "node1",
		},
		{
			Capability: "5000mb, 250vcores",
			NodeID:     "node2",
		},
	}
	partitionInfo.Queues = []dao.QueueDAOInfo{
		{
			Status:    "RUNNING",
			QueueName: "queue1",
			Capacities: dao.QueueCapacity{
				Capacity:        "3000mb,150vcores",
				MaxCapacity:     "4000mb, 200vcores",
				UsedCapacity:    "1000mb, 50vcores",
				AbsUsedCapacity: "50",
			},
			ChildQueues: []dao.QueueDAOInfo{
				{
					Status:    "RUNNING",
					QueueName: "queue3",
					Capacities: dao.QueueCapacity{
						Capacity:        "300mb,50vcores",
						MaxCapacity:     "300mb, 100vcores",
						UsedCapacity:    "50mb, 5vcores",
						AbsUsedCapacity: "100",
					},
					ChildQueues: nil,
				},
			},
		},
		{
			Status:    "RUNNING",
			QueueName: "queue2",
			Capacities: dao.QueueCapacity{
				Capacity:        "2000mb,150vcores",
				MaxCapacity:     "3000mb, 200vcores",
				UsedCapacity:    "500mb, 25vcores",
				AbsUsedCapacity: "50",
			},
			ChildQueues: nil,
		},
	}
	return partitionInfo
}

func getDummyClusterJson() []dao.ClusterDAOInfo {
	clustersInfo := []dao.ClusterDAOInfo{
		{
			ClusterName:           "k8s",
			ActiveNodes:           "2",
			CompletedApplications: "10",
			FailedContainers:      "10",
			FailedApplications:    "10",
			PendingContainers:     "10",
			PendingApplications:   "10",
			RunningContainers:     "10",
			RunningApplications:   "10",
			TotalContainers:       "10",
			TotalApplications:     "10",
			TotalNodes:            "10",
			FailedNodes:           "0",
		},
	}
	return clustersInfo
}
