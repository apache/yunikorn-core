/*
Copyright 2019 The Unity Scheduler Authors

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
	"encoding/json"
	"github.com/golang/glog"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/webservice/dao"
	"net/http"
	"strconv"
)

func GetQueueInfo(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	lists := gClusterInfo.ListPartitions()
	for _, k := range lists {
		glog.Info(k)
		partitionInfo := getPartitionJson(k)

		if err := json.NewEncoder(w).Encode(partitionInfo); err != nil {
			panic(err)
		}
	}
}

func GetClusterInfo(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	lists := gClusterInfo.ListPartitions()
	for _, k := range lists {
		glog.Info(k)
		clusterInfo := getClusterJson(k)
		var clustersInfo []dao.ClusterDAOInfo
		clustersInfo = append(clustersInfo, *clusterInfo)

		if err := json.NewEncoder(w).Encode(clustersInfo); err != nil {
			panic(err)
		}
	}
}

func writeHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Origin")
	w.WriteHeader(http.StatusOK)
}

func getClusterJson(name string) *dao.ClusterDAOInfo {
	clusterInfo := &dao.ClusterDAOInfo{}
	partitionContext := gClusterInfo.GetPartition(name)
	clusterInfo.TotalJobs = strconv.Itoa(partitionContext.GetTotalJobCount())
	clusterInfo.TotalContainers = strconv.Itoa(partitionContext.GetTotalAllocationCount())
	clusterInfo.TotalNodes = strconv.Itoa(partitionContext.GetTotalNodeCount())
	clusterInfo.ClusterName = "kubernetes"

	clusterInfo.RunningJobs = strconv.Itoa(partitionContext.GetTotalJobCount())
	clusterInfo.RunningContainers = strconv.Itoa(partitionContext.GetTotalAllocationCount())
	clusterInfo.ActiveNodes = strconv.Itoa(partitionContext.GetTotalNodeCount())

	return clusterInfo
}

func getPartitionJson(name string) *dao.PartitionDAOInfo {
	partitionInfo := &dao.PartitionDAOInfo{}

	partitionContext := gClusterInfo.GetPartition(name)
	queueDAOInfo := partitionContext.GetQueueInfos()

	partitionInfo.PartitionName = partitionContext.Name
	partitionInfo.Capacity = dao.PartitionCapacity{
		Capacity:     partitionContext.TotalPartitionResource.String(),
		UsedCapacity: "0",
	}
	partitionInfo.Queues = queueDAOInfo

	return partitionInfo
}
