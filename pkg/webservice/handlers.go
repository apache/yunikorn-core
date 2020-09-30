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
package webservice

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func getStackInfo(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	var stack = func() []byte {
		buf := make([]byte, 1024)
		for {
			n := runtime.Stack(buf, true)
			if n < len(buf) {
				return buf[:n]
			}
			buf = make([]byte, 2*len(buf))
		}
	}
	if _, err := w.Write(stack()); err != nil {
		log.Logger().Error("GetStackInfo error", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func getQueueInfo(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	lists := gClusterInfo.ListPartitions()
	for _, k := range lists {
		partitionInfo := getPartitionJSON(k)

		if err := json.NewEncoder(w).Encode(partitionInfo); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func getClusterInfo(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	lists := gClusterInfo.ListPartitions()
	for _, k := range lists {
		clusterInfo := getClusterJSON(k)
		var clustersInfo []dao.ClusterDAOInfo
		clustersInfo = append(clustersInfo, *clusterInfo)

		if err := json.NewEncoder(w).Encode(clustersInfo); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func getClusterUtilization(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	var clusterUtil []*dao.ClustersUtilDAOInfo
	var utilizations []*dao.ClusterUtilDAOInfo
	lists := gClusterInfo.ListPartitions()
	for _, k := range lists {
		partition := gClusterInfo.GetPartition(k)
		utilizations = getClusterUtilJSON(partition)
		clusterUtil = append(clusterUtil, &dao.ClustersUtilDAOInfo{
			PartitionName: partition.Name,
			ClustersUtil:  utilizations,
		})
	}

	if err := json.NewEncoder(w).Encode(clusterUtil); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func getApplicationsInfo(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	queueName := r.URL.Query().Get("queue")
	queueErr := validateQueue(queueName)
	if queueErr != nil {
		http.Error(w, queueErr.Error(), http.StatusBadRequest)
		return
	}

	var appsDao []*dao.ApplicationDAOInfo
	lists := gClusterInfo.ListPartitions()
	for _, k := range lists {
		partition := gClusterInfo.GetPartition(k)
		appList := partition.GetApplications()
		for _, app := range appList {
			if len(queueName) == 0 || strings.EqualFold(queueName, app.QueueName) {
				appsDao = append(appsDao, getApplicationJSON(app))
			}
		}
	}

	if err := json.NewEncoder(w).Encode(appsDao); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func validateQueue(queueName string) error {
	if queueName != "" {
		queueNameArr := strings.Split(queueName, ".")
		for _, name := range queueNameArr {
			if !configs.QueueNameRegExp.MatchString(name) {
				return fmt.Errorf("problem in queue query parameter parsing as queue param "+
					"%s contains invalid queue name %s. Queue name must only have "+
					"alphanumeric characters, - or _, and be no longer than 64 characters", queueName, name)
			}
		}
	}
	return nil
}

func getNodesInfo(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	var result []*dao.NodesDAOInfo
	lists := gClusterInfo.ListPartitions()
	for _, k := range lists {
		var nodesDao []*dao.NodeDAOInfo
		partition := gClusterInfo.GetPartition(k)
		for _, node := range partition.GetNodes() {
			nodeDao := getNodeJSON(node)
			nodesDao = append(nodesDao, nodeDao)
		}
		result = append(result, &dao.NodesDAOInfo{
			PartitionName: partition.Name,
			Nodes:         nodesDao,
		})
	}

	if err := json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func getNodesUtilization(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	lists := gClusterInfo.ListPartitions()
	var result []*dao.NodesUtilDAOInfo
	for _, k := range lists {
		partition := gClusterInfo.GetPartition(k)
		for name := range partition.GetTotalPartitionResource().Resources {
			nodesUtil := getNodesUtilJSON(partition, name)
			result = append(result, nodesUtil)
		}
	}
	if err := json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func validateConf(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	requestBytes, err := ioutil.ReadAll(r.Body)
	if err == nil {
		_, err = configs.LoadSchedulerConfigFromByteArray(requestBytes)
	}
	var result dao.ValidateConfResponse
	if err != nil {
		result.Allowed = false
		result.Reason = err.Error()
	} else {
		result.Allowed = true
	}
	if err = json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Origin")
}

func getClusterJSON(name string) *dao.ClusterDAOInfo {
	clusterInfo := &dao.ClusterDAOInfo{}
	partitionContext := gClusterInfo.GetPartition(name)
	clusterInfo.TotalApplications = strconv.Itoa(partitionContext.GetTotalApplicationCount())
	clusterInfo.TotalContainers = strconv.Itoa(partitionContext.GetTotalAllocationCount())
	clusterInfo.TotalNodes = strconv.Itoa(partitionContext.GetTotalNodeCount())
	clusterInfo.ClusterName = "kubernetes"

	clusterInfo.RunningApplications = strconv.Itoa(partitionContext.GetTotalApplicationCount())
	clusterInfo.RunningContainers = strconv.Itoa(partitionContext.GetTotalAllocationCount())
	clusterInfo.ActiveNodes = strconv.Itoa(partitionContext.GetTotalNodeCount())

	return clusterInfo
}

func getClusterUtilJSON(partition *cache.PartitionInfo) []*dao.ClusterUtilDAOInfo {
	var utils []*dao.ClusterUtilDAOInfo
	var getResource bool = true
	total := partition.GetTotalPartitionResource()
	if resources.IsZero(total) {
		getResource = false
	}
	used := partition.Root.GetAllocatedResource()
	if len(used.Resources) == 0 {
		getResource = false
	}
	if getResource {
		percent := resources.CalculateAbsUsedCapacity(total, used)
		for name, value := range percent.Resources {
			utilization := &dao.ClusterUtilDAOInfo{
				ResourceType: name,
				Total:        int64(total.Resources[name]),
				Used:         int64(used.Resources[name]),
				Usage:        fmt.Sprintf("%d", int64(value)) + "%",
			}
			utils = append(utils, utilization)
		}
	} else if !getResource {
		utilization := &dao.ClusterUtilDAOInfo{
			ResourceType: "N/A",
			Total:        int64(-1),
			Used:         int64(-1),
			Usage:        "N/A",
		}
		utils = append(utils, utilization)
	}
	return utils
}

func getPartitionJSON(name string) *dao.PartitionDAOInfo {
	partitionInfo := &dao.PartitionDAOInfo{}

	partitionContext := gClusterInfo.GetPartition(name)
	queueDAOInfo := partitionContext.Root.GetQueueInfos()

	partitionInfo.PartitionName = partitionContext.Name
	partitionInfo.Capacity = dao.PartitionCapacity{
		Capacity:     partitionContext.GetTotalPartitionResource().String(),
		UsedCapacity: "0",
	}
	partitionInfo.Queues = queueDAOInfo

	return partitionInfo
}

func getApplicationJSON(app *cache.ApplicationInfo) *dao.ApplicationDAOInfo {
	var allocationInfos []dao.AllocationDAOInfo
	allocations := app.GetAllAllocations()
	for _, alloc := range allocations {
		allocInfo := dao.AllocationDAOInfo{
			AllocationKey:    alloc.AllocationProto.AllocationKey,
			AllocationTags:   alloc.AllocationProto.AllocationTags,
			UUID:             alloc.AllocationProto.UUID,
			ResourcePerAlloc: alloc.AllocatedResource.DAOString(),
			Priority:         alloc.AllocationProto.Priority.String(),
			QueueName:        alloc.AllocationProto.QueueName,
			NodeID:           alloc.AllocationProto.NodeID,
			ApplicationID:    alloc.AllocationProto.ApplicationID,
			Partition:        alloc.AllocationProto.PartitionName,
		}
		allocationInfos = append(allocationInfos, allocInfo)
	}

	return &dao.ApplicationDAOInfo{
		ApplicationID:  app.ApplicationID,
		UsedResource:   app.GetAllocatedResource().DAOString(),
		Partition:      app.Partition,
		QueueName:      app.QueueName,
		SubmissionTime: app.SubmissionTime,
		Allocations:    allocationInfos,
		State:          app.GetApplicationState(),
	}
}

func getNodeJSON(nodeInfo *cache.NodeInfo) *dao.NodeDAOInfo {
	var allocations []*dao.AllocationDAOInfo
	for _, alloc := range nodeInfo.GetAllAllocations() {
		allocInfo := &dao.AllocationDAOInfo{
			AllocationKey:    alloc.AllocationProto.AllocationKey,
			AllocationTags:   alloc.AllocationProto.AllocationTags,
			UUID:             alloc.AllocationProto.UUID,
			ResourcePerAlloc: alloc.AllocatedResource.DAOString(),
			Priority:         alloc.AllocationProto.Priority.String(),
			QueueName:        alloc.AllocationProto.QueueName,
			NodeID:           alloc.AllocationProto.NodeID,
			ApplicationID:    alloc.AllocationProto.ApplicationID,
			Partition:        alloc.AllocationProto.PartitionName,
		}
		allocations = append(allocations, allocInfo)
	}

	return &dao.NodeDAOInfo{
		NodeID:      nodeInfo.NodeID,
		HostName:    nodeInfo.Hostname,
		RackName:    nodeInfo.Rackname,
		Capacity:    nodeInfo.GetCapacity().DAOString(),
		Occupied:    nodeInfo.GetOccupiedResource().DAOString(),
		Allocated:   nodeInfo.GetAllocatedResource().DAOString(),
		Available:   nodeInfo.GetAvailableResource().DAOString(),
		Allocations: allocations,
		Schedulable: nodeInfo.IsSchedulable(),
	}
}

func getNodesUtilJSON(partition *cache.PartitionInfo, name string) *dao.NodesUtilDAOInfo {
	mapResult := make([]int, 10)
	mapName := make([][]string, 10)
	var v float64
	var nodeUtil []*dao.NodeUtilDAOInfo
	for _, node := range partition.GetNodes() {
		total, ok := node.GetCapacity().Resources[name]
		if !ok {
			total = 0
		}
		resourceAllocated, ok := node.GetAllocatedResource().Resources[name]
		if !ok {
			resourceAllocated = 0
		}
		if float64(total) > 0 {
			v = float64(resourceAllocated) / float64(total)
		} else {
			v = float64(0)
		}
		idx := int(math.Dim(math.Ceil(v*10), 1))
		mapResult[idx]++
		mapName[idx] = append(mapName[idx], node.NodeID)
	}
	for k := 0; k < 10; k++ {
		util := &dao.NodeUtilDAOInfo{
			BucketID:   fmt.Sprintf("%d", k+1),
			BucketName: fmt.Sprintf("%d", k*10) + "-" + fmt.Sprintf("%d", (k+1)*10) + "%",
			NumOfNodes: int64(mapResult[k]),
			NodeNames:  mapName[k],
		}
		nodeUtil = append(nodeUtil, util)
	}
	return &dao.NodesUtilDAOInfo{
		ResourceType: name,
		NodesUtil:    nodeUtil,
	}
}

func getApplicationHistory(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	// There is nothing to return but we did not really encounter a problem
	if imHistory == nil {
		http.Error(w, "Internal metrics collection is not enabled.", http.StatusNotImplemented)
		return
	}
	var result []*dao.ApplicationHistoryDAOInfo
	// get a copy of the records: if the array contains nil values they will always be at the
	// start and we cannot shortcut the loop using a break, we must finish iterating
	records := imHistory.GetRecords()
	for _, record := range records {
		if record == nil {
			continue
		}
		element := &dao.ApplicationHistoryDAOInfo{
			Timestamp:         record.Timestamp.UnixNano(),
			TotalApplications: strconv.Itoa(record.TotalApplications),
		}
		result = append(result, element)
	}
	if err := json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func getContainerHistory(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	// There is nothing to return but we did not really encounter a problem
	if imHistory == nil {
		http.Error(w, "Internal metrics collection is not enabled.", http.StatusNotImplemented)
		return
	}
	var result []*dao.ContainerHistoryDAOInfo
	// get a copy of the records: if the array contains nil values they will always be at the
	// start and we cannot shortcut the loop using a break, we must finish iterating
	records := imHistory.GetRecords()
	for _, record := range records {
		if record == nil {
			continue
		}
		element := &dao.ContainerHistoryDAOInfo{
			Timestamp:       record.Timestamp.UnixNano(),
			TotalContainers: strconv.Itoa(record.TotalContainers),
		}
		result = append(result, element)
	}
	if err := json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

}

func getClusterConfig(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	conf := configs.ConfigContext.Get(gClusterInfo.GetPolicyGroup())
	var marshalledConf []byte
	var err error
	// check if we have a request for json output
	if r.Header.Get("Accept") == "application/json" {
		marshalledConf, err = json.Marshal(&conf)
	} else {
		w.Header().Set("Content-Type", "application/x-yaml; charset=UTF-8")
		marshalledConf, err = yaml.Marshal(&conf)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	// add readable form of checksum
	sha256 := fmt.Sprintf("\nsha256 checksum: %X", conf.Checksum)
	marshalledConf = append(marshalledConf, sha256...)
	if _, err = w.Write(marshalledConf); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func updateConfig(w http.ResponseWriter, r *http.Request) {
	lock.Lock()
	defer lock.Unlock()
	writeHeaders(w)
	requestBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		buildUpdateResponse(false, err.Error(), w)
		return
	}
	// validation is already called when loading the config
	schedulerConf, err := configs.LoadSchedulerConfigFromByteArray(requestBytes)
	if err != nil {
		buildUpdateResponse(false, err.Error(), w)
		return
	}
	oldConf, err := updateConfiguration(string(requestBytes))
	if err != nil {
		buildUpdateResponse(false, err.Error(), w)
		return
	}
	err = gClusterInfo.UpdateSchedulerConfig(schedulerConf)
	if err != nil {
		errorMsg := err.Error()
		// revert configmap changes
		_, err := updateConfiguration(oldConf)
		if err != nil {
			msg := "Configuration rollback failed" + "\n" + err.Error()
			errorMsg += "\n" + msg
			log.Logger().Error(msg)
		}
		buildUpdateResponse(false, errorMsg, w)
		return
	}
	buildUpdateResponse(true, "", w)
}

func buildUpdateResponse(success bool, reason string, w http.ResponseWriter) {
	if len(reason) > 0 {
		log.Logger().Info("Result of configuration update: ",
			zap.Bool("Result", success),
			zap.String("Reason in case of failure", reason))
	}

	if success {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("Configuration updated successfully"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		http.Error(w, reason, http.StatusConflict)
	}
}
func updateConfiguration(conf string) (string, error) {
	if plugin := plugins.GetConfigPlugin(); plugin != nil {
		// checking predicates
		resp := plugin.UpdateConfiguration(&si.UpdateConfigurationRequest{
			Configs: conf,
		})
		if resp.Success {
			return resp.OldConfig, nil
		}
		return resp.OldConfig, fmt.Errorf(resp.Reason)
	}
	return "", fmt.Errorf("config plugin not found")
}
