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

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	metrics2 "github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects"
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

	lists := schedulerContext.GetPartitionMapClone()
	for _, partition := range lists {
		partitionInfo := getPartitionJSON(partition)

		if err := json.NewEncoder(w).Encode(partitionInfo); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func getClusterInfo(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	lists := schedulerContext.GetPartitionMapClone()
	for _, partition := range lists {
		clusterInfo := getClusterJSON(partition)
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
	lists := schedulerContext.GetPartitionMapClone()
	for _, partition := range lists {
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
	lists := schedulerContext.GetPartitionMapClone()
	for _, partition := range lists {
		appList := partition.GetApplications()
		for _, app := range appList {
			if len(queueName) == 0 || strings.EqualFold(queueName, app.GetQueueName()) {
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
	lists := schedulerContext.GetPartitionMapClone()
	for _, partition := range lists {
		var nodesDao []*dao.NodeDAOInfo
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

	lists := schedulerContext.GetPartitionMapClone()
	var result []*dao.NodesUtilDAOInfo
	for _, partition := range lists {
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

func getClusterJSON(partition *scheduler.PartitionContext) *dao.ClusterDAOInfo {
	clusterInfo := &dao.ClusterDAOInfo{}
	clusterInfo.TotalApplications = strconv.Itoa(partition.GetTotalApplicationCount())
	clusterInfo.TotalContainers = strconv.Itoa(partition.GetTotalAllocationCount())
	clusterInfo.TotalNodes = strconv.Itoa(partition.GetTotalNodeCount())
	clusterInfo.ClusterName = "kubernetes"

	clusterInfo.RunningApplications = strconv.Itoa(partition.GetTotalApplicationCount())
	clusterInfo.RunningContainers = strconv.Itoa(partition.GetTotalAllocationCount())
	clusterInfo.ActiveNodes = strconv.Itoa(partition.GetTotalNodeCount())

	return clusterInfo
}

func getClusterUtilJSON(partition *scheduler.PartitionContext) []*dao.ClusterUtilDAOInfo {
	var utils []*dao.ClusterUtilDAOInfo
	var getResource bool = true
	total := partition.GetTotalPartitionResource()
	if resources.IsZero(total) {
		getResource = false
	}
	used := partition.GetAllocatedResource()
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

func getPartitionJSON(partition *scheduler.PartitionContext) *dao.PartitionDAOInfo {
	partitionInfo := &dao.PartitionDAOInfo{}

	queueDAOInfo := partition.GetQueueInfos()

	partitionInfo.PartitionName = partition.Name
	partitionInfo.Capacity = dao.PartitionCapacity{
		Capacity:     partition.GetTotalPartitionResource().DAOString(),
		UsedCapacity: partition.GetAllocatedResource().DAOString(),
	}
	partitionInfo.Queues = queueDAOInfo

	return partitionInfo
}

func getApplicationJSON(app *objects.Application) *dao.ApplicationDAOInfo {
	var allocationInfos []dao.AllocationDAOInfo
	allocations := app.GetAllAllocations()
	for _, alloc := range allocations {
		allocInfo := dao.AllocationDAOInfo{
			AllocationKey:    alloc.AllocationKey,
			AllocationTags:   alloc.Tags,
			UUID:             alloc.UUID,
			ResourcePerAlloc: alloc.AllocatedResource.DAOString(),
			Priority:         strconv.Itoa(int(alloc.Priority)),
			QueueName:        alloc.QueueName,
			NodeID:           alloc.NodeID,
			ApplicationID:    alloc.ApplicationID,
			Partition:        alloc.PartitionName,
		}
		allocationInfos = append(allocationInfos, allocInfo)
	}

	return &dao.ApplicationDAOInfo{
		ApplicationID:  app.ApplicationID,
		UsedResource:   app.GetAllocatedResource().DAOString(),
		Partition:      app.Partition,
		QueueName:      app.QueueName,
		SubmissionTime: app.SubmissionTime.Unix(),
		Allocations:    allocationInfos,
		State:          app.CurrentState(),
	}
}

func getNodeJSON(node *objects.Node) *dao.NodeDAOInfo {
	var allocations []*dao.AllocationDAOInfo
	for _, alloc := range node.GetAllAllocations() {
		allocInfo := &dao.AllocationDAOInfo{
			AllocationKey:    alloc.AllocationKey,
			AllocationTags:   alloc.Tags,
			UUID:             alloc.UUID,
			ResourcePerAlloc: alloc.AllocatedResource.DAOString(),
			Priority:         strconv.Itoa(int(alloc.Priority)),
			QueueName:        alloc.QueueName,
			NodeID:           alloc.NodeID,
			ApplicationID:    alloc.ApplicationID,
			Partition:        alloc.PartitionName,
		}
		allocations = append(allocations, allocInfo)
	}

	return &dao.NodeDAOInfo{
		NodeID:      node.NodeID,
		HostName:    node.Hostname,
		RackName:    node.Rackname,
		Capacity:    node.GetCapacity().DAOString(),
		Occupied:    node.GetOccupiedResource().DAOString(),
		Allocated:   node.GetAllocatedResource().DAOString(),
		Available:   node.GetAvailableResource().DAOString(),
		Allocations: allocations,
		Schedulable: node.IsSchedulable(),
	}
}

func getNodesUtilJSON(partition *scheduler.PartitionContext, name string) *dao.NodesUtilDAOInfo {
	mapResult := make([]int, 10)
	mapName := make([][]string, 10)
	var v float64
	var resourceExist = true
	var nodeUtil []*dao.NodeUtilDAOInfo
	for _, node := range partition.GetNodes() {
		total := node.GetCapacity()
		if total.Resources[name] <= 0 {
			resourceExist = false
		}
		resourceAllocated := node.GetAllocatedResource()
		if _, ok := resourceAllocated.Resources[name]; !ok {
			resourceExist = false
		}
		if resourceExist {
			v = float64(resources.CalculateAbsUsedCapacity(total, resourceAllocated).Resources[name])
			idx := int(math.Dim(math.Ceil(v/10), 1))
			mapResult[idx]++
			mapName[idx] = append(mapName[idx], node.NodeID)
		}
	}
	for k := 0; k < 10; k++ {
		if resourceExist {
			util := &dao.NodeUtilDAOInfo{
				BucketName: fmt.Sprintf("%d", k*10) + "-" + fmt.Sprintf("%d", (k+1)*10) + "%",
				NumOfNodes: int64(mapResult[k]),
				NodeNames:  mapName[k],
			}
			nodeUtil = append(nodeUtil, util)
		} else {
			util := &dao.NodeUtilDAOInfo{
				BucketName: fmt.Sprintf("%d", k*10) + "-" + fmt.Sprintf("%d", (k+1)*10) + "%",
				NumOfNodes: int64(-1),
				NodeNames:  []string{"N/A"},
			}
			nodeUtil = append(nodeUtil, util)
		}
		mapResult[k] = 0
		mapName[k] = []string{}
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

	conf := configs.ConfigContext.Get(schedulerContext.GetPolicyGroup())
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
		buildUpdateResponse(err, w)
		return
	}
	// validation is already called when loading the config
	var newConf *configs.SchedulerConfig
	newConf, err = configs.LoadSchedulerConfigFromByteArray(requestBytes)
	if err != nil {
		buildUpdateResponse(err, w)
		return
	}
	// This fails if we have more than 1 RM
	// Do not think the plugins will even work with multiple RMs
	var oldConf string
	oldConf, err = updateConfiguration(string(requestBytes))
	if err != nil {
		buildUpdateResponse(err, w)
		return
	}
	// This fails if we have no RM registered or more than 1 RM
	err = schedulerContext.UpdateSchedulerConfig(newConf)
	if err != nil {
		// revert configmap changes
		_, err2 := updateConfiguration(oldConf)
		if err2 != nil {
			err = fmt.Errorf("update failed: %s\nupdate rollback failed: %s", err.Error(), err2.Error())
		}
		buildUpdateResponse(err, w)
		return
	}
	buildUpdateResponse(nil, w)
}

func checkHealthStatus(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	metrics := metrics2.GetSchedulerMetrics()
	result := scheduler.GetSchedulerHealthStatus(metrics, schedulerContext)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func buildUpdateResponse(err error, w http.ResponseWriter) {
	if err == nil {
		w.WriteHeader(http.StatusOK)
		if _, err = w.Write([]byte("Configuration updated successfully")); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		log.Logger().Info("Configuration update failed with errors",
			zap.Error(err))
		http.Error(w, err.Error(), http.StatusConflict)
	}
}

func updateConfiguration(conf string) (string, error) {
	if plugin := plugins.GetConfigPlugin(); plugin != nil {
		// use the plugin to update the configuration in the configMap
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

func getPartitions(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	lists := gClusterInfo.ListPartitions()
	for _, k := range lists {
		partitionInfo := getPartition(k)

		if err := json.NewEncoder(w).Encode(partitionInfo); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func getPartition(name string) *dao.PartitionInfo {
	partitionInfo := &dao.PartitionInfo{}
	partitionContext := gClusterInfo.GetPartition(name)
	partitionInfo.Name = partitionContext.Name
	partitionInfo.State = partitionContext.StateMachine.Current()
	partitionInfo.LastStateTransitionTime = partitionContext.StateTime.String()

	capacityInfo := dao.PartitionCapacity{}
	capacityInfo.Capacity = partitionContext.GetTotalPartitionResource().String()
	capacityInfo.UsedCapacity = partitionContext.Root.GetAllocatedResource().String()
	partitionInfo.Capacity = capacityInfo
	partitionInfo.NodeSortingPolicy = partitionContext.GetNodeSortingPolicy().String()

	applicationsInfo := dao.Applications{}
	appList := partitionContext.GetApplications()
	applicationsState := make(map[string]int)

	for _, app := range appList {
		applicationsState[app.GetApplicationState()]++
	}
	applicationsInfo.Running = applicationsState["Running"]
	applicationsInfo.Pending = applicationsState["Waiting"] + applicationsState["Accepted"] + applicationsState["Starting"] + applicationsState["New"]
	applicationsInfo.Completed = applicationsState["Completed"]
	applicationsInfo.Failed = applicationsState["Killed"] + applicationsState["Rejected"]
	applicationsInfo.Total = applicationsInfo.Running + applicationsInfo.Pending + applicationsInfo.Completed + applicationsInfo.Failed
	partitionInfo.Applications = applicationsInfo
	return partitionInfo
}
