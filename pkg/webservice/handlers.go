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
	"io"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	metrics2 "github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/metrics/history"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-core/pkg/scheduler"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"

	"github.com/gorilla/mux"
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
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getClusterInfo(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	lists := schedulerContext.GetPartitionMapClone()
	clustersInfo := getClusterDAO(lists)
	if err := json.NewEncoder(w).Encode(clustersInfo); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func validateQueue(queuePath string) error {
	if queuePath != "" {
		queueNameArr := strings.Split(queuePath, ".")
		for _, name := range queueNameArr {
			if !configs.QueueNameRegExp.MatchString(name) {
				return fmt.Errorf("problem in queue query parameter parsing as queue param "+
					"%s contains invalid queue name %s. Queue name must only have "+
					"alphanumeric characters, - or _, and be no longer than 64 characters", queuePath, name)
			}
		}
	}
	return nil
}

func validateConf(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	requestBytes, err := io.ReadAll(r.Body)
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
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Origin")
}

func buildJSONErrorResponse(w http.ResponseWriter, detail string, code int) {
	w.WriteHeader(code)
	errorInfo := dao.NewYAPIError(nil, code, detail)
	if jsonErr := json.NewEncoder(w).Encode(errorInfo); jsonErr != nil {
		log.Logger().Error(fmt.Sprintf("Problem in sending error response in JSON format. Error response: %s", detail))
	}
}

func getClusterJSON(partition *scheduler.PartitionContext) *dao.ClusterDAOInfo {
	clusterInfo := &dao.ClusterDAOInfo{}
	clusterInfo.StartTime = schedulerContext.GetStartTime().UnixNano()
	rmInfo := schedulerContext.GetRMInfoMapClone()
	clusterInfo.RMBuildInformation = getRMBuildInformation(rmInfo)
	clusterInfo.PartitionName = common.GetPartitionNameWithoutClusterID(partition.Name)
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

	queueDAOInfo := partition.GetQueueInfo()

	partitionInfo.PartitionName = common.GetPartitionNameWithoutClusterID(partition.Name)
	capacity := partition.GetTotalPartitionResource()
	usedCapacity := partition.GetAllocatedResource()
	partitionInfo.Capacity = dao.PartitionCapacity{
		Capacity:     capacity.DAOMap(),
		UsedCapacity: usedCapacity.DAOMap(),
		Utilization:  resources.CalculateAbsUsedCapacity(capacity, usedCapacity).DAOMap(),
	}
	partitionInfo.Queues = queueDAOInfo

	return partitionInfo
}

func getApplicationJSON(app *objects.Application) *dao.ApplicationDAOInfo {
	allocations := app.GetAllAllocations()
	allocationInfo := make([]dao.AllocationDAOInfo, 0, len(allocations))
	placeholders := app.GetAllPlaceholderData()
	placeholderInfo := make([]dao.PlaceholderDAOInfo, 0, len(placeholders))
	var requestTime int64

	for _, alloc := range allocations {
		if alloc.GetPlaceholderUsed() {
			requestTime = alloc.GetPlaceholderCreateTime().UnixNano()
		} else {
			requestTime = alloc.Ask.GetCreateTime().UnixNano()
		}
		allocTime := alloc.GetCreateTime().UnixNano()
		allocInfo := dao.AllocationDAOInfo{
			AllocationKey:    alloc.AllocationKey,
			AllocationTags:   alloc.Tags,
			RequestTime:      requestTime,
			AllocationTime:   allocTime,
			AllocationDelay:  allocTime - requestTime,
			UUID:             alloc.UUID,
			ResourcePerAlloc: alloc.AllocatedResource.DAOMap(),
			PlaceholderUsed:  alloc.GetPlaceholderUsed(),
			Priority:         strconv.Itoa(int(alloc.Priority)),
			QueueName:        alloc.QueueName,
			NodeID:           alloc.NodeID,
			ApplicationID:    alloc.ApplicationID,
			Partition:        alloc.PartitionName,
		}
		allocationInfo = append(allocationInfo, allocInfo)
	}
	stateLog := app.GetStateLog()
	stateLogInfo := make([]dao.StateDAOInfo, 0, len(stateLog))
	for _, state := range stateLog {
		stateInfo := dao.StateDAOInfo{
			Time:             state.Time.UnixNano(),
			ApplicationState: state.ApplicationState,
		}
		stateLogInfo = append(stateLogInfo, stateInfo)
	}

	for _, taskGroup := range placeholders {
		phInfo := dao.PlaceholderDAOInfo{
			TaskGroupName: taskGroup.TaskGroupName,
			Count:         taskGroup.Count,
			MinResource:   taskGroup.MinResource.DAOMap(),
			Replaced:      taskGroup.Replaced,
			TimedOut:      taskGroup.TimedOut,
		}
		placeholderInfo = append(placeholderInfo, phInfo)
	}

	return &dao.ApplicationDAOInfo{
		ApplicationID:   app.ApplicationID,
		UsedResource:    app.GetAllocatedResource().DAOMap(),
		MaxUsedResource: app.GetMaxAllocatedResource().DAOMap(),
		Partition:       common.GetPartitionNameWithoutClusterID(app.Partition),
		QueueName:       app.GetQueuePath(),
		SubmissionTime:  app.SubmissionTime.UnixNano(),
		FinishedTime:    common.ZeroTimeInUnixNano(app.FinishedTime()),
		Allocations:     allocationInfo,
		State:           app.CurrentState(),
		User:            app.GetUser().User,
		RejectedMessage: app.GetRejectedMessage(),
		PlaceholderData: placeholderInfo,
		StateLog:        stateLogInfo,
	}
}

func getNodeJSON(node *objects.Node) *dao.NodeDAOInfo {
	apps := node.GetAllAllocations()
	allocations := make([]*dao.AllocationDAOInfo, 0, len(apps))
	var requestTime int64
	for _, alloc := range apps {
		if alloc.GetPlaceholderUsed() {
			requestTime = alloc.GetPlaceholderCreateTime().UnixNano()
		} else {
			requestTime = alloc.Ask.GetCreateTime().UnixNano()
		}
		allocTime := alloc.GetCreateTime().UnixNano()
		allocInfo := &dao.AllocationDAOInfo{
			AllocationKey:    alloc.AllocationKey,
			AllocationTags:   alloc.Tags,
			RequestTime:      requestTime,
			AllocationTime:   allocTime,
			AllocationDelay:  allocTime - requestTime,
			UUID:             alloc.UUID,
			ResourcePerAlloc: alloc.AllocatedResource.DAOMap(),
			Priority:         strconv.Itoa(int(alloc.Priority)),
			QueueName:        alloc.QueueName,
			NodeID:           alloc.NodeID,
			ApplicationID:    alloc.ApplicationID,
			PlaceholderUsed:  alloc.GetPlaceholderUsed(),
			Partition:        alloc.PartitionName,
		}
		allocations = append(allocations, allocInfo)
	}

	return &dao.NodeDAOInfo{
		NodeID:      node.NodeID,
		HostName:    node.Hostname,
		RackName:    node.Rackname,
		Capacity:    node.GetCapacity().DAOMap(),
		Occupied:    node.GetOccupiedResource().DAOMap(),
		Allocated:   node.GetAllocatedResource().DAOMap(),
		Available:   node.GetAvailableResource().DAOMap(),
		Utilized:    node.GetUtilizedResource().DAOMap(),
		Allocations: allocations,
		Schedulable: node.IsSchedulable(),
	}
}

func getNodesUtilJSON(partition *scheduler.PartitionContext, name string) *dao.NodesUtilDAOInfo {
	mapResult := make([]int, 10)
	mapName := make([][]string, 10)
	var v float64
	var nodeUtil []*dao.NodeUtilDAOInfo
	for _, node := range partition.GetNodes() {
		resourceExist := true
		// check resource exist or not
		total := node.GetCapacity()
		if total.Resources[name] <= 0 {
			resourceExist = false
		}
		resourceAllocated := node.GetAllocatedResource()
		if _, ok := resourceAllocated.Resources[name]; !ok {
			resourceExist = false
		}
		// if resource exist in node, record the bucket it should go
		if resourceExist {
			v = float64(resources.CalculateAbsUsedCapacity(total, resourceAllocated).Resources[name])
			idx := int(math.Dim(math.Ceil(v/10), 1))
			mapResult[idx]++
			mapName[idx] = append(mapName[idx], node.NodeID)
		}
	}
	// put number of nodes and node name to different buckets
	for k := 0; k < 10; k++ {
		util := &dao.NodeUtilDAOInfo{
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
		buildJSONErrorResponse(w, "Internal metrics collection is not enabled.", http.StatusNotImplemented)
		return
	}
	// get a copy of the records: if the array contains nil values they will always be at the
	// start and we cannot shortcut the loop using a break, we must finish iterating
	records := imHistory.GetRecords()
	result := getAppHistoryDAO(records)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getContainerHistory(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	// There is nothing to return but we did not really encounter a problem
	if imHistory == nil {
		buildJSONErrorResponse(w, "Internal metrics collection is not enabled.", http.StatusNotImplemented)
		return
	}
	// get a copy of the records: if the array contains nil values they will always be at the
	// start and we cannot shortcut the loop using a break, we must finish iterating
	records := imHistory.GetRecords()
	result := getContainerHistoryDAO(records)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
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
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
	if _, err = w.Write(marshalledConf); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func createClusterConfig(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	queryParams := r.URL.Query()
	dryRun, dryRunExists := queryParams["dry_run"]
	if !dryRunExists {
		buildJSONErrorResponse(w, "Dry run param is missing. Please check the usage documentation", http.StatusBadRequest)
		return
	}
	if dryRun[0] != "1" {
		buildJSONErrorResponse(w, "Invalid \"dry_run\" query param. Currently, only dry_run=1 is supported. Please check the usage documentation", http.StatusBadRequest)
		return
	}
	if len(queryParams) != 1 {
		buildJSONErrorResponse(w, "Invalid query parameters. Please check the usage documentation", http.StatusBadRequest)
		return
	}
	requestBytes, err := io.ReadAll(r.Body)
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
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func updateClusterConfig(w http.ResponseWriter, r *http.Request) {
	lock.Lock()
	defer lock.Unlock()
	writeHeaders(w)
	requestBytes, err := io.ReadAll(r.Body)
	if err != nil {
		buildUpdateResponse(err, w)
		return
	}
	newConf, err := configs.ParseAndValidateConfig(requestBytes)
	if err != nil {
		buildUpdateResponse(err, w)
		return
	}
	if !isChecksumEqual(newConf.Checksum) {
		buildUpdateResponse(fmt.Errorf("the base configuration is changed"), w)
		return
	}
	configs.SetChecksum(requestBytes, newConf)
	newConfStr := configs.GetConfigurationString(requestBytes)
	// This fails if we have more than 1 RM
	// Do not think the plugins will even work with multiple RMs
	var oldConf string
	oldConf, err = updateConfiguration(newConfStr)
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

func isChecksumEqual(checksum string) bool {
	return configs.ConfigContext.Get(schedulerContext.GetPolicyGroup()).Checksum == checksum
}

func checkHealthStatus(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	// Fetch last healthCheck result
	result := schedulerContext.GetLastHealthCheckResult()
	if result != nil {
		if !result.Healthy {
			log.Logger().Error("Scheduler is not healthy", zap.Any("health check info", *result))
			buildJSONErrorResponse(w, "Scheduler is not healthy", http.StatusServiceUnavailable)
		} else {
			log.Logger().Info("Scheduler is healthy", zap.Any("health check info", *result))
			if err := json.NewEncoder(w).Encode(result); err != nil {
				buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
			}
		}
	} else {
		log.Logger().Info("The healthy status of scheduler is not found", zap.Any("health check info", ""))
		buildJSONErrorResponse(w, "The healthy status of scheduler is not found", http.StatusNotFound)
	}
}

func buildUpdateResponse(err error, w http.ResponseWriter) {
	if err == nil {
		w.WriteHeader(http.StatusOK)
		if _, err = w.Write([]byte("Configuration updated successfully")); err != nil {
			buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		log.Logger().Info("Configuration update failed with errors",
			zap.Error(err))
		buildJSONErrorResponse(w, err.Error(), http.StatusConflict)
	}
}

func updateConfiguration(conf string) (string, error) {
	if plugin := plugins.GetResourceManagerCallbackPlugin(); plugin != nil {
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

	lists := schedulerContext.GetPartitionMapClone()
	partitionsInfo := getPartitionInfoDAO(lists)
	if err := json.NewEncoder(w).Encode(partitionsInfo); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getPartitionQueues(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	writeHeaders(w)
	partitionName, partitionExists := vars["partition"]
	if !partitionExists {
		buildJSONErrorResponse(w, "Partition is missing in URL path. Please check the usage documentation", http.StatusBadRequest)
		return
	}
	if len(vars) != 1 {
		buildJSONErrorResponse(w, "Incorrect URL path. Please check the usage documentation", http.StatusBadRequest)
		return
	}
	var partitionQueuesDAOInfo dao.PartitionQueueDAOInfo
	var partition = schedulerContext.GetPartitionWithoutClusterID(partitionName)
	if partition != nil {
		partitionQueuesDAOInfo = partition.GetPartitionQueues()
	} else {
		buildJSONErrorResponse(w, "Partition not found", http.StatusBadRequest)
		return
	}
	if err := json.NewEncoder(w).Encode(partitionQueuesDAOInfo); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getPartitionNodes(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	writeHeaders(w)
	partition, partitionExists := vars["partition"]
	if !partitionExists {
		buildJSONErrorResponse(w, "Partition is missing in URL path. Please check the usage documentation", http.StatusBadRequest)
		return
	}
	if len(vars) != 1 {
		buildJSONErrorResponse(w, "Incorrect URL path. Please check the usage documentation", http.StatusBadRequest)
		return
	}
	partitionContext := schedulerContext.GetPartitionWithoutClusterID(partition)
	if partitionContext != nil {
		ns := partitionContext.GetNodes()
		nodesDao := make([]*dao.NodeDAOInfo, 0, len(ns))
		for _, node := range ns {
			nodeDao := getNodeJSON(node)
			nodesDao = append(nodesDao, nodeDao)
		}
		if err := json.NewEncoder(w).Encode(nodesDao); err != nil {
			buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		buildJSONErrorResponse(w, "Partition not found", http.StatusBadRequest)
	}
}

func getQueueApplications(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	writeHeaders(w)
	partition, partitionExists := vars["partition"]
	if !partitionExists {
		buildJSONErrorResponse(w, "Partition is missing in URL path. Please check the usage documentation", http.StatusBadRequest)
		return
	}
	queueName, queueNameExists := vars["queue"]
	if !queueNameExists {
		buildJSONErrorResponse(w, "Queue is missing in URL path. Please check the usage documentation", http.StatusBadRequest)
		return
	}
	queueErr := validateQueue(queueName)
	if queueErr != nil {
		buildJSONErrorResponse(w, queueErr.Error(), http.StatusBadRequest)
		return
	}
	if len(vars) != 2 {
		buildJSONErrorResponse(w, "Incorrect URL path. Please check the usage documentation", http.StatusBadRequest)
		return
	}

	partitionContext := schedulerContext.GetPartitionWithoutClusterID(partition)
	if partitionContext == nil {
		buildJSONErrorResponse(w, "Partition not found", http.StatusBadRequest)
		return
	}

	queue := partitionContext.GetQueue(queueName)
	if queue == nil {
		buildJSONErrorResponse(w, "Queue not found", http.StatusBadRequest)
		return
	}

	apps := queue.GetCopyOfApps()
	completedApps := queue.GetCopyOfCompletedApps()
	appsDao := make([]*dao.ApplicationDAOInfo, 0, len(apps)+len(completedApps))
	for _, app := range apps {
		appsDao = append(appsDao, getApplicationJSON(app))
	}
	for _, app := range completedApps {
		appsDao = append(appsDao, getApplicationJSON(app))
	}

	if err := json.NewEncoder(w).Encode(appsDao); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func setLogLevel(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	writeHeaders(w)
	level := vars["level"]
	if err := log.SetLogLevel(level); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusBadRequest)
	}
}

func getLogLevel(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	zapConfig := log.GetConfig()
	if _, err := w.Write([]byte(zapConfig.Level.Level().String())); err != nil {
		log.Logger().Error("Could not get log level", zap.Error(err))
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getPartitionInfoDAO(lists map[string]*scheduler.PartitionContext) []*dao.PartitionInfo {
	var result []*dao.PartitionInfo

	for _, partitionContext := range lists {
		partitionInfo := &dao.PartitionInfo{}
		partitionInfo.ClusterID = partitionContext.RmID
		partitionInfo.Name = common.GetPartitionNameWithoutClusterID(partitionContext.Name)
		partitionInfo.State = partitionContext.GetCurrentState()
		partitionInfo.LastStateTransitionTime = partitionContext.GetStateTime().UnixNano()

		capacityInfo := dao.PartitionCapacity{}
		capacity := partitionContext.GetTotalPartitionResource()
		usedCapacity := partitionContext.GetAllocatedResource()
		capacityInfo.Capacity = capacity.DAOMap()
		capacityInfo.UsedCapacity = usedCapacity.DAOMap()
		capacityInfo.Utilization = resources.CalculateAbsUsedCapacity(capacity, usedCapacity).DAOMap()
		partitionInfo.Capacity = capacityInfo
		partitionInfo.NodeSortingPolicy = dao.NodeSortingPolicy{
			Type:            partitionContext.GetNodeSortingPolicyType().String(),
			ResourceWeights: partitionContext.GetNodeSortingResourceWeights(),
		}

		appList := partitionContext.GetApplications()
		appList = append(appList, partitionContext.GetCompletedApplications()...)
		applicationsState := make(map[string]int)
		totalApplications := 0
		for _, app := range appList {
			applicationsState[app.CurrentState()]++
			totalApplications++
		}
		applicationsState["total"] = totalApplications
		partitionInfo.Applications = applicationsState
		result = append(result, partitionInfo)
	}

	return result
}

func getAppHistoryDAO(records []*history.MetricsRecord) []*dao.ApplicationHistoryDAOInfo {
	var result []*dao.ApplicationHistoryDAOInfo

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

	return result
}

func getNodesDAO(lists map[string]*scheduler.PartitionContext) []*dao.NodesDAOInfo {
	var result []*dao.NodesDAOInfo

	for _, partition := range lists {
		ns := partition.GetNodes()
		nodesDao := make([]*dao.NodeDAOInfo, 0, len(ns))
		for _, node := range ns {
			nodeDao := getNodeJSON(node)
			nodesDao = append(nodesDao, nodeDao)
		}
		result = append(result, &dao.NodesDAOInfo{
			PartitionName: common.GetPartitionNameWithoutClusterID(partition.Name),
			Nodes:         nodesDao,
		})
	}

	return result
}

func getContainerHistoryDAO(records []*history.MetricsRecord) []*dao.ContainerHistoryDAOInfo {
	var result []*dao.ContainerHistoryDAOInfo

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

	return result
}

func getClustersUtilDAO(lists map[string]*scheduler.PartitionContext) []*dao.ClustersUtilDAOInfo {
	var result []*dao.ClustersUtilDAOInfo

	for _, partition := range lists {
		result = append(result, &dao.ClustersUtilDAOInfo{
			PartitionName: common.GetPartitionNameWithoutClusterID(partition.Name),
			ClustersUtil:  getClusterUtilJSON(partition),
		})
	}

	return result
}
func getNodesUtilDAO(lists map[string]*scheduler.PartitionContext) []*dao.NodesUtilDAOInfo {
	var result []*dao.NodesUtilDAOInfo

	for _, partition := range lists {
		partitionResource := partition.GetTotalPartitionResource()
		// partitionResource can be null if the partition has no node
		if partitionResource != nil {
			for name := range partitionResource.Resources {
				result = append(result, getNodesUtilJSON(partition, name))
			}
		}
	}

	return result
}

func getApplicationsDAO(lists map[string]*scheduler.PartitionContext) []*dao.ApplicationDAOInfo {
	var result []*dao.ApplicationDAOInfo

	for _, partition := range lists {
		var appList []*objects.Application
		appList = append(appList, partition.GetApplications()...)
		appList = append(appList, partition.GetCompletedApplications()...)
		appList = append(appList, partition.GetRejectedApplications()...)

		for _, app := range appList {
			result = append(result, getApplicationJSON(app))
		}
	}

	return result
}

func getPartitionQueuesDAO(lists map[string]*scheduler.PartitionContext) []dao.PartitionQueueDAOInfo {
	var result []dao.PartitionQueueDAOInfo

	for _, partition := range lists {
		result = append(result, partition.GetPartitionQueues())
	}

	return result
}

func getClusterDAO(lists map[string]*scheduler.PartitionContext) []*dao.ClusterDAOInfo {
	var result []*dao.ClusterDAOInfo

	for _, partition := range lists {
		result = append(result, getClusterJSON(partition))
	}

	return result
}

func getRMBuildInformation(lists map[string]*scheduler.RMInformation) []map[string]string {
	var result []map[string]string

	for _, rmInfo := range lists {
		result = append(result, rmInfo.RMBuildInformation)
	}

	return result
}

func getMetrics(w http.ResponseWriter, r *http.Request) {
	metrics2.GetRuntimeMetrics().Collect()
	promhttp.Handler().ServeHTTP(w, r)
}
