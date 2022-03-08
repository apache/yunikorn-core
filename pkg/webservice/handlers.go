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

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	metrics2 "github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics/history"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"

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

func getQueueInfo(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	lists := schedulerContext.GetPartitionMapClone()
	for _, partition := range lists {
		partitionInfo := getPartitionJSON(partition)

		if err := json.NewEncoder(w).Encode(partitionInfo); err != nil {
			buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
		}
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

func getClusterUtilization(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	lists := schedulerContext.GetPartitionMapClone()
	clusterUtil := getClustersUtilDAO(lists)
	if err := json.NewEncoder(w).Encode(clusterUtil); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getApplicationsInfo(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	queuePath := r.URL.Query().Get("queue")
	queueErr := validateQueue(queuePath)
	if queueErr != nil {
		buildJSONErrorResponse(w, queueErr.Error(), http.StatusBadRequest)
		return
	}

	lists := schedulerContext.GetPartitionMapClone()
	appsDao := make([]*dao.ApplicationDAOInfo, 0, len(lists))
	state := r.URL.Query().Get("applicationState")
	user := r.URL.Query().Get("user")
	addDao := func(app *objects.Application) {
		if (len(queuePath) == 0 || strings.EqualFold(queuePath, app.GetQueuePath())) &&
			(len(user) == 0 || strings.EqualFold(user, app.GetUser().User)) {
			appJSON := getApplicationJSON(app)
			// the application state may get changed before we convert it to dao
			if len(state) == 0 || strings.EqualFold(state, appJSON.State) {
				appsDao = append(appsDao, appJSON)
			}
		}
	}

	for _, partition := range lists {
		for _, app := range partition.GetApplications() {
			addDao(app)
		}
		for _, app := range partition.GetCompletedApplications() {
			addDao(app)
		}
		for _, app := range partition.GetRejectedApplications() {
			addDao(app)
		}
	}

	if err := json.NewEncoder(w).Encode(appsDao); err != nil {
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

func getNodesInfo(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	lists := schedulerContext.GetPartitionMapClone()
	result := getNodesDAO(lists)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getNodesUtilization(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	lists := schedulerContext.GetPartitionMapClone()
	result := getNodesUtilDAO(lists)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
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
	clusterInfo.StartTime = schedulerContext.GetStartTime().Format("2006-01-02T15:04:05-0700")
	rmInfos := schedulerContext.GetRMInfoMapClone()
	clusterInfo.RMBuildInformation = getRMBuildInformations(rmInfos)
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

	queueDAOInfo := partition.GetQueueInfos()

	partitionInfo.PartitionName = common.GetPartitionNameWithoutClusterID(partition.Name)
	partitionInfo.Capacity = dao.PartitionCapacity{
		Capacity:     partition.GetTotalPartitionResource().DAOString(),
		UsedCapacity: partition.GetAllocatedResource().DAOString(),
	}
	partitionInfo.Queues = queueDAOInfo

	return partitionInfo
}

func getApplicationJSON(app *objects.Application) *dao.ApplicationDAOInfo {
	allocations := app.GetAllAllocations()
	allocationInfos := make([]dao.AllocationDAOInfo, 0, len(allocations))
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
		ApplicationID:   app.ApplicationID,
		UsedResource:    app.GetAllocatedResource().DAOString(),
		MaxUsedResource: app.GetMaxAllocatedResource().DAOString(),
		Partition:       common.GetPartitionNameWithoutClusterID(app.Partition),
		QueueName:       app.QueuePath,
		SubmissionTime:  app.SubmissionTime.UnixNano(),
		FinishedTime:    common.ZeroTimeInUnixNano(app.FinishedTime()),
		Allocations:     allocationInfos,
		State:           app.CurrentState(),
		User:            app.GetUser().User,
		RejectedMessage: app.GetRejectedMessage(),
	}
}

func getNodeJSON(node *objects.Node) *dao.NodeDAOInfo {
	apps := node.GetAllAllocations()
	allocations := make([]*dao.AllocationDAOInfo, 0, len(apps))
	for _, alloc := range apps {
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
		Utilized:    node.GetUtilizedResource().DAOString(),
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
	metrics := metrics2.GetSchedulerMetrics()
	result := scheduler.GetSchedulerHealthStatus(metrics, schedulerContext)
	if !result.Healthy {
		log.Logger().Error("Scheduler is not healthy", zap.Any("health check values", result.HealthChecks))
		buildJSONErrorResponse(w, "Scheduler is not healthy", http.StatusServiceUnavailable)
	}
	if err := json.NewEncoder(w).Encode(result); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
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
		partitionInfo.LastStateTransitionTime = partitionContext.GetStateTime().String()

		capacityInfo := dao.PartitionCapacity{}
		capacity := partitionContext.GetTotalPartitionResource()
		usedCapacity := partitionContext.GetAllocatedResource()
		capacityInfo.Capacity = capacity.DAOString()
		capacityInfo.UsedCapacity = usedCapacity.DAOString()
		capacityInfo.Utilization = resources.CalculateAbsUsedCapacity(capacity, usedCapacity).DAOString()
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

		for _, app := range appList {
			result = append(result, getApplicationJSON(app))
		}
	}

	return result
}

func getPartitionDAO(lists map[string]*scheduler.PartitionContext) []*dao.PartitionDAOInfo {
	var result []*dao.PartitionDAOInfo

	for _, partition := range lists {
		result = append(result, getPartitionJSON(partition))
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

func getRMBuildInformations(lists map[string]*scheduler.RMInformation) []map[string]string {
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
