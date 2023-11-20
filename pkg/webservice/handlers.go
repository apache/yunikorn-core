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
	"sort"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/log"
	metrics2 "github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/metrics/history"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-core/pkg/scheduler"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

const (
	PartitionDoesNotExists   = "Partition not found"
	MissingParamsName        = "Missing parameters"
	QueueDoesNotExists       = "Queue not found"
	UserDoesNotExists        = "User not found"
	GroupDoesNotExists       = "Group not found"
	UserNameMissing          = "User name is missing"
	GroupNameMissing         = "Group name is missing"
	ApplicationDoesNotExists = "Application not found"
	NodeDoesNotExists        = "Node not found"
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
		log.Log(log.REST).Error("GetStackInfo error", zap.Error(err))
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
		log.Log(log.REST).Error(fmt.Sprintf("Problem in sending error response in JSON format. Error response: %s", detail))
	}
}

func getClusterJSON(partition *scheduler.PartitionContext) *dao.ClusterDAOInfo {
	clusterInfo := &dao.ClusterDAOInfo{}
	clusterInfo.StartTime = schedulerContext.GetStartTime().UnixNano()
	rmInfo := schedulerContext.GetRMInfoMapClone()
	clusterInfo.RMBuildInformation = getRMBuildInformation(rmInfo)
	clusterInfo.PartitionName = common.GetPartitionNameWithoutClusterID(partition.Name)
	clusterInfo.ClusterName = "kubernetes"
	return clusterInfo
}

func getClusterUtilJSON(partition *scheduler.PartitionContext) []*dao.ClusterUtilDAOInfo {
	var utils []*dao.ClusterUtilDAOInfo
	var getResource = true
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
				Usage:        fmt.Sprintf("%d%%", int64(value)),
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

func getAllocationDAO(alloc *objects.Allocation) *dao.AllocationDAOInfo {
	var requestTime int64
	if alloc.IsPlaceholderUsed() {
		requestTime = alloc.GetPlaceholderCreateTime().UnixNano()
	} else {
		requestTime = alloc.GetAsk().GetCreateTime().UnixNano()
	}
	allocTime := alloc.GetCreateTime().UnixNano()
	allocDAO := &dao.AllocationDAOInfo{
		AllocationKey:    alloc.GetAllocationKey(),
		AllocationTags:   alloc.GetTagsClone(),
		RequestTime:      requestTime,
		AllocationTime:   allocTime,
		AllocationDelay:  allocTime - requestTime,
		UUID:             alloc.GetUUID(),
		ResourcePerAlloc: alloc.GetAllocatedResource().DAOMap(),
		PlaceholderUsed:  alloc.IsPlaceholderUsed(),
		Placeholder:      alloc.IsPlaceholder(),
		TaskGroupName:    alloc.GetTaskGroup(),
		Priority:         strconv.Itoa(int(alloc.GetPriority())),
		NodeID:           alloc.GetNodeID(),
		ApplicationID:    alloc.GetApplicationID(),
		Partition:        alloc.GetPartitionName(),
		Preempted:        alloc.IsPreempted(),
	}
	return allocDAO
}

func getAllocationsDAO(allocations []*objects.Allocation) []*dao.AllocationDAOInfo {
	allocsDAO := make([]*dao.AllocationDAOInfo, 0, len(allocations))
	for _, alloc := range allocations {
		allocsDAO = append(allocsDAO, getAllocationDAO(alloc))
	}
	return allocsDAO
}

func getPlaceholderDAO(ph *objects.PlaceholderData) *dao.PlaceholderDAOInfo {
	phDAO := &dao.PlaceholderDAOInfo{
		TaskGroupName: ph.TaskGroupName,
		Count:         ph.Count,
		MinResource:   ph.MinResource.DAOMap(),
		Replaced:      ph.Replaced,
		TimedOut:      ph.TimedOut,
	}
	return phDAO
}

func getPlaceholdersDAO(entries []*objects.PlaceholderData) []*dao.PlaceholderDAOInfo {
	phsDAO := make([]*dao.PlaceholderDAOInfo, 0, len(entries))
	for _, entry := range entries {
		phsDAO = append(phsDAO, getPlaceholderDAO(entry))
	}
	return phsDAO
}

func getStateDAO(entry *objects.StateLogEntry) *dao.StateDAOInfo {
	state := &dao.StateDAOInfo{
		Time:             entry.Time.UnixNano(),
		ApplicationState: entry.ApplicationState,
	}
	return state
}

func getStatesDAO(entries []*objects.StateLogEntry) []*dao.StateDAOInfo {
	statesDAO := make([]*dao.StateDAOInfo, 0, len(entries))
	for _, entry := range entries {
		statesDAO = append(statesDAO, getStateDAO(entry))
	}
	return statesDAO
}

func getApplicationDAO(app *objects.Application) *dao.ApplicationDAOInfo {
	if app == nil {
		return &dao.ApplicationDAOInfo{}
	}

	return &dao.ApplicationDAOInfo{
		ApplicationID:      app.ApplicationID,
		UsedResource:       app.GetAllocatedResource().DAOMap(),
		MaxUsedResource:    app.GetMaxAllocatedResource().DAOMap(),
		PendingResource:    app.GetPendingResource().DAOMap(),
		Partition:          common.GetPartitionNameWithoutClusterID(app.Partition),
		QueueName:          app.GetQueuePath(),
		SubmissionTime:     app.SubmissionTime.UnixNano(),
		FinishedTime:       common.ZeroTimeInUnixNano(app.FinishedTime()),
		Requests:           getAllocationAsksDAO(app.GetAllRequests()),
		Allocations:        getAllocationsDAO(app.GetAllAllocations()),
		State:              app.CurrentState(),
		User:               app.GetUser().User,
		Groups:             app.GetUser().Groups,
		RejectedMessage:    app.GetRejectedMessage(),
		PlaceholderData:    getPlaceholdersDAO(app.GetAllPlaceholderData()),
		StateLog:           getStatesDAO(app.GetStateLog()),
		HasReserved:        app.HasReserved(),
		Reservations:       app.GetReservations(),
		MaxRequestPriority: app.GetAskMaxPriority(),
	}
}

func getAllocationLogsDAO(logEntries []*objects.AllocationLogEntry) []*dao.AllocationAskLogDAOInfo {
	logsDAO := make([]*dao.AllocationAskLogDAOInfo, len(logEntries))
	sort.SliceStable(logEntries, func(i, j int) bool {
		return logEntries[i].LastOccurrence.Before(logEntries[j].LastOccurrence)
	})
	for i, entry := range logEntries {
		logsDAO[i] = &dao.AllocationAskLogDAOInfo{
			Message:        entry.Message,
			LastOccurrence: entry.LastOccurrence.UnixNano(),
			Count:          entry.Count,
		}
	}
	return logsDAO
}

func getAllocationAskDAO(ask *objects.AllocationAsk) *dao.AllocationAskDAOInfo {
	return &dao.AllocationAskDAOInfo{
		AllocationKey:       ask.GetAllocationKey(),
		AllocationTags:      ask.GetTagsClone(),
		RequestTime:         ask.GetCreateTime().UnixNano(),
		ResourcePerAlloc:    ask.GetAllocatedResource().DAOMap(),
		PendingCount:        ask.GetPendingAskRepeat(),
		Priority:            strconv.Itoa(int(ask.GetPriority())),
		RequiredNodeID:      ask.GetRequiredNode(),
		ApplicationID:       ask.GetApplicationID(),
		Partition:           common.GetPartitionNameWithoutClusterID(ask.GetPartitionName()),
		Placeholder:         ask.IsPlaceholder(),
		PlaceholderTimeout:  ask.GetTimeout().Nanoseconds(),
		TaskGroupName:       ask.GetTaskGroup(),
		AllocationLog:       getAllocationLogsDAO(ask.GetAllocationLog()),
		TriggeredPreemption: ask.HasTriggeredPreemption(),
		Originator:          ask.IsOriginator(),
	}
}

func getAllocationAsksDAO(asks []*objects.AllocationAsk) []*dao.AllocationAskDAOInfo {
	asksDAO := make([]*dao.AllocationAskDAOInfo, 0, len(asks))
	for _, ask := range asks {
		if ask.GetPendingAskRepeat() > 0 {
			asksDAO = append(asksDAO, getAllocationAskDAO(ask))
		}
	}
	return asksDAO
}

func getNodeDAO(node *objects.Node) *dao.NodeDAOInfo {
	return &dao.NodeDAOInfo{
		NodeID:       node.NodeID,
		HostName:     node.Hostname,
		RackName:     node.Rackname,
		Attributes:   node.GetAttributes(),
		Capacity:     node.GetCapacity().DAOMap(),
		Occupied:     node.GetOccupiedResource().DAOMap(),
		Allocated:    node.GetAllocatedResource().DAOMap(),
		Available:    node.GetAvailableResource().DAOMap(),
		Utilized:     node.GetUtilizedResource().DAOMap(),
		Allocations:  getAllocationsDAO(node.GetAllAllocations()),
		Schedulable:  node.IsSchedulable(),
		IsReserved:   node.IsReserved(),
		Reservations: node.GetReservationKeys(),
	}
}

func getNodesDAO(entries []*objects.Node) []*dao.NodeDAOInfo {
	nodesDAO := make([]*dao.NodeDAOInfo, 0, len(entries))
	for _, entry := range entries {
		nodesDAO = append(nodesDAO, getNodeDAO(entry))
	}
	return nodesDAO
}

// getNodeUtilisation loads the node utilisation based on the dominant resource used
// for the default partition. Dominant resource is defined as the highest utilised resource
// type on the root queue based on the registered resources.
// Use the default partition until we get YUNIKORN-2088 fixed
func getNodeUtilisation(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	partitionContext := schedulerContext.GetPartitionWithoutClusterID(configs.DefaultPartition)
	if partitionContext == nil {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusBadRequest)
		return
	}
	// calculate the dominant resource based on root queue usage and size
	rootQ := partitionContext.GetQueue(configs.RootQueue)
	rootMax := rootQ.GetMaxResource()
	// if no nodes have been registered return an empty object
	nodesDao := &dao.NodesUtilDAOInfo{}
	if !resources.IsZero(rootMax) {
		// if nothing is used we get an empty dominant resource and return an empty object
		rootUsed := rootQ.GetAllocatedResource()
		dominant := rootUsed.DominantResourceType(rootMax)
		nodesDao = getNodesUtilJSON(partitionContext, dominant)
	}
	if err := json.NewEncoder(w).Encode(nodesDao); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

// getNodesUtilJSON loads the nodes utilisation for a partition for a specific resource type.
func getNodesUtilJSON(partition *scheduler.PartitionContext, name string) *dao.NodesUtilDAOInfo {
	mapResult := make([]int, 10)
	mapName := make([][]string, 10)
	var v float64
	var nodeUtil []*dao.NodeUtilDAOInfo
	var idx int
	for _, node := range partition.GetNodes() {
		// check resource exist or not: only count if node advertises the resource
		total := node.GetCapacity()
		if _, ok := total.Resources[name]; !ok {
			continue
		}
		resourceAllocated := node.GetAllocatedResource()
		// if resource exist in node, record the bucket it should go into,
		// otherwise none is used, and it should end up in the 0 bucket
		if _, ok := resourceAllocated.Resources[name]; ok {
			v = float64(resources.CalculateAbsUsedCapacity(total, resourceAllocated).Resources[name])
			idx = int(math.Dim(math.Ceil(v/10), 1))
		} else {
			idx = 0
		}
		mapResult[idx]++
		mapName[idx] = append(mapName[idx], node.NodeID)
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

	var marshalledConf []byte
	var err error

	conf := getClusterConfigDAO()

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

func getClusterConfigDAO() *dao.ConfigDAOInfo {
	// merge core config with extra config
	conf := dao.ConfigDAOInfo{
		SchedulerConfig: configs.ConfigContext.Get(schedulerContext.GetPolicyGroup()),
		Extra:           configs.GetConfigMap(),
	}

	return &conf
}

func checkHealthStatus(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	// Fetch last healthCheck result
	result := schedulerContext.GetLastHealthCheckResult()
	if result != nil {
		if !result.Healthy {
			log.Log(log.SchedHealth).Error("Scheduler is not healthy", zap.Any("health check info", *result))
			if err := json.NewEncoder(w).Encode(result); err != nil {
				buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
			}
		} else {
			log.Log(log.SchedHealth).Info("Scheduler is healthy", zap.Any("health check info", *result))
			if err := json.NewEncoder(w).Encode(result); err != nil {
				buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
			}
		}
	} else {
		log.Log(log.SchedHealth).Info("The healthy status of scheduler is not found", zap.Any("health check info", ""))
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
		log.Log(log.REST).Info("Configuration update failed with errors",
			zap.Error(err))
		buildJSONErrorResponse(w, err.Error(), http.StatusConflict)
	}
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
	writeHeaders(w)
	vars := httprouter.ParamsFromContext(r.Context())
	if vars == nil {
		buildJSONErrorResponse(w, MissingParamsName, http.StatusBadRequest)
		return
	}
	partitionName := vars.ByName("partition")
	var partitionQueuesDAOInfo dao.PartitionQueueDAOInfo
	var partition = schedulerContext.GetPartitionWithoutClusterID(partitionName)
	if partition != nil {
		partitionQueuesDAOInfo = partition.GetPartitionQueues()
	} else {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusBadRequest)
		return
	}
	if err := json.NewEncoder(w).Encode(partitionQueuesDAOInfo); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getPartitionNodes(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	vars := httprouter.ParamsFromContext(r.Context())
	if vars == nil {
		buildJSONErrorResponse(w, MissingParamsName, http.StatusBadRequest)
		return
	}
	partition := vars.ByName("partition")
	partitionContext := schedulerContext.GetPartitionWithoutClusterID(partition)
	if partitionContext != nil {
		nodesDao := getNodesDAO(partitionContext.GetNodes())
		if err := json.NewEncoder(w).Encode(nodesDao); err != nil {
			buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusBadRequest)
	}
}

func getPartitionNode(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	vars := httprouter.ParamsFromContext(r.Context())
	if vars == nil {
		buildJSONErrorResponse(w, MissingParamsName, http.StatusBadRequest)
		return
	}
	partition := vars.ByName("partition")
	partitionContext := schedulerContext.GetPartitionWithoutClusterID(partition)
	if partitionContext != nil {
		nodeID := vars.ByName("node")
		node := partitionContext.GetNode(nodeID)
		if node == nil {
			buildJSONErrorResponse(w, NodeDoesNotExists, http.StatusBadRequest)
			return
		}
		nodeDao := getNodeDAO(node)
		if err := json.NewEncoder(w).Encode(nodeDao); err != nil {
			buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusBadRequest)
	}
}

func getQueueApplications(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	vars := httprouter.ParamsFromContext(r.Context())
	if vars == nil {
		buildJSONErrorResponse(w, MissingParamsName, http.StatusBadRequest)
		return
	}
	partition := vars.ByName("partition")
	queueName := vars.ByName("queue")
	queueErr := validateQueue(queueName)
	if queueErr != nil {
		buildJSONErrorResponse(w, queueErr.Error(), http.StatusBadRequest)
		return
	}
	partitionContext := schedulerContext.GetPartitionWithoutClusterID(partition)
	if partitionContext == nil {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusBadRequest)
		return
	}
	queue := partitionContext.GetQueue(queueName)
	if queue == nil {
		buildJSONErrorResponse(w, QueueDoesNotExists, http.StatusBadRequest)
		return
	}

	appsDao := make([]*dao.ApplicationDAOInfo, 0)
	for _, app := range queue.GetCopyOfApps() {
		appsDao = append(appsDao, getApplicationDAO(app))
	}

	if err := json.NewEncoder(w).Encode(appsDao); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getApplication(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	vars := httprouter.ParamsFromContext(r.Context())
	if vars == nil {
		buildJSONErrorResponse(w, MissingParamsName, http.StatusBadRequest)
		return
	}
	partition := vars.ByName("partition")
	queueName := vars.ByName("queue")
	application := vars.ByName("application")
	queueErr := validateQueue(queueName)
	if queueErr != nil {
		buildJSONErrorResponse(w, queueErr.Error(), http.StatusBadRequest)
		return
	}
	partitionContext := schedulerContext.GetPartitionWithoutClusterID(partition)
	if partitionContext == nil {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusBadRequest)
		return
	}
	queue := partitionContext.GetQueue(queueName)
	if queue == nil {
		buildJSONErrorResponse(w, QueueDoesNotExists, http.StatusBadRequest)
		return
	}
	app := queue.GetApplication(application)
	if app == nil {
		buildJSONErrorResponse(w, ApplicationDoesNotExists, http.StatusBadRequest)
		return
	}
	appDao := getApplicationDAO(app)
	if err := json.NewEncoder(w).Encode(appDao); err != nil {
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

		partitionInfo.TotalNodes = partitionContext.GetTotalNodeCount()
		appList := partitionContext.GetApplications()
		appList = append(appList, partitionContext.GetCompletedApplications()...)
		appList = append(appList, partitionContext.GetRejectedApplications()...)
		applicationsState := make(map[string]int)
		totalApplications := 0
		for _, app := range appList {
			applicationsState[app.CurrentState()]++
			totalApplications++
		}
		applicationsState["total"] = totalApplications
		partitionInfo.Applications = applicationsState
		partitionInfo.TotalContainers = partitionContext.GetTotalAllocationCount()
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

func getPartitionNodesDAO(lists map[string]*scheduler.PartitionContext) []*dao.NodesDAOInfo {
	var result []*dao.NodesDAOInfo

	for _, partition := range lists {
		nodesDao := getNodesDAO(partition.GetNodes())
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

func getApplicationsDAO(lists map[string]*scheduler.PartitionContext) []*dao.ApplicationDAOInfo {
	var result []*dao.ApplicationDAOInfo

	for _, partition := range lists {
		var appList []*objects.Application
		appList = append(appList, partition.GetApplications()...)
		appList = append(appList, partition.GetCompletedApplications()...)
		appList = append(appList, partition.GetRejectedApplications()...)

		for _, app := range appList {
			result = append(result, getApplicationDAO(app))
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

func getResourceManagerDiagnostics() map[string]interface{} {
	result := make(map[string]interface{}, 0)

	plugin := plugins.GetStateDumpPlugin()

	// get state dump from RM
	dumpStr, err := plugin.GetStateDump()
	if err != nil {
		// might be not implemented
		log.Log(log.REST).Debug("Unable to get RM state dump", zap.Error(err))
		result["Error"] = err.Error()
		return result
	}

	// convert to JSON map
	if err = json.Unmarshal([]byte(dumpStr), &result); err != nil {
		log.Log(log.REST).Warn("Unable to parse RM state dump", zap.Error(err))
		result["Error"] = err.Error()
	}

	return result
}

func getMetrics(w http.ResponseWriter, r *http.Request) {
	metrics2.GetRuntimeMetrics().Collect()
	promhttp.Handler().ServeHTTP(w, r)
}

func getUsersResourceUsage(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	userManager := ugm.GetUserManager()
	usersResources := userManager.GetUsersResources()
	var result []*dao.UserResourceUsageDAOInfo
	for _, tracker := range usersResources {
		result = append(result, tracker.GetUserResourceUsageDAOInfo())
	}
	if err := json.NewEncoder(w).Encode(result); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getUserResourceUsage(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	vars := httprouter.ParamsFromContext(r.Context())
	if vars == nil {
		buildJSONErrorResponse(w, MissingParamsName, http.StatusBadRequest)
		return
	}
	user := vars.ByName("user")
	if user == "" {
		buildJSONErrorResponse(w, UserNameMissing, http.StatusBadRequest)
		return
	}
	userTracker := ugm.GetUserManager().GetUserTracker(user)
	if userTracker == nil {
		buildJSONErrorResponse(w, UserDoesNotExists, http.StatusBadRequest)
		return
	}
	var result = userTracker.GetUserResourceUsageDAOInfo()
	if err := json.NewEncoder(w).Encode(result); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getGroupsResourceUsage(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	userManager := ugm.GetUserManager()
	groupsResources := userManager.GetGroupsResources()
	var result []*dao.GroupResourceUsageDAOInfo
	for _, tracker := range groupsResources {
		result = append(result, tracker.GetGroupResourceUsageDAOInfo())
	}
	if err := json.NewEncoder(w).Encode(result); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getGroupResourceUsage(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	vars := httprouter.ParamsFromContext(r.Context())
	if vars == nil {
		buildJSONErrorResponse(w, MissingParamsName, http.StatusBadRequest)
		return
	}
	group := vars.ByName("group")
	if group == "" {
		buildJSONErrorResponse(w, GroupNameMissing, http.StatusBadRequest)
		return
	}
	groupTracker := ugm.GetUserManager().GetGroupTracker(group)
	if groupTracker == nil {
		buildJSONErrorResponse(w, GroupDoesNotExists, http.StatusBadRequest)
		return
	}
	var result = groupTracker.GetGroupResourceUsageDAOInfo()
	if err := json.NewEncoder(w).Encode(result); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getEvents(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	eventSystem := events.GetEventSystem()
	if !eventSystem.IsEventTrackingEnabled() {
		buildJSONErrorResponse(w, "Event tracking is disabled", http.StatusBadRequest)
		return
	}

	count := uint64(10000)
	var start uint64

	if countStr := r.URL.Query().Get("count"); countStr != "" {
		c, err := strconv.ParseUint(countStr, 10, 64)
		if err != nil {
			buildJSONErrorResponse(w, err.Error(), http.StatusBadRequest)
			return
		}
		if c == 0 {
			buildJSONErrorResponse(w, `0 is not a valid value for "count"`, http.StatusBadRequest)
			return
		}
		count = c
	}

	if startStr := r.URL.Query().Get("start"); startStr != "" {
		i, err := strconv.ParseUint(startStr, 10, 64)
		if err != nil {
			buildJSONErrorResponse(w, err.Error(), http.StatusBadRequest)
			return
		}
		start = i
	}

	records, lowestID, highestID := eventSystem.GetEventsFromID(start, count)
	eventDao := dao.EventRecordDAO{
		InstanceUUID: schedulerContext.GetUUID(),
		LowestID:     lowestID,
		HighestID:    highestID,
		EventRecords: records,
	}
	if err := json.NewEncoder(w).Encode(eventDao); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}
