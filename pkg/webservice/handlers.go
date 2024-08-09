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
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/locking"
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
	InvalidUserName          = "Invalid user name"
	InvalidGroupName         = "Invalid group name"
	UserDoesNotExists        = "User not found"
	GroupDoesNotExists       = "Group not found"
	UserNameMissing          = "User name is missing"
	GroupNameMissing         = "Group name is missing"
	ApplicationDoesNotExists = "Application not found"
	NodeDoesNotExists        = "Node not found"

	AppStateActive    = "active"
	AppStateRejected  = "rejected"
	AppStateCompleted = "completed"
)

var allowedActiveStatusMsg string
var allowedAppActiveStatuses map[string]bool
var streamingLimiter *StreamingLimiter
var maxRESTResponseSize atomic.Uint64

func init() {
	allowedAppActiveStatuses = make(map[string]bool)

	allowedAppActiveStatuses[strings.ToLower(objects.New.String())] = true
	allowedAppActiveStatuses[strings.ToLower(objects.Accepted.String())] = true
	allowedAppActiveStatuses[strings.ToLower(objects.Running.String())] = true
	allowedAppActiveStatuses[strings.ToLower(objects.Completing.String())] = true
	allowedAppActiveStatuses[strings.ToLower(objects.Failing.String())] = true
	allowedAppActiveStatuses[strings.ToLower(objects.Resuming.String())] = true

	var activeStatuses []string
	for k := range allowedAppActiveStatuses {
		activeStatuses = append(activeStatuses, k)
	}
	allowedActiveStatusMsg = fmt.Sprintf("Only following active statuses are allowed: %s", strings.Join(activeStatuses, ","))

	streamingLimiter = NewStreamingLimiter()

	configs.AddConfigMapCallback("rest-response-size", func() {
		newSize := common.GetConfigurationUint(configs.GetConfigMap(), configs.CMRESTResponseSize, configs.DefaultRESTResponseSize)
		if newSize == 0 {
			log.Log(log.REST).Warn("Illegal value `0` for config key, using default",
				zap.String("key", configs.CMRESTResponseSize),
				zap.Uint64("default", configs.DefaultRESTResponseSize))
			newSize = configs.DefaultRESTResponseSize
		}

		log.Log(log.REST).Info("Reloading max REST event response size setting",
			zap.Uint64("current", maxRESTResponseSize.Load()),
			zap.Uint64("new", newSize))
		maxRESTResponseSize.Store(newSize)
	})
	maxRESTResponseSize.Store(configs.DefaultRESTResponseSize)
}

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

	lists := schedulerContext.Load().GetPartitionMapClone()
	clustersInfo := getClusterDAO(lists)
	if err := json.NewEncoder(w).Encode(clustersInfo); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func validateQueue(queuePath string) error {
	if queuePath != "" {
		queueNameArr := strings.Split(queuePath, ".")
		for _, name := range queueNameArr {
			if err := configs.IsQueueNameValid(name); err != nil {
				return err
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
	ctx := schedulerContext.Load()
	clusterInfo.StartTime = ctx.GetStartTime().UnixNano()
	rmInfo := ctx.GetRMInfoMapClone()
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
		requestTime = alloc.GetCreateTime().UnixNano()
	}
	allocTime := alloc.GetCreateTime().UnixNano()
	allocDAO := &dao.AllocationDAOInfo{
		AllocationKey:    alloc.GetAllocationKey(),
		AllocationTags:   alloc.GetTagsClone(),
		RequestTime:      requestTime,
		AllocationTime:   allocTime,
		AllocationDelay:  allocTime - requestTime,
		ResourcePerAlloc: alloc.GetAllocatedResource().DAOMap(),
		PlaceholderUsed:  alloc.IsPlaceholderUsed(),
		Placeholder:      alloc.IsPlaceholder(),
		TaskGroupName:    alloc.GetTaskGroup(),
		Priority:         strconv.Itoa(int(alloc.GetPriority())),
		NodeID:           alloc.GetNodeID(),
		ApplicationID:    alloc.GetApplicationID(),
		Preempted:        alloc.IsPreempted(),
		Originator:       alloc.IsOriginator(),
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

func getApplicationDAO(app *objects.Application, summary *objects.ApplicationSummary) *dao.ApplicationDAOInfo {
	if app == nil {
		return &dao.ApplicationDAOInfo{}
	}

	return &dao.ApplicationDAOInfo{
		ApplicationID:       app.ApplicationID,
		UsedResource:        app.GetAllocatedResource().DAOMap(),
		MaxUsedResource:     app.GetMaxAllocatedResource().DAOMap(),
		PendingResource:     app.GetPendingResource().DAOMap(),
		Partition:           common.GetPartitionNameWithoutClusterID(app.Partition),
		QueueName:           app.GetQueuePath(),
		SubmissionTime:      app.SubmissionTime.UnixNano(),
		FinishedTime:        common.ZeroTimeInUnixNano(app.FinishedTime()),
		Requests:            getAllocationAsksDAO(app.GetAllRequests()),
		Allocations:         getAllocationsDAO(app.GetAllAllocations()),
		State:               app.CurrentState(),
		User:                app.GetUser().User,
		Groups:              app.GetUser().Groups,
		RejectedMessage:     app.GetRejectedMessage(),
		PlaceholderData:     getPlaceholdersDAO(app.GetAllPlaceholderData()),
		StateLog:            getStatesDAO(app.GetStateLog()),
		HasReserved:         app.HasReserved(),
		Reservations:        app.GetReservations(),
		MaxRequestPriority:  app.GetAskMaxPriority(),
		StartTime:           app.StartTime().UnixMilli(),
		ResourceUsage:       summary.ResourceUsage,
		PreemptedResource:   summary.PreemptedResource,
		PlaceholderResource: summary.PlaceholderResource,
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

func getAllocationAskDAO(ask *objects.Allocation) *dao.AllocationAskDAOInfo {
	return &dao.AllocationAskDAOInfo{
		AllocationKey:       ask.GetAllocationKey(),
		AllocationTags:      ask.GetTagsClone(),
		RequestTime:         ask.GetCreateTime().UnixNano(),
		ResourcePerAlloc:    ask.GetAllocatedResource().DAOMap(),
		Priority:            strconv.Itoa(int(ask.GetPriority())),
		RequiredNodeID:      ask.GetRequiredNode(),
		ApplicationID:       ask.GetApplicationID(),
		Placeholder:         ask.IsPlaceholder(),
		TaskGroupName:       ask.GetTaskGroup(),
		AllocationLog:       getAllocationLogsDAO(ask.GetAllocationLog()),
		TriggeredPreemption: ask.HasTriggeredPreemption(),
		Originator:          ask.IsOriginator(),
		SchedulingAttempted: ask.IsSchedulingAttempted(),
		TriggeredScaleUp:    ask.HasTriggeredScaleUp(),
	}
}

func getAllocationAsksDAO(asks []*objects.Allocation) []*dao.AllocationAskDAOInfo {
	asksDAO := make([]*dao.AllocationAskDAOInfo, 0, len(asks))
	for _, ask := range asks {
		if !ask.IsAllocated() {
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
// Only check the default partition
// Deprecated - To be removed in next major release. Replaced with getNodesUtilisations
func getNodeUtilisation(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	partitionContext := schedulerContext.Load().GetPartitionWithoutClusterID(configs.DefaultPartition)
	if partitionContext == nil {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusInternalServerError)
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
// Deprecated - To be removed in next major release. Replaced with getPartitionNodesUtilJSON
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

func getNodeUtilisations(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	var result []*dao.PartitionNodesUtilDAOInfo
	for _, part := range schedulerContext.Load().GetPartitionMapClone() {
		result = append(result, getPartitionNodesUtilJSON(part))
	}

	if err := json.NewEncoder(w).Encode(result); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

// getPartitionNodesUtilJSON retrieves the utilization of all resource types for nodes within a specific partition.
func getPartitionNodesUtilJSON(partition *scheduler.PartitionContext) *dao.PartitionNodesUtilDAOInfo {
	type UtilizationBucket struct {
		NodeCount []int      // 10 buckets, each bucket contains number of nodes
		NodeList  [][]string // 10 buckets, each bucket contains node name list
	}
	resourceBuckets := make(map[string]*UtilizationBucket) // key is resource type, value is UtilizationBucket

	// put nodes to buckets
	for _, node := range partition.GetNodes() {
		capacity := node.GetCapacity()
		resourceAllocated := node.GetAllocatedResource()
		absUsedCapacity := resources.CalculateAbsUsedCapacity(capacity, resourceAllocated)

		// append to bucket based on resource type, only count if node advertises the resource
		for resourceType := range capacity.Resources {
			idx := 0
			if absValue, ok := absUsedCapacity.Resources[resourceType]; ok {
				v := float64(absValue)
				idx = int(math.Dim(math.Ceil(v/10), 1))
			}

			// create resource bucket if not exist
			if _, ok := resourceBuckets[resourceType]; !ok {
				resourceBuckets[resourceType] = &UtilizationBucket{
					NodeCount: make([]int, 10),
					NodeList:  make([][]string, 10),
				}
			}

			resourceBuckets[resourceType].NodeCount[idx]++
			resourceBuckets[resourceType].NodeList[idx] = append(resourceBuckets[resourceType].NodeList[idx], node.NodeID)
		}
	}

	// build result
	var nodesUtilList []*dao.NodesUtilDAOInfo
	for resourceType, bucket := range resourceBuckets {
		var nodesUtil []*dao.NodeUtilDAOInfo
		for k := 0; k < 10; k++ {
			util := &dao.NodeUtilDAOInfo{
				BucketName: fmt.Sprintf("%d", k*10) + "-" + fmt.Sprintf("%d", (k+1)*10) + "%",
				NumOfNodes: int64(bucket.NodeCount[k]),
				NodeNames:  bucket.NodeList[k],
			}
			nodesUtil = append(nodesUtil, util)
		}
		nodeUtilization := &dao.NodesUtilDAOInfo{
			ResourceType: resourceType,
			NodesUtil:    nodesUtil,
		}
		nodesUtilList = append(nodesUtilList, nodeUtilization)
	}

	return &dao.PartitionNodesUtilDAOInfo{
		ClusterID:     partition.RmID,
		Partition:     common.GetPartitionNameWithoutClusterID(partition.Name),
		NodesUtilList: nodesUtilList,
	}
}

func getApplicationHistory(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	// There is nothing to return but we did not really encounter a problem
	if imHistory == nil {
		buildJSONErrorResponse(w, "Internal metrics collection is not enabled.", http.StatusInternalServerError)
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
		buildJSONErrorResponse(w, "Internal metrics collection is not enabled.", http.StatusInternalServerError)
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
		return
	}
	if _, err = w.Write(marshalledConf); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getClusterConfigDAO() *dao.ConfigDAOInfo {
	// merge core config with extra config
	conf := dao.ConfigDAOInfo{
		SchedulerConfig:          configs.ConfigContext.Get(schedulerContext.Load().GetPolicyGroup()),
		Extra:                    configs.GetConfigMap(),
		DeadlockDetectionEnabled: locking.IsTrackingEnabled(),
		DeadlockTimeoutSeconds:   locking.GetDeadlockTimeoutSeconds(),
	}

	return &conf
}

func checkHealthStatus(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)

	// Fetch last healthCheck result
	result := schedulerContext.Load().GetLastHealthCheckResult()
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
		log.Log(log.SchedHealth).Info("Health check is not available")
		buildJSONErrorResponse(w, "Health check is not available", http.StatusNotFound)
	}
}

func getPartitions(w http.ResponseWriter, _ *http.Request) {
	writeHeaders(w)

	lists := schedulerContext.Load().GetPartitionMapClone()
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
	var partition = schedulerContext.Load().GetPartitionWithoutClusterID(partitionName)
	if partition != nil {
		partitionQueuesDAOInfo = partition.GetPartitionQueues()
	} else {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusNotFound)
		return
	}
	if err := json.NewEncoder(w).Encode(partitionQueuesDAOInfo); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getPartitionQueue(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	vars := httprouter.ParamsFromContext(r.Context())
	if vars == nil {
		buildJSONErrorResponse(w, MissingParamsName, http.StatusBadRequest)
		return
	}
	partition := vars.ByName("partition")
	partitionContext := schedulerContext.Load().GetPartitionWithoutClusterID(partition)
	if partitionContext == nil {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusNotFound)
		return
	}
	queueName := vars.ByName("queue")
	unescapedQueueName, err := url.QueryUnescape(queueName)
	if err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}
	queueErr := validateQueue(unescapedQueueName)
	if queueErr != nil {
		buildJSONErrorResponse(w, queueErr.Error(), http.StatusBadRequest)
		return
	}
	queue := partitionContext.GetQueue(unescapedQueueName)
	if queue == nil {
		buildJSONErrorResponse(w, QueueDoesNotExists, http.StatusNotFound)
		return
	}
	queueDao := queue.GetPartitionQueueDAOInfo(r.URL.Query().Has("subtree"))
	if err := json.NewEncoder(w).Encode(queueDao); err != nil {
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
	partitionContext := schedulerContext.Load().GetPartitionWithoutClusterID(partition)
	if partitionContext != nil {
		nodesDao := getNodesDAO(partitionContext.GetNodes())
		if err := json.NewEncoder(w).Encode(nodesDao); err != nil {
			buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusNotFound)
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
	partitionContext := schedulerContext.Load().GetPartitionWithoutClusterID(partition)
	if partitionContext != nil {
		nodeID := vars.ByName("node")
		node := partitionContext.GetNode(nodeID)
		if node == nil {
			buildJSONErrorResponse(w, NodeDoesNotExists, http.StatusNotFound)
			return
		}
		nodeDao := getNodeDAO(node)
		if err := json.NewEncoder(w).Encode(nodeDao); err != nil {
			buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusNotFound)
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
	unescapedQueueName, err := url.QueryUnescape(queueName)
	if err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}
	queueErr := validateQueue(unescapedQueueName)
	if queueErr != nil {
		buildJSONErrorResponse(w, queueErr.Error(), http.StatusBadRequest)
		return
	}
	partitionContext := schedulerContext.Load().GetPartitionWithoutClusterID(partition)
	if partitionContext == nil {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusNotFound)
		return
	}
	queue := partitionContext.GetQueue(unescapedQueueName)
	if queue == nil {
		buildJSONErrorResponse(w, QueueDoesNotExists, http.StatusNotFound)
		return
	}

	appsDao := make([]*dao.ApplicationDAOInfo, 0)
	for _, app := range queue.GetCopyOfApps() {
		summary := app.GetApplicationSummary(partitionContext.RmID)
		appsDao = append(appsDao, getApplicationDAO(app, summary))
	}

	if err := json.NewEncoder(w).Encode(appsDao); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getPartitionApplicationsByState(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	vars := httprouter.ParamsFromContext(r.Context())
	if vars == nil {
		buildJSONErrorResponse(w, MissingParamsName, http.StatusBadRequest)
		return
	}
	partition := vars.ByName("partition")
	appState := strings.ToLower(vars.ByName("state"))

	partitionContext := schedulerContext.Load().GetPartitionWithoutClusterID(partition)
	if partitionContext == nil {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusNotFound)
		return
	}
	var appList []*objects.Application
	switch appState {
	case AppStateActive:
		if status := strings.ToLower(r.URL.Query().Get("status")); status != "" {
			if !allowedAppActiveStatuses[status] {
				buildJSONErrorResponse(w, allowedActiveStatusMsg, http.StatusBadRequest)
				return
			}
			for _, app := range partitionContext.GetApplications() {
				if strings.ToLower(app.CurrentState()) == status {
					appList = append(appList, app)
				}
			}
		} else {
			appList = partitionContext.GetApplications()
		}
	case AppStateRejected:
		appList = partitionContext.GetRejectedApplications()
	case AppStateCompleted:
		appList = partitionContext.GetCompletedApplications()
	default:
		buildJSONErrorResponse(w, fmt.Sprintf("Only following application states are allowed: %s, %s, %s", AppStateActive, AppStateRejected, AppStateCompleted), http.StatusBadRequest)
		return
	}
	appsDao := make([]*dao.ApplicationDAOInfo, 0, len(appList))
	for _, app := range appList {
		summary := app.GetApplicationSummary(partitionContext.RmID)
		appsDao = append(appsDao, getApplicationDAO(app, summary))
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
	unescapedQueueName, err := url.QueryUnescape(queueName)
	if err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}
	application := vars.ByName("application")
	partitionContext := schedulerContext.Load().GetPartitionWithoutClusterID(partition)
	if partitionContext == nil {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusNotFound)
		return
	}
	var app *objects.Application
	if len(unescapedQueueName) == 0 {
		app = partitionContext.GetApplication(application)
	} else {
		queueErr := validateQueue(unescapedQueueName)
		if queueErr != nil {
			buildJSONErrorResponse(w, queueErr.Error(), http.StatusBadRequest)
			return
		}
		queue := partitionContext.GetQueue(unescapedQueueName)
		if queue == nil {
			buildJSONErrorResponse(w, QueueDoesNotExists, http.StatusNotFound)
			return
		}
		app = queue.GetApplication(application)
	}
	if app == nil {
		buildJSONErrorResponse(w, ApplicationDoesNotExists, http.StatusNotFound)
		return
	}

	summary := app.GetApplicationSummary(partitionContext.RmID)
	appDao := getApplicationDAO(app, summary)
	if err := json.NewEncoder(w).Encode(appDao); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getPartitionRules(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	vars := httprouter.ParamsFromContext(r.Context())
	if vars == nil {
		buildJSONErrorResponse(w, MissingParamsName, http.StatusBadRequest)
		return
	}
	partition := vars.ByName("partition")
	partitionContext := schedulerContext.Load().GetPartitionWithoutClusterID(partition)
	if partitionContext == nil {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusNotFound)
		return
	}
	rulesDao := partitionContext.GetPlacementRules()
	if err := json.NewEncoder(w).Encode(rulesDao); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getQueueApplicationsByState(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	vars := httprouter.ParamsFromContext(r.Context())
	if vars == nil {
		buildJSONErrorResponse(w, MissingParamsName, http.StatusBadRequest)
		return
	}

	partition := vars.ByName("partition")
	queueName := vars.ByName("queue")
	appState := strings.ToLower(vars.ByName("state"))
	status := strings.ToLower(r.URL.Query().Get("status"))

	partitionContext := schedulerContext.Load().GetPartitionWithoutClusterID(partition)
	if partitionContext == nil {
		buildJSONErrorResponse(w, PartitionDoesNotExists, http.StatusNotFound)
		return
	}
	unescapedQueueName, err := url.QueryUnescape(queueName)
	if err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}
	queueErr := validateQueue(unescapedQueueName)
	if queueErr != nil {
		buildJSONErrorResponse(w, queueErr.Error(), http.StatusBadRequest)
		return
	}
	queue := partitionContext.GetQueue(unescapedQueueName)
	if queue == nil {
		buildJSONErrorResponse(w, QueueDoesNotExists, http.StatusNotFound)
		return
	}
	if appState != AppStateActive {
		buildJSONErrorResponse(w, fmt.Sprintf("Only following application states are allowed: %s", AppStateActive), http.StatusBadRequest)
		return
	}
	if status != "" && !allowedAppActiveStatuses[status] {
		buildJSONErrorResponse(w, allowedActiveStatusMsg, http.StatusBadRequest)
		return
	}

	appsDao := make([]*dao.ApplicationDAOInfo, 0)
	for _, app := range queue.GetCopyOfApps() {
		if status == "" || strings.ToLower(app.CurrentState()) == status {
			summary := app.GetApplicationSummary(partitionContext.RmID)
			appsDao = append(appsDao, getApplicationDAO(app, summary))
		}
	}

	if err := json.NewEncoder(w).Encode(appsDao); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getPartitionInfoDAO(lists map[string]*scheduler.PartitionContext) []*dao.PartitionInfo {
	result := make([]*dao.PartitionInfo, 0, len(lists))

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
	result := make([]*dao.ApplicationHistoryDAOInfo, 0)

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
	result := make([]*dao.NodesDAOInfo, 0, len(lists))

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
	result := make([]*dao.ContainerHistoryDAOInfo, 0)

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
	result := make([]*dao.ApplicationDAOInfo, 0, len(lists))

	for _, partition := range lists {
		var appList []*objects.Application
		appList = append(appList, partition.GetApplications()...)
		appList = append(appList, partition.GetCompletedApplications()...)
		appList = append(appList, partition.GetRejectedApplications()...)

		for _, app := range appList {
			summary := app.GetApplicationSummary(partition.RmID)
			result = append(result, getApplicationDAO(app, summary))
		}
	}

	return result
}

func getPlacementRulesDAO(lists map[string]*scheduler.PartitionContext) []*dao.RuleDAOInfo {
	result := make([]*dao.RuleDAOInfo, 0, len(lists))

	for _, partition := range lists {
		result = append(result, &dao.RuleDAOInfo{
			Partition: common.GetPartitionNameWithoutClusterID(partition.Name),
			Rules:     partition.GetPlacementRules(),
		})
	}

	return result
}

func getPartitionQueuesDAO(lists map[string]*scheduler.PartitionContext) []dao.PartitionQueueDAOInfo {
	result := make([]dao.PartitionQueueDAOInfo, 0, len(lists))

	for _, partition := range lists {
		result = append(result, partition.GetPartitionQueues())
	}

	return result
}

func getClusterDAO(lists map[string]*scheduler.PartitionContext) []*dao.ClusterDAOInfo {
	result := make([]*dao.ClusterDAOInfo, 0, len(lists))

	for _, partition := range lists {
		result = append(result, getClusterJSON(partition))
	}

	return result
}

func getRMBuildInformation(lists map[string]*scheduler.RMInformation) []map[string]string {
	result := make([]map[string]string, 0, len(lists))

	for _, rmInfo := range lists {
		result = append(result, rmInfo.RMBuildInformation)
	}

	return result
}

func getResourceManagerDiagnostics() map[string]interface{} {
	result := make(map[string]interface{})

	// if the RM has not registered state dump the plugin will be nil
	plugin := plugins.GetStateDumpPlugin()
	if plugin == nil {
		result["empty"] = "Resource Manager did not register callback"
		return result
	}

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

func getUsersResourceUsage(w http.ResponseWriter, _ *http.Request) {
	writeHeaders(w)
	userManager := ugm.GetUserManager()
	usersResources := userManager.GetUsersResources()
	result := make([]*dao.UserResourceUsageDAOInfo, len(usersResources))
	for i, tracker := range usersResources {
		result[i] = tracker.GetUserResourceUsageDAOInfo()
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
	unescapedUser, err := url.QueryUnescape(user)
	if err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !configs.UserRegExp.MatchString(unescapedUser) {
		buildJSONErrorResponse(w, InvalidUserName, http.StatusBadRequest)
		return
	}
	userTracker := ugm.GetUserManager().GetUserTracker(unescapedUser)
	if userTracker == nil {
		buildJSONErrorResponse(w, UserDoesNotExists, http.StatusNotFound)
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
	result := make([]*dao.GroupResourceUsageDAOInfo, len(groupsResources))
	for i, tracker := range groupsResources {
		result[i] = tracker.GetGroupResourceUsageDAOInfo()
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
	unescapedGroupName, err := url.QueryUnescape(group)
	if err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !configs.GroupRegExp.MatchString(unescapedGroupName) {
		buildJSONErrorResponse(w, InvalidGroupName, http.StatusBadRequest)
		return
	}
	groupTracker := ugm.GetUserManager().GetGroupTracker(unescapedGroupName)
	if groupTracker == nil {
		buildJSONErrorResponse(w, GroupDoesNotExists, http.StatusNotFound)
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
		buildJSONErrorResponse(w, "Event tracking is disabled", http.StatusInternalServerError)
		return
	}

	maxCount := maxRESTResponseSize.Load()
	count := maxCount
	if countStr := r.URL.Query().Get("count"); countStr != "" {
		var err error
		count, err = strconv.ParseUint(countStr, 10, 64)
		if err != nil {
			buildJSONErrorResponse(w, err.Error(), http.StatusBadRequest)
			return
		}
		if count > maxCount {
			count = maxCount
		}
		if count == 0 {
			buildJSONErrorResponse(w, `0 is not a valid value for "count"`, http.StatusBadRequest)
			return
		}
	}

	var start uint64
	if startStr := r.URL.Query().Get("start"); startStr != "" {
		var err error
		start, err = strconv.ParseUint(startStr, 10, 64)
		if err != nil {
			buildJSONErrorResponse(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	records, lowestID, highestID := eventSystem.GetEventsFromID(start, count)
	eventDao := dao.EventRecordDAO{
		InstanceUUID: schedulerContext.Load().GetUUID(),
		LowestID:     lowestID,
		HighestID:    highestID,
		EventRecords: records,
	}
	if err := json.NewEncoder(w).Encode(eventDao); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func getStream(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	eventSystem := events.GetEventSystem()
	if !eventSystem.IsEventTrackingEnabled() {
		buildJSONErrorResponse(w, "Event tracking is disabled", http.StatusInternalServerError)
		return
	}

	f, ok := w.(http.Flusher)
	if !ok {
		buildJSONErrorResponse(w, "Writer does not implement http.Flusher", http.StatusInternalServerError)
		return
	}

	if !streamingLimiter.AddHost(r.Host) {
		buildJSONErrorResponse(w, "Too many streaming connections", http.StatusServiceUnavailable)
		return
	}
	defer streamingLimiter.RemoveHost(r.Host)

	var count uint64
	if countStr := r.URL.Query().Get("count"); countStr != "" {
		var err error
		count, err = strconv.ParseUint(countStr, 10, 64)
		if err != nil {
			buildJSONErrorResponse(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	rc := http.NewResponseController(w)
	// make sure both deadlines can be set
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		log.Log(log.REST).Error("Cannot set write deadline", zap.Error(err))
		buildJSONErrorResponse(w, fmt.Sprintf("Cannot set write deadline: %v", err), http.StatusInternalServerError)
		return
	}
	if err := rc.SetReadDeadline(time.Time{}); err != nil {
		log.Log(log.REST).Error("Cannot set read deadline", zap.Error(err))
		buildJSONErrorResponse(w, fmt.Sprintf("Cannot set read deadline: %v", err), http.StatusInternalServerError)
		return
	}
	enc := json.NewEncoder(w)
	stream := eventSystem.CreateEventStream(r.Host, count)
	defer eventSystem.RemoveStream(stream)

	if err := enc.Encode(dao.YunikornID{
		InstanceUUID: schedulerContext.Load().GetUUID(),
	}); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}
	f.Flush()

	// Reading events in an infinite loop until either the client disconnects or Yunikorn closes the channel.
	// This results in a persistent HTTP connection where the message body is never closed.
	// Write deadline is adjusted before sending data to the client.
	for {
		select {
		case <-r.Context().Done():
			log.Log(log.REST).Info("Connection closed for event stream client",
				zap.String("host", r.Host))
			return
		case e, ok := <-stream.Events:
			err := rc.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err != nil {
				// should not fail at this point
				log.Log(log.REST).Error("Cannot set write deadline", zap.Error(err))
				buildJSONErrorResponse(w, fmt.Sprintf("Cannot set write deadline: %v", err), http.StatusOK) // status code is already 200 at this point
				return
			}

			if !ok {
				// the channel was closed by the event system itself
				msg := "Event stream was closed by the producer"
				buildJSONErrorResponse(w, msg, http.StatusOK) // status code is 200 at this point, cannot be changed
				log.Log(log.REST).Error(msg)
				return
			}

			if err := enc.Encode(e); err != nil {
				log.Log(log.REST).Error("Marshalling error",
					zap.String("host", r.Host))
				buildJSONErrorResponse(w, err.Error(), http.StatusOK) // status code is 200 at this point, cannot be changed
				return
			}
			f.Flush()
		}
	}
}
