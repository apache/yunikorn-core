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
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v2"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/metrics/history"
	"github.com/apache/yunikorn-core/pkg/scheduler"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const partitionNameWithoutClusterID = "default"
const normalizedPartitionName = "[rm-123]default"
const startConf = `
partitions:
  - name: default
    nodesortpolicy:
        type: fair
    queues:
      - name: root
        properties:
          first: "some value with spaces"
          second: somethingElse
`
const updatedConf = `
partitions:
  - name: default
    nodesortpolicy:
        type: binpacking
    queues:
      - name: root
        properties:
          first: "changedValue"
`

const baseConf = `
partitions:
  - name: default
    nodesortpolicy:
        type: fair
    queues:
      - name: root
        submitacl: "*"
`

const invalidConf = `
partitions:
  - name: default
    nodesortpolicy:
        type: invalid
    queues:
      - name: root
`

const configDefault = `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: default
          - name: noapps
`

const configStateDumpFilePath = `
partitions:
  - name: default
    statedumpfilepath: "tmp/non-default-yunikorn-state.txt"
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: default
          - name: noapps
`

const configMultiPartitions = `
partitions: 
  - 
    name: gpu
    queues: 
      - 
        name: root
  - 
    name: default
    nodesortpolicy:
        type: fair
    queues: 
      - 
        name: root
        queues: 
          - 
            name: default
            submitacl: "*"
`
const configTwoLevelQueues = `
partitions: 
  - 
    name: gpu
    queues: 
      - 
        name: root
  - 
    name: default
    nodesortpolicy: 
      type: binpacking
    queues: 
      - 
        name: root
        properties:
          application.sort.policy: stateaware
        childtemplate:
          properties:
            application.sort.policy: stateaware
          resources:
            guaranteed:
              memory: 400000
            max:
              memory: 600000
        queues: 
          - 
            name: a
            queues: 
              - 
                name: a1
                resources: 
                  guaranteed: 
                    memory: 500000
                    vcore: 50000
                  max: 
                    memory: 800000
                    vcore: 80000
            resources: 
              guaranteed: 
                memory: 500000
                vcore: 50000
              max: 
                memory: 800000
                vcore: 80000
          - 
            name: b
            resources: 
              guaranteed: 
                memory: 400000
                vcore: 40000
              max: 
                memory: 600000
                vcore: 60000
          - 
            name: c
            resources: 
              guaranteed: 
                memory: 100000
                vcore: 10000
              max: 
                memory: 100000
                vcore: 10000
`

const rmID = "rm-123"
const policyGroup = "default-policy-group"
const queueName = "root.default"
const nodeID = "node-1"

// setup To take care of setting up config, cluster, partitions etc
func setup(t *testing.T, config string, partitionCount int) *scheduler.PartitionContext {
	var err error
	schedulerContext, err = scheduler.NewClusterContext(rmID, policyGroup, []byte(config))
	assert.NilError(t, err, "Error when load clusterInfo from config")

	assert.Equal(t, partitionCount, len(schedulerContext.GetPartitionMapClone()))

	// Check default partition
	partitionName := common.GetNormalizedPartitionName("default", rmID)
	part := schedulerContext.GetPartition(partitionName)
	assert.Equal(t, 0, len(part.GetApplications()))
	return part
}

// simple wrapper to make creating an app easier
func newApplication(appID, partitionName, queueName, rmID string, ugi security.UserGroup) *objects.Application {
	userGroup := ugi
	if ugi.User == "" {
		userGroup = security.UserGroup{User: "testuser", Groups: []string{"testgroup"}}
	}
	siApp := &si.AddApplicationRequest{
		ApplicationID: appID,
		QueueName:     queueName,
		PartitionName: partitionName,
	}
	return objects.NewApplication(siApp, userGroup, nil, rmID)
}

func TestValidateConf(t *testing.T) {
	confTests := []struct {
		content          string
		expectedResponse dao.ValidateConfResponse
	}{
		{
			content: baseConf,
			expectedResponse: dao.ValidateConfResponse{
				Allowed: true,
				Reason:  "",
			},
		},
		{
			content: invalidConf,
			expectedResponse: dao.ValidateConfResponse{
				Allowed: false,
				Reason:  "undefined policy: invalid",
			},
		},
	}
	for _, test := range confTests {
		// No err check: new request always returns correctly
		//nolint: errcheck
		req, _ := http.NewRequest("POST", "", strings.NewReader(test.content))
		resp := &MockResponseWriter{}
		validateConf(resp, req)
		var vcr dao.ValidateConfResponse
		err := json.Unmarshal(resp.outputBytes, &vcr)
		assert.NilError(t, err, "failed to unmarshal ValidateConfResponse from response body")
		assert.Equal(t, vcr.Allowed, test.expectedResponse.Allowed, "allowed flag incorrect")
		assert.Equal(t, vcr.Reason, test.expectedResponse.Reason, "response text not as expected")
	}
}

func TestApplicationHistory(t *testing.T) {
	// make sure the history is nil when we finish this test
	defer ResetIMHistory()
	// No err check: new request always returns correctly
	//nolint: errcheck
	req, _ := http.NewRequest("GET", "", strings.NewReader(""))
	resp := &MockResponseWriter{}
	// no init should return nothing
	getApplicationHistory(resp, req)

	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, "failed to unmarshal app history response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, http.StatusNotImplemented, resp.statusCode, "app history handler returned wrong status")
	assert.Equal(t, errInfo.Message, "Internal metrics collection is not enabled.", "JSON error message is incorrect")
	assert.Equal(t, errInfo.StatusCode, http.StatusNotImplemented)

	// init should return null and thus no records
	imHistory = history.NewInternalMetricsHistory(5)
	resp = &MockResponseWriter{}
	getApplicationHistory(resp, req)
	var appHist []dao.ApplicationHistoryDAOInfo
	err = json.Unmarshal(resp.outputBytes, &appHist)
	assert.NilError(t, err, "failed to unmarshal app history response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, resp.statusCode, 0, "app response should have no status")
	assert.Equal(t, len(appHist), 0, "empty response must have no records")

	// add new history records
	imHistory.Store(1, 0)
	imHistory.Store(2, 0)
	imHistory.Store(30, 0)
	resp = &MockResponseWriter{}
	getApplicationHistory(resp, req)
	err = json.Unmarshal(resp.outputBytes, &appHist)
	assert.NilError(t, err, "failed to unmarshal app history response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, resp.statusCode, 0, "app response should have no status")
	assert.Equal(t, len(appHist), 3, "incorrect number of records returned")
	assert.Equal(t, appHist[0].TotalApplications, "1", "metric 1 should be 1 apps and was not")
	assert.Equal(t, appHist[2].TotalApplications, "30", "metric 3 should be 30 apps and was not")

	// add new history records roll over the limit
	// this gives us a list of (oldest to newest): 2, 30, 40, 50, 300
	imHistory.Store(40, 0)
	imHistory.Store(50, 0)
	imHistory.Store(300, 0)
	resp = &MockResponseWriter{}
	getApplicationHistory(resp, req)
	err = json.Unmarshal(resp.outputBytes, &appHist)
	assert.NilError(t, err, "failed to unmarshal app history response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, resp.statusCode, 0, "app response should have no status")
	assert.Equal(t, len(appHist), 5, "incorrect number of records returned")
	assert.Equal(t, appHist[0].TotalApplications, "2", "metric 1 should be 1 apps and was not")
	assert.Equal(t, appHist[4].TotalApplications, "300", "metric 5 should be 300 apps and was not")
}

func TestContainerHistory(t *testing.T) {
	// make sure the history is nil when we finish this test
	defer ResetIMHistory()
	// No err check: new request always returns correctly
	//nolint: errcheck
	req, _ := http.NewRequest("GET", "", strings.NewReader(""))
	resp := &MockResponseWriter{}
	// no init should return nothing
	getContainerHistory(resp, req)

	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, "failed to unmarshal container history response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, http.StatusNotImplemented, resp.statusCode, "container history handler returned wrong status")
	assert.Equal(t, errInfo.Message, "Internal metrics collection is not enabled.", "JSON error message is incorrect")
	assert.Equal(t, errInfo.StatusCode, http.StatusNotImplemented)

	// init should return null and thus no records
	imHistory = history.NewInternalMetricsHistory(5)
	resp = &MockResponseWriter{}
	getContainerHistory(resp, req)
	var contHist []dao.ContainerHistoryDAOInfo
	err = json.Unmarshal(resp.outputBytes, &contHist)
	assert.NilError(t, err, "failed to unmarshal container history response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, resp.statusCode, 0, "container response should have no status")
	assert.Equal(t, len(contHist), 0, "empty response must have no records")

	// add new history records
	imHistory.Store(0, 1)
	imHistory.Store(0, 2)
	imHistory.Store(0, 30)
	resp = &MockResponseWriter{}
	getContainerHistory(resp, req)
	err = json.Unmarshal(resp.outputBytes, &contHist)
	assert.NilError(t, err, "failed to unmarshal container history response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, resp.statusCode, 0, "container response should have no status")
	assert.Equal(t, len(contHist), 3, "incorrect number of records returned")
	assert.Equal(t, contHist[0].TotalContainers, "1", "metric 1 should be 1 apps and was not")
	assert.Equal(t, contHist[2].TotalContainers, "30", "metric 3 should be 30 apps and was not")

	// add new history records roll over the limit
	// this gives us a list of (oldest to newest): 2, 30, 40, 50, 300
	imHistory.Store(0, 40)
	imHistory.Store(0, 50)
	imHistory.Store(0, 300)
	resp = &MockResponseWriter{}
	getContainerHistory(resp, req)
	err = json.Unmarshal(resp.outputBytes, &contHist)
	assert.NilError(t, err, "failed to unmarshal container history response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, resp.statusCode, 0, "container response should have no status")
	assert.Equal(t, len(contHist), 5, "incorrect number of records returned")
	assert.Equal(t, contHist[0].TotalContainers, "2", "metric 1 should be 1 apps and was not")
	assert.Equal(t, contHist[4].TotalContainers, "300", "metric 5 should be 300 apps and was not")
}

func TestGetConfigYAML(t *testing.T) {
	var err error
	schedulerContext, err = scheduler.NewClusterContext(rmID, policyGroup, []byte(startConf))
	assert.NilError(t, err, "Error when load clusterInfo from config")
	// No err check: new request always returns correctly
	//nolint: errcheck
	req, _ := http.NewRequest("GET", "", nil)
	resp := &MockResponseWriter{}
	getClusterConfig(resp, req)
	// yaml unmarshal handles the checksum add the end automatically in this implementation
	conf := &configs.SchedulerConfig{}
	err = yaml.Unmarshal(resp.outputBytes, conf)
	assert.NilError(t, err, "failed to unmarshal config from response body")
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "fair", "node sort policy set incorrectly, not fair")

	startConfSum := conf.Checksum
	assert.Assert(t, len(startConfSum) > 0, "checksum boundary not found")

	// change the config
	err = schedulerContext.UpdateRMSchedulerConfig(rmID, []byte(updatedConf))
	assert.NilError(t, err, "Error when updating clusterInfo from config")
	// check that we return yaml by default, unmarshal will error when we don't
	req.Header.Set("Accept", "unknown")
	getClusterConfig(resp, req)
	err = yaml.Unmarshal(resp.outputBytes, conf)
	assert.NilError(t, err, "failed to unmarshal config from response body (updated config)")
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "binpacking", "node sort policy not updated")
	assert.Assert(t, startConfSum != conf.Checksum, "checksums did not change in output")
}

func TestGetConfigJSON(t *testing.T) {
	setup(t, startConf, 1)
	// No err check: new request always returns correctly
	//nolint: errcheck
	req, _ := http.NewRequest("GET", "", nil)
	req.Header.Set("Accept", "application/json")
	resp := &MockResponseWriter{}
	getClusterConfig(resp, req)

	conf := &configs.SchedulerConfig{}
	err := json.Unmarshal(resp.outputBytes, conf)
	assert.NilError(t, err, "failed to unmarshal config from response body (json)")
	startConfSum := conf.Checksum
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "fair", "node sort policy set incorrectly, not fair (json)")

	// change the config
	err = schedulerContext.UpdateRMSchedulerConfig(rmID, []byte(updatedConf))
	assert.NilError(t, err, "Error when updating clusterInfo from config")

	getClusterConfig(resp, req)
	err = json.Unmarshal(resp.outputBytes, conf)
	assert.NilError(t, err, "failed to unmarshal config from response body (json, updated config)")
	assert.Assert(t, startConfSum != conf.Checksum, "checksums did not change in json output: %s, %s", startConfSum, conf.Checksum)
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "binpacking", "node sort policy not updated (json)")
}

func TestBuildUpdateResponseSuccess(t *testing.T) {
	resp := &MockResponseWriter{}
	buildUpdateResponse(nil, resp)
	assert.Equal(t, http.StatusOK, resp.statusCode, "Response should be OK")
}

func TestBuildUpdateResponseFailure(t *testing.T) {
	resp := &MockResponseWriter{}
	err := fmt.Errorf("ConfigMapUpdate failed")
	buildUpdateResponse(err, resp)

	var errInfo dao.YAPIError
	err1 := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err1, "failed to unmarshal updateconfig dao response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, http.StatusConflict, resp.statusCode, "Status code is wrong")
	assert.Assert(t, strings.Contains(string(errInfo.Message), err.Error()), "Error message should contain the reason")
	assert.Equal(t, errInfo.StatusCode, http.StatusConflict)
}

func TestGetClusterUtilJSON(t *testing.T) {
	setup(t, configDefault, 1)

	// check build information of RM
	buildInfoMap := make(map[string]string)
	buildInfoMap["buildDate"] = "2006-01-02T15:04:05-0700"
	buildInfoMap["buildVersion"] = "latest"
	buildInfoMap["isPluginVersion"] = "false"
	schedulerContext.SetRMInfo(rmID, buildInfoMap)
	rmInfo := schedulerContext.GetRMInfoMapClone()
	assert.Equal(t, 1, len(rmInfo))
	rmBuildInformationMaps := getRMBuildInformation(rmInfo)
	assert.Equal(t, 1, len(rmBuildInformationMaps))
	assert.Equal(t, rmBuildInformationMaps[0]["buildDate"], buildInfoMap["buildDate"])
	assert.Equal(t, rmBuildInformationMaps[0]["buildVersion"], buildInfoMap["buildVersion"])
	assert.Equal(t, rmBuildInformationMaps[0]["isPluginVersion"], buildInfoMap["isPluginVersion"])
	assert.Equal(t, rmBuildInformationMaps[0]["rmId"], rmID)

	// Check test partitions
	partitionName := common.GetNormalizedPartitionName("default", rmID)
	partition := schedulerContext.GetPartition(partitionName)
	assert.Equal(t, partitionName, partition.Name)
	// new app to partition
	appID := "appID-1"
	app := newApplication(appID, partitionName, queueName, rmID, security.UserGroup{})
	err := partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")
	// case of total resource and allocated resource undefined
	utilZero := &dao.ClusterUtilDAOInfo{
		ResourceType: "N/A",
		Total:        int64(-1),
		Used:         int64(-1),
		Usage:        "N/A",
	}
	result0 := getClusterUtilJSON(partition)
	assert.Equal(t, ContainsObj(result0, utilZero), true)

	// add node to partition with allocations
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 1000, siCommon.CPU: 1000}).ToProto()
	node1 := objects.NewNode(&si.NodeInfo{NodeID: nodeID, SchedulableResource: nodeRes})

	resAlloc1 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 500, siCommon.CPU: 300})
	resAlloc2 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 300, siCommon.CPU: 200})
	ask1 := objects.NewAllocationAsk("alloc-1", appID, resAlloc1)
	ask2 := objects.NewAllocationAsk("alloc-2", appID, resAlloc2)
	alloc1 := objects.NewAllocation("alloc-1-uuid", nodeID, ask1)
	alloc2 := objects.NewAllocation("alloc-2-uuid", nodeID, ask2)
	allocs := []*objects.Allocation{alloc1, alloc2}
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")

	// set expected result
	utilMem := &dao.ClusterUtilDAOInfo{
		ResourceType: siCommon.Memory,
		Total:        int64(1000),
		Used:         int64(800),
		Usage:        "80%",
	}
	utilCore := &dao.ClusterUtilDAOInfo{
		ResourceType: siCommon.CPU,
		Total:        int64(1000),
		Used:         int64(500),
		Usage:        "50%",
	}
	// check result fit answer or not
	result := getClusterUtilJSON(partition)
	assert.Equal(t, ContainsObj(result, utilMem), true)
	assert.Equal(t, ContainsObj(result, utilCore), true)
}

func ContainsObj(slice interface{}, contains interface{}) bool {
	value := reflect.ValueOf(slice)
	for i := 0; i < value.Len(); i++ {
		if value.Index(i).Interface() == contains {
			return true
		}
		if reflect.DeepEqual(value.Index(i).Interface(), contains) {
			return true
		}
		if fmt.Sprintf("%#v", value.Index(i).Interface()) == fmt.Sprintf("%#v", contains) {
			return true
		}
	}
	return false
}

func TestGetNodesUtilJSON(t *testing.T) {
	partition := setup(t, configDefault, 1)

	// create test application
	appID := "app1"
	app := newApplication(appID, partition.Name, queueName, rmID, security.UserGroup{})
	err := partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	// create test nodes
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 1000, siCommon.CPU: 1000}).ToProto()
	nodeRes2 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 1000, siCommon.CPU: 1000, "GPU": 10}).ToProto()
	node1ID := "node-1"
	node1 := objects.NewNode(&si.NodeInfo{NodeID: node1ID, SchedulableResource: nodeRes})
	node2ID := "node-2"
	node2 := objects.NewNode(&si.NodeInfo{NodeID: node2ID, SchedulableResource: nodeRes2})

	// create test allocations
	resAlloc1 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 500, siCommon.CPU: 300})
	resAlloc2 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 300, siCommon.CPU: 500, "GPU": 5})
	ask1 := objects.NewAllocationAsk("alloc-1", appID, resAlloc1)
	ask2 := objects.NewAllocationAsk("alloc-2", appID, resAlloc2)
	allocs := []*objects.Allocation{objects.NewAllocation("alloc-1-uuid", node1ID, ask1)}
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")
	allocs = []*objects.Allocation{objects.NewAllocation("alloc-2-uuid", node2ID, ask2)}
	err = partition.AddNode(node2, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")

	// get nodes utilization
	res1 := getNodesUtilJSON(partition, siCommon.Memory)
	res2 := getNodesUtilJSON(partition, siCommon.CPU)
	res3 := getNodesUtilJSON(partition, "GPU")
	resNon := getNodesUtilJSON(partition, "non-exist")
	subres1 := res1.NodesUtil
	subres2 := res2.NodesUtil
	subres3 := res3.NodesUtil
	subresNon := resNon.NodesUtil

	assert.Equal(t, res1.ResourceType, siCommon.Memory)
	assert.Equal(t, subres1[2].NumOfNodes, int64(1))
	assert.Equal(t, subres1[4].NumOfNodes, int64(1))
	assert.Equal(t, subres1[2].NodeNames[0], node2ID)
	assert.Equal(t, subres1[4].NodeNames[0], node1ID)

	assert.Equal(t, res2.ResourceType, siCommon.CPU)
	assert.Equal(t, subres2[2].NumOfNodes, int64(1))
	assert.Equal(t, subres2[4].NumOfNodes, int64(1))
	assert.Equal(t, subres2[2].NodeNames[0], node1ID)
	assert.Equal(t, subres2[4].NodeNames[0], node2ID)

	assert.Equal(t, res3.ResourceType, "GPU")
	assert.Equal(t, subres3[4].NumOfNodes, int64(1))
	assert.Equal(t, subres3[4].NodeNames[0], node2ID)

	assert.Equal(t, resNon.ResourceType, "non-exist")
	assert.Equal(t, subresNon[0].NumOfNodes, int64(0))
	assert.Equal(t, len(subresNon[0].NodeNames), 0)
}

func addAndConfirmApplicationExists(t *testing.T, partitionName string, partition *scheduler.PartitionContext, appName string) *objects.Application {
	// add a new app
	app := newApplication(appName, partitionName, "root.default", rmID, security.UserGroup{User: "testuser", Groups: []string{"testgroup"}})
	err := partition.AddApplication(app)
	assert.NilError(t, err, "Failed to add Application to Partition.")
	assert.Equal(t, app.CurrentState(), objects.New.String())
	return app
}

func TestPartitions(t *testing.T) {
	defaultPartition := setup(t, configMultiPartitions, 2)
	partitionName := defaultPartition.Name

	// add a new app
	addAndConfirmApplicationExists(t, partitionName, defaultPartition, "app-0")

	// add a new app1 - accepted
	app1 := addAndConfirmApplicationExists(t, partitionName, defaultPartition, "app-1")
	app1.SetState(objects.Accepted.String())

	// add a new app2 - starting
	app2 := addAndConfirmApplicationExists(t, partitionName, defaultPartition, "app-2")
	app2.SetState(objects.Starting.String())

	// add a new app3 - running
	app3 := addAndConfirmApplicationExists(t, partitionName, defaultPartition, "app-3")
	app3.SetState(objects.Running.String())

	// add a new app4 - completing
	app4 := addAndConfirmApplicationExists(t, partitionName, defaultPartition, "app-4")
	app4.SetState(objects.Completing.String())

	// add a new app5 - rejected
	app5 := addAndConfirmApplicationExists(t, partitionName, defaultPartition, "app-5")
	app5.SetState(objects.Rejected.String())

	// add a new app6 - completed
	app6 := addAndConfirmApplicationExists(t, partitionName, defaultPartition, "app-6")
	app6.SetState(objects.Completed.String())

	// add a new app7 - failed
	app7 := addAndConfirmApplicationExists(t, partitionName, defaultPartition, "app-7")
	app7.SetState(objects.Failed.String())

	NewWebApp(schedulerContext, nil)

	// create test nodes
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 500, siCommon.CPU: 500}).ToProto()
	node1ID := "node-1"
	node1 := objects.NewNode(&si.NodeInfo{NodeID: node1ID, SchedulableResource: nodeRes})
	node2ID := "node-2"
	node2 := objects.NewNode(&si.NodeInfo{NodeID: node2ID, SchedulableResource: nodeRes})

	// create test allocations
	resAlloc1 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 100, siCommon.CPU: 400})
	resAlloc2 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 200, siCommon.CPU: 300})
	ask1 := objects.NewAllocationAsk("alloc-1", app6.ApplicationID, resAlloc1)
	ask2 := objects.NewAllocationAsk("alloc-2", app3.ApplicationID, resAlloc2)
	allocs := []*objects.Allocation{objects.NewAllocation("alloc-1-uuid", node1ID, ask1)}
	err := defaultPartition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")
	allocs = []*objects.Allocation{objects.NewAllocation("alloc-2-uuid", node2ID, ask2)}
	err = defaultPartition.AddNode(node2, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")

	var req *http.Request
	req, err = http.NewRequest("GET", "/ws/v1/partitions", strings.NewReader(""))
	assert.NilError(t, err, "App Handler request failed")
	resp := &MockResponseWriter{}
	var partitionInfo []*dao.PartitionInfo
	getPartitions(resp, req)
	err = json.Unmarshal(resp.outputBytes, &partitionInfo)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body: %s", string(resp.outputBytes))

	cs := make(map[string]*dao.PartitionInfo, 2)
	for _, d := range partitionInfo {
		cs[d.Name] = d
	}

	assert.Assert(t, cs["default"] != nil)
	assert.Equal(t, cs["default"].ClusterID, "rm-123")
	assert.Equal(t, cs["default"].Name, "default")
	assert.Equal(t, cs["default"].NodeSortingPolicy.Type, "fair")
	assert.Equal(t, cs["default"].NodeSortingPolicy.ResourceWeights["vcore"], 1.0)
	assert.Equal(t, cs["default"].NodeSortingPolicy.ResourceWeights["memory"], 1.0)
	assert.Equal(t, cs["default"].Applications["total"], 8)
	assert.Equal(t, cs["default"].Applications[objects.New.String()], 1)
	assert.Equal(t, cs["default"].Applications[objects.Accepted.String()], 1)
	assert.Equal(t, cs["default"].Applications[objects.Starting.String()], 1)
	assert.Equal(t, cs["default"].Applications[objects.Running.String()], 1)
	assert.Equal(t, cs["default"].Applications[objects.Completing.String()], 1)
	assert.Equal(t, cs["default"].Applications[objects.Rejected.String()], 1)
	assert.Equal(t, cs["default"].Applications[objects.Completed.String()], 1)
	assert.Equal(t, cs["default"].Applications[objects.Failed.String()], 1)
	assert.DeepEqual(t, cs["default"].Capacity.Capacity, map[string]int64{"memory": 1000, "vcore": 1000})
	assert.DeepEqual(t, cs["default"].Capacity.UsedCapacity, map[string]int64{"memory": 300, "vcore": 700})
	assert.DeepEqual(t, cs["default"].Capacity.Utilization, map[string]int64{"memory": 30, "vcore": 70})
	assert.Equal(t, cs["default"].State, "Active")

	assert.Assert(t, cs["gpu"] != nil)
	assert.Equal(t, cs["gpu"].ClusterID, "rm-123")
	assert.Equal(t, cs["gpu"].Name, "gpu")
	assert.Equal(t, cs["default"].NodeSortingPolicy.Type, "fair")
	assert.Equal(t, cs["default"].NodeSortingPolicy.ResourceWeights["vcore"], 1.0)
	assert.Equal(t, cs["default"].NodeSortingPolicy.ResourceWeights["memory"], 1.0)
	assert.Equal(t, cs["gpu"].Applications["total"], 0)
}

func TestMetricsNotEmpty(t *testing.T) {
	req, err := http.NewRequest("GET", "/ws/v1/metrics", strings.NewReader(""))
	assert.NilError(t, err, "Error while creating the request")
	rr := httptest.NewRecorder()
	mux := http.HandlerFunc(promhttp.Handler().ServeHTTP)
	handler := loggingHandler(mux, "/ws/v1/metrics")
	handler.ServeHTTP(rr, req)
	assert.Assert(t, len(rr.Body.Bytes()) > 0, "Metrics response should not be empty")
}

func TestGetPartitionQueuesHandler(t *testing.T) {
	setup(t, configTwoLevelQueues, 2)

	NewWebApp(schedulerContext, nil)

	var req *http.Request
	req, err := http.NewRequest("GET", "/ws/v1/partition/default/queues", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID}}))
	assert.NilError(t, err, "Get Queues for PartitionQueues Handler request failed")
	resp := &MockResponseWriter{}
	var partitionQueuesDao dao.PartitionQueueDAOInfo
	getPartitionQueues(resp, req)
	err = json.Unmarshal(resp.outputBytes, &partitionQueuesDao)
	assert.NilError(t, err, "failed to unmarshal PartitionQueues dao response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, partitionQueuesDao.Children[0].Parent, "root")
	assert.Equal(t, partitionQueuesDao.Children[1].Parent, "root")
	assert.Equal(t, partitionQueuesDao.Children[2].Parent, "root")
	assert.Equal(t, len(partitionQueuesDao.Properties), 1)
	assert.Equal(t, partitionQueuesDao.Properties["application.sort.policy"], "stateaware")
	assert.Equal(t, len(partitionQueuesDao.TemplateInfo.Properties), 1)
	assert.Equal(t, partitionQueuesDao.TemplateInfo.Properties["application.sort.policy"], "stateaware")

	maxResourcesConf := make(map[string]string)
	maxResourcesConf["memory"] = "600000"
	maxResource, err := resources.NewResourceFromConf(maxResourcesConf)
	assert.NilError(t, err)
	assert.DeepEqual(t, partitionQueuesDao.TemplateInfo.MaxResource, maxResource.DAOMap())

	guaranteedResourcesConf := make(map[string]string)
	guaranteedResourcesConf["memory"] = "400000"
	guaranteedResources, err := resources.NewResourceFromConf(guaranteedResourcesConf)
	assert.NilError(t, err)
	assert.DeepEqual(t, partitionQueuesDao.TemplateInfo.GuaranteedResource, guaranteedResources.DAOMap())

	// Partition not exists
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/queues", strings.NewReader(""))
	req.WithContext(context.WithValue(req.Context(), "partition", "notexists"))
	assert.NilError(t, err, "Get Queues for PartitionQueues Handler request failed")
	resp = &MockResponseWriter{}
	getPartitionQueues(resp, req)
	assertPartitionExists(t, resp)
}

func TestGetClusterInfo(t *testing.T) {
	setup(t, configTwoLevelQueues, 2)

	resp := &MockResponseWriter{}
	getClusterInfo(resp, nil)
	var data []*dao.ClusterDAOInfo
	err := json.Unmarshal(resp.outputBytes, &data)
	assert.NilError(t, err)
	assert.Equal(t, 2, len(data))

	cs := make(map[string]*dao.ClusterDAOInfo, 2)
	for _, d := range data {
		cs[d.PartitionName] = d
	}

	assert.Assert(t, cs["default"] != nil)
	assert.Assert(t, cs["gpu"] != nil)
}

func TestGetPartitionNodes(t *testing.T) {
	partition := setup(t, configDefault, 1)

	// create test application
	appID := "app1"
	app := newApplication(appID, partition.Name, queueName, rmID, security.UserGroup{User: "testuser", Groups: []string{"testgroup"}})
	err := partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	// create test nodes
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 1000, siCommon.CPU: 1000}).ToProto()
	node1ID := "node-1"
	node1 := objects.NewNode(&si.NodeInfo{NodeID: node1ID, SchedulableResource: nodeRes})
	node2ID := "node-2"
	node2 := objects.NewNode(&si.NodeInfo{NodeID: node2ID, SchedulableResource: nodeRes})

	// create test allocations
	resAlloc1 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 500, siCommon.CPU: 300})
	resAlloc2 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 300, siCommon.CPU: 500})
	ask1 := objects.NewAllocationAsk("alloc-1", appID, resAlloc1)
	ask2 := objects.NewAllocationAsk("alloc-2", appID, resAlloc2)
	allocs := []*objects.Allocation{objects.NewAllocation("alloc-1-uuid", node1ID, ask1)}
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")
	allocs = []*objects.Allocation{objects.NewAllocation("alloc-2-uuid", node2ID, ask2)}
	err = partition.AddNode(node2, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")

	NewWebApp(schedulerContext, nil)

	var req *http.Request
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/nodes", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID}}))
	assert.NilError(t, err, "Get Nodes for PartitionNodes Handler request failed")
	resp := &MockResponseWriter{}
	var partitionNodesDao []*dao.NodeDAOInfo
	getPartitionNodes(resp, req)
	err = json.Unmarshal(resp.outputBytes, &partitionNodesDao)
	assert.NilError(t, err, "failed to unmarshal PartitionNodes dao response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, 1, len(partitionNodesDao[0].Allocations))
	for _, node := range partitionNodesDao {
		assert.Equal(t, 1, len(node.Allocations))
		if !node.IsReserved {
			assert.Equal(t, len(node.Reservations), 0)
		} else {
			assert.Check(t, len(node.Reservations) > 0, "Get wrong reservation info from node dao")
		}

		if node.NodeID == node1ID {
			assert.Equal(t, node.NodeID, node1ID)
			assert.Equal(t, "alloc-1", node.Allocations[0].AllocationKey)
			assert.Equal(t, "alloc-1-uuid", node.Allocations[0].UUID)
			assert.DeepEqual(t, map[string]int64{"memory": 50, "vcore": 30}, node.Utilized)
		} else {
			assert.Equal(t, node.NodeID, node2ID)
			assert.Equal(t, "alloc-2", node.Allocations[0].AllocationKey)
			assert.Equal(t, "alloc-2-uuid", node.Allocations[0].UUID)
			assert.DeepEqual(t, map[string]int64{"memory": 30, "vcore": 50}, node.Utilized)
		}
	}

	var req1 *http.Request
	req1, err = http.NewRequest("GET", "/ws/v1/partition/default/nodes", strings.NewReader(""))
	req.WithContext(context.WithValue(req.Context(), "partition", "notexists"))
	assert.NilError(t, err, "Get Nodes for PartitionNodes Handler request failed")
	resp1 := &MockResponseWriter{}
	getPartitionNodes(resp1, req1)
	assertPartitionExists(t, resp1)
}

// addApp Add app to the given partition and assert the app count, state etc
func addApp(t *testing.T, id string, part *scheduler.PartitionContext, queueName string, isCompleted bool) *objects.Application {
	return addAppWithUserGroup(t, id, part, queueName, isCompleted, security.UserGroup{})
}

// addApp Add app to the given partition and assert the app count, state etc
func addAppWithUserGroup(t *testing.T, id string, part *scheduler.PartitionContext, queueName string, isCompleted bool, userGroup security.UserGroup) *objects.Application {
	initSize := len(part.GetApplications())
	app := newApplication(id, part.Name, queueName, rmID, userGroup)
	err := part.AddApplication(app)
	assert.NilError(t, err, "Failed to add Application to Partition.")
	assert.Equal(t, app.CurrentState(), objects.New.String())
	assert.Equal(t, 1+initSize, len(part.GetApplications()))
	if isCompleted {
		app.SetState(objects.Completing.String())
		err = app.HandleApplicationEvent(objects.CompleteApplication)
		assert.NilError(t, err, "The app should have completed")
	}
	return app
}

func TestGetQueueApplicationsHandler(t *testing.T) {
	part := setup(t, configDefault, 1)

	// add an application
	app := addApp(t, "app-1", part, "root.default", false)

	// add placeholder to test PlaceholderDAOInfo
	tg := "tg-1"
	res := &si.Resource{
		Resources: map[string]*si.Quantity{"vcore": {Value: 1}},
	}
	ask := objects.NewAllocationAskFromSI(&si.AllocationAsk{
		ApplicationID:  "app-1",
		PartitionName:  part.Name,
		TaskGroupName:  tg,
		ResourceAsk:    res,
		Placeholder:    true,
		MaxAllocations: 1})
	err := app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	app.SetTimedOutPlaceholder(tg, 1)

	NewWebApp(schedulerContext, nil)

	var req *http.Request
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/queue/root.default/applications", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "queue", Value: "root.default"},
	}))
	assert.NilError(t, err, "Get Queue Applications Handler request failed")
	resp := &MockResponseWriter{}
	var appsDao []*dao.ApplicationDAOInfo
	getQueueApplications(resp, req)
	err = json.Unmarshal(resp.outputBytes, &appsDao)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, len(appsDao), 1)

	if !appsDao[0].HasReserved {
		assert.Equal(t, len(appsDao[0].Reservations), 0)
	} else {
		assert.Check(t, len(appsDao[0].Reservations) > 0, "app should have at least 1 reservation")
	}

	// check PlaceholderData
	assert.Equal(t, len(appsDao[0].PlaceholderData), 1)
	assert.Equal(t, appsDao[0].PlaceholderData[0].TaskGroupName, tg)
	assert.DeepEqual(t, appsDao[0].PlaceholderData[0].MinResource, map[string]int64{"vcore": 1})
	assert.Equal(t, appsDao[0].PlaceholderData[0].Replaced, int64(0))
	assert.Equal(t, appsDao[0].PlaceholderData[0].Count, int64(1))
	assert.Equal(t, appsDao[0].PlaceholderData[0].TimedOut, int64(1))

	// test nonexistent partition
	var req1 *http.Request
	req1, err = http.NewRequest("GET", "/ws/v1/partition/default/queue/root.default/applications", strings.NewReader(""))
	req1 = req1.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "partition", Value: "notexists"},
		httprouter.Param{Key: "queue", Value: "root.default"},
	}))
	assert.NilError(t, err, "Get Queue Applications Handler request failed")
	resp1 := &MockResponseWriter{}
	getQueueApplications(resp1, req1)
	assertPartitionExists(t, resp1)

	// test nonexistent queue
	var req2 *http.Request
	req2, err = http.NewRequest("GET", "/ws/v1/partition/default/queue/root.default/applications", strings.NewReader(""))
	req2 = req2.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "queue", Value: "notexists"},
	}))
	assert.NilError(t, err, "Get Queue Applications Handler request failed")
	resp2 := &MockResponseWriter{}
	getQueueApplications(resp2, req2)
	assertQueueExists(t, resp2)

	// test queue without applications
	var req3 *http.Request
	req3, err = http.NewRequest("GET", "/ws/v1/partition/default/queue/root.noapps/applications", strings.NewReader(""))
	req3 = req3.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "queue", Value: "root.noapps"},
	}))
	assert.NilError(t, err, "Get Queue Applications Handler request failed")
	resp3 := &MockResponseWriter{}
	var appsDao3 []*dao.ApplicationDAOInfo
	getQueueApplications(resp3, req3)
	err = json.Unmarshal(resp3.outputBytes, &appsDao3)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, len(appsDao3), 0)
}

func TestGetApplicationHandler(t *testing.T) {
	part := setup(t, configDefault, 1)

	// add 1 application
	app := addApp(t, "app-1", part, "root.default", false)
	res := &si.Resource{
		Resources: map[string]*si.Quantity{"vcore": {Value: 1}},
	}
	ask := objects.NewAllocationAskFromSI(&si.AllocationAsk{
		ApplicationID:  "app-1",
		PartitionName:  part.Name,
		ResourceAsk:    res,
		MaxAllocations: 1})
	err := app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")

	NewWebApp(schedulerContext, nil)

	var req *http.Request
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/queue/root.default/application/app-1", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "queue", Value: "root.default"},
		httprouter.Param{Key: "application", Value: "app-1"},
	}))
	assert.NilError(t, err, "Get Application Handler request failed")
	resp := &MockResponseWriter{}
	var appsDao *dao.ApplicationDAOInfo
	getApplication(resp, req)
	err = json.Unmarshal(resp.outputBytes, &appsDao)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body: %s", string(resp.outputBytes))

	if !appsDao.HasReserved {
		assert.Equal(t, len(appsDao.Reservations), 0)
	} else {
		assert.Check(t, len(appsDao.Reservations) > 0, "app should have at least 1 reservation")
	}

	// test nonexistent partition
	var req1 *http.Request
	req1, err = http.NewRequest("GET", "/ws/v1/partition/default/queue/root.default/application/app-1", strings.NewReader(""))
	req1 = req1.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "partition", Value: "notexists"},
		httprouter.Param{Key: "queue", Value: "root.default"},
		httprouter.Param{Key: "application", Value: "app-1"},
	}))
	assert.NilError(t, err, "Get Application Handler request failed")
	resp1 := &MockResponseWriter{}
	getApplication(resp1, req1)
	assertPartitionExists(t, resp1)

	// test nonexistent queue
	var req2 *http.Request
	req2, err = http.NewRequest("GET", "/ws/v1/partition/default/queue/root.default/application/app-1", strings.NewReader(""))
	req2 = req2.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "queue", Value: "notexists"},
		httprouter.Param{Key: "application", Value: "app-1"},
	}))
	assert.NilError(t, err, "Get Application Handler request failed")
	resp2 := &MockResponseWriter{}
	getApplication(resp2, req2)
	assertQueueExists(t, resp2)

	// test nonexistent application
	var req3 *http.Request
	req3, err = http.NewRequest("GET", "/ws/v1/partition/default/queue/root.noapps/application/app-1", strings.NewReader(""))
	req3 = req3.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "queue", Value: "root.noapps"},
		httprouter.Param{Key: "application", Value: "app-1"},
	}))
	assert.NilError(t, err, "Get Application Handler request failed")
	resp3 := &MockResponseWriter{}
	getApplication(resp3, req3)
	assertApplicationExists(t, resp3)
}

func assertPartitionExists(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body")
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, "Incorrect Status code")
	assert.Equal(t, errInfo.Message, PartitionDoesNotExists, "JSON error message is incorrect")
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func assertQueueExists(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body")
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, "Incorrect Status code")
	assert.Equal(t, errInfo.Message, QueueDoesNotExists, "JSON error message is incorrect")
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func assertApplicationExists(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body")
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, "Incorrect Status code")
	assert.Equal(t, errInfo.Message, "Application not found", "JSON error message is incorrect")
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func assertUserExists(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body")
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, "Incorrect Status code")
	assert.Equal(t, errInfo.Message, "User not found", "JSON error message is incorrect")
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func assertUserNameExists(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body")
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, "Incorrect Status code")
	assert.Equal(t, errInfo.Message, "User name is missing", "JSON error message is incorrect")
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func assertGroupExists(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body")
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, "Incorrect Status code")
	assert.Equal(t, errInfo.Message, "Group not found", "JSON error message is incorrect")
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func assertGroupNameExists(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body")
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, "Incorrect Status code")
	assert.Equal(t, errInfo.Message, "Group name is missing", "JSON error message is incorrect")
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func TestValidateQueue(t *testing.T) {
	err := validateQueue("root.test.test123")
	assert.NilError(t, err, "Queue path is correct but stil throwing error.")

	invalidQueuePath := "root.test.test123@"
	invalidQueueName := "test123@"
	err1 := validateQueue(invalidQueuePath)
	assert.Error(t, err1, "problem in queue query parameter parsing as queue param "+invalidQueuePath+" contains invalid queue name "+invalidQueueName+". Queue name must only have alphanumeric characters, - or _, and be no longer than 64 characters")

	err2 := validateQueue("root")
	assert.NilError(t, err2, "Queue path is correct but stil throwing error.")
}

func TestFullStateDumpPath(t *testing.T) {
	schedulerContext = prepareSchedulerContext(t, false)

	partitionContext := schedulerContext.GetPartitionMapClone()
	context := partitionContext[normalizedPartitionName]
	app := newApplication("appID", normalizedPartitionName, "root.default", rmID, security.UserGroup{})
	err := context.AddApplication(app)
	assert.NilError(t, err, "failed to add Application to partition")

	imHistory = history.NewInternalMetricsHistory(5)
	req, err2 := http.NewRequest("GET", "/ws/v1/getfullstatedump", strings.NewReader(""))
	assert.NilError(t, err2)
	req = mux.SetURLVars(req, make(map[string]string))
	resp := &MockResponseWriter{}

	getFullStateDump(resp, req)
	statusCode := resp.statusCode
	assert.Check(t, statusCode != http.StatusInternalServerError, "response status code")
	var aggregated AggregatedStateInfo
	err = json.Unmarshal(resp.outputBytes, &aggregated)
	assert.NilError(t, err)
	verifyStateDumpJSON(t, &aggregated)
}

func TestGetLoggerLevel(t *testing.T) {
	req, err := http.NewRequest("GET", "/ws/v1/loglevel", strings.NewReader(""))
	assert.NilError(t, err)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(getLogLevel)
	handler.ServeHTTP(rr, req)

	expected := "debug"
	assert.Equal(t, rr.Body.String(), expected,
		fmt.Sprintf("handler returned unexpected body: got %v want %v", rr.Body.String(), expected))
	assert.Equal(t, rr.Code, http.StatusOK)
}

func TestSetLoggerLevel(t *testing.T) {
	// invalid
	req, err := http.NewRequest("PUT", "/ws/v1/loglevel", strings.NewReader(""))
	assert.NilError(t, err)
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "level", Value: "invalid"},
	}))
	rr := httptest.NewRecorder()

	handler := http.HandlerFunc(setLogLevel)
	handler.ServeHTTP(rr, req)
	assert.Equal(t, rr.Code, http.StatusBadRequest)

	// valid
	req, err = http.NewRequest("PUT", "/ws/v1/loglevel", strings.NewReader(""))
	assert.NilError(t, err)

	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "level", Value: "error"},
	}))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	assert.Equal(t, rr.Code, http.StatusOK)
}

func TestSpecificUserAndGroupResourceUsage(t *testing.T) {
	prepareUserAndGroupContext(t)
	// Test user name is missing
	req, err := http.NewRequest("GET", "/ws/v1/partition/default/usage/user/", strings.NewReader(""))
	assert.NilError(t, err, "Get User Resource Usage Handler request failed")
	resp := &MockResponseWriter{}
	getUserResourceUsage(resp, req)
	assertUserNameExists(t, resp)

	// Test group name is missing
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/group/", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "user", Value: "testuser"},
		httprouter.Param{Key: "group", Value: ""},
	}))
	assert.NilError(t, err, "Get Group Resource Usage Handler request failed")
	resp = &MockResponseWriter{}
	getGroupResourceUsage(resp, req)
	assertGroupNameExists(t, resp)

	// Test existed user query
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/user/", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "user", Value: "testuser"},
		httprouter.Param{Key: "group", Value: "testgroup"},
	}))
	assert.NilError(t, err, "Get User Resource Usage Handler request failed")
	resp = &MockResponseWriter{}
	getUserResourceUsage(resp, req)

	// Test non-existing user query
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/user/", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "user", Value: "testNonExistingUser"},
		httprouter.Param{Key: "group", Value: "testgroup"},
	}))
	assert.NilError(t, err, "Get User Resource Usage Handler request failed")
	resp = &MockResponseWriter{}
	getUserResourceUsage(resp, req)
	assertUserExists(t, resp)

	// Test existed group query
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/group/", strings.NewReader(""))
	assert.NilError(t, err, "Get Group Resource Usage Handler request failed")
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "user", Value: "testuser"},
		httprouter.Param{Key: "group", Value: "testgroup"},
	}))
	var groupResourceUsageDao *dao.GroupResourceUsageDAOInfo
	getGroupResourceUsage(resp, req)
	err = json.Unmarshal(resp.outputBytes, &groupResourceUsageDao)
	assert.NilError(t, err, "failed to unmarshal group resource usage dao response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, groupResourceUsageDao.Queues.ResourceUsage.String(),
		resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.CPU: 1}).String())

	// Test non-existing group query
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/group/", strings.NewReader(""))
	assert.NilError(t, err, "Get Group Resource Usage Handler request failed")
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "user", Value: "testuser"},
		httprouter.Param{Key: "group", Value: "testNonExistingGroup"},
	}))
	getGroupResourceUsage(resp, req)
	assertGroupExists(t, resp)
}

func TestUsersAndGroupsResourceUsage(t *testing.T) {
	prepareUserAndGroupContext(t)
	var req *http.Request
	req, err := http.NewRequest("GET", "/ws/v1/partition/default/usage/users", strings.NewReader(""))
	assert.NilError(t, err, "Get Users Resource Usage Handler request failed")
	resp := &MockResponseWriter{}
	var usersResourceUsageDao []*dao.UserResourceUsageDAOInfo
	getUsersResourceUsage(resp, req)
	err = json.Unmarshal(resp.outputBytes, &usersResourceUsageDao)
	assert.NilError(t, err, "failed to unmarshal users resource usage dao response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, usersResourceUsageDao[0].Queues.ResourceUsage.String(),
		resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.CPU: 1}).String())

	// Assert existing users
	assert.Equal(t, len(usersResourceUsageDao), 1)
	assert.Equal(t, usersResourceUsageDao[0].UserName, "testuser")

	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/groups", strings.NewReader(""))
	assert.NilError(t, err, "Get Groups Resource Usage Handler request failed")

	var groupsResourceUsageDao []*dao.GroupResourceUsageDAOInfo
	getGroupsResourceUsage(resp, req)
	err = json.Unmarshal(resp.outputBytes, &groupsResourceUsageDao)
	assert.NilError(t, err, "failed to unmarshal groups resource usage dao response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, groupsResourceUsageDao[0].Queues.ResourceUsage.String(),
		resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.CPU: 1}).String())

	// Assert existing groups
	assert.Equal(t, len(groupsResourceUsageDao), 1)
	assert.Equal(t, groupsResourceUsageDao[0].GroupName, "testgroup")
}

func prepareSchedulerContext(t *testing.T, stateDumpConf bool) *scheduler.ClusterContext {
	var config []byte
	if !stateDumpConf {
		config = []byte(configDefault)
	} else {
		config = []byte(configStateDumpFilePath)
	}
	var err error
	schedulerContext, err = scheduler.NewClusterContext(rmID, policyGroup, config)
	assert.NilError(t, err, "Error when load clusterInfo from config")
	assert.Equal(t, 1, len(schedulerContext.GetPartitionMapClone()))

	return schedulerContext
}

func prepareUserAndGroupContext(t *testing.T) {
	part := setup(t, configDefault, 1)
	userManager := ugm.GetUserManager()
	userManager.ClearUserTrackers()
	userManager.ClearGroupTrackers()

	// add 1 application
	app := addAppWithUserGroup(t, "app-1", part, "root.default", false, security.UserGroup{
		User:   "",
		Groups: []string{""},
	})
	res := &si.Resource{
		Resources: map[string]*si.Quantity{"vcore": {Value: 1}},
	}
	ask := objects.NewAllocationAskFromSI(&si.AllocationAsk{
		ApplicationID:  "app-1",
		PartitionName:  part.Name,
		ResourceAsk:    res,
		MaxAllocations: 1})
	err := app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")

	// add an alloc
	uuid := "uuid-1"
	allocInfo := objects.NewAllocation(uuid, "node-1", ask)
	app.AddAllocation(allocInfo)
	assert.Assert(t, app.IsStarting(), "Application did not return starting state after alloc: %s", app.CurrentState())

	NewWebApp(schedulerContext, nil)
}

func verifyStateDumpJSON(t *testing.T, aggregated *AggregatedStateInfo) {
	assert.Check(t, aggregated.Timestamp != 0)
	assert.Check(t, len(aggregated.Partitions) > 0)
	assert.Check(t, len(aggregated.Nodes) > 0)
	assert.Check(t, len(aggregated.ClusterInfo) > 0)
	assert.Check(t, len(aggregated.Queues) > 0)
	assert.Check(t, len(aggregated.LogLevel) > 0)
}
