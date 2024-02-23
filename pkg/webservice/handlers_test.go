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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/metrics/history"
	"github.com/apache/yunikorn-core/pkg/scheduler"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const unmarshalError = "Failed to unmarshal error response from response body"
const statusCodeError = "Incorrect Status code"
const jsonMessageError = "JSON error message is incorrect"

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
          maxapplications: 10
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
                properties:
                  application.sort.policy: fifo
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
`

const userGroupLimitsConfig = `
partitions:
    - name: default
      queues:
        - name: root
          parent: true
          submitacl: '*'
          queues:
            - name: parent1
              parent: true
              limits:
                - limit: ""
                  users:
                    - test_user
                  maxapplications: 0
                  maxresources:
                    cpu: "200"
`

const userGroupLimitsInvalidConfig = `
partitions:
    - name: default
      queues:
        - name: root
          parent: true
          submitacl: '*'
          queues:
            - name: parent1
              parent: true
              limits:
                - limit: ""
                  users:
                    - test_user
                  maxapplications: 1
                  maxresources:
                    cpu: "0"
`

const userGroupLimitsInvalidConfig1 = `
partitions:
    - name: default
      queues:
        - name: root
          parent: true
          submitacl: '*'
          queues:
            - name: parent1
              parent: true
              limits:
                - limit: ""
                  users:
                    - test_user
`

const groupsLimitsConfig = `
partitions:
    - name: default
      queues:
        - name: root
          parent: true
          submitacl: '*'
          queues:
            - name: default
              limits:
                - limit: ""
                  groups:
                    - testgroup
                  maxresources:
                    cpu: "200"
`

const rmID = "rm-123"
const policyGroup = "default-policy-group"
const queueName = "root.default"
const nodeID = "node-1"

var (
	updatedExtraConf = map[string]string{
		"log.level":                  "info",
		"service.schedulingInterval": "1s",
		"admissionController.accessControl.bypassAuth": "false",
	}
)

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
		assert.NilError(t, err, unmarshalError)
		assert.Equal(t, vcr.Allowed, test.expectedResponse.Allowed, "allowed flag incorrect")
		assert.Equal(t, vcr.Reason, test.expectedResponse.Reason, "response text not as expected")
	}
}

func TestUserGroupLimits(t *testing.T) {
	confTests := []struct {
		content          string
		expectedResponse dao.ValidateConfResponse
	}{
		{
			content: userGroupLimitsConfig,
			expectedResponse: dao.ValidateConfResponse{
				Allowed: true,
				Reason:  common.Empty,
			},
		},
		{
			content: userGroupLimitsInvalidConfig,
			expectedResponse: dao.ValidateConfResponse{
				Allowed: false,
				Reason:  "MaxResources should be greater than zero in '' limit",
			},
		},
		{
			content: userGroupLimitsInvalidConfig1,
			expectedResponse: dao.ValidateConfResponse{
				Allowed: false,
				Reason:  "invalid resource combination for limit  all resource limits are null",
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
		assert.NilError(t, err, unmarshalError)
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
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusInternalServerError, resp.statusCode, "app history handler returned wrong status")
	assert.Equal(t, errInfo.Message, "Internal metrics collection is not enabled.", jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusInternalServerError)

	// init should return null and thus no records
	imHistory = history.NewInternalMetricsHistory(5)
	resp = &MockResponseWriter{}
	getApplicationHistory(resp, req)
	var appHist []dao.ApplicationHistoryDAOInfo
	err = json.Unmarshal(resp.outputBytes, &appHist)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, resp.statusCode, 0, "app response should have no status")
	assert.Assert(t, appHist != nil, "appHist should not be nil")
	assert.Equal(t, len(appHist), 0, "empty response must have no records")

	// add new history records
	imHistory.Store(1, 0)
	imHistory.Store(2, 0)
	imHistory.Store(30, 0)
	resp = &MockResponseWriter{}
	getApplicationHistory(resp, req)
	err = json.Unmarshal(resp.outputBytes, &appHist)
	assert.NilError(t, err, unmarshalError)
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
	assert.NilError(t, err, unmarshalError)
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
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusInternalServerError, resp.statusCode, "container history handler returned wrong status")
	assert.Equal(t, errInfo.Message, "Internal metrics collection is not enabled.", jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusInternalServerError)

	// init should return null and thus no records
	imHistory = history.NewInternalMetricsHistory(5)
	resp = &MockResponseWriter{}
	getContainerHistory(resp, req)
	var contHist []dao.ContainerHistoryDAOInfo
	err = json.Unmarshal(resp.outputBytes, &contHist)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, resp.statusCode, 0, "container response should have no status")
	assert.Check(t, contHist != nil, "contHist should not be nil")
	assert.Equal(t, len(contHist), 0, "empty response must have no records")

	// add new history records
	imHistory.Store(0, 1)
	imHistory.Store(0, 2)
	imHistory.Store(0, 30)
	resp = &MockResponseWriter{}
	getContainerHistory(resp, req)
	err = json.Unmarshal(resp.outputBytes, &contHist)
	assert.NilError(t, err, unmarshalError)
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
	assert.NilError(t, err, unmarshalError)
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
	conf := &dao.ConfigDAOInfo{}
	err = yaml.Unmarshal(resp.outputBytes, conf)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "fair", "node sort policy set incorrectly, not fair")

	startConfSum := conf.Checksum
	assert.Assert(t, len(startConfSum) > 0, "checksum boundary not found")

	// change the config
	err = schedulerContext.UpdateRMSchedulerConfig(rmID, []byte(updatedConf))
	assert.NilError(t, err, "Error when updating clusterInfo from config")
	configs.SetConfigMap(updatedExtraConf)

	// check that we return yaml by default, unmarshal will error when we don't
	req.Header.Set("Accept", "unknown")
	getClusterConfig(resp, req)
	err = yaml.Unmarshal(resp.outputBytes, conf)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "binpacking", "node sort policy not updated")
	assert.Assert(t, startConfSum != conf.Checksum, "checksums did not change in output")
	assert.DeepEqual(t, conf.Extra, updatedExtraConf)

	// reset extra config map
	configs.SetConfigMap(map[string]string{})
}

func TestGetConfigJSON(t *testing.T) {
	setup(t, startConf, 1)
	// No err check: new request always returns correctly
	//nolint: errcheck
	req, _ := http.NewRequest("GET", "", nil)
	req.Header.Set("Accept", "application/json")
	resp := &MockResponseWriter{}
	getClusterConfig(resp, req)

	conf := &dao.ConfigDAOInfo{}
	err := json.Unmarshal(resp.outputBytes, conf)
	assert.NilError(t, err, unmarshalError)
	startConfSum := conf.Checksum
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "fair", "node sort policy set incorrectly, not fair (json)")

	// change the config
	err = schedulerContext.UpdateRMSchedulerConfig(rmID, []byte(updatedConf))
	assert.NilError(t, err, "Error when updating clusterInfo from config")
	configs.SetConfigMap(updatedExtraConf)

	getClusterConfig(resp, req)
	err = json.Unmarshal(resp.outputBytes, conf)
	assert.NilError(t, err, unmarshalError)
	assert.Assert(t, startConfSum != conf.Checksum, "checksums did not change in json output: %s, %s", startConfSum, conf.Checksum)
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "binpacking", "node sort policy not updated (json)")
	assert.DeepEqual(t, conf.Extra, updatedExtraConf)

	// reset extra config map
	configs.SetConfigMap(map[string]string{})
}

func TestGetClusterUtilJSON(t *testing.T) {
	setup(t, configDefault, 1)

	// check build information of RM
	buildInfoMap := make(map[string]string)
	buildInfoMap["buildDate"] = "2006-01-02T15:04:05-0700"
	buildInfoMap["buildVersion"] = "latest"
	buildInfoMap["isPluginVersion"] = "false"
	schedulerContext.SetRMInfo(rmID, buildInfoMap)
	rmBuildInformationMaps := getRMBuildInformation(nil)
	assert.Equal(t, 0, len(rmBuildInformationMaps))
	rmInfo := schedulerContext.GetRMInfoMapClone()
	assert.Equal(t, 1, len(rmInfo))
	rmBuildInformationMaps = getRMBuildInformation(rmInfo)
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
	alloc1 := objects.NewAllocation(nodeID, ask1)
	alloc2 := objects.NewAllocation(nodeID, ask2)
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
	node1ID := "node-1"
	node1 := objects.NewNode(&si.NodeInfo{NodeID: node1ID, SchedulableResource: nodeRes})
	node2ID := "node-2"
	nodeRes2 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 1000, siCommon.CPU: 1000, "GPU": 10}).ToProto()
	node2 := objects.NewNode(&si.NodeInfo{NodeID: node2ID, SchedulableResource: nodeRes2})
	node3ID := "node-3"
	nodeCPU := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.CPU: 1000}).ToProto()
	node3 := objects.NewNode(&si.NodeInfo{NodeID: node3ID, SchedulableResource: nodeCPU})

	// create test allocations
	resAlloc1 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 500, siCommon.CPU: 300})
	resAlloc2 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 300, siCommon.CPU: 500, "GPU": 5})
	ask1 := objects.NewAllocationAsk("alloc-1", appID, resAlloc1)
	ask2 := objects.NewAllocationAsk("alloc-2", appID, resAlloc2)
	allocs := []*objects.Allocation{objects.NewAllocation(node1ID, ask1)}
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")
	allocs = []*objects.Allocation{objects.NewAllocation(node2ID, ask2)}
	err = partition.AddNode(node2, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")
	err = partition.AddNode(node3, nil)
	assert.NilError(t, err, "add node to partition should not have failed")

	// two nodes advertise memory: must show up in the list
	result := getNodesUtilJSON(partition, siCommon.Memory)
	subResult := result.NodesUtil
	assert.Equal(t, result.ResourceType, siCommon.Memory)
	assert.Equal(t, subResult[2].NumOfNodes, int64(1))
	assert.Equal(t, subResult[4].NumOfNodes, int64(1))
	assert.Equal(t, subResult[2].NodeNames[0], node2ID)
	assert.Equal(t, subResult[4].NodeNames[0], node1ID)

	// three nodes advertise cpu: must show up in the list
	result = getNodesUtilJSON(partition, siCommon.CPU)
	subResult = result.NodesUtil
	assert.Equal(t, result.ResourceType, siCommon.CPU)
	assert.Equal(t, subResult[0].NumOfNodes, int64(1))
	assert.Equal(t, subResult[0].NodeNames[0], node3ID)
	assert.Equal(t, subResult[2].NumOfNodes, int64(1))
	assert.Equal(t, subResult[2].NodeNames[0], node1ID)
	assert.Equal(t, subResult[4].NumOfNodes, int64(1))
	assert.Equal(t, subResult[4].NodeNames[0], node2ID)

	// one node advertise GPU: must show up in the list
	result = getNodesUtilJSON(partition, "GPU")
	subResult = result.NodesUtil
	assert.Equal(t, result.ResourceType, "GPU")
	assert.Equal(t, subResult[4].NumOfNodes, int64(1))
	assert.Equal(t, subResult[4].NodeNames[0], node2ID)

	result = getNodesUtilJSON(partition, "non-exist")
	subResult = result.NodesUtil
	assert.Equal(t, result.ResourceType, "non-exist")
	assert.Equal(t, subResult[0].NumOfNodes, int64(0))
	assert.Equal(t, len(subResult[0].NodeNames), 0)
}

func TestGetNodeUtilisation(t *testing.T) {
	NewWebApp(&scheduler.ClusterContext{}, nil)

	// var req *http.Request
	req, err := http.NewRequest("GET", "/ws/v1/scheduler/node-utilization", strings.NewReader(""))
	assert.NilError(t, err, "Get node utilisation Handler request failed")
	req = req.WithContext(context.TODO())
	resp := &MockResponseWriter{}

	getNodeUtilisation(resp, req)
	var errInfo dao.YAPIError
	err = json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, "getNodeUtilisation should have returned and error")

	partition := setup(t, configDefault, 1)
	utilisation := &dao.NodesUtilDAOInfo{}
	err = json.Unmarshal(resp.outputBytes, utilisation)
	assert.NilError(t, err, "getNodeUtilisation should have returned an empty object")
	assert.Equal(t, utilisation.ResourceType, "", "unexpected type returned")
	assert.Equal(t, len(utilisation.NodesUtil), 0, "no nodes should be returned")
	assert.Assert(t, confirmNodeCount(utilisation.NodesUtil, 0), "unexpected number of nodes returned should be 0")

	// create test nodes
	node1ID := "node-1"
	node2ID := "node-2"
	node1 := addNode(t, partition, node1ID, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	node2 := addNode(t, partition, node2ID, resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 5}))

	// get nodes utilization
	getNodeUtilisation(resp, req)
	utilisation = &dao.NodesUtilDAOInfo{}
	err = json.Unmarshal(resp.outputBytes, utilisation)
	assert.NilError(t, err, "getNodeUtilisation should have returned an object")
	assert.Equal(t, utilisation.ResourceType, "", "unexpected type returned")
	assert.Equal(t, len(utilisation.NodesUtil), 10, "empty usage: unexpected bucket count returned")
	assert.Assert(t, confirmNodeCount(utilisation.NodesUtil, 0), "unexpected number of nodes returned should be 0")

	resAlloc := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	ask := objects.NewAllocationAsk("alloc-1", "app", resAlloc)
	alloc := objects.NewAllocation(node1ID, ask)
	assert.Assert(t, node1.AddAllocation(alloc), "unexpected failure adding allocation to node")
	rootQ := partition.GetQueue("root")
	err = rootQ.IncAllocatedResource(resAlloc, false)
	assert.NilError(t, err, "unexpected error returned setting allocated resource on queue")
	// get nodes utilization
	getNodeUtilisation(resp, req)
	utilisation = &dao.NodesUtilDAOInfo{}
	err = json.Unmarshal(resp.outputBytes, utilisation)
	assert.NilError(t, err, "getNodeUtilisation should have returned an object")
	assert.Equal(t, utilisation.ResourceType, "first", "expected first as type returned")
	assert.Equal(t, len(utilisation.NodesUtil), 10, "empty usage: unexpected bucket count returned")
	assert.Assert(t, confirmNodeCount(utilisation.NodesUtil, 2), "unexpected number of nodes returned should be 2")

	// make second type dominant by using all
	resAlloc = resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5})
	ask = objects.NewAllocationAsk("alloc-2", "app", resAlloc)
	alloc = objects.NewAllocation(node2ID, ask)
	assert.Assert(t, node2.AddAllocation(alloc), "unexpected failure adding allocation to node")
	err = rootQ.IncAllocatedResource(resAlloc, false)
	assert.NilError(t, err, "unexpected error returned setting allocated resource on queue")
	// get nodes utilization
	getNodeUtilisation(resp, req)
	utilisation = &dao.NodesUtilDAOInfo{}
	err = json.Unmarshal(resp.outputBytes, utilisation)
	assert.NilError(t, err, "getNodeUtilisation should have returned an object")
	assert.Equal(t, utilisation.ResourceType, "second", "expected second as type returned")
	assert.Equal(t, len(utilisation.NodesUtil), 10, "empty usage: unexpected bucket count returned")
	assert.Assert(t, confirmNodeCount(utilisation.NodesUtil, 1), "unexpected number of nodes returned should be 1")
}

func addNode(t *testing.T, partition *scheduler.PartitionContext, nodeId string, resource *resources.Resource) *objects.Node {
	nodeRes := resource.ToProto()
	node := objects.NewNode(&si.NodeInfo{NodeID: nodeId, SchedulableResource: nodeRes})
	err := partition.AddNode(node, nil)
	assert.NilError(t, err, "adding node to partition should not fail")
	return node
}

func addAllocatedResource(t *testing.T, node *objects.Node, allocationKey string, appID string, quantityMap map[string]resources.Quantity) {
	t.Helper()
	resAlloc := resources.NewResourceFromMap(quantityMap)
	ask := objects.NewAllocationAsk(allocationKey, appID, resAlloc)
	alloc := objects.NewAllocation(node.NodeID, ask)
	assert.Assert(t, node.AddAllocation(alloc), "unexpected failure adding allocation to node")
}

func confirmNodeCount(info []*dao.NodeUtilDAOInfo, count int64) bool {
	var total int64
	for _, node := range info {
		total += node.NumOfNodes
	}
	return total == count
}

func addAndConfirmApplicationExists(t *testing.T, partitionName string, partition *scheduler.PartitionContext, appName string) *objects.Application {
	// add a new app
	app := newApplication(appName, partitionName, "root.default", rmID, security.UserGroup{User: "testuser", Groups: []string{"testgroup"}})
	err := partition.AddApplication(app)
	assert.NilError(t, err, "Failed to add Application to Partition.")
	assert.Equal(t, app.CurrentState(), objects.New.String())
	return app
}

func TestGetPartitionNodesUtilJSON(t *testing.T) {
	// setup
	partition := setup(t, configDefault, 1)
	appID := "app1"
	node1ID := "node-1"
	node2ID := "node-2"
	node3ID := "node-3"

	// create test nodes
	node1 := addNode(t, partition, node1ID, resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 1000, siCommon.CPU: 1000}))
	node2 := addNode(t, partition, node2ID, resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 1000, siCommon.CPU: 1000, "GPU": 10}))
	addNode(t, partition, node3ID, resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.CPU: 1000}))

	// create test allocations
	addAllocatedResource(t, node1, "alloc-1", appID, map[string]resources.Quantity{siCommon.Memory: 500, siCommon.CPU: 300})
	addAllocatedResource(t, node2, "alloc-2", appID, map[string]resources.Quantity{siCommon.Memory: 300, siCommon.CPU: 500, "GPU": 5})

	// assert partition nodes utilization
	result := getPartitionNodesUtilJSON(partition)
	assert.Equal(t, result.ClusterID, rmID)
	assert.Equal(t, result.Partition, "default")
	assert.Equal(t, len(result.NodesUtilList), 3, "Should have 3 resource types(CPU/Memory/GPU) in the list.")

	// two nodes advertise memory: must show up in the list
	memoryNodesUtil := getNodesUtilByType(t, result.NodesUtilList, siCommon.Memory)
	assert.Equal(t, memoryNodesUtil.NodesUtil[2].NumOfNodes, int64(1))
	assert.Equal(t, memoryNodesUtil.NodesUtil[4].NumOfNodes, int64(1))
	assert.Equal(t, memoryNodesUtil.NodesUtil[2].NodeNames[0], node2ID)
	assert.Equal(t, memoryNodesUtil.NodesUtil[4].NodeNames[0], node1ID)

	// three nodes advertise cpu: must show up in the list
	cpuNodesUtil := getNodesUtilByType(t, result.NodesUtilList, siCommon.CPU)
	assert.Equal(t, cpuNodesUtil.NodesUtil[0].NumOfNodes, int64(1))
	assert.Equal(t, cpuNodesUtil.NodesUtil[0].NodeNames[0], node3ID)
	assert.Equal(t, cpuNodesUtil.NodesUtil[2].NumOfNodes, int64(1))
	assert.Equal(t, cpuNodesUtil.NodesUtil[2].NodeNames[0], node1ID)
	assert.Equal(t, cpuNodesUtil.NodesUtil[4].NumOfNodes, int64(1))
	assert.Equal(t, cpuNodesUtil.NodesUtil[4].NodeNames[0], node2ID)

	// one node advertise GPU: must show up in the list
	gpuNodesUtil := getNodesUtilByType(t, result.NodesUtilList, "GPU")
	assert.Equal(t, gpuNodesUtil.NodesUtil[4].NumOfNodes, int64(1))
	assert.Equal(t, gpuNodesUtil.NodesUtil[4].NodeNames[0], node2ID)
}

func TestGetNodeUtilisations(t *testing.T) {
	// setup
	NewWebApp(&scheduler.ClusterContext{}, nil)
	req, err := http.NewRequest("GET", "/ws/v1/scheduler/node-utilizations", strings.NewReader(""))
	assert.NilError(t, err, "Get node utilisations Handler request failed")
	resp := &MockResponseWriter{}

	getNodeUtilisations(resp, req)
	var partitionNodesUtilDAOInfo []*dao.PartitionNodesUtilDAOInfo
	err = json.Unmarshal(resp.outputBytes, &partitionNodesUtilDAOInfo)
	assert.NilError(t, err, "should decode a empty list of *dao.PartitionNodesUtilDAOInfo")
	assert.Equal(t, len(partitionNodesUtilDAOInfo), 0)

	// setup partitions
	schedulerContext, err = scheduler.NewClusterContext(rmID, policyGroup, []byte(configMultiPartitions))
	assert.NilError(t, err, "Error when load clusterInfo from config")
	schedulerContext.GetPartition("default")
	defaultPartition := schedulerContext.GetPartition(common.GetNormalizedPartitionName("default", rmID))
	gpuPartition := schedulerContext.GetPartition(common.GetNormalizedPartitionName("gpu", rmID))

	// add nodes to partitions
	node1 := addNode(t, defaultPartition, "node-1", resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 10}))
	node2 := addNode(t, defaultPartition, "node-2", resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 10, "vcore": 5}))
	node3 := addNode(t, defaultPartition, "node-3", resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 20, "vcore": 15}))
	node4 := addNode(t, gpuPartition, "node-4", resources.NewResourceFromMap(map[string]resources.Quantity{"gpu": 10}))
	// add allocatedResource to nodes
	addAllocatedResource(t, node1, "alloc-1", "app-1", map[string]resources.Quantity{"memory": 1})
	addAllocatedResource(t, node2, "alloc-1", "app-1", map[string]resources.Quantity{"memory": 1, "vcore": 1})
	addAllocatedResource(t, node3, "alloc-1", "app-1", map[string]resources.Quantity{"memory": 1, "vcore": 1})
	addAllocatedResource(t, node4, "alloc-1", "app-1", map[string]resources.Quantity{"gpu": 1})

	// get nodes utilizations
	getNodeUtilisations(resp, req)
	err = json.Unmarshal(resp.outputBytes, &partitionNodesUtilDAOInfo)
	assert.NilError(t, err, "should decode a list of *dao.PartitionNodesUtilDAOInfo")
	assert.Equal(t, len(partitionNodesUtilDAOInfo), 2)
	assert.Equal(t, partitionNodesUtilDAOInfo[0].ClusterID, rmID)
	assert.Equal(t, partitionNodesUtilDAOInfo[1].ClusterID, rmID)

	defaultPartitionNodesUtilDAOInfo := partitionNodesUtilDAOInfo[0]
	gpuPartitionNodesUtilDAOInfo := partitionNodesUtilDAOInfo[1]
	if defaultPartitionNodesUtilDAOInfo.Partition == "gpu" {
		defaultPartitionNodesUtilDAOInfo = partitionNodesUtilDAOInfo[1]
		gpuPartitionNodesUtilDAOInfo = partitionNodesUtilDAOInfo[0]
	}

	assert.Equal(t, len(defaultPartitionNodesUtilDAOInfo.NodesUtilList), 2)
	assert.Equal(t, len(gpuPartitionNodesUtilDAOInfo.NodesUtilList), 1)

	assertNodeUtilisationContent(t, defaultPartitionNodesUtilDAOInfo, "memory", 3)
	assertNodeUtilisationContent(t, defaultPartitionNodesUtilDAOInfo, "vcore", 2)
	assertNodeUtilisationContent(t, gpuPartitionNodesUtilDAOInfo, "gpu", 1)
}

func assertNodeUtilisationContent(t *testing.T, partitionNodesUtilDAOInfo *dao.PartitionNodesUtilDAOInfo, resourceType string, expectedNodeCount int) {
	t.Helper()
	nodeUtilisation := getNodesUtilByType(t, partitionNodesUtilDAOInfo.NodesUtilList, resourceType)
	assert.Equal(t, nodeUtilisation.ResourceType, resourceType, fmt.Sprintf("should have returned '%s', but got '%s'", resourceType, nodeUtilisation.ResourceType))
	assert.Equal(t, len(nodeUtilisation.NodesUtil), 10, fmt.Sprintf("should have 10 bucket, but got %d", len(nodeUtilisation.NodesUtil)))
	assert.Assert(t,
		confirmNodeCount(nodeUtilisation.NodesUtil, int64(expectedNodeCount)),
		fmt.Sprintf("unexpected number of nodes returned, should be %d", expectedNodeCount),
	)
}

func getNodesUtilByType(t *testing.T, nodesUtilList []*dao.NodesUtilDAOInfo, resourceType string) *dao.NodesUtilDAOInfo {
	t.Helper()
	for _, nodesUtil := range nodesUtilList {
		if nodesUtil.ResourceType == resourceType {
			return nodesUtil
		}
	}
	t.Fatalf("should have returned a *dao.NodesUtilDAOInfo with resourceType %s", resourceType)
	return nil
}

func TestPartitions(t *testing.T) {
	schedulerContext = &scheduler.ClusterContext{}

	var req *http.Request
	req, err := http.NewRequest("GET", "/ws/v1/partitions", strings.NewReader(""))
	assert.NilError(t, err, "App Handler request failed")

	resp := &MockResponseWriter{}
	var partitionInfo []*dao.PartitionInfo
	getPartitions(resp, req)
	err = json.Unmarshal(resp.outputBytes, &partitionInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Check(t, partitionInfo != nil, "partitionInfo should not be nil")
	assert.Equal(t, len(partitionInfo), 0)

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
	allocs := []*objects.Allocation{objects.NewAllocation(node1ID, ask1)}
	err = defaultPartition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")
	allocs = []*objects.Allocation{objects.NewAllocation(node2ID, ask2)}
	err = defaultPartition.AddNode(node2, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")

	req, err = http.NewRequest("GET", "/ws/v1/partitions", strings.NewReader(""))
	assert.NilError(t, err, "App Handler request failed")
	resp = &MockResponseWriter{}
	getPartitions(resp, req)
	err = json.Unmarshal(resp.outputBytes, &partitionInfo)
	assert.NilError(t, err, unmarshalError)

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
	handler := http.HandlerFunc(promhttp.Handler().ServeHTTP)
	handler.ServeHTTP(rr, req)
	assert.Assert(t, len(rr.Body.Bytes()) > 0, "Metrics response should not be empty")
}

func TestGetPartitionQueuesHandler(t *testing.T) {
	setup(t, configTwoLevelQueues, 2)

	NewWebApp(schedulerContext, nil)

	tMaxResource, err := resources.NewResourceFromConf(map[string]string{"memory": "600000"})
	assert.NilError(t, err)
	tGuaranteedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "400000"})
	assert.NilError(t, err)

	templateInfo := dao.TemplateInfo{
		MaxApplications:    10,
		MaxResource:        tMaxResource.DAOMap(),
		GuaranteedResource: tGuaranteedResource.DAOMap(),
		Properties: map[string]string{
			configs.ApplicationSortPolicy: policies.StateAwarePolicy.String(),
		},
	}

	maxResource, err := resources.NewResourceFromConf(map[string]string{"memory": "800000", "vcore": "80000"})
	assert.NilError(t, err)
	guaranteedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "500000", "vcore": "50000"})
	assert.NilError(t, err)

	var req *http.Request
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/queues", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID}}))
	assert.NilError(t, err, "Get Queues for PartitionQueues Handler request failed")
	resp := &MockResponseWriter{}
	var partitionQueuesDao dao.PartitionQueueDAOInfo
	getPartitionQueues(resp, req)
	err = json.Unmarshal(resp.outputBytes, &partitionQueuesDao)
	assert.NilError(t, err, unmarshalError)
	// assert root fields
	assert.Equal(t, partitionQueuesDao.QueueName, configs.RootQueue)
	assert.Equal(t, partitionQueuesDao.Status, objects.Active.String())
	assert.Equal(t, partitionQueuesDao.Partition, configs.DefaultPartition)
	assert.Assert(t, partitionQueuesDao.PendingResource == nil)
	assert.Assert(t, partitionQueuesDao.MaxResource == nil)
	assert.Assert(t, partitionQueuesDao.GuaranteedResource == nil)
	assert.Assert(t, partitionQueuesDao.AllocatedResource == nil)
	assert.Assert(t, partitionQueuesDao.PreemptingResource == nil)
	assert.Assert(t, partitionQueuesDao.HeadRoom == nil)
	assert.Assert(t, !partitionQueuesDao.IsLeaf)
	assert.Assert(t, partitionQueuesDao.IsManaged)
	assert.Equal(t, partitionQueuesDao.Parent, "")
	assert.Assert(t, partitionQueuesDao.AbsUsedCapacity == nil)
	assert.Equal(t, partitionQueuesDao.MaxRunningApps, uint64(0))
	assert.Equal(t, partitionQueuesDao.RunningApps, uint64(0))
	assert.Equal(t, partitionQueuesDao.CurrentPriority, configs.MinPriority)
	assert.Assert(t, partitionQueuesDao.AllocatingAcceptedApps == nil)
	assert.Equal(t, len(partitionQueuesDao.Properties), 1)
	assert.Equal(t, partitionQueuesDao.Properties[configs.ApplicationSortPolicy], policies.StateAwarePolicy.String())
	assert.DeepEqual(t, partitionQueuesDao.TemplateInfo, &templateInfo)

	// assert child root.a fields
	assert.Equal(t, len(partitionQueuesDao.Children), 1)
	child := &partitionQueuesDao.Children[0]
	assert.Equal(t, child.QueueName, "root.a")
	assert.Equal(t, child.Status, objects.Active.String())
	assert.Equal(t, child.Partition, "")
	assert.Assert(t, child.PendingResource == nil)
	assert.DeepEqual(t, child.MaxResource, maxResource.DAOMap())
	assert.DeepEqual(t, child.GuaranteedResource, guaranteedResource.DAOMap())
	assert.Assert(t, child.AllocatedResource == nil)
	assert.Assert(t, child.PreemptingResource == nil)
	assert.DeepEqual(t, child.HeadRoom, maxResource.DAOMap())
	assert.Assert(t, !child.IsLeaf)
	assert.Assert(t, child.IsManaged)
	assert.Equal(t, child.Parent, configs.RootQueue)
	assert.Assert(t, child.AbsUsedCapacity == nil)
	assert.Equal(t, child.MaxRunningApps, uint64(0))
	assert.Equal(t, child.RunningApps, uint64(0))
	assert.Equal(t, child.CurrentPriority, configs.MinPriority)
	assert.Assert(t, child.AllocatingAcceptedApps == nil)
	assert.Equal(t, len(child.Properties), 1)
	assert.Equal(t, child.Properties[configs.ApplicationSortPolicy], policies.StateAwarePolicy.String())
	assert.DeepEqual(t, child.TemplateInfo, &templateInfo)

	// assert child root.a.a1 fields
	assert.Equal(t, len(partitionQueuesDao.Children[0].Children), 1)
	child = &partitionQueuesDao.Children[0].Children[0]
	assert.Equal(t, child.QueueName, "root.a.a1")
	assert.Equal(t, child.Status, objects.Active.String())
	assert.Equal(t, child.Partition, "")
	assert.Assert(t, child.PendingResource == nil)
	assert.DeepEqual(t, child.MaxResource, maxResource.DAOMap())
	assert.DeepEqual(t, child.GuaranteedResource, guaranteedResource.DAOMap())
	assert.Assert(t, child.AllocatedResource == nil)
	assert.Assert(t, child.PreemptingResource == nil)
	assert.DeepEqual(t, child.HeadRoom, maxResource.DAOMap())
	assert.Assert(t, child.IsLeaf)
	assert.Assert(t, child.IsManaged)
	assert.Equal(t, child.Parent, "root.a")
	assert.Assert(t, child.AbsUsedCapacity == nil)
	assert.Equal(t, child.MaxRunningApps, uint64(0))
	assert.Equal(t, child.RunningApps, uint64(0))
	assert.Equal(t, child.CurrentPriority, configs.MinPriority)
	assert.Assert(t, child.AllocatingAcceptedApps == nil)
	assert.Equal(t, len(child.Properties), 1)
	assert.Equal(t, child.Properties[configs.ApplicationSortPolicy], policies.FifoSortPolicy.String())
	assert.Assert(t, child.TemplateInfo == nil)

	// Partition not exists
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/queues", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{httprouter.Param{Key: "partition", Value: "notexists"}}))
	assert.NilError(t, err, "Get Queues for PartitionQueues Handler request failed")
	resp = &MockResponseWriter{}
	getPartitionQueues(resp, req)
	assertPartitionNotExists(t, resp)

	// test params name missing
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/queues", strings.NewReader(""))
	assert.NilError(t, err, "Get Queues for PartitionQueues Handler request failed")
	resp = &MockResponseWriter{}
	getPartitionQueues(resp, req)
	assertParamsMissing(t, resp)
}

func TestGetClusterInfo(t *testing.T) {
	schedulerContext = &scheduler.ClusterContext{}
	resp := &MockResponseWriter{}
	getClusterInfo(resp, nil)
	var data []*dao.ClusterDAOInfo
	err := json.Unmarshal(resp.outputBytes, &data)
	assert.NilError(t, err)
	assert.Equal(t, 0, len(data))

	setup(t, configTwoLevelQueues, 2)

	resp = &MockResponseWriter{}
	getClusterInfo(resp, nil)
	err = json.Unmarshal(resp.outputBytes, &data)
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
	attributesOfnode1 := map[string]string{"Disk": "SSD"}
	attributesOfnode2 := map[string]string{"Devices": "camera"}
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 1000, siCommon.CPU: 1000}).ToProto()
	node1ID := "node-1"
	node1 := objects.NewNode(&si.NodeInfo{NodeID: node1ID, Attributes: attributesOfnode1, SchedulableResource: nodeRes})
	node2ID := "node-2"
	node2 := objects.NewNode(&si.NodeInfo{NodeID: node2ID, Attributes: attributesOfnode2, SchedulableResource: nodeRes})

	// create test allocations
	resAlloc1 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 500, siCommon.CPU: 300})
	resAlloc2 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 300, siCommon.CPU: 500})
	ask1 := objects.NewAllocationAsk("alloc-1", appID, resAlloc1)
	ask2 := objects.NewAllocationAsk("alloc-2", appID, resAlloc2)
	allocs := []*objects.Allocation{objects.NewAllocation(node1ID, ask1)}
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")
	allocs = []*objects.Allocation{objects.NewAllocation(node2ID, ask2)}
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
	assert.NilError(t, err, unmarshalError)
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
			assert.Equal(t, "alloc-1-0", node.Allocations[0].UUID)
			assert.Equal(t, "alloc-1-0", node.Allocations[0].AllocationID)
			assert.DeepEqual(t, attributesOfnode1, node.Attributes)
			assert.DeepEqual(t, map[string]int64{"memory": 50, "vcore": 30}, node.Utilized)
		} else {
			assert.Equal(t, node.NodeID, node2ID)
			assert.Equal(t, "alloc-2", node.Allocations[0].AllocationKey)
			assert.Equal(t, "alloc-2-0", node.Allocations[0].UUID)
			assert.Equal(t, "alloc-2-0", node.Allocations[0].AllocationID)
			assert.DeepEqual(t, attributesOfnode2, node.Attributes)
			assert.DeepEqual(t, map[string]int64{"memory": 30, "vcore": 50}, node.Utilized)
		}
	}

	req, err = http.NewRequest("GET", "/ws/v1/partition/default/nodes", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{httprouter.Param{Key: "partition", Value: "notexists"}}))
	assert.NilError(t, err, "Get Nodes for PartitionNodes Handler request failed")
	resp1 := &MockResponseWriter{}
	getPartitionNodes(resp1, req)
	assertPartitionNotExists(t, resp1)

	// test params name missing
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/nodes", strings.NewReader(""))
	assert.NilError(t, err, "Get Nodes for PartitionNodes Handler request failed")
	resp = &MockResponseWriter{}
	getPartitionNodes(resp, req)
	assertParamsMissing(t, resp)

	// Test specific node
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/node/node-1", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{httprouter.Param{Key: "node", Value: "node-1"}}))
	assert.NilError(t, err, "Get Node for PartitionNode Handler request failed")
	resp = &MockResponseWriter{}
	getPartitionNode(resp, req)

	// Test node id is missing
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/node/node-1", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{httprouter.Param{Key: "partition", Value: "default"}, httprouter.Param{Key: "node", Value: ""}}))
	assert.NilError(t, err, "Get Node for PartitionNode Handler request failed")
	resp = &MockResponseWriter{}
	getPartitionNode(resp, req)
	assertNodeIDNotExists(t, resp)
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
		currentCount := len(part.GetCompletedApplications())
		err = app.HandleApplicationEvent(objects.CompleteApplication)
		assert.NilError(t, err, "The app should have completed")
		err = common.WaitFor(10*time.Millisecond, time.Second, func() bool {
			newCount := len(part.GetCompletedApplications())
			return newCount == currentCount+1
		})
		assert.NilError(t, err, "the completed application should have been processed")
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
	assert.NilError(t, err, unmarshalError)
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
	assertPartitionNotExists(t, resp1)

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
	assertQueueNotExists(t, resp2)

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
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, len(appsDao3), 0)

	// test missing params name
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/queue/root.default/applications", strings.NewReader(""))
	assert.NilError(t, err, "Get Queue Applications Handler request failed")
	resp = &MockResponseWriter{}
	getQueueApplications(resp, req)
	assertParamsMissing(t, resp)
}

func checkLegalGetAppsRequest(t *testing.T, url string, params httprouter.Params, expected []*dao.ApplicationDAOInfo) {
	req, err := http.NewRequest("GET", url, strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, params))
	assert.NilError(t, err)
	resp := &MockResponseWriter{}
	var appsDao []*dao.ApplicationDAOInfo
	getPartitionApplicationsByState(resp, req)
	err = json.Unmarshal(resp.outputBytes, &appsDao)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, len(appsDao), len(expected))
}

func checkIllegalGetAppsRequest(t *testing.T, url string, params httprouter.Params, assertFunc func(t *testing.T, resp *MockResponseWriter)) {
	req, err := http.NewRequest("GET", url, strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, params))
	assert.NilError(t, err)
	resp := &MockResponseWriter{}
	getPartitionApplicationsByState(resp, req)
	assertFunc(t, resp)
}

func TestGetPartitionApplicationsByStateHandler(t *testing.T) {
	defaultPartition := setup(t, configDefault, 1)
	NewWebApp(schedulerContext, nil)

	// add a new application
	app1 := addApp(t, "app-1", defaultPartition, "root.default", false)
	app1.SetState(objects.New.String())

	// add a running application
	app2 := addApp(t, "app-2", defaultPartition, "root.default", false)
	app2.SetState(objects.Running.String())

	// add a completed application
	app3 := addApp(t, "app-3", defaultPartition, "root.default", true)

	// add a rejected application
	app4 := newApplication("app-4", defaultPartition.Name, "root.default", rmID, security.UserGroup{})
	rejectedMessage := fmt.Sprintf("Failed to place application %s: application rejected: no placement rule matched", app3.ApplicationID)
	defaultPartition.AddRejectedApplication(app3, rejectedMessage)

	// test get active app
	expectedActiveDao := []*dao.ApplicationDAOInfo{getApplicationDAO(app1), getApplicationDAO(app2)}
	checkLegalGetAppsRequest(t, "/ws/v1/partition/default/applications/Active", httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "state", Value: "Active"}}, expectedActiveDao)

	// test get active app with running state
	expectedRunningDao := []*dao.ApplicationDAOInfo{getApplicationDAO(app2)}
	checkLegalGetAppsRequest(t, "/ws/v1/partition/default/applications/Active?status=Running", httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "state", Value: "Active"}}, expectedRunningDao)

	// test get completed app
	expectedCompletedDao := []*dao.ApplicationDAOInfo{getApplicationDAO(app3)}
	checkLegalGetAppsRequest(t, "/ws/v1/partition/default/applications/Completed", httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "state", Value: "Completed"}}, expectedCompletedDao)

	// test get rejected app
	expectedRejectedDao := []*dao.ApplicationDAOInfo{getApplicationDAO(app4)}
	checkLegalGetAppsRequest(t, "/ws/v1/partition/default/applications/Rejected", httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "state", Value: "Rejected"}}, expectedRejectedDao)

	// test nonexistent partition
	checkIllegalGetAppsRequest(t, "/ws/v1/partition/default/applications/Active", httprouter.Params{
		httprouter.Param{Key: "partition", Value: "notexists"},
		httprouter.Param{Key: "state", Value: "Active"}}, assertPartitionNotExists)

	// test disallow state
	checkIllegalGetAppsRequest(t, "/ws/v1/partition/default/applications/Accepted", httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "state", Value: "Accepted"}}, assertAppStateAllow)

	// test disallow active state
	checkIllegalGetAppsRequest(t, "/ws/v1/partition/default/applications/Active?status=invalid", httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "state", Value: "Active"}}, assertActiveStateAllow)

	// test missing params name
	checkIllegalGetAppsRequest(t, "/ws/v1/partition/default/applications/Active", nil, assertParamsMissing)
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
	assert.NilError(t, err, unmarshalError)

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
	assertPartitionNotExists(t, resp1)

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
	assertQueueNotExists(t, resp2)

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
	assertApplicationNotExists(t, resp3)

	// test without queue
	var req4 *http.Request
	req4, err = http.NewRequest("GET", "/ws/v1/partition/default/application/app-1", strings.NewReader(""))
	req4 = req4.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "application", Value: "app-1"},
	}))
	assert.NilError(t, err, "Get Application Handler request failed")
	resp4 := &MockResponseWriter{}
	var appsDao4 *dao.ApplicationDAOInfo
	getApplication(resp4, req4)
	err = json.Unmarshal(resp4.outputBytes, &appsDao4)
	assert.NilError(t, err, unmarshalError)

	// test invalid queue name
	var req5 *http.Request
	req5, err = http.NewRequest("GET", "/ws/v1/partition/default/queue/root.default/application/app-1", strings.NewReader(""))
	req5 = req5.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "queue", Value: "root.test.test123@"},
		httprouter.Param{Key: "application", Value: "app-1"},
	}))
	assert.NilError(t, err, "Get Application Handler request failed")
	resp5 := &MockResponseWriter{}
	getApplication(resp5, req5)
	var errInfo dao.YAPIError
	err = json.Unmarshal(resp5.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp5.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, "problem in queue query parameter parsing as queue param root.test.test123@ contains invalid queue name test123@. Queue name must only have alphanumeric characters, - or _, and be no longer than 64 characters", jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)

	// test missing params name
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/queue/root.default/application/app-1", strings.NewReader(""))
	assert.NilError(t, err, "Get Application Handler request failed")
	resp = &MockResponseWriter{}
	getApplication(resp, req)
	assertParamsMissing(t, resp)
}

func assertParamsMissing(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, MissingParamsName, jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func assertPartitionNotExists(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusNotFound, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, PartitionDoesNotExists, jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusNotFound)
}

func assertQueueNotExists(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusNotFound, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, QueueDoesNotExists, jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusNotFound)
}

func assertApplicationNotExists(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusNotFound, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, ApplicationDoesNotExists, jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusNotFound)
}

func assertUserExists(t *testing.T, resp *MockResponseWriter, expected *dao.UserResourceUsageDAOInfo) {
	var actual *dao.UserResourceUsageDAOInfo
	err := json.Unmarshal(resp.outputBytes, &actual)
	assert.NilError(t, err, unmarshalError)
	assert.DeepEqual(t, actual, expected)
}

func assertUserNotExists(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusNotFound, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, UserDoesNotExists, jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusNotFound)
}

func assertUserNameMissing(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, UserNameMissing, jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func assertGroupExists(t *testing.T, resp *MockResponseWriter, expected *dao.GroupResourceUsageDAOInfo) {
	var actual *dao.GroupResourceUsageDAOInfo
	err := json.Unmarshal(resp.outputBytes, &actual)
	assert.NilError(t, err, unmarshalError)
	assert.DeepEqual(t, actual, expected)
}

func assertGroupNotExists(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusNotFound, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, GroupDoesNotExists, jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusNotFound)
}

func assertGroupNameMissing(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, GroupNameMissing, jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func assertNodeIDNotExists(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusNotFound, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, NodeDoesNotExists, jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusNotFound)
}

func assertAppStateAllow(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, "Only following application states are allowed: active, rejected, completed", jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func assertActiveStateAllow(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, allowedActiveStatusMsg, jsonMessageError)
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
	original := configs.GetConfigMap()
	defer func() {
		configs.SetConfigMap(original)
	}()
	configMap := map[string]string{
		"log.level": "WARN",
	}
	configs.SetConfigMap(configMap)

	schedulerContext = prepareSchedulerContext(t)

	partitionContext := schedulerContext.GetPartitionMapClone()
	context := partitionContext[normalizedPartitionName]
	app := newApplication("appID", normalizedPartitionName, "root.default", rmID, security.UserGroup{})
	err := context.AddApplication(app)
	assert.NilError(t, err, "failed to add Application to partition")

	imHistory = history.NewInternalMetricsHistory(5)
	req, err2 := http.NewRequest("GET", "/ws/v1/getfullstatedump", strings.NewReader(""))
	assert.NilError(t, err2)
	resp := &MockResponseWriter{}

	getFullStateDump(resp, req)
	statusCode := resp.statusCode
	assert.Check(t, statusCode != http.StatusInternalServerError, "response status code")
	var aggregated AggregatedStateInfo
	err = json.Unmarshal(resp.outputBytes, &aggregated)
	assert.NilError(t, err)
	verifyStateDumpJSON(t, &aggregated)
}

func TestSpecificUserAndGroupResourceUsage(t *testing.T) {
	prepareUserAndGroupContext(t, groupsLimitsConfig)
	// Test user name is missing
	req, err := http.NewRequest("GET", "/ws/v1/partition/default/usage/user/", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "group", Value: "testgroup"},
	}))
	assert.NilError(t, err, "Get User Resource Usage Handler request failed")
	resp := &MockResponseWriter{}
	getUserResourceUsage(resp, req)
	assertUserNameMissing(t, resp)

	// Test group name is missing
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/group/", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "user", Value: "testuser"},
		httprouter.Param{Key: "group", Value: ""},
	}))
	assert.NilError(t, err, "Get Group Resource Usage Handler request failed")
	resp = &MockResponseWriter{}
	getGroupResourceUsage(resp, req)
	assertGroupNameMissing(t, resp)

	// Test existed user query
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/user/", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "user", Value: "testuser"},
		httprouter.Param{Key: "group", Value: "testgroup"},
	}))
	assert.NilError(t, err, "Get User Resource Usage Handler request failed")
	resp = &MockResponseWriter{}
	getUserResourceUsage(resp, req)
	assertUserExists(t, resp,
		&dao.UserResourceUsageDAOInfo{
			UserName: "testuser",
			Groups:   map[string]string{"app-1": "testgroup"},
			Queues: &dao.ResourceUsageDAOInfo{
				QueuePath:           "root",
				ResourceUsage:       resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1}),
				RunningApplications: []string{"app-1"},
				Children: []*dao.ResourceUsageDAOInfo{
					{
						QueuePath:           "root.default",
						ResourceUsage:       resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1}),
						RunningApplications: []string{"app-1"},
					},
				},
			},
		})

	// Test non-existing user query
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/user/", strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "user", Value: "testNonExistingUser"},
		httprouter.Param{Key: "group", Value: "testgroup"},
	}))
	assert.NilError(t, err, "Get User Resource Usage Handler request failed")
	resp = &MockResponseWriter{}
	getUserResourceUsage(resp, req)
	assertUserNotExists(t, resp)

	// Test existed group query
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/group/", strings.NewReader(""))
	assert.NilError(t, err, "Get Group Resource Usage Handler request failed")
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "user", Value: "testuser"},
		httprouter.Param{Key: "group", Value: "testgroup"},
	}))
	resp = &MockResponseWriter{}
	getGroupResourceUsage(resp, req)
	assertGroupExists(t, resp,
		&dao.GroupResourceUsageDAOInfo{
			GroupName:    "testgroup",
			Applications: []string{"app-1"},
			Queues: &dao.ResourceUsageDAOInfo{
				QueuePath:           "root",
				ResourceUsage:       resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1}),
				RunningApplications: []string{"app-1"},
				Children: []*dao.ResourceUsageDAOInfo{
					{
						QueuePath:           "root.default",
						ResourceUsage:       resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1}),
						MaxResources:        resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 200}),
						RunningApplications: []string{"app-1"},
					},
				},
			},
		})

	// Test non-existing group query
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/group/", strings.NewReader(""))
	assert.NilError(t, err, "Get Group Resource Usage Handler request failed")
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "user", Value: "testuser"},
		httprouter.Param{Key: "group", Value: "testNonExistingGroup"},
	}))
	resp = &MockResponseWriter{}
	getGroupResourceUsage(resp, req)
	assertGroupNotExists(t, resp)

	// Test params name missing
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/group/", strings.NewReader(""))
	assert.NilError(t, err, "Get Group Resource Usage Handler request failed")
	resp = &MockResponseWriter{}
	getGroupResourceUsage(resp, req)
	assertParamsMissing(t, resp)
}

func TestUsersAndGroupsResourceUsage(t *testing.T) {
	prepareUserAndGroupContext(t, groupsLimitsConfig)
	var req *http.Request
	req, err := http.NewRequest("GET", "/ws/v1/partition/default/usage/users", strings.NewReader(""))
	assert.NilError(t, err, "Get Users Resource Usage Handler request failed")
	resp := &MockResponseWriter{}
	var usersResourceUsageDao []*dao.UserResourceUsageDAOInfo
	getUsersResourceUsage(resp, req)
	err = json.Unmarshal(resp.outputBytes, &usersResourceUsageDao)
	assert.NilError(t, err, unmarshalError)
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
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, groupsResourceUsageDao[0].Queues.ResourceUsage.String(),
		resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.CPU: 1}).String())

	// Assert existing groups
	assert.Equal(t, len(groupsResourceUsageDao), 1)
	assert.Equal(t, groupsResourceUsageDao[0].GroupName, "testgroup")

	// test empty user group
	prepareEmptyUserGroupContext()

	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/users", strings.NewReader(""))
	assert.NilError(t, err, "Get Users Resource Usage Handler request failed")
	resp = &MockResponseWriter{}
	getUsersResourceUsage(resp, req)
	var userResourceUsageDao []*dao.UserResourceUsageDAOInfo
	err = json.Unmarshal(resp.outputBytes, &userResourceUsageDao)
	assert.NilError(t, err, unmarshalError)
	assert.DeepEqual(t, userResourceUsageDao, []*dao.UserResourceUsageDAOInfo{})

	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/groups", strings.NewReader(""))
	assert.NilError(t, err, "Get Groups Resource Usage Handler request failed")
	resp = &MockResponseWriter{}
	getGroupsResourceUsage(resp, req)
	var groupResourceUsageDao []*dao.GroupResourceUsageDAOInfo
	err = json.Unmarshal(resp.outputBytes, &groupResourceUsageDao)
	assert.NilError(t, err, unmarshalError)
	assert.DeepEqual(t, groupResourceUsageDao, []*dao.GroupResourceUsageDAOInfo{})
}

func TestGetEvents(t *testing.T) {
	prepareSchedulerContext(t)
	appEvent, nodeEvent, queueEvent := addEvents(t)

	checkAllEvents(t, []*si.EventRecord{appEvent, nodeEvent, queueEvent})

	checkSingleEvent(t, appEvent, "count=1")
	checkSingleEvent(t, queueEvent, "start=2")

	// illegal requests
	checkIllegalBatchRequest(t, "count=xyz", `strconv.ParseUint: parsing "xyz": invalid syntax`)
	checkIllegalBatchRequest(t, "count=-100", `strconv.ParseUint: parsing "-100": invalid syntax`)
	checkIllegalBatchRequest(t, "count=0", `0 is not a valid value for "count`)
	checkIllegalBatchRequest(t, "start=xyz", `strconv.ParseUint: parsing "xyz": invalid syntax`)
	checkIllegalBatchRequest(t, "start=-100", `strconv.ParseUint: parsing "-100": invalid syntax`)
}

func TestGetEventsWhenTrackingDisabled(t *testing.T) {
	original := configs.GetConfigMap()
	defer func() {
		ev := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
		ev.Stop()
		configs.SetConfigMap(original)
	}()
	configMap := map[string]string{
		configs.CMEventTrackingEnabled: "false",
	}
	configs.SetConfigMap(configMap)
	events.Init()
	ev := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	ev.StartServiceWithPublisher(false)

	req, err := http.NewRequest("GET", "/ws/v1/events/batch", strings.NewReader(""))
	assert.NilError(t, err)
	readIllegalRequest(t, req, http.StatusInternalServerError, "Event tracking is disabled")
}

func TestGetStream(t *testing.T) {
	ev, req := initEventsAndCreateRequest(t)
	defer ev.Stop()
	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req = req.Clone(cancelCtx)

	resp := NewResponseRecorderWithDeadline() // MockResponseWriter does not implement http.Flusher

	go func() {
		time.Sleep(200 * time.Millisecond)
		ev.AddEvent(&si.EventRecord{
			TimestampNano: 111,
			ObjectID:      "app-1",
		})
		ev.AddEvent(&si.EventRecord{
			TimestampNano: 222,
			ObjectID:      "node-1",
		})
		ev.AddEvent(&si.EventRecord{
			TimestampNano: 333,
			ObjectID:      "app-2",
		})
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	getStream(resp, req)

	output := make([]byte, 256)
	n, err := resp.Body.Read(output)
	assert.NilError(t, err, "cannot read response body")

	lines := strings.Split(string(output[:n]), "\n")
	assertEvent(t, lines[0], 111, "app-1")
	assertEvent(t, lines[1], 222, "node-1")
	assertEvent(t, lines[2], 333, "app-2")
}

func TestGetStream_StreamClosedByProducer(t *testing.T) {
	ev, req := initEventsAndCreateRequest(t)
	defer ev.Stop()
	resp := NewResponseRecorderWithDeadline() // MockResponseWriter does not implement http.Flusher

	go func() {
		time.Sleep(200 * time.Millisecond)
		ev.AddEvent(&si.EventRecord{
			TimestampNano: 111,
			ObjectID:      "app-1",
		})
		time.Sleep(100 * time.Millisecond)
		ev.CloseAllStreams()
	}()

	getStream(resp, req)

	output := make([]byte, 256)
	n, err := resp.Body.Read(output)
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.NilError(t, err, "cannot read response body")
	lines := strings.Split(string(output[:n]), "\n")
	assertEvent(t, lines[0], 111, "app-1")
	assertYunikornError(t, lines[1], "Event stream was closed by the producer")
}

func TestGetStream_NotFlusherImpl(t *testing.T) {
	var req *http.Request
	req, err := http.NewRequest("GET", "/ws/v1/events/stream", strings.NewReader(""))
	assert.NilError(t, err)
	resp := &MockResponseWriter{}

	getStream(resp, req)

	assert.Assert(t, strings.Contains(string(resp.outputBytes), "Writer does not implement http.Flusher"))
	assert.Equal(t, http.StatusInternalServerError, resp.statusCode)
}

func TestGetStream_Count(t *testing.T) {
	ev, req := initEventsAndCreateRequest(t)
	defer ev.Stop()
	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req = req.Clone(cancelCtx)
	resp := NewResponseRecorderWithDeadline() // MockResponseWriter does not implement http.Flusher

	// add some existing events
	ev.AddEvent(&si.EventRecord{TimestampNano: 0})
	ev.AddEvent(&si.EventRecord{TimestampNano: 1})
	ev.AddEvent(&si.EventRecord{TimestampNano: 2})
	time.Sleep(100 * time.Millisecond) // let the events propagate

	// case #1: "count" not set
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	getStream(resp, req)
	output := make([]byte, 256)
	n, err := resp.Body.Read(output)
	assert.Error(t, io.EOF, err.Error())
	assert.Equal(t, 0, n)

	// case #2: "count" is set to "2"
	req, err = http.NewRequest("GET", "/ws/v1/events/stream", strings.NewReader(""))
	assert.NilError(t, err)
	cancelCtx, cancel = context.WithCancel(context.Background())
	req = req.Clone(cancelCtx)
	defer cancel()
	req.URL.RawQuery = "count=2"
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	getStream(resp, req)
	output = make([]byte, 256)
	n, err = resp.Body.Read(output)
	assert.NilError(t, err)
	lines := strings.Split(string(output[:n]), "\n")
	assertEvent(t, lines[0], 1, "")
	assertEvent(t, lines[1], 2, "")

	// case #3: illegal value
	req, err = http.NewRequest("GET", "/ws/v1/events/stream", strings.NewReader(""))
	assert.NilError(t, err)
	cancelCtx, cancel = context.WithCancel(context.Background())
	req = req.Clone(cancelCtx)
	defer cancel()
	req.URL.RawQuery = "count=xyz"
	getStream(resp, req)
	output = make([]byte, 256)
	n, err = resp.Body.Read(output)
	assert.NilError(t, err)
	line := string(output[:n])
	assertYunikornError(t, line, `strconv.ParseUint: parsing "xyz": invalid syntax`)
}

func TestGetStream_TrackingDisabled(t *testing.T) {
	original := configs.GetConfigMap()
	defer func() {
		ev := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
		ev.Stop()
		configs.SetConfigMap(original)
	}()
	configMap := map[string]string{
		configs.CMEventTrackingEnabled: "false",
	}
	configs.SetConfigMap(configMap)
	_, req := initEventsAndCreateRequest(t)
	resp := httptest.NewRecorder()

	assertGetStreamError(t, req, resp, http.StatusInternalServerError, "Event tracking is disabled")
}

func TestGetStream_NoWriteDeadline(t *testing.T) {
	ev, req := initEventsAndCreateRequest(t)
	defer ev.Stop()
	resp := httptest.NewRecorder() // does not have SetWriteDeadline()

	assertGetStreamError(t, req, resp, http.StatusInternalServerError, "Cannot set write deadline: feature not supported")
}

func TestGetStream_SetWriteDeadlineFails(t *testing.T) {
	ev, req := initEventsAndCreateRequest(t)
	defer ev.Stop()
	resp := NewResponseRecorderWithDeadline()
	resp.setWriteFailsAt = 2 // only the second SetWriteDeadline() will fail
	resp.setWriteFails = true

	go func() {
		time.Sleep(200 * time.Millisecond)
		ev.AddEvent(&si.EventRecord{
			TimestampNano: 111,
			ObjectID:      "app-1",
		})
	}()

	getStream(resp, req)
	checkGetStreamErrorResult(t, resp.Result(), http.StatusInternalServerError, "Cannot set write deadline: SetWriteDeadline failed")
}

func TestGetStream_SetReadDeadlineFails(t *testing.T) {
	_, req := initEventsAndCreateRequest(t)
	resp := NewResponseRecorderWithDeadline()
	resp.setReadFails = true

	assertGetStreamError(t, req, resp, http.StatusInternalServerError, "Cannot set read deadline: SetReadDeadline failed")
}

func TestGetStream_Limit(t *testing.T) {
	current := configs.GetConfigMap()
	defer func() {
		configs.SetConfigMap(current)
	}()
	configs.SetConfigMap(map[string]string{
		configs.CMMaxEventStreams: "3",
	})
	resp := NewResponseRecorderWithDeadline()
	ev, req := initEventsAndCreateRequest(t)
	defer ev.Stop()

	cancelCtx, cancel := context.WithCancel(context.Background())
	req = req.Clone(cancelCtx)
	defer cancel()
	req.Host = "host-1"

	// start simulated connections in the background
	go getStream(NewResponseRecorderWithDeadline(), req)
	go getStream(NewResponseRecorderWithDeadline(), req)
	go getStream(NewResponseRecorderWithDeadline(), req)

	// wait until the StreamingLimiter.AddHost() calls
	err := common.WaitFor(time.Millisecond, time.Second, func() bool {
		streamingLimiter.Lock()
		defer streamingLimiter.Unlock()
		return streamingLimiter.streams == 3
	})
	assert.NilError(t, err)
	assertGetStreamError(t, req, resp, http.StatusServiceUnavailable, "Too many streaming connections")
}

func assertGetStreamError(t *testing.T, req *http.Request, resp interface{}, statusCode int, expectedMsg string) {
	t.Helper()
	var response *http.Response

	switch rec := resp.(type) {
	case *ResponseRecorderWithDeadline:
		getStream(rec, req)
		response = rec.Result()
	case *httptest.ResponseRecorder:
		getStream(rec, req)
		response = rec.Result()
	default:
		t.Fatalf("unknown response recorder type")
	}

	checkGetStreamErrorResult(t, response, statusCode, expectedMsg)
}

func checkGetStreamErrorResult(t *testing.T, response *http.Response, statusCode int, expectedMsg string) {
	t.Helper()
	output := make([]byte, 256)
	n, err := response.Body.Read(output)
	assert.NilError(t, err)
	line := string(output[:n])
	assertYunikornError(t, line, expectedMsg)
	assert.Equal(t, statusCode, response.StatusCode)
}

func initEventsAndCreateRequest(t *testing.T) (*events.EventSystemImpl, *http.Request) {
	t.Helper()
	events.Init()
	ev := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	ev.StartServiceWithPublisher(false)

	var req *http.Request
	req, err := http.NewRequest("GET", "/ws/v1/events/stream", strings.NewReader(""))
	assert.NilError(t, err)

	return ev, req
}

func assertEvent(t *testing.T, output string, tsNano int64, objectID string) {
	t.Helper()
	var evt si.EventRecord
	err := json.Unmarshal([]byte(output), &evt)
	assert.NilError(t, err)
	assert.Equal(t, tsNano, evt.TimestampNano)
	assert.Equal(t, objectID, evt.ObjectID)
}

func assertYunikornError(t *testing.T, output, errMsg string) {
	t.Helper()
	var ykErr dao.YAPIError
	err := json.Unmarshal([]byte(output), &ykErr)
	assert.NilError(t, err)
	assert.Equal(t, errMsg, ykErr.Description)
	assert.Equal(t, errMsg, ykErr.Message)
}

func addEvents(t *testing.T) (appEvent, nodeEvent, queueEvent *si.EventRecord) {
	t.Helper()
	events.Init()
	ev := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	ev.StartServiceWithPublisher(false)
	protoRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"cpu": 1,
	}).ToProto()

	appEvent = &si.EventRecord{
		Type:              si.EventRecord_APP,
		TimestampNano:     100,
		Message:           "app event",
		EventChangeType:   si.EventRecord_ADD,
		EventChangeDetail: si.EventRecord_APP_ALLOC,
		ObjectID:          "app",
		ReferenceID:       "alloc",
		Resource:          protoRes,
	}
	ev.AddEvent(appEvent)
	nodeEvent = &si.EventRecord{
		Type:              si.EventRecord_NODE,
		TimestampNano:     101,
		Message:           "node event",
		EventChangeType:   si.EventRecord_ADD,
		EventChangeDetail: si.EventRecord_APP_ALLOC,
		ObjectID:          "node",
		ReferenceID:       "alloc",
		Resource:          protoRes,
	}
	ev.AddEvent(nodeEvent)
	queueEvent = &si.EventRecord{
		Type:              si.EventRecord_QUEUE,
		TimestampNano:     102,
		Message:           "queue event",
		EventChangeType:   si.EventRecord_REMOVE,
		EventChangeDetail: si.EventRecord_QUEUE_APP,
		ObjectID:          "root.default",
		ReferenceID:       "app",
	}
	ev.AddEvent(queueEvent)
	noEvents := 0
	err := common.WaitFor(10*time.Millisecond, time.Second, func() bool {
		noEvents = ev.Store.CountStoredEvents()
		return noEvents == 3
	})
	assert.NilError(t, err, "wanted 3 events, got %d", noEvents)
	return appEvent, nodeEvent, queueEvent
}

func checkSingleEvent(t *testing.T, event *si.EventRecord, query string) {
	req, err := http.NewRequest("GET", "/ws/v1/events/batch?"+query, strings.NewReader(""))
	assert.NilError(t, err)
	eventDao := getEventRecordDao(t, req)
	assert.Assert(t, eventDao.InstanceUUID != "")
	assert.Equal(t, 1, len(eventDao.EventRecords))
	compareEvents(t, event, eventDao.EventRecords[0])
}

func checkIllegalBatchRequest(t *testing.T, query, msg string) {
	t.Helper()
	req, err := http.NewRequest("GET", "/ws/v1/events/batch?"+query, strings.NewReader(""))
	assert.NilError(t, err)
	readIllegalRequest(t, req, http.StatusBadRequest, msg)
}

func readIllegalRequest(t *testing.T, req *http.Request, statusCode int, errMsg string) {
	t.Helper()
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(getEvents)
	handler.ServeHTTP(rr, req)
	assert.Equal(t, statusCode, rr.Code)
	jsonBytes := make([]byte, 256)
	n, err := rr.Body.Read(jsonBytes)
	assert.NilError(t, err, "cannot read response body")
	var errObject dao.YAPIError
	err = json.Unmarshal(jsonBytes[:n], &errObject)
	assert.NilError(t, err, "cannot unmarshal events dao")
	assert.Assert(t, strings.Contains(errObject.Message, errMsg), "Error message [%s] not found inside the string: [%s]", errMsg, errObject.Message)
}

func checkAllEvents(t *testing.T, events []*si.EventRecord) {
	t.Helper()
	req, err := http.NewRequest("GET", "/ws/v1/events/batch/", strings.NewReader(""))
	assert.NilError(t, err)
	eventDao := getEventRecordDao(t, req)

	for i := 0; i < len(events); i++ {
		compareEvents(t, events[i], eventDao.EventRecords[i])
	}
}

func compareEvents(t *testing.T, event, eventFromDao *si.EventRecord) {
	t.Helper()
	assert.Equal(t, event.TimestampNano, eventFromDao.TimestampNano)
	assert.Equal(t, event.EventChangeType, eventFromDao.EventChangeType)
	assert.Equal(t, event.EventChangeDetail, eventFromDao.EventChangeDetail)
	assert.Equal(t, event.ObjectID, eventFromDao.ObjectID)
	assert.Equal(t, event.ReferenceID, eventFromDao.ReferenceID)
	assert.Equal(t, event.Message, eventFromDao.Message)
	res0 := resources.NewResourceFromProto(event.Resource)
	res1 := resources.NewResourceFromProto(eventFromDao.Resource)
	assert.Assert(t, resources.Equals(res0, res1))
}

func getEventRecordDao(t *testing.T, req *http.Request) dao.EventRecordDAO {
	t.Helper()
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(getEvents)
	handler.ServeHTTP(rr, req)
	jsonBytes := make([]byte, 2048)
	n, err := rr.Body.Read(jsonBytes)
	assert.NilError(t, err, "cannot read response body")
	var eventDao dao.EventRecordDAO
	err = json.Unmarshal(jsonBytes[:n], &eventDao)
	assert.NilError(t, err, "cannot unmarshal events dao")
	return eventDao
}

func prepareSchedulerContext(t *testing.T) *scheduler.ClusterContext {
	config := []byte(configDefault)
	var err error
	schedulerContext, err = scheduler.NewClusterContext(rmID, policyGroup, config)
	assert.NilError(t, err, "Error when load clusterInfo from config")
	assert.Equal(t, 1, len(schedulerContext.GetPartitionMapClone()))

	return schedulerContext
}

func prepareUserAndGroupContext(t *testing.T, config string) {
	clearUserManager()
	part := setup(t, config, 1)

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
	allocInfo := objects.NewAllocation("node-1", ask)
	app.AddAllocation(allocInfo)
	assert.Assert(t, app.IsStarting(), "Application did not return starting state after alloc: %s", app.CurrentState())

	NewWebApp(schedulerContext, nil)
}

func prepareEmptyUserGroupContext() {
	clearUserManager()
	NewWebApp(&scheduler.ClusterContext{}, nil)
}

func clearUserManager() {
	userManager := ugm.GetUserManager()
	userManager.ClearUserTrackers()
	userManager.ClearGroupTrackers()
}

func verifyStateDumpJSON(t *testing.T, aggregated *AggregatedStateInfo) {
	assert.Check(t, aggregated.Timestamp != 0)
	assert.Check(t, len(aggregated.Partitions) > 0)
	assert.Check(t, len(aggregated.Nodes) > 0)
	assert.Check(t, len(aggregated.ClusterInfo) > 0)
	assert.Check(t, len(aggregated.Queues) > 0)
	assert.Check(t, len(aggregated.LogLevel) > 0)
	assert.Check(t, len(aggregated.Config.SchedulerConfig.Partitions) > 0)
	assert.Check(t, len(aggregated.Config.Extra) > 0)
}

func TestCheckHealthStatusNotFound(t *testing.T) {
	NewWebApp(&scheduler.ClusterContext{}, nil)
	req, err := http.NewRequest("GET", "/ws/v1/scheduler/healthcheck", strings.NewReader(""))
	assert.NilError(t, err, "Error while creating the healthcheck request")
	resp := &MockResponseWriter{}
	checkHealthStatus(resp, req)

	var errInfo dao.YAPIError
	err = json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusNotFound, errInfo.StatusCode, statusCodeError)
	assert.Equal(t, "Health check is not available", errInfo.Message, jsonMessageError)
}

func TestCheckHealthStatus(t *testing.T) {
	runHealthCheckTest(t, &dao.SchedulerHealthDAOInfo{
		Healthy: true,
		HealthChecks: []dao.HealthCheckInfo{
			{
				Name:             "Scheduling errors",
				Succeeded:        true,
				Description:      "Check for scheduling error entries in metrics",
				DiagnosisMessage: "There were 0 scheduling errors logged in the metrics",
			},
		},
	})

	runHealthCheckTest(t, &dao.SchedulerHealthDAOInfo{
		Healthy: false,
		HealthChecks: []dao.HealthCheckInfo{
			{
				Name:             "Failed nodes",
				Succeeded:        false,
				Description:      "Check for failed nodes entries in metrics",
				DiagnosisMessage: "There were 1 failed nodes logged in the metrics",
			},
		},
	})
}

func runHealthCheckTest(t *testing.T, expected *dao.SchedulerHealthDAOInfo) {
	schedulerContext := &scheduler.ClusterContext{}
	schedulerContext.SetLastHealthCheckResult(expected)
	NewWebApp(schedulerContext, nil)

	req, err := http.NewRequest("GET", "/ws/v1/scheduler/healthcheck", strings.NewReader(""))
	assert.NilError(t, err, "Error while creating the healthcheck request")
	resp := &MockResponseWriter{}
	checkHealthStatus(resp, req)

	var actual dao.SchedulerHealthDAOInfo
	err = json.Unmarshal(resp.outputBytes, &actual)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, expected.Healthy, actual.Healthy)
	assert.Equal(t, len(expected.HealthChecks), len(actual.HealthChecks))
	for i, expectedHealthCheck := range expected.HealthChecks {
		actualHealthCheck := actual.HealthChecks[i]
		assert.Equal(t, expectedHealthCheck.Name, actualHealthCheck.Name)
		assert.Equal(t, expectedHealthCheck.Succeeded, actualHealthCheck.Succeeded)
		assert.Equal(t, expectedHealthCheck.Description, actualHealthCheck.Description)
		assert.Equal(t, expectedHealthCheck.DiagnosisMessage, actualHealthCheck.DiagnosisMessage)
	}
}

type ResponseRecorderWithDeadline struct {
	*httptest.ResponseRecorder
	setWriteFails   bool
	setWriteFailsAt int
	setWriteCalls   int
	setReadFails    bool
}

func (rrd *ResponseRecorderWithDeadline) SetWriteDeadline(_ time.Time) error {
	rrd.setWriteCalls++
	if rrd.setWriteFails && rrd.setWriteCalls == rrd.setWriteFailsAt {
		return errors.New("SetWriteDeadline failed")
	}
	return nil
}

func (rrd *ResponseRecorderWithDeadline) SetReadDeadline(_ time.Time) error {
	if rrd.setReadFails {
		return errors.New("SetReadDeadline failed")
	}
	return nil
}

func NewResponseRecorderWithDeadline() *ResponseRecorderWithDeadline {
	return &ResponseRecorderWithDeadline{
		ResponseRecorder: httptest.NewRecorder(),
	}
}
