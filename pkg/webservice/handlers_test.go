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
	"net/http"
	"net/http/httptest"
	"net/url"
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
	"github.com/apache/yunikorn-core/pkg/scheduler/placement/types"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	unmarshalError   = "Failed to unmarshal error response from response body"
	statusCodeError  = "Incorrect Status code"
	jsonMessageError = "JSON error message is incorrect"
	httpRequestError = "HTTP request creation failed"
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
          application.sort.policy: fifo
        childtemplate:
          maxapplications: 10
          properties:
            application.sort.policy: fifo
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

const placementRuleConfig = `
partitions:
    - name: default
      placementrules:
      - name: user
      - name: tag
        value: namespace
        create: true
        parent:
          name: fixed
          value: root.namespaces
      - name: fixed
        value: root.default
      queues:
        - name: root
          parent: true
          submitacl: '*'
          queues:
            - name: default
`

const rmID = "rm-123"
const policyGroup = "default-policy-group"
const queueName = "root.default"
const nodeID = "node-1"
const invalidQueueName = "root.parent.test%Zt%23%3Art%3A%2F_ff-test"

var (
	updatedExtraConf = map[string]string{
		"log.level":                  "info",
		"service.schedulingInterval": "1s",
		"admissionController.accessControl.bypassAuth": "false",
	}
)

// setup To take care of setting up config, cluster, partitions etc
func setup(t *testing.T, config string, partitionCount int) *scheduler.PartitionContext {
	ctx, err := scheduler.NewClusterContext(rmID, policyGroup, []byte(config))
	assert.NilError(t, err, "Error when load clusterInfo from config")
	schedulerContext.Store(ctx)

	assert.Equal(t, partitionCount, len(schedulerContext.Load().GetPartitionMapClone()))

	// Check default partition
	partitionName := common.GetNormalizedPartitionName("default", rmID)
	part := schedulerContext.Load().GetPartition(partitionName)
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

func TestGetStackInfo(t *testing.T) {
	// normal case
	req, err := http.NewRequest("GET", "/stack", strings.NewReader(baseConf))
	assert.NilError(t, err, "Error creating request")
	resp := &MockResponseWriter{}
	getStackInfo(resp, req)
	assertIsStackInfo(t, resp.outputBytes)

	// Create a deep call stack (30 calls) and check if the stack trace is larger than 5000 bytes
	// assuming RAM is not less than 5000 bytes
	var deepFunction func(int)
	deepFunction = func(depth int) {
		if depth > 0 {
			deepFunction(depth - 1)
		} else {
			resp = &MockResponseWriter{}
			req, err = http.NewRequest("GET", "/stack", nil)
			assert.NilError(t, err, httpRequestError)
			getStackInfo(resp, req)
			assertIsStackInfo(t, resp.outputBytes)
			assert.Check(t, len(resp.outputBytes) > 5000, "Expected stack trace larger than 5000 bytes")
		}
	}
	deepFunction(30)
}

// assert stack trace
func assertIsStackInfo(t *testing.T, outputBytes []byte) {
	assert.Assert(t, strings.Contains(string(outputBytes), "goroutine"), "Stack trace should be present in the response")
	assert.Assert(t, strings.Contains(string(outputBytes), "test"), "Stack trace should be present in the response")
	assert.Assert(t, strings.Contains(string(outputBytes), "github.com/apache/yunikorn-core/pkg/webservice.getStackInfo"), "Stack trace should be present in the response")
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
	ctx, err := scheduler.NewClusterContext(rmID, policyGroup, []byte(startConf))
	assert.NilError(t, err, "Error when load clusterInfo from config")
	schedulerContext.Store(ctx)
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
	err = schedulerContext.Load().UpdateRMSchedulerConfig(rmID, []byte(updatedConf))
	assert.NilError(t, err, "Error when updating clusterInfo from config")
	configs.SetConfigMap(updatedExtraConf)

	// check that we return yaml by default, unmarshal will error when we don't
	req.Header.Set("Accept", "unknown")
	resp = &MockResponseWriter{}
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
	err = schedulerContext.Load().UpdateRMSchedulerConfig(rmID, []byte(updatedConf))
	assert.NilError(t, err, "Error when updating clusterInfo from config")
	configs.SetConfigMap(updatedExtraConf)

	resp = &MockResponseWriter{}
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
	schedulerContext.Load().SetRMInfo(rmID, buildInfoMap)
	rmBuildInformationMaps := getRMBuildInformation(nil)
	assert.Equal(t, 0, len(rmBuildInformationMaps))
	rmInfo := schedulerContext.Load().GetRMInfoMapClone()
	assert.Equal(t, 1, len(rmInfo))
	rmBuildInformationMaps = getRMBuildInformation(rmInfo)
	assert.Equal(t, 1, len(rmBuildInformationMaps))
	assert.Equal(t, rmBuildInformationMaps[0]["buildDate"], buildInfoMap["buildDate"])
	assert.Equal(t, rmBuildInformationMaps[0]["buildVersion"], buildInfoMap["buildVersion"])
	assert.Equal(t, rmBuildInformationMaps[0]["isPluginVersion"], buildInfoMap["isPluginVersion"])
	assert.Equal(t, rmBuildInformationMaps[0]["rmId"], rmID)

	// Check test partitions
	partitionName := common.GetNormalizedPartitionName("default", rmID)
	partition := schedulerContext.Load().GetPartition(partitionName)
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
	alloc1 := newAlloc("alloc-1", appID, nodeID, resAlloc1)
	alloc2 := newAlloc("alloc-2", appID, nodeID, resAlloc2)
	err = partition.AddNode(node1)
	assert.NilError(t, err, "add node to partition should not have failed")
	_, allocAdded, err := partition.UpdateAllocation(alloc1)
	assert.NilError(t, err, "failed to add alloc1")
	assert.Check(t, allocAdded)
	_, allocAdded, err = partition.UpdateAllocation(alloc2)
	assert.NilError(t, err, "failed to add alloc2")
	assert.Check(t, allocAdded)

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
	app := newApplication("app1", partition.Name, queueName, rmID, security.UserGroup{})
	err := partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	// create test nodes
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 1000, siCommon.CPU: 1000}).ToProto()
	node1 := objects.NewNode(&si.NodeInfo{NodeID: "node-1", SchedulableResource: nodeRes})
	nodeRes2 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 1000, siCommon.CPU: 1000, "GPU": 10}).ToProto()
	node2 := objects.NewNode(&si.NodeInfo{NodeID: "node-2", SchedulableResource: nodeRes2})
	nodeCPU := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.CPU: 1000}).ToProto()
	node3 := objects.NewNode(&si.NodeInfo{NodeID: "node-3", SchedulableResource: nodeCPU})

	// create test allocations
	resAlloc1 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 500, siCommon.CPU: 300})
	resAlloc2 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 300, siCommon.CPU: 500, "GPU": 5})
	alloc1 := newAlloc("alloc-1", app.ApplicationID, node1.NodeID, resAlloc1)
	allocs := []*objects.Allocation{alloc1}
	err = partition.AddNode(node1)
	assert.NilError(t, err, "add node to partition should not have failed")
	_, allocCreated, err := partition.UpdateAllocation(allocs[0])
	assert.NilError(t, err, "add alloc-1 should not have failed")
	assert.Check(t, allocCreated)
	alloc2 := newAlloc("alloc-2", app.ApplicationID, node2.NodeID, resAlloc2)
	allocs = []*objects.Allocation{alloc2}
	err = partition.AddNode(node2)
	assert.NilError(t, err, "add node to partition should not have failed")
	_, allocCreated, err = partition.UpdateAllocation(allocs[0])
	assert.NilError(t, err, "add alloc-2 should not have failed")
	assert.Check(t, allocCreated)
	err = partition.AddNode(node3)
	assert.NilError(t, err, "add node to partition should not have failed")

	// two nodes advertise memory: must show up in the list
	result := getNodesUtilJSON(partition, siCommon.Memory)
	subResult := result.NodesUtil
	assert.Equal(t, result.ResourceType, siCommon.Memory)
	assert.Equal(t, subResult[2].NumOfNodes, int64(1))
	assert.Equal(t, subResult[4].NumOfNodes, int64(1))
	assert.Equal(t, subResult[2].NodeNames[0], node2.NodeID)
	assert.Equal(t, subResult[4].NodeNames[0], node1.NodeID)

	// three nodes advertise cpu: must show up in the list
	result = getNodesUtilJSON(partition, siCommon.CPU)
	subResult = result.NodesUtil
	assert.Equal(t, result.ResourceType, siCommon.CPU)
	assert.Equal(t, subResult[0].NumOfNodes, int64(1))
	assert.Equal(t, subResult[0].NodeNames[0], node3.NodeID)
	assert.Equal(t, subResult[2].NumOfNodes, int64(1))
	assert.Equal(t, subResult[2].NodeNames[0], node1.NodeID)
	assert.Equal(t, subResult[4].NumOfNodes, int64(1))
	assert.Equal(t, subResult[4].NodeNames[0], node2.NodeID)

	// one node advertise GPU: must show up in the list
	result = getNodesUtilJSON(partition, "GPU")
	subResult = result.NodesUtil
	assert.Equal(t, result.ResourceType, "GPU")
	assert.Equal(t, subResult[4].NumOfNodes, int64(1))
	assert.Equal(t, subResult[4].NodeNames[0], node2.NodeID)

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
	node1 := addNode(t, partition, "node-1", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10}))
	node2 := addNode(t, partition, "node-2", resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10, "second": 5}))

	// get nodes utilization
	resp = &MockResponseWriter{}
	getNodeUtilisation(resp, req)
	utilisation = &dao.NodesUtilDAOInfo{}
	err = json.Unmarshal(resp.outputBytes, utilisation)
	assert.NilError(t, err, "getNodeUtilisation should have returned an object")
	assert.Equal(t, utilisation.ResourceType, "", "unexpected type returned")
	assert.Equal(t, len(utilisation.NodesUtil), 10, "empty usage: unexpected bucket count returned")
	assert.Assert(t, confirmNodeCount(utilisation.NodesUtil, 0), "unexpected number of nodes returned should be 0")

	resAlloc := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	alloc := newAlloc("alloc-1", "app", node1.NodeID, resAlloc)
	assert.Assert(t, node1.TryAddAllocation(alloc), "unexpected failure adding allocation to node")
	rootQ := partition.GetQueue("root")
	err = rootQ.TryIncAllocatedResource(resAlloc)
	assert.NilError(t, err, "unexpected error returned setting allocated resource on queue")
	// get nodes utilization
	resp = &MockResponseWriter{}
	getNodeUtilisation(resp, req)
	utilisation = &dao.NodesUtilDAOInfo{}
	err = json.Unmarshal(resp.outputBytes, utilisation)
	assert.NilError(t, err, "getNodeUtilisation should have returned an object")
	assert.Equal(t, utilisation.ResourceType, "first", "expected first as type returned")
	assert.Equal(t, len(utilisation.NodesUtil), 10, "empty usage: unexpected bucket count returned")
	assert.Assert(t, confirmNodeCount(utilisation.NodesUtil, 2), "unexpected number of nodes returned should be 2")

	// make second type dominant by using all
	resAlloc = resources.NewResourceFromMap(map[string]resources.Quantity{"second": 5})
	alloc = newAlloc("alloc-2", "app", node2.NodeID, resAlloc)
	assert.Assert(t, node2.TryAddAllocation(alloc), "unexpected failure adding allocation to node")
	err = rootQ.TryIncAllocatedResource(resAlloc)
	assert.NilError(t, err, "unexpected error returned setting allocated resource on queue")
	// get nodes utilization
	resp = &MockResponseWriter{}
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
	err := partition.AddNode(node)
	assert.NilError(t, err, "adding node to partition should not fail")
	return node
}

func addAllocatedResource(t *testing.T, node *objects.Node, allocationKey string, appID string, quantityMap map[string]resources.Quantity) {
	t.Helper()
	resAlloc := resources.NewResourceFromMap(quantityMap)
	alloc := newAlloc(allocationKey, appID, node.NodeID, resAlloc)
	assert.Assert(t, node.TryAddAllocation(alloc), "unexpected failure adding allocation to node")
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

	// create test nodes
	node1 := addNode(t, partition, "node-1", resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 1000, siCommon.CPU: 1000}))
	node2 := addNode(t, partition, "node-2", resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 1000, siCommon.CPU: 1000, "GPU": 10}))
	node3 := addNode(t, partition, "node-3", resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.CPU: 1000}))

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
	assert.Equal(t, memoryNodesUtil.NodesUtil[2].NodeNames[0], node2.NodeID)
	assert.Equal(t, memoryNodesUtil.NodesUtil[4].NodeNames[0], node1.NodeID)

	// three nodes advertise cpu: must show up in the list
	cpuNodesUtil := getNodesUtilByType(t, result.NodesUtilList, siCommon.CPU)
	assert.Equal(t, cpuNodesUtil.NodesUtil[0].NumOfNodes, int64(1))
	assert.Equal(t, cpuNodesUtil.NodesUtil[0].NodeNames[0], node3.NodeID)
	assert.Equal(t, cpuNodesUtil.NodesUtil[2].NumOfNodes, int64(1))
	assert.Equal(t, cpuNodesUtil.NodesUtil[2].NodeNames[0], node1.NodeID)
	assert.Equal(t, cpuNodesUtil.NodesUtil[4].NumOfNodes, int64(1))
	assert.Equal(t, cpuNodesUtil.NodesUtil[4].NodeNames[0], node2.NodeID)

	// one node advertise GPU: must show up in the list
	gpuNodesUtil := getNodesUtilByType(t, result.NodesUtilList, "GPU")
	assert.Equal(t, gpuNodesUtil.NodesUtil[4].NumOfNodes, int64(1))
	assert.Equal(t, gpuNodesUtil.NodesUtil[4].NodeNames[0], node2.NodeID)
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
	ctx, err := scheduler.NewClusterContext(rmID, policyGroup, []byte(configMultiPartitions))
	assert.NilError(t, err, "Error when load clusterInfo from config")
	schedulerContext.Store(ctx)
	assert.NilError(t, err, "Error when load clusterInfo from config")
	schedulerContext.Load().GetPartition("default")
	defaultPartition := schedulerContext.Load().GetPartition(common.GetNormalizedPartitionName("default", rmID))
	gpuPartition := schedulerContext.Load().GetPartition(common.GetNormalizedPartitionName("gpu", rmID))

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
	resp = &MockResponseWriter{}
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

func TestPartitions(t *testing.T) { //nolint:funlen
	schedulerContext.Store(&scheduler.ClusterContext{})

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

	// add a new app2 - running
	app2 := addAndConfirmApplicationExists(t, partitionName, defaultPartition, "app-2")
	app2.SetState(objects.Running.String())

	// add a new app3 - completing
	app3 := addAndConfirmApplicationExists(t, partitionName, defaultPartition, "app-3")
	app3.SetState(objects.Completing.String())

	// add a new app4 - rejected
	app4 := addAndConfirmApplicationExists(t, partitionName, defaultPartition, "app-4")
	app4.SetState(objects.Rejected.String())

	// add a new app5 - completed
	app5 := addAndConfirmApplicationExists(t, partitionName, defaultPartition, "app-5")
	app5.SetState(objects.Completed.String())

	// add a new app7 - failed
	app6 := addAndConfirmApplicationExists(t, partitionName, defaultPartition, "app-6")
	app6.SetState(objects.Failed.String())

	NewWebApp(schedulerContext.Load(), nil)

	// create test nodes
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 500, siCommon.CPU: 500}).ToProto()
	node1ID := "node-1"
	node1 := objects.NewNode(&si.NodeInfo{NodeID: node1ID, SchedulableResource: nodeRes})
	node2ID := "node-2"
	node2 := objects.NewNode(&si.NodeInfo{NodeID: node2ID, SchedulableResource: nodeRes})

	// create test allocations
	resAlloc1 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 100, siCommon.CPU: 400})
	resAlloc2 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 200, siCommon.CPU: 300})
	alloc1 := newAlloc("alloc-1", app5.ApplicationID, node1ID, resAlloc1)
	allocs := []*objects.Allocation{alloc1}
	err = defaultPartition.AddNode(node1)
	assert.NilError(t, err, "add node to partition should not have failed")
	_, allocCreated, err := defaultPartition.UpdateAllocation(allocs[0])
	assert.NilError(t, err, "add alloc-1 should not have failed")
	assert.Check(t, allocCreated)
	alloc2 := newAlloc("alloc-2", app2.ApplicationID, node1ID, resAlloc2)
	allocs = []*objects.Allocation{alloc2}
	err = defaultPartition.AddNode(node2)
	assert.NilError(t, err, "add node to partition should not have failed")
	_, allocCreated, err = defaultPartition.UpdateAllocation(allocs[0])
	assert.NilError(t, err, "add alloc-2 should not have failed")
	assert.Check(t, allocCreated)

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
	assert.Equal(t, cs["default"].Applications["total"], 7)
	assert.Equal(t, cs["default"].Applications[objects.New.String()], 1)
	assert.Equal(t, cs["default"].Applications[objects.Accepted.String()], 1)
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

//nolint:funlen
func TestGetPartitionQueuesHandler(t *testing.T) {
	setup(t, configTwoLevelQueues, 2)

	NewWebApp(schedulerContext.Load(), nil)

	tMaxResource, err := resources.NewResourceFromConf(map[string]string{"memory": "600000"})
	assert.NilError(t, err)
	tGuaranteedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "400000"})
	assert.NilError(t, err)

	templateInfo := dao.TemplateInfo{
		MaxApplications:    10,
		MaxResource:        tMaxResource.DAOMap(),
		GuaranteedResource: tGuaranteedResource.DAOMap(),
		Properties: map[string]string{
			configs.ApplicationSortPolicy: policies.FifoSortPolicy.String(),
		},
	}

	maxResource, err := resources.NewResourceFromConf(map[string]string{"memory": "800000", "vcore": "80000"})
	assert.NilError(t, err)
	guaranteedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "500000", "vcore": "50000"})
	assert.NilError(t, err)

	var req *http.Request
	req, err = createRequest(t, "/ws/v1/partition/default/queues", map[string]string{"partition": partitionNameWithoutClusterID})
	assert.NilError(t, err, "Get Queues for PartitionQueues Handler request failed")
	resp := &MockResponseWriter{}
	var partitionQueuesDao dao.PartitionQueueDAOInfo
	getPartitionQueues(resp, req)
	err = json.Unmarshal(resp.outputBytes, &partitionQueuesDao)
	assert.NilError(t, err, unmarshalError)
	// assert root fields
	assertPartitionQueueDaoInfo(t, &partitionQueuesDao, configs.RootQueue, configs.DefaultPartition, nil, nil, false, true, "", &templateInfo)

	// assert child root.a fields
	assert.Equal(t, len(partitionQueuesDao.Children), 1)
	child := &partitionQueuesDao.Children[0]
	assertPartitionQueueDaoInfo(t, child, "root.a", "", maxResource.DAOMap(), guaranteedResource.DAOMap(), false, true, "root", &templateInfo)

	// assert child root.a.a1 fields
	assert.Equal(t, len(partitionQueuesDao.Children[0].Children), 1)
	child = &partitionQueuesDao.Children[0].Children[0]
	assertPartitionQueueDaoInfo(t, child, "root.a.a1", "", maxResource.DAOMap(), guaranteedResource.DAOMap(), true, true, "root.a", nil)

	// test partition not exists
	req, err = createRequest(t, "/ws/v1/partition/default/queues", map[string]string{"partition": "notexists"})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getPartitionQueues(resp, req)
	assertPartitionNotExists(t, resp)

	// test params name missing
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/queues", strings.NewReader(""))
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getPartitionQueues(resp, req)
	assertParamsMissing(t, resp)
}

func assertPartitionQueueDaoInfo(t *testing.T, partitionQueueDAOInfo *dao.PartitionQueueDAOInfo, queueName string, partition string, maxResource map[string]int64, gResource map[string]int64, leaf bool, isManaged bool, parent string, templateInfo *dao.TemplateInfo) {
	assert.Equal(t, partitionQueueDAOInfo.QueueName, queueName)
	assert.Equal(t, partitionQueueDAOInfo.Status, objects.Active.String())
	assert.Equal(t, partitionQueueDAOInfo.Partition, partition)
	assert.Assert(t, partitionQueueDAOInfo.PendingResource == nil)
	assert.DeepEqual(t, partitionQueueDAOInfo.MaxResource, maxResource)
	assert.DeepEqual(t, partitionQueueDAOInfo.GuaranteedResource, gResource)
	assert.Assert(t, partitionQueueDAOInfo.AllocatedResource == nil)
	assert.Assert(t, partitionQueueDAOInfo.PreemptingResource == nil)
	assert.DeepEqual(t, partitionQueueDAOInfo.HeadRoom, maxResource)
	assert.Equal(t, partitionQueueDAOInfo.IsLeaf, leaf)
	assert.Equal(t, partitionQueueDAOInfo.IsManaged, isManaged)
	assert.Equal(t, partitionQueueDAOInfo.Parent, parent)
	assert.Assert(t, partitionQueueDAOInfo.AbsUsedCapacity == nil)
	assert.Equal(t, partitionQueueDAOInfo.MaxRunningApps, uint64(0))
	assert.Equal(t, partitionQueueDAOInfo.RunningApps, uint64(0))
	assert.Equal(t, partitionQueueDAOInfo.CurrentPriority, configs.MinPriority)
	assert.Assert(t, partitionQueueDAOInfo.AllocatingAcceptedApps == nil)
	assert.Equal(t, len(partitionQueueDAOInfo.Properties), 1)
	assert.Equal(t, partitionQueueDAOInfo.Properties[configs.ApplicationSortPolicy], policies.FifoSortPolicy.String())
	assert.DeepEqual(t, partitionQueueDAOInfo.TemplateInfo, templateInfo)
}

func TestGetPartitionQueueHandler(t *testing.T) {
	partitionQueuesHandler := "/ws/v1/partition/default/queue/"
	queueA := "root.a"
	setup(t, configTwoLevelQueues, 2)

	NewWebApp(schedulerContext.Load(), nil)

	// test specific queue
	var partitionQueueDao1 dao.PartitionQueueDAOInfo
	req, err := createRequest(t, partitionQueuesHandler+queueA, map[string]string{"partition": "default", "queue": "root.a"})
	assert.NilError(t, err, "HTTP request create failed")
	resp := &MockResponseWriter{}
	getPartitionQueue(resp, req)
	err = json.Unmarshal(resp.outputBytes, &partitionQueueDao1)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, partitionQueueDao1.QueueName, "root.a")
	assert.Equal(t, len(partitionQueueDao1.Children), 0)
	assert.Equal(t, len(partitionQueueDao1.ChildNames), 1)
	assert.Equal(t, partitionQueueDao1.ChildNames[0], "root.a.a1")

	// test hierarchy queue
	var partitionQueueDao2 dao.PartitionQueueDAOInfo
	req, err = createRequest(t, partitionQueuesHandler+"root.a?subtree", map[string]string{"partition": "default", "queue": "root.a"})
	assert.NilError(t, err, "HTTP request create failed")
	resp = &MockResponseWriter{}
	getPartitionQueue(resp, req)
	err = json.Unmarshal(resp.outputBytes, &partitionQueueDao2)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, partitionQueueDao2.QueueName, "root.a")
	assert.Equal(t, len(partitionQueueDao2.Children), 1)
	assert.Equal(t, len(partitionQueueDao2.ChildNames), 1)
	assert.Equal(t, partitionQueueDao2.Children[0].QueueName, "root.a.a1")
	assert.Equal(t, partitionQueueDao2.ChildNames[0], "root.a.a1")

	// test partition not exists
	req, err = createRequest(t, partitionQueuesHandler+queueA, map[string]string{"partition": "notexists"})
	assert.NilError(t, err, "HTTP request create failed")
	resp = &MockResponseWriter{}
	getPartitionQueue(resp, req)
	assertPartitionNotExists(t, resp)

	// test params name missing
	req, err = http.NewRequest("GET", partitionQueuesHandler+queueA, strings.NewReader(""))
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getPartitionQueue(resp, req)
	assertParamsMissing(t, resp)

	// test invalid queue name
	req, err = createRequest(t, partitionQueuesHandler+queueA, map[string]string{"partition": "default", "queue": "root.notexists!"})
	assert.NilError(t, err, "HTTP request create failed")
	resp = &MockResponseWriter{}
	getPartitionQueue(resp, req)
	assertQueueInvalid(t, resp, "root.notexists!", "notexists!")

	// test queue is not exists
	req, err = createRequest(t, partitionQueuesHandler+queueA, map[string]string{"partition": "default", "queue": "notexists"})
	assert.NilError(t, err, "HTTP request create failed")
	resp = &MockResponseWriter{}
	getPartitionQueue(resp, req)
	assertQueueNotExists(t, resp)

	// test queue name with special characters escaped properly
	queueName := url.QueryEscape("root.parent.test@t#:rt:/_ff-test")
	req, err = createRequest(t, partitionQueuesHandler+queueName, map[string]string{"partition": partitionNameWithoutClusterID, "queue": queueName})
	assert.NilError(t, err, "HTTP request create failed")
	resp = &MockResponseWriter{}
	getPartitionQueue(resp, req)
	assertQueueNotExists(t, resp)

	// test queue name with special characters escaped not properly, catch error at request level
	_, err = http.NewRequest("GET", partitionQueuesHandler+invalidQueueName, strings.NewReader(""))
	assert.ErrorContains(t, err, "invalid URL escape")

	// test queue name with special characters escaped not properly, catch error while un escaping queue name
	req, err = createRequest(t, partitionQueuesHandler+queueA, map[string]string{"partition": partitionNameWithoutClusterID, "queue": invalidQueueName})
	assert.NilError(t, err, "HTTP request create failed")
	resp = &MockResponseWriter{}
	getPartitionQueue(resp, req)
	var errInfo dao.YAPIError
	err = json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, "invalid URL escape \"%Zt\"", jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func TestGetClusterInfo(t *testing.T) {
	schedulerContext.Store(&scheduler.ClusterContext{})
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
	alloc1 := newAlloc("alloc-1", appID, node1ID, resAlloc1)
	allocs := []*objects.Allocation{alloc1}
	err = partition.AddNode(node1)
	assert.NilError(t, err, "add node to partition should not have failed")
	_, allocCreated, err := partition.UpdateAllocation(allocs[0])
	assert.NilError(t, err, "add alloc-1 should not have failed")
	assert.Check(t, allocCreated)

	alloc2 := newAlloc("alloc-2", appID, node2ID, resAlloc2)
	allocs = []*objects.Allocation{alloc2}
	err = partition.AddNode(node2)
	assert.NilError(t, err, "add node to partition should not have failed")
	_, allocCreated, err = partition.UpdateAllocation(allocs[0])
	assert.NilError(t, err, "add alloc-2 should not have failed")
	assert.Check(t, allocCreated)

	NewWebApp(schedulerContext.Load(), nil)

	var req *http.Request
	req, err = createRequest(t, "/ws/v1/partition/default/nodes", map[string]string{"partition": partitionNameWithoutClusterID})
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
			assertNodeInfo(t, node, node1ID, "alloc-1", attributesOfnode1, map[string]int64{"memory": 50, "vcore": 30})
		} else {
			assertNodeInfo(t, node, node2ID, "alloc-2", attributesOfnode2, map[string]int64{"memory": 30, "vcore": 50})
		}
	}

	req, err = createRequest(t, "/ws/v1/partition/default/nodes", map[string]string{"partition": "notexists"})
	assert.NilError(t, err, "Get Nodes for PartitionNodes Handler request failed")
	resp = &MockResponseWriter{}
	getPartitionNodes(resp, req)
	assertPartitionNotExists(t, resp)

	// test params name missing
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/nodes", strings.NewReader(""))
	assert.NilError(t, err, "Get Nodes for PartitionNodes Handler request failed")
	resp = &MockResponseWriter{}
	getPartitionNodes(resp, req)
	assertParamsMissing(t, resp)
}

func TestGetPartitionNode(t *testing.T) {
	partition := setup(t, configDefault, 1)

	// create test application
	appID := "app-1"
	app := newApplication(appID, partition.Name, queueName, rmID, security.UserGroup{User: "testuser", Groups: []string{"testgroup"}})
	err := partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	// create test nodes
	attributesOfnode1 := map[string]string{"Disk": "SSD"}
	attributesOfnode2 := map[string]string{"Devices": "camera"}
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 1000, siCommon.CPU: 1000}).ToProto()
	node1ID := "node_1"
	node1 := objects.NewNode(&si.NodeInfo{NodeID: node1ID, Attributes: attributesOfnode1, SchedulableResource: nodeRes})
	node2ID := "node_2"
	node2 := objects.NewNode(&si.NodeInfo{NodeID: node2ID, Attributes: attributesOfnode2, SchedulableResource: nodeRes})

	// create test allocations
	resAlloc1 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 500, siCommon.CPU: 300})
	resAlloc2 := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 300, siCommon.CPU: 500})
	alloc1 := newAlloc("alloc-1", appID, node1ID, resAlloc1)
	err = partition.AddNode(node1)
	assert.NilError(t, err, "add node to partition should not have failed")
	_, allocCreated, err := partition.UpdateAllocation(alloc1)
	assert.NilError(t, err, "add alloc-1 should not have failed")
	assert.Check(t, allocCreated)
	falloc1 := newForeignAlloc("foreign-1", "", node1ID, resAlloc1, siCommon.AllocTypeDefault, 0)
	_, allocCreated, err = partition.UpdateAllocation(falloc1)
	assert.NilError(t, err, "add falloc-1 should not have failed")
	assert.Check(t, allocCreated)
	falloc2 := newForeignAlloc("foreign-2", "", node1ID, resAlloc2, siCommon.AllocTypeStatic, 123)
	_, allocCreated, err = partition.UpdateAllocation(falloc2)
	assert.NilError(t, err, "add falloc-2 should not have failed")
	assert.Check(t, allocCreated)

	alloc2 := newAlloc("alloc-2", appID, node2ID, resAlloc2)
	err = partition.AddNode(node2)
	assert.NilError(t, err, "add node to partition should not have failed")
	_, allocCreated, err = partition.UpdateAllocation(alloc2)
	assert.NilError(t, err, "add alloc-2 should not have failed")
	assert.Check(t, allocCreated)

	NewWebApp(schedulerContext.Load(), nil)

	var req *http.Request
	// Test specific node
	req, err = createRequest(t, "/ws/v1/partition/default/node/node_1", map[string]string{"partition": "default", "node": "node_1"})
	assert.NilError(t, err, "Get Node for PartitionNode Handler request failed")
	resp := &MockResponseWriter{}
	getPartitionNode(resp, req)
	var nodeInfo dao.NodeDAOInfo
	err = json.Unmarshal(resp.outputBytes, &nodeInfo)
	assert.NilError(t, err, unmarshalError)
	assertNodeInfo(t, &nodeInfo, node1ID, "alloc-1", attributesOfnode1, map[string]int64{"memory": 50, "vcore": 30})
	assert.Equal(t, 2, len(nodeInfo.ForeignAllocations))
	if nodeInfo.ForeignAllocations[0].AllocationKey == "foreign-1" {
		assertForeignAllocation(t, "foreign-1", "0", node1ID, resAlloc1, true, nodeInfo.ForeignAllocations[0])
		assertForeignAllocation(t, "foreign-2", "123", node1ID, resAlloc2, false, nodeInfo.ForeignAllocations[1])
	} else {
		assertForeignAllocation(t, "foreign-1", "0", node1ID, resAlloc1, true, nodeInfo.ForeignAllocations[1])
		assertForeignAllocation(t, "foreign-2", "123", node1ID, resAlloc2, false, nodeInfo.ForeignAllocations[0])
	}

	// Test node id is missing
	req, err = createRequest(t, "/ws/v1/partition/default/node/node_1", map[string]string{"partition": "default", "node": ""})
	assert.NilError(t, err, "Get Node for PartitionNode Handler request failed")
	resp = &MockResponseWriter{}
	getPartitionNode(resp, req)
	assertNodeIDNotExists(t, resp)

	// Test param missing
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/node", strings.NewReader(""))
	assert.NilError(t, err, "Get Node for PartitionNode Handler request failed")
	resp = &MockResponseWriter{}
	getPartitionNode(resp, req)
	assertParamsMissing(t, resp)

	// Test partition does not exist
	req, err = createRequest(t, "/ws/v1/partition/notexists/node/node_1", map[string]string{"partition": "notexists"})
	assert.NilError(t, err, "Get Nodes for PartitionNode Handler request failed")
	resp = &MockResponseWriter{}
	getPartitionNodes(resp, req)
	assertPartitionNotExists(t, resp)
}

func assertNodeInfo(t *testing.T, node *dao.NodeDAOInfo, expectedID string, expectedAllocationKey string, expectedAttibute map[string]string, expectedUtilized map[string]int64) {
	assert.Equal(t, expectedID, node.NodeID)
	assert.Equal(t, expectedAllocationKey, node.Allocations[0].AllocationKey)
	assert.DeepEqual(t, expectedAttibute, node.Attributes)
	assert.DeepEqual(t, expectedUtilized, node.Utilized)
}

func assertForeignAllocation(t *testing.T, key, priority, nodeID string, expectedRes *resources.Resource, preemptable bool, info *dao.ForeignAllocationDAOInfo) {
	t.Helper()
	assert.Equal(t, key, info.AllocationKey)
	assert.Equal(t, priority, info.Priority)
	assert.Equal(t, nodeID, info.NodeID)
	resMap := make(map[string]resources.Quantity)
	for k, v := range info.ResourcePerAlloc {
		resMap[k] = resources.Quantity(v)
	}
	resFromInfo := resources.NewResourceFromMap(resMap)
	assert.Assert(t, resources.Equals(resFromInfo, expectedRes))
	assert.Equal(t, preemptable, info.Preemptable)
	assert.Equal(t, 1, len(info.AllocationTags))
	if info.AllocationKey == "foreign-1" {
		assert.Equal(t, siCommon.AllocTypeDefault, info.AllocationTags[siCommon.Foreign])
	} else {
		assert.Equal(t, siCommon.AllocTypeStatic, info.AllocationTags[siCommon.Foreign])
	}
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
		err = common.WaitForCondition(10*time.Millisecond, time.Second, func() bool {
			newCount := len(part.GetCompletedApplications())
			return newCount == currentCount+1
		})
		assert.NilError(t, err, "the completed application should have been processed")
	}
	return app
}

func TestGetQueueApplicationsHandler(t *testing.T) {
	handlerURL := "/ws/v1/partition/default/queue/"
	handlerSuffix := "/applications"
	defaultQueue := "root.default"
	part := setup(t, configDefault, 1)

	// add an application
	app := addApp(t, "app-1", part, defaultQueue, false)

	// add placeholder to test PlaceholderDAOInfo
	tg := "tg-1"
	res := &si.Resource{
		Resources: map[string]*si.Quantity{"vcore": {Value: 1}},
	}
	ask := objects.NewAllocationFromSI(&si.Allocation{
		ApplicationID:    "app-1",
		PartitionName:    part.Name,
		TaskGroupName:    tg,
		ResourcePerAlloc: res,
		Placeholder:      true})
	err := app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")
	app.SetTimedOutPlaceholder(tg, 1)

	NewWebApp(schedulerContext.Load(), nil)

	var req *http.Request
	req, err = createRequest(t, handlerURL+defaultQueue+handlerSuffix, map[string]string{"partition": partitionNameWithoutClusterID, "queue": defaultQueue})
	assert.NilError(t, err)
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
	req1, err = createRequest(t, handlerURL+defaultQueue+handlerSuffix, map[string]string{"partition": "notexists", "queue": defaultQueue})
	assert.NilError(t, err)
	resp1 := &MockResponseWriter{}
	getQueueApplications(resp1, req1)
	assertPartitionNotExists(t, resp1)

	// test nonexistent queue
	var req2 *http.Request
	req2, err = createRequest(t, handlerURL+defaultQueue+handlerSuffix, map[string]string{"partition": partitionNameWithoutClusterID, "queue": "notexists"})
	assert.NilError(t, err)
	resp2 := &MockResponseWriter{}
	getQueueApplications(resp2, req2)
	assertQueueNotExists(t, resp2)

	// test queue without applications
	var req3 *http.Request
	req3, err = createRequest(t, handlerURL+"root.noapps"+handlerSuffix, map[string]string{"partition": partitionNameWithoutClusterID, "queue": "root.noapps"})
	assert.NilError(t, err)
	resp3 := &MockResponseWriter{}
	var appsDao3 []*dao.ApplicationDAOInfo
	getQueueApplications(resp3, req3)
	err = json.Unmarshal(resp3.outputBytes, &appsDao3)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, len(appsDao3), 0)

	// test missing params name
	req, err = createRequest(t, handlerURL+queueName+handlerSuffix, map[string]string{})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getQueueApplications(resp, req)
	assertParamsMissing(t, resp)

	// test queue name with special characters escaped properly
	queueName := url.QueryEscape("root.parent.test@t#:rt:/_ff-test")
	req, err = createRequest(t, handlerURL+queueName+handlerSuffix, map[string]string{"partition": partitionNameWithoutClusterID, "queue": queueName})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getQueueApplications(resp, req)
	assertQueueNotExists(t, resp)

	// test queue name with special characters escaped not properly, catch error at request level
	_, err = http.NewRequest("GET", handlerURL+invalidQueueName+handlerSuffix, strings.NewReader(""))
	assert.ErrorContains(t, err, "invalid URL escape")

	// test queue name with special characters escaped not properly, catch error while un escaping queue name
	req, err = createRequest(t, handlerURL+defaultQueue+handlerSuffix, map[string]string{"partition": partitionNameWithoutClusterID, "queue": invalidQueueName})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getQueueApplications(resp, req)
	var errInfo dao.YAPIError
	err = json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, "invalid URL escape \"%Zt\"", jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func createRequest(t *testing.T, url string, paramsMap map[string]string) (*http.Request, error) {
	var err error
	var req *http.Request
	req, err = http.NewRequest("GET", url, strings.NewReader(""))
	assert.NilError(t, err, "Handler request create failed")
	var params httprouter.Params
	for k, v := range paramsMap {
		param := httprouter.Param{Key: k, Value: v}
		params = append(params, param)
	}
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, params))
	return req, err
}

func checkLegalGetAppsRequest(t *testing.T, url string, params httprouter.Params, expected []*dao.ApplicationDAOInfo) {
	req, err := http.NewRequest("GET", url, strings.NewReader(""))
	assert.NilError(t, err, "HTTP request create failed")
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
	assert.NilError(t, err, "HTTP request create failed")
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, params))
	assert.NilError(t, err)
	resp := &MockResponseWriter{}
	getPartitionApplicationsByState(resp, req)
	assertFunc(t, resp)
}

func TestGetPartitionApplicationsByStateHandler(t *testing.T) {
	defaultPartition := setup(t, configDefault, 1)
	NewWebApp(schedulerContext.Load(), nil)

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
	sum1 := app1.GetApplicationSummary(defaultPartition.RmID)
	sum2 := app2.GetApplicationSummary(defaultPartition.RmID)
	expectedActiveDao := []*dao.ApplicationDAOInfo{getApplicationDAO(app1, sum1), getApplicationDAO(app2, sum2)}
	checkLegalGetAppsRequest(t, "/ws/v1/partition/default/applications/Active", httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "state", Value: "Active"}}, expectedActiveDao)

	// test get active app with running state
	expectedRunningDao := []*dao.ApplicationDAOInfo{getApplicationDAO(app2, sum2)}
	checkLegalGetAppsRequest(t, "/ws/v1/partition/default/applications/Active?status=Running", httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "state", Value: "Active"}}, expectedRunningDao)

	// test get completed app
	sum3 := app3.GetApplicationSummary(defaultPartition.RmID)
	expectedCompletedDao := []*dao.ApplicationDAOInfo{getApplicationDAO(app3, sum3)}
	checkLegalGetAppsRequest(t, "/ws/v1/partition/default/applications/Completed", httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "state", Value: "Completed"}}, expectedCompletedDao)

	// test get rejected app
	sum4 := app4.GetApplicationSummary(defaultPartition.RmID)
	expectedRejectedDao := []*dao.ApplicationDAOInfo{getApplicationDAO(app4, sum4)}
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
		httprouter.Param{Key: "state", Value: "Accepted"}}, assertAppStateNotAllow)

	// test disallow active state
	checkIllegalGetAppsRequest(t, "/ws/v1/partition/default/applications/Active?status=invalid", httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "state", Value: "Active"}}, assertActiveStateNotAllow)

	// test missing params name
	checkIllegalGetAppsRequest(t, "/ws/v1/partition/default/applications/Active", nil, assertParamsMissing)
}

func checkGetQueueAppByState(t *testing.T, partition, queue, state, status string, expectedApp []*objects.Application) {
	var url string
	if status == "" {
		url = fmt.Sprintf("/ws/v1/partition/%s/queue/%s/applications/%s", partition, queue, state)
	} else {
		url = fmt.Sprintf("/ws/v1/partition/%s/queue/%s/applications/%s?status=%s", partition, queue, state, status)
	}
	req, err := http.NewRequest("GET", url, strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "partition", Value: partition},
		httprouter.Param{Key: "queue", Value: queue},
		httprouter.Param{Key: "state", Value: state},
	}))

	assert.NilError(t, err, "")
	resp := &MockResponseWriter{}
	getQueueApplicationsByState(resp, req)

	var specificStatusApplicationsDAO []*dao.ApplicationDAOInfo
	err = json.Unmarshal(resp.outputBytes, &specificStatusApplicationsDAO)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, len(specificStatusApplicationsDAO), len(expectedApp))

	daoAppIDs := make(map[string]bool)
	expectedAppIDs := make(map[string]bool)
	for _, app := range specificStatusApplicationsDAO {
		daoAppIDs[app.ApplicationID] = true
	}
	for _, app := range expectedApp {
		expectedAppIDs[app.ApplicationID] = true
	}
	assert.DeepEqual(t, daoAppIDs, expectedAppIDs)
}

func checkGetQueueAppByIllegalStateOrStatus(t *testing.T, partition, queue, state, status string) {
	var url string
	if status == "" {
		url = fmt.Sprintf("/ws/v1/partition/%s/queue/%s/applications/%s", partition, queue, state)
	} else {
		url = fmt.Sprintf("/ws/v1/partition/%s/queue/%s/applications/%s?status=%s", partition, queue, state, status)
	}
	req, err := http.NewRequest("GET", url, strings.NewReader(""))
	req = req.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "partition", Value: partition},
		httprouter.Param{Key: "queue", Value: queue},
		httprouter.Param{Key: "state", Value: state},
	}))

	assert.NilError(t, err, "")
	resp := &MockResponseWriter{}
	getQueueApplicationsByState(resp, req)

	var errInfo dao.YAPIError
	err = json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	var expectedErrMsg string
	if strings.ToLower(state) != AppStateActive {
		expectedErrMsg = fmt.Sprintf("Only following application states are allowed: %s", AppStateActive)
	} else if status != "" {
		expectedErrMsg = allowedActiveStatusMsg
	}
	assert.Equal(t, errInfo.Message, expectedErrMsg)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func TestGetQueueApplicationsByStateHandler(t *testing.T) {
	defaultPartition := setup(t, configDefault, 1)
	NewWebApp(schedulerContext.Load(), nil)

	// Accept status
	app1 := addApp(t, "app-1", defaultPartition, "root.default", false)
	app1.SetState(objects.New.String())
	app2 := addApp(t, "app-2", defaultPartition, "root.default", false)
	app2.SetState(objects.Accepted.String())
	app3 := addApp(t, "app-3", defaultPartition, "root.default", false)
	app3.SetState(objects.Running.String())
	app4 := addApp(t, "app-4", defaultPartition, "root.default", false)
	app4.SetState(objects.Completing.String())
	app5 := addApp(t, "app-5", defaultPartition, "root.default", false)
	app5.SetState(objects.Failing.String())
	app6 := addApp(t, "app-6", defaultPartition, "root.default", false)
	app6.SetState(objects.Resuming.String())

	newStateAppList := []*objects.Application{app1}
	checkGetQueueAppByState(t, "default", "root.default", "Active", "New", newStateAppList)
	acceptedStatusAppList := []*objects.Application{app2}
	checkGetQueueAppByState(t, "default", "root.default", "Active", "Accepted", acceptedStatusAppList)
	runningStatusAppList := []*objects.Application{app3}
	checkGetQueueAppByState(t, "default", "root.default", "Active", "Running", runningStatusAppList)
	completingStatusAppList := []*objects.Application{app4}
	checkGetQueueAppByState(t, "default", "root.default", "Active", "Completing", completingStatusAppList)
	failingStatusAppList := []*objects.Application{app5}
	checkGetQueueAppByState(t, "default", "root.default", "Active", "Failing", failingStatusAppList)
	resumingStatusAppList := []*objects.Application{app6}
	checkGetQueueAppByState(t, "default", "root.default", "Active", "Resuming", resumingStatusAppList)
	allStatusAppList := []*objects.Application{app1, app2, app3, app4, app5, app6}
	checkGetQueueAppByState(t, "default", "root.default", "Active", "", allStatusAppList)

	checkGetQueueAppByIllegalStateOrStatus(t, "default", "root.default", "Rejected", "")
	checkGetQueueAppByIllegalStateOrStatus(t, "default", "root.default", "Completed", "")
	checkGetQueueAppByIllegalStateOrStatus(t, "default", "root.default", "Active", "Invalid")
}

//nolint:funlen
func TestGetApplicationHandler(t *testing.T) {
	part := setup(t, configDefault, 1)

	// add 1 application
	app := addApp(t, "app-1", part, "root.default", false)
	res := &si.Resource{
		Resources: map[string]*si.Quantity{"vcore": {Value: 1}},
	}
	ask := objects.NewAllocationFromSI(&si.Allocation{
		ApplicationID:    "app-1",
		PartitionName:    part.Name,
		ResourcePerAlloc: res})
	err := app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")

	NewWebApp(schedulerContext.Load(), nil)

	var req *http.Request
	req, err = createRequest(t, "/ws/v1/partition/default/queue/root.default/application/app-1", map[string]string{"partition": partitionNameWithoutClusterID, "queue": "root.default", "application": "app-1"})
	assert.NilError(t, err)
	resp := &MockResponseWriter{}
	var appsDao *dao.ApplicationDAOInfo
	getApplication(resp, req)
	err = json.Unmarshal(resp.outputBytes, &appsDao)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, appsDao.ApplicationID, "app-1")
	assert.Equal(t, appsDao.Partition, "default")
	assert.Equal(t, appsDao.QueueName, "root.default")
	assert.Equal(t, len(appsDao.Allocations), 0)

	// test nonexistent partition
	var req1 *http.Request
	req1, err = createRequest(t, "/ws/v1/partition/default/queue/root.default/application/app-1", map[string]string{"partition": "notexists", "queue": "root.default", "application": "app-1"})
	assert.NilError(t, err)
	resp1 := &MockResponseWriter{}
	getApplication(resp1, req1)
	assertPartitionNotExists(t, resp1)

	// test nonexistent queue
	var req2 *http.Request
	req2, err = createRequest(t, "/ws/v1/partition/default/queue/root.default/application/app-1", map[string]string{"partition": partitionNameWithoutClusterID, "queue": "notexists", "application": "app-1"})
	assert.NilError(t, err)
	resp2 := &MockResponseWriter{}
	getApplication(resp2, req2)
	assertQueueNotExists(t, resp2)

	// test nonexistent application
	var req3 *http.Request
	req3, err = createRequest(t, "/ws/v1/partition/default/queue/root.noapps/application/app-1", map[string]string{"partition": partitionNameWithoutClusterID, "queue": "root.noapps", "application": "app-1"})
	assert.NilError(t, err)
	resp3 := &MockResponseWriter{}
	getApplication(resp3, req3)
	assertApplicationNotExists(t, resp3)

	// test without queue
	var req4 *http.Request
	req4, err = createRequest(t, "/ws/v1/partition/default/application/app-1", map[string]string{"partition": partitionNameWithoutClusterID, "application": "app-1"})
	assert.NilError(t, err)
	resp4 := &MockResponseWriter{}
	var appsDao4 *dao.ApplicationDAOInfo
	getApplication(resp4, req4)
	err = json.Unmarshal(resp4.outputBytes, &appsDao4)
	assert.NilError(t, err, unmarshalError)
	assert.Assert(t, appsDao4 != nil)
	assert.Equal(t, appsDao4.ApplicationID, "app-1")
	assert.Equal(t, appsDao4.Partition, "default")
	assert.Equal(t, appsDao4.QueueName, "root.default")
	assert.Equal(t, len(appsDao4.Reservations), 0)

	// test invalid queue name
	var req5 *http.Request
	req5, err = createRequest(t, "/ws/v1/partition/default/queue/root.default/application/app-1", map[string]string{"partition": partitionNameWithoutClusterID, "queue": "root.test.test123!", "application": "app-1"})
	assert.NilError(t, err)
	resp5 := &MockResponseWriter{}
	getApplication(resp5, req5)
	assertQueueInvalid(t, resp5, "root.test.test123!", "test123!")

	// test missing params name
	req, err = createRequest(t, "/ws/v1/partition/default/queue/root.default/application/app-1", map[string]string{})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getApplication(resp, req)
	assertParamsMissing(t, resp)

	// test queue name with special characters escaped properly
	queueName := url.QueryEscape("root.parent.test@t#:rt:/_ff-test")
	req, err = createRequest(t, "/ws/v1/partition/default/queue/root.default/application/app-1", map[string]string{"partition": partitionNameWithoutClusterID, "queue": queueName})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getApplication(resp, req)
	assertQueueNotExists(t, resp)

	// test queue name with special characters escaped not properly, catch error at request level
	_, err = http.NewRequest("GET", "/ws/v1/partition/default/queue/"+invalidQueueName+"/application/app-1", strings.NewReader(""))
	assert.ErrorContains(t, err, "invalid URL escape")

	// test queue name with special characters escaped not properly, catch error while un escaping queue name
	req, err = createRequest(t, "/ws/v1/partition/default/queue/root.default/application/app-1", map[string]string{"partition": partitionNameWithoutClusterID, "queue": invalidQueueName})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getApplication(resp, req)
	var errInfo dao.YAPIError
	err = json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, "invalid URL escape \"%Zt\"", jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)

	// test additional application details
	var req6 *http.Request
	req6, err = http.NewRequest("GET", "/ws/v1/partition/default/queue/root.default/application/app-1", strings.NewReader(""))
	assert.NilError(t, err, "HTTP request create failed")
	req6 = req6.WithContext(context.WithValue(req.Context(), httprouter.ParamsKey, httprouter.Params{
		httprouter.Param{Key: "partition", Value: partitionNameWithoutClusterID},
		httprouter.Param{Key: "queue", Value: "root.default"},
		httprouter.Param{Key: "application", Value: "app-1"},
	}))
	assert.NilError(t, err, "Get Application Handler request failed")
	resp6 := &MockResponseWriter{}
	var appDao *dao.ApplicationDAOInfo
	getApplication(resp6, req6)
	appSummary := app.GetApplicationSummary(partitionNameWithoutClusterID)
	err = json.Unmarshal(resp6.outputBytes, &appDao)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, "app-1", appDao.ApplicationID)
	assert.Equal(t, app.StartTime().UnixMilli(), appDao.StartTime)
	assert.Assert(t, resources.EqualsTracked(appSummary.ResourceUsage, appDao.ResourceUsage))
	assert.Assert(t, resources.EqualsTracked(appSummary.PreemptedResource, appDao.PreemptedResource))
	assert.Assert(t, resources.EqualsTracked(appSummary.PlaceholderResource, appDao.PlaceholderResource))
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

func assertQueueInvalid(t *testing.T, resp *MockResponseWriter, invalidQueuePath string, invalidQueueName string) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, common.InvalidQueueName.Error(), jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
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

func assertInvalidGroupName(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, InvalidGroupName, jsonMessageError)
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

func assertAppStateNotAllow(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, "Only following application states are allowed: active, rejected, completed", jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func assertActiveStateNotAllow(t *testing.T, resp *MockResponseWriter) {
	var errInfo dao.YAPIError
	err := json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, allowedActiveStatusMsg, jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}

func TestValidateQueue(t *testing.T) {
	err := validateQueue("root.test.test123")
	assert.NilError(t, err, "Queue path is correct but still throwing error.")

	invalidQueuePath := "root.test.test123!"
	err1 := validateQueue(invalidQueuePath)
	assert.Error(t, err1, common.InvalidQueueName.Error())

	err2 := validateQueue("root")
	assert.NilError(t, err2, "Queue path is correct but still throwing error.")

	err3 := validateQueue("root.@recovery@")
	assert.NilError(t, err3, "Queue path is correct but still throwing error.")
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

	prepareSchedulerContext(t)

	partitionContext := schedulerContext.Load().GetPartitionMapClone()
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
	// default config has only one partition
	verifyStateDumpJSON(t, &aggregated, 1)
}

func TestSpecificUserResourceUsage(t *testing.T) {
	prepareUserAndGroupContext(t, groupsLimitsConfig)

	// Test existed user query
	req, err := createRequest(t, "/ws/v1/partition/default/usage/user/", map[string]string{"user": "testuser", "group": "testgroup"})
	assert.NilError(t, err)
	resp := &MockResponseWriter{}
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
	req, err = createRequest(t, "/ws/v1/partition/default/usage/user/", map[string]string{"user": "testNonExistingUser", "group": "testgroup"})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getUserResourceUsage(resp, req)
	assertUserNotExists(t, resp)

	// Test username with special characters escaped properly
	validUser := url.QueryEscape("test_a-b_c@#d@do:mai/n.com")
	req, err = createRequest(t, "/ws/v1/partition/default/usage/user/", map[string]string{"user": validUser, "group": "testgroup"})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getUserResourceUsage(resp, req)
	assertUserNotExists(t, resp)

	// Test username with special characters not escaped properly, catch error at request level
	invalidUser := "test_a-b_c%Zt@#d@do:mai/n.com"
	_, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/user/"+invalidUser, strings.NewReader(""))
	assert.ErrorContains(t, err, "invalid URL escape")

	// Test username with special characters not escaped properly, catch error while un escaping username
	req, err = createRequest(t, "/ws/v1/partition/default/usage/user/test", map[string]string{"user": invalidUser, "group": "testgroup"})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getUserResourceUsage(resp, req)
	var errInfo dao.YAPIError
	err = json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, "invalid URL escape \"%Zt\"", jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)

	// Test invalid user name that does not match UserRegExp
	invalidUserName := "1InvalidUser"
	req, err = createRequest(t, "/ws/v1/partition/default/usage/user/", map[string]string{"user": invalidUserName, "group": "testgroup"})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getUserResourceUsage(resp, req)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode)
	var invalidUserError dao.YAPIError
	err = json.Unmarshal(resp.outputBytes, &invalidUserError)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, InvalidUserName, invalidUserError.Message)
	assert.Equal(t, http.StatusBadRequest, invalidUserError.StatusCode)
}

func TestSpecificGroupResourceUsage(t *testing.T) {
	prepareUserAndGroupContext(t, groupsLimitsConfig)
	// Test existed group query
	req, err := createRequest(t, "/ws/v1/partition/default/usage/group", map[string]string{"user": "testuser", "group": "testgroup"})
	assert.NilError(t, err)
	resp := &MockResponseWriter{}
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
	req, err = createRequest(t, "/ws/v1/partition/default/usage/group", map[string]string{"user": "testuser", "group": "testNonExistingGroup"})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getGroupResourceUsage(resp, req)
	assertGroupNotExists(t, resp)

	// Test params name missing
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/group/", strings.NewReader(""))
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getGroupResourceUsage(resp, req)
	assertParamsMissing(t, resp)

	// Test group name with special characters escaped properly
	validGroup := url.QueryEscape("test_a-b_c@#d@do:mai/n.com")
	req, err = createRequest(t, "/ws/v1/partition/default/usage/group", map[string]string{"user": "testuser", "group": validGroup})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getGroupResourceUsage(resp, req)
	assertInvalidGroupName(t, resp)

	// Test group name with special characters not escaped properly, catch error at request level
	invalidGroup := "test_a-b_c%Zt@#d@do:mai/n.com"
	_, err = http.NewRequest("GET", "/ws/v1/partition/default/usage/group/"+invalidGroup, strings.NewReader(""))
	assert.ErrorContains(t, err, "invalid URL escape")

	// Test group name with special characters not escaped properly, catch error while un escaping group name
	req, err = createRequest(t, "/ws/v1/partition/default/usage/group/test", map[string]string{"user": "testuser", "group": invalidGroup})
	assert.NilError(t, err)
	resp = &MockResponseWriter{}
	getGroupResourceUsage(resp, req)
	var errInfo dao.YAPIError
	err = json.Unmarshal(resp.outputBytes, &errInfo)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, http.StatusBadRequest, resp.statusCode, statusCodeError)
	assert.Equal(t, errInfo.Message, "invalid URL escape \"%Zt\"", jsonMessageError)
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
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
	resp = &MockResponseWriter{}
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

	// "count" too high
	maxRESTResponseSize.Store(1)
	defer maxRESTResponseSize.Store(configs.DefaultRESTResponseSize)
	checkSingleEvent(t, appEvent, "count=3")
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
	setup(t, configDefault, 1)
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
	assertInstanceUUID(t, lines[0])
	assertEvent(t, lines[1], 111, "app-1")
	assertEvent(t, lines[2], 222, "node-1")
	assertEvent(t, lines[3], 333, "app-2")
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
	assertInstanceUUID(t, lines[0])
	assertEvent(t, lines[1], 111, "app-1")
	assertYunikornError(t, lines[2], "Event stream was closed by the producer")
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
	setup(t, configDefault, 1)
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
	assert.NilError(t, err)
	lines := strings.Split(string(output[:n]), "\n")
	assertInstanceUUID(t, lines[0])

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
	lines = strings.Split(string(output[:n]), "\n")
	assertInstanceUUID(t, lines[0])
	assertEvent(t, lines[1], 1, "")
	assertEvent(t, lines[2], 2, "")

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

	assertGetStreamError(t, false, req, resp, http.StatusInternalServerError, "Event tracking is disabled")
}

func TestGetStream_NoWriteDeadline(t *testing.T) {
	ev, req := initEventsAndCreateRequest(t)
	defer ev.Stop()
	resp := httptest.NewRecorder() // does not have SetWriteDeadline()

	assertGetStreamError(t, false, req, resp, http.StatusInternalServerError, "Cannot set write deadline: feature not supported")
}

func TestGetStream_SetWriteDeadlineFails(t *testing.T) {
	setup(t, configDefault, 1)
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
	checkGetStreamErrorResult(t, true, resp.Result(), http.StatusOK, "Cannot set write deadline: SetWriteDeadline failed")
}

func TestGetStream_SetReadDeadlineFails(t *testing.T) {
	_, req := initEventsAndCreateRequest(t)
	resp := NewResponseRecorderWithDeadline()
	resp.setReadFails = true

	assertGetStreamError(t, false, req, resp, http.StatusInternalServerError, "Cannot set read deadline: SetReadDeadline failed")
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
	err := common.WaitForCondition(time.Millisecond, time.Second, func() bool {
		streamingLimiter.Lock()
		defer streamingLimiter.Unlock()
		return streamingLimiter.streams == 3
	})
	assert.NilError(t, err)
	assertGetStreamError(t, false, req, resp, http.StatusServiceUnavailable, "Too many streaming connections")
}

func assertGetStreamError(t *testing.T, withUUID bool, req *http.Request, resp interface{}, statusCode int, expectedMsg string) {
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

	checkGetStreamErrorResult(t, withUUID, response, statusCode, expectedMsg)
}

func checkGetStreamErrorResult(t *testing.T, withUUID bool, response *http.Response, statusCode int, expectedMsg string) {
	t.Helper()
	output := make([]byte, 256)
	n, err := response.Body.Read(output)
	assert.NilError(t, err)
	if withUUID {
		lines := strings.Split(string(output[:n]), "\n")
		assertInstanceUUID(t, lines[0])
		assertYunikornError(t, lines[1], expectedMsg)
	} else {
		line := string(output[:n])
		assertYunikornError(t, line, expectedMsg)
	}
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

func assertInstanceUUID(t *testing.T, output string) {
	var id dao.YunikornID
	err := json.Unmarshal([]byte(output), &id)
	assert.NilError(t, err)
	assert.Assert(t, id.InstanceUUID != "")
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
	noEvents := uint64(0)
	err := common.WaitForCondition(10*time.Millisecond, time.Second, func() bool {
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

func prepareSchedulerContext(t *testing.T) {
	config := []byte(configDefault)
	var err error
	ctx, err := scheduler.NewClusterContext(rmID, policyGroup, config)
	schedulerContext.Store(ctx)
	assert.NilError(t, err, "Error when load clusterInfo from config")
	assert.Equal(t, 1, len(schedulerContext.Load().GetPartitionMapClone()))
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
	ask := objects.NewAllocationFromSI(&si.Allocation{
		ApplicationID:    "app-1",
		PartitionName:    part.Name,
		ResourcePerAlloc: res})
	err := app.AddAllocationAsk(ask)
	assert.NilError(t, err, "ask should have been added to app")

	// add an alloc
	allocInfo := newAlloc(ask.GetAllocationKey(), ask.GetApplicationID(), "node-1", ask.GetAllocatedResource())
	app.AddAllocation(allocInfo)
	assert.Assert(t, app.IsRunning(), "Application did not return running state after alloc: %s", app.CurrentState())

	NewWebApp(schedulerContext.Load(), nil)
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

func verifyStateDumpJSON(t *testing.T, aggregated *AggregatedStateInfo, partitionCount int) {
	assert.Check(t, aggregated.Timestamp != 0)
	assert.Check(t, len(aggregated.Partitions) == partitionCount, "incorrect partition count")
	assert.Check(t, len(aggregated.Nodes) > 0)
	assert.Check(t, len(aggregated.ClusterInfo) > 0)
	assert.Check(t, len(aggregated.Queues) == 1, "should only have root queue")
	assert.Check(t, len(aggregated.LogLevel) > 0)
	assert.Check(t, len(aggregated.Config.SchedulerConfig.Partitions) == partitionCount, "incorrect partition count")
	assert.Check(t, len(aggregated.Config.Extra) > 0)
	assert.Check(t, aggregated.RMDiagnostics["empty"] != nil, "expected no RM registered for diagnostics")
	assert.Check(t, len(aggregated.PlacementRules) == partitionCount, "incorrect partition count")
	assert.Check(t, len(aggregated.PlacementRules[0].Rules) == 2, "incorrect rule count")
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
	testSchedulerContext := &scheduler.ClusterContext{}
	testSchedulerContext.SetLastHealthCheckResult(expected)
	NewWebApp(testSchedulerContext, nil)

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

func TestGetPartitionRuleHandler(t *testing.T) {
	setup(t, configDefault, 1)

	NewWebApp(schedulerContext.Load(), nil)

	// test partition not exists
	req, err := createRequest(t, "/ws/v1/partition/default/placementrules", map[string]string{"partition": "notexists"})
	assert.NilError(t, err, httpRequestError)
	resp := &MockResponseWriter{}
	getPartitionRules(resp, req)
	assertPartitionNotExists(t, resp)

	// test params name missing
	req, err = http.NewRequest("GET", "/ws/v1/partition/default/placementrules", strings.NewReader(""))
	assert.NilError(t, err, httpRequestError)
	resp = &MockResponseWriter{}
	getPartitionRules(resp, req)
	assertParamsMissing(t, resp)

	// default config without rules defined
	req, err = createRequest(t, "/ws/v1/partition/default/placementrules", map[string]string{"partition": partitionNameWithoutClusterID})
	assert.NilError(t, err, httpRequestError)
	var partitionRules []*dao.RuleDAO
	resp = &MockResponseWriter{}
	getPartitionRules(resp, req)
	err = json.Unmarshal(resp.outputBytes, &partitionRules)
	assert.NilError(t, err, unmarshalError)
	// assert content: default is provided and recovery
	assert.Equal(t, len(partitionRules), 2)
	assert.Equal(t, partitionRules[0].Name, types.Provided)
	assert.Equal(t, partitionRules[1].Name, types.Recovery)

	// change the config: 3 rules, expect recovery also
	err = schedulerContext.Load().UpdateRMSchedulerConfig(rmID, []byte(placementRuleConfig))
	assert.NilError(t, err, "Error when updating clusterInfo from config")
	req, err = createRequest(t, "/ws/v1/partition/default/placementrules", map[string]string{"partition": partitionNameWithoutClusterID})
	assert.NilError(t, err, httpRequestError)
	resp = &MockResponseWriter{}
	getPartitionRules(resp, req)
	err = json.Unmarshal(resp.outputBytes, &partitionRules)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, len(partitionRules), 4)
	assert.Equal(t, partitionRules[0].Name, types.User)
	assert.Equal(t, partitionRules[1].Name, types.Tag)
	assert.Equal(t, partitionRules[1].ParentRule.Name, types.Fixed)
	assert.Equal(t, partitionRules[2].Name, types.Fixed)
	assert.Equal(t, partitionRules[3].Name, types.Recovery)
}

func TestRedirectDebugHandler(t *testing.T) {
	NewWebApp(&scheduler.ClusterContext{}, nil)
	base := "http://yunikorn.host.com:9080"
	code := http.StatusMovedPermanently
	tests := []struct {
		name     string
		reqURL   string
		redirect string
	}{
		{"statedump", "/ws/v1/fullstatedump", "/debug/fullstatedump"},
		{"stacks", "/ws/v1/stack", "/debug/stack"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := createRequest(t, base+tt.reqURL, map[string]string{})
			assert.NilError(t, err, httpRequestError)
			resp := &MockResponseWriter{}
			redirectDebug(resp, req)
			assert.Equal(t, resp.statusCode, code, "expected moved permanently status")
			loc := resp.Header().Get("Location")
			assert.Assert(t, loc != "", "expected redirect header to be set")
			assert.Assert(t, strings.HasSuffix(loc, tt.redirect), "expected redirect to debug")
			assert.Assert(t, strings.Contains(string(resp.outputBytes), http.StatusText(code)))
		})
	}
}

func TestSetMaxRESTResponseSize(t *testing.T) {
	current := configs.GetConfigMap()
	defer configs.SetConfigMap(current)

	configs.SetConfigMap(map[string]string{
		configs.CMRESTResponseSize: "1234",
	})
	assert.Equal(t, uint64(1234), maxRESTResponseSize.Load())

	configs.SetConfigMap(map[string]string{})
	assert.Equal(t, uint64(10000), maxRESTResponseSize.Load())

	configs.SetConfigMap(map[string]string{
		configs.CMRESTResponseSize: "xyz",
	})
	assert.Equal(t, uint64(10000), maxRESTResponseSize.Load())

	configs.SetConfigMap(map[string]string{
		configs.CMRESTResponseSize: "0",
	})
	assert.Equal(t, uint64(10000), maxRESTResponseSize.Load())

	configs.SetConfigMap(map[string]string{
		configs.CMRESTResponseSize: "-1",
	})
	assert.Equal(t, uint64(10000), maxRESTResponseSize.Load())
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

func newAlloc(allocationKey string, appID string, nodeID string, resAlloc *resources.Resource) *objects.Allocation {
	return objects.NewAllocationFromSI(&si.Allocation{
		AllocationKey:    allocationKey,
		ApplicationID:    appID,
		NodeID:           nodeID,
		ResourcePerAlloc: resAlloc.ToProto(),
	})
}

func newForeignAlloc(allocationKey string, appID string, nodeID string, resAlloc *resources.Resource, fType string, priority int32) *objects.Allocation {
	return objects.NewAllocationFromSI(&si.Allocation{
		AllocationKey:    allocationKey,
		NodeID:           nodeID,
		ResourcePerAlloc: resAlloc.ToProto(),
		AllocationTags: map[string]string{
			siCommon.Foreign: fType,
		},
		Priority: priority,
	})
}
