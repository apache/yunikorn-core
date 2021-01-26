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
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics/history"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

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

const rmID = "rm-123"
const policyGroup = "default-policy-group"

// simple wrapper to make creating an app easier
func newApplication(appID, partitionName, queueName, rmID string) *objects.Application {
	siApp := &si.AddApplicationRequest{
		ApplicationID: appID,
		QueueName:     queueName,
		PartitionName: partitionName,
	}
	return objects.NewApplication(siApp, security.UserGroup{}, nil, rmID)
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
	assert.Equal(t, resp.statusCode, http.StatusNotImplemented, "app history handler returned wrong status")

	// init should return null and thus no records
	imHistory = history.NewInternalMetricsHistory(5)
	resp = &MockResponseWriter{}
	getApplicationHistory(resp, req)
	var appHist []dao.ApplicationHistoryDAOInfo
	err := json.Unmarshal(resp.outputBytes, &appHist)
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
	assert.Equal(t, resp.statusCode, http.StatusNotImplemented, "container history handler returned wrong status")

	// init should return null and thus no records
	imHistory = history.NewInternalMetricsHistory(5)
	resp = &MockResponseWriter{}
	getContainerHistory(resp, req)
	var contHist []dao.ContainerHistoryDAOInfo
	err := json.Unmarshal(resp.outputBytes, &contHist)
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
	configs.MockSchedulerConfigByData([]byte(startConf))
	var err error
	schedulerContext, err = scheduler.NewClusterContext(rmID, policyGroup)
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

	// pull the checksum out of the response
	parts := strings.SplitAfter(string(resp.outputBytes), "sha256 checksum: ")
	assert.Equal(t, len(parts), 2, "checksum boundary not found")
	startConfSum := parts[1]

	// change the config
	configs.MockSchedulerConfigByData([]byte(updatedConf))
	err = schedulerContext.UpdateRMSchedulerConfig(rmID)
	assert.NilError(t, err, "Error when updating clusterInfo from config")
	// check that we return yaml by default, unmarshal will error when we don't
	req.Header.Set("Accept", "unknown")
	getClusterConfig(resp, req)
	err = yaml.Unmarshal(resp.outputBytes, conf)
	assert.NilError(t, err, "failed to unmarshal config from response body (updated config)")
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "binpacking", "node sort policy not updated")

	parts = strings.SplitAfter(string(resp.outputBytes), "sha256 checksum: ")
	assert.Equal(t, len(parts), 2, "checksum boundary not found")
	updatedConfSum := parts[1]
	assert.Assert(t, startConfSum != updatedConfSum, "checksums did not change in output")
}

func TestGetConfigJSON(t *testing.T) {
	configs.MockSchedulerConfigByData([]byte(startConf))
	var err error
	schedulerContext, err = scheduler.NewClusterContext(rmID, policyGroup)
	assert.NilError(t, err, "Error when load clusterInfo from config")
	// No err check: new request always returns correctly
	//nolint: errcheck
	req, _ := http.NewRequest("GET", "", nil)
	req.Header.Set("Accept", "application/json")
	resp := &MockResponseWriter{}
	getClusterConfig(resp, req)

	// json unmarshal does not handle the checksum add the end automatically
	// need to remove it before unmarshalling
	parts := strings.SplitAfter(string(resp.outputBytes), "\n")
	assert.Equal(t, len(parts), 2, "checksum boundary not found (json)")
	startConfSum := parts[1]
	conf := &configs.SchedulerConfig{}
	err = json.Unmarshal([]byte(parts[0]), conf)
	assert.NilError(t, err, "failed to unmarshal config from response body (json)")
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "fair", "node sort policy set incorrectly, not fair (json)")

	// change the config
	configs.MockSchedulerConfigByData([]byte(updatedConf))
	err = schedulerContext.UpdateRMSchedulerConfig(rmID)
	assert.NilError(t, err, "Error when updating clusterInfo from config")

	getClusterConfig(resp, req)
	parts = strings.SplitAfter(string(resp.outputBytes), "\n")
	assert.Equal(t, len(parts), 2, "checksum boundary not found (json changed)")
	assert.Assert(t, startConfSum != parts[1], "checksums did not change in json output: %s, %s", startConfSum, parts[1])
	err = json.Unmarshal([]byte(parts[0]), conf)
	assert.NilError(t, err, "failed to unmarshal config from response body (json, updated config)")
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "binpacking", "node sort policy not updated (json)")
}

func TestQueryParamInAppsHandler(t *testing.T) {
	configs.MockSchedulerConfigByData([]byte(configDefault))
	var err error
	schedulerContext, err = scheduler.NewClusterContext(rmID, policyGroup)
	assert.NilError(t, err, "Error when load clusterInfo from config")

	assert.Equal(t, 1, len(schedulerContext.GetPartitionMapClone()))

	// Check default partition
	partitionName := "[" + rmID + "]default"
	part := schedulerContext.GetPartition(partitionName)
	assert.Equal(t, 0, len(part.GetApplications()))

	// add a new app
	app := newApplication("app-1", partitionName, "root.default", rmID)
	err = part.AddApplication(app)
	assert.NilError(t, err, "Failed to add Application to Partition.")
	assert.Equal(t, app.CurrentState(), objects.New.String())
	assert.Equal(t, 1, len(part.GetApplications()))

	NewWebApp(schedulerContext, nil)

	// Passing "root.default" as filter return 1 application
	var req *http.Request
	req, err = http.NewRequest("GET", "/ws/v1/apps?queue=root.default", strings.NewReader(""))
	assert.NilError(t, err, "App Handler request failed")
	resp := &MockResponseWriter{}
	var appsDao []*dao.ApplicationDAOInfo
	getApplicationsInfo(resp, req)
	err = json.Unmarshal(resp.outputBytes, &appsDao)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, len(appsDao), 1)

	// Passing "root.q1" as filter return 0 application as there is no app running in "root.q1" queue
	req, err = http.NewRequest("GET", "/ws/v1/apps?queue=root.q1", strings.NewReader(""))
	assert.NilError(t, err, "App Handler request failed")
	resp = &MockResponseWriter{}
	getApplicationsInfo(resp, req)
	err = json.Unmarshal(resp.outputBytes, &appsDao)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, len(appsDao), 0)

	// Not passing any queue filter (only handler endpoint) return all applications
	req, err = http.NewRequest("GET", "/ws/v1/apps", strings.NewReader(""))
	assert.NilError(t, err, "App Handler request failed")
	resp = &MockResponseWriter{}
	getApplicationsInfo(resp, req)
	err = json.Unmarshal(resp.outputBytes, &appsDao)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, len(appsDao), 1)

	// Passing "root.q1.default" as filter return 0 application though child queue name is same but different queue path
	req, err = http.NewRequest("GET", "/ws/v1/apps?queue=root.q1.default", strings.NewReader(""))
	assert.NilError(t, err, "App Handler request failed")
	resp = &MockResponseWriter{}
	getApplicationsInfo(resp, req)
	err = json.Unmarshal(resp.outputBytes, &appsDao)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body: %s", string(resp.outputBytes))
	assert.Equal(t, len(appsDao), 0)

	// Passing "root.default(spe" as filter throws bad request error as queue name doesn't comply with expected characters
	req, err = http.NewRequest("GET", "/ws/v1/apps?queue=root.default(spe", strings.NewReader(""))
	assert.NilError(t, err, "App Handler request failed")
	resp = &MockResponseWriter{}
	getApplicationsInfo(resp, req)
	assert.Equal(t, 400, resp.statusCode)
}

type FakeConfigPlugin struct {
	generateError bool
}

func (f FakeConfigPlugin) UpdateConfiguration(args *si.UpdateConfigurationRequest) *si.UpdateConfigurationResponse {
	if f.generateError {
		return &si.UpdateConfigurationResponse{
			Success: false,
			Reason:  "configuration update error",
		}
	}
	return &si.UpdateConfigurationResponse{
		Success:   true,
		OldConfig: startConf,
	}
}

func TestSaveConfigMapNoError(t *testing.T) {
	plugins.RegisterSchedulerPlugin(&FakeConfigPlugin{generateError: false})
	oldConf, err := updateConfiguration(updatedConf)
	assert.NilError(t, err, "No error expected")
	assert.Equal(t, oldConf, startConf, " Wrong returned configuration")
}

func TestSaveConfigMapErrorExpected(t *testing.T) {
	plugins.RegisterSchedulerPlugin(&FakeConfigPlugin{generateError: true})
	oldConf, err := updateConfiguration(updatedConf)
	assert.Assert(t, err != nil, "Missing expected error")
	assert.Equal(t, oldConf, "", " Wrong returned configuration")
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
	assert.Equal(t, http.StatusConflict, resp.statusCode, "Status code is wrong")
	assert.Assert(t, strings.Contains(string(resp.outputBytes), err.Error()), "Error message should contain the reason")
}

func TestUpdateConfig(t *testing.T) {
	prepareSchedulerForConfigChange(t)
	resp := &MockResponseWriter{}
	req, err := http.NewRequest("PUT", "", strings.NewReader(updatedConf))
	assert.NilError(t, err, "Failed to create the request")
	updateClusterConfig(resp, req)
	assert.NilError(t, err, "No error expected")
	assert.Equal(t, http.StatusOK, resp.statusCode, "No error expected")
}

func TestUpdateConfigInvalidConf(t *testing.T) {
	prepareSchedulerForConfigChange(t)
	resp := &MockResponseWriter{}
	req, err := http.NewRequest("PUT", "", strings.NewReader(invalidConf))
	assert.NilError(t, err, "Failed to create the request")
	updateClusterConfig(resp, req)
	assert.Equal(t, http.StatusConflict, resp.statusCode, "Status code is wrong")
	assert.Assert(t, len(string(resp.outputBytes)) > 0, "Error message is expected")
}

func prepareSchedulerForConfigChange(t *testing.T) {
	plugins.RegisterSchedulerPlugin(&FakeConfigPlugin{generateError: false})
	configs.MockSchedulerConfigByData([]byte(startConf))
	var err error
	schedulerContext, err = scheduler.NewClusterContext(rmID, policyGroup)
	assert.NilError(t, err, "Error when load clusterInfo from config")
}

func TestGetClusterUtilJSON(t *testing.T) {
	configs.MockSchedulerConfigByData([]byte(configDefault))
	var err error
	schedulerContext, err = scheduler.NewClusterContext(rmID, policyGroup)
	assert.NilError(t, err, "Error when load clusterInfo from config")
	assert.Equal(t, 1, len(schedulerContext.GetPartitionMapClone()))

	// Check test partitions
	partitionName := "[" + rmID + "]default"
	partition := schedulerContext.GetPartition(partitionName)
	assert.Equal(t, partitionName, partition.Name)
	// new app to partition
	queueName := "root.default"
	appID := "appID-1"
	app := newApplication(appID, partitionName, queueName, rmID)
	err = partition.AddApplication(app)
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
	nodeID := "node-1"
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{resources.MEMORY: 1000, resources.VCORE: 1000}).ToProto()
	node1 := objects.NewNode(&si.NewNodeInfo{NodeID: nodeID, SchedulableResource: nodeRes})

	resAlloc1 := resources.NewResourceFromMap(map[string]resources.Quantity{resources.MEMORY: 500, resources.VCORE: 300})
	resAlloc2 := resources.NewResourceFromMap(map[string]resources.Quantity{resources.MEMORY: 300, resources.VCORE: 200})
	ask1 := &objects.AllocationAsk{
		AllocationKey:     "alloc-1",
		QueueName:         queueName,
		ApplicationID:     appID,
		AllocatedResource: resAlloc1,
	}
	ask2 := &objects.AllocationAsk{
		AllocationKey:     "alloc-2",
		QueueName:         queueName,
		ApplicationID:     appID,
		AllocatedResource: resAlloc2,
	}
	alloc1 := objects.NewAllocation("alloc-1-uuid", nodeID, ask1)
	alloc2 := objects.NewAllocation("alloc-2-uuid", nodeID, ask2)
	allocs := []*objects.Allocation{alloc1, alloc2}
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")

	// set expected result
	utilMem := &dao.ClusterUtilDAOInfo{
		ResourceType: resources.MEMORY,
		Total:        int64(1000),
		Used:         int64(800),
		Usage:        "80%",
	}
	utilCore := &dao.ClusterUtilDAOInfo{
		ResourceType: resources.VCORE,
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
	configs.MockSchedulerConfigByData([]byte(configDefault))
	var err error
	schedulerContext, err = scheduler.NewClusterContext(rmID, policyGroup)
	assert.NilError(t, err, "Error when load schedulerContext from config")
	assert.Equal(t, 1, len(schedulerContext.GetPartitionMapClone()))

	// Check test partition
	partitionName := "[" + rmID + "]default"
	partition := schedulerContext.GetPartition(partitionName)
	assert.Equal(t, partitionName, partition.Name)
	// create test application
	queueName := "root.default"
	appID := "app1"
	app := newApplication(appID, partitionName, queueName, rmID)
	err = partition.AddApplication(app)
	assert.NilError(t, err, "add application to partition should not have failed")

	// create test nodes
	nodeRes := resources.NewResourceFromMap(map[string]resources.Quantity{resources.MEMORY: 1000, resources.VCORE: 1000}).ToProto()
	node1ID := "node-1"
	node1 := objects.NewNode(&si.NewNodeInfo{NodeID: node1ID, SchedulableResource: nodeRes})
	node2ID := "node-2"
	node2 := objects.NewNode(&si.NewNodeInfo{NodeID: node2ID, SchedulableResource: nodeRes})
	// create test allocations
	resAlloc1 := resources.NewResourceFromMap(map[string]resources.Quantity{resources.MEMORY: 500, resources.VCORE: 300})
	resAlloc2 := resources.NewResourceFromMap(map[string]resources.Quantity{resources.MEMORY: 300, resources.VCORE: 500})
	ask1 := &objects.AllocationAsk{
		AllocationKey:     "alloc-1",
		QueueName:         queueName,
		ApplicationID:     appID,
		AllocatedResource: resAlloc1,
	}
	ask2 := &objects.AllocationAsk{
		AllocationKey:     "alloc-2",
		QueueName:         queueName,
		ApplicationID:     appID,
		AllocatedResource: resAlloc2,
	}
	allocs := []*objects.Allocation{objects.NewAllocation("alloc-1-uuid", node1ID, ask1)}
	err = partition.AddNode(node1, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")
	allocs = []*objects.Allocation{objects.NewAllocation("alloc-2-uuid", node2ID, ask2)}
	err = partition.AddNode(node2, allocs)
	assert.NilError(t, err, "add node to partition should not have failed")

	// get nodes utilization
	res1 := getNodesUtilJSON(partition, resources.MEMORY)
	res2 := getNodesUtilJSON(partition, resources.VCORE)
	resNon := getNodesUtilJSON(partition, "non-exist")
	subres1 := res1.NodesUtil
	subres2 := res2.NodesUtil
	subresNon := resNon.NodesUtil

	assert.Equal(t, res1.ResourceType, resources.MEMORY)
	assert.Equal(t, subres1[2].NumOfNodes, int64(1))
	assert.Equal(t, subres1[4].NumOfNodes, int64(1))
	assert.Equal(t, subres1[2].NodeNames[0], node2ID)
	assert.Equal(t, subres1[4].NodeNames[0], node1ID)

	assert.Equal(t, res2.ResourceType, resources.VCORE)
	assert.Equal(t, subres2[2].NumOfNodes, int64(1))
	assert.Equal(t, subres2[4].NumOfNodes, int64(1))
	assert.Equal(t, subres2[2].NodeNames[0], node1ID)
	assert.Equal(t, subres2[4].NodeNames[0], node2ID)

	assert.Equal(t, resNon.ResourceType, "non-exist")
	assert.Equal(t, subresNon[0].NumOfNodes, int64(-1))
	assert.Equal(t, subresNon[0].NodeNames[0], "N/A")
}

func addAndConfirmApplicationExists(t *testing.T, partitionName string, partition *scheduler.PartitionContext, appName string) *objects.Application {
	// add a new app
	app := newApplication(appName, partitionName, "root.default", rmID)
	err := partition.AddApplication(app)
	assert.NilError(t, err, "Failed to add Application to Partition.")
	assert.Equal(t, app.CurrentState(), objects.New.String())
	return app
}

func TestPartitions(t *testing.T) {
	configs.MockSchedulerConfigByData([]byte(configMultiPartitions))
	var err error
	schedulerContext, err = scheduler.NewClusterContext(rmID, policyGroup)
	assert.NilError(t, err, "Error when load clusterInfo from config")
	assert.Equal(t, 2, len(schedulerContext.GetPartitionMapClone()))

	// Check default partition
	partitionName := "[" + rmID + "]default"
	defaultPartition := schedulerContext.GetPartition(partitionName)
	assert.Equal(t, 0, len(defaultPartition.GetApplications()))

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

	var req *http.Request
	req, err = http.NewRequest("GET", "/ws/v1/partitions", strings.NewReader(""))
	assert.NilError(t, err, "App Handler request failed")
	resp := &MockResponseWriter{}
	var partitionInfo []dao.PartitionInfo
	getPartitions(resp, req)
	err = json.Unmarshal(resp.outputBytes, &partitionInfo)
	assert.NilError(t, err, "failed to unmarshal applications dao response from response body: %s", string(resp.outputBytes))
	for _, part := range partitionInfo {
		if part.Name == partitionName {
			assert.Equal(t, part.Name, "[rm-123]default")
			assert.Equal(t, part.NodeSortingPolicy, "fair")
			assert.Equal(t, part.Applications["total"], 8)
			assert.Equal(t, part.Applications[objects.New.String()], 1)
			assert.Equal(t, part.Applications[objects.Accepted.String()], 1)
			assert.Equal(t, part.Applications[objects.Starting.String()], 1)
			assert.Equal(t, part.Applications[objects.Running.String()], 1)
			assert.Equal(t, part.Applications[objects.Completing.String()], 1)
			assert.Equal(t, part.Applications[objects.Rejected.String()], 1)
			assert.Equal(t, part.Applications[objects.Completed.String()], 1)
			assert.Equal(t, part.Applications[objects.Failed.String()], 1)
			assert.Equal(t, part.State, "Active")
		} else {
			assert.Equal(t, part.Name, "[rm-123]gpu")
			assert.Equal(t, part.NodeSortingPolicy, "fair")
			assert.Equal(t, part.Applications["total"], 0)
		}
	}
}

func TestCreateClusterConfig(t *testing.T) {
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

	configs.MockSchedulerConfigByData([]byte(configDefault))
	var err error
	schedulerContext, err = scheduler.NewClusterContext(rmID, policyGroup)
	assert.NilError(t, err, "Error when load clusterInfo from config")
	for _, test := range confTests {
		// No err check: new request always returns correctly
		//nolint: errcheck
		req, _ := http.NewRequest("POST", "/ws/v1/config?dry_run=1", strings.NewReader(test.content))
		rr := httptest.NewRecorder()
		mux := http.HandlerFunc(createClusterConfig)
		handler := loggingHandler(mux, "/ws/v1/config")
		handler.ServeHTTP(rr, req)
		var vcr dao.ValidateConfResponse
		err = json.Unmarshal(rr.Body.Bytes(), &vcr)
		assert.NilError(t, err, "failed to unmarshal ValidateConfResponse from response body")
		assert.Equal(t, vcr.Allowed, test.expectedResponse.Allowed, "allowed flag incorrect")
		assert.Equal(t, vcr.Reason, test.expectedResponse.Reason, "response text not as expected")
	}

	// When "dry_run" is not passed
	//nolint: errcheck
	req, err := http.NewRequest("POST", "/ws/v1/config", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux := http.HandlerFunc(createClusterConfig)
	handler := loggingHandler(mux, "/ws/v1/config")
	handler.ServeHTTP(rr, req)

	var errInfo dao.YAPIError
	err = json.Unmarshal(rr.Body.Bytes(), &errInfo)
	assert.NilError(t, err, "failed to unmarshal ValidateConfResponse from response body")
	assert.Equal(t, errInfo.Message, "Dry run param is missing. Please check the usage documentation\n", "JSON error message is incorrect")
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)

	// When "dry_run" value is invalid
	//nolint: errcheck
	req, err = http.NewRequest("POST", "/ws/v1/config?dry_run=0", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr = httptest.NewRecorder()
	mux = http.HandlerFunc(createClusterConfig)
	handler = loggingHandler(mux, "/ws/v1/config?dry_run=0")
	handler.ServeHTTP(rr, req)
	err = json.Unmarshal(rr.Body.Bytes(), &errInfo)
	assert.NilError(t, err, "failed to unmarshal ValidateConfResponse from response body")
	assert.Equal(t, errInfo.Message, "Invalid \"dry_run\" query param. Currently, only dry_run=1 is supported. Please check the usage documentation\n", "JSON error message is incorrect")
	assert.Equal(t, errInfo.StatusCode, http.StatusBadRequest)
}
