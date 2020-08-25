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
	"github.com/apache/incubator-yunikorn-core/pkg/common/commonevents"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/schedulerevent"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
	"net/http"
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics/history"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
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

const invalidConf = `
partitions:
  - name: default
    nodesortpolicy:
        type: invalid
    queues:
      - name: root
`

func TestValidateConf(t *testing.T) {
	tests := []struct {
		content          string
		expectedResponse dao.ValidateConfResponse
	}{
		{
			content: `
partitions:
  - name: default
    nodesortpolicy:
        type: fair
    queues:
      - name: root
`,
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
	for _, test := range tests {
		// No err check: new request always returns correctly
		//nolint: errcheck
		req, _ := http.NewRequest("POST", "", strings.NewReader(test.content))
		resp := &MockResponseWriter{}
		validateConf(resp, req)
		var vcr dao.ValidateConfResponse
		err := json.Unmarshal(resp.outputBytes, &vcr)
		assert.NilError(t, err, "failed to unmarshal ValidateConfResponse from response body")
		assert.Equal(t, vcr.Allowed, test.expectedResponse.Allowed)
		assert.Equal(t, vcr.Reason, test.expectedResponse.Reason)
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
	const rmID = "rm-123"
	const policyGroup = "default-policy-group"
	gClusterInfo = cache.NewClusterInfo()

	configs.MockSchedulerConfigByData([]byte(startConf))
	if _, err := cache.SetClusterInfoFromConfigFile(gClusterInfo, rmID, policyGroup); err != nil {
		t.Fatalf("Error when load clusterInfo from config %v", err)
	}
	// No err check: new request always returns correctly
	//nolint: errcheck
	req, _ := http.NewRequest("GET", "", nil)
	resp := &MockResponseWriter{}
	getClusterConfig(resp, req)
	// yaml unmarshal handles the checksum add the end automatically in this implementation
	conf := &configs.SchedulerConfig{}
	err := yaml.Unmarshal(resp.outputBytes, conf)
	assert.NilError(t, err, "failed to unmarshal config from response body")
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "fair", "node sort policy set incorrectly, not fair")

	// pull the checksum out of the response
	parts := strings.SplitAfter(string(resp.outputBytes), "sha256 checksum: ")
	assert.Equal(t, len(parts), 2, "checksum boundary not found")
	startConfSum := parts[1]

	// change the config
	configs.MockSchedulerConfigByData([]byte(updatedConf))
	if _, _, err = cache.UpdateClusterInfoFromConfigFile(gClusterInfo, rmID); err != nil {
		t.Fatalf("Error when updating clusterInfo from config %v", err)
	}
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
	const rmID = "rm-123"
	const policyGroup = "default-policy-group"
	gClusterInfo = cache.NewClusterInfo()

	configs.MockSchedulerConfigByData([]byte(startConf))
	if _, err := cache.SetClusterInfoFromConfigFile(gClusterInfo, rmID, policyGroup); err != nil {
		t.Fatalf("Error when load clusterInfo from config %v", err)
	}
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
	err := json.Unmarshal([]byte(parts[0]), conf)
	assert.NilError(t, err, "failed to unmarshal config from response body (json)")
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "fair", "node sort policy set incorrectly, not fair (json)")

	// change the config
	configs.MockSchedulerConfigByData([]byte(updatedConf))
	if _, _, err = cache.UpdateClusterInfoFromConfigFile(gClusterInfo, rmID); err != nil {
		t.Fatalf("Error when updating clusterInfo from config %v", err)
	}
	getClusterConfig(resp, req)
	parts = strings.SplitAfter(string(resp.outputBytes), "\n")
	assert.Equal(t, len(parts), 2, "checksum boundary not found (json changed)")
	assert.Assert(t, startConfSum != parts[1], "checksums did not change in json output: %s, %s", startConfSum, parts[1])
	err = json.Unmarshal([]byte(parts[0]), conf)
	assert.NilError(t, err, "failed to unmarshal config from response body (json, updated config)")
	assert.Equal(t, conf.Partitions[0].NodeSortPolicy.Type, "binpacking", "node sort policy not updated (json)")
}

func TestQueryParamInAppsHandler(t *testing.T) {
	const configDefault = `
partitions:
  - name: default
    queues:
      - name: root
        queues:
        - name: default
`
	rmID := "rm-123"
	policyGroup := "default-policy-group"
	clusterInfo := cache.NewClusterInfo()

	configs.MockSchedulerConfigByData([]byte(configDefault))
	if _, err := cache.SetClusterInfoFromConfigFile(clusterInfo, rmID, policyGroup); err != nil {
		t.Fatalf("Error when load clusterInfo from config %v", err)
	}
	assert.Equal(t, 1, len(clusterInfo.ListPartitions()))

	// Check default partition
	partitionName := "[" + rmID + "]default"
	defaultPartition := clusterInfo.GetPartition(partitionName)
	assert.Equal(t, 0, len(defaultPartition.GetApplications()))

	// add a new app
	appInfo := cache.CreateNewApplicationInfo("app-1", partitionName, "root.default")
	err := cache.AddAppToPartition(appInfo, defaultPartition)
	assert.NilError(t, err, "Failed to add Application to Partition.")
	assert.Equal(t, appInfo.GetApplicationState(), "New")
	assert.Equal(t, 1, len(defaultPartition.GetApplications()))

	NewWebApp(clusterInfo, nil)

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

type fakeConfigPlugin struct {
	generateError bool
}

func (f fakeConfigPlugin) UpdateConfigMap(args *si.ConfigMapArgs) (string, error) {
	if f.generateError {
		return "", fmt.Errorf("configMapError")
	}
	return startConf, nil
}

func TestSaveConfigMapNoError(t *testing.T) {
	plugins.RegisterSchedulerPlugin(&fakeConfigPlugin{generateError: false})
	oldConf, err := saveConfigmap(updatedConf)
	assert.NilError(t, err, "No error expected")
	assert.Equal(t, oldConf, startConf, " Wrong returned configuration")
}

func TestSaveConfigMapErrorExpected(t *testing.T) {
	plugins.RegisterSchedulerPlugin(&fakeConfigPlugin{generateError: true})
	oldConf, err := saveConfigmap(updatedConf)
	assert.Assert(t, err != nil, "Missing expected error")
	assert.Equal(t, oldConf, "", " Wrong returned configuration")
}

func TestBuildUpdateResponse(t *testing.T) {
	testCases := []struct {
		name    string
		success bool
		reason  string
	}{
		{"Success", true, ""},
		{"Failed", false, "CofigMap update Failed"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &MockResponseWriter{}
			buildUpdateResponse(tc.success, tc.reason, resp)
			var ucr dao.UpdateConfResponse
			err := json.Unmarshal(resp.outputBytes, &ucr)
			assert.NilError(t, err, "No error expected")
			assert.Equal(t, tc.reason, ucr.Reason, "Response reason should match")
			assert.Equal(t, tc.success, ucr.Success, "Response success flag should match")
		})
	}
}

type mockScheduler struct {
	clusterInfo *cache.ClusterInfo
}

func (m mockScheduler) HandleEvent(ev interface{}) {
	go handlePartitionUpdates(ev)
}

func handlePartitionUpdates(ev interface{}) {
	switch v := ev.(type) {
	case *schedulerevent.SchedulerUpdatePartitionsConfigEvent:
		v.ResultChannel <- &commonevents.Result{
			Succeeded: true,
		}
	case *schedulerevent.SchedulerDeletePartitionsConfigEvent:
		v.ResultChannel <- &commonevents.Result{
			Succeeded: true,
		}
	}
}

func TestUpdateConfig(t *testing.T) {
	plugins.RegisterSchedulerPlugin(&fakeConfigPlugin{generateError: false})
	gClusterInfo = cache.NewClusterInfo()
	scheduler := mockScheduler{gClusterInfo}
	gClusterInfo.EventHandlers.SchedulerEventHandler = scheduler
	configs.MockSchedulerConfigByData([]byte(startConf))
	if _, err := cache.SetClusterInfoFromConfigFile(gClusterInfo, "rmID", "default-policy-group"); err != nil {
		t.Fatalf("Error when load clusterInfo from config %v", err)
	}

	testCases := []struct {
		name             string
		newConf          string
		expectedResponse dao.UpdateConfResponse
	}{
		{"Valid conf", updatedConf, dao.UpdateConfResponse{
			Success: true,
			Reason:  "",
		}},
		{"Invalid conf", invalidConf, dao.UpdateConfResponse{
			Success: false,
			Reason:  "undefined policy: invalid",
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &MockResponseWriter{}
			req, _ := http.NewRequest("PUT", "", strings.NewReader(tc.newConf))
			updateConfig(resp, req)
			var ucr dao.UpdateConfResponse
			err := json.Unmarshal(resp.outputBytes, &ucr)
			assert.NilError(t, err, "No error expected")
			assert.DeepEqual(t, ucr, tc.expectedResponse)
		})
	}
}
