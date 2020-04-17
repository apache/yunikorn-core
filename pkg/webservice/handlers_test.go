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
	"net/http"
	"strings"
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/metrics/history"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
)

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
			content: `
partitions:
  - name: default
    nodesortpolicy:
        type: invalid
    queues:
      - name: root
`,
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
		if err := json.Unmarshal(resp.outputBytes, &vcr); err != nil {
			t.Fatalf("failed to unmarshal ValidateConfResponse from response body: %s", string(resp.outputBytes))
		}
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
	if err := json.Unmarshal(resp.outputBytes, &appHist); err != nil {
		t.Fatalf("failed to unmarshal app history response from response body: %s", string(resp.outputBytes))
	}
	assert.Equal(t, resp.statusCode, 0, "app response should have no status")
	assert.Equal(t, len(appHist), 0, "empty response must have no records")

	// add new history records
	imHistory.Store(1, 0)
	imHistory.Store(2, 0)
	imHistory.Store(30, 0)
	resp = &MockResponseWriter{}
	getApplicationHistory(resp, req)
	if err := json.Unmarshal(resp.outputBytes, &appHist); err != nil {
		t.Fatalf("failed to unmarshal app history response from response body: %s", string(resp.outputBytes))
	}
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
	if err := json.Unmarshal(resp.outputBytes, &appHist); err != nil {
		t.Fatalf("failed to unmarshal app history response from response body: %s", string(resp.outputBytes))
	}
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
	if err := json.Unmarshal(resp.outputBytes, &contHist); err != nil {
		t.Fatalf("failed to unmarshal container history response from response body: %s", string(resp.outputBytes))
	}
	assert.Equal(t, resp.statusCode, 0, "container response should have no status")
	assert.Equal(t, len(contHist), 0, "empty response must have no records")

	// add new history records
	imHistory.Store(0, 1)
	imHistory.Store(0, 2)
	imHistory.Store(0, 30)
	resp = &MockResponseWriter{}
	getContainerHistory(resp, req)
	if err := json.Unmarshal(resp.outputBytes, &contHist); err != nil {
		t.Fatalf("failed to unmarshal container history response from response body: %s", string(resp.outputBytes))
	}
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
	if err := json.Unmarshal(resp.outputBytes, &contHist); err != nil {
		t.Fatalf("failed to unmarshal container history response from response body: %s", string(resp.outputBytes))
	}
	assert.Equal(t, resp.statusCode, 0, "container response should have no status")
	assert.Equal(t, len(contHist), 5, "incorrect number of records returned")
	assert.Equal(t, contHist[0].TotalContainers, "2", "metric 1 should be 1 apps and was not")
	assert.Equal(t, contHist[4].TotalContainers, "300", "metric 5 should be 300 apps and was not")
}
