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
	"fmt"
	"io"
	"net/http"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/metrics/history"
	"github.com/apache/yunikorn-core/pkg/scheduler"
)

const base = "http://localhost:9080"

func Test_RedirectDebugHandler(t *testing.T) {
	defer ResetIMHistory()
	s := NewWebApp(&scheduler.ClusterContext{}, history.NewInternalMetricsHistory(5))
	s.StartWebApp()
	defer func(s *WebService) {
		err := s.StopWebApp()
		if err != nil {
			t.Fatal("failed to stop webapp")
		}
	}(s)
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
			client := &http.Client{
				CheckRedirect: func(req *http.Request, via []*http.Request) error {
					if req.URL.Path != tt.redirect {
						return fmt.Errorf("expected redirect to '%s' got '%s'", tt.redirect, req.URL.Path)
					}
					return nil
				},
			}
			resp, err := client.Get(base + tt.reqURL)
			assert.NilError(t, err, "unexpected error returned")
			_ = resp.Body.Close() // not interested in the error
			assert.Equal(t, resp.StatusCode, http.StatusOK, "expected OK after redirect")
		})
	}
}

func Test_RouterHandling(t *testing.T) {
	s := NewWebApp(&scheduler.ClusterContext{}, nil)
	s.StartWebApp()
	defer func(s *WebService) {
		err := s.StopWebApp()
		if err != nil {
			t.Fatal("failed to stop webapp")
		}
	}(s)
	client := &http.Client{}
	// unsupported POST
	resp, err := client.Post(base+"/ws/v1/clusters", "application/json; charset=UTF-8", nil)
	assert.NilError(t, err, "unexpected error returned")
	assert.Equal(t, resp.StatusCode, http.StatusMethodNotAllowed, "expected method not allowed")
	var body []byte
	body, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close() // not interested in the error
	assert.NilError(t, err, "unexpected error reading body")
	assert.Assert(t, body != nil, "expected body with status text")
	resp, err = client.Head(base + "/ws/v1/clusters")
	assert.NilError(t, err, "unexpected error returned")
	body, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.NilError(t, err, "unexpected error reading body")
	assert.Assert(t, body != nil, "expected body with status text")
	assert.Equal(t, resp.StatusCode, http.StatusMethodNotAllowed, "expected method not allowed")
	// get with trailing slash
	resp, err = client.Get(base + "/ws/v1/clusters/")
	assert.NilError(t, err, "unexpected error returned")
	_ = resp.Body.Close()
	assert.Equal(t, resp.StatusCode, http.StatusOK, "expected OK")
	// get with case difference
	resp, err = client.Get(base + "/ws/v1/CLUSTERS")
	assert.NilError(t, err, "unexpected error returned")
	_ = resp.Body.Close()
	assert.Equal(t, resp.StatusCode, http.StatusOK, "expected OK")
}

func Test_HeaderChecks(t *testing.T) {
	s := NewWebApp(&scheduler.ClusterContext{}, nil)
	s.StartWebApp()
	defer func(s *WebService) {
		err := s.StopWebApp()
		if err != nil {
			t.Fatal("failed to stop webapp")
		}
	}(s)
	client := http.DefaultClient
	tests := []struct {
		name     string
		reqURL   string
		method   string
		expected string
	}{
		{"get options", "/ws/v1/clusters", http.MethodOptions, "GET, OPTIONS"},
		{"get", "/ws/v1/clusters", http.MethodGet, "GET, OPTIONS"},
		{"post options", "/ws/v1/validate-conf", http.MethodOptions, "OPTIONS, POST"},
		{"post", "/ws/v1/validate-conf", http.MethodPost, "OPTIONS, POST"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, base+tt.reqURL, nil)
			assert.NilError(t, err, "unexpected error creating request")
			var resp *http.Response
			resp, err = client.Do(req)
			assert.NilError(t, err, "unexpected error executing request")
			assert.Equal(t, resp.StatusCode, http.StatusOK, "expected OK")
			switch tt.method {
			case http.MethodGet, http.MethodPost:
				assert.Equal(t, resp.Header.Get("Access-Control-Allow-Methods"), tt.expected, "wrong methods returned")
			case http.MethodOptions:
				// OPTIONS requests are handled by default via httpdrouter, not defined in the routes
				assert.Equal(t, resp.Header.Get("Allow"), tt.expected, "expected only get and options to be returned")
			}
			var body []byte
			body, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			assert.NilError(t, err, "unexpected error reading body")
			assert.Assert(t, body != nil, "expected body with status text")
		})
	}
}
