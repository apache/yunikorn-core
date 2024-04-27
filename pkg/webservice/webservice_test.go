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
	"compress/gzip"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler"
)

func TestCompressionWithDummyRoute(t *testing.T) {
	m := NewWebApp(scheduler.NewScheduler().GetClusterContext(), nil)
	// dummy route and corresponding handler
	testRoute := route{
		"testHelloWord",
		"GET",
		"/ws/v1/helloWorld",
		getHelloWorld,
	}
	router := httprouter.New()
	testHandler := gzipHandler(loggingHandler(testRoute.HandlerFunc, testRoute.Name))
	router.Handler(testRoute.Method, testRoute.Pattern, testHandler)

	// start simulation server
	m.httpServer = &http.Server{Addr: ":9080", Handler: router, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		httpError := m.httpServer.ListenAndServe()
		if httpError != nil {
			log.Log(log.REST).Error("HTTP serving error",
				zap.Error(httpError))
		}
	}()
	defer func() {
		err := m.StopWebApp()
		assert.NilError(t, err, "Error when closing webapp service.")
	}()

	err := common.WaitFor(500*time.Millisecond, 5*time.Second, func() bool {
		conn, connErr := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", "9080"), time.Second)
		if connErr == nil {
			defer conn.Close()
			return true
		}
		return false
	})
	assert.NilError(t, err, "Ｗeb app failed to start in 2 seconds.")

	u := &url.URL{
		Host:   "localhost:9080",
		Scheme: "http",
		Path:   "/ws/v1/helloWorld",
	}

	// request without gzip compression
	var buf io.ReadWriter
	req, err := http.NewRequest("GET", u.String(), buf)
	assert.NilError(t, err, "Create new http request failed.")
	req.Header.Set("Accept", "application/json")

	// prevent http.DefaultClient from automatically adding gzip header
	req.Header.Set("Accept-Encoding", "deflate")
	resp, err := http.DefaultClient.Do(req)
	assert.NilError(t, err, "Request failed to send.")
	defer resp.Body.Close()
	byteArr, err := io.ReadAll(resp.Body)
	assert.NilError(t, err, "Failed when reading data.")
	var respMsg map[string]string
	err = json.Unmarshal(byteArr, &respMsg)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, respMsg["data"], "hello world")

	// request with gzip compression enabled
	req.Header.Set("Accept-Encoding", "gzip")
	resp2, err := http.DefaultClient.Do(req)
	assert.NilError(t, err, "Request failed to send.")
	defer resp2.Body.Close()
	gzipReader, err := gzip.NewReader(resp2.Body)
	assert.NilError(t, err, "Failed to create gzip reader.")
	byteArr2, err := io.ReadAll(gzipReader)
	assert.NilError(t, err, "Failed when reading data.")
	var respMsg2 map[string]string
	err = json.Unmarshal(byteArr2, &respMsg2)
	assert.NilError(t, err, unmarshalError)
	assert.Equal(t, respMsg2["data"], "hello world")
	defer gzipReader.Close()
}

func getHelloWorld(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	result := map[string]string{
		"data": "hello world",
	}

	if err := json.NewEncoder(w).Encode(result); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}
