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
	"net/http"
	"time"
)

// InternalMetricHistory needs resetting between tests
// using defer to make sure it is cleaned up
func ResetIMHistory() {
	imHistory = nil
}

// Mock response writer that is used for testing the handlers
type MockResponseWriter struct {
	statusCode  int
	outputBytes []byte
	header      http.Header
}

func (trw *MockResponseWriter) Header() http.Header {
	if trw.header == nil {
		trw.header = make(http.Header)
	}
	return trw.header
}

func (trw *MockResponseWriter) Write(bytes []byte) (int, error) {
	trw.outputBytes = append(trw.outputBytes, bytes...)
	return len(bytes), nil
}

func (trw *MockResponseWriter) WriteHeader(statusCode int) {
	trw.statusCode = statusCode
}

func (trw *MockResponseWriter) SetWriteDeadline(deadline time.Time) error {
	return nil
}
