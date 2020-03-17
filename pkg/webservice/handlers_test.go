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
    "github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
    "gotest.tools/assert"
    "net/http"
    "strings"
    "testing"
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
                Error:   "",
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
                Error:   "undefined policy: invalid",
            },
        },
    }
    for _, test := range tests {
        req, _ := http.NewRequest("POST", "",
            strings.NewReader(test.content))
        resp := &TestResponseWriter{}
        ValidateConf(resp, req)
        var vcr dao.ValidateConfResponse
        if err := json.Unmarshal(resp.outputBytes, &vcr); err != nil {
            t.Errorf("failed to unmarshal ValidateConfResponse from response body: %s", string(resp.outputBytes))
        } else {
            assert.Equal(t, vcr.Allowed, test.expectedResponse.Allowed)
            assert.Equal(t, vcr.Error, test.expectedResponse.Error)
        }
    }
}

type TestResponseWriter struct {
    outputBytes []byte
    header http.Header
}

func (trw *TestResponseWriter) Header() http.Header {
    if trw.header == nil {
        trw.header = make(http.Header)
    }
    return trw.header
}

func (trw *TestResponseWriter) Write(bytes []byte) (int, error) {
    trw.outputBytes = bytes
    return len(bytes), nil
}

func (trw *TestResponseWriter) WriteHeader(statusCode int) {
}
