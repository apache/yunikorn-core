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

package tests

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

type RClient struct {
	BaseURL *url.URL
}

func (c *RClient) GetEvents() (*dao.EventRecordDAO, error) {
	req, err := c.newRequest("GET", "ws/v1/events/batch")
	if err != nil {
		return nil, err
	}
	var events *dao.EventRecordDAO
	_, err = c.do(req, &events)
	return events, err
}

func (c *RClient) newRequest(method, path string) (*http.Request, error) {
	rel := &url.URL{Path: path}
	wsUrl := &url.URL{
		Host:   "localhost:9080",
		Scheme: "http",
	}

	u := wsUrl.ResolveReference(rel)
	var buf io.ReadWriter
	req, err := http.NewRequest(method, u.String(), buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Golang_Spider_Bot/3.0")
	return req, nil
}

func (c *RClient) do(req *http.Request, v interface{}) (*http.Response, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(v)
	return resp, err
}
