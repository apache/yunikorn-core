//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

module github.com/apache/yunikorn-core

go 1.16

require (
	github.com/HdrHistogram/hdrhistogram-go v1.0.1 // indirect
	github.com/apache/yunikorn-scheduler-interface v0.0.0-20230328133840-b29b20a234e0
	github.com/google/btree v1.1.2
	github.com/google/uuid v1.2.0
	github.com/gorilla/mux v1.7.3
	github.com/looplab/fsm v1.0.1
	github.com/opentracing/opentracing-go v1.2.0
	github.com/prometheus/client_golang v1.13.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.37.0
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/uber/jaeger-lib v2.4.0+incompatible
	go.uber.org/zap v1.24.0
	golang.org/x/net v0.8.0
	google.golang.org/grpc v1.31.0
	gopkg.in/yaml.v2 v2.4.0
	gotest.tools/v3 v3.0.3
)

replace (
	golang.org/x/crypto => golang.org/x/crypto v0.7.0
	golang.org/x/lint => golang.org/x/lint v0.0.0-20210508222113-6edffad5e616
	golang.org/x/net => golang.org/x/net v0.8.0
	golang.org/x/sys => golang.org/x/sys v0.6.0
	golang.org/x/text => golang.org/x/text v0.8.0
	golang.org/x/tools => golang.org/x/tools v0.7.0
)
