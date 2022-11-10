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
	github.com/apache/yunikorn-scheduler-interface v0.0.0-20221110194141-115505199f57
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/google/btree v1.0.1
	github.com/google/uuid v1.2.0
	github.com/gorilla/mux v1.7.3
	github.com/looplab/fsm v0.1.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v0.9.4
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.4.1
	github.com/prometheus/procfs v0.0.8 // indirect
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/uber/jaeger-lib v2.4.0+incompatible
	go.uber.org/atomic v1.5.1 // indirect
	go.uber.org/multierr v1.4.0 // indirect
	go.uber.org/zap v1.13.0
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/net v0.0.0-20220812174116-3211cb980234
	golang.org/x/tools v0.1.12 // indirect
	google.golang.org/grpc v1.26.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.2.8
	gotest.tools v2.2.0+incompatible
	honnef.co/go/tools v0.0.1-2020.1.3 // indirect
)
