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

go 1.20

require (
	github.com/apache/yunikorn-scheduler-interface v0.0.0-20231020041412-6f80d179257c
	github.com/google/btree v1.1.2
	github.com/google/go-cmp v0.5.9
	github.com/google/uuid v1.3.0
	github.com/julienschmidt/httprouter v1.3.0
	github.com/looplab/fsm v1.0.1
	github.com/opentracing/opentracing-go v1.2.0
	github.com/prometheus/client_golang v1.13.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.37.0
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/uber/jaeger-lib v2.4.0+incompatible
	go.uber.org/zap v1.24.0
	golang.org/x/net v0.17.0
	google.golang.org/grpc v1.56.0
	gopkg.in/yaml.v3 v3.0.1
	gotest.tools/v3 v3.0.3
)

require (
	github.com/HdrHistogram/hdrhistogram-go v1.0.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
)

replace (
	golang.org/x/crypto => golang.org/x/crypto v0.13.0
	golang.org/x/lint => golang.org/x/lint v0.0.0-20210508222113-6edffad5e616
	golang.org/x/net => golang.org/x/net v0.17.0
	golang.org/x/sys => golang.org/x/sys v0.13.0
	golang.org/x/text => golang.org/x/text v0.13.0
	golang.org/x/tools => golang.org/x/tools v0.13.0
)
