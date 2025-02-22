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

go 1.23

require (
	github.com/apache/yunikorn-scheduler-interface v0.0.0-20241016105739-f0e241aa0146
	github.com/google/btree v1.1.2
	github.com/google/go-cmp v0.6.0
	github.com/google/uuid v1.6.0
	github.com/julienschmidt/httprouter v1.3.0
	github.com/looplab/fsm v1.0.1
	github.com/prometheus/client_golang v1.18.0
	github.com/prometheus/client_model v0.5.0
	github.com/prometheus/common v0.45.0
	github.com/sasha-s/go-deadlock v0.3.5
	go.uber.org/zap v1.26.0
	golang.org/x/exp v0.0.0-20240409090435-93d18d7e34b8
	golang.org/x/net v0.25.0
	golang.org/x/time v0.5.0
	google.golang.org/grpc v1.65.0
	gopkg.in/yaml.v3 v3.0.1
	gotest.tools/v3 v3.5.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/matttproud/golang_protobuf_extensions/v2 v2.0.0 // indirect
	github.com/petermattis/goid v0.0.0-20240813172612-4fcff4a6cae7 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240528184218-531527333157 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)

replace (
	github.com/petermattis/goid => github.com/petermattis/goid v0.0.0-20240813172612-4fcff4a6cae7
	golang.org/x/crypto => golang.org/x/crypto v0.31.0
	golang.org/x/net => golang.org/x/net v0.33.0
	golang.org/x/sys => golang.org/x/sys v0.24.0
	golang.org/x/text => golang.org/x/text v0.17.0
	golang.org/x/tools => golang.org/x/tools v0.24.0
)
