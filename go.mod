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

go 1.25.0

require (
	github.com/apache/yunikorn-scheduler-interface v0.0.0-20260323095809-c1eb5e00dd66
	github.com/go-ldap/ldap/v3 v3.4.13
	github.com/google/btree v1.1.3
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/julienschmidt/httprouter v1.3.0
	github.com/looplab/fsm v1.0.3
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/client_model v0.6.2
	github.com/prometheus/common v0.67.5
	github.com/sasha-s/go-deadlock v0.3.9
	go.uber.org/zap v1.27.1
	go.yaml.in/yaml/v3 v3.0.4
	golang.org/x/exp v0.0.0-20260312153236-7ab1446f8b90
	golang.org/x/time v0.15.0
	google.golang.org/grpc v1.79.3
	gotest.tools/v3 v3.5.2
)

require (
	github.com/Azure/go-ntlmssp v0.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-asn1-ber/asn1-ber v1.5.8-0.20250403174932-29230038a667 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/petermattis/goid v0.0.0-20250813065127-a731cc31b4fe // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace (
	golang.org/x/crypto => golang.org/x/crypto v0.49.0
	golang.org/x/net => golang.org/x/net v0.52.0
	golang.org/x/sys => golang.org/x/sys v0.42.0
	golang.org/x/text => golang.org/x/text v0.35.0
)
