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

module github.com/apache/yunikorn-core/scripts/vetlock

go 1.25.0

// The analyser is taken from a standalone module rather than from gvisor: it is the gvisor
// checklocks analyser with the fixes that are still pending upstream, without which the
// annotations in this repository either panic the tool or are silently dropped, plus the
// additional analyses this repository runs. The module is a temporary home until a permanent one
// is sorted out.
require github.com/tigerquoll/vet-lock v0.8.0

require (
	golang.org/x/mod v0.34.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/tools v0.43.0 // indirect
)
