//go:build tools

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

// Package tools pins the build tools that are not imported by the code base. This is a
// module of its own so that the tool dependencies never end up in the go.mod of the
// scheduler itself. The blank import is what keeps the version in go.mod and go.sum.
package tools

import (
	_ "github.com/tigerquoll/vet-lock/cmd/vet-lock"
)
