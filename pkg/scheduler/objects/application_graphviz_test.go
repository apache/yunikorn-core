//go:build graphviz
// +build graphviz

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

package objects

import (
	"os"
	"testing"

	"github.com/looplab/fsm"
	"gotest.tools/v3/assert"
)

func TestApplicationFsmGraph(t *testing.T) {
	graph := fsm.Visualize(NewAppState())

	err := os.MkdirAll("../../../build/fsm", 0755)
	assert.NilError(t, err, "Creating output dir failed")
	os.WriteFile("../../../build/fsm/application-state.dot", []byte(graph), 0644)
	assert.NilError(t, err, "Writing graph failed")
}
