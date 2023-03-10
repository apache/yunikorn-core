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

package placement

import (
	"fmt"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/placement/types"
)

// A simple test rule to place an application based on a nil application.
// Testing only.
type testRule struct {
	basicRule
}

func (tr *testRule) getName() string {
	return types.Test
}

// Simple init for the test rule: allow everything as per a normal rule.
func (tr *testRule) initialise(conf configs.PlacementRule) error {
	tr.create = conf.Create
	tr.filter = newFilter(conf.Filter)
	var err = error(nil)
	if conf.Parent != nil {
		tr.parent, err = newRule(*conf.Parent)
	}
	return err
}

// Simple test rule that just checks the app passed in and returns fixed queue names.
func (tr *testRule) placeApplication(app *objects.Application, queueFn func(string) *objects.Queue) (string, error) {
	if app == nil {
		return "", fmt.Errorf("nil app passed in")
	}
	if queuePath := app.GetQueuePath(); queuePath != "" {
		return replaceDot(queuePath), nil
	}
	return "test", nil
}
