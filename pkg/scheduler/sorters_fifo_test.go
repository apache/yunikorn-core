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

package scheduler

import (
	"fmt"
	"testing"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/stretchr/testify/assert"
)

func TestBasicFifoPolicy(t *testing.T) {
	partition, err := newTestPartition()
	assert.NoError(t, err, "create partition create failed")
	root, err := createRootQueue(nil)
	assert.NoError(t, err, "create root queue failed")
	testQ, err := createManagedQueue(root, "test-queue", false, nil)
	assert.NoError(t, err, "create test-queue queue failed")

	testCases := []struct {
		appInitStates  []string
		expectedStates  []string
	}{
		{[]string{"New", "New", "New", "New", "New"},
			[]string{"New", "New", "New", "New", "New"}},
		{[]string{"Running", "Accepted", "New", "New", "New"},
			[]string{"Running", "Running", "New", "New", "New"}},
		{[]string{"Accepted", "Accepted", "Accepted", "Accepted", "Accepted"},
			[]string{"Running", "Running", "Running", "Running", "Running"}},
		{[]string{"Accepted", "Accepted", "Accepted", "Accepted", "Accepted"},
			[]string{"Running", "Running", "Running", "Running", "Running"}},
		{[]string{"Rejected", "Accepted", "New", "New", "New"},
			[]string{"Rejected", "Running", "New", "New", "New"}},
	}
	for _, tc := range testCases {
		apps := createApps(partition, testQ, tc.appInitStates)
		assert.NoError(t, verifyAppState(apps, tc.appInitStates))
		b := &BasicFifoPolicy{}
		b.Apply(apps)
		assert.NoError(t, verifyAppState(apps, tc.expectedStates))
	}
}

func TestAppStateAwareFifoPolicy(t *testing.T) {
	partition, err := newTestPartition()
	assert.NoError(t, err, "create partition create failed")
	root, err := createRootQueue(nil)
	assert.NoError(t, err, "create root queue failed")
	testQ, err := createManagedQueue(root, "test-queue", false, nil)
	assert.NoError(t, err, "create test-queue queue failed")

	testCases := []struct {
		appInitStates  []string
		expectedStates  []string
	}{
		{[]string{"New", "New", "New", "New", "New"},
			[]string{"New", "New", "New", "New", "New"}},
			// first app accepted, it can run; rest apps should remain accepted
		{[]string{"Accepted", "Accepted", "Accepted", "Accepted", "Accepted"},
			[]string{"Scheduable", "Accepted", "Accepted", "Accepted", "Accepted"}},
			// first app scheduable not running, rest apps should remain accepted
		{[]string{"Scheduable", "Accepted", "Accepted", "Accepted", "Accepted"},
			[]string{"Scheduable", "Accepted", "Accepted", "Accepted", "Accepted"}},
			// first app rejected, second app gets chance to be scheduled
		{[]string{"Rejected", "Accepted", "Accepted", "Accepted", "Accepted"},
			[]string{"Rejected", "Scheduable", "Accepted", "Accepted", "Accepted"}},
			// first 3 app completed, second app gets chance to be scheduled
		{[]string{"Completed", "Rejected", "Completed", "Accepted", "Accepted"},
			[]string{"Completed", "Rejected", "Completed", "Scheduable", "Accepted"}},
			// if previous app is scheduable but not running, apps keep Accepted
		{[]string{"Completed", "Rejected", "Scheduable", "Accepted", "Accepted"},
			[]string{"Completed", "Rejected", "Scheduable", "Accepted", "Accepted"}},
			// if previous app is running, app can be scheduled
		{[]string{"Completed", "Rejected", "Running", "Accepted", "Accepted"},
			[]string{"Completed", "Rejected", "Running", "Scheduable", "Accepted"}},
			// all running
		{[]string{"Running", "Running", "Running", "Running", "Running"},
			[]string{"Running", "Running", "Running", "Running", "Running"}},
	}
	for _, tc := range testCases {
		apps := createApps(partition, testQ, tc.appInitStates)
		assert.NoError(t, verifyAppState(apps, tc.appInitStates))
		b := &AppStateAwareFifoPolicy{}
		b.Apply(apps)
		assert.NoError(t, verifyAppState(apps, tc.expectedStates))
	}
}

func createApps(partition *partitionSchedulingContext, queue *SchedulingQueue, appStates []string) []*SchedulingApplication {
	var apps []*SchedulingApplication
	for idx, state := range appStates {
		appID := fmt.Sprintf("app-%d", idx)
		appInfo := cache.NewApplicationInfo(appID, "default", "root.test-queue", security.UserGroup{}, nil)
		app := newSchedulingApplication(appInfo)
		app.ApplicationInfo.SetApplicationState(state)
		_ = partition.addSchedulingApplication(app)
		queue.addSchedulingApplication(app)
		apps = append(apps, app)
	}
	return apps
}

func verifyAppState(apps []*SchedulingApplication, states []string) error {
	for idx, app := range apps {
		if app.ApplicationInfo.GetApplicationState() != states[idx] {
			return fmt.Errorf("expecting %s at position %d, but got %s",
				states[idx], idx, app.ApplicationInfo.GetApplicationState())
		}
	}
	return nil
}