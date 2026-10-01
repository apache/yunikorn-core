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

package ugm

import (
	"strconv"
	"strings"
	"sync"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/events/mock"
)

// TestWildCardConfigReloadDuringScheduling reloads the configuration while the scheduling hot path
// walks the queue hierarchy. Every walk that reaches a queue without a tracker creates one, and
// the creation used to read the wild card limits straight from the manager, which the reload
// replaces wholesale.
//
// Run with -race. On the unfixed code this reports a data race between the map read in
// getUserWildCardLimitsConfig and the map replacement in replaceLimitConfigs.
func TestWildCardConfigReloadDuringScheduling(t *testing.T) {
	setupUGM()
	manager := GetUserManager()
	user := security.UserGroup{User: "test-user", Groups: []string{"test-group"}}
	conf := createUpdateConfig(user.User, user.Groups[0])

	var wg sync.WaitGroup
	iterations := 200

	// the configuration reload: replaces the limit maps under the manager lock.
	// assert must not run here: it calls FailNow, which is only valid on the test goroutine.
	var reloadErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if err := manager.UpdateConfig(conf.Queues[0], "root"); err != nil {
				reloadErr = err
				return
			}
		}
	}()

	// the scheduling hot path: a queue path that has no tracker yet on every iteration, so the
	// walk has to create the queue trackers as it descends
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			queuePath := "root.parent.child" + strconv.Itoa(i)
			manager.Headroom(queuePath, "app-"+strconv.Itoa(i), user)
			manager.CanRunApp(queuePath, "app-"+strconv.Itoa(i), user)
		}
	}()

	wg.Wait()
	assert.NilError(t, reloadErr)
}

// TestWildCardConfigReloadDuringIncrease is the same race on the resource tracking path.
func TestWildCardConfigReloadDuringIncrease(t *testing.T) {
	setupUGM()
	manager := GetUserManager()
	user := security.UserGroup{User: "test-user", Groups: []string{"test-group"}}
	conf := createUpdateConfig(user.User, user.Groups[0])
	usage, err := resources.NewResourceFromConf(map[string]string{"memory": "10", "vcores": "10"})
	assert.NilError(t, err)

	var wg sync.WaitGroup
	iterations := 200

	// see TestWildCardConfigReloadDuringScheduling: the error is carried out to the test goroutine
	var reloadErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if err := manager.UpdateConfig(conf.Queues[0], "root"); err != nil {
				reloadErr = err
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			queuePath := "root.parent.increase" + strconv.Itoa(i)
			manager.IncreaseTrackedResource(queuePath, "app-"+strconv.Itoa(i), usage, user)
		}
	}()

	wg.Wait()
	assert.NilError(t, reloadErr)
}

// TestWildCardLimitsResolvedByCaller pins down the mechanism that keeps a single hierarchy walk
// consistent: a queue tracker takes the wild card limits handed to it by the caller and never
// consults the manager itself. The manager is deliberately loaded with a different configuration,
// so any level that still read the global singleton would show the manager's values instead.
func TestWildCardLimitsResolvedByCaller(t *testing.T) {
	setupUGM()
	manager := GetUserManager()
	defer manager.ClearConfigLimits()

	managerRes := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 111})
	managerLimit := &LimitConfig{maxApplications: 1, maxResources: managerRes}
	manager.userWildCardLimitsConfig = map[string]*LimitConfig{
		"root":               managerLimit,
		"root.parent":        managerLimit,
		"root.parent.child1": managerLimit,
	}

	callerRes := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 222})
	callerLimit := &LimitConfig{maxApplications: 2, maxResources: callerRes}
	callerLimits := map[string]*LimitConfig{
		"root":               callerLimit,
		"root.parent":        callerLimit,
		"root.parent.child1": callerLimit,
	}

	userTracker := newUserTracker("caller-user", newUGMEvents(mock.NewEventSystemDisabled()), callerLimits)
	// the walk creates parent and child1 on the way down, each of them a separate newQueueTracker call
	assert.Assert(t, userTracker.canRunApp(strings.Split(path1, configs.DOT), TestApp1, callerLimits))

	parent := userTracker.queueTracker.childQueueTrackers["parent"]
	child := parent.childQueueTrackers["child1"]
	for _, qt := range []*QueueTracker{userTracker.queueTracker, parent, child} {
		assert.Assert(t, qt.useWildCard, qt.queuePath)
		assert.Equal(t, uint64(2), qt.maxRunningApps, qt.queuePath)
		assert.Assert(t, resources.Equals(callerRes, qt.maxResources), qt.queuePath)
	}
}
