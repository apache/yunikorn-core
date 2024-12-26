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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
)

const (
	queuePathParent = "root.parent"
	queuePathLeaf   = "root.parent.leaf"
)

var (
	largeResource = map[string]string{
		"memory": "100",
		"vcores": "100",
	}
	mediumResource = map[string]string{
		"memory": "50",
		"vcores": "50",
	}
	mediumResourceWithMemOnly = map[string]string{
		"memory": "50",
	}
	mediumResourceWithVcoresOnly = map[string]string{
		"vcores": "50",
	}
	tinyResource = map[string]string{
		"memory": "25",
		"vcores": "25",
	}
	nilResource = map[string]string{}
)

func TestUserManagerOnceInitialization(t *testing.T) {
	manager := GetUserManager()
	assert.Equal(t, manager, manager)
	manager1 := GetUserManager()
	assert.Equal(t, manager, manager1)
}

func TestGetGroup(t *testing.T) {
	setupUGM()
	user := security.UserGroup{User: "test", Groups: []string{"test", "test1"}}
	manager := GetUserManager()

	// create config with limits for "test" group and ensure picked up group is "test"
	conf := createUpdateConfig(user.User, user.Groups[0])
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	group := manager.ensureGroup(user, "root.parent.leaf")
	assert.Equal(t, group, "test")

	// Even though ordering of groups has been changed in UserGroup obj, still "test" should be picked up
	user1 := security.UserGroup{User: "test", Groups: []string{"test1", "test"}}
	group = manager.ensureGroup(user1, "root.parent")
	assert.Equal(t, group, "test")

	// clear all configs and ensure there is no tracking for "test" group as there is no matching
	conf = createConfigWithoutLimits()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	usage1, err := resources.NewResourceFromConf(map[string]string{"memory": "5", "vcores": "5"})
	assert.NilError(t, err)

	manager.IncreaseTrackedResource("root.parent.leaf", TestApp1, usage1, user1)
	assert.Equal(t, len(manager.userTrackers), 1)
	assert.Equal(t, len(manager.groupTrackers), 0)

	group = manager.ensureGroup(user1, "root.parent.leaf")
	assert.Equal(t, group, "")
	group = manager.ensureGroup(user1, "root.parent")
	assert.Equal(t, group, "")
	group = manager.ensureGroup(user1, "root")
	assert.Equal(t, group, "")

	// create config with groups as "test_root" for root queue path and "test_parent" for root.parent queue path but no group defined for
	// "root.parent.leaf" queue path. ensure immediate parent queue path "root.parent" has been used for group matching and picked.
	// Same group would be used even for all queues coming under "root.parent.*" hierarchy.
	// But for queues under "root.parent1.*" hierarchy (including root.parent1 queue) , "test_root" would be used as there is no group defined for that hierarchical queue path
	user = security.UserGroup{User: "test1", Groups: []string{"test", "test_root", "test_parent"}}
	conf = createConfigWithDifferentGroups(user.User, user.Groups[0], "memory", "10", 50, 5)
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
	group = manager.ensureGroup(user, "root.parent.leaf")
	assert.Equal(t, group, "test_parent")
	group = manager.ensureGroup(user, "root.parent")
	assert.Equal(t, group, "test_parent")
	group = manager.ensureGroup(user, "root")
	assert.Equal(t, group, "test_root")
	group = manager.ensureGroup(user, "root.parent1.leaf1")
	assert.Equal(t, group, "test_root")

	// create config with groups as "test_root" for root queue path and "test_parent" for root.parent queue path but no group defined for
	// "root.parent.leaf" queue path. Passing just "root" as queue path returns "test_root" as searching starts from lowest level queue in entire queue path
	conf = createConfigWithDifferentGroups(user.User, user.Groups[0], "memory", "10", 50, 5)
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
	group = manager.ensureGroup(user, "root")
	assert.Equal(t, group, "test_root")

	// create config with groups as "test_root" for root queue path and "test_parent" for root.parent queue path but no group defined for
	// "root.parent.leaf" queue path. since "test_parent" group is not defined in UserGroup obj, "test_root" would be matched and picked.
	user = security.UserGroup{User: "test1", Groups: []string{"test", "test_root"}}
	conf = createConfigWithDifferentGroups(user.User, user.Groups[0], "memory", "10", 50, 5)
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
	group = manager.ensureGroup(user, "root.parent")
	assert.Equal(t, group, "test_root")
	group = manager.ensureGroup(user, "root")
	assert.Equal(t, group, "test_root")

	// do some activities with different users - user "test1" and "test2" belongs to "test_root" group
	// ensure user "test1" and "test2" and its group linkage is intact through appGroupTrackers map
	manager.IncreaseTrackedResource("root.parent.leaf", TestApp1, usage1, user)
	assert.Equal(t, len(manager.userTrackers), 2)

	user2 := security.UserGroup{User: "test2", Groups: []string{"test", "test_root"}}
	manager.IncreaseTrackedResource("root.parent.leaf", TestApp2, usage1, user2)

	userGroupTrackersAssertsMap := map[string]string{}
	userGroupTrackersAssertsMap[user.User] = TestApp1
	userGroupTrackersAssertsMap[user2.User] = TestApp2
	for u, a := range userGroupTrackersAssertsMap {
		ut := manager.GetUserTracker(u)
		actualGT := ut.appGroupTrackers[a]
		expectedGT := manager.GetGroupTracker("test_root")
		assert.Equal(t, manager.GetGroupTracker("test_root") != nil, true)
		assert.Equal(t, actualGT, expectedGT)
		assert.Equal(t, actualGT.groupName, "test_root")
	}

	assert.Equal(t, len(manager.userTrackers), 3)
	assert.Equal(t, len(manager.groupTrackers), 2)

	user3 := security.UserGroup{User: "test3", Groups: []string{"test"}}
	manager.IncreaseTrackedResource("root.parent.leaf", TestApp2, usage1, user3)
	assert.Equal(t, len(manager.userTrackers), 4)
	ut := manager.GetUserTracker(user3.User)
	actualGT := ut.appGroupTrackers[TestApp2]
	if actualGT != nil {
		t.Errorf("group tracker should be nil as there is no group found for user %s", user3.User)
	}

	// create config without any limit settings for groups and
	// since earlier group (test_root) limit settings has been cleared, ensure earlier user and group linkage through appGroupTrackers map has been removed
	// and cleared as necessary
	conf = createConfigWithDifferentGroups(user2.User, "", "memory", "10", 50, 5)
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
	assert.Equal(t, len(manager.userTrackers), 4)
	assert.Equal(t, len(manager.groupTrackers), 0)

	ut = manager.GetUserTracker(user.User)
	assert.Equal(t, ut.appGroupTrackers[TestApp1] == nil, true)
	assert.Equal(t, manager.GetGroupTracker("test_root") == nil, true)
}

func TestAddRemoveUserAndGroups(t *testing.T) {
	// Queue setup:
	// root->parent->child1
	// root->parent->child2
	user := security.UserGroup{User: "test", Groups: []string{"test"}}
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "5M", "vcore": "5"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}
	manager := GetUserManager()
	manager.ClearUserTrackers()
	manager.ClearGroupTrackers()

	manager.IncreaseTrackedResource("", "", usage1, user)
	manager.IncreaseTrackedResource(queuePath1, TestApp1, usage1, user)

	groupTrackers := manager.GetGroupTrackers()
	assert.Equal(t, len(groupTrackers), 0)
	assertUGM(t, user, usage1, 1)
	assert.Equal(t, user.User, manager.GetUserTracker(user.User).userName)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) == nil, true)

	manager.IncreaseTrackedResource(queuePath1, TestApp1, usage1, user)
	assertUGM(t, user, resources.Multiply(usage1, 2), 1)

	user1 := security.UserGroup{User: "test1", Groups: []string{"test1"}}
	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	manager.IncreaseTrackedResource(queuePath2, TestApp2, usage2, user1)
	assertUGM(t, user1, usage2, 2)
	assert.Equal(t, user.User, manager.GetUserTracker(user.User).userName)
	assert.Equal(t, user1.User, manager.GetUserTracker(user1.User).userName)

	assert.Equal(t, true, manager.GetUserTracker(user.User).queueTracker.isQueuePathTrackedCompletely(strings.Split(queuePath1, configs.DOT)))
	assert.Equal(t, true, manager.GetUserTracker(user1.User).queueTracker.isQueuePathTrackedCompletely(strings.Split(queuePath2, configs.DOT)))
	assert.Equal(t, false, manager.GetUserTracker(user1.User).queueTracker.isQueuePathTrackedCompletely(strings.Split(queuePath1, configs.DOT)))
	assert.Equal(t, false, manager.GetUserTracker(user.User).queueTracker.isQueuePathTrackedCompletely(strings.Split(queuePath2, configs.DOT)))
	assert.Equal(t, false, manager.GetUserTracker(user.User).queueTracker.isQueuePathTrackedCompletely(strings.Split(queuePath3, configs.DOT)))
	assert.Equal(t, false, manager.GetUserTracker(user.User).queueTracker.isQueuePathTrackedCompletely(strings.Split(queuePath4, configs.DOT)))

	assert.Equal(t, true, manager.GetUserTracker(user.Groups[0]).queueTracker.isQueuePathTrackedCompletely(strings.Split(queuePath1, configs.DOT)))
	assert.Equal(t, true, manager.GetUserTracker(user1.Groups[0]).queueTracker.isQueuePathTrackedCompletely(strings.Split(queuePath2, configs.DOT)))
	assert.Equal(t, false, manager.GetUserTracker(user1.Groups[0]).queueTracker.isQueuePathTrackedCompletely(strings.Split(queuePath1, configs.DOT)))
	assert.Equal(t, false, manager.GetUserTracker(user.Groups[0]).queueTracker.isQueuePathTrackedCompletely(strings.Split(queuePath2, configs.DOT)))
	assert.Equal(t, false, manager.GetUserTracker(user.Groups[0]).queueTracker.isQueuePathTrackedCompletely(strings.Split(queuePath3, configs.DOT)))
	assert.Equal(t, false, manager.GetUserTracker(user.Groups[0]).queueTracker.isQueuePathTrackedCompletely(strings.Split(queuePath4, configs.DOT)))

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "5M", "vcore": "5"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}

	manager.DecreaseTrackedResource("", "", usage1, user, false)
	manager.DecreaseTrackedResource(queuePath1, TestApp1, usage3, user, false)
	assertUGM(t, user, usage1, 2)

	manager.DecreaseTrackedResource(queuePath1, TestApp1, usage3, user, true)
	assert.Equal(t, 1, len(manager.GetUserTrackers()), "userTrackers count should be 1")
	assert.Equal(t, 0, len(manager.GetGroupTrackers()), "groupTrackers count should be 0")

	manager.DecreaseTrackedResource(queuePath2, TestApp2, usage2, user1, true)
	assert.Equal(t, 0, len(manager.GetUserTrackers()), "userTrackers count should be 0")
	assert.Equal(t, 0, len(manager.GetGroupTrackers()), "groupTrackers count should be 0")

	assert.Assert(t, manager.GetUserTracker(user.User) == nil)
	assert.Assert(t, manager.GetGroupTracker(user.Groups[0]) == nil)
}

func TestUpdateConfig(t *testing.T) {
	setupUGM()
	// Queue setup:
	// root->parent
	user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	manager := GetUserManager()

	expectedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "50", "vcores": "50"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}

	conf := createConfig(user.User, user.Groups[0], "memory", "50", 50, 5)
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	usage, err := resources.NewResourceFromConf(map[string]string{"memory": "10", "vcores": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
	}
	assertMaxLimits(t, user, expectedResource, 5)

	for i := 1; i <= 5; i++ {
		manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
	}

	// configure max resource for root.parent lesser than current resource usage. should be allowed to set but user cannot be allowed to do any activity further
	conf = createConfig(user.User, user.Groups[0], "memory", "50", 40, 4)
	err = manager.UpdateConfig(conf.Queues[0], "root")
	assert.NilError(t, err)
	headroom := manager.Headroom(queuePath1, TestApp1, user)
	assert.Equal(t, headroom.FitInMaxUndef(usage), false)

	// configure max resource for root and parent to allow one more application to run
	conf = createConfig(user.User, user.Groups[0], "memory", "50", 60, 6)
	err = manager.UpdateConfig(conf.Queues[0], "root")
	assert.NilError(t, err, "unable to set the limit for user user1 because current resource usage is greater than config max resource for root.parent")

	manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)

	// configure max resource for root lesser than current resource usage. should be allowed to set but user cannot be allowed to do any activity further
	conf = createConfig(user.User, user.Groups[0], "memory", "50", 10, 10)
	err = manager.UpdateConfig(conf.Queues[0], "root")
	assert.NilError(t, err)
	headroom = manager.Headroom(queuePath1, TestApp1, user)
	assert.Equal(t, headroom.FitInMaxUndef(usage), false)
}

func TestUseWildCard(t *testing.T) {
	setupUGM()
	manager := GetUserManager()
	user1 := security.UserGroup{User: "user1", Groups: []string{"group1"}}

	expectedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "50", "vcores": "50"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}
	usage, err := resources.NewResourceFromConf(map[string]string{"memory": "10", "vcores": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
	}

	expectedHeadroom, err := resources.NewResourceFromConf(map[string]string{"memory": "50", "vcores": "50"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedHeadroom)
	}

	user2 := security.UserGroup{User: "user2", Groups: []string{"group2"}}
	conf := createUpdateConfigWithWildCardUsersAndGroups(user2.User, user2.Groups[0], "*", "*", "50", "50")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	// user1 fallback on wild card user limit. user1 max resources and max applications would be overwritten with wild card user limit settings
	headroom := manager.Headroom(queuePath1, TestApp1, user1)
	assert.Assert(t, resources.Equals(headroom, expectedHeadroom))

	// user2 has its own settings, so doesn't fallback on wild card user limit.
	headroom = manager.Headroom(queuePath1, TestApp1, user2)
	assert.Assert(t, resources.Equals(headroom, resources.Multiply(usage, 7)))

	// user1 uses wild card user limit settings.
	assert.Equal(t, manager.GetUserTracker(user1.User).queueTracker.maxRunningApps, uint64(20))
	assert.Equal(t, manager.GetUserTracker(user1.User).queueTracker.childQueueTrackers["parent"].maxRunningApps, uint64(10))
	assert.Equal(t, manager.GetUserTracker(user1.User).queueTracker.childQueueTrackers["parent"].childQueueTrackers["child1"].maxRunningApps, uint64(0))
	assert.Equal(t, resources.Equals(manager.GetUserTracker(user1.User).queueTracker.maxResources, resources.Multiply(usage, 14)), true)
	assert.Assert(t, resources.Equals(manager.GetUserTracker(user1.User).queueTracker.childQueueTrackers["parent"].maxResources, expectedHeadroom))
	assert.Assert(t, resources.Equals(manager.GetUserTracker(user1.User).queueTracker.childQueueTrackers["parent"].childQueueTrackers["child1"].maxResources, nil))
	assert.Assert(t, manager.GetUserTracker(user1.User).queueTracker.useWildCard)
	assert.Assert(t, manager.GetUserTracker(user1.User).queueTracker.childQueueTrackers["parent"].useWildCard)
	assert.Assert(t, !manager.GetUserTracker(user1.User).queueTracker.childQueueTrackers["parent"].childQueueTrackers["child1"].useWildCard)

	// user2 uses its own settings.
	assert.Equal(t, manager.GetUserTracker(user2.User).queueTracker.maxRunningApps, uint64(20))
	assert.Equal(t, manager.GetUserTracker(user2.User).queueTracker.childQueueTrackers["parent"].maxRunningApps, uint64(10))
	assert.Equal(t, manager.GetUserTracker(user2.User).queueTracker.childQueueTrackers["parent"].childQueueTrackers["child1"].maxRunningApps, uint64(0))
	assert.Assert(t, resources.Equals(manager.GetUserTracker(user2.User).queueTracker.maxResources, resources.Multiply(usage, 14)))
	assert.Assert(t, resources.Equals(manager.GetUserTracker(user2.User).queueTracker.childQueueTrackers["parent"].maxResources, resources.Multiply(usage, 7)))
	assert.Assert(t, resources.Equals(manager.GetUserTracker(user2.User).queueTracker.childQueueTrackers["parent"].childQueueTrackers["child1"].maxResources, nil))
	assert.Assert(t, !manager.GetUserTracker(user2.User).queueTracker.useWildCard)
	assert.Assert(t, !manager.GetUserTracker(user2.User).queueTracker.childQueueTrackers["parent"].useWildCard)
	assert.Assert(t, !manager.GetUserTracker(user2.User).queueTracker.childQueueTrackers["parent"].childQueueTrackers["child1"].useWildCard)

	for i := 0; i < 5; i++ {
		// should run as user has already fallen back on wild card user limit set on "root.parent" map[memory:50 vcores:50]
		manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user1)
	}

	// should not run as user has exceeded wild card user limit set on "root.parent" map[memory:50 vcores:50]
	headroom = manager.Headroom(queuePath1, TestApp3, user1)
	assert.Assert(t, !headroom.FitInMaxUndef(usage))

	// clear all configs. Since wild card user limit is not there, all users used its settings earlier under the same queue path should start using its own value
	conf = createConfigWithoutLimits()
	manager.IncreaseTrackedResource(queuePath2, TestApp2, usage, user2)
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"), err)

	headroom = manager.Headroom(queuePath1, TestApp1, user1)
	assert.Assert(t, resources.Equals(headroom, nil))

	assert.Equal(t, manager.GetUserTracker(user1.User).queueTracker.maxRunningApps, uint64(0))
	assert.Equal(t, manager.GetUserTracker(user1.User).queueTracker.childQueueTrackers["parent"].maxRunningApps, uint64(0))
	assert.Equal(t, manager.GetUserTracker(user1.User).queueTracker.childQueueTrackers["parent"].childQueueTrackers["child1"].maxRunningApps, uint64(0))
	assert.Assert(t, resources.Equals(manager.GetUserTracker(user1.User).queueTracker.maxResources, nil))
	assert.Assert(t, resources.Equals(manager.GetUserTracker(user1.User).queueTracker.childQueueTrackers["parent"].maxResources, nil))
	assert.Assert(t, resources.Equals(manager.GetUserTracker(user1.User).queueTracker.childQueueTrackers["parent"].childQueueTrackers["child1"].maxResources, nil))

	assert.Equal(t, manager.GetUserTracker(user2.User).queueTracker.maxRunningApps, uint64(0))
	assert.Equal(t, manager.GetUserTracker(user2.User).queueTracker.childQueueTrackers["parent"].maxRunningApps, uint64(0))
	assert.Equal(t, manager.GetUserTracker(user2.User).queueTracker.childQueueTrackers["parent"].childQueueTrackers["child1"].maxRunningApps, uint64(0))
	assert.Assert(t, resources.Equals(manager.GetUserTracker(user2.User).queueTracker.maxResources, nil))
	assert.Assert(t, resources.Equals(manager.GetUserTracker(user2.User).queueTracker.childQueueTrackers["parent"].maxResources, nil))
	assert.Assert(t, resources.Equals(manager.GetUserTracker(user2.User).queueTracker.childQueueTrackers["parent"].childQueueTrackers["child1"].maxResources, nil))

	// set limit for user1 explicitly. New limit should precede the wild card user limit
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, user1.Groups[0], "", "", "50", "50")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	headroom = manager.Headroom(queuePath1, TestApp1, user1)
	assert.Assert(t, resources.Equals(headroom, resources.Multiply(usage, 2)))
	assert.Assert(t, !manager.GetUserTracker(user1.User).queueTracker.useWildCard)
	assert.Assert(t, !manager.GetUserTracker(user1.User).queueTracker.childQueueTrackers["parent"].useWildCard)
	assert.Assert(t, !manager.GetUserTracker(user1.User).queueTracker.childQueueTrackers["parent"].childQueueTrackers["child1"].useWildCard)
}

func TestUpdateConfigWithWildCardUsersAndGroups(t *testing.T) {
	setupUGM()
	// Queue setup:
	// root->parent
	user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	conf := createUpdateConfig(user.User, user.Groups[0])
	manager := GetUserManager()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	expectedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "50", "vcores": "50"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}
	usage, err := resources.NewResourceFromConf(map[string]string{"memory": "10", "vcores": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}
	assertMaxLimits(t, user, expectedResource, 5)

	for i := 1; i <= 5; i++ {
		manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
	}

	// configure max resource for root.parent as map[memory:60 vcores:60] to allow one more application to run
	conf = createConfig(user.User, user.Groups[0], "memory", "50", 60, 6)
	err = manager.UpdateConfig(conf.Queues[0], "root")
	assert.NilError(t, err, "unable to set the limit for user user1 because current resource usage is greater than config max resource for root.parent")

	// should run as user 'user' setting is map[memory:60 vcores:60] and total usage of "root.parent" is map[memory:50 vcores:50]
	manager.IncreaseTrackedResource(queuePath1, TestApp2, usage, user)

	// should not run as user 'user' setting is map[memory:60 vcores:60] and total usage of "root.parent" is map[memory:60 vcores:60]
	headroom := manager.Headroom(queuePath1, TestApp3, user)
	assert.Equal(t, headroom.FitInMaxUndef(usage), false)

	// configure max resource for root.parent to allow one more application to run through wild card user settings (not through specific user)
	// configure limits for user2 only. However, user1 should not be cleared as it has running applications
	user1 := security.UserGroup{User: "user2", Groups: []string{"group1"}}
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, user1.Groups[0], "*", "*", "70", "70")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
	// ensure both user1 & group1 has not been removed from local maps
	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) != nil, true)

	headroom = manager.Headroom(queuePath1, TestApp2, user)
	assert.Equal(t, resources.Equals(headroom, usage), true)

	// user1 still should be able to run app as wild card user '*' setting is map[memory:70 vcores:70] for "root.parent" and
	// total usage of "root.parent" is map[memory:60 vcores:60]
	manager.IncreaseTrackedResource(queuePath1, TestApp2, usage, user)

	// user1 should not be able to run app as wild card user '*' setting is map[memory:70 vcores:70] for "root.parent"
	// and total usage of "root.parent" is map[memory:70 vcores:70]
	headroom = manager.Headroom(queuePath1, TestApp3, user)
	assert.Equal(t, headroom.FitInMaxUndef(usage), false)

	// configure max resource for group1 * root.parent (map[memory:70 vcores:70]) higher than wild card group * root.parent settings (map[memory:10 vcores:10])
	// ensure group's specific settings has been used for enforcement checks as specific limits always has higher precedence when compared to wild card group limit settings
	// group1 quota (map[memory:70 vcores:70]) has been removed from root.parent.leaf.
	// so resource usage has been decreased for the same group and that too for entire queue hierarchy (root->parent->leaf)
	// since group1 quota has been configured for root.parent, resource usage would be increased from
	// the place where it has been left.
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, user1.Groups[0], "*", "*", "10", "10")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	// configure max resource for user2 * root.parent (map[memory:70 vcores:70]) higher than wild card user * root.parent settings (map[memory:10 vcores:10])
	// ensure user's specific settings has been used for enforcement checks as specific limits always has higher precedence when compared to wild card user limit settings
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, "", "*", "", "10", "10")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	// can be allowed to run upto resource usage map[memory:70 vcores:70]
	for i := 1; i <= 7; i++ {
		manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user1)
	}

	// user2 should not be able to run app as user2 max limit is map[memory:70 vcores:70] and usage so far is map[memory:70 vcores:70]
	headroom = manager.Headroom(queuePath1, TestApp1, user1)
	assert.Equal(t, headroom.FitInMaxUndef(usage), false)

	user3 := security.UserGroup{User: "user3", Groups: []string{"group3"}}
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, user1.Groups[0], "", "*", "10", "10")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	// user3 should be able to run app as group3 uses wild card group limit settings map[memory:10 vcores:10]
	manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user3)

	// user4 (though belongs to different group, group4) should not be able to run app as group4 also
	// uses wild card group limit settings map[memory:10 vcores:10]
	user4 := security.UserGroup{User: "user4", Groups: []string{"group4"}}
	headroom = manager.Headroom(queuePath1, TestApp1, user4)
	assert.Equal(t, headroom.FitInMaxUndef(usage), false)

	conf = createUpdateConfigWithWildCardUsersAndGroups(user4.User, user4.Groups[0], "", "*", "10", "10")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	// Since app is TestApp1, gt of "*" would be used as it is already mapped. group4 won't be used
	headroom = manager.Headroom(queuePath1, TestApp1, user4)
	assert.Equal(t, headroom.FitInMaxUndef(usage), false)

	// Now group4 would be used as user4 is running TestApp2 for the first time. So can be allowed to run upto resource usage map[memory:70 vcores:70]
	for i := 1; i <= 7; i++ {
		manager.IncreaseTrackedResource(queuePath1, TestApp2, usage, user4)
	}

	// user4 should not be able to run app as user4 max limit is map[memory:70 vcores:70] and usage so far is map[memory:70 vcores:70]
	headroom = manager.Headroom(queuePath1, TestApp1, user4)
	assert.Equal(t, headroom.FitInMaxUndef(usage), false)
}

func TestUpdateConfigClearEarlierSetLimits(t *testing.T) {
	setupUGM()
	// Queue setup:
	// root->parent
	user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	conf := createUpdateConfig(user.User, user.Groups[0])

	manager := GetUserManager()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	expectedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "50", "vcores": "50"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}
	assertMaxLimits(t, user, expectedResource, 5)

	// create config user2 * root.parent with [50, 50] and maxapps as 5 (twice for root), but not user1.
	// so user1 should not be there as it doesn't have any running applications
	user1 := security.UserGroup{User: "user2", Groups: []string{"group2"}}
	conf = createUpdateConfig(user1.User, user1.Groups[0])
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	expectedResource, err = resources.NewResourceFromConf(map[string]string{"memory": "50", "vcores": "50"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}

	// ensure user1 has been removed from local maps
	assert.Equal(t, manager.GetUserTracker(user.User) == nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) == nil, true)
	assertMaxLimits(t, user1, expectedResource, 5)

	// override user2 * root.parent config with [60, 60] and maxapps as 6 (twice for root)
	conf = createConfig(user1.User, user1.Groups[0], "memory", "10", 60, 6)
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	expectedResource, err = resources.NewResourceFromConf(map[string]string{"memory": "60", "vcores": "60"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}
	assert.Equal(t, manager.GetUserTracker(user1.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user1.Groups[0]) != nil, true)
	assertMaxLimits(t, user1, expectedResource, 6)

	expectedResource, err = resources.NewResourceFromConf(map[string]string{"memory": "70", "vcores": "70"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}

	// override user2 * root.parent maxapps as [70, 70] and maxapps as 10 (twice for root)
	// and wild card settings
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, user1.Groups[0], "*", "*", "10", "10")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
	assert.Equal(t, manager.GetUserTracker(user1.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user1.Groups[0]) != nil, true)
	assertMaxLimits(t, user1, expectedResource, 10)
	assertWildCardLimits(t, m.userWildCardLimitsConfig, expectedResource)
	assertWildCardLimits(t, m.groupWildCardLimitsConfig, expectedResource)

	// config without limits - should clear all earlier set configs
	conf = createConfigWithoutLimits()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
	assert.Equal(t, manager.GetUserTracker(user.User) == nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) == nil, true)
	assert.Equal(t, manager.GetUserTracker(user1.User) == nil, true)
	assert.Equal(t, manager.GetGroupTracker(user1.Groups[0]) == nil, true)
	assert.Equal(t, len(manager.userWildCardLimitsConfig), 0)
	assert.Equal(t, len(manager.groupWildCardLimitsConfig), 0)
}

func TestUpdateConfigClearEarlierSetGroupLimits(t *testing.T) {
	setupUGM()
	user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	conf := createConfigWithGroupOnly(user.Groups[0], 50, 5)

	manager := GetUserManager()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	usage, err := resources.NewResourceFromConf(map[string]string{"memory": "25", "vcores": "25"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
	}
	cQueue := "root.parent.leaf"
	for i := 1; i <= 2; i++ {
		manager.IncreaseTrackedResource(cQueue, TestApp1, usage, user)
	}
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
	headroom := manager.Headroom(cQueue, TestApp1, user)
	assert.Equal(t, headroom.FitInMaxUndef(usage), false)
}

func TestSetMaxLimitsForRemovedUsers(t *testing.T) {
	setupUGM()
	// Queue setup:
	// root->parent
	user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	conf := createUpdateConfig(user.User, user.Groups[0])
	manager := GetUserManager()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	expectedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "50", "vcores": "50"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}
	usage, err := resources.NewResourceFromConf(map[string]string{"memory": "10", "vcores": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
	}
	assertMaxLimits(t, user, expectedResource, 5)

	for i := 1; i <= 2; i++ {
		manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
	}
	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) != nil, true)

	manager.DecreaseTrackedResource(queuePath1, TestApp1, usage, user, false)
	manager.DecreaseTrackedResource(queuePath1, TestApp1, usage, user, true)
	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) != nil, true)

	manager.IncreaseTrackedResource("root.parent.leaf", TestApp1, usage, user)
	headroom := manager.Headroom("root.parent.leaf", TestApp1, user)
	assert.Equal(t, headroom.FitInMaxUndef(usage), false)

	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) != nil, true)

	manager.DecreaseTrackedResource("root.parent.leaf", TestApp1, usage, user, true)

	conf = createConfigWithoutLimits()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	for i := 1; i <= 2; i++ {
		manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
	}
	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) == nil, true)

	manager.DecreaseTrackedResource(queuePath1, TestApp1, usage, user, false)
	manager.DecreaseTrackedResource(queuePath1, TestApp1, usage, user, true)
	assert.Equal(t, manager.GetUserTracker(user.User) == nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) == nil, true)
}

func TestUserGroupHeadroom(t *testing.T) {
	setupUGM()
	// Queue setup:
	// root->parent
	user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	conf := createUpdateConfig(user.User, user.Groups[0])
	manager := GetUserManager()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	expectedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "50", "vcores": "50"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}
	usage, err := resources.NewResourceFromConf(map[string]string{"memory": "10", "vcores": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
	}
	assertMaxLimits(t, user, expectedResource, 5)
	headroom := manager.Headroom("root.parent.leaf", TestApp1, user)
	assert.Equal(t, resources.Equals(headroom, usage), true)

	manager.IncreaseTrackedResource("root.parent.leaf", TestApp1, usage, user)
	headroom = manager.Headroom("root.parent.leaf", TestApp1, user)
	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) != nil, true)
	assert.Equal(t, resources.Equals(headroom, resources.Multiply(usage, 0)), true)

	headroom = manager.Headroom("root.parent.leaf", TestApp1, user)
	assert.Equal(t, headroom.FitInMaxUndef(usage), false)

	// configure limits only for group
	conf = createUpdateConfigWithWildCardUsersAndGroups("", user.Groups[0], "*", "*", "80", "80")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	usage1, err := resources.NewResourceFromConf(map[string]string{"memory": "70", "vcores": "70"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
	}
	// ensure group headroom returned when there is no limit settings configured for user
	// group1 quota (map[memory:70 vcores:70]) has been removed from root.parent.leaf.
	// so resource usage has been decreased for the same group and that too for entire queue hierarchy (root->parent->leaf)
	// since group1 quota has been configured for root.parent, resource usage would be increased from
	// the place where it has been left. so there is no usage after the recent config change, entire group's quota would be returned as headroom.
	headroom = manager.Headroom("root.parent", TestApp1, user)
	assert.Equal(t, resources.Equals(headroom, usage1), true)
}

func TestDecreaseTrackedResourceForGroupTracker(t *testing.T) {
	setupUGM()
	// Queue setup:
	// root->parent
	user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	conf := createConfigWithoutLimits()
	conf.Queues[0].Queues[0].Limits = []configs.Limit{
		{
			Limit:           "parent queue limit for a specific group",
			Groups:          user.Groups,
			MaxResources:    map[string]string{"memory": "100"},
			MaxApplications: 2,
		},
	}
	manager := GetUserManager()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	usage, err := resources.NewResourceFromConf(map[string]string{"memory": "50"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
	}

	manager.IncreaseTrackedResource("root.parent", TestApp1, usage, user)
	groupTracker := m.GetGroupTracker(user.Groups[0])
	assert.Equal(t, groupTracker != nil, true)
	assert.Equal(t, groupTracker.queueTracker.childQueueTrackers["parent"].runningApplications[TestApp1], true)
	assert.Equal(t, resources.Equals(groupTracker.queueTracker.childQueueTrackers["parent"].resourceUsage, usage), true)

	manager.DecreaseTrackedResource("root.parent", TestApp1, usage, user, true)

	groupTracker = m.GetGroupTracker(user.Groups[0])
	assert.Equal(t, groupTracker != nil, true)
	assert.Equal(t, groupTracker.queueTracker.childQueueTrackers["parent"].runningApplications[TestApp1], false)
	assert.Equal(t, resources.Equals(groupTracker.queueTracker.childQueueTrackers["parent"].resourceUsage, resources.Zero), true)

	manager.IncreaseTrackedResource("root.parent", TestApp1, usage, user)
	groupTracker = m.GetGroupTracker(user.Groups[0])
	assert.Equal(t, groupTracker != nil, true)
	assert.Equal(t, groupTracker.queueTracker.childQueueTrackers["parent"].runningApplications[TestApp1], true)
	assert.Equal(t, resources.Equals(groupTracker.queueTracker.childQueueTrackers["parent"].resourceUsage, usage), true)

	conf.Queues[0].Queues[0].Limits = []configs.Limit{
		{
			Limit:  "parent queue limit for a specific group",
			Groups: user.Groups,
		},
	}
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	manager.DecreaseTrackedResource("root.parent", TestApp1, usage, user, true)
	groupTracker = m.GetGroupTracker(user.Groups[0])
	assert.Equal(t, groupTracker == nil, true)
}

func TestIncreaseTrackedResourceForGroupTracker(t *testing.T) {
	setupUGM()
	// Queue setup:
	// root->parent
	user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	conf := createConfigWithoutLimits()
	conf.Queues[0].Queues[0].Limits = []configs.Limit{
		{
			Limit:           "parent queue limit for a specific group",
			Groups:          user.Groups,
			MaxResources:    map[string]string{"memory": "100"},
			MaxApplications: 2,
		},
	}
	manager := GetUserManager()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	usage1, err := resources.NewResourceFromConf(map[string]string{"memory": "50"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	manager.IncreaseTrackedResource("root.parent", TestApp1, usage1, user)
	groupTracker := m.GetGroupTracker(user.Groups[0])
	assert.Equal(t, groupTracker != nil, true)
	assert.Equal(t, groupTracker.queueTracker.childQueueTrackers["parent"].runningApplications[TestApp1], true)
	assert.Equal(t, resources.Equals(groupTracker.queueTracker.childQueueTrackers["parent"].resourceUsage, usage1), true)

	usage2, err := resources.NewResourceFromConf(map[string]string{"memory": "30"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}

	manager.IncreaseTrackedResource("root.parent", TestApp2, usage2, user)
	assert.Equal(t, groupTracker.queueTracker.childQueueTrackers["parent"].runningApplications[TestApp2], true)
	assert.Equal(t, resources.Equals(groupTracker.queueTracker.childQueueTrackers["parent"].resourceUsage, resources.Add(usage1, usage2)), true)
}

func TestUserGroupLimitWithMultipleApps(t *testing.T) {
	// Increase app rsources to different child queue, which have different group linkage
	// Queue setup:
	//   root.parent with group1 limit
	//   root.parent.child1 with group2 limit
	//   root.parent.child2 with no limit

	// Increase TestApp1 resource to root.parent.child1, it should be tracked by group2's groupTracker (limit was set in root.parent.child1 with group2 limit)
	// Increase TestApp2 resource to root.parent.child2, it should be tracked by group1's groupTracker (limit was set in root.parent with group1 limit)
	// Decrease TestApp1 resource from root.parent.child1, resources should be cleaned up from group2's groupTracker
	// Decrease TestApp2 resource from root.parent.child2, resources should be cleaned up from group1's groupTracker

	// Setup
	setupUGM()
	manager := GetUserManager()
	userGroup := security.UserGroup{User: "user", Groups: []string{"group1", "group2"}}

	conf := createConfigWithDifferentGroupsLinkage(userGroup.Groups[0], userGroup.Groups[1], 10, 10, 3)
	assert.NilError(t, m.UpdateConfig(conf.Queues[0], "root"))
	usage, err := resources.NewResourceFromConf(map[string]string{"memory": "10", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
	}

	// run different apps with different groups linkage (different queue limit settings)
	manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, userGroup)
	manager.IncreaseTrackedResource(queuePath2, TestApp2, usage, userGroup)

	// ensure different groups are linked and resource usage is correct
	assert.Equal(t, len(manager.getUserTracker("user").appGroupTrackers), 2)
	gt1 := manager.getUserTracker("user").appGroupTrackers[TestApp1]
	gt2 := manager.getUserTracker("user").appGroupTrackers[TestApp2]
	assert.Equal(t, gt1.groupName, "group2")
	assert.Equal(t, resources.Equals(gt1.queueTracker.resourceUsage, usage), true)
	assert.Equal(t, gt2.groupName, "group1")
	assert.Equal(t, resources.Equals(gt2.queueTracker.resourceUsage, usage), true)

	// limit has reached for both the groups
	headroom := manager.Headroom(queuePath1, TestApp1, userGroup)
	assert.Equal(t, resources.Equals(headroom, resources.Zero), true, "init headroom is not expected")
	headroom = manager.Headroom(queuePath2, TestApp2, userGroup)
	assert.Equal(t, resources.Equals(headroom, resources.Zero), true, "init headroom is not expected")

	// remove the apps
	manager.DecreaseTrackedResource(queuePath1, TestApp1, usage, userGroup, true)
	manager.DecreaseTrackedResource(queuePath2, TestApp2, usage, userGroup, true)

	// assert group linkage has been removed
	assert.Equal(t, len(manager.getUserTracker("user").appGroupTrackers), 0)
}

//nolint:funlen
func TestCanRunApp(t *testing.T) {
	testCases := []struct {
		name   string
		limits []configs.Limit
	}{
		{
			name: "specific user limit",
			limits: []configs.Limit{
				{
					Limit:           "specific user limit",
					Users:           []string{"user1"},
					MaxApplications: 1,
				},
			},
		},
		{
			name: "specific group limit",
			limits: []configs.Limit{
				{
					Limit:           "specific group limit",
					Groups:          []string{"group1"},
					MaxApplications: 1,
				},
			},
		},
		{
			name: "wildcard user limit",
			limits: []configs.Limit{
				{
					Limit:           "wildcard user limit",
					Users:           []string{"*"},
					MaxApplications: 1,
				},
			},
		},
		{
			name: "wildcard group limit",
			limits: []configs.Limit{
				{
					Limit:           "specific group limit",
					Groups:          []string{"nonexistent-group"},
					MaxApplications: 100,
				},
				{
					Limit:           "wildcard group limit",
					Groups:          []string{"*"},
					MaxApplications: 1,
				},
			},
		},
		{
			name: "specific user lower than specific group limit",
			limits: []configs.Limit{
				{
					Limit:           "specific user limit",
					Users:           []string{"user1"},
					MaxApplications: 1,
				},
				{
					Limit:           "specific group limit",
					Groups:          []string{"group1"},
					MaxApplications: 100,
				},
			},
		},
		{
			name: "specific group lower than specific user limit",
			limits: []configs.Limit{
				{
					Limit:           "specific user limit",
					Users:           []string{"user1"},
					MaxApplications: 100,
				},
				{
					Limit:           "specific group limit",
					Groups:          []string{"group1"},
					MaxApplications: 1,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setupUGM()
			// Queue setup:
			// root->default
			conf := configs.PartitionConfig{
				Name: "default",
				Queues: []configs.QueueConfig{
					{
						Name:      "root",
						Parent:    true,
						SubmitACL: "*",
						Queues: []configs.QueueConfig{
							{
								Name:      "default",
								Parent:    false,
								SubmitACL: "*",
								Limits:    tc.limits,
							},
						},
					},
				},
			}
			manager := GetUserManager()
			assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

			user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
			usage, err := resources.NewResourceFromConf(map[string]string{"memory": "50"})
			if err != nil {
				t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
			}

			canRunApp := manager.CanRunApp("root.default", TestApp1, user)
			assert.Equal(t, canRunApp, true, fmt.Sprintf("user %s should be able to run app %s", user.User, TestApp1))

			manager.IncreaseTrackedResource("root.default", TestApp1, usage, user)
			canRunApp = manager.CanRunApp("root.default", TestApp2, user)
			assert.Equal(t, canRunApp, false, fmt.Sprintf("user %s shouldn't be able to run app %s", user.User, TestApp2))
		})
	}
}

func TestSeparateUserGroupHeadroom(t *testing.T) {
	testCases := []struct {
		name string
		user security.UserGroup
		conf configs.PartitionConfig
	}{
		{
			name: "headroom with only user limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit([]string{"user1"}, nil, mediumResource, 2),
			}),
		},
		{
			name: "headroom with only group limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit(nil, []string{"group1"}, mediumResource, 2),
			}),
		},
		{
			name: "headroom with user limit lower than group limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit([]string{"user1"}, nil, mediumResource, 2),
				createLimit(nil, []string{"group1"}, largeResource, 2),
			}),
		},
		{
			name: "headroom with group limit lower than user limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit([]string{"user1"}, nil, largeResource, 2),
				createLimit(nil, []string{"group1"}, mediumResource, 2),
			}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setupUGM()

			manager := GetUserManager()
			assert.NilError(t, manager.UpdateConfig(tc.conf.Queues[0], "root"))

			usage, err := resources.NewResourceFromConf(tinyResource)
			if err != nil {
				t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
			}

			manager.IncreaseTrackedResource(queuePathParent, TestApp1, usage, tc.user)
			headroom := manager.Headroom(queuePathParent, TestApp1, tc.user)
			expectedHeadroom, err := resources.NewResourceFromConf(tinyResource)
			if err != nil {
				t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
			}

			assert.Equal(t, resources.Equals(headroom, expectedHeadroom), true)
		})
	}
}

func TestUserGroupLimit(t *testing.T) { //nolint:funlen
	testCases := []struct {
		name                          string
		user                          security.UserGroup
		conf                          configs.PartitionConfig
		initExpectedHeadroomResource  map[string]string
		finalExpectedHeadroomResource map[string]string
		canRunApp                     bool
		isHeadroomAvailable           bool
	}{
		// unmixed user and group limit
		{
			name: "maxresources with a specific user limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit([]string{"user1"}, nil, mediumResource, 2),
			}),
			initExpectedHeadroomResource:  mediumResource,
			finalExpectedHeadroomResource: nil,
			canRunApp:                     true,
		},
		{
			name: "maxapplications with a specific user limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit([]string{"user1"}, nil, largeResource, 1),
			}),
			initExpectedHeadroomResource:  largeResource,
			finalExpectedHeadroomResource: mediumResource,
			isHeadroomAvailable:           true,
		},
		{
			name: "maxresources with a specific user limit and a wildcard user limit for a not specific user",
			user: security.UserGroup{User: "user2", Groups: []string{"group2"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit([]string{"user1"}, nil, largeResource, 2),
				createLimit([]string{"*"}, nil, mediumResource, 2),
			}),
			initExpectedHeadroomResource:  mediumResource,
			finalExpectedHeadroomResource: nil,
			canRunApp:                     true,
		},
		{
			name: "maxapplications with a specific user limit and a wildcard user limit for a not specific user",
			user: security.UserGroup{User: "user2", Groups: []string{"group2"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit([]string{"user1"}, nil, largeResource, 2),
				createLimit([]string{"*"}, nil, largeResource, 1),
			}),
			initExpectedHeadroomResource:  largeResource,
			finalExpectedHeadroomResource: mediumResource,
			isHeadroomAvailable:           true,
		},
		{
			name: "maxresources with a specific user limit and a wildcard user limit for a specific user",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit([]string{"user1"}, nil, mediumResource, 2),
				createLimit([]string{"*"}, nil, largeResource, 2),
			}),
			initExpectedHeadroomResource:  mediumResource,
			finalExpectedHeadroomResource: nil,
			canRunApp:                     true,
		},
		{
			name: "maxapplications with a specific user limit and a wildcard user limit for a specific user",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit([]string{"user1"}, nil, largeResource, 1),
				createLimit([]string{"*"}, nil, largeResource, 2),
			}),
			initExpectedHeadroomResource:  largeResource,
			finalExpectedHeadroomResource: mediumResource,
			isHeadroomAvailable:           true,
		},
		{
			name: "maxresources with a wildcard user limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit(nil, []string{"*"}, mediumResource, 2),
			}),
			initExpectedHeadroomResource:  mediumResource,
			finalExpectedHeadroomResource: nil,
			canRunApp:                     true,
		},
		{
			name: "maxapplications with a wildcard user limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit(nil, []string{"*"}, largeResource, 1),
			}),
			initExpectedHeadroomResource:  largeResource,
			finalExpectedHeadroomResource: mediumResource,
			isHeadroomAvailable:           true,
		},
		{
			name: "maxresources with a specific group limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit(nil, []string{"group1"}, mediumResource, 2),
			}),
			initExpectedHeadroomResource:  mediumResource,
			finalExpectedHeadroomResource: nil,
			canRunApp:                     true,
		},
		{
			name: "maxapplications with a specific group limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit(nil, []string{"group1"}, largeResource, 1),
			}),
			initExpectedHeadroomResource:  largeResource,
			finalExpectedHeadroomResource: mediumResource,
			isHeadroomAvailable:           true,
		},
		{
			name: "maxresources with a specific group limit and a wildcard group limit for a not specific group user",
			user: security.UserGroup{User: "user2", Groups: []string{"group2"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit(nil, []string{"group1"}, largeResource, 2),
				createLimit(nil, []string{"*"}, mediumResource, 2),
			}),
			initExpectedHeadroomResource:  mediumResource,
			finalExpectedHeadroomResource: nil,
			canRunApp:                     true,
		},
		{
			name: "maxapplications with a specific group limit and a wildcard group limit for a not specific group user",
			user: security.UserGroup{User: "user2", Groups: []string{"group2"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit(nil, []string{"group1"}, largeResource, 2),
				createLimit(nil, []string{"*"}, largeResource, 1),
			}),
			initExpectedHeadroomResource:  largeResource,
			finalExpectedHeadroomResource: mediumResource,
			isHeadroomAvailable:           true,
		},
		{
			name: "maxresources with a specific group limit and a wildcard group limit for a specific group user",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit(nil, []string{"group1"}, mediumResource, 2),
				createLimit(nil, []string{"*"}, largeResource, 2),
			}),
			initExpectedHeadroomResource:  mediumResource,
			finalExpectedHeadroomResource: nil,
			canRunApp:                     true,
		},
		{
			name: "maxapplications with a specific group limit and a wildcard group limit for a specific group user",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit(nil, []string{"group1"}, largeResource, 1),
				createLimit(nil, []string{"*"}, largeResource, 2),
			}),
			initExpectedHeadroomResource:  largeResource,
			finalExpectedHeadroomResource: mediumResource,
			isHeadroomAvailable:           true,
		},
		// mixed user and group limit
		{
			name: "maxresources with user limit lower than group limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit([]string{"user1"}, nil, mediumResource, 2),
				createLimit(nil, []string{"group1"}, largeResource, 2),
			}),
			initExpectedHeadroomResource:  mediumResource,
			finalExpectedHeadroomResource: nil,
			canRunApp:                     true,
		},
		{
			name: "maxapplications with user limit lower than group limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit([]string{"user1"}, nil, largeResource, 1),
				createLimit(nil, []string{"group1"}, largeResource, 2),
			}),
			initExpectedHeadroomResource:  largeResource,
			finalExpectedHeadroomResource: mediumResource,
			isHeadroomAvailable:           true,
		},
		{
			name: "maxresources with gorup limit lower than user limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit([]string{"user1"}, nil, largeResource, 2),
				createLimit(nil, []string{"group1"}, mediumResource, 2),
			}),
			initExpectedHeadroomResource:  mediumResource,
			finalExpectedHeadroomResource: nil,
			canRunApp:                     true,
		},
		{
			name: "maxapplications with group limit lower than user limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			conf: createConfigWithLimits([]configs.Limit{
				createLimit([]string{"user1"}, nil, largeResource, 2),
				createLimit(nil, []string{"group1"}, largeResource, 1),
			}),
			initExpectedHeadroomResource:  largeResource,
			finalExpectedHeadroomResource: mediumResource,
			isHeadroomAvailable:           true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// clear tracked resource
			setupUGM()

			manager := GetUserManager()
			assert.NilError(t, manager.UpdateConfig(tc.conf.Queues[0], "root"))

			usage, err := resources.NewResourceFromConf(mediumResource)
			assert.NilError(t, err, fmt.Sprintf("can't create resource from %v", mediumResource))

			initExpectedHeadroom, err := resources.NewResourceFromConf(tc.initExpectedHeadroomResource)
			assert.NilError(t, err, fmt.Sprintf("can't create resource from %v", tc.initExpectedHeadroomResource))
			headroom := manager.Headroom(queuePathParent, TestApp1, tc.user)
			assert.Equal(t, resources.Equals(headroom, initExpectedHeadroom), true, "init headroom is not expected")

			manager.IncreaseTrackedResource(queuePathParent, TestApp1, usage, tc.user)
			userTracker := manager.GetUserTracker(tc.user.User)
			assert.Equal(t, userTracker != nil, true, fmt.Sprintf("can't get user tracker: %s", tc.user.User))
			assert.Equal(t, resources.Equals(userTracker.queueTracker.resourceUsage, usage), true, "user tracker resource usage is not expected at root level")
			assert.Equal(t, userTracker.queueTracker.runningApplications[TestApp1], true, fmt.Sprintf("%s is not in runningApplications for user tracker %s at root level", TestApp1, tc.user.User))
			assert.Equal(t, userTracker.queueTracker.childQueueTrackers["parent"] != nil, true, fmt.Sprintf("can't get root.parent queue tracker in user tracker: %s", tc.user.User))
			assert.Equal(t, resources.Equals(userTracker.queueTracker.childQueueTrackers["parent"].resourceUsage, usage), true, "user tracker resource usage is not expected at root.parent level")
			assert.Equal(t, userTracker.queueTracker.childQueueTrackers["parent"].runningApplications[TestApp1], true, fmt.Sprintf("%s is not in runningApplications for user tracker %s at root.parent level", TestApp1, tc.user.User))

			finalExpectedHeadroom, err := resources.NewResourceFromConf(tc.finalExpectedHeadroomResource)
			assert.NilError(t, err, fmt.Sprintf("can't create resource from %v", tc.finalExpectedHeadroomResource))
			headroom = manager.Headroom(queuePathParent, TestApp1, tc.user)
			assert.Equal(t, resources.Equals(headroom, finalExpectedHeadroom), true, "final headroom is not expected")
			assert.Equal(t, manager.CanRunApp(queuePathParent, TestApp2, tc.user), tc.canRunApp)
			headroom = manager.Headroom(queuePathParent, TestApp2, tc.user)
			assert.Equal(t, headroom.FitInMaxUndef(usage), tc.isHeadroomAvailable)
		})
	}
}

func TestUserGroupMaxResourcesChange(t *testing.T) { //nolint:funlen
	testCases := []struct {
		name      string
		user      security.UserGroup
		limits    []configs.Limit
		newLimits []configs.Limit
	}{
		{
			name: "Updated specific user & group max resources",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			limits: []configs.Limit{
				createLimit([]string{"user1"}, []string{"group1"}, largeResource, 2),
			},
			newLimits: []configs.Limit{
				createLimit([]string{"user1"}, []string{"group1"}, mediumResource, 2),
			},
		},
		{
			name: "Updated specific user & group max resources with mem only",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			limits: []configs.Limit{
				createLimit([]string{"user1"}, []string{"group1"}, mediumResource, 2),
			},
			newLimits: []configs.Limit{
				createLimit([]string{"user1"}, []string{"group1"}, mediumResourceWithMemOnly, 2),
			},
		},
		{
			name: "Updated specific user & group max resources with vcores only",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			limits: []configs.Limit{
				createLimit([]string{"user1"}, []string{"group1"}, mediumResourceWithMemOnly, 2),
			},
			newLimits: []configs.Limit{
				createLimit([]string{"user1"}, []string{"group1"}, mediumResourceWithVcoresOnly, 2),
			},
		},
		{
			name: "Updated specific user & group max resources with nil resource",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			limits: []configs.Limit{
				createLimit([]string{"user1"}, []string{"group1"}, mediumResourceWithMemOnly, 2),
			},
			newLimits: []configs.Limit{
				createLimit([]string{"user1"}, []string{"group1"}, nilResource, 2),
			},
		},
		{
			name: "Updated specific user & group max resources with nil",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			limits: []configs.Limit{
				createLimit([]string{"user1"}, []string{"group1"}, mediumResourceWithMemOnly, 2),
			},
			newLimits: []configs.Limit{
				createLimit([]string{"user1"}, []string{"group1"}, nil, 2),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setupUGM()

			manager := GetUserManager()
			conf := createConfigWithLimits(tc.limits)
			assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

			expectedResource, err := resources.NewResourceFromConf(tc.limits[0].MaxResources)
			if err != nil {
				t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
			}
			assert.Equal(t, resources.Equals(manager.GetUserTracker("user1").queueTracker.childQueueTrackers["parent"].maxResources, expectedResource), true)
			assert.Equal(t, resources.Equals(manager.GetGroupTracker("group1").queueTracker.childQueueTrackers["parent"].maxResources, expectedResource), true)

			conf.Queues[0].Queues[0].Limits = tc.newLimits
			assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

			expectedResource, err = resources.NewResourceFromConf(tc.newLimits[0].MaxResources)
			if err != nil {
				t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
			}
			assert.Equal(t, resources.Equals(manager.GetUserTracker("user1").queueTracker.childQueueTrackers["parent"].maxResources, expectedResource), true)
			assert.Equal(t, resources.Equals(manager.GetGroupTracker("group1").queueTracker.childQueueTrackers["parent"].maxResources, expectedResource), true)
		})
	}
}

func TestUserGroupLimitChange(t *testing.T) { //nolint:funlen
	testCases := []struct {
		name                 string
		user                 security.UserGroup
		limits               []configs.Limit
		newLimits            []configs.Limit
		maxAppsExceeded      bool
		maxResourcesExceeded bool
	}{
		// user limit only
		{
			name: "maxresources with an updated specific user limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			limits: []configs.Limit{
				createLimit([]string{"user1"}, nil, largeResource, 2),
			},
			newLimits: []configs.Limit{
				createLimit([]string{"user1"}, nil, mediumResource, 2),
			},
			maxResourcesExceeded: true,
		},
		{
			name: "maxapplications with an updated specific user limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			limits: []configs.Limit{
				createLimit([]string{"user1"}, nil, largeResource, 2),
			},
			newLimits: []configs.Limit{
				createLimit([]string{"user1"}, nil, largeResource, 1),
			},
			maxAppsExceeded: true,
		},

		// group limit only
		{

			name: "maxresources with an updated specific group limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			limits: []configs.Limit{
				createLimit(nil, []string{"group1"}, largeResource, 2),
			},
			newLimits: []configs.Limit{
				createLimit(nil, []string{"group1"}, mediumResource, 2),
			},
			maxResourcesExceeded: true,
		},
		{
			name: "maxapplications with an updated specific group limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			limits: []configs.Limit{
				createLimit(nil, []string{"group1"}, largeResource, 2),
			},
			newLimits: []configs.Limit{
				createLimit(nil, []string{"group1"}, largeResource, 1),
			},
			maxAppsExceeded: true,
		},

		// user wilcard limit
		{
			name: "maxresources with an updated wildcard user limit",
			user: security.UserGroup{User: "user2", Groups: []string{"group2"}},
			limits: []configs.Limit{
				createLimit([]string{"user1"}, nil, tinyResource, 2),
				createLimit([]string{"*"}, nil, largeResource, 2),
			},
			newLimits: []configs.Limit{
				createLimit([]string{"user1"}, nil, largeResource, 2),
				createLimit([]string{"*"}, nil, mediumResource, 2),
			},
			maxResourcesExceeded: true,
		},
		{
			name: "maxapplications with an updated wildcard user limit",
			user: security.UserGroup{User: "user2", Groups: []string{"group2"}},
			limits: []configs.Limit{
				createLimit([]string{"user1"}, nil, tinyResource, 1),
				createLimit([]string{"*"}, nil, largeResource, 2),
			},
			newLimits: []configs.Limit{
				createLimit([]string{"user1"}, nil, largeResource, 2),
				createLimit([]string{"*"}, nil, largeResource, 1),
			},
			maxAppsExceeded: true,
		},

		// group wilcard limit
		{

			name: "maxresources with an updated wildcard group limit",
			user: security.UserGroup{User: "user2", Groups: []string{"group2"}},
			limits: []configs.Limit{
				createLimit(nil, []string{"group1"}, tinyResource, 2),
				createLimit(nil, []string{"*"}, largeResource, 2),
			},
			newLimits: []configs.Limit{
				createLimit(nil, []string{"group1"}, largeResource, 2),
				createLimit(nil, []string{"*"}, mediumResource, 2),
			},
			maxResourcesExceeded: true,
		},
		{
			name: "maxapplications with an updated wildcard group limit",
			user: security.UserGroup{User: "user2", Groups: []string{"group2"}},
			limits: []configs.Limit{
				createLimit(nil, []string{"group1"}, tinyResource, 1),
				createLimit(nil, []string{"*"}, largeResource, 2),
			},
			newLimits: []configs.Limit{
				createLimit(nil, []string{"group1"}, largeResource, 2),
				createLimit(nil, []string{"*"}, largeResource, 1),
			},
			maxAppsExceeded: true,
		},

		// in a different limit
		{
			name: "maxresources with a new specific user limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			limits: []configs.Limit{
				{
					Limit:           "parent queue limit for specific user",
					Users:           []string{"user1"},
					MaxResources:    map[string]string{"memory": "100", "vcores": "100"},
					MaxApplications: 2,
				},
			},
			newLimits: []configs.Limit{
				{
					Limit:           "new parent queue limit for specific user",
					Users:           []string{"user1"},
					MaxResources:    map[string]string{"memory": "50", "vcores": "50"},
					MaxApplications: 2,
				},
			},
			maxResourcesExceeded: true,
		},
		{
			name: "maxapplications with a new specific user limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			limits: []configs.Limit{
				{
					Limit:           "parent queue limit for specific user",
					Users:           []string{"user1"},
					MaxResources:    map[string]string{"memory": "100", "vcores": "100"},
					MaxApplications: 2,
				},
			},
			newLimits: []configs.Limit{
				{
					Limit:           "new parent queue limit for specific user",
					Users:           []string{"user1"},
					MaxResources:    map[string]string{"memory": "100", "vcores": "100"},
					MaxApplications: 1,
				},
			},
			maxAppsExceeded: true,
		},
		{
			name: "maxresources with a new specific group limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			limits: []configs.Limit{
				{
					Limit:           "parent queue limit for specific group",
					Groups:          []string{"group1"},
					MaxResources:    map[string]string{"memory": "100", "vcores": "100"},
					MaxApplications: 2,
				},
			},
			newLimits: []configs.Limit{
				{
					Limit:           "new parent queue limit for specific group",
					Groups:          []string{"group1"},
					MaxResources:    map[string]string{"memory": "50", "vcores": "50"},
					MaxApplications: 2,
				},
			},
			maxResourcesExceeded: true,
		},
		{
			name: "maxapplications with a new specific group limit",
			user: security.UserGroup{User: "user1", Groups: []string{"group1"}},
			limits: []configs.Limit{
				{
					Limit:           "parent queue limit for specific group",
					Groups:          []string{"group1"},
					MaxResources:    map[string]string{"memory": "100", "vcores": "100"},
					MaxApplications: 2,
				},
			},
			newLimits: []configs.Limit{
				{
					Limit:           "new parent queue limit for specific group",
					Groups:          []string{"group1"},
					MaxResources:    map[string]string{"memory": "100", "vcores": "100"},
					MaxApplications: 1,
				},
			},
			maxAppsExceeded: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setupUGM()

			manager := GetUserManager()
			conf := createConfigWithLimits(tc.limits)

			assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

			usage, err := resources.NewResourceFromConf(mediumResource)
			if err != nil {
				t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
			}

			manager.IncreaseTrackedResource(queuePathParent, TestApp1, usage, tc.user)
			manager.IncreaseTrackedResource(queuePathParent, TestApp2, usage, tc.user)
			manager.DecreaseTrackedResource(queuePathParent, TestApp2, usage, tc.user, true)

			conf.Queues[0].Queues[0].Limits = tc.newLimits
			assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

			assert.Equal(t, manager.CanRunApp(queuePathParent, TestApp2, tc.user), !tc.maxAppsExceeded)
			headroom := manager.Headroom(queuePathParent, TestApp2, tc.user)
			assert.Equal(t, headroom.FitInMaxUndef(usage), !tc.maxResourcesExceeded)
		})
	}
}

func TestMultipleGroupLimitChange(t *testing.T) {
	setupUGM()

	manager := GetUserManager()
	conf := createConfigWithLimits([]configs.Limit{
		createLimit(nil, []string{"group1", "group2"}, largeResource, 2),
		createLimit(nil, []string{"*"}, mediumResource, 1),
	})
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	user1 := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	user2 := security.UserGroup{User: "user2", Groups: []string{"group2"}}
	user3 := security.UserGroup{User: "user3", Groups: []string{"group3"}}

	usage, err := resources.NewResourceFromConf(mediumResource)
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
	}

	// all users can increate usage within the quota
	manager.IncreaseTrackedResource(queuePathParent, "test-app-1-1", usage, user1)
	manager.IncreaseTrackedResource(queuePathParent, "test-app-2-1", usage, user2)
	manager.IncreaseTrackedResource(queuePathParent, "test-app-3-1", usage, user3)

	// remove group2 from the specific group
	conf.Queues[0].Queues[0].Limits[0].Groups = []string{"group1"}
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	// user1 still can increase usage within the quota
	manager.IncreaseTrackedResource(queuePathParent, "test-app-1-2", usage, user1)

	// user2 can't increase usage more than wildcard limit
	headroom := manager.Headroom(queuePathParent, "test-app-2-2", user2)
	assert.Equal(t, headroom.FitInMaxUndef(usage), false)

	// user3 can't increase usage more than wildcard limit
	headroom = manager.Headroom(queuePathParent, "test-app-3-2", user3)
	assert.Equal(t, headroom.FitInMaxUndef(usage), false)
}

func createLimit(users, groups []string, maxResources map[string]string, maxApps uint64) configs.Limit {
	return configs.Limit{
		Users:           users,
		Groups:          groups,
		MaxResources:    maxResources,
		MaxApplications: maxApps,
	}
}

func createConfigWithLimits(limits []configs.Limit) configs.PartitionConfig {
	return configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:      "parent",
						Parent:    true,
						SubmitACL: "*",
						Queues:    nil,
						Limits:    limits,
					},
				},
			},
		},
	}
}

func createUpdateConfigWithWildCardUsersAndGroups(user string, group string, wildUser string, wildGroup string, memory string, vcores string) configs.PartitionConfig {
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:      "parent",
						Parent:    true,
						SubmitACL: "*",
						Queues:    nil,
						Limits: []configs.Limit{
							{
								Limit: "parent queue limit for specific user",
								Users: []string{
									user,
								},
								Groups: []string{
									group,
								},
								MaxResources: map[string]string{
									"memory": "70",
									"vcores": "70",
								},
								MaxApplications: 10,
							},
							{
								Limit: "parent queue limit for wild card user",
								Users: []string{
									wildUser,
								},
								Groups: []string{
									wildGroup,
								},
								MaxResources: map[string]string{
									"memory": memory,
									"vcores": vcores,
								},
								MaxApplications: 10,
							},
						},
					},
				},
				Limits: []configs.Limit{
					{
						Limit: "root queue limit",
						Users: []string{
							user, wildUser,
						},
						Groups: []string{
							group, wildGroup,
						},
						MaxResources: map[string]string{
							"memory": "140",
							"vcores": "140",
						},
						MaxApplications: 20,
					},
				},
			},
		},
	}
	return conf
}

func createUpdateConfig(user string, group string) configs.PartitionConfig {
	return createConfig(user, group, "memory", "10", 50, 5)
}

func createConfig(user string, group string, resourceKey string, resourceValue string, mem int, maxApps uint64) configs.PartitionConfig {
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:      "parent",
						Parent:    true,
						SubmitACL: "*",
						Queues: []configs.QueueConfig{
							{
								Name:      "leaf",
								Parent:    false,
								SubmitACL: "*",
								Queues:    nil,
								Limits: []configs.Limit{
									{
										Limit: "leaf queue limit",
										Users: []string{
											user,
										},
										Groups: []string{
											group,
										},
										MaxResources: map[string]string{
											resourceKey: resourceValue,
											"vcores":    "10",
										},
										MaxApplications: maxApps,
									},
								},
							},
						},
						Limits: []configs.Limit{
							{
								Limit: "parent queue limit",
								Users: []string{
									user,
								},
								Groups: []string{
									group,
								},
								MaxResources: map[string]string{
									"memory": strconv.Itoa(mem),
									"vcores": strconv.Itoa(mem),
								},
								MaxApplications: maxApps,
							},
						},
					},
				},
				Limits: []configs.Limit{
					{
						Limit: "root queue limit",
						Users: []string{
							user,
						},
						Groups: []string{
							group,
						},
						MaxResources: map[string]string{
							"memory": strconv.Itoa(mem * 2),
							"vcores": strconv.Itoa(mem * 2),
						},
						MaxApplications: maxApps * 2,
					},
				},
			},
		},
	}
	return conf
}

func createConfigWithDifferentGroups(user string, group string, resourceKey string, resourceValue string, mem int, maxApps uint64) configs.PartitionConfig {
	var rootGroups []string
	var parentGroups []string
	if group != "" {
		parentGroups = []string{
			group + "_parent",
		}
		rootGroups = []string{
			group + "_root",
		}
	}
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:      "parent",
						Parent:    true,
						SubmitACL: "*",
						Queues:    nil,
						Limits: []configs.Limit{
							{
								Limit: "parent queue limit",
								Users: []string{
									user,
								},
								Groups: parentGroups,
								MaxResources: map[string]string{
									"memory": strconv.Itoa(mem),
									"vcores": strconv.Itoa(mem),
								},
								MaxApplications: maxApps,
							},
						},
					},
				},
				Limits: []configs.Limit{
					{
						Limit: "root queue limit",
						Users: []string{
							user,
						},
						Groups: rootGroups,
						MaxResources: map[string]string{
							"memory": strconv.Itoa(mem * 2),
							"vcores": strconv.Itoa(mem * 2),
						},
						MaxApplications: maxApps * 2,
					},
				},
			},
		},
	}
	return conf
}

func createConfigWithDifferentGroupsLinkage(group1 string, group2 string, mem int, vcore int, maxApps uint64) configs.PartitionConfig {
	// root.parent with group1 limit
	// root.parent.child1 with group2 limit
	// root.parent.child2 with no limit
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:      "parent",
						Parent:    true,
						SubmitACL: "*",
						Queues: []configs.QueueConfig{
							{
								Name:      "child1",
								Parent:    false,
								SubmitACL: "*",
								Queues:    nil,
								Limits: []configs.Limit{
									{
										Limit: "child queue with group2 limit",
										Groups: []string{
											group2,
										},
										MaxResources: map[string]string{
											"memory": strconv.Itoa(mem),
											"vcore":  strconv.Itoa(vcore),
										},
										MaxApplications: maxApps,
									},
								},
							},
							{
								Name:      "child2",
								Parent:    false,
								SubmitACL: "*",
								Queues:    nil,
							},
						},
						Limits: []configs.Limit{
							{
								Limit: "parent queue with group1 limit",
								Groups: []string{
									group1,
								},
								MaxResources: map[string]string{
									"memory": strconv.Itoa(mem),
									"vcore":  strconv.Itoa(vcore),
								},
								MaxApplications: maxApps,
							},
						},
					},
				},
			},
		},
	}
	return conf
}

func createConfigWithGroupOnly(group string, mem int, maxApps uint64) configs.PartitionConfig {
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:      "parent",
						Parent:    true,
						SubmitACL: "*",
						Queues: []configs.QueueConfig{
							{
								Name:      "leaf",
								Parent:    false,
								SubmitACL: "*",
								Queues:    nil,
							},
						},
						Limits: []configs.Limit{
							{
								Limit: "parent queue limit",
								Groups: []string{
									group,
								},
								MaxResources: map[string]string{
									"memory": strconv.Itoa(mem),
									"vcores": strconv.Itoa(mem),
								},
								MaxApplications: maxApps,
							},
						},
					},
				},
			},
		},
	}
	return conf
}

func createConfigWithoutLimits() configs.PartitionConfig {
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:      "parent",
						Parent:    true,
						SubmitACL: "*",
						Queues:    nil,
					},
				},
			},
		},
	}
	return conf
}

func setupUGM() {
	manager := GetUserManager()
	manager.ClearUserTrackers()
	manager.ClearGroupTrackers()
	manager.ClearConfigLimits()
}

func assertUGM(t *testing.T, userGroup security.UserGroup, expected *resources.Resource, usersCount int) {
	manager := GetUserManager()
	assert.Equal(t, usersCount, len(manager.GetUserTrackers()), "userTrackers count not as expected")
	assert.Equal(t, 0, len(manager.GetGroupTrackers()), "groupTrackers count should be 0")
	userTR := manager.GetUserTracker(userGroup.User)
	assert.Assert(t, userTR != nil, "user tracker should be defined")
	assert.Assert(t, resources.Equals(userTR.queueTracker.resourceUsage, expected), "user max resource for root not correct")
	groupTR := manager.GetGroupTracker(userGroup.Groups[0])
	assert.Assert(t, groupTR == nil, "group tracker should not be defined")
}

func assertMaxLimits(t *testing.T, userGroup security.UserGroup, expectedResource *resources.Resource, expectedMaxApps int) {
	manager := GetUserManager()
	assert.Equal(t, manager.GetUserTracker(userGroup.User).queueTracker.maxRunningApps, uint64(expectedMaxApps*2))
	assert.Equal(t, manager.GetGroupTracker(userGroup.Groups[0]).queueTracker.maxRunningApps, uint64(expectedMaxApps*2))
	assert.Equal(t, resources.Equals(manager.GetUserTracker(userGroup.User).queueTracker.maxResources, resources.Multiply(expectedResource, 2)), true)
	assert.Equal(t, resources.Equals(manager.GetGroupTracker(userGroup.Groups[0]).queueTracker.maxResources, resources.Multiply(expectedResource, 2)), true)
	assert.Equal(t, manager.GetUserTracker(userGroup.User).queueTracker.childQueueTrackers["parent"].maxRunningApps, uint64(expectedMaxApps))
	assert.Equal(t, manager.GetGroupTracker(userGroup.Groups[0]).queueTracker.childQueueTrackers["parent"].maxRunningApps, uint64(expectedMaxApps))
	assert.Equal(t, resources.Equals(manager.GetUserTracker(userGroup.User).queueTracker.childQueueTrackers["parent"].maxResources, expectedResource), true)
	assert.Equal(t, resources.Equals(manager.GetGroupTracker(userGroup.Groups[0]).queueTracker.childQueueTrackers["parent"].maxResources, expectedResource), true)
}

func assertWildCardLimits(t *testing.T, limitsConfig map[string]*LimitConfig, expectedResource *resources.Resource) {
	assert.Equal(t, limitsConfig["root"].maxApplications, uint64(20))
	assert.Equal(t, limitsConfig["root.parent"].maxApplications, uint64(10))
	expResource := limitsConfig["root"].maxResources
	assert.Equal(t, resources.Equals(expResource, resources.Multiply(expectedResource, 2)), true)
	expResource = limitsConfig["root.parent"].maxResources
	configuredResource, err := resources.NewResourceFromConf(map[string]string{"memory": "10", "vcores": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, configuredResource)
	}
	assert.Equal(t, resources.Equals(expResource, configuredResource), true)
}
