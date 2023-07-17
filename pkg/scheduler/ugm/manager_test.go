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
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
)

func TestUserManagerOnceInitialization(t *testing.T) {
	manager := GetUserManager()
	assert.Equal(t, manager, manager)
	manager1 := GetUserManager()
	assert.Equal(t, manager, manager1)
}

func TestGetGroup(t *testing.T) {
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
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	increased := manager.IncreaseTrackedResource("root.parent.leaf", TestApp1, usage1, user1)
	if !increased {
		t.Errorf("unable to increase tracked resource. queuepath: root.parent.leaf, application id: %s, resource usage: %s, user: %s", TestApp1, usage1.String(), user.User)
	}
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
	// once group has been matched, it doesn't change for other queue paths and should not change because lowest level queue group should be used
	user = security.UserGroup{User: "test1", Groups: []string{"test", "test_root", "test_parent"}}
	conf = createConfigWithDifferentGroups(user.User, user.Groups[0], "memory", "10", 50, 5)
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
	group = manager.ensureGroup(user, "root.parent.leaf")
	assert.Equal(t, group, "test_parent")
	group = manager.ensureGroup(user, "root.parent")
	assert.Equal(t, group, "test_parent")
	group = manager.ensureGroup(user, "root")
	assert.Equal(t, group, "test_parent")

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
	increased = manager.IncreaseTrackedResource("root.parent.leaf", TestApp1, usage1, user)
	if !increased {
		t.Errorf("unable to increase tracked resource. queuepath: root.parent.leaf, application id: %s, resource usage: %s, user: %s", TestApp1, usage1.String(), user.User)
	}
	assert.Equal(t, len(manager.userTrackers), 2)

	user2 := security.UserGroup{User: "test2", Groups: []string{"test", "test_root"}}
	increased = manager.IncreaseTrackedResource("root.parent.leaf", TestApp2, usage1, user2)
	if !increased {
		t.Errorf("unable to increase tracked resource. queuepath: root.parent.leaf, application id: %s, resource usage: %s, user: %s", TestApp2, usage1.String(), user2.User)
	}
	assert.Equal(t, len(manager.userTrackers), 3)

	ut := manager.getUserTracker(user.User, false)
	actualGT := ut.appGroupTrackers[TestApp1]
	expectedGT := manager.getGroupTracker("test_root", false)
	assert.Equal(t, manager.getGroupTracker("test_root", false) != nil, true)
	assert.Equal(t, actualGT, expectedGT)
	assert.Equal(t, actualGT.groupName, "test_root")

	ut = manager.getUserTracker(user2.User, false)
	actualGT = ut.appGroupTrackers[TestApp2]
	expectedGT = manager.getGroupTracker("test_root", false)
	assert.Equal(t, manager.getGroupTracker("test_root", false) != nil, true)
	assert.Equal(t, actualGT, expectedGT)
	assert.Equal(t, actualGT.groupName, "test_root")

	assert.Equal(t, len(manager.userTrackers), 3)
	assert.Equal(t, len(manager.groupTrackers), 2)

	// create config without any limit settings for groups and
	// since earlier group (test_root) limit settings has been cleared, ensure earlier user and group linkage through appGroupTrackers map has been removed
	// and cleared as necessary
	conf = createConfigWithDifferentGroups(user2.User, "", "memory", "10", 50, 5)
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
	assert.Equal(t, len(manager.userTrackers), 3)
	assert.Equal(t, len(manager.groupTrackers), 0)

	ut = manager.getUserTracker(user.User, false)
	assert.Equal(t, ut.appGroupTrackers[TestApp1] == nil, true)
	assert.Equal(t, manager.getGroupTracker("test_root", false) == nil, true)
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

	increased := manager.IncreaseTrackedResource("", "", usage1, user)
	if increased {
		t.Errorf("mandatory parameters are missing. queuepath: , application id: , resource usage: %s, user: %s", usage1.String(), user.User)
	}

	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage1, user)
	if !increased {
		t.Errorf("unable to increase tracked resource. queuepath: %s, application id: %s, resource usage: %s, user: %s", queuePath1, TestApp1, usage1.String(), user.User)
	}

	userTrackers := manager.GetUsersResources()
	userTracker := userTrackers[0]
	groupTrackers := manager.GetGroupsResources()
	assert.Equal(t, len(groupTrackers), 0)
	assert.Equal(t, false, manager.isUserRemovable(userTracker))
	assertUGM(t, user, usage1, 1)
	assert.Equal(t, user.User, manager.GetUserTracker(user.User).userName)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) == nil, true)

	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage1, user)
	if !increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, usage1)
	}
	assertUGM(t, user, resources.Multiply(usage1, 2), 1)

	user1 := security.UserGroup{User: "test1", Groups: []string{"test1"}}
	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	increased = manager.IncreaseTrackedResource(queuePath2, TestApp2, usage2, user1)
	if !increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath2, TestApp2, usage2)
	}
	assertUGM(t, user1, usage2, 2)
	assert.Equal(t, user.User, manager.GetUserTracker(user.User).userName)
	assert.Equal(t, user1.User, manager.GetUserTracker(user1.User).userName)

	assert.Equal(t, true, manager.GetUserTracker(user.User).queueTracker.IsQueuePathTrackedCompletely(queuePath1))
	assert.Equal(t, true, manager.GetUserTracker(user1.User).queueTracker.IsQueuePathTrackedCompletely(queuePath2))
	assert.Equal(t, false, manager.GetUserTracker(user1.User).queueTracker.IsQueuePathTrackedCompletely(queuePath1))
	assert.Equal(t, false, manager.GetUserTracker(user.User).queueTracker.IsQueuePathTrackedCompletely(queuePath2))
	assert.Equal(t, false, manager.GetUserTracker(user.User).queueTracker.IsQueuePathTrackedCompletely(queuePath3))
	assert.Equal(t, false, manager.GetUserTracker(user.User).queueTracker.IsQueuePathTrackedCompletely(queuePath4))

	assert.Equal(t, true, manager.GetUserTracker(user.Groups[0]).queueTracker.IsQueuePathTrackedCompletely(queuePath1))
	assert.Equal(t, true, manager.GetUserTracker(user1.Groups[0]).queueTracker.IsQueuePathTrackedCompletely(queuePath2))
	assert.Equal(t, false, manager.GetUserTracker(user1.Groups[0]).queueTracker.IsQueuePathTrackedCompletely(queuePath1))
	assert.Equal(t, false, manager.GetUserTracker(user.Groups[0]).queueTracker.IsQueuePathTrackedCompletely(queuePath2))
	assert.Equal(t, false, manager.GetUserTracker(user.Groups[0]).queueTracker.IsQueuePathTrackedCompletely(queuePath3))
	assert.Equal(t, false, manager.GetUserTracker(user.Groups[0]).queueTracker.IsQueuePathTrackedCompletely(queuePath4))

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "5M", "vcore": "5"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}

	decreased := manager.DecreaseTrackedResource("", "", usage1, user, false)
	assert.Equal(t, decreased, false)

	decreased = manager.DecreaseTrackedResource(queuePath1, TestApp1, usage3, user, false)
	if !decreased {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage3, err)
	}
	assertUGM(t, user, usage1, 2)

	decreased = manager.DecreaseTrackedResource(queuePath1, TestApp1, usage3, user, true)
	if !decreased {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage3, err)
	}
	assert.Equal(t, 1, len(manager.GetUsersResources()), "userTrackers count should be 1")
	assert.Equal(t, 0, len(manager.GetGroupsResources()), "groupTrackers count should be 1")

	decreased = manager.DecreaseTrackedResource(queuePath2, TestApp2, usage2, user1, true)
	if !decreased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage2, err)
	}
	assert.Equal(t, 0, len(manager.GetUsersResources()), "userTrackers count should be 0")
	assert.Equal(t, 0, len(manager.GetGroupsResources()), "groupTrackers count should be 0")

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
		increased := manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
		if !increased {
			t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage, err)
		}
	}

	userTrackers := manager.GetUsersResources()
	userTracker := userTrackers[0]
	groupTrackers := manager.GetGroupsResources()
	groupTracker := groupTrackers[0]
	assert.Equal(t, false, manager.isUserRemovable(userTracker))
	assert.Equal(t, false, manager.isGroupRemovable(groupTracker))

	// configure max resource for root.parent lesser than current resource usage. should be allowed to set but user cannot be allowed to do any activity further
	conf = createConfig(user.User, user.Groups[0], "memory", "50", 40, 4)
	err = manager.UpdateConfig(conf.Queues[0], "root")
	assert.NilError(t, err)
	increased := manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
	if increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}

	// configure max resource for root and parent to allow one more application to run
	conf = createConfig(user.User, user.Groups[0], "memory", "50", 60, 6)
	err = manager.UpdateConfig(conf.Queues[0], "root")
	assert.NilError(t, err, "unable to set the limit for user user1 because current resource usage is greater than config max resource for root.parent")

	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
	if !increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}

	// configure max resource for root lesser than current resource usage. should be allowed to set but user cannot be allowed to do any activity further
	conf = createConfig(user.User, user.Groups[0], "memory", "50", 10, 10)
	err = manager.UpdateConfig(conf.Queues[0], "root")
	assert.NilError(t, err)
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
	if increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}
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
		increased := manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
		if !increased {
			t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, expectedResource)
		}
	}

	// configure max resource for root.parent as map[memory:60 vcores:60] to allow one more application to run
	conf = createConfig(user.User, user.Groups[0], "memory", "50", 60, 6)
	err = manager.UpdateConfig(conf.Queues[0], "root")
	assert.NilError(t, err, "unable to set the limit for user user1 because current resource usage is greater than config max resource for root.parent")

	// should run as user 'user' setting is map[memory:60 vcores:60] and total usage of "root.parent" is map[memory:50 vcores:50]
	increased := manager.IncreaseTrackedResource(queuePath1, TestApp2, usage, user)
	if !increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}

	// should not run as user 'user' setting is map[memory:60 vcores:60] and total usage of "root.parent" is map[memory:60 vcores:60]
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp3, usage, user)
	if increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}

	// configure max resource for root.parent to allow one more application to run through wild card user settings (not through specific user)
	// configure limits for user2 only. However, user1 should not be cleared as it has running applications
	user1 := security.UserGroup{User: "user2", Groups: []string{"group1"}}
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, user1.Groups[0], "*", "*", "70", "70")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
	// ensure both user1 & group1 has not been removed from local maps
	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) != nil, true)

	// user1 still should be able to run app as wild card user '*' setting is map[memory:70 vcores:70] for "root.parent" and
	// total usage of "root.parent" is map[memory:60 vcores:60]
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp2, usage, user)
	if !increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp2, user1)
	}

	// user1 should not be able to run app as wild card user '*' setting is map[memory:70 vcores:70] for "root.parent"
	// and total usage of "root.parent" is map[memory:70 vcores:70]
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp3, usage, user)
	if increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp3, user1)
	}

	// configure max resource for group1 * root.parent (map[memory:70 vcores:70]) higher than wild card group * root.parent settings (map[memory:10 vcores:10])
	// ensure group's specific settings has been used for enforcement checks as specific limits always has higher precedence when compared to wild card group limit settings
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, user1.Groups[0], "*", "*", "10", "10")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user1)
	if !increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user1)
	}

	// configure max resource for user2 * root.parent (map[memory:70 vcores:70]) higher than wild card user * root.parent settings (map[memory:10 vcores:10])
	// ensure user's specific settings has been used for enforcement checks as specific limits always has higher precedence when compared to wild card user limit settings
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, "", "*", "", "10", "10")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	// can be allowed to run upto resource usage map[memory:70 vcores:70]
	for i := 1; i <= 6; i++ {
		increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user1)
		if !increased {
			t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
		}
	}

	// user2 should not be able to run app as user2 max limit is map[memory:70 vcores:70] and usage so far is map[memory:70 vcores:70]
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user1)
	if increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}

	user3 := security.UserGroup{User: "user3", Groups: []string{"group3"}}
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, user1.Groups[0], "", "*", "10", "10")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	// user3 should be able to run app as group3 uses wild card group limit settings map[memory:10 vcores:10]
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user3)
	if !increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}

	// user4 (though belongs to different group, group4) should not be able to run app as group4 also
	// uses wild card group limit settings map[memory:10 vcores:10]
	user4 := security.UserGroup{User: "user4", Groups: []string{"group4"}}
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user4)
	assert.Equal(t, increased, false)

	conf = createUpdateConfigWithWildCardUsersAndGroups(user4.User, user4.Groups[0], "", "*", "10", "10")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	// can be allowed to run upto resource usage map[memory:70 vcores:70]
	for i := 1; i <= 7; i++ {
		increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user4)
		if !increased {
			t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user1)
		}
	}

	// user4 should not be able to run app as user4 max limit is map[memory:70 vcores:70] and usage so far is map[memory:70 vcores:70]
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user4)
	assert.Equal(t, increased, false)
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
		increased := manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
		assert.Equal(t, increased, true, "unable to increase tracked resource: queuepath "+queuePath1+", app "+TestApp1+", res "+usage.String())
	}
	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) != nil, true)

	decreased := manager.DecreaseTrackedResource(queuePath1, TestApp1, usage, user, false)
	assert.Equal(t, decreased, true)

	decreased = manager.DecreaseTrackedResource(queuePath1, TestApp1, usage, user, true)
	assert.Equal(t, decreased, true)
	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) != nil, true)

	increased := manager.IncreaseTrackedResource("root.parent.leaf", TestApp1, usage, user)
	assert.Equal(t, increased, true, "unable to increase tracked resource: queuepath root.parent.leaf, app "+TestApp1+", res "+usage.String())

	increased = manager.IncreaseTrackedResource("root.parent.leaf", TestApp1, usage, user)
	assert.Equal(t, increased, false, "unable to increase tracked resource: queuepath root.parent.leaf, app "+TestApp1+", res "+usage.String())

	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) != nil, true)

	decreased = manager.DecreaseTrackedResource("root.parent.leaf", TestApp1, usage, user, true)
	assert.Equal(t, decreased, true, "unable to decrease tracked resource: queuepath root.parent.leaf, app "+TestApp1+", res "+usage.String())

	userTrackers := manager.GetUsersResources()
	userTracker := userTrackers[0]
	groupTrackers := manager.GetGroupsResources()
	groupTracker := groupTrackers[0]
	assert.Equal(t, true, manager.isUserRemovable(userTracker))
	assert.Equal(t, true, manager.isGroupRemovable(groupTracker))

	conf = createConfigWithoutLimits()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	for i := 1; i <= 2; i++ {
		increased := manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
		assert.Equal(t, increased, true, "unable to increase tracked resource: queuepath "+queuePath1+", app "+TestApp1+", res "+usage.String())
	}
	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) == nil, true)

	decreased = manager.DecreaseTrackedResource(queuePath1, TestApp1, usage, user, false)
	assert.Equal(t, decreased, true)

	decreased = manager.DecreaseTrackedResource(queuePath1, TestApp1, usage, user, true)
	assert.Equal(t, decreased, true)
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
	headroom := manager.Headroom("root.parent.leaf", user)
	assert.Equal(t, resources.Equals(headroom, usage), true)

	increased := manager.IncreaseTrackedResource("root.parent.leaf", TestApp1, usage, user)
	assert.Equal(t, increased, true, "unable to increase tracked resource: queuepath "+queuePath1+", app "+TestApp1+", res "+usage.String())

	headroom = manager.Headroom("root.parent.leaf", user)
	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) != nil, true)
	assert.Equal(t, resources.Equals(headroom, resources.Multiply(usage, 0)), true)

	increased = manager.IncreaseTrackedResource("root.parent.leaf", TestApp1, usage, user)
	assert.Equal(t, increased, false, "unable to increase tracked resource: queuepath "+queuePath1+", app "+TestApp1+", res "+usage.String())

	// configure limits only for group
	conf = createUpdateConfigWithWildCardUsersAndGroups("", user.Groups[0], "*", "*", "80", "80")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	usage1, err := resources.NewResourceFromConf(map[string]string{"memory": "70", "vcores": "70"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage)
	}
	// ensure group headroom returned when there is no limit settings configured for user
	headroom = manager.Headroom("root.parent", user)
	assert.Equal(t, resources.Equals(headroom, resources.Sub(usage1, usage)), true)
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
}

func assertUGM(t *testing.T, userGroup security.UserGroup, expected *resources.Resource, usersCount int) {
	manager := GetUserManager()
	assert.Equal(t, usersCount, len(manager.GetUsersResources()), "userTrackers count should be "+strconv.Itoa(usersCount))
	assert.Equal(t, 0, len(manager.GetGroupsResources()), "groupTrackers count should be "+strconv.Itoa(0))
	userRes := manager.GetUserResources(userGroup)
	assert.Equal(t, resources.Equals(userRes, expected), true)
	groupRes := manager.GetGroupResources(userGroup.Groups[0])
	assert.Equal(t, resources.Equals(groupRes, nil), true)
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
