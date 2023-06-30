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
	group, err := manager.getGroup(user)
	assert.NilError(t, err)
	assert.Equal(t, group, "test")
	user = security.UserGroup{User: "test", Groups: []string{}}
	group, err = manager.getGroup(user)
	assert.Equal(t, err.Error(), "group is not available in usergroup for user test")
	assert.Equal(t, group, "")
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
	groupTracker := groupTrackers[0]
	assert.Equal(t, false, manager.isUserRemovable(userTracker))
	assert.Equal(t, false, manager.isGroupRemovable(groupTracker))
	assertUGM(t, user, usage1, 1)
	assert.Equal(t, user.User, manager.GetUserTracker(user.User).userName)
	assert.Equal(t, user.Groups[0], manager.GetGroupTracker(user.Groups[0]).groupName)

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
	assert.Equal(t, user.Groups[0], manager.GetGroupTracker(user.Groups[0]).groupName)
	assert.Equal(t, user1.User, manager.GetUserTracker(user1.User).userName)
	assert.Equal(t, user1.Groups[0], manager.GetGroupTracker(user1.Groups[0]).groupName)

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
	assert.Equal(t, 1, len(manager.GetGroupsResources()), "groupTrackers count should be 1")

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

	// configure max resource for root.parent to allow one more application to run
	conf = createConfig(user.User, user.Groups[0], "memory", "50", 60, 6)
	err = manager.UpdateConfig(conf.Queues[0], "root")
	assert.NilError(t, err, "unable to set the limit for user user1 because current resource usage is greater than config max resource for root.parent")

	// should run as user 'user' setting is map[memory:60 vcores:60] and total usage of "root.parent" is map[memory:50 vcores:50]
	increased := manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
	if !increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}

	// should not run as user 'user' setting is map[memory:60 vcores:60] and total usage of "root.parent" is map[memory:60 vcores:60]
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
	if increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}

	// configure max resource for root.parent to allow one more application to run through wild card user settings (not through specific user)
	user1 := security.UserGroup{User: "user2", Groups: []string{"group2"}}
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, user1.Groups[0], "*", "*", "70", "70")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	// should run as wild card user '*' setting is map[memory:70 vcores:70] and total usage of "root.parent" is map[memory:60 vcores:60]
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
	if !increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}

	// should not run as wild card user '*' setting is map[memory:70 vcores:70] and total usage of "root.parent" is map[memory:70 vcores:70]
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
	if increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}

	// configure max resource for root.parent to allow one more application to run through wild card group settings (not through specific group)
	// also wild card user limit settings not set
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, user1.Groups[0], "", "*", "80", "80")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	// should run as wild card group '*' setting is map[memory:80 vcores:80] and total usage of "root.parent" is map[memory:70 vcores:70]
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
	if !increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}

	// should not run as wild card group '*' setting is map[memory:80 vcores:80] and total usage of "root.parent" is map[memory:80 vcores:80]
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
	if increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}

	// should run as wild card group '*' setting is map[memory:80 vcores:80] and total usage of "root.parent" is map[memory:70 vcores:70]
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user1)
	if !increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user1)
	}

	// configure max resource for user2 * root.parent (map[memory:70 vcores:70]) higher than wild card user * root.parent settings (map[memory:10 vcores:10])
	// ensure user's specific settings overrides the wild card user limit settings
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, user1.Groups[0], "", "*", "10", "10")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	// can be allowed to run upto resource usage map[memory:70 vcores:70]
	for i := 1; i <= 6; i++ {
		increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user1)
		if !increased {
			t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
		}
	}

	// should not run as user2 max limit is map[memory:70 vcores:70] and usage so far is map[memory:70 vcores:70]
	increased = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user1)
	if increased {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, user)
	}
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

	// create config user2 * root.parent with [50, 50] and maxapps as 5 (twice for root), but not user1. so user1 should not be there as it doesn't have any running applications
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
	assert.Equal(t, manager.GetUserTracker(user.User) == nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) == nil, true)
	assertMaxLimits(t, user1, expectedResource, 6)

	expectedResource, err = resources.NewResourceFromConf(map[string]string{"memory": "70", "vcores": "70"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}

	// override user2 * root.parent maxapps as [70, 70] and maxapps as 10 (twice for root)
	// and wild card settings
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, user1.Groups[0], "*", "*", "10", "10")
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
	assert.Equal(t, manager.GetUserTracker(user.User) == nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) == nil, true)
	assertMaxLimits(t, user1, expectedResource, 10)
	assertWildCardLimits(t, m.userWildCardLimitsConfig, expectedResource)
	assertWildCardLimits(t, m.groupWildCardLimitsConfig, expectedResource)

	print("ggg")
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

	conf = createConfigWithoutLimits()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	for i := 1; i <= 2; i++ {
		increased := manager.IncreaseTrackedResource(queuePath1, TestApp1, usage, user)
		assert.Equal(t, increased, true, "unable to increase tracked resource: queuepath "+queuePath1+", app "+TestApp1+", res "+usage.String())
	}
	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) != nil, true)

	decreased = manager.DecreaseTrackedResource(queuePath1, TestApp1, usage, user, false)
	assert.Equal(t, decreased, true)

	decreased = manager.DecreaseTrackedResource(queuePath1, TestApp1, usage, user, true)
	assert.Equal(t, decreased, true)
	assert.Equal(t, manager.GetUserTracker(user.User) == nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) == nil, true)
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
	assert.Equal(t, usersCount, len(manager.GetGroupsResources()), "groupTrackers count should be "+strconv.Itoa(usersCount))
	userRes := manager.GetUserResources(userGroup)
	assert.Equal(t, resources.Equals(userRes, expected), true)
	groupRes := manager.GetGroupResources(userGroup.Groups[0])
	assert.Equal(t, resources.Equals(groupRes, expected), true)
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
