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

	err = manager.IncreaseTrackedResource("", "", usage1, user)
	assert.Error(t, err, "mandatory parameters are missing. queuepath: , application id: , resource usage: "+usage1.String()+", user: "+user.User)

	err = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage1, user)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage1, err)
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

	err = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage1, user)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage1, err)
	}
	assertUGM(t, user, resources.Multiply(usage1, 2), 1)

	user1 := security.UserGroup{User: "test1", Groups: []string{"test1"}}
	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	err = manager.IncreaseTrackedResource(queuePath2, TestApp2, usage2, user1)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage2, err)
	}
	assertUGM(t, user1, usage2, 2)
	assert.Equal(t, user.User, manager.GetUserTracker(user.User).userName)
	assert.Equal(t, user.Groups[0], manager.GetGroupTracker(user.Groups[0]).groupName)
	assert.Equal(t, user1.User, manager.GetUserTracker(user1.User).userName)
	assert.Equal(t, user1.Groups[0], manager.GetGroupTracker(user1.Groups[0]).groupName)

	assert.Equal(t, true, manager.IsQueuePathTrackedCompletely(manager.GetUserTracker(user.User).queueTracker, queuePath1))
	assert.Equal(t, true, manager.IsQueuePathTrackedCompletely(manager.GetUserTracker(user1.User).queueTracker, queuePath2))
	assert.Equal(t, false, manager.IsQueuePathTrackedCompletely(manager.GetUserTracker(user1.User).queueTracker, queuePath1))
	assert.Equal(t, false, manager.IsQueuePathTrackedCompletely(manager.GetUserTracker(user.User).queueTracker, queuePath2))
	assert.Equal(t, false, manager.IsQueuePathTrackedCompletely(manager.GetUserTracker(user.User).queueTracker, queuePath3))
	assert.Equal(t, false, manager.IsQueuePathTrackedCompletely(manager.GetUserTracker(user.User).queueTracker, queuePath4))

	assert.Equal(t, true, manager.IsQueuePathTrackedCompletely(manager.GetUserTracker(user.Groups[0]).queueTracker, queuePath1))
	assert.Equal(t, true, manager.IsQueuePathTrackedCompletely(manager.GetUserTracker(user1.Groups[0]).queueTracker, queuePath2))
	assert.Equal(t, false, manager.IsQueuePathTrackedCompletely(manager.GetUserTracker(user1.Groups[0]).queueTracker, queuePath1))
	assert.Equal(t, false, manager.IsQueuePathTrackedCompletely(manager.GetUserTracker(user.Groups[0]).queueTracker, queuePath2))
	assert.Equal(t, false, manager.IsQueuePathTrackedCompletely(manager.GetUserTracker(user.Groups[0]).queueTracker, queuePath3))
	assert.Equal(t, false, manager.IsQueuePathTrackedCompletely(manager.GetUserTracker(user.Groups[0]).queueTracker, queuePath4))

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "5M", "vcore": "5"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}

	err = manager.DecreaseTrackedResource("", "", usage1, user, false)
	assert.Error(t, err, "mandatory parameters are missing. queuepath: , application id: , resource usage: "+usage1.String()+", user: "+user.User)

	err = manager.DecreaseTrackedResource(queuePath1, TestApp1, usage3, user, false)
	if err != nil {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage3, err)
	}
	assertUGM(t, user, usage1, 2)

	err = manager.DecreaseTrackedResource(queuePath1, TestApp1, usage3, user, true)
	if err != nil {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage3, err)
	}
	assert.Equal(t, 1, len(manager.GetUsersResources()), "userTrackers count should be 1")
	assert.Equal(t, 1, len(manager.GetGroupsResources()), "groupTrackers count should be 1")

	err = manager.DecreaseTrackedResource(queuePath2, TestApp2, usage2, user1, true)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage2, err)
	}
	assert.Equal(t, 0, len(manager.GetUsersResources()), "userTrackers count should be 0")
	assert.Equal(t, 0, len(manager.GetGroupsResources()), "groupTrackers count should be 0")

	assert.Assert(t, manager.GetUserTracker(user.User) == nil)
	assert.Assert(t, manager.GetGroupTracker(user.Groups[0]) == nil)
}

func TestIncreaseUserResourceWithInvalidConfig(t *testing.T) {
	setupUGM()
	// Queue setup:
	// root->parent
	user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	manager := GetUserManager()

	expectedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "5", "vcores": "5"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}

	m.userLimitsConfig[user.User] = make(map[string]map[string]interface{})
	m.userLimitsConfig[user.User][queuePath1] = make(map[string]interface{})
	m.userLimitsConfig[user.User][queuePath1][maxapplications] = -2
	assert.Error(t, manager.IncreaseTrackedResource(queuePath1, TestApp1, expectedResource, user), "unable to set the max applications. user: "+user.User+", queuepath : "+queuePath1+", applicationid: "+TestApp1)

	m.userLimitsConfig[user.User][queuePath1][maxapplications] = uint64(2)
	m.userLimitsConfig[user.User][queuePath1][maxresources] = make(map[string]interface{})
	assert.Error(t, manager.IncreaseTrackedResource(queuePath1, TestApp1, expectedResource, user), "unable to set the max resources. user: "+user.User+", queuepath : "+queuePath1+", applicationid: "+TestApp1)

	m.userLimitsConfig[user.User][queuePath1][maxapplications] = uint64(2)
	m.userLimitsConfig[user.User][queuePath1][maxresources] = map[string]string{"invalid": "-5", "vcores": "5"}
	assert.Error(t, manager.IncreaseTrackedResource(queuePath1, TestApp1, expectedResource, user), "unable to set the max resources. user: "+user.User+", queuepath : "+queuePath1+", applicationid: "+TestApp1+", usage: map[memory:5 vcores:5], reason: invalid quantity")

	m.userLimitsConfig[user.User][queuePath1][maxapplications] = uint64(2)
	m.userLimitsConfig[user.User][queuePath1][maxresources] = map[string]string{"memory": "5", "vcores": "5"}
	m.groupLimitsConfig[user.Groups[0]] = make(map[string]map[string]interface{})
	m.groupLimitsConfig[user.Groups[0]][queuePath1] = make(map[string]interface{})
	m.groupLimitsConfig[user.Groups[0]][queuePath1][maxapplications] = -2
	assert.Error(t, manager.IncreaseTrackedResource(queuePath1, TestApp1, expectedResource, user), "unable to set the max applications. group: "+user.Groups[0]+", queuepath : "+queuePath1+", applicationid: "+TestApp1)

	m.userLimitsConfig[user.User][queuePath1][maxapplications] = uint64(2)
	m.userLimitsConfig[user.User][queuePath1][maxresources] = map[string]string{"memory": "5", "vcores": "5"}
	m.groupLimitsConfig[user.Groups[0]][queuePath1][maxapplications] = uint64(2)
	m.groupLimitsConfig[user.Groups[0]][queuePath1][maxresources] = make(map[string]interface{})
	assert.Error(t, manager.IncreaseTrackedResource(queuePath1, TestApp1, expectedResource, user), "unable to set the max resources. group: "+user.Groups[0]+", queuepath : "+queuePath1+", applicationid: "+TestApp1)

	m.userLimitsConfig[user.User][queuePath1][maxapplications] = uint64(2)
	m.userLimitsConfig[user.User][queuePath1][maxresources] = map[string]string{"memory": "5", "vcores": "5"}
	m.groupLimitsConfig[user.Groups[0]][queuePath1][maxapplications] = uint64(2)
	m.groupLimitsConfig[user.Groups[0]][queuePath1][maxresources] = map[string]string{"invalid": "-5", "vcores": "5"}
	assert.Error(t, manager.IncreaseTrackedResource(queuePath1, TestApp1, expectedResource, user), "unable to set the max resources. group: "+user.Groups[0]+", queuepath : "+queuePath1+", applicationid: "+TestApp1+", reason: invalid quantity")
}

func TestUpdateConfig(t *testing.T) {
	setupUGM()
	// Queue setup:
	// root->parent
	user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	manager := GetUserManager()

	expectedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "5", "vcores": "5"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}

	conf := createConfig(user.User, user.Groups[0], "memory", "-10")
	assert.Error(t, manager.UpdateConfig(conf.Queues[0], "root"), "unable to set the limit for user user1 because invalid quantity")
	conf = createConfig(user.User, user.Groups[0], "invalid", "invalidate")
	assert.Error(t, manager.UpdateConfig(conf.Queues[0], "root"), "unable to set the limit for user user1 because invalid quantity")
	conf = createUpdateConfig(user.User, user.Groups[0])

	manager = GetUserManager()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	assertMaxLimits(t, user, expectedResource, 1)

	for i := 1; i <= 2; i++ {
		err = manager.IncreaseTrackedResource(queuePath1, TestApp1, expectedResource, user)
		if err != nil {
			t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, expectedResource, err)
		}
	}
	assert.Error(t, manager.UpdateConfig(conf.Queues[0], "root"), "unable to set the limit for user user1 because current resource usage is greater than config max resource for root.parent")

	err = manager.IncreaseTrackedResource(queuePath1, TestApp1, expectedResource, user)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, expectedResource, err)
	}
	assert.Error(t, manager.UpdateConfig(conf.Queues[0], "root"), "unable to set the limit for user user1 because current resource usage is greater than config max resource for root")
}

func TestUpdateConfigWithWildCardUsersAndGroups(t *testing.T) {
	setupUGM()
	// Queue setup:
	// root->parent
	user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	conf := createUpdateConfig(user.User, user.Groups[0])
	manager := GetUserManager()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	expectedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "5", "vcores": "5"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}
	assertMaxLimits(t, user, expectedResource, 1)

	for i := 1; i <= 2; i++ {
		err = manager.IncreaseTrackedResource(queuePath1, TestApp1, expectedResource, user)
		if err != nil {
			t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, expectedResource, err)
		}
	}
	assert.Error(t, manager.UpdateConfig(conf.Queues[0], "root"), "unable to set the limit for user user1 because current resource usage is greater than config max resource for root.parent")

	err = manager.IncreaseTrackedResource(queuePath1, TestApp1, expectedResource, user)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, expectedResource, err)
	}
	assert.Error(t, manager.UpdateConfig(conf.Queues[0], "root"), "unable to set the limit for user user1 because current resource usage is greater than config max resource for root")

	user1 := security.UserGroup{User: "user2", Groups: []string{"group2"}}
	conf = createUpdateConfigWithWildCardUsersAndGroups(user1.User, user1.Groups[0])
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))
}

func createUpdateConfigWithWildCardUsersAndGroups(user string, group string) configs.PartitionConfig {
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
								Limit: "root queue limit",
								Users: []string{
									user, "*",
								},
								Groups: []string{
									group, "*",
								},
								MaxResources: map[string]string{
									"memory": "50",
									"vcores": "50",
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
							user, "*",
						},
						Groups: []string{
							group, "*",
						},
						MaxResources: map[string]string{
							"memory": "100",
							"vcores": "100",
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
	return createConfig(user, group, "memory", "10")
}

func createConfig(user string, group string, resourceKey string, resourceValue string) configs.PartitionConfig {
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
								Limit: "root queue limit",
								Users: []string{
									user,
								},
								Groups: []string{
									group,
								},
								MaxResources: map[string]string{
									"memory": "5",
									"vcores": "5",
								},
								MaxApplications: 1,
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
							resourceKey: resourceValue,
							"vcores":    "10",
						},
						MaxApplications: 2,
					},
				},
			},
		},
	}
	return conf
}

func TestUpdateConfigClearEarlierSetLimits(t *testing.T) {
	setupUGM()
	// Queue setup:
	// root->parent
	user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	conf := createUpdateConfig(user.User, user.Groups[0])
	
	manager := GetUserManager()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	expectedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "5", "vcores": "5"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}
	assertMaxLimits(t, user, expectedResource, 1)

	user1 := security.UserGroup{User: "user2", Groups: []string{"group2"}}
	conf = createUpdateConfig(user1.User, user1.Groups[0])
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	expectedResource, err = resources.NewResourceFromConf(map[string]string{"memory": "5", "vcores": "5"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}
	assertMaxLimits(t, user, resources.NewResource(), 0)
	assertMaxLimits(t, user1, expectedResource, 1)
}

func TestSetMaxLimitsForRemovedUsers(t *testing.T) {
	setupUGM()
	// Queue setup:
	// root->parent
	user := security.UserGroup{User: "user1", Groups: []string{"group1"}}
	conf := createUpdateConfig(user.User, user.Groups[0])
	manager := GetUserManager()
	assert.NilError(t, manager.UpdateConfig(conf.Queues[0], "root"))

	expectedResource, err := resources.NewResourceFromConf(map[string]string{"memory": "5", "vcores": "5"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, expectedResource)
	}
	assertMaxLimits(t, user, expectedResource, 1)

	for i := 1; i <= 2; i++ {
		err = manager.IncreaseTrackedResource(queuePath1, TestApp1, expectedResource, user)
		if err != nil {
			t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, expectedResource, err)
		}
	}
	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) != nil, true)

	err = manager.DecreaseTrackedResource(queuePath1, TestApp1, expectedResource, user, false)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, expectedResource, err)
	}
	err = manager.DecreaseTrackedResource(queuePath1, TestApp1, expectedResource, user, true)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, expectedResource, err)
	}
	assert.Equal(t, manager.GetUserTracker(user.User) == nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) == nil, true)

	err = manager.IncreaseTrackedResource(queuePath1, TestApp1, expectedResource, user)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, expectedResource, err)
	}
	assert.Equal(t, manager.GetUserTracker(user.User) != nil, true)
	assert.Equal(t, manager.GetGroupTracker(user.Groups[0]) != nil, true)
	assertMaxLimits(t, user, expectedResource, 1)
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
	assert.Equal(t, resources.Equals(manager.GetUserTracker(userGroup.User).queueTracker.maxResourceUsage, resources.Multiply(expectedResource, 2)), true)
	assert.Equal(t, resources.Equals(manager.GetGroupTracker(userGroup.Groups[0]).queueTracker.maxResourceUsage, resources.Multiply(expectedResource, 2)), true)
	assert.Equal(t, manager.GetUserTracker(userGroup.User).queueTracker.childQueueTrackers["parent"].maxRunningApps, uint64(expectedMaxApps))
	assert.Equal(t, manager.GetGroupTracker(userGroup.Groups[0]).queueTracker.childQueueTrackers["parent"].maxRunningApps, uint64(expectedMaxApps))
	assert.Equal(t, resources.Equals(manager.GetUserTracker(userGroup.User).queueTracker.childQueueTrackers["parent"].maxResourceUsage, expectedResource), true)
	assert.Equal(t, resources.Equals(manager.GetGroupTracker(userGroup.Groups[0]).queueTracker.childQueueTrackers["parent"].maxResourceUsage, expectedResource), true)
}
