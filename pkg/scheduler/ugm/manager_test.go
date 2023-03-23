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
	"testing"

	"gotest.tools/v3/assert"

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

func TestPreserveRunningApps(t *testing.T) {
	var app1Res *resources.Resource
	var app2Res *resources.Resource
	var twoAppRes *resources.Resource
	var err error
	app1Res, err = resources.NewResourceFromConf(map[string]string{"memory": "10000", "vcore": "10m"})
	assert.NilError(t, err, "could not create resource")
	app2Res, err = resources.NewResourceFromConf(map[string]string{"memory": "25000", "vcore": "15m"})
	assert.NilError(t, err, "could not create resource")
	twoAppRes, err = resources.NewResourceFromConf(map[string]string{"memory": "35000", "vcore": "25m"})
	assert.NilError(t, err, "could not create resource")

	user := security.UserGroup{
		User:   "testUser",
		Groups: []string{"dev"},
	}
	manager := newManager()

	err = manager.IncreaseTrackedResource("root.parent.subparent.leaf", "app-1", app1Res, user)
	assert.NilError(t, err, "could not increase tracked resource")
	err = manager.IncreaseTrackedResource("root.parent.subparent.leaf", "app-2", app2Res, user)
	assert.NilError(t, err, "could not increase tracked resource")

	// non-existing queue
	moveResult, err2 := manager.PreserveRunningApplications("root.parent.subparent.nonexistingleaf")
	assert.NilError(t, err2, "unexpected error")
	assert.Equal(t, 0, moveResult.NumAppsForUser)
	assert.Equal(t, 0, moveResult.NumAppsForGroups)
	assert.Equal(t, 0, len(moveResult.Groups))
	assert.Equal(t, 0, len(moveResult.Users))
	assert.Assert(t, resources.Equals(resources.Zero, moveResult.TotalGroupTrackedResource), "moved resource of groups is not empty")
	assert.Assert(t, resources.Equals(resources.Zero, moveResult.TotalGroupTrackedResource), "moved resource of users is not empty")
	assert.Equal(t, 1, len(manager.GetUsersResources()))
	assert.Equal(t, 1, len(manager.GetGroupsResources()))
	userTracker := manager.GetUsersResources()[0]
	checkResource(t, twoAppRes, "root", userTracker.queueTracker)
	checkResource(t, twoAppRes, "root.parent", userTracker.queueTracker)
	checkResource(t, twoAppRes, "root.parent.subparent", userTracker.queueTracker)
	checkResource(t, twoAppRes, "root.parent.subparent.leaf", userTracker.queueTracker)
	_, found := userTracker.GetResourceUsage("root.preserved#")
	assert.Equal(t, false, found)
	groupTracker := manager.GetGroupsResources()[0]
	checkResource(t, twoAppRes, "root", userTracker.queueTracker)
	checkResource(t, twoAppRes, "root.parent", userTracker.queueTracker)
	checkResource(t, twoAppRes, "root.parent.subparent", userTracker.queueTracker)
	checkResource(t, twoAppRes, "root.parent.subparent.leaf", userTracker.queueTracker)
	_, found = groupTracker.GetResourceUsage("root.preserved#")
	assert.Equal(t, false, found)

	// existing queue
	moveResult, err = manager.PreserveRunningApplications("root.parent.subparent.leaf")
	assert.NilError(t, err, "unexpected error")
	assert.DeepEqual(t, []string{"dev"}, moveResult.Groups)
	assert.DeepEqual(t, []string{"testUser"}, moveResult.Users)
	assert.Equal(t, 2, moveResult.NumAppsForUser)
	assert.Equal(t, 2, moveResult.NumAppsForGroups)
	assert.Assert(t, resources.Equals(twoAppRes, moveResult.TotalUserTrackedResource))
	assert.Assert(t, resources.Equals(twoAppRes, moveResult.TotalGroupTrackedResource))
	assert.Equal(t, 1, len(manager.GetUsersResources()))
	assert.Equal(t, 1, len(manager.GetGroupsResources()))
	userTracker = manager.GetUsersResources()[0]
	checkResource(t, twoAppRes, "root", userTracker.queueTracker)
	checkResource(t, resources.Zero, "root.parent", userTracker.queueTracker)
	checkResource(t, resources.Zero, "root.parent.subparent", userTracker.queueTracker)
	checkResource(t, resources.Zero, "root.parent.subparent.leaf", userTracker.queueTracker)
	checkResource(t, twoAppRes, "root.preserved#", userTracker.queueTracker)
	groupTracker = manager.GetGroupsResources()[0]
	checkResource(t, twoAppRes, "root", groupTracker.queueTracker)
	checkResource(t, resources.Zero, "root.parent", groupTracker.queueTracker)
	checkResource(t, resources.Zero, "root.parent.subparent", groupTracker.queueTracker)
	checkResource(t, resources.Zero, "root.parent.subparent.leaf", groupTracker.queueTracker)
	checkResource(t, twoAppRes, "root.preserved#", groupTracker.queueTracker)
}

func checkResource(t *testing.T, expected *resources.Resource, queue string, tracker *QueueTracker) {
	res, found := tracker.getResourceUsageForQueue(queue)
	assert.Equal(t, true, found)
	assert.Assert(t, resources.Equals(expected, res),
		fmt.Sprintf("tracked resource mismatch for queue %s, expected %v, got %v", queue, expected, res))
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
