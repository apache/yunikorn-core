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
	"testing"

	"gotest.tools/assert"

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

	userTrackers := manager.GetUserTrackers()
	userTracker := userTrackers["test"]
	groupTrackers := manager.GetGroupTrackers()
	groupTracker := groupTrackers["test"]
	assert.Equal(t, false, manager.isUserRemovable(userTracker))
	assert.Equal(t, false, manager.isGroupRemovable(groupTracker))
	assertUGM(t, user, usage1, 1)

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
	assert.Equal(t, 1, len(manager.GetUserTrackers()), "userTrackers count should be 1")
	assert.Equal(t, 1, len(manager.GetGroupTrackers()), "groupTrackers count should be 1")

	err = manager.DecreaseTrackedResource(queuePath2, TestApp2, usage2, user1, true)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage2, err)
	}
	assert.Equal(t, 0, len(manager.GetUserTrackers()), "userTrackers count should be 0")
	assert.Equal(t, 0, len(manager.GetGroupTrackers()), "groupTrackers count should be 0")
}

func assertUGM(t *testing.T, userGroup security.UserGroup, expected *resources.Resource, usersCount int) {
	manager := GetUserManager()
	assert.Equal(t, usersCount, len(manager.GetUserTrackers()), "userTrackers count should be 2")
	assert.Equal(t, usersCount, len(manager.GetGroupTrackers()), "groupTrackers count should be 2")
	userRes, err := manager.GetUserResources(userGroup)
	assert.NilError(t, err)
	assert.Equal(t, userRes.String(), expected.String())
	groupRes, err := manager.GetGroupResources(userGroup.Groups[0])
	assert.NilError(t, err)
	assert.Equal(t, groupRes.String(), expected.String())
	assert.Equal(t, usersCount, len(manager.GetUsersResources()))
	assert.Equal(t, usersCount, len(manager.GetGroupsResources()))
}
