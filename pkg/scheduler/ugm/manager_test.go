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
	manager := newManager()
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
	manager := newManager()

	err = manager.IncreaseTrackedResource("", "", usage1, user)
	assert.Error(t, err, "mandatory parameters are missing. queuepath: , application id: , resource usage: "+usage1.String()+", user: "+user.User)

	err = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage1, user)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage1, err)
	}

	userTrackers := manager.getUserTrackers()
	userTracker := userTrackers["test"]
	groupTrackers := manager.getGroupTrackers()
	groupTracker := groupTrackers["test"]
	assert.Equal(t, false, manager.isUserRemovable(userTracker))
	assert.Equal(t, false, manager.isGroupRemovable(groupTracker))
	assert.Equal(t, 1, len(manager.getUserTrackers()), "userTrackers count should be 1")
	assert.Equal(t, 1, len(manager.getGroupTrackers()), "groupTrackers count should be 1")

	err = manager.IncreaseTrackedResource(queuePath1, TestApp1, usage1, user)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage1, err)
	}
	assert.Equal(t, 1, len(manager.getUserTrackers()), "userTrackers count should be 1")
	assert.Equal(t, 1, len(manager.getGroupTrackers()), "groupTrackers count should be 1")

	user1 := security.UserGroup{User: "test1", Groups: []string{"test1"}}
	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	err = manager.IncreaseTrackedResource(queuePath2, TestApp2, usage2, user1)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage2, err)
	}
	assert.Equal(t, 2, len(manager.getUserTrackers()), "userTrackers count should be 2")
	assert.Equal(t, 2, len(manager.getGroupTrackers()), "groupTrackers count should be 2")

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
	assert.Equal(t, 2, len(manager.getUserTrackers()), "userTrackers count should be 2")
	assert.Equal(t, 2, len(manager.getGroupTrackers()), "groupTrackers count should be 2")

	err = manager.DecreaseTrackedResource(queuePath1, TestApp1, usage3, user, true)
	if err != nil {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage3, err)
	}
	assert.Equal(t, 1, len(manager.getUserTrackers()), "userTrackers count should be 1")
	assert.Equal(t, 1, len(manager.getGroupTrackers()), "groupTrackers count should be 1")

	err = manager.DecreaseTrackedResource(queuePath2, TestApp2, usage2, user1, true)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage2, err)
	}
	assert.Equal(t, 0, len(manager.getUserTrackers()), "userTrackers count should be 0")
	assert.Equal(t, 0, len(manager.getGroupTrackers()), "groupTrackers count should be 0")
}
