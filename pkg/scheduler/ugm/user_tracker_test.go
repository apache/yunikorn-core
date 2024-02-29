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
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	TestApp1 = "test-app-1"
	TestApp2 = "test-app-2"
	TestApp3 = "test-app-3"
	TestApp4 = "test-app-4"

	queuePath1 = "root.parent.child1"
	queuePath2 = "root.parent.child2"
	queuePath3 = "root.parent.child1.child12"
	queuePath4 = "root.parent.child12"
)

const (
	path1 = "root.parent.child1"
	path2 = "root.parent.child2"
	path3 = "root.parent.child1.child12"
	path4 = "root.parent.child12"
	path5 = "root.parent"
)

func TestIncreaseTrackedResource(t *testing.T) {
	// Queue setup:
	// root->parent->child1->child12
	// root->parent->child2
	// root->parent->child12 (similar name like above leaf queue, but it is being treated differently as similar names are allowed)
	// Initialize ugm
	GetUserManager()
	user := security.UserGroup{User: "test", Groups: []string{"test"}}
	eventSystem := mock.NewEventSystem()
	userTracker := newUserTracker(user.User, newUGMEvents(eventSystem))
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}
	userTracker.increaseTrackedResource(path1, TestApp1, usage1)
	groupTracker := newGroupTracker(user.User, newUGMEvents(mock.NewEventSystemDisabled()))
	userTracker.setGroupForApp(TestApp1, groupTracker)
	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	userTracker.increaseTrackedResource(path2, TestApp2, usage2)
	assert.Equal(t, 3, len(eventSystem.Events))
	assert.Equal(t, si.EventRecord_UG_USER_RESOURCE, eventSystem.Events[0].EventChangeDetail)
	assert.Equal(t, si.EventRecord_ADD, eventSystem.Events[0].EventChangeType)
	assert.Equal(t, si.EventRecord_UG_APP_LINK, eventSystem.Events[1].EventChangeDetail)
	assert.Equal(t, "test", eventSystem.Events[1].ObjectID)
	assert.Equal(t, TestApp1, eventSystem.Events[1].ReferenceID)
	assert.Equal(t, si.EventRecord_SET, eventSystem.Events[1].EventChangeType)
	assert.Equal(t, si.EventRecord_UG_USER_RESOURCE, eventSystem.Events[2].EventChangeDetail)
	assert.Equal(t, si.EventRecord_ADD, eventSystem.Events[2].EventChangeType)

	userTracker.setGroupForApp(TestApp2, groupTracker)

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "30M", "vcore": "30"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	userTracker.increaseTrackedResource(path3, TestApp3, usage3)
	userTracker.setGroupForApp(TestApp3, groupTracker)

	usage4, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	userTracker.increaseTrackedResource(path4, TestApp4, usage4)
	userTracker.setGroupForApp(TestApp4, groupTracker)

	actualResources := getUserResource(userTracker)
	assert.Equal(t, "map[mem:80000000 vcore:80000]", actualResources["root"].String(), "wrong resource")
	assert.Equal(t, "map[mem:80000000 vcore:80000]", actualResources["root.parent"].String(), "wrong resource")
	assert.Equal(t, "map[mem:40000000 vcore:40000]", actualResources["root.parent.child1"].String(), "wrong resource")
	assert.Equal(t, "map[mem:30000000 vcore:30000]", actualResources["root.parent.child1.child12"].String(), "wrong resource")
	assert.Equal(t, "map[mem:20000000 vcore:20000]", actualResources["root.parent.child2"].String(), "wrong resource")
	assert.Equal(t, "map[mem:20000000 vcore:20000]", actualResources["root.parent.child12"].String(), "wrong resource")
	assert.Equal(t, 4, len(userTracker.getTrackedApplications()))
}

func TestDecreaseTrackedResource(t *testing.T) {
	// Queue setup:
	// root->parent->child1
	// root->parent->child2
	// Initialize ugm
	GetUserManager()
	user := security.UserGroup{User: "test", Groups: []string{"test"}}
	eventSystem := mock.NewEventSystem()
	userTracker := newUserTracker(user.User, newUGMEvents(eventSystem))

	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "70M", "vcore": "70"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}
	userTracker.increaseTrackedResource(path1, TestApp1, usage1)
	groupTracker := newGroupTracker(user.User, newUGMEvents(mock.NewEventSystemDisabled())
	userTracker.setGroupForApp(TestApp1, groupTracker)
	assert.Equal(t, 1, len(userTracker.getTrackedApplications()))

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	userTracker.increaseTrackedResource(path2, TestApp2, usage2)
	userTracker.setGroupForApp(TestApp2, groupTracker)
	actualResources := getUserResource(userTracker)

	assert.Equal(t, 2, len(userTracker.getTrackedApplications()))
	assert.Equal(t, "map[mem:90000000 vcore:90000]", actualResources["root"].String(), "wrong resource")
	assert.Equal(t, "map[mem:90000000 vcore:90000]", actualResources["root.parent"].String(), "wrong resource")
	assert.Equal(t, "map[mem:70000000 vcore:70000]", actualResources["root.parent.child1"].String(), "wrong resource")
	assert.Equal(t, "map[mem:20000000 vcore:20000]", actualResources["root.parent.child2"].String(), "wrong resource")

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	removeQT := userTracker.decreaseTrackedResource(path1, TestApp1, usage3, false)
	eventSystem.Reset()
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")
	assert.Equal(t, si.EventRecord_UG_USER_RESOURCE, eventSystem.Events[0].EventChangeDetail)
	assert.Equal(t, si.EventRecord_REMOVE, eventSystem.Events[0].EventChangeType)

	removeQT = userTracker.decreaseTrackedResource(path2, TestApp2, usage3, false)
	actualResources1 := getUserResource(userTracker)

	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")
	assert.Equal(t, "map[mem:70000000 vcore:70000]", actualResources1["root"].String(), "wrong resource")
	assert.Equal(t, "map[mem:70000000 vcore:70000]", actualResources1["root.parent"].String(), "wrong resource")
	assert.Equal(t, "map[mem:60000000 vcore:60000]", actualResources1["root.parent.child1"].String(), "wrong resource")
	assert.Equal(t, "map[mem:10000000 vcore:10000]", actualResources1["root.parent.child2"].String(), "wrong resource")

	usage4, err := resources.NewResourceFromConf(map[string]string{"mem": "60M", "vcore": "60"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}

	removeQT = userTracker.decreaseTrackedResource(path1, TestApp1, usage4, true)
	eventSystem.Reset()
	assert.Equal(t, 1, len(userTracker.getTrackedApplications()))
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")
	assert.Equal(t, 2, len(eventSystem.Events))
	assert.Equal(t, si.EventRecord_UG_USER_RESOURCE, eventSystem.Events[0].EventChangeDetail)
	assert.Equal(t, si.EventRecord_REMOVE, eventSystem.Events[0].EventChangeType)
	assert.Equal(t, si.EventRecord_UG_APP_LINK, eventSystem.Events[1].EventChangeDetail)
	assert.Equal(t, si.EventRecord_REMOVE, eventSystem.Events[1].EventChangeType)

	usage5, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage5)
	}
	removeQT = userTracker.decreaseTrackedResource(path2, TestApp2, usage5, true)
	assert.Equal(t, 0, len(userTracker.getTrackedApplications()))
	assert.Equal(t, removeQT, true, "wrong remove queue tracker value")
}

func TestSetAndClearMaxLimits(t *testing.T) {
	// Queue setup:
	// root->parent->child1
	// Initialize ugm
	GetUserManager()
	user := security.UserGroup{User: "test", Groups: []string{"test"}}
	eventSystem := mock.NewEventSystem()
	userTracker := newUserTracker(user.User, newUGMEvents(eventSystem))
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}
	userTracker.increaseTrackedResource(path1, TestApp1, usage1)

	eventSystem.Reset()
	userTracker.setLimits(path1, resources.Multiply(usage1, 5), 5, false, false)
	userTracker.setLimits(path5, resources.Multiply(usage1, 10), 10, false, false)
	assert.Equal(t, 2, len(eventSystem.Events))
	assert.Equal(t, si.EventRecord_UG_USER_LIMIT, eventSystem.Events[0].EventChangeDetail)
	assert.Equal(t, si.EventRecord_SET, eventSystem.Events[0].EventChangeType)
	assert.Equal(t, path1, eventSystem.Events[0].ReferenceID)
	assert.Equal(t, si.EventRecord_UG_USER_LIMIT, eventSystem.Events[1].EventChangeDetail)
	assert.Equal(t, si.EventRecord_SET, eventSystem.Events[1].EventChangeType)
	assert.Equal(t, path5, eventSystem.Events[1].ReferenceID)

	userTracker.increaseTrackedResource(path1, TestApp1, usage1)
  userTracker.increaseTrackedResource(path1, TestApp2, usage1)
	path1expectedHeadroom := resources.NewResourceFromMap(map[string]resources.Quantity{
		"mem":   20000000,
		"vcore": 20000,
	})
	hierarchy1 := strings.Split(path1, configs.DOT)
	assert.Assert(t, resources.Equals(userTracker.headroom(hierarchy1), path1expectedHeadroom))
	path5expectedHeadroom := resources.NewResourceFromMap(map[string]resources.Quantity{
		"mem":   70000000,
		"vcore": 70000,
	})
	hierarchy5 := strings.Split(path5, configs.DOT)
	assert.Assert(t, resources.Equals(userTracker.headroom(hierarchy5), path5expectedHeadroom))
	assert.Assert(t, userTracker.canRunApp(hierarchy1, TestApp4))

	// lower limits
	userTracker.setLimits(path1, usage1, 1, false, false)
	userTracker.setLimits(path5, resources.Multiply(usage1, 2), 1, false, false)
	lowerChildHeadroom := resources.NewResourceFromMap(map[string]resources.Quantity{
		"mem":   -20000000,
		"vcore": -20000,
	})
	assert.Assert(t, resources.Equals(userTracker.headroom(hierarchy1), lowerChildHeadroom))
	lowerParentHeadroom := resources.NewResourceFromMap(map[string]resources.Quantity{
		"mem":   -10000000,
		"vcore": -10000,
	})
	assert.Assert(t, resources.Equals(userTracker.headroom(hierarchy5), lowerParentHeadroom))
	assert.Assert(t, !userTracker.canRunApp(hierarchy1, TestApp4))
	assert.Assert(t, !userTracker.canRunApp(hierarchy5, TestApp4))

	// clear limits
	eventSystem.Reset()
	userTracker.clearLimits(path1, true)
	assert.Assert(t, resources.Equals(userTracker.headroom(hierarchy1), lowerParentHeadroom))
	assert.Assert(t, resources.Equals(userTracker.headroom(hierarchy5), lowerParentHeadroom))
	assert.Assert(t, !userTracker.canRunApp(hierarchy1, TestApp4))
	assert.Assert(t, !userTracker.canRunApp(hierarchy5, TestApp4))
	userTracker.clearLimits(path5, true)
	assert.Assert(t, userTracker.headroom(hierarchy1) == nil)
	assert.Assert(t, userTracker.headroom(hierarchy5) == nil)
	assert.Assert(t, userTracker.canRunApp(hierarchy1, TestApp4))
	assert.Assert(t, userTracker.canRunApp(hierarchy5, TestApp4))
	assert.Equal(t, 2, len(eventSystem.Events))
	assert.Equal(t, si.EventRecord_REMOVE, eventSystem.Events[0].EventChangeType)
	assert.Equal(t, si.EventRecord_UG_USER_LIMIT, eventSystem.Events[0].EventChangeDetail)
	assert.Equal(t, si.EventRecord_REMOVE, eventSystem.Events[1].EventChangeType)
	assert.Equal(t, si.EventRecord_UG_USER_LIMIT, eventSystem.Events[1].EventChangeDetail)
}

func TestUTCanRunApp(t *testing.T) {
	manager := GetUserManager()
	defer manager.ClearConfigLimits()
	user := security.UserGroup{User: "test", Groups: []string{"test"}}
	userTracker := newUserTracker(user.User, newUGMEvents(mock.NewEventSystemDisabled()))
	maxRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"cpu": 1000,
	})
	manager.userWildCardLimitsConfig["root.parent"] = &LimitConfig{
		maxResources:    maxRes,
		maxApplications: 3,
	} // manipulate map directly to avoid hierarchy creation

	hierarchy1 := strings.Split(path1, configs.DOT)
	assert.Assert(t, userTracker.canRunApp(hierarchy1, TestApp1))
	// make sure wildcard limits are applied
	assert.Equal(t, uint64(3), userTracker.queueTracker.childQueueTrackers["parent"].maxRunningApps)
	assert.Assert(t, resources.Equals(maxRes, userTracker.queueTracker.childQueueTrackers["parent"].maxResources))
	assert.Assert(t, userTracker.queueTracker.childQueueTrackers["parent"].useWildCard)

	// maxApps limit hit
	userTracker.setLimits(path1, nil, 1, false, false)
	userTracker.increaseTrackedResource(path1, TestApp1, resources.NewResourceFromMap(map[string]resources.Quantity{
		"cpu": 1000,
	}))
	assert.Assert(t, userTracker.canRunApp(hierarchy1, TestApp1))
	assert.Assert(t, !userTracker.canRunApp(hierarchy1, TestApp2))
}

func getUserResource(ut *UserTracker) map[string]*resources.Resource {
	resources := make(map[string]*resources.Resource)
	usage := ut.GetUserResourceUsageDAOInfo()
	return internalGetResource(usage.Queues, resources)
}
